"""Build the embedding retrieval index (architecture-doc §5.1) over the
active datasets + their `dataset_mapping` fields (and, opt-in, the registered
TTL ontologies).

Each active dataset, each of its `dataset_mapping` rows and (with
--with-ontologies) each ontology class/property becomes one short text
snippet, embedded with a local sentence-transformer and stored in a
FAISS index (decided over pgvector 2026-08-21 — no OS-level Postgres
extension available on this host and no sudo to install one; FAISS needs
neither). At query time, a question gets embedded and matched against this
index to retrieve only the handful of real ontology terms / dataset fields
relevant to it, instead of dumping the whole schema into an LLM prompt.

This module is both an importable library (`build_index`, `load_index`,
`search`) and a CLI, mirroring `ontop/generate_obda.py`'s pattern. Read-only
against the database: only SELECTs.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np
import rdflib
from rdflib import RDF, RDFS, OWL

from app.db import get_connection
from app.ontology_publish import get_registry

INDEX_DIR = Path(__file__).resolve().parent.parent / "retrieval-index"
DEFAULT_INDEX_PATH = INDEX_DIR / "index.faiss"
DEFAULT_META_PATH = INDEX_DIR / "index_meta.json"

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# Cosine-similarity floor below which a top-1 hit is treated as "no relevant
# match" rather than a real answer (CPHR-89: out-of-domain queries score
# ~0.20 vs 0.5-0.85 for real matches, so 0.3 sits cleanly between the two).
NO_MATCH_THRESHOLD = 0.3


@dataclass
class IndexEntry:
    text: str
    kind: str  # "class" | "object_property" | "datatype_property" | "dataset" | "dataset_field"
    source: str  # graph_key (ttl) or "dataset_mapping"
    uri: str | None = None
    label: str | None = None
    dataset_id: int | None = None
    dataset_title: str | None = None
    field_name: str | None = None
    ontology_mapping: str | None = None
    unit: str | None = None
    value_range: str | None = None


def _local_name(uri: rdflib.URIRef) -> str:
    text = str(uri)
    return text.split("#")[-1] if "#" in text else text.rsplit("/", 1)[-1]


def _label_and_comment(graph: rdflib.Graph, subject: rdflib.URIRef) -> tuple[str, str]:
    label = graph.value(subject, RDFS.label)
    comment = graph.value(subject, RDFS.comment)
    return (str(label) if label else _local_name(subject)), (str(comment) if comment else "")


def extract_ontology_entries() -> list[IndexEntry]:
    """One entry per owl:Class / owl:ObjectProperty / owl:DatatypeProperty
    across every registered ttl (currently just "ayurveda", see
    ontology_publish.get_registry() — iterating the registry rather than
    hardcoding a path means this picks up future ontologies automatically)."""
    entries: list[IndexEntry] = []

    for graph_key, meta in get_registry().items():
        file_path: Path = meta["file_path"]
        if not file_path.exists():
            print(f"skip graph_key={graph_key}: {file_path} does not exist")
            continue

        graph = rdflib.Graph()
        graph.parse(str(file_path), format="turtle")

        for cls in sorted(graph.subjects(RDF.type, OWL.Class), key=str):
            label, comment = _label_and_comment(graph, cls)
            text = f"{label}: {comment}" if comment else label
            entries.append(IndexEntry(text=text, kind="class", source=graph_key, uri=str(cls), label=label))

        for prop in sorted(graph.subjects(RDF.type, OWL.ObjectProperty), key=str):
            label, comment = _label_and_comment(graph, prop)
            domain = graph.value(prop, RDFS.domain)
            range_ = graph.value(prop, RDFS.range)
            shape = f" ({_local_name(domain)} -> {_local_name(range_)})" if domain is not None and range_ is not None else ""
            text = f"{label}{shape}: {comment}" if comment else f"{label}{shape}"
            entries.append(IndexEntry(text=text, kind="object_property", source=graph_key, uri=str(prop), label=label))

        for prop in sorted(graph.subjects(RDF.type, OWL.DatatypeProperty), key=str):
            label, comment = _label_and_comment(graph, prop)
            domain = graph.value(prop, RDFS.domain)
            shape = f" (property of {_local_name(domain)})" if domain is not None else ""
            text = f"{label}{shape}: {comment}" if comment else f"{label}{shape}"
            entries.append(IndexEntry(text=text, kind="datatype_property", source=graph_key, uri=str(prop), label=label))

    return entries


def _unit_name(unit: str | None) -> str | None:
    """qudt IRI -> its local name ("http://qudt.org/vocab/unit/MilliM" -> "MilliM")."""
    return unit.rstrip("/").rsplit("/", 1)[-1] if unit else None


def extract_dataset_entries() -> list[IndexEntry]:
    """One entry per active dataset and one per mapped field of it --
    "this physical field, in this dataset, means this ontology concept",
    with the field's definition/unit/range from dataset_mapping.metadata so a
    question can match on any of them. Inactive datasets are skipped: they
    can't be queried, so retrieving them only misleads the agent."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.dataset_id, m.title, COALESCE(m.description, ''), COALESCE(m.keywords, ''),
                   COALESCE(c.category_name, '')
            FROM dataset_master m
            LEFT JOIN category_master c ON c.category_id = m.category_id
            WHERE m.is_active
            ORDER BY m.dataset_id
        """)
        datasets = cur.fetchall()
        cur.execute("""
            SELECT dm.dataset_id, dmr.title, dm.field_name, dm.ontology_mapping, dm.ontology_mapping_to_display,
                   dm.value_range, dm.metadata
            FROM dataset_mapping dm
            JOIN dataset_master dmr ON dmr.dataset_id = dm.dataset_id
            WHERE dmr.is_active
            ORDER BY dm.dataset_id, dm.field_name
        """)
        fields = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    entries = []
    for dataset_id, title, description, keywords, category in datasets:
        text = f"Dataset '{title}' ({category}): {description[:300]} Keywords: {keywords}"
        entries.append(IndexEntry(text=text, kind="dataset", source="dataset_master",
                                  dataset_id=dataset_id, dataset_title=title))

    for dataset_id, title, field_name, ontology_mapping, display, value_range, metadata in fields:
        metadata = metadata or {}
        concept = display or ontology_mapping
        text = f"Dataset '{title}' has field '{field_name}', representing '{concept}'"
        if metadata.get("definition"):
            text += f": {metadata['definition']}"
        unit = _unit_name(metadata.get("unit"))
        if unit:
            text += f" Unit: {unit}."
        entries.append(IndexEntry(
            text=text, kind="dataset_field", source="dataset_mapping",
            dataset_id=dataset_id, dataset_title=title,
            field_name=field_name, ontology_mapping=ontology_mapping,
            unit=unit, value_range=value_range,
        ))
    return entries


def build_index(
    index_path: Path = DEFAULT_INDEX_PATH,
    meta_path: Path = DEFAULT_META_PATH,
    with_ontologies: bool = False,
) -> dict[str, int]:
    """with_ontologies also indexes every class/property of every registered
    ttl. Off by default: the registry is now full public ontologies (DOID,
    ENVO, SWEET, CF...), whose tens of thousands of terms drown out the few
    dozen dataset fields a question can actually be answered from."""
    import faiss
    from sentence_transformers import SentenceTransformer

    entries = extract_dataset_entries()
    if with_ontologies:
        entries += extract_ontology_entries()
    if not entries:
        raise SystemExit("No entries extracted from ttl/dataset_mapping — nothing to index.")

    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    vectors = model.encode([e.text for e in entries], convert_to_numpy=True, normalize_embeddings=True).astype("float32")

    index = faiss.IndexFlatIP(vectors.shape[1])  # normalized vectors -> inner product == cosine similarity
    index.add(vectors)

    index_path.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(index_path))
    meta_path.write_text(
        json.dumps({"model": EMBEDDING_MODEL_NAME, "entries": [asdict(e) for e in entries]}, indent=2),
        encoding="utf-8",
    )

    by_kind: dict[str, int] = {}
    for e in entries:
        by_kind[e.kind] = by_kind.get(e.kind, 0) + 1
    return by_kind


def load_index(index_path: Path = DEFAULT_INDEX_PATH, meta_path: Path = DEFAULT_META_PATH):
    import faiss

    index = faiss.read_index(str(index_path))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return index, meta["model"], [IndexEntry(**e) for e in meta["entries"]]


# Loaded once per process and reloaded only when the index file changes:
# the agent calls search() several times per question, and loading the
# sentence-transformer alone takes seconds.
_loaded: dict[tuple, tuple] = {}
_models: dict = {}


def _cached_index(index_path: Path, meta_path: Path):
    key = (str(index_path), str(meta_path))
    mtime = (index_path.stat().st_mtime, meta_path.stat().st_mtime)
    if key not in _loaded or _loaded[key][0] != mtime:
        _loaded[key] = (mtime, load_index(index_path, meta_path))
    return _loaded[key][1]


def _cached_model(model_name: str):
    if model_name not in _models:
        from sentence_transformers import SentenceTransformer
        _models[model_name] = SentenceTransformer(model_name)
    return _models[model_name]


def search(
    query: str,
    k: int = 5,
    index_path: Path = DEFAULT_INDEX_PATH,
    meta_path: Path = DEFAULT_META_PATH,
    min_score: float | None = None,
) -> list[tuple[float, IndexEntry]]:
    """min_score filters out hits below the threshold (e.g. NO_MATCH_THRESHOLD)
    so a caller can tell "no relevant match" apart from a genuine low-ranked hit."""
    index, model_name, entries = _cached_index(index_path, meta_path)
    model = _cached_model(model_name)
    query_vec = model.encode([query], convert_to_numpy=True, normalize_embeddings=True).astype("float32")
    scores, indices = index.search(query_vec, k)
    results = [(float(score), entries[i]) for score, i in zip(scores[0], indices[0]) if i != -1]
    if min_score is not None:
        results = [(score, entry) for score, entry in results if score >= min_score]
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--index", type=Path, default=DEFAULT_INDEX_PATH)
    ap.add_argument("--meta", type=Path, default=DEFAULT_META_PATH)
    ap.add_argument("--query", type=str, default=None, help="If given, skip building and just run a search against the existing index.")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--with-ontologies", action="store_true", help="Also index every registered ttl's classes/properties (slow, large).")
    ap.add_argument("--min-score", type=float, default=None, help=f"Drop hits below this cosine similarity (try {NO_MATCH_THRESHOLD} for 'no match' filtering).")
    args = ap.parse_args()

    if args.query:
        results = search(args.query, k=args.k, index_path=args.index, meta_path=args.meta, min_score=args.min_score)
        if not results:
            print("(no match above threshold)")
        for score, entry in results:
            print(f"{score:.4f}  [{entry.kind}]  {entry.text}")
        return

    counts = build_index(args.index, args.meta, with_ontologies=args.with_ontologies)
    print(f"Wrote index to {args.index} and metadata to {args.meta}")
    print("Entries by kind:", counts, "- total:", sum(counts.values()))


if __name__ == "__main__":
    main()
