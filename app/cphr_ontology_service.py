from __future__ import annotations

import hashlib
import json
import os
import threading
from collections import defaultdict
from pathlib import Path

import rdflib
from rdflib import BNode, Literal, URIRef
from rdflib.collection import Collection
from rdflib.namespace import DCTERMS, OWL, RDF, RDFS, SKOS, XSD

from app import ontology_publish

# Read API over any ontology registered in ontology-ttl-files/_registry.json
# (the same registry POST /ontology/ttl/{graph_key} writes to). Every public
# function takes the registry graph_key, so /ontology/summary, /classes,
# /properties, /graph, /search, /field-map and /ontology/v3/classes work for
# every uploaded ontology instead of one hardcoded TTL.
#
# Parsing is expensive for the big ontologies (DOID: ~300k triples, ~18s and
# ~430MB while parsing), so the parsed graph is never kept -- only the small
# snapshot dict built from it, cached in memory and as JSON on disk. Both
# caches are keyed by the file's path + size + mtime, so re-uploading a TTL
# invalidates them automatically. The disk cache lives outside the repo
# (rsync --delete deploys would wipe anything inside it).

_CACHE_DIR = Path(os.getenv("CML_ONTOLOGY_CACHE_DIR", str(Path.home() / ".cache" / "cml-ontology-snapshots")))
_SNAPSHOT_FORMAT_VERSION = 3

_OBO_DEFINITION = URIRef("http://purl.obolibrary.org/obo/IAO_0000115")
_LABEL_PREDICATES = (RDFS.label, SKOS.prefLabel)
_COMMENT_PREDICATES = (_OBO_DEFINITION, SKOS.definition, RDFS.comment, DCTERMS.description)
_DATATYPE_RANGE_NAMESPACES = (str(XSD), str(RDF.PlainLiteral), str(RDFS.Literal))
# SOSA/SSN state domains/ranges with schema.org's weaker *Includes predicates
# instead of rdfs:domain/range; read both so their properties attach to classes.
_DOMAIN_PREDICATES = (RDFS.domain, URIRef("http://schema.org/domainIncludes"), URIRef("https://schema.org/domainIncludes"))
_RANGE_PREDICATES = (RDFS.range, URIRef("http://schema.org/rangeIncludes"), URIRef("https://schema.org/rangeIncludes"))


class UnknownOntologyError(KeyError):
    pass


_memory_cache: dict[str, tuple[tuple, dict]] = {}
_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)


def _ttl_path(graph_key: str) -> Path:
    registry = ontology_publish.get_registry()
    entry = registry.get(graph_key)
    if entry is None:
        raise UnknownOntologyError(
            f"Unknown ontology graph_key {graph_key!r}. Known keys: {sorted(registry)}"
        )
    return Path(entry["file_path"])


def list_ontology_keys() -> list[str]:
    return sorted(ontology_publish.get_registry())


# ---------------------------------------------------------------------------
# Snapshot building
# ---------------------------------------------------------------------------


def _best_literal(graph: rdflib.Graph, subject, predicates) -> str:
    """First English (or untagged) literal across predicates, in priority order."""
    for predicate in predicates:
        fallback = None
        for value in graph.objects(subject, predicate):
            if not isinstance(value, Literal):
                continue
            if value.language in (None, "en", "en-us", "en-gb"):
                return str(value)
            fallback = fallback or str(value)
        if fallback:
            return fallback
    return ""


def _name(graph: rdflib.Graph, uri: URIRef) -> str:
    """Compact CURIE (e.g. 'sosa:Observation', 'obo:DOID_2355') when the file
    declares a named prefix for it, otherwise the full IRI (a default-prefix
    CURIE like ':OEO_00000365' means nothing outside its own file)."""
    try:
        curie = graph.namespace_manager.curie(uri, generate=False)
    except Exception:
        return str(uri)
    return str(uri) if curie.startswith(":") else curie


def _named_members(graph: rdflib.Graph, node) -> list[URIRef]:
    """A named class, or the named members of an owl:unionOf blank node
    (how SOSA/DCAT express 'domain is A or B')."""
    if isinstance(node, URIRef):
        return [node]
    if isinstance(node, BNode):
        union = graph.value(node, OWL.unionOf)
        if union is not None:
            return [member for member in Collection(graph, union) if isinstance(member, URIRef)]
    return []


def _is_deprecated(graph: rdflib.Graph, uri: URIRef) -> bool:
    return any(str(value).lower() == "true" for value in graph.objects(uri, OWL.deprecated))


def _build_snapshot(graph_key: str, ttl_path: Path) -> dict[str, object]:
    graph = rdflib.Graph()
    graph.parse(ttl_path, format="turtle")

    class_uris = {
        uri
        for class_type in (OWL.Class, RDFS.Class)
        for uri in graph.subjects(RDF.type, class_type)
        if isinstance(uri, URIRef) and not _is_deprecated(graph, uri)
    }
    deprecated_class_count = sum(
        1 for uri in set(graph.subjects(RDF.type, OWL.Class)) if isinstance(uri, URIRef) and _is_deprecated(graph, uri)
    )

    classes = []
    for uri in class_uris:
        parents = [
            _name(graph, parent)
            for parent in graph.objects(uri, RDFS.subClassOf)
            if isinstance(parent, URIRef) and parent in class_uris
        ]
        name = _name(graph, uri)
        classes.append(
            {
                "name": name,
                "iri": str(uri),
                "label": _best_literal(graph, uri, _LABEL_PREDICATES) or name,
                "comment": _best_literal(graph, uri, _COMMENT_PREDICATES),
                "parents": sorted(set(parents)),
                # Kept for response-shape compatibility with the old
                # Ayurveda-only API; there's no per-ontology field map now.
                "group": graph_key,
                "section_title": None,
                "covered_fields": [],
                "descriptions": [],
                "design_notes": [],
            }
        )

    property_types: dict[URIRef, str] = {}
    for uri in graph.subjects(RDF.type, OWL.ObjectProperty):
        if isinstance(uri, URIRef):
            property_types[uri] = "object"
    for uri in graph.subjects(RDF.type, OWL.DatatypeProperty):
        if isinstance(uri, URIRef):
            property_types[uri] = "datatype"
    # Plain rdf:Property (DCAT, SKOS) -- classify by range.
    for uri in graph.subjects(RDF.type, RDF.Property):
        if isinstance(uri, URIRef) and uri not in property_types:
            ranges = [str(r) for r in graph.objects(uri, RDFS.range)]
            is_literal = bool(ranges) and all(r.startswith(_DATATYPE_RANGE_NAMESPACES) for r in ranges)
            property_types[uri] = "datatype" if is_literal else "object"

    datatype_properties = []
    object_properties = []
    for uri, property_type in property_types.items():
        if _is_deprecated(graph, uri):
            continue
        name = _name(graph, uri)
        record = {
            "name": name,
            "iri": str(uri),
            "label": _best_literal(graph, uri, _LABEL_PREDICATES) or name,
            "comment": _best_literal(graph, uri, _COMMENT_PREDICATES),
            "property_type": property_type,
            "domains": sorted({_name(graph, m) for p in _DOMAIN_PREDICATES for d in graph.objects(uri, p) for m in _named_members(graph, d)}),
            "ranges": sorted({_name(graph, m) for p in _RANGE_PREDICATES for r in graph.objects(uri, p) for m in _named_members(graph, r)}),
            "design_notes": [],
        }
        (datatype_properties if property_type == "datatype" else object_properties).append(record)

    classes.sort(key=lambda item: item["name"])
    datatype_properties.sort(key=lambda item: item["name"])
    object_properties.sort(key=lambda item: item["name"])

    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for class_info in classes:
        for parent_name in class_info["parents"]:
            children_by_parent[parent_name].append(class_info["name"])

    ontology_uris = sorted(uri for uri in graph.subjects(RDF.type, OWL.Ontology) if isinstance(uri, URIRef))
    # A merged file (SWEET: 226 module headers) has no single ontology header
    # worth describing it by -- fall back to the registry label then.
    ontology_uri = ontology_uris[0] if len(ontology_uris) == 1 else None
    registry_label = ontology_publish.get_registry().get(graph_key, {}).get("label", graph_key)

    schema_types = {OWL.Class, RDFS.Class, OWL.Ontology, OWL.ObjectProperty, OWL.DatatypeProperty,
                    OWL.AnnotationProperty, RDF.Property, OWL.Restriction, SKOS.ConceptScheme}
    individuals = {
        s for s, o in graph.subject_objects(RDF.type)
        if isinstance(s, URIRef) and o not in schema_types and s not in class_uris and s not in property_types
    }
    title = (
        _best_literal(graph, ontology_uri, (DCTERMS.title, RDFS.label, URIRef("http://purl.org/dc/elements/1.1/title")))
        if ontology_uri is not None else ""
    )
    description = (
        _best_literal(graph, ontology_uri, (DCTERMS.description, RDFS.comment, URIRef("http://purl.org/dc/elements/1.1/description")))
        if ontology_uri is not None else ""
    )
    created = (
        _best_literal(graph, ontology_uri, (DCTERMS.created, DCTERMS.modified, DCTERMS.issued, OWL.versionInfo))
        if ontology_uri is not None else ""
    )

    summary = {
        "graph_key": graph_key,
        "title": title or registry_label,
        "description": description,
        "created": created,
        "ontology_iri": str(ontology_uri) if ontology_uri is not None else None,
        "module_count": len(ontology_uris),
        "triple_count": len(graph),
        "class_count": len(classes),
        "object_property_count": len(object_properties),
        "datatype_property_count": len(datatype_properties),
        # Vocabularies such as CF standard names and QUDT are mostly SKOS
        # concepts / instances rather than classes -- counted so an empty
        # class list isn't mistaken for an empty ontology.
        "concept_count": len({s for s in graph.subjects(RDF.type, SKOS.Concept) if isinstance(s, URIRef)}),
        # Everything else with an rdf:type -- e.g. QUDT's units are instances of
        # qudt:Unit, not classes or SKOS concepts.
        "individual_count": len(individuals),
        "deprecated_class_count": deprecated_class_count,
        "groups": {graph_key: len(classes)} if classes else {},
    }

    return {
        "summary": summary,
        "classes": classes,
        "children_by_parent": {key: sorted(value) for key, value in children_by_parent.items()},
        "datatype_properties": datatype_properties,
        "object_properties": object_properties,
        "field_map": {"sections": [], "class_index": {}},
    }


def _with_indexes(snapshot: dict[str, object]) -> dict[str, object]:
    class_by_name = {item["name"]: item for item in snapshot["classes"]}
    property_by_name = {item["name"]: item for item in snapshot["datatype_properties"] + snapshot["object_properties"]}
    iri_to_name = {item["iri"]: item["name"] for item in snapshot["classes"]}
    return {**snapshot, "class_by_name": class_by_name, "property_by_name": property_by_name, "class_iri_to_name": iri_to_name}


def load_ontology_snapshot(graph_key: str) -> dict[str, object]:
    ttl_path = _ttl_path(graph_key)
    stat = ttl_path.stat()
    fingerprint = (str(ttl_path), stat.st_size, stat.st_mtime_ns, _SNAPSHOT_FORMAT_VERSION)

    cached = _memory_cache.get(graph_key)
    if cached and cached[0] == fingerprint:
        return cached[1]

    with _locks[graph_key]:
        cached = _memory_cache.get(graph_key)
        if cached and cached[0] == fingerprint:
            return cached[1]

        cache_file = _CACHE_DIR / f"{hashlib.sha1(repr(fingerprint).encode()).hexdigest()}.json"
        snapshot = None
        if cache_file.exists():
            try:
                snapshot = json.loads(cache_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                snapshot = None
        if snapshot is None:
            snapshot = _build_snapshot(graph_key, ttl_path)
            try:
                _CACHE_DIR.mkdir(parents=True, exist_ok=True)
                tmp = cache_file.with_suffix(".tmp")
                tmp.write_text(json.dumps(snapshot), encoding="utf-8")
                tmp.replace(cache_file)
            except OSError:
                pass  # disk cache is an optimisation only

        snapshot = _with_indexes(snapshot)
        _memory_cache[graph_key] = (fingerprint, snapshot)
        return snapshot


# ---------------------------------------------------------------------------
# Lookups used by the endpoints
# ---------------------------------------------------------------------------


def _resolve_class_name(snapshot: dict[str, object], value: str) -> str | None:
    """Accepts a CURIE ('sosa:Sensor'), a full IRI, or a bare local name when
    it's unambiguous within the ontology."""
    value = value.strip()
    if value in snapshot["class_by_name"]:
        return value
    if value in snapshot["class_iri_to_name"]:
        return snapshot["class_iri_to_name"][value]
    local_matches = [
        name for name in snapshot["class_by_name"]
        if name.rsplit(":", 1)[-1] == value or name.rsplit("/", 1)[-1] == value or name.rsplit("#", 1)[-1] == value
    ]
    return local_matches[0] if len(local_matches) == 1 else None


def resolve_class_name(graph_key: str, value: str) -> str | None:
    return _resolve_class_name(load_ontology_snapshot(graph_key), value)


def _class_properties(snapshot: dict[str, object], name: str) -> tuple[list, list, list]:
    datatype_properties = [p for p in snapshot["datatype_properties"] if name in p["domains"]]
    outgoing = [p for p in snapshot["object_properties"] if name in p["domains"]]
    incoming = [p for p in snapshot["object_properties"] if name in p["ranges"]]
    return datatype_properties, outgoing, incoming


def get_class_detail(graph_key: str, class_name: str) -> dict[str, object] | None:
    snapshot = load_ontology_snapshot(graph_key)
    name = _resolve_class_name(snapshot, class_name)
    if name is None:
        return None
    datatype_properties, outgoing, incoming = _class_properties(snapshot, name)
    return {
        **snapshot["class_by_name"][name],
        "children": snapshot["children_by_parent"].get(name, []),
        "datatype_properties": datatype_properties,
        "outgoing_object_properties": outgoing,
        "incoming_object_properties": incoming,
    }


def _class_v3_view(class_info: dict, snapshot: dict[str, object]) -> dict[str, object]:
    """One class, its own datatype + object properties (domain match), and its
    direct children -- enough to render one tree node without a follow-up call."""
    name = class_info["name"]
    datatype_properties, outgoing, _ = _class_properties(snapshot, name)
    children = snapshot["children_by_parent"].get(name, [])
    return {
        "name": name,
        "iri": class_info["iri"],
        "label": class_info["label"],
        "comment": class_info["comment"],
        "group": class_info["group"],
        "parents": class_info["parents"],
        "children": children,
        "has_children": bool(children),
        "datatype_properties": datatype_properties,
        "object_properties": outgoing,
    }


def list_ontology_classes_v3(graph_key: str, parent: str | None) -> dict[str, object] | None:
    """Children of `parent` (each with its own properties), or the top-level
    classes -- those with no parent inside this ontology -- when `parent` is
    omitted/blank. Returns None if a non-blank `parent` isn't a known class."""
    snapshot = load_ontology_snapshot(graph_key)
    parent_name: str | None = None

    if parent and parent.strip():
        parent_name = _resolve_class_name(snapshot, parent)
        if parent_name is None:
            return None
        child_names = snapshot["children_by_parent"].get(parent_name, [])
    else:
        child_names = [item["name"] for item in snapshot["classes"] if not item["parents"]]

    items = [_class_v3_view(snapshot["class_by_name"][name], snapshot) for name in sorted(child_names)]
    return {"graph_key": graph_key, "parent": parent_name, "count": len(items), "items": items}


def search_ontology(graph_key: str, query: str) -> dict[str, list[dict[str, object]]]:
    search_text = query.strip().lower()
    snapshot = load_ontology_snapshot(graph_key)

    def matches(record: dict[str, object]) -> bool:
        haystack = " ".join(str(record.get(key, "")) for key in ("name", "label", "comment")).lower()
        return search_text in haystack

    return {
        "classes": [item for item in snapshot["classes"] if matches(item)],
        "properties": [
            item for item in snapshot["datatype_properties"] + snapshot["object_properties"] if matches(item)
        ],
    }


def build_graph_view(graph_key: str) -> dict[str, object]:
    snapshot = load_ontology_snapshot(graph_key)

    nodes = [
        {"id": c["name"], "iri": c["iri"], "label": c["label"], "group": c["group"], "type": "class"}
        for c in snapshot["classes"]
    ]

    edges = []
    for class_info in snapshot["classes"]:
        for parent_name in class_info["parents"]:
            edges.append(
                {"source": class_info["name"], "target": parent_name, "label": "subClassOf", "type": "inheritance"}
            )

    unresolved_object_properties = []
    for property_info in snapshot["object_properties"]:
        if property_info["domains"] and property_info["ranges"]:
            for domain_name in property_info["domains"]:
                for range_name in property_info["ranges"]:
                    edges.append(
                        {
                            "source": domain_name,
                            "target": range_name,
                            "label": property_info["label"],
                            "property_name": property_info["name"],
                            "type": "object_property",
                        }
                    )
        else:
            unresolved_object_properties.append(property_info)

    return {
        "graph_key": graph_key,
        "nodes": nodes,
        "edges": edges,
        "unresolved_object_properties": unresolved_object_properties,
    }
