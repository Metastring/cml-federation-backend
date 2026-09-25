from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import rdflib

from app.endpoints.ontology import FUSEKI_SPARQL_ENDPOINT

# Interim mechanism (architecture-doc Phase 0/1 prerequisite) for updating the
# ontology TTLs while the schema is still actively changing across the team.
# Each registry entry ties together the on-disk file that both
# cphr_ontology_service.py (the pttiwari11 file-based read API) and the SPARQL
# grounding path consume, with the Fuseki named graph that mirrors it.
#
# The registry itself is a small JSON index colocated with the TTLs (not a
# hardcoded dict) so graph_keys can be added/removed via the API at runtime,
# without a code change or app restart, and so it stays correct across
# multiple worker processes (each request re-reads it, same as the TTL files).
#
# The old cml-biodiversity/economic/metadata ontologies (files + their Fuseki
# named graphs) were removed 2026-08-10 to consolidate on a single ayurveda
# ontology going forward. See scratchpad backups from that session if the old
# TTL content or graph contents are ever needed again.

_ONTOLOGY_TTL_DIR = Path(__file__).resolve().parent.parent / "ontology-ttl-files"
_REGISTRY_INDEX_PATH = _ONTOLOGY_TTL_DIR / "_registry.json"

_GRAPH_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

# Empty on purpose: the Ayurveda ontology was replaced on 2026-09-25 by the 13
# standard ontologies tracked in _registry.json, so a missing registry file
# must not resurrect an entry pointing at a deleted TTL.
_BOOTSTRAP_REGISTRY: dict[str, dict[str, str]] = {}


class OntologyKeyError(KeyError):
    pass


class TtlValidationError(ValueError):
    pass


class InvalidGraphKeyError(ValueError):
    pass


def _validate_graph_key(graph_key: str) -> None:
    if not _GRAPH_KEY_PATTERN.match(graph_key):
        raise InvalidGraphKeyError(
            f"Invalid graph_key {graph_key!r} — only letters, digits, '-' and '_' are allowed "
            "(it's used to build a file path and a Fuseki graph URI)."
        )


def _load_registry() -> dict[str, dict[str, str]]:
    if not _REGISTRY_INDEX_PATH.exists():
        _save_registry(_BOOTSTRAP_REGISTRY)
        return dict(_BOOTSTRAP_REGISTRY)
    return json.loads(_REGISTRY_INDEX_PATH.read_text(encoding="utf-8"))


def _save_registry(registry: dict[str, dict[str, str]]) -> None:
    _ONTOLOGY_TTL_DIR.mkdir(parents=True, exist_ok=True)
    _REGISTRY_INDEX_PATH.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")


def get_registry() -> dict[str, dict[str, str | Path]]:
    """Public, always-fresh view of the registry: {graph_key: {label, file_path (resolved Path), fuseki_graph}}."""
    raw = _load_registry()
    return {
        graph_key: {**entry, "file_path": _ONTOLOGY_TTL_DIR / entry["file_path"]}
        for graph_key, entry in raw.items()
    }


def _entry(graph_key: str, registry: dict[str, dict[str, str]] | None = None) -> dict[str, str]:
    registry = registry if registry is not None else _load_registry()
    entry = registry.get(graph_key)
    if entry is None:
        raise OntologyKeyError(
            f"Unknown ontology graph_key {graph_key!r}. Known keys: {sorted(registry)}"
        )
    return entry


def _resolved_file_path(entry: dict[str, str]) -> Path:
    return _ONTOLOGY_TTL_DIR / entry["file_path"]


def register_graph_key(graph_key: str, label: str | None = None) -> dict[str, str]:
    """Create a new registry entry if graph_key doesn't exist yet; no-op (returns
    the existing entry) if it already does — upload is idempotent either way."""
    _validate_graph_key(graph_key)
    registry = _load_registry()
    if graph_key in registry:
        return registry[graph_key]

    entry = {
        "label": label or graph_key,
        "file_path": f"{graph_key}.ttl",
        "fuseki_graph": f"http://cml.org/ontology/{graph_key}",
    }
    registry[graph_key] = entry
    _save_registry(registry)
    return entry


def deregister_graph_key(graph_key: str) -> dict[str, str]:
    registry = _load_registry()
    entry = _entry(graph_key, registry)
    del registry[graph_key]
    _save_registry(registry)
    return entry


def validate_ttl(content: str) -> dict[str, int]:
    """Parse content as Turtle. Raises TtlValidationError with the underlying
    parser message on failure; returns basic counts on success so callers can
    sanity-check "did this upload actually add anything" without a second parse.
    """
    graph = rdflib.Graph()
    try:
        graph.parse(data=content, format="turtle")
    except Exception as exc:  # rdflib raises several distinct exception types
        raise TtlValidationError(str(exc)) from exc

    classes = set(graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
    object_properties = set(graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))
    datatype_properties = set(graph.subjects(rdflib.RDF.type, rdflib.OWL.DatatypeProperty))

    return {
        "triple_count": len(graph),
        "class_count": len(classes),
        "object_property_count": len(object_properties),
        "datatype_property_count": len(datatype_properties),
    }


def read_ttl(graph_key: str) -> str:
    entry = _entry(graph_key)
    return _resolved_file_path(entry).read_text(encoding="utf-8")


def save_ttl(graph_key: str, content: str) -> Path:
    """Overwrite the registered file for graph_key. Caller must validate first —
    this does not re-validate, so it can also be used for pre-validated content."""
    entry = _entry(graph_key)
    file_path = _resolved_file_path(entry)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")
    return file_path


def delete_ttl_file(graph_key: str) -> Path | None:
    """Delete the on-disk file for graph_key, if present. Returns the path that
    was removed, or None if there was no file to remove."""
    entry = _entry(graph_key)
    file_path = _resolved_file_path(entry)
    if file_path.exists():
        file_path.unlink()
        return file_path
    return None


def _fuseki_gsp_endpoint() -> str:
    """Derive the Graph Store Protocol write endpoint from the query endpoint.

    e.g. http://host:3030/cml-ontology/query -> http://host:3030/cml-ontology/data
    """
    base = FUSEKI_SPARQL_ENDPOINT.rsplit("/", 1)[0]
    return f"{base}/data"


async def push_to_fuseki(graph_key: str, content: str) -> dict[str, str]:
    """Replace the named graph in Fuseki with `content` via the SPARQL 1.1
    Graph Store HTTP Protocol (PUT = full replace, not append — avoids
    accumulating stale triples across repeated uploads)."""
    entry = _entry(graph_key)
    graph_uri = entry["fuseki_graph"]
    endpoint = _fuseki_gsp_endpoint()

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.put(
            endpoint,
            params={"graph": graph_uri},
            content=content.encode("utf-8"),
            headers={"Content-Type": "text/turtle"},
        )
        response.raise_for_status()

    return {"fuseki_endpoint": endpoint, "graph": graph_uri, "status": str(response.status_code)}


async def delete_graph_from_fuseki(graph_key: str) -> dict[str, str]:
    entry = _entry(graph_key)
    graph_uri = entry["fuseki_graph"]
    endpoint = _fuseki_gsp_endpoint()

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.delete(endpoint, params={"graph": graph_uri})
        # Fuseki returns 404 if the graph doesn't exist — treat that as
        # already-deleted rather than an error, everything else must succeed.
        if response.status_code != 404:
            response.raise_for_status()

    return {"fuseki_endpoint": endpoint, "graph": graph_uri, "status": str(response.status_code)}
