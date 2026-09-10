from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from app import custom_ontology_service as svc

# Custom Ontology Builder: lets a user author their own ontology (details ->
# class tree -> properties -> mappings) instead of hand-editing a TTL file.
# Sibling to /ontology/v3 (the read-only tree API for the predefined
# ontology) -- this one is read/write, backed by Postgres while the ontology
# is being authored, and only becomes a TTL file + Fuseki graph at publish
# time (via app/ontology_publish.py, unchanged).
router = APIRouter(prefix="/ontology/builder", tags=["cphr-ontology-builder-apis"])


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class OntologyCreateRequest(BaseModel):
    graph_key: str
    title: str
    acronym: str
    visibility: str = "Public"
    status: str = "Staging"
    description: str | None = None
    categories: list[str] = []
    bibliographic_refs: list[str] = []
    contact: str | None = None


class OntologyUpdateRequest(BaseModel):
    title: str | None = None
    acronym: str | None = None
    visibility: str | None = None
    status: str | None = None
    description: str | None = None
    categories: list[str] | None = None
    bibliographic_refs: list[str] | None = None
    contact: str | None = None


class ClassCreateRequest(BaseModel):
    name: str
    label: str
    definition: str | None = None
    synonyms: list[str] = []
    parent_name: str | None = None


class ClassUpdateRequest(BaseModel):
    label: str | None = None
    definition: str | None = None
    synonyms: list[str] | None = None
    parent_name: str | None = None


class PropertyCreateRequest(BaseModel):
    name: str
    label: str
    property_type: str  # "datatype" | "object"
    range_value: str
    comment: str | None = None
    cardinality_note: str | None = None


class MappingCreateRequest(BaseModel):
    relation: str  # one of svc.RELATION_TYPES
    target_kind: str  # "internal" | "external_ontology" | "external_uri"
    to_class_name: str | None = None
    to_ontology_graph_key: str | None = None
    to_ref: str | None = None


# ---------------------------------------------------------------------------
# Error mapping -- service raises typed exceptions, this turns them into the
# right HTTP status instead of every service function importing fastapi.
# ---------------------------------------------------------------------------

_NOT_FOUND_ERRORS = (
    svc.OntologyNotFoundError,
    svc.ClassNotFoundError,
    svc.PropertyNotFoundError,
    svc.MappingNotFoundError,
    svc.VersionNotFoundError,
)


def _raise_for(exc: Exception):
    if isinstance(exc, _NOT_FOUND_ERRORS):
        raise HTTPException(status_code=404, detail=str(exc) or "Not found") from exc
    if isinstance(exc, svc.ConflictError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, svc.ValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise exc


# ---------------------------------------------------------------------------
# Ontology
# ---------------------------------------------------------------------------


@router.post("")
def create_ontology(payload: OntologyCreateRequest):
    try:
        return svc.create_ontology(**payload.model_dump())
    except Exception as exc:
        _raise_for(exc)


@router.get("")
def list_ontologies():
    return {"items": svc.list_ontologies()}


@router.get("/{graph_key}")
def get_ontology(graph_key: str):
    ontology = svc.get_ontology(graph_key)
    if not ontology:
        raise HTTPException(status_code=404, detail=f"Ontology {graph_key!r} not found")
    return ontology


@router.patch("/{graph_key}")
def update_ontology(graph_key: str, payload: OntologyUpdateRequest):
    updated = svc.update_ontology(graph_key, payload.model_dump(exclude_unset=True))
    if not updated:
        raise HTTPException(status_code=404, detail=f"Ontology {graph_key!r} not found")
    return updated


@router.delete("/{graph_key}")
def delete_ontology(graph_key: str):
    deleted = svc.delete_ontology(graph_key)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Ontology {graph_key!r} not found")
    return {"graph_key": graph_key, "deleted": True}


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------


@router.get("/{graph_key}/classes")
def list_classes(graph_key: str, parent: str | None = Query(default=None)):
    try:
        return svc.list_classes(graph_key, parent)
    except Exception as exc:
        _raise_for(exc)


@router.get("/{graph_key}/classes/{name}")
def get_class(graph_key: str, name: str):
    try:
        return svc.get_class(graph_key, name)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{graph_key}/classes")
def create_class(graph_key: str, payload: ClassCreateRequest):
    try:
        return svc.create_class(graph_key, **payload.model_dump())
    except Exception as exc:
        _raise_for(exc)


@router.patch("/{graph_key}/classes/{name}")
def update_class(graph_key: str, name: str, payload: ClassUpdateRequest):
    try:
        return svc.update_class(graph_key, name, payload.model_dump(exclude_unset=True))
    except Exception as exc:
        _raise_for(exc)


@router.delete("/{graph_key}/classes/{name}")
def delete_class(graph_key: str, name: str):
    try:
        svc.delete_class(graph_key, name)
    except Exception as exc:
        _raise_for(exc)
    return {"graph_key": graph_key, "name": name, "deleted": True}


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


@router.post("/{graph_key}/classes/{name}/properties")
def create_property(graph_key: str, name: str, payload: PropertyCreateRequest):
    try:
        return svc.create_property(graph_key, name, **payload.model_dump())
    except Exception as exc:
        _raise_for(exc)


@router.delete("/{graph_key}/properties/{name}")
def delete_property(graph_key: str, name: str):
    try:
        svc.delete_property(graph_key, name)
    except Exception as exc:
        _raise_for(exc)
    return {"graph_key": graph_key, "name": name, "deleted": True}


# ---------------------------------------------------------------------------
# Mappings (replace multi-inheritance -- see custom_class_mapping's comment
# in the migration)
# ---------------------------------------------------------------------------


@router.post("/{graph_key}/classes/{name}/mappings")
def create_mapping(graph_key: str, name: str, payload: MappingCreateRequest):
    try:
        return svc.create_mapping(graph_key, name, **payload.model_dump())
    except Exception as exc:
        _raise_for(exc)


@router.delete("/{graph_key}/mappings/{mapping_id}")
def delete_mapping(graph_key: str, mapping_id: int):
    try:
        svc.delete_mapping(graph_key, mapping_id)
    except Exception as exc:
        _raise_for(exc)
    return {"graph_key": graph_key, "mapping_id": mapping_id, "deleted": True}


# ---------------------------------------------------------------------------
# TTL preview / Validate / Publish / Versions
# ---------------------------------------------------------------------------


@router.get("/{graph_key}/ttl", response_class=PlainTextResponse)
def preview_ttl(graph_key: str):
    """Current draft serialized to Turtle, computed on the fly -- nothing is
    written or pushed anywhere. Useful to sanity-check before Validate/Publish."""
    try:
        return svc.serialize_to_ttl(graph_key)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{graph_key}/validate")
def validate_ontology(graph_key: str):
    try:
        return svc.validate_ontology(graph_key)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{graph_key}/publish")
async def publish_ontology(graph_key: str):
    try:
        return await svc.publish_ontology(graph_key)
    except Exception as exc:
        _raise_for(exc)


@router.get("/{graph_key}/versions")
def list_versions(graph_key: str):
    try:
        return {"items": svc.list_versions(graph_key)}
    except Exception as exc:
        _raise_for(exc)


@router.post("/{graph_key}/versions")
def create_version(graph_key: str):
    """Explicit '+ New version' action -- snapshots the current draft.
    Decoupled from Publish (see custom_ontology_version's migration comment)."""
    try:
        return svc.create_version(graph_key)
    except Exception as exc:
        _raise_for(exc)


@router.get("/{graph_key}/versions/{version_no}")
def get_version(graph_key: str, version_no: int):
    try:
        return svc.get_version(graph_key, version_no)
    except Exception as exc:
        _raise_for(exc)
