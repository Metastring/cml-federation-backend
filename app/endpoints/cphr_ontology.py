from fastapi import APIRouter, HTTPException, Query

from app.cphr_ontology_service import (
    UnknownOntologyError,
    build_graph_view,
    get_class_detail,
    list_ontology_keys,
    load_ontology_snapshot,
    resolve_class_name,
    search_ontology,
)


router = APIRouter(prefix="/ontology", tags=["cphr-ontology-apis"])

# Every route reads one registered ontology, picked by graph_key (see
# GET /ontology/ttl for the list). The first call for a large ontology parses
# its TTL (DOID takes ~20s); later calls are served from cache.
GRAPH_KEY = Query(..., description="Registered ontology key, e.g. 'sosa', 'envo', 'doid' -- see GET /ontology/ttl")


def _snapshot(graph_key: str) -> dict:
    try:
        return load_ontology_snapshot(graph_key)
    except UnknownOntologyError:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown ontology {graph_key!r}. Available: {list_ontology_keys()}",
        )


def _page(items: list, limit: int | None, offset: int) -> list:
    return items[offset: offset + limit] if limit is not None else items[offset:]


@router.get("/summary")
def get_ontology_summary(graph_key: str = GRAPH_KEY):
    return _snapshot(graph_key)["summary"]


@router.get("/classes")
def list_ontology_classes(
    graph_key: str = GRAPH_KEY,
    group: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, description="Page size; omit for all classes"),
    offset: int = Query(default=0, ge=0),
):
    classes = _snapshot(graph_key)["classes"]
    if group:
        classes = [item for item in classes if item["group"] == group]
    return {
        "graph_key": graph_key,
        "count": len(classes),
        "offset": offset,
        "items": _page(classes, limit, offset),
    }


@router.get("/classes/{class_name:path}")
def get_ontology_class(class_name: str, graph_key: str = GRAPH_KEY):
    """class_name may be a CURIE ('sosa:Sensor'), a full IRI, or an unambiguous local name."""
    _snapshot(graph_key)
    detail = get_class_detail(graph_key, class_name)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Class {class_name!r} not found in ontology {graph_key!r}")
    return detail


@router.get("/properties")
def list_ontology_properties(
    graph_key: str = GRAPH_KEY,
    property_type: str | None = Query(default=None, pattern="^(object|datatype)$"),
    class_name: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, description="Page size; omit for all properties"),
    offset: int = Query(default=0, ge=0),
):
    snapshot = _snapshot(graph_key)
    properties = snapshot["datatype_properties"] + snapshot["object_properties"]

    if property_type:
        properties = [item for item in properties if item["property_type"] == property_type]

    if class_name:
        resolved = resolve_class_name(graph_key, class_name) or class_name
        properties = [item for item in properties if resolved in item["domains"] or resolved in item["ranges"]]

    properties = sorted(properties, key=lambda item: (item["property_type"], item["name"]))

    return {
        "graph_key": graph_key,
        "count": len(properties),
        "offset": offset,
        "items": _page(properties, limit, offset),
    }


@router.get("/graph")
def get_ontology_graph(graph_key: str = GRAPH_KEY):
    _snapshot(graph_key)
    return build_graph_view(graph_key)


@router.get("/search")
def search_ontology_terms(q: str = Query(..., min_length=1), graph_key: str = GRAPH_KEY):
    _snapshot(graph_key)
    results = search_ontology(graph_key, q)
    return {
        "graph_key": graph_key,
        "query": q,
        "class_count": len(results["classes"]),
        "property_count": len(results["properties"]),
        **results,
    }


@router.get("/field-map")
def get_ontology_field_map(graph_key: str = GRAPH_KEY):
    """Field maps were an Ayurveda-only markdown file; the uploaded standard
    ontologies have none, so this returns an empty map for them."""
    return {"graph_key": graph_key, **_snapshot(graph_key)["field_map"]}
