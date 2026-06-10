from fastapi import APIRouter, HTTPException, Query

from app.cphr_ontology_service import (
    build_graph_view,
    get_class_detail,
    load_ontology_snapshot,
    search_ontology,
)


router = APIRouter(prefix="/ontology", tags=["cphr-ontology-apis"])


@router.get("/summary")
def get_ontology_summary():
    snapshot = load_ontology_snapshot()
    return snapshot["summary"]


@router.get("/classes")
def list_ontology_classes(group: str | None = Query(default=None)):
    snapshot = load_ontology_snapshot()
    classes = snapshot["classes"]
    if group:
        classes = [item for item in classes if item["group"] == group]
    return {
        "count": len(classes),
        "items": classes,
    }


@router.get("/classes/{class_name}")
def get_ontology_class(class_name: str):
    detail = get_class_detail(class_name)
    if not detail:
        raise HTTPException(status_code=404, detail="Ontology class not found")
    return detail


@router.get("/properties")
def list_ontology_properties(
    property_type: str | None = Query(default=None, pattern="^(object|datatype)$"),
    class_name: str | None = Query(default=None),
):
    snapshot = load_ontology_snapshot()
    properties = snapshot["datatype_properties"] + snapshot["object_properties"]

    if property_type:
        properties = [item for item in properties if item["property_type"] == property_type]

    if class_name:
        normalized_class_name = class_name.removeprefix("cphr:")
        properties = [
            item
            for item in properties
            if normalized_class_name in item["domains"] or normalized_class_name in item["ranges"]
        ]

    properties = sorted(properties, key=lambda item: (item["property_type"], item["name"]))

    return {
        "count": len(properties),
        "items": properties,
    }


@router.get("/graph")
def get_ontology_graph():
    return build_graph_view()


@router.get("/search")
def search_ontology_terms(q: str = Query(..., min_length=1)):
    results = search_ontology(q)
    return {
        "query": q,
        "class_count": len(results["classes"]),
        "property_count": len(results["properties"]),
        **results,
    }


@router.get("/field-map")
def get_ontology_field_map():
    snapshot = load_ontology_snapshot()
    return snapshot["field_map"]
