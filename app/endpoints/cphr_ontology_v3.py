from fastapi import APIRouter, HTTPException, Query

from app.cphr_ontology_service import UnknownOntologyError, list_ontology_classes_v3, list_ontology_keys

# New heading for the ontology-tree-shaped APIs. Grows here going forward;
# the old flat endpoints (app/endpoints/cphr_ontology.py, GET /ontology/*)
# stay in place until the frontend has migrated off them, then get deleted.
router = APIRouter(prefix="/ontology/v3", tags=["cphr-ontology-v3-apis"])


@router.get("/classes")
def get_ontology_classes_v3(
    graph_key: str = Query(..., description="Registered ontology key, e.g. 'sosa', 'envo', 'doid' -- see GET /ontology/ttl"),
    parent: str | None = Query(
        default=None,
        description=(
            "Class to list children of, as a CURIE ('sosa:Sensor'), full IRI, or "
            "unambiguous local name. Omit to get the top-level classes (those with "
            "no parent inside this ontology). A leaf class returns an empty items list."
        ),
    ),
):
    """Single class-tree endpoint: no `parent` -> top-level classes; `parent`
    set -> that class's direct children. Every returned class carries its own
    datatype and object properties plus its children's names, so the frontend
    can render one tree level per call without a second round trip."""
    try:
        result = list_ontology_classes_v3(graph_key, parent)
    except UnknownOntologyError:
        raise HTTPException(status_code=404, detail=f"Unknown ontology {graph_key!r}. Available: {list_ontology_keys()}")
    if result is None:
        raise HTTPException(status_code=404, detail=f"Ontology class not found in {graph_key!r}: {parent!r}")
    return result
