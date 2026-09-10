from fastapi import APIRouter, HTTPException, Query

from app.cphr_ontology_service import list_ontology_classes_v3

# New heading for the ontology-tree-shaped APIs. Grows here going forward;
# the old flat endpoints (app/endpoints/cphr_ontology.py, GET /ontology/*)
# stay in place until the frontend has migrated off them, then get deleted.
router = APIRouter(prefix="/ontology/v3", tags=["cphr-ontology-v3-apis"])


@router.get("/classes")
def get_ontology_classes_v3(
    parent: str | None = Query(
        default=None,
        description=(
            "Class to list children of, e.g. 'PlantSpecies' or 'TaxonConcept' "
            "('cphr:' prefix optional). Omit to get the top-level classes "
            "(PlantSpecies, Dravya, Disease, Dosha). A class with no children "
            "(a leaf) returns an empty items list."
        ),
    )
):
    """Single class-tree endpoint: no `parent` -> top-level classes; `parent`
    set -> that class's direct children. Every returned class carries its own
    datatype and object properties plus its children's names, so the frontend
    can render one tree level per call without a second round trip."""
    result = list_ontology_classes_v3(parent)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Ontology class not found: {parent!r}")
    return result
