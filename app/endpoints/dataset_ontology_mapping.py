from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app import dataset_ontology_mapping_service as svc
from app import dataset_registration_service as registration_svc

# Real "map this dataset's fields to an ontology" flow: pick an ontology
# (predefined or custom-built), then pick which of ITS fields each dataset
# column corresponds to. Sits alongside the existing dataset registration
# endpoints (app/endpoints/dataset_details.py) as the next step after
# POST /dataset-registry, without touching that file's frontend-facing
# /dataset-mapping-update contract.
router = APIRouter(prefix="/dataset-ontology-mapping", tags=["Registration APIs"])


class MappingEntry(BaseModel):
    field_name: str
    ontology_field: str


class SaveMappingsRequest(BaseModel):
    ontology_graph_key: str
    mappings: list[MappingEntry]


_NOT_FOUND_ERRORS = (
    svc.DatasetNotFoundError,
    svc.OntologyNotFoundError,
    svc.FieldNotFoundError,
    svc.MappingNotFoundError,
)


def _raise_for(exc: Exception):
    if isinstance(exc, _NOT_FOUND_ERRORS):
        raise HTTPException(status_code=404, detail=str(exc) or "Not found") from exc
    raise exc


@router.get("/ontologies")
def list_ontologies():
    """Step 1 of the mapping flow: every ontology a dataset can be mapped
    against (predefined + custom-built)."""
    return {"items": svc.list_registered_ontologies()}


@router.get("/ontologies/{graph_key}/fields")
def get_ontology_fields(graph_key: str):
    """Step 2: the chosen ontology's fields, for the field-mapping dropdown."""
    try:
        return svc.get_ontology_fields(graph_key)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/suggest")
def suggest_mappings(dataset_id: int, ontology_graph_key: str):
    """Auto-suggest field -> ontology_field pairs from the source detected in
    step 1, for the "auto" badges shown before the user adjusts anything."""
    try:
        return registration_svc.suggest_mappings(dataset_id, ontology_graph_key)
    except registration_svc.SourceConfigNotFoundError as exc:
        raise HTTPException(
            status_code=404, detail=str(exc) or "No source detected for this dataset yet"
        ) from exc
    except svc.OntologyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{dataset_id}/mappings")
def save_mappings(dataset_id: int, payload: SaveMappingsRequest):
    """Step 3: save {dataset field name -> ontology field} pairs, validated
    against the chosen ontology's real field list."""
    try:
        return svc.save_mappings(
            dataset_id, payload.ontology_graph_key, [m.model_dump() for m in payload.mappings]
        )
    except Exception as exc:
        _raise_for(exc)


@router.get("/{dataset_id}/mappings")
def list_mappings(dataset_id: int):
    try:
        return {"items": svc.list_mappings(dataset_id)}
    except Exception as exc:
        _raise_for(exc)


@router.delete("/{dataset_id}/mappings/{dataset_mapping_id}")
def delete_mapping(dataset_id: int, dataset_mapping_id: int):
    try:
        svc.delete_mapping(dataset_id, dataset_mapping_id)
    except Exception as exc:
        _raise_for(exc)
    return {"dataset_id": dataset_id, "dataset_mapping_id": dataset_mapping_id, "deleted": True}
