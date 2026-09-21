from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from app import dataset_registration_service as svc
from app.endpoints.dataset_details import DatasetRegistryInput

# The registration wizard's step 1 (source & details) and step 3
# (review & publish) endpoints. Step 2 (ontology mapping) stays in
# dataset_ontology_mapping.py -- this router only adds a /suggest route
# there, it doesn't duplicate the mapping CRUD.
router = APIRouter(prefix="/dataset-registration", tags=["Registration APIs"])


class DraftUpdateInput(BaseModel):
    title: str | None = None
    description: str | None = None
    citation: str | None = None
    doi: str | None = None
    language: str | None = None
    data_language: str | None = None
    license: str | None = None
    keywords: str | None = None
    dataset_type: str | None = None
    category_id: str | None = None
    node_name: str | None = None
    node_maintained_by: str | None = None


class URLSourceInput(BaseModel):
    source_url: str
    method: str = "GET"
    auth_header: str | None = None
    response_format: str = "JSON"


class SelectTableInput(BaseModel):
    table_name: str


class FileReferenceInput(BaseModel):
    reference_uri: str
    file_format: str | None = None
    access_credentials_ref: str | None = None


class DatabaseReferenceInput(BaseModel):
    connection_string: str
    table_name: str
    engine: str = "PostgreSQL"


class MapServiceInput(BaseModel):
    map_service_url: str
    layer_type: str = "Vector (WFS)"
    layer_name: str


def _raise_for(exc: Exception):
    if isinstance(exc, (svc.DatasetNotFoundError, svc.SourceConfigNotFoundError)):
        raise HTTPException(status_code=404, detail=str(exc) or "Not found") from exc
    if isinstance(exc, (ValueError, svc.UnsupportedFileTypeError)):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise exc


@router.post("/draft")
def create_draft(payload: DatasetRegistryInput):
    """Step 1: create the draft dataset before a source is even picked."""
    try:
        return svc.create_draft(payload)
    except Exception as exc:
        _raise_for(exc)


@router.patch("/{dataset_id}")
def update_draft(dataset_id: int, payload: DraftUpdateInput):
    """Edit draft details -- powers "Save as draft" from any step."""
    try:
        return svc.update_draft(dataset_id, payload.model_dump(exclude_none=True))
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/source/file")
async def upload_file_source(dataset_id: int, file: UploadFile = File(...)):
    """Upload a CSV/XLSX file -> detected fields + sample values."""
    try:
        return await svc.save_file_source(dataset_id, file)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/source/url")
async def register_url_source(dataset_id: int, payload: URLSourceInput):
    """"Test connection" -- fetches a sample response and detects fields."""
    try:
        return await svc.test_and_save_url_source(
            dataset_id, payload.source_url, payload.method, payload.auth_header, payload.response_format
        )
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/source/database")
async def upload_database_dump(dataset_id: int, file: UploadFile = File(...)):
    """Upload a .sql dump -> statically parsed table list (never executed)."""
    try:
        return await svc.save_database_dump_source(dataset_id, file)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/source/database/select-table")
def select_database_table(dataset_id: int, payload: SelectTableInput):
    """Pick which table from the dump to register -> its detected columns."""
    try:
        return svc.select_database_table(dataset_id, payload.table_name)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/file")
def save_file_reference(dataset_id: int, payload: FileReferenceInput):
    """v2 step 2 (File path): record a URI on the contributor's own
    infrastructure -- never fetched in full, unlike /source/file above."""
    try:
        return svc.save_file_reference_source(
            dataset_id, payload.reference_uri, payload.file_format, payload.access_credentials_ref
        )
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/file/verify")
async def verify_file_reference(dataset_id: int):
    """"Verify reachability" for a file reference (http/https only -- see
    dataset_registration_service.verify_file_reference for other schemes)."""
    try:
        return await svc.verify_file_reference(dataset_id)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/url")
async def save_url_reference(dataset_id: int, payload: URLSourceInput):
    """v2 step 2 (URL / API) -- same "test + save" behaviour as
    /source/url above (v1's URL source was already reference-only), kept
    as its own path so the frontend can call a consistent
    /reference/{file,url,database,map-service} shape across all four
    v2 source-type tiles."""
    try:
        return await svc.test_and_save_url_source(
            dataset_id, payload.source_url, payload.method, payload.auth_header, payload.response_format
        )
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/database")
def save_database_reference(dataset_id: int, payload: DatabaseReferenceInput):
    """v2 step 2 (Database): record a read-only connection string + which
    table/view to register. Does not connect -- call .../verify next."""
    try:
        return svc.save_database_reference_source(
            dataset_id, payload.connection_string, payload.table_name, payload.engine
        )
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/database/verify")
def verify_database_reference(dataset_id: int):
    """"Verify reachability" for a database reference. PostgreSQL only
    this pass -- other engines report back as unsupported (no driver)."""
    try:
        return svc.verify_database_reference(dataset_id)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/map-service")
def save_map_service_reference(dataset_id: int, payload: MapServiceInput):
    """v2 step 2 (Map service): record an already-hosted WMS/WFS/WMTS
    layer -- a reference to the contributor's own GeoServer, not an
    upload to map-module-backend."""
    try:
        return svc.save_map_service_source(
            dataset_id, payload.map_service_url, payload.layer_type, payload.layer_name
        )
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/reference/map-service/verify")
async def verify_map_service_reference(dataset_id: int):
    """"Verify reachability" for a map service: fetches GetCapabilities
    and checks the named layer is present in it."""
    try:
        return await svc.verify_map_service(dataset_id)
    except Exception as exc:
        _raise_for(exc)


@router.get("/{dataset_id}/review-summary")
def review_summary(dataset_id: int):
    """Step 3 screen's data: source type, category, fields-mapped count."""
    try:
        return svc.get_review_summary(dataset_id)
    except Exception as exc:
        _raise_for(exc)


@router.post("/{dataset_id}/publish")
def publish_dataset(dataset_id: int):
    """Finalize registration -- status becomes "Pending review"."""
    try:
        return svc.publish(dataset_id)
    except Exception as exc:
        _raise_for(exc)
