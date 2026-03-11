from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from pydantic import BaseModel, ValidationError
from typing import Optional, List
from datetime import date
from app.db import get_connection
from psycopg2.extras import RealDictCursor
import csv
import io
import json
from openpyxl import load_workbook


class TechnicalMetadataIn(BaseModel):
    data_format: Optional[str] = None
    encoding: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    file_size: Optional[int] = None
    primary_key: Optional[str] = None
    storage_location: Optional[str] = None
    api_endpoint: Optional[str] = None
    rdf_graph_uri: Optional[str] = None
    ontology_reference: Optional[str] = None


class GovernanceMetadataIn(BaseModel):
    data_owner: Optional[str] = None
    data_steward: Optional[str] = None
    owner_email: Optional[str] = None
    compliance_status: Optional[str] = None
    access_level: Optional[str] = None
    data_classification: Optional[str] = None
    retention_policy: Optional[str] = None
    pii_present: Optional[bool] = None
    license: Optional[str] = None


class OperationalMetadataIn(BaseModel):
    publish_date: Optional[str] = None
    registration_date: Optional[str] = None
    last_refresh_date: Optional[str] = None
    refresh_frequency: Optional[str] = None
    refresh_method: Optional[str] = None
    environment: Optional[str] = None
    ingestion_pipeline: Optional[str] = None
    supported_by: Optional[str] = None
    last_job_run_time: Optional[str] = None


class CollaborationMetadataIn(BaseModel):
    review_status: Optional[str] = None
    rating: Optional[float] = None
    documentation_link: Optional[str] = None
    tags: Optional[str] = None


class QualityMetadataIn(BaseModel):
    completeness_score: Optional[float] = None
    accuracy_score: Optional[float] = None
    freshness_score: Optional[float] = None
    consistency_score: Optional[float] = None
    duplicate_count: Optional[int] = None
    null_percentage: Optional[float] = None
    validation_status: Optional[str] = None
    last_quality_check_date: Optional[str] = None


class UsageMetadataIn(BaseModel):
    query_count: Optional[int] = None
    download_count: Optional[int] = None
    api_call_count: Optional[int] = None
    last_accessed: Optional[str] = None
    active_users_count: Optional[int] = None
    popularity_score: Optional[float] = None


class DatasetMasterIn(BaseModel):
    title: str
    description: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    language: Optional[str] = None
    data_language: Optional[str] = None
    license: Optional[str] = None
    publication_date: Optional[date] = None
    metadata_modified_date: Optional[date] = None
    registration_date: Optional[date] = None
    is_active: Optional[bool] = True
    keywords: Optional[List[str]] = None
    dataset_version: Optional[str] = None
    schema_version: Optional[str] = None
    domain: Optional[str] = None
    dataset_type: Optional[str] = None
    source_system: Optional[str] = None
    status: Optional[str] = None


class MetadataItem(BaseModel):
    dataset_id: Optional[int] = None
    dataset: Optional[DatasetMasterIn] = None
    technical: Optional[TechnicalMetadataIn] = None
    governance: Optional[GovernanceMetadataIn] = None
    operational: Optional[OperationalMetadataIn] = None
    collaboration: Optional[CollaborationMetadataIn] = None
    quality: Optional[QualityMetadataIn] = None
    usage: Optional[UsageMetadataIn] = None


class MetadataImportRequest(BaseModel):
    items: List[MetadataItem]

router = APIRouter()



@router.get("/metadata")
def get_metadata(title: str, category_name: str):
    # Normalize title
    normalized_title = title
    
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                cat.category_name,
                ds.title AS dataset_title,
                ds.description,
                ds.citation,
                ds.doi,
                ds.language,
                ds.data_language,
                ds.license,
                ds.publication_date,
                ds.metadata_modified_date AS last_updated,
                ds.registration_date,
                ds.is_active,
                ds.keywords,
                ds.dataset_type,

                c.name AS contact_name,
                c.role AS contact_role,
                c.email AS contact_email,
                c.organization AS contact_organization,
                c.address AS contact_address,
                c.city AS contact_city,
                c.state AS contact_state,
                c.country AS contact_country,

                p.publisher_name,
                p.country AS publisher_country,
                p.record_count,

                s.temporal_start_date,
                s.temporal_end_date,
                s.geographic_scope,
                s.taxonomic_scope,
                s.taxonomic_authority,

                m.field_name,
                m.ontology_mapping,
                m.data_type,

                st.stat_name,
                st.stat_value,
                st.measurement_date
                
            FROM
                category_master cat
            JOIN
                dataset_master ds ON ds.category_id = cat.category_id
            LEFT JOIN
                dataset_contacts c ON c.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_publisher p ON p.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_scope s ON s.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_mapping m ON m.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_statistics st ON st.dataset_id = ds.dataset_id

            WHERE
                cat.category_name = %s AND ds.title = %s

            ORDER BY
                cat.category_name, ds.title, m.field_name;
        """, (category_name, normalized_title))

        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Metadata not found for the specified dataset and category")

        dataset_details = {
            "category_name": category_name,
            "dataset_title": normalized_title,  # return alias instead of original
            "description": rows[0]['description'],
            "citation": rows[0]['citation'],
            "doi": rows[0]['doi'],
            "language": rows[0]['language'],
            "data_language": rows[0]['data_language'],
            "license": rows[0]['license'],
            "publication_date": rows[0]['publication_date'],
            "last_updated": rows[0]['last_updated'],
            "registration_date": rows[0]['registration_date'],
            "is_active": rows[0]['is_active'],
            "keywords": rows[0]['keywords'],
            "dataset_type": rows[0]['dataset_type'],
            "contacts": [],
            "publishers": [],
            "scopes": [],
            "fields": [],
            "statistics": []
        }

        for row in rows:
            contact = {
                "name": row['contact_name'],
                "role": row['contact_role'],
                "email": row['contact_email'],
                "organization": row['contact_organization'],
                "address": row['contact_address'],
                "city": row['contact_city'],
                "state": row['contact_state'],
                "country": row['contact_country']
            }
            if contact and contact not in dataset_details["contacts"]:
                dataset_details["contacts"].append(contact)

            publisher = {
                "publisher_name": row['publisher_name'],
                "country": row['publisher_country'],
                "record_count": row['record_count']
            }
            if publisher and publisher not in dataset_details["publishers"]:
                dataset_details["publishers"].append(publisher)

            scope = {
                "temporal_start_date": row['temporal_start_date'],
                "temporal_end_date": row['temporal_end_date'],
                "geographic_scope": row['geographic_scope'],
                "taxonomic_scope": row['taxonomic_scope'],
                "taxonomic_authority": row['taxonomic_authority']
            }
            if scope not in dataset_details["scopes"]:
                dataset_details["scopes"].append(scope)

            field = {
                "field_name": row['field_name'],
                "ontology_mapping": row['ontology_mapping'],
                "data_type": row['data_type']
            }
            if field not in dataset_details["fields"]:
                dataset_details["fields"].append(field)

            stat = {
                "stat_name": row['stat_name'],
                "stat_value": row['stat_value'],
                "measurement_date": row['measurement_date']
            }
            if stat not in dataset_details["statistics"]:
                dataset_details["statistics"].append(stat)

        return dataset_details
    finally:
        conn.close()


def _upsert_single_metadata(cursor, table_name: str, dataset_id: str, data: dict):
    if not data:
        return

    # Remove None values so we don't overwrite with NULL unnecessarily
    clean = {k: v for k, v in data.items() if v is not None}
    if not clean:
        return

    columns = list(clean.keys())
    values = list(clean.values())

    # Check if row already exists for this dataset_id
    cursor.execute(f"SELECT 1 FROM {table_name} WHERE dataset_id = %s", (dataset_id,))
    exists = cursor.fetchone() is not None

    if exists:
        set_clause = ", ".join([f"{col} = %s" for col in columns])
        cursor.execute(
            f"UPDATE {table_name} SET {set_clause}, updated_at = NOW() WHERE dataset_id = %s",
            values + [dataset_id],
        )
    else:
        col_clause = ", ".join(["dataset_id"] + columns)
        placeholders = ", ".join(["%s"] * (len(columns) + 1))
        cursor.execute(
            f"INSERT INTO {table_name} ({col_clause}) VALUES ({placeholders})",
            [dataset_id] + values,
        )


def _import_metadata_from_payload(payload: MetadataImportRequest):
    """Core implementation for importing metadata from a structured payload.

    Shared by JSON body and JSON-file upload endpoints.
    """

    conn = get_connection()
    try:
        cursor = conn.cursor()

        for item in payload.items:
            dataset_id = item.dataset_id

            # If dataset_id is not provided, create a new dataset_master row
            if dataset_id is None:
                if item.dataset is None:
                    raise HTTPException(status_code=400, detail="Either dataset_id or dataset details must be provided")

                dm = item.dataset
                dm_data = dm.model_dump()

                # Convert keywords list to a semicolon-separated string if provided
                keywords_list = dm_data.pop("keywords", None)
                if keywords_list:
                    dm_data["keywords"] = "; ".join(keywords_list)

                # Ensure is_active has a default True if not explicitly set
                if dm_data.get("is_active") is None:
                    dm_data["is_active"] = True

                # Build dynamic INSERT for dataset_master using non-None fields
                dm_clean = {k: v for k, v in dm_data.items() if v is not None}
                dm_columns = list(dm_clean.keys())
                dm_values = list(dm_clean.values())
                dm_cols_clause = ", ".join(dm_columns)
                dm_placeholders = ", ".join(["%s"] * len(dm_columns))

                cursor.execute(
                    f"INSERT INTO dataset_master ({dm_cols_clause}) VALUES ({dm_placeholders}) RETURNING dataset_id",
                    dm_values,
                )
                dataset_id = cursor.fetchone()[0]

            if item.technical:
                _upsert_single_metadata(cursor, "technical_metadata", dataset_id, item.technical.dict())
            if item.governance:
                _upsert_single_metadata(cursor, "governance_metadata", dataset_id, item.governance.dict())
            if item.operational:
                _upsert_single_metadata(cursor, "operational_metadata", dataset_id, item.operational.dict())
            if item.collaboration:
                _upsert_single_metadata(cursor, "collaboration_metadata", dataset_id, item.collaboration.dict())
            if item.quality:
                _upsert_single_metadata(cursor, "quality_metadata", dataset_id, item.quality.dict())
            if item.usage:
                _upsert_single_metadata(cursor, "usage_metadata", dataset_id, item.usage.dict())

        conn.commit()
        return {"status": "success", "items_processed": len(payload.items)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/v2/metadata/import-json")
def import_metadata_json(payload: MetadataImportRequest):
    return _import_metadata_from_payload(payload)


@router.post("/v2/metadata/import-json-file")
async def import_metadata_json_file(file: UploadFile = File(...)):
    """Import metadata from an uploaded JSON file.

    The file must contain the same structure expected by
    MetadataImportRequest.
    """

    try:
        content = await file.read()
        try:
            raw_data = json.loads(content.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON file uploaded")

        try:
            payload = MetadataImportRequest.model_validate(raw_data)
        except ValidationError as ve:
            raise HTTPException(status_code=400, detail=ve.errors())

        return _import_metadata_from_payload(payload)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/v2/metadata/import-csv")
async def import_metadata_csv(
    category: str = Query(..., regex="^(technical|governance|operational|collaboration|quality|usage)$"),
    file: UploadFile = File(...),
):
    """Import metadata for a single category from a CSV or XLSX file."""

    table_map = {
        "technical": "technical_metadata",
        "governance": "governance_metadata",
        "operational": "operational_metadata",
        "collaboration": "collaboration_metadata",
        "quality": "quality_metadata",
        "usage": "usage_metadata",
    }

    try:
        content = await file.read()
        filename = (file.filename or "").lower()

        rows = []
        if filename.endswith(".xlsx") or filename.endswith(".xlsm"):
            try:
                wb = load_workbook(io.BytesIO(content), data_only=True)
                ws = wb.active
            except Exception:
                raise HTTPException(status_code=400, detail="Unable to read Excel file for metadata import")

            headers = []
            for idx, excel_row in enumerate(ws.iter_rows(values_only=True), start=1):
                if idx == 1:
                    headers = [str(h).strip() if h is not None else "" for h in excel_row]
                    continue

                if not any(cell is not None and str(cell).strip() != "" for cell in excel_row):
                    continue

                row_dict = {}
                for header, cell in zip(headers, excel_row):
                    if not header:
                        continue
                    value = "" if cell is None else str(cell)
                    row_dict[header] = value
                rows.append(row_dict)
        else:
            try:
                text_stream = io.StringIO(content.decode("utf-8"))
            except Exception:
                raise HTTPException(status_code=400, detail="Unable to decode CSV file as UTF-8")
            reader = csv.DictReader(text_stream)
            rows = list(reader)

        conn = get_connection()
        cursor = conn.cursor()

        dataset_master_fields = {
            "title",
            "description",
            "dataset_version",
            "schema_version",
            "domain",
            "dataset_type",
            "language",
            "source_system",
            "status",
        }

        count = 0
        for row in rows:
            dataset_id = row.get("dataset_id")

            if not dataset_id or str(dataset_id).strip() == "":
                dm_data = {
                    k: v
                    for k, v in row.items()
                    if k in dataset_master_fields and v not in (None, "")
                }

                if "title" not in dm_data:
                    continue

                dm_columns = list(dm_data.keys())
                dm_values = list(dm_data.values())
                dm_cols_clause = ", ".join(dm_columns)
                dm_placeholders = ", ".join(["%s"] * len(dm_columns))

                cursor.execute(
                    f"INSERT INTO dataset_master ({dm_cols_clause}) VALUES ({dm_placeholders}) RETURNING dataset_id",
                    dm_values,
                )
                dataset_id = cursor.fetchone()[0]

            data = {
                k: (v if v != "" else None)
                for k, v in row.items()
                if k not in dataset_master_fields and k != "dataset_id"
            }

            _upsert_single_metadata(cursor, table_map[category], dataset_id, data)
            count += 1

        conn.commit()
        return {"status": "success", "rows_processed": count}
    except HTTPException:
        if 'conn' in locals():
            conn.rollback()
        raise
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@router.get("/v2/metadata/search")
def search_metadata(query: str = Query(..., min_length=1)):
    """Search across dataset and standard metadata.

    The search is case-insensitive and matches substrings, so it works
    for half-words, full words, or full phrases.

    Returns a list of datasets with their category information.
    """

    search_pattern = f"%{query.lower()}%"

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            """
            SELECT DISTINCT
                ds.dataset_id,
                ds.title AS dataset_title,
                ds.description,
                cat.category_id,
                cat.category_name
            FROM
                dataset_master ds
            LEFT JOIN category_master cat ON ds.category_id = cat.category_id
            LEFT JOIN technical_metadata tm ON tm.dataset_id = ds.dataset_id
            LEFT JOIN governance_metadata gm ON gm.dataset_id = ds.dataset_id
            LEFT JOIN operational_metadata om ON om.dataset_id = ds.dataset_id
            LEFT JOIN collaboration_metadata cm ON cm.dataset_id = ds.dataset_id
            LEFT JOIN quality_metadata qm ON qm.dataset_id = ds.dataset_id
            LEFT JOIN usage_metadata um ON um.dataset_id = ds.dataset_id
            LEFT JOIN dataset_mapping dm ON dm.dataset_id = ds.dataset_id
            LEFT JOIN dataset_contacts c ON c.dataset_id = ds.dataset_id
            LEFT JOIN dataset_publisher p ON p.dataset_id = ds.dataset_id
            LEFT JOIN dataset_scope s ON s.dataset_id = ds.dataset_id
            LEFT JOIN dataset_tags dt ON dt.dataset_id = ds.dataset_id
            LEFT JOIN tags t ON t.tag_id = dt.tag_id
            WHERE
                ds.is_active = TRUE
                AND LOWER(
                    -- dataset_master (all non-id columns)
                    COALESCE(ds.title, '') || ' ' ||
                    COALESCE(ds.description, '') || ' ' ||
                    COALESCE(ds.citation, '') || ' ' ||
                    COALESCE(ds.doi, '') || ' ' ||
                    COALESCE(ds.language, '') || ' ' ||
                    COALESCE(ds.data_language, '') || ' ' ||
                    COALESCE(ds.license, '') || ' ' ||
                    COALESCE(ds.dataset_version, '') || ' ' ||
                    COALESCE(ds.schema_version, '') || ' ' ||
                    COALESCE(ds.domain, '') || ' ' ||
                    COALESCE(ds.dataset_type, '') || ' ' ||
                    COALESCE(ds.source_system, '') || ' ' ||
                    COALESCE(ds.status, '') || ' ' ||
                    COALESCE(ds.keywords::text, '') || ' ' ||
                    COALESCE(ds.publication_date::text, '') || ' ' ||
                    COALESCE(ds.metadata_modified_date::text, '') || ' ' ||
                    COALESCE(ds.registration_date::text, '') || ' ' ||
                    COALESCE(ds.is_active::text, '') || ' ' ||
                    COALESCE(ds.created_at::text, '') || ' ' ||
                    COALESCE(ds.updated_at::text, '') || ' ' ||

                    -- category
                    COALESCE(cat.category_name, '') || ' ' ||

                    -- technical_metadata (all non-id columns)
                    COALESCE(tm.data_format, '') || ' ' ||
                    COALESCE(tm.encoding, '') || ' ' ||
                    COALESCE(tm.row_count::text, '') || ' ' ||
                    COALESCE(tm.column_count::text, '') || ' ' ||
                    COALESCE(tm.file_size::text, '') || ' ' ||
                    COALESCE(tm.primary_key, '') || ' ' ||
                    COALESCE(tm.storage_location, '') || ' ' ||
                    COALESCE(tm.api_endpoint, '') || ' ' ||
                    COALESCE(tm.rdf_graph_uri, '') || ' ' ||
                    COALESCE(tm.ontology_reference, '') || ' ' ||
                    COALESCE(tm.created_at::text, '') || ' ' ||
                    COALESCE(tm.updated_at::text, '') || ' ' ||

                    -- governance_metadata (all non-id columns)
                    COALESCE(gm.data_owner, '') || ' ' ||
                    COALESCE(gm.data_steward, '') || ' ' ||
                    COALESCE(gm.owner_email, '') || ' ' ||
                    COALESCE(gm.compliance_status, '') || ' ' ||
                    COALESCE(gm.access_level, '') || ' ' ||
                    COALESCE(gm.data_classification, '') || ' ' ||
                    COALESCE(gm.retention_policy, '') || ' ' ||
                    COALESCE(gm.pii_present::text, '') || ' ' ||
                    COALESCE(gm.license, '') || ' ' ||
                    COALESCE(gm.created_at::text, '') || ' ' ||
                    COALESCE(gm.updated_at::text, '') || ' ' ||

                    -- operational_metadata (all non-id columns)
                    COALESCE(om.publish_date::text, '') || ' ' ||
                    COALESCE(om.registration_date::text, '') || ' ' ||
                    COALESCE(om.last_refresh_date::text, '') || ' ' ||
                    COALESCE(om.refresh_frequency, '') || ' ' ||
                    COALESCE(om.refresh_method, '') || ' ' ||
                    COALESCE(om.environment, '') || ' ' ||
                    COALESCE(om.ingestion_pipeline, '') || ' ' ||
                    COALESCE(om.supported_by, '') || ' ' ||
                    COALESCE(om.last_job_run_time::text, '') || ' ' ||
                    COALESCE(om.created_at::text, '') || ' ' ||
                    COALESCE(om.updated_at::text, '') || ' ' ||

                    -- collaboration_metadata (all non-id columns)
                    COALESCE(cm.review_status, '') || ' ' ||
                    COALESCE(cm.rating::text, '') || ' ' ||
                    COALESCE(cm.documentation_link, '') || ' ' ||
                    COALESCE(cm.tags, '') || ' ' ||
                    COALESCE(cm.created_at::text, '') || ' ' ||
                    COALESCE(cm.updated_at::text, '') || ' ' ||

                    -- quality_metadata (all non-id columns)
                    COALESCE(qm.completeness_score::text, '') || ' ' ||
                    COALESCE(qm.accuracy_score::text, '') || ' ' ||
                    COALESCE(qm.freshness_score::text, '') || ' ' ||
                    COALESCE(qm.consistency_score::text, '') || ' ' ||
                    COALESCE(qm.duplicate_count::text, '') || ' ' ||
                    COALESCE(qm.null_percentage::text, '') || ' ' ||
                    COALESCE(qm.validation_status, '') || ' ' ||
                    COALESCE(qm.last_quality_check_date::text, '') || ' ' ||
                    COALESCE(qm.created_at::text, '') || ' ' ||
                    COALESCE(qm.updated_at::text, '') || ' ' ||

                    -- usage_metadata (all non-id columns)
                    COALESCE(um.query_count::text, '') || ' ' ||
                    COALESCE(um.download_count::text, '') || ' ' ||
                    COALESCE(um.api_call_count::text, '') || ' ' ||
                    COALESCE(um.last_accessed::text, '') || ' ' ||
                    COALESCE(um.active_users_count::text, '') || ' ' ||
                    COALESCE(um.popularity_score::text, '') || ' ' ||
                    COALESCE(um.created_at::text, '') || ' ' ||
                    COALESCE(um.updated_at::text, '') || ' ' ||

                    -- dataset_contacts (all non-id columns we use)
                    COALESCE(c.name, '') || ' ' ||
                    COALESCE(c.role, '') || ' ' ||
                    COALESCE(c.email, '') || ' ' ||
                    COALESCE(c.organization, '') || ' ' ||
                    COALESCE(c.address, '') || ' ' ||
                    COALESCE(c.city, '') || ' ' ||
                    COALESCE(c.state, '') || ' ' ||
                    COALESCE(c.country, '') || ' ' ||

                    -- dataset_publisher (all non-id columns)
                    COALESCE(p.publisher_name, '') || ' ' ||
                    COALESCE(p.country, '') || ' ' ||
                    COALESCE(p.record_count::text, '') || ' ' ||

                    -- dataset_scope (all non-id columns)
                    COALESCE(s.temporal_start_date::text, '') || ' ' ||
                    COALESCE(s.temporal_end_date::text, '') || ' ' ||
                    COALESCE(s.geographic_scope, '') || ' ' ||
                    COALESCE(s.taxonomic_scope, '') || ' ' ||
                    COALESCE(s.taxonomic_authority, '') || ' ' ||

                    -- dataset_mapping (all non-id columns we use)
                    COALESCE(dm.field_name, '') || ' ' ||
                    COALESCE(dm.ontology_mapping, '') || ' ' ||
                    COALESCE(dm.ontology_mapping_to_display, '') || ' ' ||
                    COALESCE(dm.data_type, '') || ' ' ||

                    -- tags (all non-id columns)
                    COALESCE(t.tag_name, '') || ' ' ||
                    COALESCE(t.created_at::text, '')
                ) LIKE %s
            ORDER BY
                cat.category_name,
                ds.title;
            """,
            (search_pattern,),
        )

        rows = cursor.fetchall() or []

        return {
            "query": query,
            "results": [
                {
                    "dataset_id": row["dataset_id"],
                    "dataset_title": row["dataset_title"],
                    "description": row["description"],
                    "category_id": row["category_id"],
                    "category_name": row["category_name"],
                }
                for row in rows
            ],
        }
    finally:
        conn.close()


@router.get("/v2/metadata/standard")
def get_standard_metadata(dataset_id: str):
    """Return full standard metadata for a dataset.

    This aggregates:
    - Core dataset_master fields
    - technical_metadata
    - governance_metadata
    - operational_metadata
    - collaboration_metadata
    - quality_metadata
    - usage_metadata
    - dataset_users (roles) and tags
    """

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Core dataset
        cursor.execute("SELECT * FROM dataset_master WHERE dataset_id = %s", (dataset_id,))
        dataset_row = cursor.fetchone()
        if not dataset_row:
            raise HTTPException(status_code=404, detail="Dataset not found")

        def fetch_single(table: str):
            cursor.execute(f"SELECT * FROM {table} WHERE dataset_id = %s", (dataset_id,))
            row = cursor.fetchone()
            return row if row else None

        technical = fetch_single("technical_metadata")
        governance = fetch_single("governance_metadata")
        operational = fetch_single("operational_metadata")
        collaboration = fetch_single("collaboration_metadata")
        quality = fetch_single("quality_metadata")
        usage = fetch_single("usage_metadata")

        # Roles
        cursor.execute(
            "SELECT dataset_user_id, user_id, role_type, created_at "
            "FROM dataset_users WHERE dataset_id = %s",
            (dataset_id,),
        )
        users = cursor.fetchall() or []

        # Tags
        cursor.execute(
            "SELECT t.tag_id, t.tag_name "
            "FROM dataset_tags dt "
            "JOIN tags t ON t.tag_id = dt.tag_id "
            "WHERE dt.dataset_id = %s",
            (dataset_id,),
        )
        tags = cursor.fetchall() or []

        return {
            "dataset_id": dataset_id,
            "dataset": dataset_row,
            "technical": technical,
            "governance": governance,
            "operational": operational,
            "collaboration": collaboration,
            "quality": quality,
            "usage": usage,
            "users": users,
            "tags": tags,
        }
    finally:
        conn.close()
