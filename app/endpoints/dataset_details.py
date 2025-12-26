from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
from app.db import get_connection
from datetime import date

router = APIRouter()

# ---------- Pydantic Models ----------
class Scope(BaseModel):
    temporal_start_date: Optional[date] = None
    temporal_end_date: Optional[date] = None
    geographic_scope: Optional[str] = None
    taxonomic_scope: Optional[str] = None
    taxonomic_authority: Optional[str] = None

class Publisher(BaseModel):
    publisher_name: str
    record_count: int

class Contact(BaseModel):
    name: str
    role: str
    email: str
    organization: str
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

class Mapping(BaseModel):
    field_name: str
    ontology_mapping: str
    data_type: str

class MappingItem(BaseModel):
    field_name: str
    ontology_mapping: Optional[str] = None
    description: Optional[str] = None  # For future use

class DatasetMappingUpdateInput(BaseModel):
    dataset_id: str
    mappings: List[MappingItem]

class Metric(BaseModel):
    metric_name: str
    metric_value: str

class Statistic(BaseModel):
    stat_name: str
    stat_value: str
    measurement_date: Optional[date] = None

class DatasetDetailsInput(BaseModel):
    dataset_id: str
    scopes: Optional[List[Scope]] = []
    publishers: Optional[List[Publisher]] = []
    contacts: Optional[List[Contact]] = []
    mappings: Optional[List[Mapping]] = []
    metrics: Optional[List[Metric]] = []
    statistics: Optional[List[Statistic]] = []


# Models for dataset registry (includes dataset_master, category, sources, etc.)
class CategoryInput(BaseModel):
    category_id: Optional[str] = None
    category_name: Optional[str] = None


class SourceInput(BaseModel):
    source_name: str
    base_url: Optional[str] = None
    description: Optional[str] = None


class DatasetRegistryInput(BaseModel):
    # category: either id or name (name will create a new category)
    category: Optional[CategoryInput] = None

    # dataset_master fields
    title: str
    description: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    language: Optional[str] = None
    data_language: Optional[str] = None
    license: Optional[str] = None
    is_active: Optional[bool] = True
    keywords: Optional[str] = None
    dataset_type: Optional[str] = None

    # related records
    publishers: Optional[List[Publisher]] = []
    contacts: Optional[List[Contact]] = []
    sources: Optional[List[SourceInput]] = []
    statistics: Optional[List[Statistic]] = []

# ---------- API ----------
@router.post("/dataset-details")
def save_dataset_details(details: DatasetDetailsInput):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # dataset_scope
        if details.scopes:
            for s in details.scopes:
                cur.execute("""
                    INSERT INTO dataset_scope (
                        dataset_id, temporal_start_date, temporal_end_date,
                        geographic_scope, taxonomic_scope, taxonomic_authority
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                """, (
                    details.dataset_id, s.temporal_start_date, s.temporal_end_date,
                    s.geographic_scope, s.taxonomic_scope, s.taxonomic_authority
                ))

        # dataset_publisher
        if details.publishers:
            for p in details.publishers:
                cur.execute("""
                    INSERT INTO dataset_publisher (
                        dataset_id, publisher_name, record_count
                    ) VALUES (%s,%s,%s)
                """, (details.dataset_id, p.publisher_name, p.record_count))

        # dataset_contacts
        if details.contacts:
            for c in details.contacts:
                cur.execute("""
                    INSERT INTO dataset_contacts (
                        dataset_id, name, role, email, organization, address,
                        city, state, country
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    details.dataset_id, c.name, c.role, c.email, c.organization,
                    c.address, c.city, c.state, c.country
                ))

        # dataset_mapping
        if details.mappings:
            for m in details.mappings:
                cur.execute("""
                    INSERT INTO dataset_mapping (
                        dataset_id, field_name, ontology_mapping, data_type
                    ) VALUES (%s,%s,%s,%s)
                """, (details.dataset_id, m.field_name, m.ontology_mapping, m.data_type))

        # dataset_metrics
        if details.metrics:
            for m in details.metrics:
                cur.execute("""
                    INSERT INTO dataset_metrics (
                        dataset_id, metric_name, metric_value
                    ) VALUES (%s,%s,%s)
                """, (details.dataset_id, m.metric_name, m.metric_value))

        # dataset_statistics
        if details.statistics:
            for s in details.statistics:
                cur.execute("""
                    INSERT INTO dataset_statistics (
                        dataset_id, stat_name, stat_value, measurement_date
                    ) VALUES (%s,%s,%s,%s)
                """, (details.dataset_id, s.stat_name, s.stat_value, s.measurement_date))

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "message": "All dataset details saved"}

    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/dataset-registry")
def create_dataset_registry(payload: DatasetRegistryInput):
    """Insert category (if needed), dataset_master and related records in a single transaction."""
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Handle category: prefer existing by id, then by name; insert only if name not found
        category_id = None
        if payload.category:
            # If caller supplied an id, verify it exists and use it
            if payload.category.category_id:
                cur.execute("SELECT category_id FROM category_master WHERE category_id = %s", (payload.category.category_id,))
                row = cur.fetchone()
                if row:
                    category_id = row[0]
                else:
                    return {"status": "error", "error": f"category_id {payload.category.category_id} does not exist"}

            # If caller supplied a name, try to find existing category by name
            elif payload.category.category_name:
                cur.execute("SELECT category_id FROM category_master WHERE category_name = %s", (payload.category.category_name,))
                row = cur.fetchone()
                if row:
                    category_id = row[0]
                else:
                    cur.execute("""
                        INSERT INTO category_master (category_name)
                        VALUES (%s)
                        RETURNING category_id
                    """, (payload.category.category_name,))
                    category_id = cur.fetchone()[0]

        # Insert dataset_master with system dates
        from datetime import datetime
        system_date = datetime.now().date()
        
        cur.execute("""
            INSERT INTO dataset_master (
                title, description, citation, doi, language,
                data_language, license, publication_date,
                metadata_modified_date, registration_date, is_active,
                keywords, dataset_type, category_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING dataset_id;
        """, (
            payload.title, payload.description, payload.citation,
            payload.doi, payload.language, payload.data_language,
            payload.license, system_date, system_date,
            system_date, payload.is_active, payload.keywords,
            payload.dataset_type, category_id
        ))

        dataset_id = cur.fetchone()[0]

        # dataset_publisher (avoid duplicates by name+dataset_id)
        if payload.publishers:
            for p in payload.publishers:
                cur.execute("SELECT 1 FROM dataset_publisher WHERE dataset_id=%s AND publisher_name=%s", (dataset_id, p.publisher_name))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO dataset_publisher (
                            dataset_id, publisher_name, record_count
                        ) VALUES (%s,%s,%s)
                    """, (dataset_id, p.publisher_name, p.record_count))

        # dataset_contacts (avoid duplicates by name+email+dataset_id)
        if payload.contacts:
            for c in payload.contacts:
                cur.execute("SELECT 1 FROM dataset_contacts WHERE dataset_id=%s AND name=%s AND email=%s", (dataset_id, c.name, c.email))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO dataset_contacts (
                            dataset_id, name, role, email, organization, address,
                            city, state, country
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        dataset_id, c.name, c.role, c.email, c.organization,
                        c.address, c.city, c.state, c.country
                    ))

        # source_master (avoid duplicates by source_name+dataset_id)
        if payload.sources:
            for s in payload.sources:
                cur.execute("SELECT 1 FROM source_master WHERE source_name=%s AND dataset_id=%s", (s.source_name, dataset_id))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO source_master (
                            source_name, dataset_id, base_url, description
                        ) VALUES (%s,%s,%s,%s)
                    """, (s.source_name, dataset_id, s.base_url, s.description))

        # dataset_statistics (avoid duplicates by stat_name+dataset_id+measurement_date)
        if payload.statistics:
            for st in payload.statistics:
                cur.execute("SELECT 1 FROM dataset_statistics WHERE dataset_id=%s AND stat_name=%s AND measurement_date=%s", (dataset_id, st.stat_name, st.measurement_date))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO dataset_statistics (
                            dataset_id, stat_name, stat_value, measurement_date
                        ) VALUES (%s,%s,%s,%s)
                    """, (dataset_id, st.stat_name, st.stat_value, st.measurement_date))

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "dataset_id": dataset_id, "category_id": category_id}

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"status": "error", "error": str(e)}


@router.post("/dataset-mapping-update")
def update_dataset_mapping(payload: DatasetMappingUpdateInput):
    """
    Update dataset_mapping table by dataset_id.
    Inserts field mappings with their ontology mappings.
    ontology_mapping can be null or empty.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        dataset_id = payload.dataset_id

        # Insert mappings into dataset_mapping
        inserted_count = 0
        if payload.mappings:
            for mapping in payload.mappings:
                field_name = mapping.field_name
                ontology_mapping_display = mapping.ontology_mapping or None  # Allow null/empty
                
                if not field_name:
                    return {"status": "error", "error": "field_name is required for each mapping"}
                
                # If ontology_mapping_display is provided, look up the corresponding ontology_mapping
                ontology_mapping = None
                if ontology_mapping_display:
                    cur.execute(
                        """
                        SELECT ontology_mapping FROM dataset_mapping 
                        WHERE ontology_mapping_to_display = %s 
                        LIMIT 1
                        """,
                        (ontology_mapping_display,)
                    )
                    result = cur.fetchone()
                    if result:
                        ontology_mapping = result[0]
                
                # Insert into dataset_mapping
                cur.execute(
                    """
                    INSERT INTO dataset_mapping (
                        dataset_id, field_name, ontology_mapping, ontology_mapping_to_display
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (dataset_id, field_name, ontology_mapping, ontology_mapping_display)
                )
                inserted_count += 1

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "dataset_id": dataset_id,
            "mappings_inserted": inserted_count
        }

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"status": "error", "error": str(e)}
