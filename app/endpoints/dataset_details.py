from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
from app.db import get_connection
from datetime import date



router = APIRouter()

# ---------- Pydantic Models ----------
class Scope(BaseModel):
    temporal_start_date: Optional[date]
    temporal_end_date: Optional[date]
    geographic_scope: Optional[str]
    taxonomic_scope: Optional[str]
    taxonomic_authority: Optional[str]

class Publisher(BaseModel):
    country: str
    record_count: int

class Contact(BaseModel):
    name: str
    role: str
    email: str
    organization: str
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]

class Mapping(BaseModel):
    field_name: str
    ontology_mapping: str
    data_type: str

class Metric(BaseModel):
    metric_name: str
    metric_value: str

class Statistic(BaseModel):
    stat_name: str
    stat_value: str
    measurement_date: Optional[date]

class DatasetDetailsInput(BaseModel):
    dataset_id: str
    scopes: Optional[List[Scope]] = []
    publishers: Optional[List[Publisher]] = []
    contacts: Optional[List[Contact]] = []
    mappings: Optional[List[Mapping]] = []
    metrics: Optional[List[Metric]] = []
    statistics: Optional[List[Statistic]] = []

# ---------- API ----------
@router.post("/dataset-details")
def save_dataset_details(details: DatasetDetailsInput):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # dataset_scope
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
        for p in details.publishers:
            cur.execute("""
                INSERT INTO dataset_publisher (
                    dataset_id, country, record_count
                ) VALUES (%s,%s,%s)
            """, (details.dataset_id, p.country, p.record_count))

        # dataset_contacts
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
        for m in details.mappings:
            cur.execute("""
                INSERT INTO dataset_mapping (
                    dataset_id, field_name, ontology_mapping, data_type
                ) VALUES (%s,%s,%s,%s)
            """, (details.dataset_id, m.field_name, m.ontology_mapping, m.data_type))

        # dataset_metrics
        for m in details.metrics:
            cur.execute("""
                INSERT INTO dataset_metrics (
                    dataset_id, metric_name, metric_value
                ) VALUES (%s,%s,%s)
            """, (details.dataset_id, m.metric_name, m.metric_value))

        # dataset_statistics
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
