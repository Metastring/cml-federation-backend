

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from app.db import get_connection
from datetime import date
from app.db import get_connection

router = APIRouter()

class DatasetMasterInput(BaseModel):
    title: str
    description: Optional[str]
    citation: Optional[str]
    doi: Optional[str]
    language: str
    data_language: str
    license: str
    publication_date: Optional[date]
    metadata_modified_date: Optional[date]
    registration_date: Optional[date]
    is_active: bool
    keywords: Optional[str]
    dataset_type: Optional[str]
    category_id: str

@router.post("/dataset-master")
def create_dataset_master(dataset: DatasetMasterInput):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO dataset_master (
                title, description, citation, doi, language,
                data_language, license, publication_date,
                metadata_modified_date, registration_date, is_active,
                keywords, dataset_type, category_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING dataset_id;
        """, (
            dataset.title, dataset.description, dataset.citation,
            dataset.doi, dataset.language, dataset.data_language,
            dataset.license, dataset.publication_date, dataset.metadata_modified_date,
            dataset.registration_date, dataset.is_active, dataset.keywords,
            dataset.dataset_type, dataset.category_id
        ))

        dataset_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "dataset_id": dataset_id}

    except Exception as e:
        return {"status": "error", "error": str(e)}



