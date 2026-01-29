from fastapi import APIRouter

from app.core.database import get_connection
from app.schemas.dataset_schema import (
    Scope, Publisher, Contact, Mapping, MappingItem, DatasetMappingUpdateInput, Metric, Statistic, DatasetDetailsInput, CategoryInput, SourceInput, DatasetRegistryInput
)
from app.repositories.dataset_repository import DatasetRepository

router = APIRouter()



# ---------- API ----------
@router.post("/dataset-details")
def save_dataset_details(details: DatasetDetailsInput):
    return DatasetRepository.save_dataset_details(details)

@router.post("/dataset-registry")
def create_dataset_registry(payload: DatasetRegistryInput):
    """Insert category (if needed), dataset_master and related records in a single transaction."""
    return DatasetRepository.create_dataset_registry(payload)


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
