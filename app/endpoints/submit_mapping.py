from fastapi import APIRouter, Query
from typing import Optional
from app.db import get_connection
from typing import Optional, List


router = APIRouter()

@router.post("/submit-mapping-new")
def save_mapping_new(
    category: str = Query(...),
    dataset: str = Query(...),
    participant_name: Optional[str] = Query(None),
    base_url: Optional[str] = Query(None),
    vernacular_name_common_names: Optional[str] = Query(None),
    taxon_scientific_name: Optional[str] = Query(None),
    family_name: Optional[str] = Query(None),
    habitat: Optional[str] = Query(None),
    medicinal_uses: Optional[str] = Query(None),
    other: Optional[List[str]] = Query(None)  # new param
):
    try:
        # Step 1: Insert category
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO category_master (category_name)
            VALUES (%s)
            ON CONFLICT (category_name) DO NOTHING
            RETURNING category_id
        """, (category,))
        category_result = cur.fetchone()

        if category_result:
            category_id = category_result[0]
        else:
            cur.execute("SELECT category_id FROM category_master WHERE category_name = %s", (category,))
            category_row = cur.fetchone()
            if not category_row:
                raise Exception(f"Category '{category}' not found after insert.")
            category_id = category_row[0]

        # Step 2: Insert dataset
        cur.execute("""
            INSERT INTO dataset_master (dataset_name, category_id)
            VALUES (%s, %s)
            ON CONFLICT (dataset_name, category_id) DO NOTHING
            RETURNING dataset_id
        """, (dataset, category_id))
        dataset_result = cur.fetchone()

        if dataset_result:
            dataset_id = dataset_result[0]
        else:
            cur.execute(
                "SELECT dataset_id FROM dataset_master WHERE dataset_name = %s AND category_id = %s",
                (dataset, category_id)
            )
            dataset_row = cur.fetchone()
            if not dataset_row:
                raise Exception(f"Dataset '{dataset}' not found after insert.")
            dataset_id = dataset_row[0]

        # Step 3: Insert participant
        cur.execute("""
            INSERT INTO participant_master (participant_name, dataset_id, base_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (participant_name) DO NOTHING
            RETURNING participant_id
        """, (participant_name, dataset_id, base_url))
        participant_result = cur.fetchone()

        if participant_result:
            participant_id = participant_result[0]
        else:
            cur.execute(
                "SELECT participant_id FROM participant_master WHERE participant_name = %s AND dataset_id = %s",
                (participant_name, dataset_id)
            )
            participant_row = cur.fetchone()
            if not participant_row:
                raise Exception(f"Participant '{participant_name}' not found after insert.")
            participant_id = participant_row[0]

        # Step 4: Build mapping dict from non-null fields
        mappings = {
            "vernacular_name_common_names": vernacular_name_common_names,
            "taxon_scientific_name": taxon_scientific_name,
            "family_name": family_name,
            "habitat": habitat,
            "medicinal_uses": medicinal_uses
        }
        mappings = {k: v for k, v in mappings.items() if v is not None}

        if not mappings:
            return {
                "status": "error",
                "message": "No mapping fields provided."
            }

        # Step 5: Insert field mappings
        for field_name, column_name in mappings.items():
            cur.execute(
                "SELECT id FROM federated_field_master WHERE federated_field_name = %s",
                (field_name,)
            )
            result = cur.fetchone()
            if not result:
                return {
                    "status": "error",
                    "message": f"Federated field '{field_name}' not found."
                }
            federated_field_id = result[0]


         # Step 6: Insert other fields if provided
        if other:
            for column_name in other:
                cur.execute("""
                    INSERT INTO other_field_mapping (category_id, dataset_id, column_name)
                    VALUES (%s, %s, %s)
                """, (category_id, dataset_id, column_name))

        # Insert mapping
        cur.execute("""
                INSERT INTO federated_field_mapping (
                    participant_id,
                    federated_field_id,
                    column_name,
                    dataset_id
                ) VALUES (%s, %s, %s, %s)
        """, (participant_id, federated_field_id, column_name, dataset_id))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": f"Saved {len(mappings)} mappings under category '{category}' and dataset '{dataset}'"
        }

    except Exception as e:
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return {
            "status": "error",
            "error": str(e)
        }
