from fastapi import APIRouter, Query
from typing import Optional
from typing import List, Optional

from app.db import get_connection

router = APIRouter()

@router.get("/federated-fields", response_model=List[str])
def get_federated_fields():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT federated_field_name_open_for_all FROM federated_field_master ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        return {"error": str(e)}


@router.post("/submit-mapping")
def save_mapping(
    participant_name: str = Query(...),
    vernacular_name_common_names: Optional[str] = Query(None),
    taxon_scientific_name: Optional[str] = Query(None),
    family_name: Optional[str] = Query(None),
    habitat: Optional[str] = Query(None),
    medicinal_uses: Optional[str] = Query(None)
):
    try:
        # Build the mapping dictionary only with provided fields
        mappings = {
            "vernacular_name_common_names": vernacular_name_common_names,
            "taxon_scientific_name": taxon_scientific_name,
            "family_name": family_name,
            "habitat": habitat,
            "medicinal_uses": medicinal_uses
        }

        # Filter out any None values
        mappings = {k: v for k, v in mappings.items() if v is not None}

        if not mappings:
            return {
                "status": "error",
                "message": "No mapping fields provided. Please include at least one field."
            }

        # Insert into DB
        conn = get_connection()
        cur = conn.cursor()
        for field_name, column_name in mappings.items():
            cur.execute("""
                INSERT INTO federated_field_mapping (agent_name, federated_field_name_open_for_all, column_name)
                VALUES (%s, %s, %s)
            """, (participant_name, field_name, column_name))
        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": f"✅ Saved {len(mappings)} mappings for participant '{participant_name}'"
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
