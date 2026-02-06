from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import List
from app.db import get_connection

router = APIRouter()

# ✅ Keep only one router and combine both endpoints below

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


class FieldMapping(BaseModel):
    field_name_open_for_all: str
    column_name: str

@router.post("/submit-mapping")
def save_mapping(agent_name: str = Query(...), mappings: List[FieldMapping] = []):
    try:
        conn = get_connection()
        cur = conn.cursor()
        for mapping in mappings:
            cur.execute("""
                INSERT INTO federated_field_mapping (agent_name, federated_field_name_open_for_all, column_name)
                VALUES (%s, %s, %s)
            """, (agent_name, mapping.field_name_open_for_all, mapping.column_name))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": f"✅ Saved {len(mappings)} mappings from agent '{agent_name}'"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

