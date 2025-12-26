from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.db import get_connection

router = APIRouter()

@router.get("/ontology-list")
async def get_ontology_list():
    """
    Returns all unique ontology_mapping_to_display values from the dataset_mapping table.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ontology_mapping_to_display FROM dataset_mapping WHERE ontology_mapping_to_display IS NOT NULL")
        rows = cursor.fetchall()
        unique_ontologies = sorted({row[0] for row in rows if row[0]})
        cursor.close()
        conn.close()
        return JSONResponse(content={"ontology_list": unique_ontologies})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
