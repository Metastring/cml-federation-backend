from fastapi import APIRouter, HTTPException
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter()


@router.get("/federated-sources", summary="List all federated data sources")
def get_federated_sources():
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "SELECT dataset_name, source_api, source_curl FROM public.federated_sources ORDER BY id"
        )
        rows = cursor.fetchall()
        return {"sources": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
