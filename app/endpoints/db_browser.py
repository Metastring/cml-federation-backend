from datetime import date, datetime, time
from decimal import Decimal

import psycopg2
from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from psycopg2 import sql

from app.db import DB_CONFIG, get_connection

# Read-only view of the data behind this node's registered datasets (rainfall,
# crop production, ...). Only tables linked to an active dataset_master row via
# map_layer_info.geoserver_name "<workspace>:<table>" are exposed; the node's
# own bookkeeping tables (registry, keys, ontology) never are.
router = APIRouter(prefix="/db", tags=["Dataset Tables"])

MAX_LIMIT = 1000

REGISTERED_TABLES_SQL = """
    SELECT m.dataset_id, m.title, c.category_name, split_part(l.geoserver_name, ':', 2) AS table_name
    FROM dataset_master m
    JOIN map_layer_info l ON l.dataset_id = m.dataset_id
    LEFT JOIN category_master c ON c.category_id = m.category_id
    JOIN information_schema.tables t
      ON t.table_schema = 'public' AND t.table_name = split_part(l.geoserver_name, ':', 2)
    WHERE m.is_active AND position(':' in l.geoserver_name) > 0
"""


class DatabaseUnavailable(Exception):
    pass


def _connect():
    if not DB_CONFIG.get("dbname"):
        raise DatabaseUnavailable("No database is configured on this node.")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        raise DatabaseUnavailable(f"Database is not reachable: {str(exc).strip().splitlines()[0]}")
    conn.set_session(readonly=True)
    return conn


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, memoryview)):
        return f"<{len(value)} bytes>"
    return value


def _columns(cur, table: str) -> list[dict]:
    cur.execute(
        """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [{"name": n, "type": udt if t == "USER-DEFINED" else t} for n, t, udt in cur.fetchall()]


def _count(cur, table: str) -> int:
    cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("public", table)))
    return cur.fetchone()[0]


def _list_tables() -> dict:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(REGISTERED_TABLES_SQL + " ORDER BY m.dataset_id")
            tables = [
                {
                    "dataset_id": dataset_id,
                    "dataset_title": title,
                    "category": category,
                    "table": table,
                    "row_count": _count(cur, table),
                    "columns": _columns(cur, table),
                }
                for dataset_id, title, category, table in cur.fetchall()
            ]
        return {"database": DB_CONFIG["dbname"], "available": True, "count": len(tables), "tables": tables}
    finally:
        conn.close()


def _table_data(table: str, limit: int, offset: int) -> dict:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(REGISTERED_TABLES_SQL + " AND split_part(l.geoserver_name, ':', 2) = %s LIMIT 1", (table,))
            match = cur.fetchone()
            if not match:
                raise HTTPException(status_code=404, detail=f"No registered dataset has table '{table}' on this node.")
            dataset_id, title, category, _ = match

            columns = _columns(cur, table)
            # Geometry columns come back as GeoJSON instead of hex WKB.
            select_list = sql.SQL(", ").join(
                sql.SQL("ST_AsGeoJSON({c})::json AS {c}").format(c=sql.Identifier(c["name"]))
                if c["type"] in ("geometry", "geography")
                else sql.Identifier(c["name"])
                for c in columns
            )
            cur.execute(
                sql.SQL("SELECT {} FROM {} LIMIT %s OFFSET %s").format(select_list, sql.Identifier("public", table)),
                (limit, offset),
            )
            rows = [{c["name"]: _jsonable(v) for c, v in zip(columns, row)} for row in cur.fetchall()]
            total = _count(cur, table)
        return {
            "database": DB_CONFIG["dbname"],
            "dataset_id": dataset_id,
            "dataset_title": title,
            "category": category,
            "table": table,
            "columns": columns,
            "total_rows": total,
            "limit": limit,
            "offset": offset,
            "rows": rows,
        }
    finally:
        conn.close()


@router.get("/tables", summary="List the tables of this node's registered datasets")
async def list_tables():
    try:
        return await run_in_threadpool(_list_tables)
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail={"available": False, "message": str(exc)})


@router.get("/tables/{table}", summary="Rows of one registered dataset's table")
async def get_table_data(
    table: str,
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
):
    try:
        return await run_in_threadpool(_table_data, table, limit, offset)
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail={"available": False, "message": str(exc)})
