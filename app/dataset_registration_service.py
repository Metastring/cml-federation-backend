from __future__ import annotations

import csv
import io
import re
from contextlib import contextmanager
from difflib import SequenceMatcher
from pathlib import Path

from fastapi import UploadFile
from openpyxl import load_workbook
from psycopg2.extras import Json, RealDictCursor

from app.dataset_ontology_mapping_service import get_ontology_fields
from app.db import get_connection
from app.endpoints.dataset_details import DatasetRegistryInput, _create_dataset_registry_from_payload

# Backs steps 1 ("Source & details") and 3 ("Review & publish") of the
# registration wizard. Step 2 (ontology mapping) already lives in
# dataset_ontology_mapping_service.py and is only called into here, not
# duplicated -- see suggest_mappings below.
#
# Source-type scope for this pass (product decision 2026-09-16): file
# (CSV/XLSX only, no PDF), url (test + detect JSON fields), database
# (.sql dump upload + static parse -- no live-credential connection, no
# execution of the dump). Map data (shapefile/GeoTIFF) is out of scope --
# that belongs to map-module-backend.

REPO_ROOT = Path(__file__).resolve().parent.parent
UPLOAD_DIR = REPO_ROOT / "registration-uploads"


class DatasetNotFoundError(Exception):
    pass


class SourceConfigNotFoundError(Exception):
    pass


class UnsupportedFileTypeError(Exception):
    pass


@contextmanager
def _cursor():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


def _dataset_row(cur, dataset_id: int) -> dict | None:
    cur.execute("SELECT * FROM dataset_master WHERE dataset_id = %s", (dataset_id,))
    return cur.fetchone()


def _stringify(value) -> str | None:
    return None if value is None else str(value)


# ---------------------------------------------------------------------------
# Draft lifecycle
# ---------------------------------------------------------------------------

def create_draft(payload: DatasetRegistryInput) -> dict:
    result = _create_dataset_registry_from_payload(payload, dataset_status="draft")
    if result.get("status") == "error":
        raise ValueError(result["error"])
    return result


_DRAFT_UPDATABLE_COLUMNS = {
    "title", "description", "citation", "doi", "language", "data_language",
    "license", "keywords", "dataset_type", "category_id",
}


def update_draft(dataset_id: int, fields: dict) -> dict:
    """Patch a subset of dataset_master columns -- powers "Save as draft"."""
    updates = {k: v for k, v in fields.items() if k in _DRAFT_UPDATABLE_COLUMNS}
    if not updates:
        raise ValueError("No updatable fields supplied")

    set_clause = ", ".join(f"{col} = %s" for col in updates)
    values = list(updates.values()) + [dataset_id]

    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        cur.execute(
            f"UPDATE dataset_master SET {set_clause}, updated_at = NOW() WHERE dataset_id = %s RETURNING *",
            values,
        )
        row = cur.fetchone()
    return dict(row)


# ---------------------------------------------------------------------------
# Field detection -- pure parsing helpers (no DB, no disk) so they're
# testable directly against fixture content.
# ---------------------------------------------------------------------------

def parse_csv_fields(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    first_row = next(reader, {}) or {}
    fieldnames = reader.fieldnames or list(first_row.keys())
    return [
        {"field_name": name, "sample_value": _stringify(first_row.get(name))}
        for name in fieldnames
        if name
    ]


def parse_xlsx_fields(content: bytes) -> list[dict]:
    workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    rows = workbook.active.iter_rows(values_only=True)
    header = next(rows, None) or ()
    first_data_row = next(rows, None) or ()

    fields = []
    for idx, name in enumerate(header):
        if not name:
            continue
        value = first_data_row[idx] if idx < len(first_data_row) else None
        fields.append({"field_name": str(name), "sample_value": _stringify(value)})
    return fields


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"'`]?(?P<name>[\w.]+)[\"'`]?\s*\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
_COLUMN_LINE_RE = re.compile(r"""^["'`]?(?P<name>[A-Za-z_][\w]*)["'`]?\s+(?P<type>[A-Za-z][\w\s(),]*)""")
_SQL_CONSTRAINT_LINE_STARTS = ("primary key", "foreign key", "unique", "constraint", "check", "key ")


def _split_top_level_commas(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas, ignoring commas nested in parens
    (e.g. inside a NUMERIC(10,2) type or a multi-column UNIQUE(...))."""
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def parse_sql_dump_tables(sql_text: str) -> list[dict]:
    """Statically parse CREATE TABLE statements for table + column names.

    Never executes the dump -- an uploaded .sql file is untrusted input, and
    the registration flow only needs its schema, not its data.
    """
    tables = []
    for match in _CREATE_TABLE_RE.finditer(sql_text):
        table_name = match.group("name").split(".")[-1]
        columns = []
        for raw_line in _split_top_level_commas(match.group("body")):
            line = raw_line.strip()
            if not line or line.lower().startswith(_SQL_CONSTRAINT_LINE_STARTS):
                continue
            col_match = _COLUMN_LINE_RE.match(line)
            if col_match:
                columns.append({
                    "field_name": col_match.group("name"),
                    "data_type": col_match.group("type").strip().rstrip(","),
                })
        if columns:
            tables.append({"table_name": table_name, "columns": columns})
    return tables


def _save_upload(dataset_id: int, filename: str, content: bytes) -> Path:
    target_dir = UPLOAD_DIR / str(dataset_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]", "_", filename)
    path = target_dir / safe_name
    path.write_bytes(content)
    return path


def _upsert_source_config(cur, dataset_id: int, **fields) -> dict:
    columns = ["dataset_id"] + list(fields.keys())
    values = [dataset_id] + list(fields.values())
    placeholders = ", ".join(["%s"] * len(values))
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in fields)
    cur.execute(
        f"""
        INSERT INTO dataset_source_config ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (dataset_id) DO UPDATE SET {update_clause}, updated_at = NOW()
        RETURNING *
        """,
        values,
    )
    return dict(cur.fetchone())


async def save_file_source(dataset_id: int, upload: UploadFile) -> dict:
    filename = upload.filename or "upload"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    content = await upload.read()

    if ext == "csv":
        fields = parse_csv_fields(content.decode("utf-8", errors="replace"))
    elif ext in {"xlsx", "xlsm"}:
        fields = parse_xlsx_fields(content)
    else:
        raise UnsupportedFileTypeError(f"Unsupported file type '.{ext or '?'}' -- only .csv and .xlsx are supported")

    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        stored_path = _save_upload(dataset_id, filename, content)
        return _upsert_source_config(
            cur, dataset_id,
            source_type="file",
            file_name=filename,
            file_path=str(stored_path),
            file_format=ext,
            detected_fields=Json(fields),
        )


def _parse_auth_header(auth_header: str | None) -> dict:
    if not auth_header:
        return {}
    if ":" in auth_header:
        key, _, value = auth_header.partition(":")
        return {key.strip(): value.strip()}
    return {"Authorization": auth_header}


async def test_and_save_url_source(
    dataset_id: int, source_url: str, method: str, auth_header: str | None, response_format: str
) -> dict:
    import httpx

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.request(method.upper(), source_url, headers=_parse_auth_header(auth_header))
        response.raise_for_status()
        data = response.json()

    if isinstance(data, list):
        sample = data[0] if data else {}
    elif isinstance(data, dict):
        sample = data
    else:
        sample = {}

    fields = [
        {"field_name": key, "sample_value": _stringify(value)}
        for key, value in sample.items()
    ] if isinstance(sample, dict) else []

    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        return _upsert_source_config(
            cur, dataset_id,
            source_type="url",
            source_url=source_url,
            http_method=method.upper(),
            auth_header=auth_header,
            response_format=response_format,
            detected_fields=Json(fields),
        )


async def save_database_dump_source(dataset_id: int, upload: UploadFile) -> dict:
    filename = upload.filename or "dump.sql"
    content = await upload.read()
    tables = parse_sql_dump_tables(content.decode("utf-8", errors="replace"))
    if not tables:
        raise ValueError("No CREATE TABLE statements were found in the uploaded dump")

    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        stored_path = _save_upload(dataset_id, filename, content)
        source_config = _upsert_source_config(
            cur, dataset_id,
            source_type="database",
            dump_file_name=filename,
            dump_file_path=str(stored_path),
            selected_table_name=None,
            detected_fields=Json({"tables": tables}),
        )
    return {"tables": tables, "source_config": source_config}


def select_database_table(dataset_id: int, table_name: str) -> dict:
    with _cursor() as cur:
        cur.execute("SELECT * FROM dataset_source_config WHERE dataset_id = %s", (dataset_id,))
        config = cur.fetchone()
        if not config:
            raise SourceConfigNotFoundError(dataset_id)

        tables = (config["detected_fields"] or {}).get("tables", [])
        match = next((t for t in tables if t["table_name"] == table_name), None)
        if not match:
            raise ValueError(f"Table {table_name!r} was not found in the uploaded dump")

        cur.execute(
            """
            UPDATE dataset_source_config
            SET selected_table_name = %s, detected_fields = %s, updated_at = NOW()
            WHERE dataset_id = %s
            RETURNING *
            """,
            (table_name, Json(match["columns"]), dataset_id),
        )
        return dict(cur.fetchone())


# ---------------------------------------------------------------------------
# Ontology mapping auto-suggest (step 2) -- reads detected_fields saved
# above, matches against the real ontology field list from
# dataset_ontology_mapping_service.get_ontology_fields.
# ---------------------------------------------------------------------------

def _normalize_field_name(name: str) -> str:
    return re.sub(r"[\s_\-]", "", name or "").lower()


def _best_match(field_name: str, ontology_fields: list[dict]) -> tuple[dict | None, float]:
    """Pure matching logic, split out from suggest_mappings so it's testable
    without a database: highest SequenceMatcher ratio against either the
    ontology field's own name or its display label."""
    norm_field = _normalize_field_name(field_name)
    best_field, best_score = None, 0.0
    for candidate in ontology_fields:
        score = max(
            SequenceMatcher(None, norm_field, _normalize_field_name(candidate["value"])).ratio(),
            SequenceMatcher(None, norm_field, _normalize_field_name(candidate["label"])).ratio(),
        )
        if score > best_score:
            best_field, best_score = candidate, score
    return best_field, best_score

# Below this ratio, don't suggest anything -- an empty dropdown beats a
# confidently wrong one. Between this and AUTO_THRESHOLD, still suggest it
# but flag it for the "needs review" badge instead of "auto".
SUGGEST_THRESHOLD = 0.6
AUTO_THRESHOLD = 0.85


def suggest_mappings(dataset_id: int, ontology_graph_key: str) -> dict:
    with _cursor() as cur:
        cur.execute(
            "SELECT detected_fields FROM dataset_source_config WHERE dataset_id = %s", (dataset_id,)
        )
        config = cur.fetchone()
    if not config:
        raise SourceConfigNotFoundError(dataset_id)

    detected = config["detected_fields"]
    detected = detected if isinstance(detected, list) else []

    ontology_fields = get_ontology_fields(ontology_graph_key)["items"]

    items = []
    for entry in detected:
        field_name = entry.get("field_name")
        if not field_name:
            continue
        best_field, score = _best_match(field_name, ontology_fields)
        items.append({
            "field_name": field_name,
            "suggested_ontology_field": best_field["value"] if best_field and score >= SUGGEST_THRESHOLD else None,
            "confidence": round(score, 2),
            "auto": score >= AUTO_THRESHOLD,
        })

    return {"dataset_id": dataset_id, "ontology_graph_key": ontology_graph_key, "items": items}


# ---------------------------------------------------------------------------
# Propose a new ontology term (step 2 escape hatch)
# ---------------------------------------------------------------------------

def propose_term(
    dataset_id: int, field_name: str, proposed_label: str,
    proposed_definition: str | None, target_ontology_graph_key: str | None,
) -> dict:
    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        cur.execute(
            """
            INSERT INTO ontology_term_proposal
                (dataset_id, field_name, proposed_label, proposed_definition, target_ontology_graph_key)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
            """,
            (dataset_id, field_name, proposed_label, proposed_definition, target_ontology_graph_key),
        )
        return dict(cur.fetchone())


def list_term_proposals(status: str | None = None) -> list[dict]:
    with _cursor() as cur:
        if status:
            cur.execute(
                "SELECT * FROM ontology_term_proposal WHERE status = %s ORDER BY created_at DESC", (status,)
            )
        else:
            cur.execute("SELECT * FROM ontology_term_proposal ORDER BY created_at DESC")
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Step 3 -- review & publish
# ---------------------------------------------------------------------------

def get_review_summary(dataset_id: int) -> dict:
    with _cursor() as cur:
        dataset = _dataset_row(cur, dataset_id)
        if not dataset:
            raise DatasetNotFoundError(dataset_id)

        cur.execute("SELECT * FROM dataset_source_config WHERE dataset_id = %s", (dataset_id,))
        source = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS count FROM dataset_mapping WHERE dataset_id = %s", (dataset_id,))
        fields_mapped = cur.fetchone()["count"]

    detected = source["detected_fields"] if source else None
    fields_detected = len(detected) if isinstance(detected, list) else 0

    return {
        "dataset_id": dataset_id,
        "title": dataset["title"],
        "category_id": dataset["category_id"],
        "status": dataset["status"],
        "source_type": source["source_type"] if source else None,
        "fields_detected": fields_detected,
        "fields_mapped": fields_mapped,
    }


def publish(dataset_id: int) -> dict:
    with _cursor() as cur:
        if not _dataset_row(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        cur.execute(
            "UPDATE dataset_master SET status = %s, updated_at = NOW() WHERE dataset_id = %s RETURNING *",
            ("Pending review", dataset_id),
        )
        row = dict(cur.fetchone())

    try:
        from app.retrieval_index import build_index
        build_index()
    except Exception:
        pass  # best-effort reindex -- publish must succeed even if this fails

    return row
