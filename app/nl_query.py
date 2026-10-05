"""Natural-language question -> SQL -> rows, over registered tabular datasets
(Epic CPHR-92 "Agent MVP - Phase 2", text-to-SQL path).

Pipeline for one question:
  1. pick_dataset: rank active datasets that have a queryable Postgres table
     (map_layer_info.geoserver_name "<workspace>:<table>") by embedding
     similarity to the question, using the same sentence-transformer as
     app/retrieval_index.py. A dataset-level index (~one entry per dataset)
     rather than the full ontology index: rebuilding that means parsing
     SWEET/ENVO etc., too heavy for the 8 GB host.
  2. generate_sql: send only that table's schema (+ mapped concepts + two
     sample rows) to a local open-weight LLM behind an OpenAI-compatible
     /v1/chat/completions endpoint (llama.cpp `llama-server`, see
     ~/llm/start-llm.sh). No hosted API: nothing leaves the server.
  3. validate_sql + run_sql: single SELECT/WITH statement touching only that
     table, run in a READ ONLY transaction with a statement timeout and a hard
     row cap. On a Postgres error the error is fed back to the LLM once.
  4. The answer is formatted here, not by the LLM (a small model adds ~10 s and
     can misstate numbers); the LLM's own one-line interpretation of the
     question is returned alongside so the user sees how it was read.
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field

import httpx
import numpy as np
from psycopg2 import sql as pgsql

from app.db import get_connection
from app.retrieval_index import EMBEDDING_MODEL_NAME

LLM_BASE_URL = os.getenv("NL_LLM_BASE_URL", "http://127.0.0.1:8095")
LLM_MODEL = os.getenv("NL_LLM_MODEL", "local")  # llama-server serves one model and ignores this
LLM_TIMEOUT_S = float(os.getenv("NL_LLM_TIMEOUT_S", "90"))
# llama-server slot to pin to (start-llm.sh runs --parallel 2). The agent
# (app/agent.py) uses slot 0; keeping this prompt in its own slot means the
# two don't evict each other's cached prompt prefix. -1 = any free slot.
LLM_SLOT = int(os.getenv("NL_LLM_SLOT", "1"))
SQL_TIMEOUT_MS = int(os.getenv("NL_SQL_TIMEOUT_MS", "15000"))
MAX_ROWS = int(os.getenv("NL_MAX_ROWS", "200"))

# Below this cosine similarity the best dataset is treated as "no relevant
# dataset" (same idea as retrieval_index.NO_MATCH_THRESHOLD).
DATASET_MATCH_THRESHOLD = float(os.getenv("NL_DATASET_MATCH_THRESHOLD", "0.2"))

SKIP_COLUMN_TYPES = {"USER-DEFINED", "bytea"}  # geometry etc. - useless to the LLM

# Text columns with at most this many distinct values get "values like: ..."
# hints in the schema: the real values closest in meaning to the question.
# Without them the LLM guesses filter values (e.g. category = 'Child Stunting'
# on NFHS, whose indicators are long strings like "Children under 5 years who
# are stunted (height for age) (%)") and gets 0 rows.
VALUE_HINT_MAX_DISTINCT = 300
VALUE_HINT_TOP_K = 4
VALUE_HINT_MIN_SCORE = 0.45  # name columns (city, state) score ~0.4 on unrelated questions
TEXT_TYPES = {"text", "character varying", "character"}


@dataclass
class QueryableDataset:
    dataset_id: int
    title: str
    description: str
    keywords: str
    citation: str
    table: str
    columns: list[tuple[str, str]]  # (name, data_type)
    concepts: dict[str, str] = field(default_factory=dict)  # field_name -> mapped concept

    def search_text(self) -> str:
        fields = ", ".join(f"{c} ({self.concepts[c]})" if c in self.concepts else c for c, _ in self.columns)
        return f"{self.title}. {self.description} Keywords: {self.keywords}. Fields: {fields}"


class NLQueryError(Exception):
    """A failure the caller should show to the user as-is (no stack trace)."""


# --------------------------------------------------------------------------- datasets

def load_queryable_datasets() -> list[QueryableDataset]:
    """Active datasets whose map layer points at a table that exists in this
    database's public schema."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.dataset_id, m.title, COALESCE(m.description, ''), COALESCE(m.keywords, ''),
                   COALESCE(m.citation, ''), split_part(l.geoserver_name, ':', 2)
            FROM dataset_master m
            JOIN map_layer_info l ON l.dataset_id = m.dataset_id
            WHERE m.is_active AND l.geoserver_name LIKE '%:%'
            ORDER BY m.dataset_id
        """)
        rows = cur.fetchall()

        datasets = []
        for dataset_id, title, description, keywords, citation, table in rows:
            cur.execute("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            columns = [(c, t) for c, t in cur.fetchall() if t not in SKIP_COLUMN_TYPES]
            if not columns:
                continue
            cur.execute("""
                SELECT field_name, COALESCE(ontology_mapping_to_display, ontology_mapping)
                FROM dataset_mapping WHERE dataset_id = %s
            """, (dataset_id,))
            concepts = {f: c for f, c in cur.fetchall() if c and not c.startswith("http")}
            datasets.append(QueryableDataset(dataset_id, title, description, keywords, citation, table, columns, concepts))
        cur.close()
        return datasets
    finally:
        conn.close()


_model = None
_dataset_cache: tuple[tuple, list[QueryableDataset], np.ndarray] | None = None


def _embed(texts: list[str]) -> np.ndarray:
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)


def _dataset_index() -> tuple[list[QueryableDataset], np.ndarray]:
    """Embeddings are recomputed only when the set of queryable datasets changes."""
    global _dataset_cache
    datasets = load_queryable_datasets()
    key = tuple((d.dataset_id, d.search_text()) for d in datasets)
    if _dataset_cache is None or _dataset_cache[0] != key:
        vectors = _embed([d.search_text() for d in datasets]) if datasets else np.zeros((0, 1))
        _dataset_cache = (key, datasets, vectors)
    return _dataset_cache[1], _dataset_cache[2]


def rank_datasets(question: str) -> list[tuple[float, QueryableDataset]]:
    datasets, vectors = _dataset_index()
    if not datasets:
        return []
    scores = vectors @ _embed([question])[0]
    order = np.argsort(-scores)
    return [(float(scores[i]), datasets[i]) for i in order]


# --------------------------------------------------------------------------- LLM

SYSTEM_PROMPT = """You translate a question about one database table into ONE PostgreSQL SELECT query.

Rules:
- Use ONLY the table and columns listed in SCHEMA. Never invent columns or tables.
- First line: "-- Interpretation: " followed by ONE short sentence saying how you read the question.
- Then the query. No explanation, no markdown fences.
- A table can have many rows per place (e.g. one per date). When asked which places match a condition, return one row per place with the value that answers it.
- "temperature" without min/max: for "below" use the minimum column, for "above" use the maximum column, unless the question says average.
- Condition on a single reading ("on any day", "on a single day", "ever") -> MIN()/MAX() of the reading. Totals over a period -> SUM(). "Average" -> AVG().
- Repeat the aggregate expression in HAVING; never reference a SELECT alias there.
- Round decimals with ROUND(x::numeric, 1). Add LIMIT 100.

Examples (for a different table: city_hourly(city text, country text, ts timestamp, temp_low_c float, temp_high_c float, snow_cm float)):

QUESTION: cities that got colder than 0 degrees
-- Interpretation: cities whose lowest recorded temperature is below 0 C, with that lowest value.
SELECT city, country, ROUND(MIN(temp_low_c)::numeric, 1) AS lowest_temp_c FROM city_hourly GROUP BY city, country HAVING MIN(temp_low_c) < 0 ORDER BY lowest_temp_c LIMIT 100

QUESTION: which cities in Norway had more than 20 cm of snow in a single hour
-- Interpretation: Norwegian cities whose largest single-hour snowfall exceeds 20 cm.
SELECT city, ROUND(MAX(snow_cm)::numeric, 1) AS max_hourly_snow_cm FROM city_hourly WHERE country = 'Norway' GROUP BY city HAVING MAX(snow_cm) > 20 ORDER BY max_hourly_snow_cm DESC LIMIT 100

QUESTION: total snow per country in January
-- Interpretation: sum of snowfall per country for January.
SELECT country, ROUND(SUM(snow_cm)::numeric, 1) AS total_snow_cm FROM city_hourly WHERE EXTRACT(MONTH FROM ts) = 1 GROUP BY country ORDER BY total_snow_cm DESC LIMIT 100

QUESTION: cities where the average high temperature is above 30
-- Interpretation: cities whose mean daily-high temperature exceeds 30 C.
SELECT city, country, ROUND(AVG(temp_high_c)::numeric, 1) AS avg_high_c FROM city_hourly GROUP BY city, country HAVING AVG(temp_high_c) > 30 ORDER BY avg_high_c DESC LIMIT 100"""


def _schema_block(ds: QueryableDataset, samples: list[dict], value_hints: dict[str, list[str]] | None = None) -> str:
    value_hints = value_hints or {}
    lines = [f"Table {ds.table} -- {ds.title}"]
    if ds.description:
        lines.append(f"-- {ds.description[:300]}")
    for name, dtype in ds.columns:
        concept = f"  -- {ds.concepts[name]}" if name in ds.concepts else ""
        lines.append(f"  {name} {dtype}{concept}")
        if name in value_hints:
            lines.append("    values like: " + "; ".join(f"'{v}'" for v in value_hints[name]))
    if samples:
        lines.append("Sample rows:")
        lines.extend(f"  {row}" for row in samples)
    return "\n".join(lines)


def _sample_rows(ds: QueryableDataset, n: int = 2) -> list[dict]:
    conn = get_connection()
    try:
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cols = [c for c, _ in ds.columns]
        cur.execute(pgsql.SQL("SELECT {} FROM {} LIMIT %s").format(
            pgsql.SQL(", ").join(map(pgsql.Identifier, cols)), pgsql.Identifier(ds.table)), (n,))
        return [dict(zip(cols, map(str, r))) for r in cur.fetchall()]
    finally:
        conn.close()


_distinct_cache: dict[tuple[str, str], tuple[list[str], np.ndarray]] = {}


def _column_values(ds: QueryableDataset, column: str) -> tuple[list[str], np.ndarray] | None:
    """Distinct values of a low-cardinality text column and their embeddings,
    cached per process (datasets are re-imported rarely; restart to refresh)."""
    key = (ds.table, column)
    if key not in _distinct_cache:
        conn = get_connection()
        try:
            conn.set_session(readonly=True)
            cur = conn.cursor()
            cur.execute(pgsql.SQL("SELECT DISTINCT {c} FROM {t} WHERE {c} IS NOT NULL LIMIT %s").format(
                c=pgsql.Identifier(column), t=pgsql.Identifier(ds.table)), (VALUE_HINT_MAX_DISTINCT + 1,))
            values = sorted(str(r[0]).strip() for r in cur.fetchall())
        finally:
            conn.close()
        if not values or len(values) > VALUE_HINT_MAX_DISTINCT:
            _distinct_cache[key] = ([], np.zeros((0, 1)))
        else:
            _distinct_cache[key] = (values, _embed(values))
    values, vectors = _distinct_cache[key]
    return (values, vectors) if values else None


def _value_hints(ds: QueryableDataset, question: str) -> dict[str, list[str]]:
    q = _embed([question])[0]
    hints = {}
    for name, dtype in ds.columns:
        if dtype not in TEXT_TYPES:
            continue
        found = _column_values(ds, name)
        if not found:
            continue
        values, vectors = found
        # Values named verbatim in the question first ("Good", "Kerala"):
        # short values embed poorly, so similarity alone misses them.
        exact = [v for v in values if re.search(rf"(?<!\w){re.escape(v.lower())}(?!\w)", question.lower())]
        scores = vectors @ q
        similar = [values[i] for i in np.argsort(-scores)[:VALUE_HINT_TOP_K] if scores[i] >= VALUE_HINT_MIN_SCORE]
        top = list(dict.fromkeys(exact + similar))[:VALUE_HINT_TOP_K]
        if top:
            hints[name] = top
    return hints


def _chat(messages: list[dict]) -> str:
    try:
        resp = httpx.post(
            f"{LLM_BASE_URL}/v1/chat/completions",
            json={"model": LLM_MODEL, "messages": messages, "temperature": 0, "max_tokens": 400, "id_slot": LLM_SLOT},
            timeout=LLM_TIMEOUT_S,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise NLQueryError(f"Local LLM at {LLM_BASE_URL} is not reachable ({exc.__class__.__name__}). "
                           "Start it with ~/llm/start-llm.sh.") from exc
    return resp.json()["choices"][0]["message"]["content"]


def _split_llm_output(text: str) -> tuple[str, str]:
    """-> (interpretation, sql)"""
    text = re.sub(r"```(?:sql)?", "", text).strip()
    interpretation = ""
    m = re.search(r"--\s*Interpretation:\s*(.+)", text, re.IGNORECASE)
    if m:
        interpretation = m.group(1).strip()
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--")).strip()
    return interpretation, body.rstrip(";").strip()


# --------------------------------------------------------------------------- SQL safety

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|grant|revoke|truncate|copy|vacuum|analyze|"
    r"call|do|execute|prepare|listen|notify|set|reset|lock|into|dblink\w*|lo_\w+|pg_\w+)\b",
    re.IGNORECASE,
)
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.\"]*)", re.IGNORECASE)
_CTE_NAME = re.compile(r"(?:\bwith|,)\s*([a-zA-Z_]\w*)\s+as\s*\(", re.IGNORECASE)
# FROM inside these function calls is syntax, not a table reference.
_FROM_FUNCS = re.compile(r"\b(extract|substring|trim|overlay|position)\s*\([^()]*\)", re.IGNORECASE)


def validate_sql(query: str, allowed_table: str) -> None:
    """Defence in depth on top of the READ ONLY transaction in run_sql."""
    if not query:
        raise NLQueryError("The model did not return a query.")
    if ";" in query:
        raise NLQueryError("Only a single SQL statement is allowed.")
    if not re.match(r"^\s*(select|with)\b", query, re.IGNORECASE):
        raise NLQueryError("Only SELECT queries are allowed.")
    # String literals can legitimately contain words like "set"; check keywords outside them.
    code = re.sub(r"'(?:[^']|'')*'", "''", query)
    bad = _FORBIDDEN.search(code)
    if bad:
        raise NLQueryError(f"Query uses a disallowed keyword: {bad.group(0)!r}.")
    code = _FROM_FUNCS.sub("fn()", code)
    allowed = {allowed_table.lower()} | {n.lower() for n in _CTE_NAME.findall(code)}
    for ref in _TABLE_REF.findall(code):
        name = ref.replace('"', "").split(".")[-1].lower()
        if ref.lower().startswith("public.") or "." not in ref:
            if name in allowed:
                continue
        raise NLQueryError(f"Query references a table other than {allowed_table}: {ref}.")


def run_sql(query: str) -> tuple[list[str], list[list], bool]:
    """-> (columns, rows, truncated). Rows are capped at MAX_ROWS."""
    conn = get_connection()
    try:
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute(pgsql.SQL("SET LOCAL statement_timeout = {}").format(pgsql.Literal(SQL_TIMEOUT_MS)))
        cur.execute(f"SELECT * FROM ({query}) AS nl_q LIMIT {MAX_ROWS + 1}")
        columns = [d.name for d in cur.description]
        rows = [[_jsonable(v) for v in r] for r in cur.fetchall()]
        conn.rollback()
        return columns, rows[:MAX_ROWS], len(rows) > MAX_ROWS
    finally:
        conn.close()


def _jsonable(value):
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    try:
        return float(value)  # Decimal
    except (TypeError, ValueError):
        return str(value)  # date, etc.


# --------------------------------------------------------------------------- entry point

def answer_question(question: str, dataset_id: int | None = None) -> dict:
    timings: dict[str, float] = {}
    t0 = time.perf_counter()

    ranked = rank_datasets(question)
    timings["dataset_selection_s"] = round(time.perf_counter() - t0, 2)
    if dataset_id is not None:
        ranked = [(s, d) for s, d in ranked if d.dataset_id == dataset_id]
        if not ranked:
            raise NLQueryError(f"Dataset {dataset_id} is not a queryable tabular dataset.")
    elif not ranked or ranked[0][0] < DATASET_MATCH_THRESHOLD:
        return {"question": question, "answered": False,
                "message": "I couldn't find a registered dataset relevant to this question.",
                "candidates": [_ds_summary(d, s) for s, d in ranked[:3]], "timings": timings}
    score, ds = ranked[0]

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"SCHEMA:\n{_schema_block(ds, _sample_rows(ds), _value_hints(ds, question))}\n\nQUESTION: {question}"},
    ]
    attempts = []
    for attempt in range(2):
        t = time.perf_counter()
        raw = _chat(messages)
        timings[f"llm_attempt_{attempt + 1}_s"] = round(time.perf_counter() - t, 2)
        interpretation, query = _split_llm_output(raw)
        try:
            validate_sql(query, ds.table)
            t = time.perf_counter()
            columns, rows, truncated = run_sql(query)
            timings["sql_s"] = round(time.perf_counter() - t, 2)
            break
        except Exception as exc:  # validation or Postgres error -> one repair attempt
            error = str(exc).strip().splitlines()[0]
            attempts.append({"sql": query, "error": error})
            if attempt == 1:
                raise NLQueryError(f"Could not produce a working query: {error}") from exc
            messages += [
                {"role": "assistant", "content": raw},
                {"role": "user", "content": f"That query failed with: {error}\nReturn a corrected query in the same format."},
            ]

    timings["total_s"] = round(time.perf_counter() - t0, 2)
    return {
        "question": question,
        "answered": True,
        "interpretation": interpretation,
        "dataset": _ds_summary(ds, score),
        "sql": query,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "failed_attempts": attempts,
        "other_candidates": [_ds_summary(d, s) for s, d in ranked[1:3]],
        "timings": timings,
    }


def _ds_summary(ds: QueryableDataset, score: float) -> dict:
    return {"dataset_id": ds.dataset_id, "title": ds.title, "table": ds.table,
            "citation": ds.citation, "match_score": round(score, 3)}


if __name__ == "__main__":
    import json
    import sys

    print(json.dumps(answer_question(" ".join(sys.argv[1:])), indent=2, default=str)[:4000])
