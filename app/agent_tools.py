"""Tools the CPHR-92 agent (app/agent.py) can call, one function each.

Every tool takes plain JSON-able kwargs (what the LLM produced) and returns a
short text observation for the LLM plus a structured `data` payload for the
/ask client. Observations are deliberately compact: on this CPU-only host the
LLM reads prompt tokens at ~18 tok/s, so every row echoed back costs seconds.

Tools that wrap an existing HTTP API (federated / spatial / metadata search)
call it over HTTP rather than importing its handler, so they behave exactly
like the public endpoint (including federation across peer nodes).
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Callable

import httpx
import numpy as np

from psycopg2 import sql as pgsql

from app import nl_query
from app.db import get_connection
from app.retrieval_index import search as index_search

BACKEND_URL = os.getenv("AGENT_BACKEND_URL", "http://127.0.0.1:8000")
MAP_MODULE_URL = os.getenv("AGENT_MAP_MODULE_URL", "http://127.0.0.1:8001")
FUSEKI_SPARQL_ENDPOINT = os.getenv("FUSEKI_SPARQL_ENDPOINT", "http://localhost:3030/cml-ontology/query")
HTTP_TIMEOUT_S = float(os.getenv("AGENT_HTTP_TIMEOUT_S", "30"))
SPARQL_MAX_ROWS = 25
OBS_MAX_ROWS = 10  # rows shown to the LLM per tool result; the client gets them all

# Below this a schema/dataset hit is too weak to show the LLM at all
# (retrieval_index.NO_MATCH_THRESHOLD is 0.3; keep a little slack under it
# because MiniLM scores short paraphrases like "heavy rain" ~0.3).
MIN_SCORE = 0.2


class ToolError(Exception):
    """Bad arguments or a failing backend; the message goes back to the LLM."""


@dataclass
class ToolResult:
    observation: str
    data: dict = field(default_factory=dict)
    citations: list[dict] = field(default_factory=list)  # {dataset_id, title, fields, row_count}


@dataclass
class Tool:
    name: str
    description: str
    args: dict[str, str]  # arg name -> description; all strings/ints from the LLM
    required: list[str]
    fn: Callable[..., ToolResult]


def _rows_block(columns: list[str], rows: list[list], limit: int = OBS_MAX_ROWS) -> str:
    """Compact pipe-separated table; cheaper in tokens than JSON."""
    lines = [" | ".join(columns)]
    lines += [" | ".join("" if v is None else str(v) for v in r) for r in rows[:limit]]
    if len(rows) > limit:
        lines.append(f"(+{len(rows) - limit} more rows not shown)")
    return "\n".join(lines)


def _http_json(method: str, url: str, **kwargs) -> dict | list:
    try:
        resp = httpx.request(method, url, timeout=HTTP_TIMEOUT_S, **kwargs)
    except httpx.HTTPError as exc:
        raise ToolError(f"{url} is not reachable ({exc.__class__.__name__}).") from exc
    if resp.status_code >= 400:
        detail = resp.text[:300]
        try:
            detail = resp.json().get("detail", detail)
        except ValueError:
            pass
        raise ToolError(f"{url} returned {resp.status_code}: {detail}")
    return resp.json()


# --------------------------------------------------------------------------- metadata_search (CPHR-102)

def _catalog() -> list[dict]:
    return _http_json("GET", f"{BACKEND_URL}/federation/catalog").get("datasets", [])


def _dataset_text(d: dict) -> str:
    fields = ", ".join(f.get("ontology_mapping_to_display") or f["field_name"] for f in d.get("fields") or [])
    return f"{d['title']}. {d.get('category') or ''}. {d.get('description') or ''} Keywords: {d.get('keywords') or ''}. Fields: {fields}"


def metadata_search(query: str, category: str | None = None) -> ToolResult:
    """Rank catalog datasets (this node + reachable peers) by meaning, not
    substring: /federation/catalog?q= only matches literal text, which misses
    "weather in Pune" -> "NASA POWER ... Daily Weather"."""
    datasets = _catalog()
    if category:
        datasets = [d for d in datasets if (d.get("category") or "").lower() == category.lower()]
    if not datasets:
        return ToolResult("No datasets are registered" + (f" in category {category!r}." if category else "."))

    vectors = nl_query._embed([_dataset_text(d) for d in datasets])
    scores = vectors @ nl_query._embed([query])[0]
    ranked = [(float(scores[i]), datasets[i]) for i in np.argsort(-scores) if scores[i] >= MIN_SCORE][:3]
    if not ranked:
        return ToolResult(f"No dataset matches {query!r}. Registered categories: "
                          + ", ".join(sorted({d.get('category') or '?' for d in datasets})) + ".")

    lines = []
    for score, d in ranked:
        node = (d.get("origin_node") or {}).get("name") or "this node"
        fields = ", ".join(f["field_name"] for f in d.get("fields") or [])
        lines.append(f"- dataset_id={d['dataset_id']} \"{d['title']}\" [{d.get('category')}] on {node} "
                     f"(match {score:.2f})\n  {(d.get('description') or '')[:120]}\n  fields: {fields}")
    return ToolResult(
        "\n".join(lines),
        data={"datasets": [{"dataset_id": d["dataset_id"], "title": d["title"], "category": d.get("category"),
                            "node": (d.get("origin_node") or {}).get("name"), "match_score": round(s, 3)}
                           for s, d in ranked]},
    )


# --------------------------------------------------------------------------- lookup_schema (CPHR-98)

def lookup_schema(query: str) -> ToolResult:
    """Which dataset fields mean what the question is about, with unit and
    value range: retrieval_index.search() over dataset_mapping."""
    hits = index_search(query, k=6, min_score=MIN_SCORE)
    if not hits:
        return ToolResult(f"No dataset field relates to {query!r}.")
    lines = []
    for score, e in hits:
        if e.kind == "dataset_field":
            extra = "".join([f", unit {e.unit}" if e.unit else "", f", range {e.value_range}" if e.value_range else ""])
            lines.append(f"- dataset_id={e.dataset_id} \"{e.dataset_title}\" field {e.field_name}{extra} (match {score:.2f})\n  {e.text}")
        else:
            lines.append(f"- [{e.kind}] {e.text[:250]} (match {score:.2f})")
    return ToolResult("\n".join(lines), data={"hits": [{"score": round(s, 3), "kind": e.kind, "dataset_id": e.dataset_id,
                                                         "dataset_title": e.dataset_title, "field_name": e.field_name,
                                                         "text": e.text} for s, e in hits]})


# --------------------------------------------------------------------------- query_data (text-to-SQL)

def query_data(question: str, dataset_id: int | str | None = None) -> ToolResult:
    """Aggregates/filters over one local tabular dataset via app/nl_query.py
    (which has its own LLM call, SQL validation and READ ONLY execution)."""
    if dataset_id in ("", None):
        dataset_id = None
    else:
        try:
            dataset_id = int(dataset_id)
        except (TypeError, ValueError) as exc:
            raise ToolError(f"dataset_id must be a number, got {dataset_id!r}.") from exc
    try:
        result = nl_query.answer_question(question, dataset_id)
    except nl_query.NLQueryError as exc:
        raise ToolError(str(exc)) from exc

    if not result["answered"]:
        names = ", ".join(f"{c['dataset_id']} {c['title']}" for c in result.get("candidates", []))
        return ToolResult(f"{result['message']} Closest datasets: {names or 'none'}.", data=result)

    ds = result["dataset"]
    obs = (f"Dataset {ds['dataset_id']} \"{ds['title']}\" (table {ds['table']}). "
           f"Interpretation: {result['interpretation']}\nSQL: {result['sql']}\n"
           f"{result['row_count']} row(s){' (truncated)' if result['truncated'] else ''}:\n"
           + _rows_block(result["columns"], result["rows"]))
    return ToolResult(obs, data=result, citations=[{
        "dataset_id": ds["dataset_id"], "title": ds["title"], "fields": result["columns"],
        "row_count": result["row_count"], "citation": ds.get("citation"), "sql": result["sql"],
    }])


# --------------------------------------------------------------------------- federated_search (CPHR-100)

def federated_search(search_text: str, dataset: str) -> ToolResult:
    """Text match (e.g. a place name) inside one dataset, across every node
    that holds it -- wraps POST /federated-search."""
    catalog = {d["title"].lower(): d for d in _catalog()}
    d = catalog.get(dataset.lower())
    if d is None:  # LLMs often send a dataset_id or a shortened title
        d = next((v for v in catalog.values() if str(v["dataset_id"]) == str(dataset)
                  or dataset.lower() in v["title"].lower()), None)
    if d is None:
        raise ToolError(f"Unknown dataset {dataset!r}. Use metadata_search to find the exact title.")

    payload = {"category": [d.get("category") or ""], "dataset": [d["title"]], "fields": [],
               "search_text": search_text, "limit": 50}
    body = _http_json("POST", f"{BACKEND_URL}/federated-search", json=payload)

    lines, citations, total = [], [], 0
    for title, res in (body.get("results") or {}).items():
        inner = ((res or {}).get("field_results") or {}).get("results") or {}
        rows = inner.get("results") or []
        if inner.get("error"):
            lines.append(f"{title}: error {inner['error']}")
            continue
        total += len(rows)
        columns = [c for c in (body.get("fields") or (list(rows[0]) if rows else [])) if c != "id"]
        if len(rows) == payload["limit"]:
            lines.append(f"{title}: showing only the first {len(rows)} matching rows; there are more. These are NOT "
                         "all the data: do not state totals, ranges or extremes from them (use query_data for that).")
        else:
            lines.append(f"{title}: {len(rows)} matching row(s)")
        if rows:
            lines.append(_rows_block(columns, [[r.get(c) for c in columns] for r in rows]))
            citations.append({"dataset_id": d["dataset_id"], "title": title, "fields": columns, "row_count": len(rows)})
    if not total and not lines:
        lines.append(f"No rows in {d['title']!r} match {search_text!r}.")
    return ToolResult("\n".join(lines), data=body, citations=citations)


# --------------------------------------------------------------------------- spatial_search (CPHR-101)

def spatial_search(scientific_name: str) -> ToolResult:
    """Where a species has been recorded: wraps the map module's
    POST /v2/spatial_search (GBIF-style occurrence records)."""
    body = _http_json("POST", f"{MAP_MODULE_URL}/v2/spatial_search", json={"search_text": scientific_name})
    rows = body.get("results") or []
    if not rows:
        return ToolResult(f"No occurrence records for {scientific_name!r}.", data=body)

    names = sorted({r.get("scientificname") or "" for r in rows} - {""})
    countries = sorted({r.get("countrycode") or "" for r in rows} - {""})
    lats = [r["latitude"] for r in rows if r.get("latitude") is not None]
    lons = [r["longitude"] for r in rows if r.get("longitude") is not None]
    dates = sorted(str(r["eventdate"])[:10] for r in rows if r.get("eventdate"))
    obs = [f"{len(rows)} occurrence record(s) for {', '.join(names[:5]) or scientific_name}."]
    if countries:
        obs.append(f"Countries: {', '.join(countries)}.")
    if lats:
        obs.append(f"Latitude {min(lats):.2f} to {max(lats):.2f}, longitude {min(lons):.2f} to {max(lons):.2f}.")
    if dates:
        obs.append(f"Observed {dates[0]} to {dates[-1]}.")
    columns = ["scientificname", "latitude", "longitude", "eventdate", "basisofrecord"]
    obs.append(_rows_block(columns, [[r.get(c) for c in columns] for r in rows], limit=5))
    return ToolResult("\n".join(obs), data={"results": rows},
                      citations=[{"dataset_id": None, "title": "Species occurrence records (map module)",
                                  "fields": columns, "row_count": len(rows)}])


# --------------------------------------------------------------------------- run_sparql (CPHR-99)

_SPARQL_FORBIDDEN = re.compile(r"\b(insert|delete|load|clear|create|drop|copy|move|add|service)\b", re.IGNORECASE)


def run_sparql(query: str) -> ToolResult:
    """Read-only SPARQL over the ontology graphs in Fuseki (definitions,
    labels, units). There is no Ontop endpoint on this host, so data rows
    come from query_data instead."""
    code = re.sub(r'"(?:[^"\\]|\\.)*"|<[^>]*>', '""', query)  # ignore keywords inside literals/IRIs
    if not re.search(r"\b(select|ask)\b", code, re.IGNORECASE):
        raise ToolError("Only SELECT or ASK queries are allowed.")
    bad = _SPARQL_FORBIDDEN.search(code)
    if bad:
        raise ToolError(f"Query uses a disallowed keyword: {bad.group(0)!r}.")
    if re.search(r"\bselect\b", code, re.IGNORECASE) and not re.search(r"\blimit\s+\d+", code, re.IGNORECASE):
        query = f"{query.rstrip()}\nLIMIT {SPARQL_MAX_ROWS}"

    try:
        resp = httpx.post(FUSEKI_SPARQL_ENDPOINT, data={"query": query, "timeout": "20000"},
                          headers={"Accept": "application/sparql-results+json"}, timeout=HTTP_TIMEOUT_S)
    except httpx.HTTPError as exc:
        raise ToolError(f"SPARQL endpoint not reachable ({exc.__class__.__name__}).") from exc
    if resp.status_code >= 400:
        raise ToolError(f"SPARQL error {resp.status_code}: {resp.text.strip()[:300]}")
    body = resp.json()
    if "boolean" in body:
        return ToolResult(f"ASK result: {body['boolean']}", data=body)

    columns = body["head"]["vars"]
    rows = [[b.get(c, {}).get("value") for c in columns] for b in body["results"]["bindings"]][:SPARQL_MAX_ROWS]
    if not rows:
        return ToolResult("Query returned no rows.", data=body)
    return ToolResult(f"{len(rows)} row(s):\n" + _rows_block(columns, rows), data={"columns": columns, "rows": rows})


# --------------------------------------------------------------------------- dataset list for the prompt

def datasets_prompt_block() -> str:
    """One line per locally queryable dataset, for the agent's system prompt.
    It lets the agent go straight to query_data for most questions instead
    of spending a metadata_search turn (~40 s on this host) finding the
    dataset. Stable between questions, so it stays in llama-server's cached
    prompt prefix; it only changes when datasets are (de)registered."""
    lines = []
    for d in nl_query.load_queryable_datasets():
        cols = ", ".join(c for c, _ in d.columns if c not in ("id", "source_dataset"))
        topics = f" (topics: {d.keywords.replace(',', ', ')})" if d.keywords else ""
        lines.append(f"- dataset_id={d.dataset_id} \"{d.title}\"{topics}{_date_coverage(d)}: columns {cols}")
    return "\n".join(lines) or "(none)"


def _date_coverage(d: nl_query.QueryableDataset) -> str:
    """" dates 2025-01-01 to 2025-12-31" when the table has a date column, so
    the model can tell historical data from what a question asks about (it
    otherwise passes 2025 weather off as "next week's forecast")."""
    date_cols = [c for c, t in d.columns if t == "date" or t.startswith("timestamp")]
    if not date_cols:
        return ""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(pgsql.SQL("SELECT MIN({c})::date, MAX({c})::date FROM {t}").format(
            c=pgsql.Identifier(date_cols[0]), t=pgsql.Identifier(d.table)))
        lo, hi = cur.fetchone()
    finally:
        conn.close()
    if lo is None:
        return ""
    return f", dates {lo}" if lo == hi else f", dates {lo} to {hi}"


# --------------------------------------------------------------------------- registry

TOOLS: dict[str, Tool] = {t.name: t for t in [
    Tool("metadata_search",
         "Find which registered datasets (on this node and peer nodes) cover a topic. Use first when unsure which dataset holds the answer.",
         {"query": "topic in a few words, e.g. 'air quality in cities'",
          "category": "optional: Climate, Environment, Energy or Health"},
         ["query"], metadata_search),
    Tool("lookup_schema",
         "Find which dataset fields measure something, with units and value ranges.",
         {"query": "the measurement in a few words, e.g. 'daily rainfall'"},
         ["query"], lookup_schema),
    Tool("query_data",
         "Answer a question that needs filtering, counting, ranking or aggregating rows of ONE local dataset (writes and runs SQL).",
         {"question": "a self-contained question in plain English (not SQL), e.g. 'districts in Kerala with max rainfall above 100 mm'",
          "dataset_id": "the dataset_id from metadata_search/lookup_schema"},
         ["question", "dataset_id"], query_data),
    Tool("federated_search",
         "Fetch the rows of one dataset that mention a name (place, crop, ...), searched across every node holding it.",
         {"search_text": "the name to match, e.g. 'Pune'", "dataset": "exact dataset title"},
         ["search_text", "dataset"], federated_search),
    Tool("spatial_search",
         "Where a plant/animal species has been observed (occurrence records with coordinates).",
         {"scientific_name": "e.g. 'Terminalia chebula'"},
         ["scientific_name"], spatial_search),
    Tool("run_sparql",
         "Read-only SPARQL over the ontology graphs (term definitions, labels, units). Terms are in named graphs: use GRAPH ?g { ... }.",
         {"query": "a SELECT or ASK query"},
         ["query"], run_sparql),
]}


def run_tool(name: str, args: dict) -> ToolResult:
    tool = TOOLS.get(name)
    if tool is None:
        raise ToolError(f"Unknown tool {name!r}. Available: {', '.join(TOOLS)}.")
    if not isinstance(args, dict):
        raise ToolError("action_input must be a JSON object.")
    missing = [a for a in tool.required if args.get(a) in (None, "")]
    if missing:
        raise ToolError(f"{name} needs: {', '.join(missing)}.")
    kwargs = {k: v for k, v in args.items() if k in tool.args}
    return tool.fn(**kwargs)


def tools_prompt_block() -> str:
    lines = []
    for t in TOOLS.values():
        args = ", ".join(f"{a}{'' if a in t.required else '?'}: {d}" for a, d in t.args.items())
        lines.append(f"- {t.name}: {t.description}\n    args: {{{args}}}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    print(run_tool(sys.argv[1], json.loads(sys.argv[2])).observation)
