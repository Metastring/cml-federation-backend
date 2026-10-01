"""Federation Phase 2 -- cross-node search. See ../FEDERATION_ARCHITECTURE.md
§13 for the design this implements.

Two pieces, both running identically on central and client nodes:

1. Catalog. GET /node/catalog lists this node's searchable local datasets
   (dataset_master rows with a map_layer_info table -- exactly the ones the
   local search can reach). Peers' catalogs are harvested into
   federated_dataset_cache (metadata only) and refreshed when older than
   FEDERATION_CATALOG_TTL_SECONDS, or on POST /federation/harvest.

2. Fan-out. A search request with scope="federation" (the default) is split
   by owning node: datasets this node holds go to the route's existing local
   handler; each peer gets one POST to the same route containing only its
   datasets, with scope="local" and the hop header so it never forwards
   again. Results are merged, tagged with origin_node, and a per-node status
   block reports ok / timeout / error / skipped.

Peers: every server is an equal member of one registry (see
node_federation_service). Each gets the member list the same way -- from its
own table on the registry host, from GET {REGISTRY_URL}/nodes elsewhere, or
the last cached copy while the registry is down -- and fans out to every
'active' member except itself. A server with no registry configured is
standalone and searches only itself. Node-to-node calls are unauthenticated
for now (search is a public read); they identify themselves with
X-CML-Node-Id.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from datetime import date, datetime, timezone

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse
from psycopg2.extras import Json, RealDictCursor

from app import node_federation_service as fed
from app.db import get_connection

HOP_HEADER = "X-CML-Federation-Hop"
NODE_ID_HEADER = "X-CML-Node-Id"

SEARCH_TIMEOUT = float(os.getenv("FEDERATION_SEARCH_TIMEOUT_SECONDS", "8"))
CATALOG_TIMEOUT = float(os.getenv("FEDERATION_CATALOG_TIMEOUT_SECONDS", "5"))
CATALOG_TTL = int(os.getenv("FEDERATION_CATALOG_TTL_SECONDS", "600"))
MAP_DB_SCHEMA = os.getenv("MAP_DB_SCHEMA", "public")

def _norm(title: str) -> str:
    return " ".join((title or "").lower().split())


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _node_key(nodes: dict, name: str, base_url: str) -> str:
    """Status-map key for a node: its name, or "name (base_url)" when two
    nodes share a name, so neither entry overwrites the other."""
    return name if name not in nodes else f"{name} ({base_url})"


_REGISTRY_PLACEHOLDER = "registry"


def _display_name(peer: dict, remote_rows: list[dict]) -> str:
    """Registry name for a peer; for the placeholder used when the registry
    host hasn't registered itself as a member, the name its catalog gave."""
    if peer["name"] != _REGISTRY_PLACEHOLDER:
        return peer["name"]
    return next((r["origin_node_name"] for r in remote_rows if r["origin_base_url"] == peer["base_url"]), peer["name"])


def result_fields(results: dict) -> list[str]:
    """Top-level column list for a search response: union of every dataset's
    per-dataset "fields", in first-seen order. The frontend builds its table
    columns from this and reads each row's value by column name."""
    fields: list[str] = []
    for pdata in results.values():
        for name in pdata.get("fields") or []:
            if name not in fields:
                fields.append(name)
    return fields


def self_info() -> dict:
    return {
        "node_id": fed.NODE_ID or None,
        "name": fed.NODE_NAME,
        "role": "registry host" if fed.is_registry() else "member",
        "base_url": fed.NODE_BASE_URL,
    }


# ---------------------------------------------------------------------------
# Local catalog
# ---------------------------------------------------------------------------


def local_catalog_datasets() -> list[dict]:
    """This node's searchable datasets: active dataset_master rows linked to a
    table via map_layer_info. External PARTICIPANTS APIs are searchable only
    on the node that configures them, so they're not advertised."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            f"""
            SELECT DISTINCT ON (d.dataset_id)
                   d.dataset_id, d.title, c.category_name AS category,
                   d.description, d.keywords, m.geoserver_name
            FROM {MAP_DB_SCHEMA}.dataset_master d
            JOIN {MAP_DB_SCHEMA}.map_layer_info m ON m.dataset_id = d.dataset_id
            JOIN {MAP_DB_SCHEMA}.category_master c ON c.category_id = d.category_id
            WHERE d.is_active IS TRUE AND d.title IS NOT NULL
            ORDER BY d.dataset_id, m.created_at DESC
            """
        )
        datasets = cur.fetchall()

        cur.execute(
            f"""
            SELECT dataset_id, field_name, ontology_mapping, ontology_mapping_to_display,
                   data_type, ontology_graph_key,
                   to_jsonb(dm) ->> 'sample_value' AS sample_value,
                   to_jsonb(dm) ->> 'value_range' AS value_range,
                   to_jsonb(dm) ->> 'ontology_uri' AS ontology_uri,
                   to_jsonb(dm) -> 'metadata' AS metadata
            FROM {MAP_DB_SCHEMA}.dataset_mapping dm
            ORDER BY dataset_mapping_id
            """
        )
        fields_by_dataset: dict[int, list] = {}
        for row in cur.fetchall():
            fields_by_dataset.setdefault(row["dataset_id"], []).append(
                {k: row[k] for k in ("field_name", "ontology_mapping", "ontology_mapping_to_display",
                                     "data_type", "ontology_graph_key", "sample_value", "value_range",
                                     "ontology_uri", "metadata")}
            )

        result = []
        for d in datasets:
            geoserver_name = (d["geoserver_name"] or "").strip()
            table = geoserver_name.split(":", 1)[1] if ":" in geoserver_name else geoserver_name
            row_count = date_min = date_max = None
            cur.execute("SELECT to_regclass(%s) AS t", (f'{MAP_DB_SCHEMA}."{table}"',))
            if table and cur.fetchone()["t"]:
                cur.execute(
                    "SELECT GREATEST(c.reltuples, 0)::bigint AS n FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s",
                    (MAP_DB_SCHEMA, table),
                )
                row_count = cur.fetchone()["n"]
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                      AND data_type IN ('date', 'timestamp without time zone', 'timestamp with time zone')
                    ORDER BY (column_name = 'date') DESC, ordinal_position LIMIT 1
                    """,
                    (MAP_DB_SCHEMA, table),
                )
                date_col = cur.fetchone()
                if date_col:
                    col = date_col["column_name"]
                    cur.execute(f'SELECT MIN("{col}")::date AS lo, MAX("{col}")::date AS hi FROM {MAP_DB_SCHEMA}."{table}"')
                    span = cur.fetchone()
                    date_min, date_max = span["lo"], span["hi"]
            result.append(
                {
                    "dataset_id": d["dataset_id"],
                    "title": d["title"],
                    "category": d["category"],
                    "description": d["description"],
                    "keywords": d["keywords"],
                    "fields": fields_by_dataset.get(d["dataset_id"], []),
                    "row_count": row_count,
                    "date_min": date_min,
                    "date_max": date_max,
                }
            )
        return result
    finally:
        conn.close()


def node_catalog() -> dict:
    """GET /node/catalog payload. catalog_hash lets harvesters skip rewriting
    an unchanged catalog."""
    datasets = local_catalog_datasets()
    catalog_hash = hashlib.sha1(json.dumps(datasets, sort_keys=True, default=_json_default).encode()).hexdigest()
    return {
        "node": self_info(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "catalog_hash": catalog_hash,
        "dataset_count": len(datasets),
        "datasets": json.loads(json.dumps(datasets, default=_json_default)),
    }


# ---------------------------------------------------------------------------
# Peers
# ---------------------------------------------------------------------------


def _cached_origins() -> list[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT DISTINCT ON (origin_base_url) origin_node_id, origin_node_name, origin_base_url "
            "FROM federated_dataset_cache ORDER BY origin_base_url, harvested_at DESC"
        )
        return [
            {"node_id": r["origin_node_id"], "name": r["origin_node_name"] or r["origin_base_url"],
             "base_url": r["origin_base_url"]}
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def _is_self(member: dict) -> bool:
    return (member["base_url"].rstrip("/") == fed.NODE_BASE_URL.rstrip("/")
            or (bool(fed.MEMBER_ID) and str(member.get("node_id")) == fed.MEMBER_ID))


async def list_peers(client: httpx.AsyncClient) -> tuple[list[dict], dict]:
    """(peers to search, {name: status} for members deliberately not searched)."""
    own_url = fed.NODE_BASE_URL.rstrip("/")
    members, source = await fed.directory(True, client)
    if source == "standalone":
        return [], {}
    if source == "unavailable":
        # Registry down and never reached since startup: the peers whose
        # catalogs were harvested earlier are the best guess.
        return [o for o in await asyncio.to_thread(_cached_origins) if o["base_url"] != own_url], {}

    peers, skipped = [], {}
    for n in members:
        if _is_self(n):
            continue
        # The registry just answered us, so it's up even if its own
        # heartbeat row has lapsed.
        live_registry = source == "registry" and n["base_url"].rstrip("/") == fed.REGISTRY_URL
        if n["status"] == "active" or (live_registry and n["status"] == "stale"):
            peers.append({"node_id": n["node_id"], "name": n["name"], "base_url": n["base_url"].rstrip("/")})
        elif n["status"] != "revoked":
            skipped[n["name"]] = {"status": f"skipped_{n['status']}", "base_url": n["base_url"]}

    # Until the registry host registers itself as a member, still search it.
    known = {p["base_url"] for p in peers} | {s["base_url"].rstrip("/") for s in skipped.values()}
    if fed.REGISTRY_URL and fed.REGISTRY_URL != own_url and fed.REGISTRY_URL not in known:
        peers.insert(0, {"node_id": None, "name": _REGISTRY_PLACEHOLDER, "base_url": fed.REGISTRY_URL})
    return peers, skipped


async def find_member(node_id: str) -> dict:
    """One registry row by id, from the same directory every server sees."""
    members, _ = await fed.directory(True)
    for n in members:
        if str(n["node_id"]) == node_id:
            return n
    raise fed.NodeNotFoundError(f"Node {node_id} not found")


# ---------------------------------------------------------------------------
# Catalog harvest
# ---------------------------------------------------------------------------


def _cache_ages() -> dict[str, float]:
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT origin_base_url, EXTRACT(EPOCH FROM now() - MAX(harvested_at)) AS age "
            "FROM federated_dataset_cache GROUP BY origin_base_url"
        )
        return {r["origin_base_url"]: float(r["age"]) for r in cur.fetchall()}
    finally:
        conn.close()


def _store_catalog(peer: dict, catalog: dict) -> str:
    node = catalog.get("node") or {}
    origin_id = node.get("node_id") or peer.get("node_id") or peer["base_url"]
    origin_name = node.get("name") or peer["name"]
    base_url = peer["base_url"]
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT catalog_hash FROM federated_dataset_cache WHERE origin_base_url = %s LIMIT 1", (base_url,)
        )
        row = cur.fetchone()
        if row and row[0] == catalog.get("catalog_hash"):
            cur.execute(
                "UPDATE federated_dataset_cache SET harvested_at = now(), origin_node_name = %s, origin_node_id = %s "
                "WHERE origin_base_url = %s",
                (origin_name, origin_id, base_url),
            )
            conn.commit()
            return "unchanged"
        cur.execute("DELETE FROM federated_dataset_cache WHERE origin_base_url = %s", (base_url,))
        for d in catalog.get("datasets", []):
            cur.execute(
                """
                INSERT INTO federated_dataset_cache
                    (origin_node_id, origin_node_name, origin_base_url, remote_dataset_id, title, category,
                     description, keywords, fields, row_count, date_min, date_max, catalog_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (origin_id, origin_name, base_url, d["dataset_id"], d["title"], d.get("category"),
                 d.get("description"), d.get("keywords"), Json(d.get("fields") or []), d.get("row_count"),
                 d.get("date_min"), d.get("date_max"), catalog.get("catalog_hash")),
            )
        conn.commit()
        return "updated"
    finally:
        conn.close()


async def refresh_catalogs(client: httpx.AsyncClient, peers: list[dict], force: bool = False) -> dict[str, dict]:
    """Re-harvest each peer whose cached catalog is missing or older than the
    TTL (every peer when force=True). A failed fetch keeps the old rows."""
    ages = await asyncio.to_thread(_cache_ages)

    async def one(peer: dict) -> tuple[str, dict]:
        age = ages.get(peer["base_url"])
        if not force and age is not None and age < CATALOG_TTL:
            return peer["base_url"], {"catalog": "cached", "catalog_age_s": round(age)}
        try:
            resp = await client.get(f"{peer['base_url']}/node/catalog", timeout=CATALOG_TIMEOUT)
            resp.raise_for_status()
            result = await asyncio.to_thread(_store_catalog, peer, resp.json())
            return peer["base_url"], {"catalog": result}
        except Exception as exc:
            return peer["base_url"], {
                "catalog": "unavailable" if age is None else "stale_cache",
                "catalog_error": f"{type(exc).__name__}: {exc}"[:200],
            }

    return dict(await asyncio.gather(*(one(p) for p in peers)))


def _cached_datasets(base_urls: list[str]) -> list[dict]:
    if not base_urls:
        return []
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM federated_dataset_cache WHERE origin_base_url = ANY(%s) ORDER BY origin_base_url, title",
            (base_urls,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def _cached_row_to_dataset(r: dict) -> dict:
    return {
        "dataset_id": r["remote_dataset_id"], "title": r["title"], "category": r["category"],
        "description": r["description"], "keywords": r["keywords"], "fields": r["fields"],
        "row_count": r["row_count"],
        "date_min": r["date_min"].isoformat() if r["date_min"] else None,
        "date_max": r["date_max"].isoformat() if r["date_max"] else None,
    }


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def node_datasets(node_id: str) -> dict:
    """GET /nodes/{node_id}/datasets: one node's catalog for the registry
    page's "View" button. Fetched live from the node (and stored into the
    harvest cache); if the node is unreachable, the last harvested copy.
    node_id="self" (or a registry row pointing back at this server) returns
    this node's own catalog. Revoked nodes are never contacted."""
    own_url = fed.NODE_BASE_URL.rstrip("/")

    def payload(node: dict, source: str, harvested_at: str | None, datasets: list[dict]) -> dict:
        return {"node": node, "datasets_source": source, "harvested_at": harvested_at,
                "dataset_count": len(datasets), "datasets": datasets}

    if node_id == "self":
        me = self_info()
        node = {"node_id": me["node_id"], "name": me["name"], "base_url": me["base_url"],
                "status": "self", "is_self": True}
    else:
        row = await find_member(node_id)
        base_url = row["base_url"].rstrip("/")
        node = {"node_id": row["node_id"], "name": row["name"], "base_url": row["base_url"],
                "status": row["status"], "is_self": _is_self(row)}

    if node["is_self"]:
        local = await asyncio.to_thread(local_catalog_datasets)
        return payload(node, "live", None, json.loads(json.dumps(local, default=_json_default)))
    if node["status"] == "revoked":
        return payload(node, "unavailable", None, [])

    peer = {"node_id": node["node_id"], "name": node["name"], "base_url": base_url}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{base_url}/node/catalog", timeout=CATALOG_TIMEOUT)
            resp.raise_for_status()
            catalog = resp.json()
        await asyncio.to_thread(_store_catalog, peer, catalog)
        return payload(node, "live", _utc_iso(datetime.now(timezone.utc)), catalog.get("datasets") or [])
    except Exception:
        rows = await asyncio.to_thread(_cached_datasets, [base_url])
        if not rows:
            return payload(node, "unavailable", None, [])
        harvested_at = _utc_iso(max(r["harvested_at"] for r in rows))
        return payload(node, "cache", harvested_at, [_cached_row_to_dataset(r) for r in rows])


async def remote_catalogs(force: bool = False) -> tuple[list[dict], dict, dict, list[dict]]:
    """(peers, skipped, catalog statuses, cached dataset rows) for every
    reachable peer, refreshing stale catalogs first."""
    async with httpx.AsyncClient() as client:
        peers, skipped = await list_peers(client)
        statuses = await refresh_catalogs(client, peers, force=force)
    rows = await asyncio.to_thread(_cached_datasets, [p["base_url"] for p in peers])
    return peers, skipped, statuses, rows


def split_node_suffix(title: str, node_names: set[str]) -> tuple[str, str | None]:
    """"<title> @ <node name>" -> (title, node name). Listings and search
    results use that form when two servers hold datasets with the same
    title; anything else is returned unchanged with node None."""
    if " @ " in title:
        base, node = title.rsplit(" @ ", 1)
        if node in node_names:
            return base, node
    return title, None


async def remote_metadata(title: str, category_name: str) -> dict | None:
    """GET /metadata for a dataset another server holds: found through the
    harvested catalogs and fetched from its owner. None if no peer has it."""
    _, _, _, rows = await remote_catalogs()
    node_names = {r["origin_node_name"] for r in rows if r["origin_node_name"]}
    base_title, node = split_node_suffix(title, node_names)
    candidates = [
        r for r in rows
        if _norm(r["title"]) == _norm(base_title)
        and (r["category"] or "").lower() == category_name.lower()
        and (node is None or r["origin_node_name"] == node)
    ]
    me = self_info()
    async with httpx.AsyncClient() as client:
        for r in candidates:
            try:
                resp = await client.get(
                    f"{r['origin_base_url']}/metadata",
                    params={"title": r["title"], "category_name": r["category"]},
                    headers={HOP_HEADER: "1", NODE_ID_HEADER: me["node_id"] or me["name"]},
                    timeout=SEARCH_TIMEOUT,
                )
            except httpx.HTTPError:
                continue
            if resp.status_code == 200:
                origin = {"node_id": r["origin_node_id"], "name": r["origin_node_name"], "base_url": r["origin_base_url"]}
                return {**resp.json(), "dataset_title": title, "origin_node": origin}
    return None


async def federation_catalog(category: str | None, term: str | None, q: str | None, force: bool) -> dict:
    """GET /federation/catalog: this node's datasets + every reachable peer's,
    each tagged with origin_node. `term` matches a field's ontology term IRI
    (the "similar datasets" lookup)."""
    peers, skipped, statuses, rows = await remote_catalogs(force=force)
    local = await asyncio.to_thread(local_catalog_datasets)

    me = self_info()
    datasets = [{**json.loads(json.dumps(d, default=_json_default)), "origin_node": me} for d in local]
    for r in rows:
        datasets.append(
            {
                **_cached_row_to_dataset(r),
                "origin_node": {"node_id": r["origin_node_id"], "name": r["origin_node_name"],
                                "base_url": r["origin_base_url"]},
                "harvested_at": r["harvested_at"].isoformat(),
            }
        )

    if category:
        datasets = [d for d in datasets if (d["category"] or "").lower() == category.lower()]
    if term:
        datasets = [d for d in datasets if any(f.get("ontology_mapping") == term for f in d["fields"] or [])]
    if q:
        needle = q.lower()
        datasets = [d for d in datasets if needle in f"{d['title']} {d.get('description') or ''} {d.get('keywords') or ''}".lower()]

    nodes = {me["name"]: {"status": "self", "base_url": me["base_url"]}}
    for p in peers:
        nodes[_node_key(nodes, _display_name(p, rows), p["base_url"])] = {
            "status": "peer", "base_url": p["base_url"], **statuses.get(p["base_url"], {})
        }
    for name, status in skipped.items():
        nodes[_node_key(nodes, name, status.get("base_url", ""))] = status
    return {"count": len(datasets), "nodes": nodes, "datasets": datasets}


async def harvest_all(force: bool = True) -> dict:
    async with httpx.AsyncClient() as client:
        peers, skipped = await list_peers(client)
        statuses = await refresh_catalogs(client, peers, force=force)
    return {
        "peers": {p["name"]: {"base_url": p["base_url"], **statuses.get(p["base_url"], {})} for p in peers},
        "skipped": skipped,
    }


# ---------------------------------------------------------------------------
# Search fan-out
# ---------------------------------------------------------------------------


async def _route_datasets(client: httpx.AsyncClient, payload):
    """Split payload.dataset between this node and the peers that own each
    title ("<title> @ <node>" pins one node; no datasets = every dataset in
    the requested categories). Returns (peers, skipped, catalog_status,
    remote_rows, node_names, local_titles, titles_by_peer)."""
    requested_categories = {c.strip().lower() for c in payload.category}
    me = self_info()
    peers, skipped = await list_peers(client)
    catalog_status = await refresh_catalogs(client, peers)
    remote_rows = await asyncio.to_thread(_cached_datasets, [p["base_url"] for p in peers])
    local_datasets = await asyncio.to_thread(local_catalog_datasets)

    local_titles_known = {_norm(d["title"]) for d in local_datasets}
    remote_by_title: dict[str, list[dict]] = {}
    for r in remote_rows:
        remote_by_title.setdefault(_norm(r["title"]), []).append(r)

    node_names = {me["name"]} | {r["origin_node_name"] for r in remote_rows if r["origin_node_name"]}
    if payload.dataset:
        local_titles, remote_pairs = [], []
        for t in payload.dataset:
            title, node = split_node_suffix(t, node_names)
            if node == me["name"]:
                local_titles.append(title)
            elif node:
                remote_pairs += [(r, title) for r in remote_by_title.get(_norm(title), [])
                                 if r["origin_node_name"] == node]
            else:
                if _norm(t) in local_titles_known or _norm(t) not in remote_by_title:
                    local_titles.append(t)
                remote_pairs += [(r, t) for r in remote_by_title.get(_norm(t), [])]
    else:
        # No datasets named: search everything in the requested categories.
        local_titles = [d["title"] for d in local_datasets if (d["category"] or "").lower() in requested_categories]
        remote_pairs = [(r, r["title"]) for r in remote_rows if (r["category"] or "").lower() in requested_categories]

    titles_by_peer: dict[str, list[str]] = {}
    for r, title in remote_pairs:
        titles_by_peer.setdefault(r["origin_base_url"], [])
        if title not in titles_by_peer[r["origin_base_url"]]:
            titles_by_peer[r["origin_base_url"]].append(title)
    return peers, skipped, catalog_status, remote_rows, node_names, local_titles, titles_by_peer


async def federate(request: Request, route_path: str, payload, local_handler):
    """Run `local_handler` for this node's share of the request and forward
    the rest to the owning peers, then merge. scope="local", or a request
    that already came from another node, runs only locally."""
    if payload.scope == "local" or request.headers.get(HOP_HEADER):
        return await local_handler(payload)

    me = self_info()
    nodes: dict[str, dict] = {}

    async with httpx.AsyncClient() as client:
        peers, skipped, catalog_status, remote_rows, node_names, local_titles, titles_by_peer = \
            await _route_datasets(client, payload)

        async def run_local():
            started = time.monotonic()
            if not local_titles:
                return None, {"status": "no_matching_datasets"}
            try:
                resp = await local_handler(payload.model_copy(update={"dataset": local_titles, "scope": "local"}))
                if isinstance(resp, dict):
                    return resp, {"status": "ok", "took_ms": round((time.monotonic() - started) * 1000), "datasets": local_titles}
                return None, {"status": "error", "error": "unexpected response type", "datasets": local_titles}
            except HTTPException as exc:
                return None, {"status": "no_matching_datasets" if exc.status_code == 400 else "error",
                              "error": str(exc.detail)[:300], "datasets": local_titles}

        async def run_remote(peer: dict):
            titles = titles_by_peer.get(peer["base_url"], [])
            status = {"base_url": peer["base_url"], **catalog_status.get(peer["base_url"], {})}
            if not titles:
                return peer, None, {**status, "status": "no_matching_datasets"}
            body = payload.model_dump(mode="json")
            body.update(dataset=titles, scope="local")
            started = time.monotonic()
            try:
                resp = await client.post(
                    f"{peer['base_url']}{route_path}", json=body, timeout=SEARCH_TIMEOUT,
                    headers={HOP_HEADER: "1", NODE_ID_HEADER: me["node_id"] or me["name"]},
                )
                took = round((time.monotonic() - started) * 1000)
                if resp.status_code == 200:
                    return peer, resp.json(), {**status, "status": "ok", "took_ms": took, "datasets": titles}
                return peer, None, {**status, "status": "error", "took_ms": took, "datasets": titles,
                                    "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
            except httpx.TimeoutException:
                return peer, None, {**status, "status": "timeout", "datasets": titles}
            except Exception as exc:
                return peer, None, {**status, "status": "unreachable", "datasets": titles,
                                    "error": f"{type(exc).__name__}: {exc}"[:300]}

        (local_resp, local_status), *remote_results = await asyncio.gather(
            run_local(), *(run_remote(p) for p in peers)
        )

    merged = dict(local_resp) if local_resp else {
        "category": payload.category, "dataset": payload.dataset, "valid_datasets": [],
        "invalid_datasets": [], "search_text": payload.search_text, "results": {},
    }
    merged["dataset"] = payload.dataset
    merged["results"] = {}
    merged["valid_datasets"] = []
    found_titles: set[str] = set()

    def absorb(resp: dict, origin: dict) -> None:
        for name, pdata in (resp.get("results") or {}).items():
            key = name if name not in merged["results"] else f"{name} @ {origin['name']}"
            merged["results"][key] = {**pdata, "origin_node": origin}
        for name in resp.get("valid_datasets") or []:
            found_titles.add(_norm(name))
            key = name if name not in merged["valid_datasets"] else f"{name} @ {origin['name']}"
            merged["valid_datasets"].append(key)

    nodes[me["name"]] = {"base_url": me["base_url"], "self": True, **local_status}
    if local_resp:
        absorb(local_resp, me)
    for peer, resp, status in remote_results:
        origin = {"node_id": peer.get("node_id"), "name": _display_name(peer, remote_rows), "base_url": peer["base_url"]}
        nodes[_node_key(nodes, origin["name"], peer["base_url"])] = status
        if resp:
            absorb(resp, origin)
    for name, status in skipped.items():
        nodes[_node_key(nodes, name, status.get("base_url", ""))] = status

    merged["invalid_datasets"] = [
        t for t in payload.dataset if _norm(split_node_suffix(t, node_names)[0]) not in found_titles
    ]
    merged["fields"] = result_fields(merged["results"])
    merged["scope"] = "federation"
    merged["nodes"] = nodes

    if not merged["valid_datasets"]:
        raise HTTPException(
            status_code=400,
            detail={"message": "No valid datasets provided.", "invalid_datasets": merged["invalid_datasets"], "nodes": nodes},
        )
    return merged


PRESEARCH_TIMEOUT = float(os.getenv("FEDERATION_PRESEARCH_TIMEOUT_SECONDS", "30"))


async def federate_stream(request: Request, route_path: str, payload, local_stream):
    """federate() for the SSE availability check (/pre-federated-search):
    stream this node's events and every owning peer's as they arrive, each
    tagged with origin_node, then one merged "done" event. `await local_stream(payload)`
    gives the local StreamingResponse; scope="local" or a forwarded request
    gets it unchanged."""
    if payload.scope == "local" or request.headers.get(HOP_HEADER):
        return await local_stream(payload)

    async def events():
        me = self_info()
        nodes: dict[str, dict] = {}
        queue: asyncio.Queue = asyncio.Queue()
        async with httpx.AsyncClient() as client:
            peers, skipped, catalog_status, remote_rows, node_names, local_titles, titles_by_peer = \
                await _route_datasets(client, payload)

            async def relay(origin: dict, lines, key: str, status: dict):
                # Forward data events; the source's own "done" is replaced by ours.
                started, event, count = time.monotonic(), None, 0
                try:
                    async for line in lines:
                        if line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:") and event != "done":
                            await queue.put((origin, json.loads(line[5:])))
                            count += 1
                        elif not line.strip():
                            event = None
                    nodes[key] = {**status, "status": "ok", "hits": count,
                                  "took_ms": round((time.monotonic() - started) * 1000)}
                except httpx.TimeoutException:
                    nodes[key] = {**status, "status": "timeout", "hits": count}
                except Exception as exc:
                    nodes[key] = {**status, "status": "error", "hits": count, "error": f"{type(exc).__name__}: {exc}"[:300]}

            async def run_local():
                status = {"base_url": me["base_url"], "self": True, "datasets": local_titles}
                if not local_titles:
                    nodes[me["name"]] = {**status, "status": "no_matching_datasets"}
                    return
                try:
                    resp = await local_stream(payload.model_copy(update={"dataset": local_titles, "scope": "local"}))
                except HTTPException as exc:
                    nodes[me["name"]] = {**status, "status": "no_matching_datasets" if exc.status_code == 400 else "error",
                                         "error": str(exc.detail)[:300]}
                    return

                async def lines():
                    async for chunk in resp.body_iterator:
                        text = chunk.decode() if isinstance(chunk, bytes) else chunk
                        for line in text.split("\n"):
                            yield line
                await relay(me, lines(), me["name"], status)

            async def run_remote(peer: dict):
                titles = titles_by_peer.get(peer["base_url"], [])
                origin = {"node_id": peer.get("node_id"), "name": _display_name(peer, remote_rows), "base_url": peer["base_url"]}
                key = _node_key(nodes, origin["name"], peer["base_url"])
                status = {"base_url": peer["base_url"], **catalog_status.get(peer["base_url"], {}), "datasets": titles}
                if not titles:
                    nodes[key] = {**status, "status": "no_matching_datasets"}
                    return
                body = payload.model_dump(mode="json")
                body.update(dataset=titles, scope="local")
                try:
                    async with client.stream(
                        "POST", f"{peer['base_url']}{route_path}", json=body,
                        timeout=httpx.Timeout(PRESEARCH_TIMEOUT, connect=CATALOG_TIMEOUT),
                        headers={HOP_HEADER: "1", NODE_ID_HEADER: me["node_id"] or me["name"],
                                 "Accept": "text/event-stream"},
                    ) as resp:
                        if resp.status_code != 200:
                            nodes[key] = {**status, "status": "error", "error": f"HTTP {resp.status_code}: {(await resp.aread())[:300]!r}"}
                            return
                        await relay(origin, resp.aiter_lines(), key, status)
                except httpx.TimeoutException:
                    nodes.setdefault(key, {**status, "status": "timeout"})
                except Exception as exc:
                    nodes.setdefault(key, {**status, "status": "unreachable", "error": f"{type(exc).__name__}: {exc}"[:300]})

            async def run_all():
                try:
                    await asyncio.gather(run_local(), *(run_remote(p) for p in peers))
                finally:
                    await queue.put(None)

            task = asyncio.create_task(run_all())
            seen: set[str] = set()
            total = 0
            try:
                while (item := await queue.get()) is not None:
                    origin, entry = item
                    name = entry.get("dataset_name") or ""
                    if name in seen:  # same title on two nodes -> name the second by its node
                        entry["dataset_name"] = name = f"{name} @ {origin['name']}"
                    seen.add(name)
                    entry["origin_node"] = origin
                    total += 1
                    yield f"data: {json.dumps(entry, default=_json_default)}\n\n"
            finally:
                await task
        for name, status in skipped.items():
            nodes[_node_key(nodes, name, status.get("base_url", ""))] = status
        done = {"search_text": payload.search_text.strip(), "total": total, "cached": False,
                "scope": "federation", "nodes": nodes}
        yield f"event: done\ndata: {json.dumps(done, default=_json_default)}\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")
