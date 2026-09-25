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

Peers: central fans out to 'active' nodes in node_registry. A client node
asks central's GET /nodes for the other nodes (falling back to the peers it
has cached catalogs for when central is down) and treats central itself as
a peer. A node with no CENTRAL_SERVER_URL is standalone and searches only
itself. Node-to-node calls are unauthenticated for now (search is a public
read); they identify themselves with X-CML-Node-Id.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from datetime import date, datetime

import httpx
from fastapi import HTTPException, Request
from psycopg2.extras import Json, RealDictCursor

from app import node_federation_service as fed
from app.db import get_connection

HOP_HEADER = "X-CML-Federation-Hop"
NODE_ID_HEADER = "X-CML-Node-Id"

SEARCH_TIMEOUT = float(os.getenv("FEDERATION_SEARCH_TIMEOUT_SECONDS", "8"))
CATALOG_TIMEOUT = float(os.getenv("FEDERATION_CATALOG_TIMEOUT_SECONDS", "5"))
CATALOG_TTL = int(os.getenv("FEDERATION_CATALOG_TTL_SECONDS", "600"))
MAP_DB_SCHEMA = os.getenv("MAP_DB_SCHEMA", "public")

# Last peer list a client node got from central, reused while central is down.
_last_known_peers: list[dict] = []


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


_CENTRAL_PLACEHOLDER = "central"


def _display_name(peer: dict, remote_rows: list[dict]) -> str:
    """Registry name for a peer; for the "central" placeholder a client node
    uses (central isn't in node_registry), the name central's catalog gave."""
    if peer["name"] != _CENTRAL_PLACEHOLDER:
        return peer["name"]
    return next((r["origin_node_name"] for r in remote_rows if r["origin_base_url"] == peer["base_url"]), peer["name"])


def self_info() -> dict:
    return {
        "node_id": fed.NODE_ID or None,
        "name": fed.NODE_NAME,
        "role": fed.NODE_ROLE,
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
                   d.dataset_id, d.title, COALESCE(c.category_name, 'Biodiversity') AS category,
                   d.description, d.keywords, m.geoserver_name
            FROM {MAP_DB_SCHEMA}.dataset_master d
            JOIN {MAP_DB_SCHEMA}.map_layer_info m ON m.dataset_id = d.dataset_id
            LEFT JOIN {MAP_DB_SCHEMA}.category_master c ON c.category_id = d.category_id
            WHERE d.is_active IS TRUE AND d.title IS NOT NULL
            ORDER BY d.dataset_id, m.created_at DESC
            """
        )
        datasets = cur.fetchall()

        cur.execute(
            f"""
            SELECT dataset_id, field_name, ontology_mapping, ontology_mapping_to_display,
                   data_type, ontology_graph_key
            FROM {MAP_DB_SCHEMA}.dataset_mapping
            ORDER BY dataset_mapping_id
            """
        )
        fields_by_dataset: dict[int, list] = {}
        for row in cur.fetchall():
            fields_by_dataset.setdefault(row["dataset_id"], []).append(
                {k: row[k] for k in ("field_name", "ontology_mapping", "ontology_mapping_to_display",
                                     "data_type", "ontology_graph_key")}
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


async def list_peers(client: httpx.AsyncClient) -> tuple[list[dict], dict]:
    """(peers to search, {name: status} for nodes deliberately not searched)."""
    global _last_known_peers
    own_url = fed.NODE_BASE_URL.rstrip("/")
    skipped: dict[str, dict] = {}

    if fed.NODE_ROLE == "central":
        nodes = await asyncio.to_thread(fed.list_nodes, True)
        peers = []
        for n in nodes:
            if n["base_url"].rstrip("/") == own_url:
                continue
            if n["status"] == "active":
                peers.append({"node_id": n["node_id"], "name": n["name"], "base_url": n["base_url"].rstrip("/")})
            elif n["status"] != "revoked":
                skipped[n["name"]] = {"status": f"skipped_{n['status']}", "base_url": n["base_url"]}
        return peers, skipped

    central_url = (fed.CENTRAL_SERVER_URL or "").rstrip("/")
    if not central_url:
        return [], {}  # standalone / detached: search only this node

    own_registry_id = os.getenv("CENTRAL_NODE_ID", "")
    central_peer = {"node_id": None, "name": _CENTRAL_PLACEHOLDER, "base_url": central_url}
    try:
        resp = await client.get(f"{central_url}/nodes", timeout=CATALOG_TIMEOUT)
        resp.raise_for_status()
        others = []
        for n in resp.json().get("nodes", []):
            if n["base_url"].rstrip("/") in (own_url, central_url) or n["node_id"] == own_registry_id:
                continue
            if n["status"] == "active":
                others.append({"node_id": n["node_id"], "name": n["name"], "base_url": n["base_url"].rstrip("/")})
            elif n["status"] != "revoked":
                skipped[n["name"]] = {"status": f"skipped_{n['status']}", "base_url": n["base_url"]}
        _last_known_peers = others
    except Exception:
        others = _last_known_peers or [
            o for o in await asyncio.to_thread(_cached_origins) if o["base_url"] not in (own_url, central_url)
        ]
    return [central_peer] + others, skipped


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


async def federation_catalog(category: str | None, term: str | None, q: str | None, force: bool) -> dict:
    """GET /federation/catalog: this node's datasets + every reachable peer's,
    each tagged with origin_node. `term` matches a field's ontology term IRI
    (the "similar datasets" lookup)."""
    async with httpx.AsyncClient() as client:
        peers, skipped = await list_peers(client)
        statuses = await refresh_catalogs(client, peers, force=force)
    rows = await asyncio.to_thread(_cached_datasets, [p["base_url"] for p in peers])
    local = await asyncio.to_thread(local_catalog_datasets)

    me = self_info()
    datasets = [{**json.loads(json.dumps(d, default=_json_default)), "origin_node": me} for d in local]
    for r in rows:
        datasets.append(
            {
                "dataset_id": r["remote_dataset_id"], "title": r["title"], "category": r["category"],
                "description": r["description"], "keywords": r["keywords"], "fields": r["fields"],
                "row_count": r["row_count"],
                "date_min": r["date_min"].isoformat() if r["date_min"] else None,
                "date_max": r["date_max"].isoformat() if r["date_max"] else None,
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


async def federate(request: Request, route_path: str, payload, local_handler):
    """Run `local_handler` for this node's share of the request and forward
    the rest to the owning peers, then merge. scope="local", or a request
    that already came from another node, runs only locally."""
    if payload.scope == "local" or request.headers.get(HOP_HEADER):
        return await local_handler(payload)

    requested_categories = {c.strip().lower() for c in payload.category}
    me = self_info()
    nodes: dict[str, dict] = {}

    async with httpx.AsyncClient() as client:
        peers, skipped = await list_peers(client)
        catalog_status = await refresh_catalogs(client, peers)
        remote_rows = await asyncio.to_thread(_cached_datasets, [p["base_url"] for p in peers])
        local_datasets = await asyncio.to_thread(local_catalog_datasets)

        local_titles_known = {_norm(d["title"]) for d in local_datasets}
        remote_by_title: dict[str, list[dict]] = {}
        for r in remote_rows:
            remote_by_title.setdefault(_norm(r["title"]), []).append(r)

        if payload.dataset:
            local_titles = [
                t for t in payload.dataset
                if _norm(t) in local_titles_known or _norm(t) not in remote_by_title
            ]
            remote_pairs = [(r, t) for t in payload.dataset for r in remote_by_title.get(_norm(t), [])]
        else:
            # No datasets named: search everything in the requested categories.
            local_titles = [d["title"] for d in local_datasets if (d["category"] or "").lower() in requested_categories]
            remote_pairs = [(r, r["title"]) for r in remote_rows if (r["category"] or "").lower() in requested_categories]

        titles_by_peer: dict[str, list[str]] = {}
        for r, title in remote_pairs:
            titles_by_peer.setdefault(r["origin_base_url"], [])
            if title not in titles_by_peer[r["origin_base_url"]]:
                titles_by_peer[r["origin_base_url"]].append(title)

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

    merged["invalid_datasets"] = [t for t in payload.dataset if _norm(t) not in found_titles]
    merged["scope"] = "federation"
    merged["nodes"] = nodes

    if not merged["valid_datasets"]:
        raise HTTPException(
            status_code=400,
            detail={"message": "No valid datasets provided.", "invalid_datasets": merged["invalid_datasets"], "nodes": nodes},
        )
    return merged
