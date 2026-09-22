"""Federation Phase 1 -- node manifest, registration, heartbeat, directory
listing, and detach/revoke. See ../FEDERATION_ARCHITECTURE.md for the full
design this implements (§3 core model, §4 central node registry, §6 detach
vs. deregister).

Every node (central or client) runs this same module -- the distinction is
purely which env vars a given deployment's .env sets (NODE_ROLE=central vs.
client). /node/manifest works identically everywhere; /nodes/register,
/nodes/{id}/heartbeat, /nodes and /nodes/self/detach are only meaningfully
used against whichever instance a deployment has designated central.
"""
from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timezone

import httpx
from psycopg2.extras import Json, RealDictCursor

from app.db import get_connection

NODE_ROLE = os.getenv("NODE_ROLE", "central")
NODE_ID = os.getenv("NODE_ID", "")
NODE_NAME = os.getenv("NODE_NAME", "CML Central")
NODE_BASE_URL = os.getenv("NODE_BASE_URL", "http://localhost:8000")
CENTRAL_SERVER_URL = os.getenv("CENTRAL_SERVER_URL", "")
FUSEKI_SPARQL_ENDPOINT = os.getenv("FUSEKI_SPARQL_ENDPOINT", "")
GEOSERVER_URL = os.getenv("GEOSERVER_URL", "")
CML_VERSION = os.getenv("CML_VERSION", "0.1.0")

# Beyond this many minutes without a heartbeat, an 'active' node is
# considered 'stale' and dropped from search fan-out (§4.3) even without an
# explicit detach call. Checked lazily on every /nodes listing rather than
# via a background cron, since there's no scheduler in this codebase yet.
STALE_AFTER_MINUTES = int(os.getenv("NODE_STALE_AFTER_MINUTES", "20"))


class NodeNotFoundError(Exception):
    pass


class NodeAuthError(Exception):
    pass


class InvalidManifestError(Exception):
    pass


def _hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _row_to_dict(row: dict) -> dict:
    return {
        "node_id": str(row["node_id"]),
        "name": row["name"],
        "base_url": row["base_url"],
        "sparql_endpoint": row.get("sparql_endpoint"),
        "geoserver_url": row.get("geoserver_url"),
        "maintained_by": row.get("maintained_by"),
        "status": row["status"],
        "registered_at": row["registered_at"],
        "last_heartbeat_at": row.get("last_heartbeat_at"),
        "metadata": row.get("metadata") or {},
    }


def _log_event(cursor, node_id, event_type: str, detail: str | None = None) -> None:
    cursor.execute(
        "INSERT INTO federation_sync_log (node_id, event_type, detail) VALUES (%s, %s, %s)",
        (node_id, event_type, detail),
    )


def get_manifest() -> dict:
    """`GET /node/manifest` -- this node's own self-description. Every other
    federation flow starts by fetching this document (§4.1)."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT COUNT(*) AS n FROM dataset_master WHERE status = 'published'")
        dataset_count = cursor.fetchone()["n"]
        try:
            cursor.execute("SELECT COUNT(*) AS n FROM custom_ontology")
            ontology_count = cursor.fetchone()["n"]
        except Exception:
            ontology_count = 0
    finally:
        conn.close()

    return {
        "node_id": NODE_ID or None,
        "node_name": NODE_NAME,
        "node_role": NODE_ROLE,
        "base_url": NODE_BASE_URL,
        "sparql_endpoint": FUSEKI_SPARQL_ENDPOINT or None,
        "geoserver_url": GEOSERVER_URL or None,
        "central_server_url": CENTRAL_SERVER_URL or None,
        "version": CML_VERSION,
        "dataset_count": dataset_count,
        "ontology_count": ontology_count,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def register_node(manifest_url: str, name: str, maintained_by: str | None) -> dict:
    """`POST /nodes/register` -- central fetches the node's own manifest URL
    itself before trusting anything the node claims (§4.2: "never trusts a
    self-asserted claim"). Re-registering the same base_url updates that
    node's row and reissues its key rather than creating a duplicate."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(manifest_url)
            response.raise_for_status()
            manifest = response.json()
    except Exception as exc:
        raise InvalidManifestError(
            f"Could not fetch or parse manifest at {manifest_url}: {exc}"
        ) from exc

    base_url = manifest.get("base_url")
    if not base_url:
        raise InvalidManifestError(
            f"Manifest at {manifest_url} is missing required field 'base_url'."
        )

    api_key = secrets.token_urlsafe(32)
    api_key_hash = _hash_key(api_key)

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            INSERT INTO node_registry
                (name, base_url, sparql_endpoint, geoserver_url, maintained_by,
                 status, api_key_hash, metadata)
            VALUES (%s, %s, %s, %s, %s, 'active', %s, %s)
            ON CONFLICT (base_url) DO UPDATE SET
                name = EXCLUDED.name,
                sparql_endpoint = EXCLUDED.sparql_endpoint,
                geoserver_url = EXCLUDED.geoserver_url,
                maintained_by = EXCLUDED.maintained_by,
                status = 'active',
                api_key_hash = EXCLUDED.api_key_hash
            RETURNING node_id
            """,
            (
                name,
                base_url,
                manifest.get("sparql_endpoint"),
                manifest.get("geoserver_url"),
                maintained_by,
                api_key_hash,
                Json({"manifest_node_id": manifest.get("node_id")} if manifest.get("node_id") else {}),
            ),
        )
        node_id = cursor.fetchone()["node_id"]
        _log_event(cursor, node_id, "register", f"manifest_url={manifest_url}")
        conn.commit()
    finally:
        conn.close()

    return {
        "node_id": str(node_id),
        "name": name,
        "base_url": base_url,
        "status": "active",
        "api_key": api_key,  # returned once -- only the hash is stored
    }


def _find_by_id(cursor, node_id: str) -> dict:
    cursor.execute("SELECT * FROM node_registry WHERE node_id = %s", (node_id,))
    row = cursor.fetchone()
    if not row:
        raise NodeNotFoundError(f"No node with id {node_id}")
    return row


def _find_by_api_key(cursor, api_key: str) -> dict:
    cursor.execute(
        "SELECT * FROM node_registry WHERE api_key_hash = %s", (_hash_key(api_key),)
    )
    row = cursor.fetchone()
    if not row:
        raise NodeAuthError("Invalid or revoked API key.")
    return row


def record_heartbeat(node_id: str, api_key: str, dataset_count: int | None, version: str | None) -> dict:
    """`POST /nodes/{id}/heartbeat` (§4.3). Requires the node's own API key.
    Marks the node active again -- resuming heartbeats after a detach is
    treated as the node rejoining, not a new trust decision, matching §6's
    "re-attaching later is just resuming heartbeats"."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        row = _find_by_id(cursor, node_id)
        if row["api_key_hash"] != _hash_key(api_key):
            raise NodeAuthError("API key does not match this node.")
        if row["status"] == "revoked":
            raise NodeAuthError("This node's registration has been revoked.")

        detail_parts = []
        if dataset_count is not None:
            detail_parts.append(f"dataset_count={dataset_count}")
        if version:
            detail_parts.append(f"version={version}")

        cursor.execute(
            "UPDATE node_registry SET status = 'active', last_heartbeat_at = now() "
            "WHERE node_id = %s",
            (node_id,),
        )
        _log_event(cursor, node_id, "heartbeat", ", ".join(detail_parts) or None)
        conn.commit()
    finally:
        conn.close()

    return {"node_id": node_id, "status": "active", "last_heartbeat_at": datetime.now(timezone.utc).isoformat()}


def list_nodes(include_inactive: bool = False) -> list[dict]:
    """`GET /nodes` (§4.4) -- the directory listing every node polls and
    caches locally. Lazily flips any 'active' node whose last heartbeat is
    older than STALE_AFTER_MINUTES to 'stale' before returning (§4.3)."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            UPDATE node_registry
            SET status = 'stale'
            WHERE status = 'active'
              AND last_heartbeat_at IS NOT NULL
              AND last_heartbeat_at < now() - (%s || ' minutes')::interval
            RETURNING node_id
            """,
            (STALE_AFTER_MINUTES,),
        )
        for row in cursor.fetchall():
            _log_event(cursor, row["node_id"], "stale", f"no heartbeat for {STALE_AFTER_MINUTES}m")

        if include_inactive:
            cursor.execute("SELECT * FROM node_registry ORDER BY name")
        else:
            cursor.execute(
                "SELECT * FROM node_registry WHERE status IN ('active', 'stale') ORDER BY name"
            )
        rows = cursor.fetchall()
        conn.commit()
    finally:
        conn.close()

    return [_row_to_dict(r) for r in rows]


def detach_self(api_key: str, revoke_key: bool) -> dict:
    """`POST /nodes/self/detach` (§6). Node-initiated, identified by its own
    API key rather than a path id. `revoke_key=false` is the soft/reversible
    detach; `revoke_key=true` folds in the hard deregister/revoke case too,
    per §6's "recommended implementation shortcut"."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        row = _find_by_api_key(cursor, api_key)
        new_status = "revoked" if revoke_key else "detached"

        if revoke_key:
            cursor.execute(
                "UPDATE node_registry SET status = 'revoked', api_key_hash = NULL "
                "WHERE node_id = %s",
                (row["node_id"],),
            )
        else:
            cursor.execute(
                "UPDATE node_registry SET status = 'detached' WHERE node_id = %s",
                (row["node_id"],),
            )
        _log_event(cursor, row["node_id"], "revoke" if revoke_key else "detach", None)
        conn.commit()
    finally:
        conn.close()

    return {"node_id": str(row["node_id"]), "status": new_status}


def revoke_node(node_id: str) -> dict:
    """`DELETE`-equivalent admin action (§6, central-admin-initiated hard
    path): node decommissioned, misbehaving, or the relationship ended.
    Unlike detach_self, this doesn't require the node's own key -- it's
    central revoking its *knowledge* of the node, not something the node
    agreed to. No auth guard here, matching this codebase's existing admin
    endpoints (e.g. map_module_backend's /admin/*), which are also
    unauthenticated today."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        _find_by_id(cursor, node_id)
        cursor.execute(
            "UPDATE node_registry SET status = 'revoked', api_key_hash = NULL "
            "WHERE node_id = %s",
            (node_id,),
        )
        _log_event(cursor, node_id, "revoke", "admin-initiated")
        conn.commit()
    finally:
        conn.close()

    return {"node_id": node_id, "status": "revoked"}
