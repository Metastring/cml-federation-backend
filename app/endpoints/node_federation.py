from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from app import node_federation_service as svc

# Federation Phase 1 endpoints (FEDERATION_ARCHITECTURE.md §4/§6). No prefix
# -- the doc specifies flat paths (/node/manifest, /nodes, /nodes/{id}/...)
# shared verbatim across every node.
router = APIRouter(tags=["Federation APIs"])


class RegisterNodeInput(BaseModel):
    manifest_url: str
    name: str
    maintained_by: str | None = None


class HeartbeatInput(BaseModel):
    dataset_count: int | None = None
    version: str | None = None


class DetachInput(BaseModel):
    revoke_key: bool = False


def _bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header (expected 'Bearer <api_key>').",
        )
    return authorization.split(" ", 1)[1].strip()


def _raise_for(exc: Exception):
    if isinstance(exc, svc.NodeNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, svc.NodeAuthError):
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if isinstance(exc, (svc.InvalidManifestError, ValueError)):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise exc


@router.get("/node/manifest", summary="This node's self-description")
def node_manifest():
    """Present on every node (central and client alike). Registration and
    discovery both start by fetching this document (§4.1)."""
    return svc.get_manifest()


@router.post("/nodes/register", summary="A client node registers itself with central")
async def register_node(payload: RegisterNodeInput):
    """Central fetches the caller's manifest URL itself before trusting
    anything it claims (§4.2). Returns an API key -- shown once, stored only
    as a hash -- required on every later heartbeat/detach call."""
    try:
        return await svc.register_node(payload.manifest_url, payload.name, payload.maintained_by)
    except Exception as exc:
        _raise_for(exc)


@router.post("/nodes/{node_id}/heartbeat", summary="A registered node checks in")
def node_heartbeat(
    node_id: str,
    payload: HeartbeatInput,
    authorization: str | None = Header(default=None),
):
    """Sent repeatedly (every 5-15 min) with the node's API key (§4.3). Also
    the trigger point a later harvest job (§5) would hook into."""
    try:
        api_key = _bearer_token(authorization)
        return svc.record_heartbeat(node_id, api_key, payload.dataset_count, payload.version)
    except Exception as exc:
        _raise_for(exc)


@router.get("/nodes", summary="Directory listing of federation peers")
def list_nodes(include_inactive: bool = False):
    """What central AND every client node polls and caches locally (§4.4) --
    the cache is what lets node-to-node search work without a live
    round-trip through central, and survives a node's own detach."""
    return {"nodes": svc.list_nodes(include_inactive)}


@router.post("/nodes/self/detach", summary="A node takes itself offline")
def detach_self(payload: DetachInput, authorization: str | None = Header(default=None)):
    """Node-initiated, identified by its own API key (not a path id) --
    'I'm going offline/independent for now' (§6). `revoke_key=true` folds in
    the hard deregister case too, per the doc's recommended shortcut."""
    try:
        api_key = _bearer_token(authorization)
        return svc.detach_self(api_key, payload.revoke_key)
    except Exception as exc:
        _raise_for(exc)


@router.post("/nodes/{node_id}/revoke", summary="Central-admin revokes a node")
def revoke_node(node_id: str):
    """Central-side admin action for a decommissioned or misbehaving node
    (§6) -- doesn't require the node's own key, unlike detach_self."""
    try:
        return svc.revoke_node(node_id)
    except Exception as exc:
        _raise_for(exc)
