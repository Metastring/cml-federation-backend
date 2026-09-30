from fastapi import APIRouter, HTTPException, Query

from app import federation_search
from app import node_federation_service as fed

# Federation Phase 2 catalog endpoints (FEDERATION_ARCHITECTURE.md §13).
# Search itself stays on the existing /federated-search* routes via their
# `scope` field.
router = APIRouter(tags=["Node Federation"])


@router.get("/node/catalog", summary="This node's searchable datasets (harvested by peers)")
def node_catalog():
    return federation_search.node_catalog()


@router.get("/federation/catalog", summary="Datasets across this node and all reachable peer nodes")
async def federation_catalog(
    category: str | None = Query(default=None, description="e.g. Climate, Environment"),
    term: str | None = Query(default=None, description="Ontology term IRI a field is mapped to (similar-dataset lookup)"),
    q: str | None = Query(default=None, description="Text in title / description / keywords"),
    refresh: bool = Query(default=False, description="Re-harvest every peer's catalog first instead of using the cache"),
):
    return await federation_search.federation_catalog(category, term, q, force=refresh)


@router.get("/nodes/{node_id}/datasets", summary="Datasets of one registered node, or `self` for this server")
async def node_datasets(node_id: str):
    """Live from the node when reachable, else its last harvested copy
    (datasets_source = live | cache | unavailable). A down node is a 200,
    not an error; only an unknown node_id is a 404."""
    try:
        return await federation_search.node_datasets(node_id)
    except fed.NodeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/federation/harvest", summary="Re-harvest every peer node's catalog now")
async def federation_harvest():
    return await federation_search.harvest_all(force=True)
