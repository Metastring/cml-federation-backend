from fastapi import APIRouter, Query

from app import federation_search

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


@router.post("/federation/harvest", summary="Re-harvest every peer node's catalog now")
async def federation_harvest():
    return await federation_search.harvest_all(force=True)
