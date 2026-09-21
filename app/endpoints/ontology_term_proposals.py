from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app import dataset_registration_service as svc

# "Propose a new ontology term" from the registration wizard's step 2, when
# nothing in the chosen ontology fits. Proposals are a moderation queue --
# they don't write into custom_ontology directly (product decision
# 2026-09-16). Approving/rejecting a proposal is a future moderation UI's
# job, not built here.
router = APIRouter(prefix="/ontology/terms", tags=["Registration APIs"])


class ProposeTermInput(BaseModel):
    dataset_id: int
    field_name: str
    proposed_label: str
    proposed_definition: str | None = None
    target_ontology_graph_key: str | None = None


@router.post("/propose")
def propose_term(payload: ProposeTermInput):
    try:
        return svc.propose_term(
            payload.dataset_id,
            payload.field_name,
            payload.proposed_label,
            payload.proposed_definition,
            payload.target_ontology_graph_key,
        )
    except svc.DatasetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/proposals")
def list_proposals(status: str | None = Query(default=None)):
    return {"items": svc.list_term_proposals(status)}
