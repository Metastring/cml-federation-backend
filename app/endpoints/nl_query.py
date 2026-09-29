from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from app import nl_query

# Natural-language question -> SQL over one registered tabular dataset,
# answered by a local LLM (app/nl_query.py). Epic CPHR-92.
router = APIRouter(tags=["Natural Language Query"])


class NLQueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500, examples=["give me all cities having temperature below 18 degree"])
    dataset_id: int | None = Field(default=None, description="Force a dataset instead of letting the question pick one")


@router.post("/nl-query", summary="Answer a natural-language question from registered tabular data")
async def nl_query_endpoint(body: NLQueryRequest):
    # The LLM call takes seconds on CPU; keep it off the event loop.
    try:
        return await run_in_threadpool(nl_query.answer_question, body.question.strip(), body.dataset_id)
    except nl_query.NLQueryError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
