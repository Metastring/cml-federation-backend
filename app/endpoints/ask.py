import json
import threading

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app import agent

# Natural-language question -> tool-using agent over the registered datasets
# (app/agent.py), on a local LLM. Epic CPHR-92 / CPHR-104.
router = APIRouter(tags=["Natural Language Query"])

# The agent has one llama-server slot; a second question would only queue
# behind the first inside llama-server and risk its timeouts, so queue here
# and tell the client it is waiting.
_agent_lock = threading.Lock()


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500, examples=["which 3 cities had the worst air quality?"])
    stream: bool = Field(default=True, description="Server-sent events per step (true) or one JSON reply at the end (false)")


def _sse(event: dict) -> str:
    return f"event: {event['type']}\ndata: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"


def _locked_events(question: str):
    if not _agent_lock.acquire(blocking=False):
        yield {"type": "status", "message": "Waiting for another question to finish."}
        _agent_lock.acquire()
    try:
        yield from agent.run_agent(question)
    finally:
        _agent_lock.release()


@router.post(
    "/ask",
    summary="Answer a natural-language question with a tool-using agent",
    description="Streams `text/event-stream` events: `step` (the agent's thought and tool call), "
    "`observation` (tool result), then `answer` (answer text + citations from the tool results) or `error`. "
    "Expect 1-3 minutes per question on this CPU-only host. With `stream=false` returns the `answer` event "
    "plus the full trace as one JSON object.",
)
async def ask_endpoint(body: AskRequest):
    question = body.question.strip()
    if body.stream:
        # Starlette iterates a sync generator in a worker thread, so the LLM calls don't block the event loop.
        return StreamingResponse(
            (_sse(e) for e in _locked_events(question)),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    def run() -> dict:
        trace = list(_locked_events(question))
        final = trace[-1]
        if final["type"] == "error":
            raise agent.AgentError(final["message"])
        return {**final, "trace": [e for e in trace if e["type"] in ("step", "observation")]}

    try:
        return await run_in_threadpool(run)
    except agent.AgentError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
