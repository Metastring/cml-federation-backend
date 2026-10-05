"""ReAct agent over the CML datasets (Epic CPHR-92 "Agent MVP - Phase 2",
CPHR-103).

One question -> a loop of {thought, tool call, observation} turns with a
local open-weight LLM (llama.cpp `llama-server`, ~/llm/start-llm.sh) until it
gives a final answer or runs out of steps. Tools live in app/agent_tools.py.

Design notes for a CPU-only 7B model:
- Every LLM turn is constrained to a JSON schema (llama-server turns it into
  a grammar), so the output always parses; the model can still pick a wrong
  tool or arguments, which comes back to it as an error observation.
- The conversation only ever grows by appending, and the system prompt is
  identical for every question, so llama-server reuses the cached prompt
  prefix and each turn only pays for the new tokens (~18 tok/s to read).
- Citations returned to the client come from the tool results themselves,
  not from the LLM's text, so they are always real.
"""
from __future__ import annotations

import json
import os
import time
from typing import Iterator

import httpx

from app import agent_tools
from app.nl_query import LLM_BASE_URL, LLM_MODEL
from app.prompts import AGENT_SYSTEM_PROMPT

MAX_STEPS = int(os.getenv("AGENT_MAX_STEPS", "6"))  # tool calls per question
MAX_TOOL_ERRORS = int(os.getenv("AGENT_MAX_TOOL_ERRORS", "2"))  # consecutive, before forcing an answer
LLM_TIMEOUT_S = float(os.getenv("AGENT_LLM_TIMEOUT_S", "180"))
LLM_SLOT = int(os.getenv("AGENT_LLM_SLOT", "0"))  # nl_query uses slot 1, see nl_query.LLM_SLOT
OBS_MAX_CHARS = 2500  # hard cap on one observation sent back to the LLM

FINAL = "final_answer"


class AgentError(Exception):
    """A failure the caller should show to the user as-is."""


def _step_schema(actions: list[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            "thought": {"type": "string"},
            "action": {"type": "string", "enum": actions},
            "action_input": {"type": "object"},
        },
        "required": ["thought", "action", "action_input"],
    }


def _chat(messages: list[dict], actions: list[str]) -> tuple[dict, dict]:
    """-> (parsed step, llama-server timings)"""
    try:
        resp = httpx.post(
            f"{LLM_BASE_URL}/v1/chat/completions",
            json={
                "model": LLM_MODEL,
                "messages": messages,
                "temperature": 0,
                "max_tokens": 600,
                "id_slot": LLM_SLOT,
                "response_format": {"type": "json_schema", "json_schema": {"schema": _step_schema(actions)}},
            },
            timeout=LLM_TIMEOUT_S,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise AgentError(f"Local LLM at {LLM_BASE_URL} is not reachable ({exc.__class__.__name__}). "
                         "Start it with ~/llm/start-llm.sh.") from exc
    body = resp.json()
    content = body["choices"][0]["message"]["content"]
    try:
        step = json.loads(content)
    except json.JSONDecodeError as exc:  # only if max_tokens cut the JSON off
        raise AgentError("The model's reply was cut off before it finished.") from exc
    return step, body.get("timings", {})


def _system_prompt() -> str:
    return AGENT_SYSTEM_PROMPT.format(tools=agent_tools.tools_prompt_block(),
                                      datasets=agent_tools.datasets_prompt_block())


def _clip(text: str) -> str:
    return text if len(text) <= OBS_MAX_CHARS else text[:OBS_MAX_CHARS] + "\n(observation truncated)"


def run_agent(question: str) -> Iterator[dict]:
    """Yields events as they happen (streamed to the client by POST /ask):
      {"type": "step", "step", "thought", "action", "action_input"}
      {"type": "observation", "step", "action", "observation", "ok", "data"}
      {"type": "answer", "answer", "citations", "steps", "timings"}
      {"type": "error", "message"}
    """
    t0 = time.perf_counter()
    tool_names = list(agent_tools.TOOLS)
    messages = [
        {"role": "system", "content": _system_prompt()},
        {"role": "user", "content": f"Question: {question}"},
    ]
    citations: list[dict] = []
    timings: dict[str, float] = {"llm_s": 0.0, "tools_s": 0.0}
    tool_errors = 0

    for step_no in range(1, MAX_STEPS + 2):
        # Out of tool budget (or erroring repeatedly): the only option left is to answer.
        must_answer = step_no > MAX_STEPS or tool_errors >= MAX_TOOL_ERRORS
        if must_answer:
            messages.append({"role": "user", "content": "Stop calling tools. Give your final_answer now from the observations so far; "
                                                        "if they are not enough, say what data is missing."})
        t = time.perf_counter()
        try:
            step, _ = _chat(messages, [FINAL] if must_answer else tool_names + [FINAL])
        except AgentError as exc:
            yield {"type": "error", "message": str(exc)}
            return
        timings["llm_s"] += time.perf_counter() - t
        messages.append({"role": "assistant", "content": json.dumps(step, ensure_ascii=False)})

        action, args = step.get("action"), step.get("action_input") or {}
        yield {"type": "step", "step": step_no, "thought": step.get("thought", ""), "action": action, "action_input": args}

        if action == FINAL:
            answer = str(args.get("answer") or "").strip() or "I could not produce an answer."
            timings = {k: round(v, 2) for k, v in timings.items()}
            timings["total_s"] = round(time.perf_counter() - t0, 2)
            yield {"type": "answer", "answer": answer, "citations": _dedupe(citations),
                   "steps": step_no - 1, "timings": timings}
            return

        t = time.perf_counter()
        try:
            result = agent_tools.run_tool(action, args)
            observation, ok, data = result.observation, True, result.data
            citations += result.citations
            tool_errors = 0
        except agent_tools.ToolError as exc:
            observation, ok, data = f"ERROR: {exc}", False, {}
            tool_errors += 1
        except Exception as exc:  # a bug or an unexpected backend reply; let the model route around it
            observation, ok, data = f"ERROR: {action} failed unexpectedly ({exc.__class__.__name__}: {exc})", False, {}
            tool_errors += 1
        timings["tools_s"] += time.perf_counter() - t

        observation = _clip(observation)
        messages.append({"role": "user", "content": f"Observation: {observation}"})
        yield {"type": "observation", "step": step_no, "action": action, "observation": observation, "ok": ok, "data": data}


def _dedupe(citations: list[dict]) -> list[dict]:
    seen, out = set(), []
    for c in citations:
        key = (c.get("dataset_id"), c.get("title"), c.get("sql"), c.get("row_count"))
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out


if __name__ == "__main__":
    import sys

    for event in run_agent(" ".join(sys.argv[1:])):
        event = {k: v for k, v in event.items() if k != "data"}
        print(json.dumps(event, ensure_ascii=False, default=str)[:1500], flush=True)
