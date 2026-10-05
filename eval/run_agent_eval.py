"""Run the grading.mode == "agent" questions of eval_questions.json (CPHR-106)
through the live agent (app/agent.py) and grade each answer.

Needs llama-server (~/llm/start-llm.sh) and the backend/map module that the
tools call. Slow: ~1-3 min per question on the CPU-only host.

  python eval/run_agent_eval.py                # all agent questions
  python eval/run_agent_eval.py data-01 oos-a-01   # just these ids
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.agent import run_agent  # noqa: E402

QUESTIONS_PATH = Path(__file__).resolve().parent / "eval_questions.json"
NO_DATA_PHRASES = re.compile(
    r"no data|not have|don't have|do not have|doesn't have|not available|no (registered |relevant )?dataset|"
    r"not (contain|cover|include|record)|unable to|cannot|can't|no information|out of scope",
    re.IGNORECASE,
)


def _normalise(text: str) -> str:
    text = re.sub(r"(?<=\d),(?=\d{3})", "", text.lower())  # 74,225 -> 74225
    return re.sub(r"[\s,]+", " ", text)


def grade(grading: dict, final: dict, tools_called: list[str]) -> list[str]:
    """-> list of failure reasons (empty = pass)."""
    if final["type"] != "answer":
        return [f"agent error: {final.get('message')}"]
    answer = final["answer"]
    failures = []
    if grading.get("expected_behavior") == "no_data":
        # Citations are allowed: checking a nearby dataset before refusing
        # (e.g. 2025 weather for a forecast question) is the right behaviour.
        if not NO_DATA_PHRASES.search(answer):
            failures.append("answer doesn't say the data is unavailable")
        return failures

    expected_tools = grading.get("expected_tools") or []
    if expected_tools and not set(expected_tools) & set(tools_called):
        failures.append(f"expected one of tools {expected_tools}, called {tools_called}")
    dataset_id = grading.get("expected_dataset_id")
    if dataset_id is not None and dataset_id not in {c.get("dataset_id") for c in final["citations"]}:
        failures.append(f"dataset {dataset_id} not cited")
    for text in grading.get("answer_contains") or []:
        if _normalise(text) not in _normalise(answer):
            failures.append(f"answer lacks {text!r}")
    return failures


def evaluate(ids: list[str]) -> int:
    questions = [q for q in json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))["questions"]
                 if q["grading"]["mode"] == "agent" and (not ids or q["id"] in ids)]
    passed, total_s = 0, 0.0
    for q in questions:
        t = time.perf_counter()
        events = list(run_agent(q["question"]))
        secs = time.perf_counter() - t
        total_s += secs
        tools_called = [e["action"] for e in events if e["type"] == "step" and e["action"] != "final_answer"]
        failures = grade(q["grading"], events[-1], tools_called)
        passed += not failures
        print(f"[{'PASS' if not failures else 'FAIL'}] {q['id']:10s} {secs:5.0f}s  {q['question']!r}")
        print(f"         tools: {' -> '.join(tools_called) or '(none)'}")
        print(f"         answer: {(events[-1].get('answer') or events[-1].get('message') or '')[:300]}")
        for f in failures:
            print(f"         - {f}")
        sys.stdout.flush()

    if questions:
        print(f"\n{passed}/{len(questions)} agent questions passed, mean {total_s / len(questions):.0f}s per question.")
    return 0 if passed == len(questions) else 1


if __name__ == "__main__":
    raise SystemExit(evaluate(sys.argv[1:]))
