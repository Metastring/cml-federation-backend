"""Run the CPHR-106 eval question set (eval_questions.json) against the
retrieval index for every question tagged grading.mode == "retrieval".

Questions tagged grading.mode == "agent" need the ReAct agent + tool router
(rest of Epic CPHR-92, not built yet) and are listed but skipped, so the set
doesn't need rebuilding once that lands.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.retrieval_index import search  # noqa: E402

QUESTIONS_PATH = Path(__file__).resolve().parent / "eval_questions.json"


def evaluate() -> int:
    data = json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))
    threshold = data["no_match_threshold"]
    questions = data["questions"]

    retrieval_qs = [q for q in questions if q["grading"]["mode"] == "retrieval"]
    agent_qs = [q for q in questions if q["grading"]["mode"] == "agent"]

    passed = 0
    print(f"Retrieval-gradable questions: {len(retrieval_qs)} (threshold={threshold})\n")

    for q in retrieval_qs:
        grading = q["grading"]
        results = search(q["question"], k=1)
        top_score, top_entry = results[0] if results else (0.0, None)

        if grading.get("expected_behavior") == "reject_below_threshold":
            ok = top_score < threshold
            detail = f"score={top_score:.4f} (want < {threshold})"
        else:
            ok = (
                top_score >= threshold
                and top_entry.kind == grading["expected_kind"]
                and grading["expected_text_contains"].lower() in top_entry.text.lower()
            )
            detail = f"score={top_score:.4f} kind={top_entry.kind if top_entry else None} text={top_entry.text[:70] if top_entry else None}"

        passed += ok
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {q['id']:10s} {q['question']!r}\n         {detail}")

    print(f"\n{passed}/{len(retrieval_qs)} retrieval-gradable questions passed.\n")

    print(f"Agent-gradable questions (pending ReAct agent, Epic CPHR-92): {len(agent_qs)}")
    by_category: dict[str, int] = {}
    for q in agent_qs:
        by_category[q["category"]] = by_category.get(q["category"], 0) + 1
    for category, count in sorted(by_category.items()):
        print(f"  {category}: {count}")

    return 0 if passed == len(retrieval_qs) else 1


if __name__ == "__main__":
    raise SystemExit(evaluate())
