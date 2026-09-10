"""Answer-synthesis prompt for the Phase 2 agent (Epic CPHR-92 / architecture
doc §6 "reasoning/generation engine" step): turns retrieval-index grounding
plus tool results (run_sparql / spatial_search / federated_search /
metadata_search) into the final natural-language answer.

Grounded-QA contract: answer only from the CONTEXT block below, cite the
dataset/field/row count backing every factual claim, and say so explicitly
rather than guessing when the context doesn't support an answer (this is
what the out_of_scope questions in eval/eval_questions.json check for).

Not wired to an LLM yet — Ollama/vLLM serving is a separate, not-yet-done
item in Epic CPHR-92. This module only builds the prompt string/messages;
the caller passes them to whatever chat-completion client lands later.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.retrieval_index import IndexEntry

SYSTEM_PROMPT = """You are a grounded question-answering assistant over the CPHR biodiversity/Ayurveda data platform.

Rules:
1. Answer using ONLY the facts in the CONTEXT block below. Never use outside knowledge, training data, or assumptions to fill a gap.
2. Every factual claim must cite its source inline, in the form (Dataset: <dataset title>, field: <field name>, N rows) for data results, or (Ontology: <term>) for schema/vocabulary facts.
3. If the CONTEXT does not contain enough information to answer, say so plainly — e.g. "I don't have that data" or "The <X> dataset doesn't record this" — instead of guessing, extrapolating, or inventing a plausible-sounding number/name.
4. If CONTEXT is empty, or every schema hit is a weak/unrelated match, treat the question as out of scope and say you don't have relevant data rather than answering from the closest-sounding term.
5. Never state a row count, dataset name, or field name that doesn't appear verbatim in CONTEXT.
6. Keep the answer concise: state the answer, then the citation(s). Do not restate the whole context."""


@dataclass
class DataResult:
    """One tool call's results (run_sparql / spatial_search / federated_search /
    metadata_search), already fetched — this module only formats them, it
    does not call the tools."""
    dataset_title: str
    field_names: list[str]
    row_count: int
    sample_rows: list[dict] = field(default_factory=list)
    max_samples: int = 3

    def to_context_line(self) -> str:
        fields = ", ".join(self.field_names)
        line = f"- Dataset '{self.dataset_title}', field(s) [{fields}]: {self.row_count} row(s) matched."
        if self.sample_rows:
            shown = self.sample_rows[: self.max_samples]
            for row in shown:
                line += f"\n    e.g. {row}"
            if self.row_count > len(shown):
                line += f"\n    (+{self.row_count - len(shown)} more not shown)"
        return line


def _schema_context_block(schema_hits: list[tuple[float, IndexEntry]]) -> str:
    if not schema_hits:
        return "(no relevant ontology/schema terms retrieved)"
    lines = []
    for score, entry in schema_hits:
        lines.append(f"- [{entry.kind}] {entry.text}  (similarity={score:.2f})")
    return "\n".join(lines)


def _data_context_block(data_results: list[DataResult]) -> str:
    if not data_results:
        return "(no data queried, or no tool call returned rows)"
    return "\n".join(r.to_context_line() for r in data_results)


def build_answer_synthesis_prompt(
    question: str,
    schema_hits: list[tuple[float, IndexEntry]],
    data_results: list[DataResult] | None = None,
) -> dict[str, str]:
    """Returns {"system": ..., "user": ...} ready for a chat-completion call.

    schema_hits: retrieval_index.search() output — grounds which ontology
    terms/dataset fields are relevant (vocabulary).
    data_results: actual rows fetched by whichever tool(s) the agent called
    (spatial/cross-partner/SPARQL) — this is what row-count citations come
    from. Omit for pure vocabulary questions with no data lookup.
    """
    data_results = data_results or []
    context = (
        f"SCHEMA/VOCABULARY GROUNDING:\n{_schema_context_block(schema_hits)}\n\n"
        f"DATA RESULTS:\n{_data_context_block(data_results)}"
    )
    user = f"CONTEXT:\n{context}\n\nQUESTION: {question}\n\nAnswer using only the CONTEXT above, with citations."
    return {"system": SYSTEM_PROMPT, "user": user}


def main() -> None:
    """Demo: renders a real prompt from the live retrieval index, no LLM call."""
    from app.retrieval_index import search

    question = "is Terminalia chebula commercially traded in India"
    schema_hits = search(question, k=3)
    data_results = [
        DataResult(
            dataset_title="Traded Medicinal Plants of India (TMPI)",
            field_names=["scientific_name", "official name"],
            row_count=1,
            sample_rows=[{"scientific_name": "Terminalia chebula", "official_name": "Haritaki"}],
        )
    ]
    prompt = build_answer_synthesis_prompt(question, schema_hits, data_results)
    print("--- system ---")
    print(prompt["system"])
    print("\n--- user ---")
    print(prompt["user"])


if __name__ == "__main__":
    main()
