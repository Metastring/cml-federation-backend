"""CPHR-92 agent: tool argument/safety checks and the ReAct loop, with the
LLM and backends stubbed out (no llama-server or database needed)."""
import pytest

from app import agent, agent_tools
from app.agent_tools import ToolError, ToolResult


def test_run_sparql_rejects_updates():
    with pytest.raises(ToolError):
        agent_tools.run_sparql("DELETE WHERE { ?s ?p ?o }")
    with pytest.raises(ToolError):
        agent_tools.run_sparql("SELECT * WHERE { SERVICE <http://evil> { ?s ?p ?o } }")


def test_run_sparql_adds_limit_and_ignores_keywords_in_literals(monkeypatch):
    sent = {}

    class Resp:
        status_code = 200

        def json(self):
            return {"head": {"vars": ["l"]}, "results": {"bindings": [{"l": {"value": "Drop size"}}]}}

    def fake_post(url, data, headers, timeout):
        sent["query"] = data["query"]
        return Resp()

    monkeypatch.setattr(agent_tools.httpx, "post", fake_post)
    result = agent_tools.run_sparql('SELECT ?l WHERE { ?s <http://x/label> ?l FILTER(CONTAINS(?l, "drop")) }')

    assert sent["query"].rstrip().endswith(f"LIMIT {agent_tools.SPARQL_MAX_ROWS}")
    assert "Drop size" in result.observation


def test_run_tool_validates_name_and_required_args():
    with pytest.raises(ToolError, match="Unknown tool"):
        agent_tools.run_tool("drop_tables", {})
    with pytest.raises(ToolError, match="needs: dataset_id"):
        agent_tools.run_tool("query_data", {"question": "max rainfall"})


def test_query_data_rejects_non_numeric_dataset_id():
    with pytest.raises(ToolError, match="must be a number"):
        agent_tools.query_data("max rainfall", "NASA")


def _scripted_llm(monkeypatch, steps):
    """Make agent._chat return `steps` in order, recording what it was allowed to do."""
    calls = []

    def fake_chat(messages, actions):
        calls.append({"messages": list(messages), "actions": actions})
        return steps[len(calls) - 1], {}

    monkeypatch.setattr(agent, "_chat", fake_chat)
    monkeypatch.setattr(agent, "_system_prompt", lambda: "SYSTEM")
    return calls


def test_agent_calls_tool_then_answers_with_tool_citations(monkeypatch):
    calls = _scripted_llm(monkeypatch, [
        {"thought": "rank", "action": "query_data", "action_input": {"question": "worst AQI city", "dataset_id": 115}},
        {"thought": "done", "action": "final_answer", "action_input": {"answer": "Sri Ganganagar, AQI 315"}},
    ])
    citation = {"dataset_id": 115, "title": "CPCB AQI", "fields": ["city", "aqi"], "row_count": 1, "sql": "SELECT 1"}
    monkeypatch.setattr(agent_tools, "run_tool", lambda name, args: ToolResult("city | aqi\nSri ganganagar | 315", citations=[citation]))

    events = list(agent.run_agent("worst air quality?"))

    assert [e["type"] for e in events] == ["step", "observation", "step", "answer"]
    assert events[-1]["answer"] == "Sri Ganganagar, AQI 315"
    assert events[-1]["citations"] == [citation]
    # The observation is fed back to the LLM on the next turn.
    assert calls[1]["messages"][-1] == {"role": "user", "content": "Observation: city | aqi\nSri ganganagar | 315"}


def test_agent_forces_answer_after_repeated_tool_errors(monkeypatch):
    bad = {"thought": "try", "action": "query_data", "action_input": {"question": "x", "dataset_id": 1}}
    calls = _scripted_llm(monkeypatch, [bad] * agent.MAX_TOOL_ERRORS + [
        {"thought": "give up", "action": "final_answer", "action_input": {"answer": "No data."}},
    ])

    def failing_tool(name, args):
        raise ToolError("Dataset 1 is not a queryable tabular dataset.")

    monkeypatch.setattr(agent_tools, "run_tool", failing_tool)

    events = list(agent.run_agent("anything"))

    assert events[-1]["type"] == "answer"
    assert all(not e["ok"] for e in events if e["type"] == "observation")
    assert calls[-1]["actions"] == [agent.FINAL]  # only final_answer allowed once errors hit the cap


def test_agent_stops_calling_tools_after_max_steps(monkeypatch):
    step = {"thought": "more", "action": "lookup_schema", "action_input": {"query": "rain"}}
    calls = _scripted_llm(monkeypatch, [step] * agent.MAX_STEPS + [
        {"thought": "answer", "action": "final_answer", "action_input": {"answer": "Rainfall is in dataset 116."}},
    ])
    monkeypatch.setattr(agent_tools, "run_tool", lambda name, args: ToolResult("rainfall_mm"))

    events = list(agent.run_agent("rain?"))

    assert events[-1]["type"] == "answer"
    assert len(calls) == agent.MAX_STEPS + 1
    assert calls[-1]["actions"] == [agent.FINAL]


def test_agent_reports_unreachable_llm(monkeypatch):
    def down(messages, actions):
        raise agent.AgentError("Local LLM is not reachable")

    monkeypatch.setattr(agent, "_chat", down)
    monkeypatch.setattr(agent, "_system_prompt", lambda: "SYSTEM")

    assert list(agent.run_agent("q")) == [{"type": "error", "message": "Local LLM is not reachable"}]
