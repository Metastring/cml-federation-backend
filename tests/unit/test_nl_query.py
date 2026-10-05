"""app/nl_query.py value hints, with the database and embedding model stubbed."""
import numpy as np

from app import nl_query
from app.nl_query import QueryableDataset


def _dataset():
    return QueryableDataset(119, "NFHS-5", "", "", "", "nfhs", [("indicator", "text"), ("state_name", "text"), ("value", "double precision")])


def test_value_hints_put_verbatim_values_first_and_drop_weak_matches(monkeypatch):
    columns = {
        "indicator": (["Children under 5 years who are stunted (%)", "Households using iodized salt (%)"],
                      np.array([[0.9, 0.0], [0.2, 0.0]])),
        "state_name": (["Kerala", "Punjab"], np.array([[0.1, 0.0], [0.42, 0.0]])),
    }
    monkeypatch.setattr(nl_query, "_column_values", lambda ds, col: columns.get(col))
    monkeypatch.setattr(nl_query, "_embed", lambda texts: np.array([[1.0, 0.0]]))

    hints = nl_query._value_hints(_dataset(), "child stunting in kerala")

    # Kerala: named in the question (case-insensitive), despite a low similarity.
    # Punjab: 0.42 < VALUE_HINT_MIN_SCORE, so left out. Numeric columns are never hinted.
    assert hints == {"indicator": ["Children under 5 years who are stunted (%)"], "state_name": ["Kerala"]}


def test_schema_block_lists_hinted_values_under_their_column():
    block = nl_query._schema_block(_dataset(), [], {"indicator": ["Children under 5 years who are stunted (%)"]})

    assert "  indicator text\n    values like: 'Children under 5 years who are stunted (%)'" in block
