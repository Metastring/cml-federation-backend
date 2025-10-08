from typing import List, Dict

from queries.dao.dao import (
    get_all_summary_tables,
    get_distinct_categories,
    get_summary_tables_by_category,
)


def fetch_summary_tables() -> List[Dict]:
    return get_all_summary_tables()


def fetch_categories() -> List[str]:
    return get_distinct_categories()


def fetch_summary_tables_by_category(category: str) -> List[Dict]:
    return get_summary_tables_by_category(category)

