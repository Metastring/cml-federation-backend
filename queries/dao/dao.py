from typing import List, Dict
from sqlalchemy import text
from database.database import engine


def get_all_summary_tables() -> List[Dict]:
    sql = text(
        """
        SELECT id, table_no, title, main_fields, category, table_slug
        FROM public.summary_tables
        ORDER BY id ASC
        """
    )
    with engine.connect() as conn:
        res = conn.execute(sql)
        return [dict(row._mapping) for row in res]


def get_distinct_categories() -> List[str]:
    sql = text(
        """
        SELECT DISTINCT category
        FROM public.summary_tables
        WHERE category IS NOT NULL AND category <> ''
        ORDER BY category ASC
        """
    )
    with engine.connect() as conn:
        res = conn.execute(sql)
        return [row[0] for row in res]



def get_summary_tables_by_category(category: str) -> List[Dict]:
    sql = text(
        
        """
        SELECT id, table_no, title, main_fields, category, table_slug
        FROM public.summary_tables
        WHERE category = :category
        ORDER BY id ASC
        """
    )
    with engine.connect() as conn:
        res = conn.execute(sql, {"category": category})
        return [dict(row._mapping) for row in res]

