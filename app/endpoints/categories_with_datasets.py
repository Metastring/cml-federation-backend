from fastapi import APIRouter
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter()

@router.get("/categories-with-datasets")
def get_categories_with_datasets():
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT 
                cat.category_name, 
                ds.title AS dataset_title
            FROM 
                category_master cat
            LEFT JOIN 
                dataset_master ds ON ds.category_id = cat.category_id AND ds.is_active = true
            ORDER BY 
                cat.category_name, ds.title;
        """)

        rows = cursor.fetchall()

        category_map = {}

        for row in rows:
            category = row["category_name"]
            dataset_title = row["dataset_title"]  # may be None

            if category not in category_map:
                category_map[category] = []

            if dataset_title:
                category_map[category].append(dataset_title)

        result = [
            {"category_name": category, "datasets": datasets}
            for category, datasets in category_map.items()
        ]

        return result

    finally:
        conn.close()
