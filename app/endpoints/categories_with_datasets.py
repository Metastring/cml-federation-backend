from fastapi import APIRouter
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter()


@router.get("/categories")
def get_categories():
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT category_id, category_name
            FROM category_master
            ORDER BY category_name;
        """)
        categories = cursor.fetchall()
        return categories
    finally:
        conn.close()



@router.get("/categories-with-datasets")
def get_categories_with_datasets():
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT 
                cat.category_name, 
                ds.dataset_id, 
                ds.description AS dataset_title,
                dm.field_name, 
                dm.ontology_mapping, 
                dm.data_type
            FROM 
                category_master cat
            LEFT JOIN 
                dataset_master ds ON ds.category_id = cat.category_id AND ds.is_active = true
            LEFT JOIN
                dataset_mapping dm ON dm.dataset_id = ds.dataset_id
            ORDER BY 
                cat.category_name, ds.description, dm.field_name;
        """)
        
        rows = cursor.fetchall()
        category_map = {}
        
        for row in rows:
            category = row["category_name"]
            dataset_title = row["dataset_title"]  # May be None
            dataset_id = row["dataset_id"]
            field_info = {
                "field_name": row["field_name"],
                "ontology_mapping": row["ontology_mapping"],
                "data_type": row["data_type"]
            }
            
            if category not in category_map:
                category_map[category] = {}
                
            if dataset_title:
                if dataset_title not in category_map[category]:
                    category_map[category][dataset_title] = {
                        "fields": []
                    }
                
                # Append field details if they are present (field_name may be None if no fields are present)
                if field_info["field_name"]:  
                    category_map[category][dataset_title]["fields"].append(field_info)
        
        # Transform the map into the desired output format
        result = [
            {
                "category_name": category, 
                "datasets": [
                    {
                        "dataset_title": dataset_title,
                        "fields": datasets_info["fields"]
                    }
                    for dataset_title, datasets_info in category_map[category].items()
                ]
            }
            for category in category_map
        ]
        
        return result
    
    finally:
        conn.close()