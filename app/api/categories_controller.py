"""Categories API controller."""
import logging
from typing import Any, Dict, List

from fastapi import APIRouter
from psycopg2.extras import RealDictCursor

from app.core.database import get_connection
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/categories", response_model=List[Dict[str, Any]])
def get_categories() -> List[Dict[str, Any]]:
    """Get all categories from the database.

    Retrieves all active categories ordered by name.

    Returns:
        List of category dictionaries with id and name.

    Raises:
        DatabaseError: If database query fails.

    Example:
        >>> categories = get_categories()
        >>> print(categories[0])
        {'category_id': '1', 'category_name': 'Mammals'}
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT category_id, category_name
            FROM category_master
            ORDER BY category_name;
        """
        )
        categories = cursor.fetchall()
        logger.info(
            "Categories retrieved",
            extra={"count": len(categories) if categories else 0},
        )
        return categories
    except Exception as e:
        logger.error(f"Failed to retrieve categories: {e}")
        raise DatabaseError("Failed to retrieve categories", "select") from e
    finally:
        conn.close()


@router.get("/categories-with-datasets", response_model=List[Dict[str, Any]])
def get_categories_with_datasets() -> List[Dict[str, Any]]:
    """Get categories with associated datasets and metadata.

    Returns hierarchical structure of categories containing datasets
    with field mappings and metadata.

    Returns:
        List of categories with nested datasets and field information.

    Raises:
        DatabaseError: If database query fails.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT 
                cat.category_name, 
                ds.dataset_id, 
                ds.title AS dataset_title,
                ds.description,
                ds.keywords,
                ds.publication_date,
                ds.doi,
                ds.license,
                ds.metadata_modified_date AS last_updated,
                ds.registration_date,
                c.name AS contact_name,
                dm.field_name, 
                dm.ontology_mapping,
                dm.ontology_mapping_to_display,
                dm.data_type
            FROM 
                category_master cat
            LEFT JOIN 
                dataset_master ds ON ds.category_id = cat.category_id
                AND ds.is_active = true
            LEFT JOIN
                dataset_mapping dm ON dm.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_contacts c ON c.dataset_id = ds.dataset_id
            ORDER BY 
                cat.category_name, ds.title, dm.field_name;
        """
        )

        rows = cursor.fetchall()
        category_map = _build_category_map(rows)
        result = _format_category_result(category_map)

        logger.info(
            "Categories with datasets retrieved",
            extra={"category_count": len(result)},
        )
        return result

    except Exception as e:
        logger.error(f"Failed to retrieve categories with datasets: {e}")
        raise DatabaseError(
            "Failed to retrieve categories with datasets", "select"
        ) from e
    finally:
        conn.close()


def _build_category_map(
    rows: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build hierarchical category map from database rows.

    Args:
        rows: Database result rows.

    Returns:
        Nested dictionary of categories and datasets.
    """
    category_map: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        category = row["category_name"]
        dataset_title = row["dataset_title"]
        dataset_id = row["dataset_id"]

        if category not in category_map:
            category_map[category] = {}

        if dataset_title:
            if dataset_title not in category_map[category]:
                metadata = _build_metadata(row)
                category_map[category][dataset_title] = {
                    "description": row.get("description"),
                    "metadata": metadata,
                    "fields": [],
                }

            # Append field details if they are present
            if row.get("field_name"):
                field_info = {
                    "field_name": row["field_name"],
                    "ontology_mapping": row["ontology_mapping"],
                    "ontology_mapping_to_display": row["ontology_mapping_to_display"],
                    "data_type": row["data_type"],
                }
                category_map[category][dataset_title]["fields"].append(field_info)

    return category_map


def _build_metadata(row: Dict[str, Any]) -> Dict[str, str]:
    """Build metadata dictionary from database row.

    Args:
        row: Database result row.

    Returns:
        Metadata dictionary with standardized keys.
    """
    return {
        "keywords": row.get("keywords"),
        "DOI": row.get("doi"),
        "contacts": row.get("contact_name"),
        "License": row.get("license", "CC-BY"),
        "Publication Date": str(row.get("publication_date") or "2023-06-01"),
        "Last Updated": str(row.get("last_updated") or "2025-08-06"),
        "Registration Date": str(row.get("registration_date") or "2023-01-01"),
    }


def _format_category_result(
    category_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Format category map into API response format.

    Args:
        category_map: Nested dictionary of categories and datasets.

    Returns:
        Formatted list of categories for API response.
    """
    return [
        {
            "category_name": category,
            "datasets": [
                {
                    "dataset_title": dataset_title,
                    "description": datasets_info["description"],
                    "metadata": datasets_info["metadata"],
                    "fields": datasets_info["fields"],
                }
                for dataset_title, datasets_info in category_map[category].items()
            ],
        }
        for category in category_map
    ]

