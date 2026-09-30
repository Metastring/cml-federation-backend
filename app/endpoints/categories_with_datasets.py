import asyncio
from typing import Literal

from fastapi import APIRouter, Query, Request
from app.db import get_connection
from app import federation_search
from psycopg2.extras import RealDictCursor
import psycopg2
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# primary first, then how close the ontology term is to ours
_MATCH_ORDER = {"exact": 0, "close": 1, "broader": 2, "narrower": 3, "related": 4}


def _load_term_crosswalk(cursor):
    """Index cml_term / cml_term_ontology_match for field -> term lookup.

    Returns (by_key, by_uri, by_synonym, refs) where refs maps term_key to its
    ontology references. Empty if the crosswalk migration isn't applied yet
    (e.g. a node DB lagging central), so the endpoint still works.
    """
    try:
        cursor.execute("SAVEPOINT term_crosswalk")
        cursor.execute("""
            SELECT t.term_key, t.synonyms,
                   m.ontology_graph_key, m.term_uri, m.term_label,
                   m.match_type, m.is_primary
            FROM cml_term t
            LEFT JOIN cml_term_ontology_match m ON m.term_key = t.term_key
            ORDER BY t.term_key;
        """)
        rows = cursor.fetchall()
        cursor.execute("RELEASE SAVEPOINT term_crosswalk")
    except psycopg2.errors.UndefinedTable:
        cursor.execute("ROLLBACK TO SAVEPOINT term_crosswalk")
        logger.warning("cml_term crosswalk tables missing; indicator references disabled")
        return {}, {}, {}, {}

    by_key, by_uri, by_synonym, refs = {}, {}, {}, {}
    for row in rows:
        key = row["term_key"]
        if key not in refs:
            refs[key] = []
            by_key[key.lower()] = key
            for syn in row["synonyms"] or []:
                by_synonym.setdefault(syn.lower(), []).append(key)
        if row["term_uri"]:
            by_uri.setdefault(row["term_uri"], []).append(key)
            refs[key].append({
                "ontology": row["ontology_graph_key"],
                "uri": row["term_uri"],
                "label": row["term_label"],
                "match_type": row["match_type"],
                "is_primary": row["is_primary"],
            })
    for key in refs:
        refs[key].sort(key=lambda r: (not r["is_primary"], _MATCH_ORDER.get(r["match_type"], 9)))
    for index in (by_uri, by_synonym):
        for k, keys in index.items():
            index[k] = list(dict.fromkeys(keys))
    return by_key, by_uri, by_synonym, refs


def _resolve_term(field_name, ontology_mapping, crosswalk):
    """Pick the cml_term for a dataset field, or None."""
    by_key, by_uri, by_synonym, _ = crosswalk
    mapping = (ontology_mapping or "").strip()
    name = (field_name or "").strip().lower()

    if mapping.lower() in by_key:
        return by_key[mapping.lower()]
    candidates = by_uri.get(mapping) or by_synonym.get(mapping.lower()) or by_synonym.get(name) or []
    if len(candidates) > 1:
        # one URI can back several terms (e.g. OEO energy-met vs peak-demand);
        # prefer the one whose synonyms name this column
        by_name = [k for k in candidates if k in by_synonym.get(name, [])]
        if by_name:
            return by_name[0]
    return candidates[0] if candidates else None


@router.get("/categories", tags=["Registration APIs"])
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



# @router.get("/categories-with-datasets")
# def get_categories_with_datasets():
#     conn = get_connection()
#     try:
#         cursor = conn.cursor(cursor_factory=RealDictCursor)
#         cursor.execute("""
#             SELECT 
#                 cat.category_name, 
#                 ds.dataset_id, 
#                 ds.description AS dataset_title,
#                 dm.field_name, 
#                 dm.ontology_mapping, 
#                 dm.data_type
#             FROM 
#                 category_master cat
#             LEFT JOIN 
#                 dataset_master ds ON ds.category_id = cat.category_id AND ds.is_active = true
#             LEFT JOIN
#                 dataset_mapping dm ON dm.dataset_id = ds.dataset_id
#             ORDER BY 
#                 cat.category_name, ds.description, dm.field_name;
#         """)
        
#         rows = cursor.fetchall()
#         category_map = {}
        
#         for row in rows:
#             category = row["category_name"]
#             dataset_title = row["dataset_title"]  # May be None
#             dataset_id = row["dataset_id"]
#             field_info = {
#                 "field_name": row["field_name"],
#                 "ontology_mapping": row["ontology_mapping"],
#                 "data_type": row["data_type"]
#             }
            
#             if category not in category_map:
#                 category_map[category] = {}
                
#             if dataset_title:
#                 if dataset_title not in category_map[category]:
#                     category_map[category][dataset_title] = {
#                         "fields": []
#                     }
                
#                 # Append field details if they are present (field_name may be None if no fields are present)
#                 if field_info["field_name"]:  
#                     category_map[category][dataset_title]["fields"].append(field_info)
        
#         # Transform the map into the desired output format
#         result = [
#             {
#                 "category_name": category, 
#                 "datasets": [
#                     {
#                         "dataset_title": dataset_title,
#                         "fields": datasets_info["fields"]
#                     }
#                     for dataset_title, datasets_info in category_map[category].items()
#                 ]
#             }
#             for category in category_map
#         ]
        
#         return result
    
#     finally:
#         conn.close()
@router.get("/categories-with-datasets")
async def get_categories_with_datasets(
    request: Request,
    scope: Literal["local", "federation"] = Query(
        default="federation",
        description="'federation' = this server's datasets plus every reachable peer's (default); 'local' = this server only",
    ),
):
    """Datasets grouped by category. With scope="federation" every peer's
    searchable datasets are listed too, tagged with `origin_node`; a title
    that clashes with one already listed in its category becomes
    "<title> @ <node name>", which /federated-search* and /metadata accept."""
    local = await asyncio.to_thread(_local_categories_with_datasets)
    me = federation_search.self_info()
    for group in local:
        for d in group["datasets"]:
            d["origin_node"] = me
    if scope == "local" or request.headers.get(federation_search.HOP_HEADER):
        return local

    _, _, _, rows = await federation_search.remote_catalogs()
    crosswalk = await asyncio.to_thread(_term_crosswalk)
    groups = {g["category_name"]: g for g in local}
    for r in rows:
        category = r["category"] or "Uncategorised"
        group = groups.get(category)
        if group is None:
            group = groups[category] = {"category_name": category, "datasets": []}
            local.append(group)
        origin = {"node_id": r["origin_node_id"], "name": r["origin_node_name"], "base_url": r["origin_base_url"]}
        title = r["title"]
        if any(d["dataset_title"] == title for d in group["datasets"]):
            title = f"{title} @ {origin['name']}"
        fields = []
        for f in r["fields"] or []:
            term_key = _resolve_term(f.get("field_name"), f.get("ontology_mapping"), crosswalk)
            fields.append({
                "field_name": f.get("field_name"),
                "ontology_mapping": f.get("ontology_mapping"),
                "ontology_mapping_to_display": f.get("ontology_mapping_to_display"),
                "data_type": f.get("data_type"),
                "cml_term": term_key,
                "references": crosswalk[3].get(term_key, []) if term_key else [],
            })
        group["datasets"].append({
            "dataset_title": title,
            "description": r["description"],
            "metadata": {
                "keywords": r["keywords"], "DOI": None, "contacts": None, "License": None,
                "Publication Date": None, "Last Updated": None, "Registration Date": None,
            },
            "fields": fields,
            "origin_node": origin,
        })
    return local


def _term_crosswalk():
    conn = get_connection()
    try:
        return _load_term_crosswalk(conn.cursor(cursor_factory=RealDictCursor))
    finally:
        conn.close()


def _local_categories_with_datasets():
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
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
                dataset_master ds
            JOIN
                category_master cat ON cat.category_id = ds.category_id
            LEFT JOIN
                dataset_mapping dm ON dm.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_contacts c ON c.dataset_id = ds.dataset_id
            WHERE ds.is_active = true
            ORDER BY
                cat.category_name, ds.title, dm.field_name;
        """)
        
        rows = cursor.fetchall()
        crosswalk = _load_term_crosswalk(cursor)
        term_refs = crosswalk[3]
        category_map = {}

        for row in rows:
            category = row["category_name"]
            dataset_title = row["dataset_title"]  # May be None
            dataset_id = row["dataset_id"]

            # metadata (renamed from hover_fields)
            metadata = {
                "keywords": row.get("keywords"),
                "DOI": row.get("doi"),
                "contacts": row.get("contact_name"),
                "License": row.get("license", "CC-BY"),  # fallback if missing
                "Publication Date": str(row.get("publication_date") or "2023-06-01"),
                "Last Updated": str(row.get("last_updated") or "2025-08-06"),
                "Registration Date": str(row.get("registration_date") or "2023-01-01")
            }

            field_info = {
                "field_name": row["field_name"],
                "ontology_mapping": row["ontology_mapping"],
                "ontology_mapping_to_display": row["ontology_mapping_to_display"],
                "data_type": row["data_type"]
            }
            term_key = _resolve_term(row["field_name"], row["ontology_mapping"], crosswalk)
            field_info["cml_term"] = term_key
            field_info["references"] = term_refs.get(term_key, []) if term_key else []

            if category not in category_map:
                category_map[category] = {}

            if dataset_title:
                if dataset_title not in category_map[category]:
                    category_map[category][dataset_title] = {
                        "description": row.get("description"),
                        "metadata": metadata,   # updated here
                        "fields": []
                    }
                
                # Append field details if they are present
                if field_info["field_name"]:  
                    category_map[category][dataset_title]["fields"].append(field_info)
        
        # Transform the map into the desired output format
        result = [
            {
                "category_name": category, 
                "datasets": [
                    {
                        "dataset_title": dataset_title,
                        "description": datasets_info["description"],
                        "metadata": datasets_info["metadata"],   # updated here
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
