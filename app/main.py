
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import httpx
import asyncio
import json
import math
import os
import re
import time

# Include other internal modules
from app.db import get_connection
from app.endpoints import metadata
from app.endpoints import categories_router
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import dataset_master
from app.endpoints import dataset_details
from app.endpoints import ontology
from app.endpoints import cphr_ontology
from app.endpoints import cphr_search
from psycopg2.extras import RealDictCursor



app = FastAPI()


@app.on_event("startup")
async def startup_event():
    _load_dataset_ontology_maps()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include internal routes
app.include_router(metadata.router)
app.include_router(categories_router)
app.include_router(dataset_master.router)
app.include_router(dataset_details.router)
app.include_router(ontology.router)
app.include_router(ontology.biodiversity_router)
app.include_router(cphr_ontology.router)
app.include_router(cphr_search.router)

# Participant API endpoints
CPMP_BOTANICAL_SEARCH_URL = "https://cpmp.tdu.edu.in/api/species/search/v2"
# Federated response key for CPMP species search (API body key is "keyword" but
# we surface a friendlier label in the field_results map).
CPMP_BOTANICAL_FEDERATED_FIELD = "Search Results"

PARTICIPANTS = {
    "Kew Plant Database": "http://134.209.145.106:8000/search",
    "CPMP Botanical Source": CPMP_BOTANICAL_SEARCH_URL,
    "CPMP Drug Source": "http://139.59.84.243:9087/search/search/drugname",
    "Traded Medicinal Plants of India (TMPI)": "https://tradedmedicinalplants.org/kew/webapi/advance/search",
    "Ayurahaar – The Ahara & Nutrition Portal": "https://ayurahaar.org/FoodType/webapi/ingredient/ingredient-property-list",
    "Rasashastra: A Database of Metals and Minerals used in Ayurveda": "https://rasashastra.tdu.edu.in/mm_api/advanced/search",
}

# Frontend redirect URLs for each participant dataset.
# These replace the internal API URL in the federated response so the frontend
# can link directly to the record detail page (append the record ID).
PARTICIPANT_FRONTEND_URLS: dict[str, str] = {
    "CPMP Botanical Source": "https://cpmp.tdu.edu.in/explore/plant_species?id=",
    "CPMP Drug Source": "https://cpmp.tdu.edu.in/explore/drugs?id=",
    "Traded Medicinal Plants of India (TMPI)": "https://tradedmedicinalplants.org/plant/",
    "Rasashastra: A Database of Metals and Minerals used in Ayurveda": "https://rasashastra.tdu.edu.in/mm_api/metals/details?drugId=",
}

# Schema that holds the map module's dataset tables (upload_logs, metadata, and
# the dynamic per-dataset tables registered via map_module_backend).
MAP_DB_SCHEMA = os.getenv("MAP_DB_SCHEMA", "public")

# ── Ontology field mapping loaded from dataset_mapping table ─────────────────
# Keyed by dataset title → {normalised_raw_field: ontology_mapping}
_DATASET_ONTOLOGY_MAP: dict[str, dict[str, str]] = {}


def _norm_field_key(field: str) -> str:
    """Collapse a field name to lowercase with all spaces/underscores/hyphens removed.

    This lets us fuzzy-match raw API keys like 'scientificName', 'scientific_name',
    and 'scientific name' to the same dataset_mapping.field_name entry.
    """
    return re.sub(r"[\s_\-]", "", field).lower()


def _load_dataset_ontology_maps() -> None:
    """Populate _DATASET_ONTOLOGY_MAP from the dataset_mapping table."""
    global _DATASET_ONTOLOGY_MAP
    try:
        conn = get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """
                SELECT d.title, dm.field_name, dm.ontology_mapping
                FROM dataset_mapping dm
                JOIN dataset_master d ON d.dataset_id = dm.dataset_id
                """
            )
            mapping: dict[str, dict[str, str]] = {}
            for row in cur.fetchall():
                title = row["title"]
                if title not in mapping:
                    mapping[title] = {}
                mapping[title][_norm_field_key(row["field_name"])] = row["ontology_mapping"]
            _DATASET_ONTOLOGY_MAP = mapping
        finally:
            conn.close()
    except Exception:
        pass  # Non-fatal — fall back to raw field names


def _map_row_to_ontology(row: dict, dataset_name: str) -> dict:
    """Re-key a result row using ontology field names from dataset_mapping.

    Fields whose lowercase name ends with 'id' are always kept as-is (IDs).
    Every other field is looked up in the dataset's ontology map; if no mapping
    exists the original key is preserved so no data is silently dropped.
    """
    field_map = _DATASET_ONTOLOGY_MAP.get(dataset_name, {})
    result: dict = {}
    for key, value in row.items():
        k_lower = str(key).lower()
        if k_lower.endswith("id") or k_lower == "id":
            result[key] = value
        else:
            ontology_key = field_map.get(_norm_field_key(key), key)
            result[ontology_key] = value
    return result

# ── In-memory result cache for /pre-federated-search ───────────────────────
_SEARCH_CACHE: dict[tuple, dict] = {}
_CACHE_TTL: int = int(os.getenv("PRE_SEARCH_CACHE_TTL", "300"))  # seconds (default 5 min)


def _make_cache_key(search_text: str, datasets: list[str]) -> tuple:
    return (
        search_text.strip().lower(),
        frozenset(_normalize_dataset_name(d) for d in datasets),
    )


def _get_cached(key: tuple) -> list[dict] | None:
    entry = _SEARCH_CACHE.get(key)
    if entry and (time.time() - entry["timestamp"]) < _CACHE_TTL:
        return entry["datasets"]
    if entry:
        del _SEARCH_CACHE[key]
    return None


def _value_contains(value, search_lower: str) -> bool:
    """Recursively check if search_lower appears anywhere in value."""
    if value is None:
        return False
    if isinstance(value, str):
        return search_lower in value.lower()
    if isinstance(value, (list, tuple)):
        return any(_value_contains(v, search_lower) for v in value)
    if isinstance(value, dict):
        return any(_value_contains(v, search_lower) for v in value.values())
    return False


# Internal-to-TMPI field mapping
#
# Our federated API receives internal field identifiers (e.g. "scientific_name",
# "vernacular_name_common_names"). TMPI, however, expects human-readable
# labels such as "Scientific Name" or "Common Name" in the
# "fieldToMatchWith" parameter. This mapping bridges that gap without
# changing the public /federated-search contract.
TMPI_FIELD_NAME_MAP: dict[str, str] = {
    # Trade name
    "trade_name": "Trade Name",
    "Trade Name": "Trade Name",

    # Common / vernacular name
    "common_name": "Common Name",
    "vernacular_name_common_names": "Common Name",
    "Common Name": "Common Name",

    # Official name
    "official_name": "Official Name",
    "Official Name": "Official Name",

    # Scientific name
    "scientific_name": "Scientific Name",
    "Scientific Name": "Scientific Name",
}


# For biodiversity participants that expose a common taxon schema, map
# internal field identifiers to the output field names we should
# project in the federated response.
BIODIVERSITY_RESULT_FIELD_MAP: dict[str, str] = {
    # Vernacular / common names
    "vernacular_name_common_names": "common_names",
    "common_name": "common_names",
    "common_names": "common_names",

    # Scientific name
    "scientific_name": "taxon_name",
    "taxon_name": "taxon_name",

    # Ayurahaar ingredient fields
    "ingredient_name": "common_name",
    "ingredient_common_name": "common_name",
    "sanskrit_name": "sanskrit_name",

    # Rasashastra fields
    "drugName": "drugName",
    "drug_name": "drugName",
    "synonyms": "synonyms",
    "category": "category",
}


def _project_biodiversity_result(row: dict, requested_field: str) -> dict:
    """Return a slimmed-down view for biodiversity results.

    Keeps ID-like columns ("*_id" / "id") plus the specific field
    the user searched on (mapped via BIODIVERSITY_RESULT_FIELD_MAP).
    Falls back to the original row if nothing matches.
    """

    if not requested_field:
        return row

    projected: dict = {}

    # Always preserve identifier columns
    for key, value in row.items():
        if isinstance(key, str):
            k_lower = key.lower()
            if k_lower.endswith("_id") or k_lower == "id":
                projected[key] = value

    # Determine which underlying field to read from, but always expose
    # the original requested field name in the response so we don't
    # rename columns for the frontend.
    source_key = BIODIVERSITY_RESULT_FIELD_MAP.get(requested_field, requested_field)

    if source_key in row:
        projected[requested_field] = row[source_key]
    elif requested_field in row:
        projected[requested_field] = row[requested_field]

    return projected or row


def _normalize_dataset_name(name: str) -> str:
    """Normalize dataset names for robust matching.

    - Lowercase
    - Trim whitespace
    - Normalize curly apostrophes to straight ones
    """

    return name.strip().lower().replace("’", "’")


def _fetch_layer_info(cursor, table_name: str) -> dict:
    """Return styles and attribute details (titleColumn, summaryColumn) for a map layer table."""
    styles = []
    try:
        cursor.execute(
            f"SELECT id, generated_style_name, color_by, data_type, layer_name "
            f"FROM {MAP_DB_SCHEMA}.style_metadata "
            "WHERE layer_table_name = %s AND is_active = TRUE ORDER BY id",
            (table_name,),
        )
        for s in cursor.fetchall():
            color_by = s["color_by"] or ""
            style_name = s["generated_style_name"] or f"{table_name}_{color_by}_style"
            style_title = " ".join(w.capitalize() for w in color_by.replace("_", " ").split())
            styles.append({
                "styleName": style_name,
                "styleTitle": style_title,
                "styleType": s["data_type"] or "unknown",
                "colorBy": color_by,
                "styleId": s["id"],
            })
    except Exception:
        pass

    title_column = None
    summary_columns: list[str] = []
    try:
        cursor.execute(
            """
            SELECT column_name,
                   CASE WHEN data_type IN ('character varying', 'varchar', 'text', 'char')
                        THEN true ELSE false END AS is_categorical
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name NOT IN ('geom', 'geometry', 'the_geom', 'id', 'gid', 'ogc_fid', 'dataset_id')
            ORDER BY ordinal_position
            """,
            (MAP_DB_SCHEMA, table_name),
        )
        for col in cursor.fetchall():
            col_name = col["column_name"]
            summary_columns.append(col_name)
            if title_column is None and col["is_categorical"]:
                title_column = col_name
        if title_column is None and summary_columns:
            title_column = summary_columns[0]
        summary_columns = summary_columns[:10]
    except Exception:
        pass

    return {
        "styles": styles,
        "titleColumn": title_column,
        "summaryColumn": summary_columns,
    }


def _is_cpmp_botanical_participant(participant_name: str, url: str) -> bool:
    """True when the dataset uses the CPMP species search v2 API.

    That API only accepts a ``keyword`` in the JSON body (not field names
    like ``scientific_name``). Match by URL or by dataset title so minor
    naming differences from the catalog still route correctly.
    """

    norm = _normalize_dataset_name(participant_name)
    if "cpmp botanical" in norm and "drug" not in norm:
        return True

    url_lower = (url or "").strip().lower()
    return (
        CPMP_BOTANICAL_SEARCH_URL.lower() in url_lower
        or "cpmp.tdu.edu.in/api/species/search" in url_lower
        or "139.59.84.243:9088/cml/search" in url_lower
    )


def _cpmp_botanical_search_payload(query: str) -> dict:
    """Build the CPMP species search v2 POST body (keyword-only search)."""

    return {
        "keyword": query or "",
        "page": "1",
        "size": "25",
        "taxon_status": ["Accepted"],
    }


def _resolve_participant_url(
    dataset_name: str, normalized_participants: dict[str, str]
) -> str | None:
    """Resolve a dataset title to its participant search URL."""

    url = normalized_participants.get(_normalize_dataset_name(dataset_name))
    if url:
        return url
    if _is_cpmp_botanical_participant(dataset_name, ""):
        return CPMP_BOTANICAL_SEARCH_URL
    return None


async def _fetch_cpmp_botanical_source(
    client,
    participant_name: str,
    url: str,
    field: str,
    query: str,
) -> dict:
    """POST to CPMP species search v2 using ``keyword`` only."""

    payload = _cpmp_botanical_search_payload(query)
    response = await client.post(url, json=payload)
    response.raise_for_status()
    data = response.json() or {}

    raw_items = data.get("searchResults", []) or []
    normalised_items = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue

        common_names = item.get("commonNames")
        if isinstance(common_names, list):
            common_names = ", ".join([c for c in common_names if c]) or None

        raw_row = {
            "taxonId": item.get("taxonId"),
            "taxonName": item.get("taxonName"),
            "commonNames": common_names,
            "matchedWith": item.get("matchedWith"),
        }
        normalised_items.append(_map_row_to_ontology(raw_row, participant_name))

    return {
        "participant_name": participant_name,
        "field": field,
        "api_url": url,
        "results": normalised_items,
    }


def _canonical_field_name(field: str) -> str:
    """Return the canonical field name used in responses.

    For some datasets we accept an alias in the request but
    surface a more explicit name in the federated response.
    """

    if not field:
        return field

    f = field.strip().lower()

    # Ayurahaar: the logical output column is "recipe_name",
    # even though the federated request field is "recipe".
    if f == "recipe":
        return "recipe_name"

    return field


def _apply_display_fields(field_results: dict, display_fields: list[str]) -> dict:
    """Re-key field_results by display_fields instead of search field names.

    All results across every searched field are combined (deduplicated) and placed
    under each display_field key so the frontend can use stable column headers.
    """
    if not display_fields:
        return field_results

    seen: set[str] = set()
    all_results: list[dict] = []
    combined_error = None

    for field_data in field_results.values():
        for row in (field_data.get("results") or []):
            dedup_key = json.dumps(row, sort_keys=True, default=str)
            if dedup_key not in seen:
                seen.add(dedup_key)
                all_results.append(row)
        if field_data.get("error"):
            combined_error = field_data["error"]

    return {
        df: {"results": all_results, "error": combined_error}
        for df in display_fields
    }


# Updated request payload model
class FederatedSearchRequest(BaseModel):
    category: list[str]
    dataset: list[str]
    fields: list[str]
    search_text: str
    display_fields: list[str] = []

async def fetch_from_participant(client, participant_name: str, url: str, field: str, query: str):
    try:
        # Special handling for CPMP Drug Source which now exposes
        # a POST JSON API at the new endpoint. The request body is a
        # plain JSON string containing the search text.
        if "139.59.84.243:9087/search/search/drugname" in url:
            payload = query or ""

            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json() or {}

            raw_items = data.get("searchResults", []) or []
            normalised_items = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                raw_row = {
                    "officialDrugId": item.get("officialDrugId"),
                    "officialDrugName": item.get("officialDrugName"),
                    "officialDrugNameEnglish": item.get("officialDrugNameEnglish"),
                    "officialDrugNameSanskrit": item.get("officialDrugNameSanskrit"),
                }
                normalised_items.append(_map_row_to_ontology(raw_row, participant_name))

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # CPMP species search v2 — always POST { keyword, page, size, taxon_status }.
        if _is_cpmp_botanical_participant(participant_name, url):
            return await _fetch_cpmp_botanical_source(
                client, participant_name, url, field, query
            )

        # Special handling for Ayurahaar – The Ahara & Nutrition Portal
        # When the federated field is "recipe", we search the recipe list.
        # Otherwise, we query the ingredient list and filter locally.
        if "ayurahaar.org" in url:
            query_norm = (query or "").strip().lower()

            # Special case: when field is "recipe", call the recipe API
            if field and field.strip().lower() == "recipe":
                recipe_url = "https://ayurahaar.org/Recipe/webapi/recipe/recipeList"
                response = await client.get(recipe_url)
                response.raise_for_status()
                data = response.json()

                matches: list[tuple[int, str, str]] = []  # (score, id, name)
                if isinstance(data, dict):
                    for rid, name in data.items():
                        if name is None:
                            continue
                        name_str = str(name)
                        name_norm = name_str.lower()
                        if query_norm in name_norm:
                            # Prefer exact (case-insensitive) matches over partial
                            score = 2 if query_norm == name_norm else 1
                            matches.append((score, str(rid), name_str))

                # Sort by score (exact first), then by recipe name for stability
                matches.sort(key=lambda x: (-x[0], x[2]))

                normalised_items = [
                    {
                        "recipe_id": int(rid) if rid.isdigit() else rid,
                        "recipe_name": name,
                    }
                    for _score, rid, name in matches
                ]

                return {
                    "participant_name": participant_name,
                    "field": field,
                    "api_url": recipe_url,
                    "results": normalised_items,
                }

            payload = {
                "FoodGroup": [],
                "NinProperties": [],
                "Dravyaguna": [],
                "Karma": [],
            }

            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

            items = data if isinstance(data, list) else []
            query_norm = (query or "").strip().lower()

            # trait_detail is a nested list: [{trait_name, trait_value, ...}, ...]
            # Flatten it so rasa/guna/vipaka etc. become top-level fields that
            # can be searched and ontology-mapped like any other field.
            # Keys are normalized trait_name values (parens stripped, lowercased,
            # spaces removed). Values are the ontology field names to use.
            _AYURAHAAR_TRAIT_MAP: dict[str, str] = {
                "rasa":          "rasa",         # "Rasa(Taste)"       → "rasa"
                "guna":          "guna",         # "Guna (Properties)" → "guna"
                "virya":         "veerya",       # "Virya (Potency)"   → "veerya"
                "vipaka":        "vipaka",       # "Vipaka"            → "vipaka"
                "doshakarma":    "dosha_action", # "DoshaKarma"        → "dosha_action"
                "ayurvedavargas": "varga",       # "Ayurveda Vargas"   → "varga"
            }

            def _flatten_ayurahaar(raw: dict) -> dict:
                flat = {k: v for k, v in raw.items() if k != "trait_detail"}
                for trait in (raw.get("trait_detail") or []):
                    if not isinstance(trait, dict):
                        continue
                    raw_name = trait.get("trait_name") or ""
                    norm_name = re.sub(r"\([^)]*\)", "", raw_name).lower().replace(" ", "")
                    field_key = _AYURAHAAR_TRAIT_MAP.get(norm_name, norm_name) or None
                    if not field_key:
                        continue
                    val = trait.get("trait_value")
                    if val is None:
                        continue
                    if field_key in flat:
                        existing = flat[field_key]
                        if isinstance(existing, list):
                            existing.append(val)
                        else:
                            flat[field_key] = [existing, val]
                    else:
                        flat[field_key] = val
                return flat

            flattened_items = [
                _flatten_ayurahaar(item)
                for item in items
                if isinstance(item, dict)
            ]

            filtered_results = []
            if query_norm:
                field_norm = (field or "").strip().lower()
                for item in flattened_items:
                    # If a specific field exists as a top-level key, restrict
                    # matching to that field; otherwise search the full object.
                    if field_norm and field_norm in item:
                        val = item[field_norm]
                        target_text = (
                            ", ".join(str(v) for v in val) if isinstance(val, list) else str(val)
                        ).lower()
                        if query_norm in target_text:
                            filtered_results.append(item)
                    else:
                        try:
                            blob = json.dumps(item, ensure_ascii=False).lower()
                        except TypeError:
                            continue
                        if query_norm in blob:
                            filtered_results.append(item)

            # Map all flattened fields to ontology names using dataset_mapping.
            normalised_items = [
                _map_row_to_ontology(item, participant_name)
                for item in filtered_results
            ]

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # Special handling for Rasashastra which exposes a JSON POST API
        if "rasashastra.tdu.edu.in" in url:
            payload = {
                "page": 1,
                "resultsPerPage": 10,
                "actionOnDoshas": [],
                "actionOnDiseases": [],
                "rasaList": [],
                "gunaList": [],
                "vipakaList": [],
                "viryaList": [],
                "summary": False,
                # Use federated search_text as the base search string
                "baseSearchString": query,
            }

            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

            raw_items = data.get("searchResult", [])
            normalised_items = []

            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                nomenclature = item.get("nomenclature") or {}
                modern_info = item.get("modernInformation") or {}
                dosage = item.get("dosageAndTreatment") or {}

                # Flatten nested Rasashastra structure then map to ontology
                raw_row = {
                    "drugId": item.get("drugId") or modern_info.get("drugId"),
                    "drug_name": modern_info.get("drugName") or nomenclature.get("drugName"),
                    "synonyms": nomenclature.get("synonyms"),
                    "dosageAndTreatment": dosage,
                    "category": nomenclature.get("category"),
                }
                normalised_items.append(_map_row_to_ontology(raw_row, participant_name))

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # Special handling for TMPI which exposes a different API contract
        if "tradedmedicinalplants.org" in url:
            # When a specific field is provided, use the advanced search API
            #   /kew/webapi/advance/search?fieldToMatchWith=<field>&stringToMatchWith=<query>
            # Otherwise, fall back to the simple findstring API which searches across all fields:
            #   /kew/webapi/findstring/<query>

            if field and field.strip() and field.strip().lower() not in {"*", "all"}:
                # Map internal field identifiers (e.g. "scientific_name") to
                # TMPI's expected display labels (e.g. "Scientific Name").
                mapped_field = TMPI_FIELD_NAME_MAP.get(field, field)
                params = {
                    "fieldToMatchWith": mapped_field,
                    "stringToMatchWith": query,
                }
                response = await client.get(url, params=params)
            else:
                findstring_url = "https://tradedmedicinalplants.org/kew/webapi/findstring/" + query
                response = await client.get(findstring_url)

            response.raise_for_status()
            data = response.json()

            # TMPI returns a list at the top level. We normalise each item to
            # match the biodiversity schema used by other participants
            # (e.g. Citizens' Portal):
            #   plantId         -> taxon_id
            #   scientificName  -> taxon_name
            #   commonName      -> common_names
            raw_items = data if isinstance(data, list) else data.get("results", [])

            normalised_items = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                # Flatten list-valued fields to comma-separated strings
                flat: dict = {}
                for k, v in item.items():
                    if isinstance(v, list):
                        flat[k] = ", ".join(str(x) for x in v if x) or None
                    else:
                        flat[k] = v

                normalised_items.append(_map_row_to_ontology(flat, participant_name))

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # Safety net: never send scientific_name (or any field) as a query
        # param to the CPMP species API — it only accepts keyword in JSON.
        if _is_cpmp_botanical_participant(participant_name, url):
            return await _fetch_cpmp_botanical_source(
                client, participant_name, url, field, query
            )

        # Default behaviour for participants that follow the common contract
        # If no specific field is provided (or a wildcard like * / all),
        # omit the field parameter so the participant can search globally.
        params = {"query": query}
        if field and field.strip() and field.strip().lower() not in {"*", "all"}:
            params["field"] = field

        response = await client.get(url, params=params)
        response.raise_for_status()
        raw_results = response.json().get("results", [])

        processed_results = [
            _map_row_to_ontology(row, participant_name) if isinstance(row, dict) else row
            for row in raw_results
        ]

        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": processed_results,
        }
    except Exception as e:
        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": [],
            "error": str(e)
        }

# @app.post("/federated-search")
# async def federated_search(payload: FederatedSearchRequest = Body(...)):
#     # Check category
#     if "biodiversity" not in [c.lower() for c in payload.category]:
#         raise HTTPException(status_code=400, detail="At least one category must be 'biodiversity'.")

#     # Validate datasets
#     invalid = [ds for ds in payload.dataset if ds not in PARTICIPANTS]
#     if invalid:
#         raise HTTPException(status_code=400, detail=f"Unknown datasets: {', '.join(invalid)}")

#     async with httpx.AsyncClient(timeout=10.0) as client:
#         # Create one task per dataset-field combination
#         tasks = [
#             fetch_from_participant(client, participant, PARTICIPANTS[participant], field, payload.search_text)
#             for participant in payload.dataset
#             for field in payload.fields
#         ]
#         responses = await asyncio.gather(*tasks)

#     # Group results by participant
#     results = {}
#     for item in responses:
#         pname = item["participant_name"]
#         if pname not in results:
#             results[pname] = {
#                 "api_url": item["api_url"],
#                 "field_results": {}
#             }
#         results[pname]["field_results"][item["field"]] = {
#             "results": item["results"],
#             "error": item.get("error")
#         }

#     return {
#         "category": payload.category,
#         "dataset": payload.dataset,
#         "fields": payload.fields,
#         "search_text": payload.search_text,
#         "results": results
#     }


async def _fetch_map_dataset_results(
    dataset_name: str, search_text: str
) -> dict | None:
    """Query a local map-module dataset for federated search results.

    Returns a dict with keys 'results' (list of row dicts) and 'table_name',
    or None if the dataset is not found in the local map DB.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            f"SELECT geoserver_name FROM {MAP_DB_SCHEMA}.metadata "
            "WHERE LOWER(name_of_dataset) = LOWER(%s) LIMIT 1",
            (dataset_name,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        geoserver_name = (row["geoserver_name"] or "").strip()
        parts = geoserver_name.split(":", 1)
        table_name = parts[1] if len(parts) == 2 else geoserver_name
        if not table_name:
            return None

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND data_type IN (
                  'character varying', 'text', 'varchar', 'character', 'name'
              )
              AND column_name NOT IN ('dataset_id', 'geom')
            ORDER BY ordinal_position
            """,
            (MAP_DB_SCHEMA, table_name),
        )
        text_columns = [r["column_name"] for r in cursor.fetchall()]
        if not text_columns:
            return {"results": [], "table_name": table_name}

        like_term = f"%{search_text.lower()}%"
        conditions = " OR ".join(
            f'LOWER(t."{col}") LIKE %(term)s' for col in text_columns
        )
        # Fetch all columns except geom (raw binary) and dataset_id
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name NOT IN ('geom', 'dataset_id')
            ORDER BY ordinal_position
            """,
            (MAP_DB_SCHEMA, table_name),
        )
        select_cols = ", ".join(f't."{r["column_name"]}"' for r in cursor.fetchall())
        if not select_cols:
            select_cols = "*"

        cursor.execute(
            f'SELECT {select_cols} FROM {MAP_DB_SCHEMA}."{table_name}" t WHERE {conditions} LIMIT 100',
            {"term": like_term},
        )
        rows = [
            {k: (None if isinstance(v, float) and not math.isfinite(v) else v) for k, v in dict(r).items()}
            for r in cursor.fetchall()
        ]
        return {"results": rows, "table_name": table_name}
    except Exception as e:
        return {"results": [], "error": str(e)}
    finally:
        conn.close()


@app.post("/federated-search")
async def federated_search(payload: FederatedSearchRequest = Body(...)):
    # Check category
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(status_code=400, detail="At least one category must be 'biodiversity'.")

    # Build a normalized lookup so that small differences (case,
    # curly vs straight apostrophe) do not break matching.
    normalized_participants = {
        _normalize_dataset_name(name): url for name, url in PARTICIPANTS.items()
    }

    resolved_participants: list[tuple[str, str]] = []  # (display_name, url)
    invalid_datasets: list[str] = []

    for ds in payload.dataset:
        url = _resolve_participant_url(ds, normalized_participants)
        if url:
            resolved_participants.append((ds, url))
        else:
            invalid_datasets.append(ds)

    # For datasets not in PARTICIPANTS, check if they live in the local map DB.
    map_dataset_results: dict[str, dict] = {}
    still_invalid: list[str] = []
    for ds in invalid_datasets:
        map_result = await _fetch_map_dataset_results(ds, payload.search_text)
        if map_result is not None:
            map_dataset_results[ds] = map_result
        else:
            still_invalid.append(ds)
    invalid_datasets = still_invalid

    if not resolved_participants and not map_dataset_results:
        raise HTTPException(status_code=400, detail="No valid datasets provided.")

    # Look up is_occurance_availabe flag from dataset_master for each
    # resolved dataset title, so we can surface it in the federated
    # results without changing the overall response shape.
    occurrence_flags: dict[str, bool] = {}
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        all_ds_names = [ds_name for ds_name, _url in resolved_participants] + list(map_dataset_results.keys())
        for ds_name in all_ds_names:
            cursor.execute(
                """
                SELECT is_occurance_available
                FROM dataset_master
                WHERE LOWER(title) = LOWER(%s)
                LIMIT 1;
                """,
                (ds_name,),
            )
            row = cursor.fetchone()
            if row is not None and "is_occurance_available" in row and row["is_occurance_available"] is not None:
                occurrence_flags[ds_name] = bool(row["is_occurance_available"])
            else:
                occurrence_flags[ds_name] = True  # local map datasets are occurrence-capable
    finally:
        conn.close()

    async with httpx.AsyncClient(timeout=10.0) as client:
        # If no fields are provided, perform a single "search everywhere" call
        # per dataset by passing an empty field string.
        #
        # CPMP species search v2 only accepts ``keyword`` in the JSON body —
        # never scientific_name or other field names. One POST per CPMP dataset
        # regardless of how many fields the UI selected.
        if payload.fields:
            tasks = []
            for participant_name, url in resolved_participants:
                if _is_cpmp_botanical_participant(participant_name, url):
                    tasks.append(
                        fetch_from_participant(
                            client,
                            participant_name,
                            url,
                            "",
                            payload.search_text,
                        )
                    )
                else:
                    for field in payload.fields:
                        tasks.append(
                            fetch_from_participant(
                                client,
                                participant_name,
                                url,
                                field,
                                payload.search_text,
                            )
                        )
        else:
            tasks = [
                fetch_from_participant(client, participant_name, url, "", payload.search_text)
                for (participant_name, url) in resolved_participants
            ]
        responses = await asyncio.gather(*tasks)

    # Group results by participant
    results = {}
    for item in responses:
        pname = item["participant_name"]
        if pname not in results:
            results[pname] = {
                "api_url": PARTICIPANT_FRONTEND_URLS.get(pname, item["api_url"]),
                "field_results": {},
                "is_occurance_available": occurrence_flags.get(pname, False),
            }
        if _is_cpmp_botanical_participant(pname, item["api_url"]):
            response_field = CPMP_BOTANICAL_FEDERATED_FIELD
        else:
            response_field = _canonical_field_name(item["field"])
        results[pname]["field_results"][response_field] = {
            "results": item["results"],
            "error": item.get("error")
        }

    # Merge in local map dataset results (datasets not in PARTICIPANTS)
    for ds_name, map_res in map_dataset_results.items():
        field_results = {}
        for f in payload.fields or [""]:
            field_results[_canonical_field_name(f) if f else "results"] = {
                "results": map_res.get("results", []),
                "error": map_res.get("error"),
            }
        results[ds_name] = {
            "api_url": f"local:{map_res.get('table_name', ds_name)}",
            "field_results": field_results,
            "is_occurance_available": occurrence_flags.get(ds_name, True),
        }

    valid_datasets = [name for (name, _url) in resolved_participants] + list(map_dataset_results.keys())

    if payload.display_fields:
        for pname in results:
            results[pname]["field_results"] = _apply_display_fields(
                results[pname]["field_results"], payload.display_fields
            )

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "valid_datasets": valid_datasets,
        "invalid_datasets": invalid_datasets,
        "fields": (
            [CPMP_BOTANICAL_FEDERATED_FIELD]
            if payload.fields
            and resolved_participants
            and all(
                _is_cpmp_botanical_participant(name, url)
                for (name, url) in resolved_participants
            )
            and not map_dataset_results
            else [_canonical_field_name(f) for f in payload.fields]
        ),
        "search_text": payload.search_text,
        "results": results
    }


@app.get("/platform-statistics")
def get_platform_statistics():
    """Return aggregated platform statistics.

    Reads the most recent row from the platform_statistics table and
    returns all its fields so the frontend can show overall counts.
    """

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT id,
                   total_datasets,
                   species_documented,
                   observations,
                   contributors,
                   last_updated
            FROM public.platform_statistics
            ORDER BY id DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()

        # If no stats row exists yet, return sensible defaults instead of 404
        if not row:
            return {
                "id": None,
                "total_datasets": 0,
                "species_documented": 0,
                "observations": 0,
                "contributors": 0,
                "last_updated": None,
            }

        return {
            "id": row["id"],
            "total_datasets": row["total_datasets"],
            "species_documented": row["species_documented"],
            "observations": row["observations"],
            "contributors": row["contributors"],
            "last_updated": row["last_updated"],
        }
    finally:
        conn.close()


def _get_map_datasets_availability(search_text: str, requested_datasets: list[str]) -> list[dict]:
    """
    For each dataset name in requested_datasets, check if a matching map table
    exists (matched by table name or display name) and whether search_text
    appears in any of its text columns.
    """
    results = []
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Load all registered map tables
        try:
            cursor.execute(
                f"SELECT DISTINCT layer_name "
                f"FROM {MAP_DB_SCHEMA}.upload_logs "
                "WHERE layer_name IS NOT NULL AND layer_name <> ''"
            )
            all_table_names: list[str] = [row["layer_name"] for row in cursor.fetchall()]
        except Exception:
            all_table_names = []

        # Build table_name → display_name from metadata.geoserver_name
        display_names: dict[str, str] = {}
        try:
            cursor.execute(
                f"SELECT geoserver_name, name_of_dataset "
                f"FROM {MAP_DB_SCHEMA}.metadata "
                "WHERE geoserver_name IS NOT NULL"
            )
            for row in cursor.fetchall():
                gn = (row["geoserver_name"] or "").strip()
                parts = gn.split(":", 1)
                table_key = parts[1] if len(parts) == 2 else gn
                if table_key and row["name_of_dataset"]:
                    display_names[table_key] = row["name_of_dataset"]
        except Exception:
            pass

        # field_name (lower) → ontology display label for matched_fields names
        field_display: dict[str, str] = {}
        try:
            cursor.execute(
                "SELECT LOWER(field_name) AS fn, ontology_mapping_to_display "
                "FROM dataset_mapping WHERE ontology_mapping_to_display IS NOT NULL"
            )
            for row in cursor.fetchall():
                if row["fn"] and row["ontology_mapping_to_display"]:
                    field_display[row["fn"]] = row["ontology_mapping_to_display"]
        except Exception:
            pass

        norm_display_to_table: dict[str, str] = {
            _normalize_dataset_name(dn): tn
            for tn, dn in display_names.items()
        }

        for requested in requested_datasets:
            req_norm = _normalize_dataset_name(requested)

            if req_norm in [_normalize_dataset_name(t) for t in all_table_names]:
                table_name = next(
                    t for t in all_table_names
                    if _normalize_dataset_name(t) == req_norm
                )
            elif req_norm in norm_display_to_table:
                table_name = norm_display_to_table[req_norm]
            else:
                continue  # not a map dataset

            display_name = display_names.get(table_name, table_name)

            # Get text-like columns
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND data_type IN (
                      'character varying', 'text', 'varchar', 'character', 'name'
                  )
                  AND column_name NOT IN ('dataset_id', 'geom')
                ORDER BY ordinal_position
                """,
                (MAP_DB_SCHEMA, table_name),
            )
            text_columns = [row["column_name"] for row in cursor.fetchall()]

            if not text_columns:
                results.append({
                    "dataset_name": display_name or table_name,
                    "available": False,
                    "count": 0,
                    "matched_fields": [],
                    "is_occurance_available": True,
                    **_fetch_layer_info(cursor, table_name),
                })
                continue

            like_term = f"%{search_text.lower()}%"
            conditions = " OR ".join(
                [f'LOWER(t."{col}") LIKE %(term)s' for col in text_columns]
            )

            # Single query: row count + per-column match flag via MAX(CASE...)
            col_cases = ", ".join(
                f'MAX(CASE WHEN LOWER(t."{col}") LIKE %(term)s THEN 1 ELSE 0 END) AS "m{i}"'
                for i, col in enumerate(text_columns)
            )
            try:
                cursor.execute(
                    f'SELECT COUNT(*) AS match_count, {col_cases} '
                    f'FROM {MAP_DB_SCHEMA}."{table_name}" t WHERE {conditions}',
                    {"term": like_term},
                )
                row = cursor.fetchone()
                count = int(row["match_count"]) if row else 0
                matched_cols = [
                    text_columns[i]
                    for i in range(len(text_columns))
                    if row and row.get(f"m{i}")
                ]
            except Exception:
                count = 0
                matched_cols = []

            matched_fields = [
                field_display.get(col.lower(), col) for col in matched_cols
            ]

            results.append({
                "dataset_name": display_name or table_name,
                "available": count > 0,
                "count": count,
                "matched_fields": matched_fields,
                "is_occurance_available": True,
                **_fetch_layer_info(cursor, table_name),
            })
    finally:
        conn.close()

    return results


@app.post("/pre-federated-search")
async def pre_federated_search(payload: FederatedSearchRequest = Body(...)):
    """
    Availability check — streams one SSE event per dataset as results arrive.

    Response: text/event-stream
      Each event:  data: <JSON object with dataset_name, available, count,
                         matched_fields, is_occurance_available>
      Final event: event: done
                   data: {"search_text": "...", "total": N, "cached": bool}

    Cached results (TTL=PRE_SEARCH_CACHE_TTL env var, default 5 min) are
    streamed immediately without hitting any external API.
    """
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(status_code=400, detail="At least one category must be 'biodiversity'.")

    search_text = payload.search_text.strip()
    if not search_text:
        raise HTTPException(status_code=400, detail="search_text must not be empty.")

    cache_key = _make_cache_key(search_text, payload.dataset)
    cached = _get_cached(cache_key)

    if cached is not None:
        async def _stream_cached():
            for entry in cached:
                yield f"data: {json.dumps(entry)}\n\n"
            yield f"event: done\ndata: {json.dumps({'search_text': search_text, 'total': len(cached), 'cached': True})}\n\n"
        return StreamingResponse(_stream_cached(), media_type="text/event-stream")

    # --- Resolve federation participants ---
    normalized_participants = {
        _normalize_dataset_name(name): (name, url)
        for name, url in PARTICIPANTS.items()
    }
    resolved: list[tuple[str, str]] = []
    for ds in payload.dataset:
        entry = normalized_participants.get(_normalize_dataset_name(ds))
        if entry:
            resolved.append(entry)
        elif _is_cpmp_botanical_participant(ds, ""):
            resolved.append((ds, CPMP_BOTANICAL_SEARCH_URL))

    # --- Occurrence flags + styles from DB (fast lookup, done before streaming) ---
    occurrence_flags: dict[str, bool] = {}
    participant_layer_info: dict[str, dict] = {}
    if resolved:
        conn = get_connection()
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            for name, _ in resolved:
                cursor.execute(
                    "SELECT is_occurance_available FROM dataset_master WHERE LOWER(title) = LOWER(%s) LIMIT 1;",
                    (name,),
                )
                row = cursor.fetchone()
                is_occ = (
                    bool(row["is_occurance_available"])
                    if row and row.get("is_occurance_available") is not None
                    else False
                )
                occurrence_flags[name] = is_occ
                if is_occ:
                    cursor.execute(
                        f"SELECT geoserver_name FROM {MAP_DB_SCHEMA}.metadata "
                        "WHERE LOWER(name_of_dataset) = LOWER(%s) LIMIT 1",
                        (name,),
                    )
                    meta_row = cursor.fetchone()
                    if meta_row and meta_row.get("geoserver_name"):
                        gn = meta_row["geoserver_name"].strip()
                        parts = gn.split(":", 1)
                        table_name = parts[1] if len(parts) == 2 else gn
                        participant_layer_info[name] = _fetch_layer_info(cursor, table_name)
        finally:
            conn.close()

    # --- Map datasets (sync DB query, run in thread pool) ---
    map_results: list[dict] = await asyncio.to_thread(
        _get_map_datasets_availability, search_text, payload.dataset
    )

    async def _stream_live():
        search_lower = search_text.lower()
        all_results: list[dict] = []

        # Federation participants — emit each result as soon as it arrives
        if resolved:
            async with httpx.AsyncClient(timeout=10.0) as client:
                tasks = [
                    asyncio.ensure_future(
                        fetch_from_participant(client, name, url, "", search_text)
                    )
                    for name, url in resolved
                ]
                for completed in asyncio.as_completed(tasks):
                    item = await completed
                    pname = item["participant_name"]
                    results_list = item.get("results") or []
                    count = len(results_list)

                    matched_fields_set: set[str] = set()
                    for result in results_list:
                        if not isinstance(result, dict):
                            continue
                        # CPMP API reports the matched field directly — use it
                        if result.get("matched_with"):
                            matched_fields_set.add(result["matched_with"])
                            continue
                        for key, value in result.items():
                            k_lower = key.lower()
                            if k_lower.endswith("_id") or k_lower in ("id", "matched_with"):
                                continue
                            if _value_contains(value, search_lower):
                                matched_fields_set.add(key)

                    entry = {
                        "dataset_name": pname,
                        "available": count > 0,
                        "count": count,
                        "matched_fields": sorted(matched_fields_set),
                        "is_occurance_available": occurrence_flags.get(pname, False),
                        **participant_layer_info.get(pname, {"styles": [], "titleColumn": None, "summaryColumn": []}),
                    }
                    all_results.append(entry)
                    yield f"data: {json.dumps(entry)}\n\n"

        # Map datasets — emit after federation results
        for r in map_results:
            all_results.append(r)
            yield f"data: {json.dumps(r)}\n\n"

        # Store in cache for future requests
        _SEARCH_CACHE[cache_key] = {"timestamp": time.time(), "datasets": all_results}

        yield f"event: done\ndata: {json.dumps({'search_text': search_text, 'total': len(all_results), 'cached': False})}\n\n"

    return StreamingResponse(_stream_live(), media_type="text/event-stream")


@app.get("/ping")
def ping():
    return {"ping": "pong"}


# ── federated-search-with-ontology ──────────────────────────────────────────

FUSEKI_SPARQL_ENDPOINT = "http://139.59.23.148:3030/myds/sparql"
# Named graphs loaded in Fuseki
_CML_GRAPH_PREFIX = "http://cml.org/ontology"


def _local_name_from_uri(uri: str) -> str:
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    if "/" in uri:
        return uri.rstrip("/").rsplit("/", 1)[-1]
    return uri


def _build_ontology_field_check_query(field: str) -> str:
    """SPARQL to find any CML ontology entity whose URI or label matches field."""
    field_bare = re.sub(r"[^a-z0-9]", "", field.lower())  # bare alphanumeric (e.g. "scientificname")
    field_spaced = field.lower().replace("_", " ")          # underscores → spaces
    field_escaped = re.escape(field.lower())                # for URI suffix regex

    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?entity WHERE {{
  {{
    GRAPH ?g {{
      ?entity ?p ?o .
      FILTER(
        REGEX(LCASE(STR(?entity)), "[/#]{field_escaped}$") ||
        REGEX(LCASE(STR(?entity)), "[/#]{field_bare}$")
      )
    }}
  }}
  UNION
  {{
    GRAPH ?g {{
      ?entity rdfs:label ?lbl .
      FILTER(
        LCASE(STR(?lbl)) = "{field.lower()}" ||
        LCASE(STR(?lbl)) = "{field_spaced}"
      )
    }}
  }}
}}
LIMIT 1
"""


async def _check_field_in_fuseki(client: httpx.AsyncClient, field: str) -> bool:
    """Return True if the field matches any entity in any CML ontology graph in Fuseki."""
    try:
        query = _build_ontology_field_check_query(field)
        response = await client.post(
            FUSEKI_SPARQL_ENDPOINT,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=10.0,
        )
        response.raise_for_status()
        bindings = response.json().get("results", {}).get("bindings", [])
        return len(bindings) > 0
    except Exception:
        return False


def _get_datasets_for_ontology_field(field: str) -> list[str]:
    """
    Query dataset_mapping to find dataset titles that have this ontology field.

    Matches on ontology_mapping = field (the canonical federated field identifier)
    OR field_name = field (the raw dataset column name).
    Returns a list of dataset_master.title values.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT DISTINCT master.title
            FROM dataset_mapping dm
            JOIN dataset_master master ON master.dataset_id = dm.dataset_id
            WHERE LOWER(dm.ontology_mapping) = LOWER(%s)
               OR LOWER(dm.field_name) = LOWER(%s)
            ORDER BY master.title
            """,
            (field, field),
        )
        rows = cursor.fetchall() or []
        return [row["title"] for row in rows if row.get("title")]
    except Exception:
        return []
    finally:
        conn.close()


@app.post("/federated-search-with-ontology")
async def federated_search_with_ontology(payload: FederatedSearchRequest = Body(...)):
    """
    Ontology-routed federated search.

    Same input/output shape as /federated-search.  Instead of searching all
    requested datasets, the API:
      1. Checks each requested field against the CML ontology in Fuseki.
      2. Looks up which datasets have that field registered in dataset_mapping.
      3. Searches ONLY those datasets for the given search_text.
    """
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(
            status_code=400,
            detail="At least one category must be 'biodiversity'.",
        )

    if not payload.fields:
        raise HTTPException(
            status_code=400,
            detail="At least one field is required for ontology-routed search.",
        )

    # ── Step 1: For each field, check Fuseki ontology + dataset_mapping ──────
    # field → list of dataset titles that carry this field
    field_to_dataset_titles: dict[str, list[str]] = {}

    async with httpx.AsyncClient(timeout=15.0) as ontology_client:
        for field in payload.fields:
            # Check Fuseki first (non-blocking on failure)
            in_fuseki = await _check_field_in_fuseki(ontology_client, field)

            # Query dataset_mapping for this field regardless of Fuseki result
            # (DB is the authoritative registry of field→dataset mappings)
            db_titles = _get_datasets_for_ontology_field(field)

            if in_fuseki or db_titles:
                field_to_dataset_titles[field] = db_titles

    if not field_to_dataset_titles:
        return {
            "category": payload.category,
            "dataset": payload.dataset,
            "valid_datasets": [],
            "invalid_datasets": payload.dataset,
            "fields": payload.fields,
            "search_text": payload.search_text,
            "results": {},
        }

    # ── Step 2: Collect all unique dataset titles from ontology mapping ───────
    all_ontology_datasets: set[str] = set()
    for titles in field_to_dataset_titles.values():
        all_ontology_datasets.update(titles)

    # ── Step 3: Resolve dataset titles to participant URLs ────────────────────
    normalized_participants = {
        _normalize_dataset_name(name): url for name, url in PARTICIPANTS.items()
    }

    resolved_participants: list[tuple[str, str]] = []
    unresolved: list[str] = []

    for ds in all_ontology_datasets:
        url = _resolve_participant_url(ds, normalized_participants)
        if url:
            resolved_participants.append((ds, url))
        else:
            unresolved.append(ds)

    # Check unresolved titles against local map DB
    map_dataset_results: dict[str, dict] = {}
    still_invalid: list[str] = []
    for ds in unresolved:
        map_result = await _fetch_map_dataset_results(ds, payload.search_text)
        if map_result is not None:
            map_dataset_results[ds] = map_result
        else:
            still_invalid.append(ds)

    invalid_datasets = still_invalid

    if not resolved_participants and not map_dataset_results:
        return {
            "category": payload.category,
            "dataset": payload.dataset,
            "valid_datasets": [],
            "invalid_datasets": list(all_ontology_datasets),
            "fields": [f for f in payload.fields if f in field_to_dataset_titles],
            "search_text": payload.search_text,
            "results": {},
        }

    # ── Step 4: Occurrence flags from dataset_master ──────────────────────────
    occurrence_flags: dict[str, bool] = {}
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        all_ds_names = (
            [name for name, _ in resolved_participants]
            + list(map_dataset_results.keys())
        )
        for ds_name in all_ds_names:
            cursor.execute(
                """
                SELECT is_occurance_available FROM dataset_master
                WHERE LOWER(title) = LOWER(%s) LIMIT 1;
                """,
                (ds_name,),
            )
            row = cursor.fetchone()
            if row is not None and row.get("is_occurance_available") is not None:
                occurrence_flags[ds_name] = bool(row["is_occurance_available"])
            else:
                occurrence_flags[ds_name] = True
    finally:
        conn.close()

    # ── Step 5: Fan-out search — only to ontology-mapped datasets ────────────
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = []
        for participant_name, url in resolved_participants:
            # Find which fields are mapped to this dataset
            fields_for_ds = [
                f for f, titles in field_to_dataset_titles.items()
                if participant_name in titles
            ]
            if not fields_for_ds:
                fields_for_ds = list(field_to_dataset_titles.keys())

            if _is_cpmp_botanical_participant(participant_name, url):
                tasks.append(
                    fetch_from_participant(
                        client, participant_name, url, "", payload.search_text
                    )
                )
            else:
                for field in fields_for_ds:
                    tasks.append(
                        fetch_from_participant(
                            client, participant_name, url, field, payload.search_text
                        )
                    )
        responses = await asyncio.gather(*tasks)

    # ── Step 6: Aggregate results (same shape as /federated-search) ───────────
    results: dict = {}
    for item in responses:
        pname = item["participant_name"]
        if pname not in results:
            results[pname] = {
                "api_url": PARTICIPANT_FRONTEND_URLS.get(pname, item["api_url"]),
                "field_results": {},
                "is_occurance_available": occurrence_flags.get(pname, False),
            }
        if _is_cpmp_botanical_participant(pname, item["api_url"]):
            response_field = CPMP_BOTANICAL_FEDERATED_FIELD
        else:
            response_field = _canonical_field_name(item["field"])
        results[pname]["field_results"][response_field] = {
            "results": item["results"],
            "error": item.get("error"),
        }

    # Merge local map dataset results
    for ds_name, map_res in map_dataset_results.items():
        fields_for_ds = [
            f for f, titles in field_to_dataset_titles.items()
            if ds_name in titles
        ] or list(field_to_dataset_titles.keys())
        field_results: dict = {}
        for f in fields_for_ds:
            key = _canonical_field_name(f) if f else "results"
            field_results[key] = {
                "results": map_res.get("results", []),
                "error": map_res.get("error"),
            }
        results[ds_name] = {
            "api_url": f"local:{map_res.get('table_name', ds_name)}",
            "field_results": field_results,
            "is_occurance_available": occurrence_flags.get(ds_name, True),
        }

    valid_datasets = (
        [name for name, _ in resolved_participants] + list(map_dataset_results.keys())
    )
    active_fields = [f for f in payload.fields if f in field_to_dataset_titles]

    if payload.display_fields:
        for pname in results:
            results[pname]["field_results"] = _apply_display_fields(
                results[pname]["field_results"], payload.display_fields
            )

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "valid_datasets": valid_datasets,
        "invalid_datasets": invalid_datasets,
        "fields": (
            [CPMP_BOTANICAL_FEDERATED_FIELD]
            if active_fields
            and resolved_participants
            and all(
                _is_cpmp_botanical_participant(name, url)
                for name, url in resolved_participants
            )
            and not map_dataset_results
            else [_canonical_field_name(f) for f in active_fields]
        ),
        "search_text": payload.search_text,
        "results": results,
    }


# ── federated-search-with-strict-ontology-check ─────────────────────────────

@app.post("/federated-search-with-strict-ontology-check")
async def federated_search_with_strict_ontology_check(payload: FederatedSearchRequest = Body(...)):
    """
    Strict-ontology federated search.

    Identical to /federated-search-with-ontology EXCEPT Step 1:
    a field is accepted ONLY when it matches an entity URI or rdfs:label in the
    CPHR Fuseki graph. Fields that exist only in dataset_mapping (e.g. drug_name,
    vernacular_name_common_names) are rejected with an empty result for that field.
    Dataset routing (Steps 2-6) is unchanged.
    """
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(
            status_code=400,
            detail="At least one category must be 'biodiversity'.",
        )

    if not payload.fields:
        raise HTTPException(
            status_code=400,
            detail="At least one field is required for ontology-routed search.",
        )

    # ── Step 1: Validate fields — Fuseki match REQUIRED ──────────────────────
    field_to_dataset_titles: dict[str, list[str]] = {}
    rejected_fields: list[str] = []

    async with httpx.AsyncClient(timeout=15.0) as ontology_client:
        for field in payload.fields:
            in_fuseki = await _check_field_in_fuseki(ontology_client, field)
            if not in_fuseki:
                rejected_fields.append(field)
                continue
            # Fuseki confirmed — use DB to find which datasets carry this field;
            # if no DB mapping exists, don't search anywhere for this field
            db_titles = _get_datasets_for_ontology_field(field)
            field_to_dataset_titles[field] = db_titles

    if not field_to_dataset_titles:
        return {
            "category": payload.category,
            "dataset": payload.dataset,
            "valid_datasets": [],
            "invalid_datasets": payload.dataset,
            "rejected_fields": rejected_fields,
            "fields": [],
            "search_text": payload.search_text,
            "results": {},
        }

    # ── Step 2: Collect all unique dataset titles from ontology mapping ───────
    all_ontology_datasets: set[str] = set()
    for titles in field_to_dataset_titles.values():
        all_ontology_datasets.update(titles)

    # ── Step 3: Resolve dataset titles to participant URLs ────────────────────
    normalized_participants = {
        _normalize_dataset_name(name): url for name, url in PARTICIPANTS.items()
    }

    resolved_participants: list[tuple[str, str]] = []
    unresolved: list[str] = []

    for ds in all_ontology_datasets:
        url = _resolve_participant_url(ds, normalized_participants)
        if url:
            resolved_participants.append((ds, url))
        else:
            unresolved.append(ds)

    map_dataset_results: dict[str, dict] = {}
    still_invalid: list[str] = []
    for ds in unresolved:
        map_result = await _fetch_map_dataset_results(ds, payload.search_text)
        if map_result is not None:
            map_dataset_results[ds] = map_result
        else:
            still_invalid.append(ds)

    invalid_datasets = still_invalid

    if not resolved_participants and not map_dataset_results:
        return {
            "category": payload.category,
            "dataset": payload.dataset,
            "valid_datasets": [],
            "invalid_datasets": list(all_ontology_datasets),
            "rejected_fields": rejected_fields,
            "fields": [f for f in payload.fields if f in field_to_dataset_titles],
            "search_text": payload.search_text,
            "results": {},
        }

    # ── Step 4: Occurrence flags from dataset_master ──────────────────────────
    occurrence_flags: dict[str, bool] = {}
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        all_ds_names = (
            [name for name, _ in resolved_participants]
            + list(map_dataset_results.keys())
        )
        for ds_name in all_ds_names:
            cursor.execute(
                """
                SELECT is_occurance_available FROM dataset_master
                WHERE LOWER(title) = LOWER(%s) LIMIT 1;
                """,
                (ds_name,),
            )
            row = cursor.fetchone()
            if row is not None and row.get("is_occurance_available") is not None:
                occurrence_flags[ds_name] = bool(row["is_occurance_available"])
            else:
                occurrence_flags[ds_name] = True
    finally:
        conn.close()

    # ── Step 5: Fan-out search — only to ontology-mapped datasets ────────────
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = []
        for participant_name, url in resolved_participants:
            fields_for_ds = [
                f for f, titles in field_to_dataset_titles.items()
                if participant_name in titles
            ]
            if not fields_for_ds:
                fields_for_ds = list(field_to_dataset_titles.keys())

            if _is_cpmp_botanical_participant(participant_name, url):
                tasks.append(
                    fetch_from_participant(
                        client, participant_name, url, "", payload.search_text
                    )
                )
            else:
                for field in fields_for_ds:
                    tasks.append(
                        fetch_from_participant(
                            client, participant_name, url, field, payload.search_text
                        )
                    )
        responses = await asyncio.gather(*tasks)

    # ── Step 6: Aggregate results (same shape as /federated-search) ───────────
    results: dict = {}
    for item in responses:
        pname = item["participant_name"]
        if pname not in results:
            results[pname] = {
                "api_url": PARTICIPANT_FRONTEND_URLS.get(pname, item["api_url"]),
                "field_results": {},
                "is_occurance_available": occurrence_flags.get(pname, False),
            }
        if _is_cpmp_botanical_participant(pname, item["api_url"]):
            response_field = CPMP_BOTANICAL_FEDERATED_FIELD
        else:
            response_field = _canonical_field_name(item["field"])
        results[pname]["field_results"][response_field] = {
            "results": item["results"],
            "error": item.get("error"),
        }

    for ds_name, map_res in map_dataset_results.items():
        fields_for_ds = [
            f for f, titles in field_to_dataset_titles.items()
            if ds_name in titles
        ] or list(field_to_dataset_titles.keys())
        field_results: dict = {}
        for f in fields_for_ds:
            key = _canonical_field_name(f) if f else "results"
            field_results[key] = {
                "results": map_res.get("results", []),
                "error": map_res.get("error"),
            }
        results[ds_name] = {
            "api_url": f"local:{map_res.get('table_name', ds_name)}",
            "field_results": field_results,
            "is_occurance_available": occurrence_flags.get(ds_name, True),
        }

    valid_datasets = (
        [name for name, _ in resolved_participants] + list(map_dataset_results.keys())
    )
    active_fields = [f for f in payload.fields if f in field_to_dataset_titles]

    if payload.display_fields:
        for pname in results:
            results[pname]["field_results"] = _apply_display_fields(
                results[pname]["field_results"], payload.display_fields
            )

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "valid_datasets": valid_datasets,
        "invalid_datasets": invalid_datasets,
        "rejected_fields": rejected_fields,
        "fields": (
            [CPMP_BOTANICAL_FEDERATED_FIELD]
            if active_fields
            and resolved_participants
            and all(
                _is_cpmp_botanical_participant(name, url)
                for name, url in resolved_participants
            )
            and not map_dataset_results
            else [_canonical_field_name(f) for f in active_fields]
        ),
        "search_text": payload.search_text,
        "results": results,
    }
