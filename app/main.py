
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
import asyncio
import json

# Include other internal modules
from app.db import get_connection
from app.endpoints import metadata
from app.endpoints import categories_router
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import dataset_master
from app.endpoints import dataset_details
from app.endpoints import ontology
from psycopg2.extras import RealDictCursor



app = FastAPI()

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

# Participant API endpoints
PARTICIPANTS = {
    "Kew Plant Database": "http://134.209.145.106:8000/search",
    "Citizens’ Portal of Medicinal Plants": "http://139.59.84.243:8050/search",
    "CPMP Botanical Source": "http://139.59.84.243:9088/cml/search",
    "CPMP Drug Source": "http://139.59.84.243:9087/search/search/drugname",
    "Traded Medicinal Plants of India (TMPI)": "https://tradedmedicinalplants.org/kew/webapi/advance/search",
    "Ayurahaar – The Ahara & Nutrition Portal": "https://ayurahaar.org/FoodType/webapi/ingredient/ingredient-property-list",
    "Rasashastra: A Database of Metals and Minerals used in Ayurveda": "https://rasashastra.tdu.edu.in/mm_api/advanced/search",
}


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

    return name.strip().lower().replace("’", "'")


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

# Updated request payload model
class FederatedSearchRequest(BaseModel):
    category: list[str]
    dataset: list[str]
    fields: list[str]
    search_text: str

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

                normalised_items.append({
                    "drug_id": item.get("officialDrugId"),
                    "drug_name": item.get("officialDrugName"),
                    "english_name": item.get("officialDrugNameEnglish"),
                    "sanskrit_name": item.get("officialDrugNameSanskrit"),
                })

            # If a specific field is requested, project results down
            # to ID columns plus that field, consistent with other
            # participants.
            if field and field.strip():
                normalised_items = [
                    _project_biodiversity_result(row, field)
                    for row in normalised_items
                ]

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # Special handling for CPMP Botanical Source which now exposes
        # a GET API for species search.
        if "139.59.84.243:9088/cml/search" in url:
            # Map our federated field to the CPMP "search_parameter".
            # The CPMP API only accepts "scientific_name" or "common_name".
            field_norm = (field or "").strip().lower()
            if field_norm in {"scientific_name", "taxon_name"}:
                search_parameter = "scientific_name"
            elif field_norm in {"vernacular_name_common_names", "common_name", "common_names"}:
                search_parameter = "common_name"
            else:
                # Default to common_name when the requested field
                # does not match one of the known scientific/common
                # name identifiers.
                search_parameter = "common_name"

            params = {
                "search_parameter": search_parameter,
                "search_text": query or "",
            }

            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json() or []

            # New CPMP API returns a top-level list of taxa.
            raw_items = data if isinstance(data, list) else data.get("results", [])
            normalised_items = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                normalised_items.append({
                    "taxon_id": item.get("taxonId"),
                    "taxon_name": item.get("taxonName"),
                    "common_names": item.get("commonNames"),
                })

            # If a specific field is requested, project results down
            # to ID columns plus that field, consistent with other
            # biodiversity datasets.
            if field and field.strip():
                normalised_items = [
                    _project_biodiversity_result(row, field)
                    for row in normalised_items
                ]

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

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

            filtered_results = []
            if query_norm:
                field_norm = (field or "").strip().lower()
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # If a specific field is requested, restrict matching to that
                    # field; otherwise, search across the full object.
                    target_text = None
                    if field_norm in {"ingredient_name", "ingredient_common_name"}:
                        target_text = (item.get("ingredient_common_name") or "").lower()
                    elif field_norm in {"sanskrit_name"}:
                        target_text = (item.get("sanskrit_name") or "").lower()

                    # Generic case: if the requested field name exists as a
                    # top-level key in the Ayurahaar ingredient object, search
                    # only within that column.
                    if target_text is None and field and field in item:
                        value = item.get(field)
                        if value is not None:
                            target_text = str(value).lower()

                    if target_text is not None:
                        if query_norm in target_text:
                            filtered_results.append(item)
                        continue

                    # Fallback: search everywhere in the object payload
                    try:
                        blob = json.dumps(item, ensure_ascii=False).lower()
                    except TypeError:
                        continue

                    if query_norm in blob:
                        filtered_results.append(item)

            # Normalise each hit: ingredient_common_name -> common_name,
            # sanskrit_name -> both taxon_name and sanskrit_name, and
            # surface ingredient_id.
            normalised_items = []
            for item in filtered_results:
                normalised_items.append({
                    "ingredient_id": item.get("ingredient_id"),
                    "taxon_name": item.get("sanskrit_name"),
                    "sanskrit_name": item.get("sanskrit_name"),
                    "common_name": item.get("ingredient_common_name"),
                })

            # If a specific field is requested, project results down to
            # ID + that field (e.g. ingredient_id + taxon_name for
            # "sanskrit_name").
            if field and field.strip():
                normalised_items = [
                    _project_biodiversity_result(row, field)
                    for row in normalised_items
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

                normalised_items.append({
                    "drug_id": item.get("drugId") or modern_info.get("drugId"),
                    "drugName": modern_info.get("drugName") or nomenclature.get("drugName"),
                    "synonyms": nomenclature.get("synonyms"),
                    "dosageAndTreatment": dosage,
                    "category": nomenclature.get("category"),
                })

            # If a specific field is requested, project results down to
            # ID + that field (e.g. drug_id + drugName).
            if field and field.strip():
                normalised_items = [
                    _project_biodiversity_result(row, field)
                    for row in normalised_items
                ]

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

                scientific_list = item.get("scientificName") or []
                common_list = item.get("commonName") or []

                taxon_name = None
                if isinstance(scientific_list, list) and scientific_list:
                    taxon_name = scientific_list[0]
                elif isinstance(scientific_list, str):
                    taxon_name = scientific_list

                common_names = None
                if isinstance(common_list, list):
                    common_names = ", ".join([c for c in common_list if c]) or None
                elif isinstance(common_list, str):
                    common_names = common_list

                normalised_items.append({
                    "taxon_id": item.get("plantId"),
                    "taxon_name": taxon_name,
                    "common_names": common_names,
                })

            # If the caller requested a specific field, project the
            # results down to only ID columns + that field.
            if field and field.strip():
                normalised_items = [
                    _project_biodiversity_result(row, field)
                    for row in normalised_items
                ]

            return {
                "participant_name": participant_name,
                "field": field,
                "api_url": url,
                "results": normalised_items,
            }

        # Default behaviour for participants that follow the common contract
        # If no specific field is provided (or a wildcard like * / all),
        # omit the field parameter so the participant can search globally.
        params = {"query": query}
        if field and field.strip() and field.strip().lower() not in {"*", "all"}:
            params["field"] = field

        response = await client.get(url, params=params)
        response.raise_for_status()
        raw_results = response.json().get("results", [])

        # For biodiversity participants that follow the common Kew/CPMP
        # contract, if a specific field was requested we only return ID
        # columns plus that field.
        biodiversity_participants = {
            "Kew Plant Database",
            "Citizens’ Portal of Medicinal Plants",
            "Citizens' Portal of Medicinal Plants",
        }

        if field and field.strip() and participant_name in biodiversity_participants:
            processed_results = []
            for row in raw_results:
                if isinstance(row, dict):
                    processed_results.append(_project_biodiversity_result(row, field))
                else:
                    processed_results.append(row)
        else:
            processed_results = raw_results

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
        norm = _normalize_dataset_name(ds)
        url = normalized_participants.get(norm)
        if url:
            resolved_participants.append((ds, url))
        else:
            invalid_datasets.append(ds)

    if not resolved_participants:
        raise HTTPException(status_code=400, detail="No valid datasets provided.")

    # Look up is_occurance_availabe flag from dataset_master for each
    # resolved dataset title, so we can surface it in the federated
    # results without changing the overall response shape.
    occurrence_flags: dict[str, bool] = {}
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        for ds_name, _url in resolved_participants:
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
                occurrence_flags[ds_name] = False
    finally:
        conn.close()

    async with httpx.AsyncClient(timeout=10.0) as client:
        # If no fields are provided, perform a single "search everywhere" call
        # per dataset by passing an empty field string.
        if payload.fields:
            tasks = [
                fetch_from_participant(client, participant_name, url, field, payload.search_text)
                for (participant_name, url) in resolved_participants
                for field in payload.fields
            ]
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
                "api_url": item["api_url"],
                "field_results": {},
                "is_occurance_available": occurrence_flags.get(pname, False),
            }
        response_field = _canonical_field_name(item["field"])
        results[pname]["field_results"][response_field] = {
            "results": item["results"],
            "error": item.get("error")
        }

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "valid_datasets": [name for (name, _url) in resolved_participants],
        "invalid_datasets": invalid_datasets,   # <--- include info for debugging
        "fields": [_canonical_field_name(f) for f in payload.fields],
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


@app.get("/ping")
def ping():
    return {"ping": "pong"}
