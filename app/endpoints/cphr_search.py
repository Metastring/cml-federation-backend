"""
cphr-search: Aayuraahar ingredient search across all Ayurahaar lookup and
ingredient-property APIs.

Search flow
-----------
Phase 1 – Lookup APIs (all fetched in parallel):
  • /foodGroup            – match food-group names
  • /nin_properties       – match NIN property names / categories
  • /traitCategory/Dravyaguna/traitNames  – match Ayurvedic trait values
  • /traitCategory/Karma/traitNames       – match Karma trait values
  • /traitCategory/Ayurveda Vargas/traitNames  – used in text search only
                                               (no dedicated POST filter key)

Phase 2 – ingredient-property-list POST:
  • One call per matched filter category (FoodGroup, NinProperties,
    Dravyaguna, Karma) – returns all ingredients in those categories.
  • One call with empty filters – all ingredients; filtered locally by
    text match across all fields.

Phase 3 – Merge, deduplicate by ingredient_id, and format.

Response shape per result
-------------------------
{
  "ingredient_id": <int>,
  "nin_food_group_id": <int|null>,
  "is_grouped_ingredient": <bool>,
  "display_object": {
      "Common Name": "...",
      "Sanskrit Name": "...",
      "Food Group": "...",
      "Part": "...",
      "Rasa(Taste)": "...",
      ...   (all present trait names)
  }
}
"""

from fastapi import APIRouter
from pydantic import BaseModel
import httpx
import asyncio

router = APIRouter(prefix="/cphr-search", tags=["cphr-search"])

_BASE = "https://ayurahaar.org/FoodType/webapi/ingredient"
_INGREDIENT_LIST_URL = f"{_BASE}/ingredient-property-list"
_FOOD_GROUP_URL = f"{_BASE}/foodGroup"
_NIN_PROPERTIES_URL = f"{_BASE}/nin_properties"
_DRAVYAGUNA_TRAITS_URL = f"{_BASE}/traitCategory/Dravyaguna/traitNames"
_KARMA_TRAITS_URL = f"{_BASE}/traitCategory/Karma/traitNames"
_VARGAS_TRAITS_URL = f"{_BASE}/traitCategory/Ayurveda%20Vargas/traitNames"


class AayuraaharSearchRequest(BaseModel):
    """Same input contract as /federated-search."""
    category: list[str]
    dataset: list[str]
    fields: list[str]
    search_text: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _contains(text, query: str) -> bool:
    """Case-insensitive partial match."""
    return query in str(text or "").lower()


def _build_display_object(item: dict) -> dict:
    """
    Return a stable key-value dict suitable for display.

    Flat fields are mapped to human-readable labels.
    Trait values are grouped by trait_name (multiple values joined with ', ').
    IDs are intentionally excluded — they live outside display_object.
    """
    display: dict = {}

    flat_fields = [
        ("ingredient_common_name", "Common Name"),
        ("sanskrit_name", "Sanskrit Name"),
        ("nin_food_group", "Food Group"),
        ("part_name", "Part"),
        ("process", "Process"),
        ("color", "Color"),
    ]
    for src, label in flat_fields:
        val = item.get(src)
        if val:
            display[label] = val

    # Group trait values by trait_name
    traits: dict[str, list[str]] = {}
    for trait in (item.get("trait_detail") or []):
        if not isinstance(trait, dict):
            continue
        tname = (trait.get("trait_name") or "").strip()
        tval = (trait.get("trait_value") or "").strip()
        if tname and tval:
            traits.setdefault(tname, []).append(tval)

    for tname, vals in traits.items():
        display[tname] = ", ".join(vals)

    return display


def _text_matches(item: dict, query: str) -> bool:
    """Return True if query appears anywhere in the ingredient's searchable content."""
    for field in (
        "ingredient_common_name", "sanskrit_name", "nin_food_group",
        "part_name", "process", "color",
    ):
        if _contains(item.get(field), query):
            return True
    for trait in (item.get("trait_detail") or []):
        if isinstance(trait, dict):
            if _contains(trait.get("trait_value"), query) or _contains(trait.get("trait_name"), query):
                return True
    return False


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.post("/aayuraahar-search")
async def aayuraahar_search(payload: AayuraaharSearchRequest):
    """
    Search Ayurahaar ingredient data using the given search_text.

    Accepts the same body as /federated-search (category, dataset, fields,
    search_text) so existing clients can call it without changes.
    Only search_text drives the search logic.
    """
    query = payload.search_text.strip().lower()

    async with httpx.AsyncClient(timeout=20.0) as client:

        # ── Phase 1: fetch all lookup APIs in parallel ──────────────────────
        (
            fg_resp, np_resp, dg_resp, km_resp, vg_resp
        ) = await asyncio.gather(
            client.get(_FOOD_GROUP_URL),
            client.get(_NIN_PROPERTIES_URL),
            client.get(_DRAVYAGUNA_TRAITS_URL),
            client.get(_KARMA_TRAITS_URL),
            client.get(_VARGAS_TRAITS_URL),
            return_exceptions=True,
        )

        matched_food_groups: list[int] = []
        matched_nin_props: list[int] = []
        matched_dravyaguna: list[int] = []
        matched_karma: list[int] = []

        if not isinstance(fg_resp, Exception) and fg_resp.status_code == 200:
            for fg in (fg_resp.json() or []):
                if _contains(fg.get("foodGroup", ""), query):
                    matched_food_groups.append(fg["foodGroupId"])

        if not isinstance(np_resp, Exception) and np_resp.status_code == 200:
            for np_item in (np_resp.json() or []):
                if (
                    _contains(np_item.get("ninPropertyName", ""), query)
                    or _contains(np_item.get("ninPropertyCategory", ""), query)
                ):
                    matched_nin_props.append(np_item["ninPropertyId"])

        if not isinstance(dg_resp, Exception) and dg_resp.status_code == 200:
            for item in ((dg_resp.json() or {}).get("Dravyaguna") or []):
                if _contains(item.get("Value", ""), query) or _contains(item.get("Trait_Name", ""), query):
                    vid = _safe_int(item.get("Value_ID"))
                    if vid is not None:
                        matched_dravyaguna.append(vid)

        if not isinstance(km_resp, Exception) and km_resp.status_code == 200:
            for item in ((km_resp.json() or {}).get("Karma") or []):
                if _contains(item.get("Value", ""), query) or _contains(item.get("Trait_Name", ""), query):
                    vid = _safe_int(item.get("Value_ID"))
                    if vid is not None:
                        matched_karma.append(vid)

        # Ayurveda Vargas: no dedicated POST filter key, but searching
        # ingredient trait_detail text in Phase 2 covers it.

        # ── Phase 2: POST ingredient-property-list ──────────────────────────
        # Build one POST body per matched filter category.
        # Always include a full-list call (empty filters) for text search.

        post_calls: list[tuple[str, dict]] = [
            ("all", {"FoodGroup": [], "NinProperties": [], "Dravyaguna": [], "Karma": []}),
        ]
        if matched_food_groups:
            post_calls.append((
                "food_group",
                {"FoodGroup": matched_food_groups, "NinProperties": [], "Dravyaguna": [], "Karma": []},
            ))
        if matched_dravyaguna:
            post_calls.append((
                "dravyaguna",
                {"FoodGroup": [], "NinProperties": [], "Dravyaguna": matched_dravyaguna, "Karma": []},
            ))
        if matched_karma:
            post_calls.append((
                "karma",
                {"FoodGroup": [], "NinProperties": [], "Dravyaguna": [], "Karma": matched_karma},
            ))
        if matched_nin_props:
            post_calls.append((
                "nin_properties",
                {"FoodGroup": [], "NinProperties": matched_nin_props, "Dravyaguna": [], "Karma": []},
            ))

        post_responses = await asyncio.gather(
            *[client.post(_INGREDIENT_LIST_URL, json=body) for _, body in post_calls],
            return_exceptions=True,
        )

    # ── Phase 3: merge, deduplicate, format ─────────────────────────────────
    collected: dict[int, dict] = {}  # ingredient_id → raw item

    for (call_type, _), resp in zip(post_calls, post_responses):
        if isinstance(resp, Exception) or resp.status_code != 200:
            continue
        items = resp.json()
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            iid = item.get("ingredient_id")
            if iid is None:
                continue

            if call_type == "all":
                # Only include when the raw text matches somewhere in the data
                if _text_matches(item, query):
                    collected[iid] = item
            else:
                # Filter-based call: include everything returned
                collected[iid] = item

    results = [
        {
            "ingredient_id": iid,
            "nin_food_group_id": item.get("nin_food_group_id"),
            "is_grouped_ingredient": item.get("isGroupedIngredient"),
            "display_object": _build_display_object(item),
        }
        for iid, item in sorted(collected.items())
    ]

    return {
        "search_text": payload.search_text,
        "total": len(results),
        "matched_filters": {
            "food_group_ids": matched_food_groups,
            "nin_property_ids": matched_nin_props,
            "dravyaguna_value_ids": matched_dravyaguna,
            "karma_value_ids": matched_karma,
        },
        "results": results,
    }
