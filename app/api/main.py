"""Application entry point and FastAPI setup."""
import asyncio
import logging
from typing import Any, Dict, List

import httpx
from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Import controllers
from app.api import categories_controller
from app.api import dataset_details_controller
from app.api import dataset_master_controller
from app.api import metadata_controller
from app.api import ontology_controller
from app.api import spaces_controller

# Import configuration
from app.core.config import settings

logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI(
    title="cml - federation",
    version=settings.app_version,
    description="Central Server for biodiversity data management",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(metadata_controller.router)
app.include_router(categories_controller.router)
app.include_router(dataset_master_controller.router)
app.include_router(dataset_details_controller.router)
app.include_router(ontology_controller.router)
app.include_router(spaces_controller.router)

# Federated search configuration
PARTICIPANTS = {
    "Kew Plant Database": "http://134.209.145.106:8000/search",
    "Citizens' Portal of Medicinal Plants": "http://139.59.84.243:8050/search",
}


class FederatedSearchRequest(BaseModel):
    """Request model for federated search across participants.

    Attributes:
        category: List of categories to search (must include 'biodiversity').
        dataset: List of dataset identifiers (must be valid participants).
        fields: List of field names to search within.
        search_text: Search query text.
    """

    category: List[str] = Field(..., min_items=1)
    dataset: List[str] = Field(..., min_items=1)
    fields: List[str] = Field(..., min_items=1)
    search_text: str = Field(..., min_length=1)


async def fetch_from_participant(
    client: httpx.AsyncClient,
    participant_name: str,
    url: str,
    field: str,
    query: str,
) -> Dict[str, Any]:
    """Fetch search results from a participant API.

    Args:
        client: Async HTTP client.
        participant_name: Name of the participant.
        url: API endpoint URL.
        field: Field to search in.
        query: Search query string.

    Returns:
        Dictionary with search results or error information.
    """
    try:
        response = await client.get(url, params={"field": field, "query": query})
        response.raise_for_status()
        logger.info(
            "Federated search participant response received",
            extra={
                "participant": participant_name,
                "field": field,
                "result_count": len(response.json().get("results", [])),
            },
        )
        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": response.json().get("results", []),
        }
    except Exception as e:
        logger.warning(
            f"Federated search error from {participant_name}: {e}",
            extra={"participant": participant_name, "field": field},
        )
        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": [],
            "error": str(e),
        }


@app.post("/federated-search", response_model=Dict[str, Any])
async def federated_search(
    payload: FederatedSearchRequest = Body(...),
) -> Dict[str, Any]:
    """Execute federated search across registered participant databases.

    Sends search queries to all specified participant databases and aggregates
    results by participant and field.

    Args:
        payload: Federated search request parameters.

    Returns:
        Aggregated search results from all participants.

    Raises:
        HTTPException: If validation fails or no valid datasets provided.
    """
    # Validate category requirement
    if "biodiversity" not in [c.lower() for c in payload.category]:
        logger.warning(
            "Federated search rejected: missing biodiversity category",
            extra={"categories": payload.category},
        )
        raise HTTPException(
            status_code=400,
            detail="At least one category must be 'biodiversity'.",
        )

    # Validate and filter datasets
    valid_datasets = [ds for ds in payload.dataset if ds in PARTICIPANTS]
    invalid_datasets = [ds for ds in payload.dataset if ds not in PARTICIPANTS]

    if not valid_datasets:
        logger.warning(
            "Federated search rejected: no valid datasets",
            extra={"requested": payload.dataset},
        )
        raise HTTPException(
            status_code=400, detail="No valid datasets provided."
        )

    logger.info(
        "Federated search initiated",
        extra={
            "valid_datasets": len(valid_datasets),
            "invalid_datasets": len(invalid_datasets),
            "fields": len(payload.fields),
            "search_text": payload.search_text[:50],
        },
    )

    # Execute parallel requests to all participants
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [
            fetch_from_participant(
                client, participant, PARTICIPANTS[participant], field, payload.search_text
            )
            for participant in valid_datasets
            for field in payload.fields
        ]
        responses = await asyncio.gather(*tasks)

    # Aggregate results by participant
    results: Dict[str, Any] = {}
    for item in responses:
        pname = item["participant_name"]
        if pname not in results:
            results[pname] = {"api_url": item["api_url"], "field_results": {}}
        results[pname]["field_results"][item["field"]] = {
            "results": item["results"],
            "error": item.get("error"),
        }

    logger.info(
        "Federated search completed",
        extra={
            "participants_queried": len(results),
            "total_fields": sum(
                len(r.get("field_results", {})) for r in results.values()
            ),
        },
    )

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "valid_datasets": valid_datasets,
        "invalid_datasets": invalid_datasets,
        "fields": payload.fields,
        "search_text": payload.search_text,
        "results": results,
    }


@app.get("/ping", response_model=Dict[str, str])
def ping() -> Dict[str, str]:
    """Health check endpoint.

    Returns:
        Ping response indicating server is active.

    Example:
        >>> response = ping()
        >>> print(response)
        {'ping': 'pong'}
    """
    logger.debug("Health check (ping) endpoint called")
    return {"ping": "pong"}


@app.on_event("startup")
def startup_event() -> None:
    """Application startup event handler."""
    logger.info(
        f"Application started: {settings.app_name} v{settings.app_version}"
    )


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Application shutdown event handler."""
    logger.info(f"Application shutdown: {settings.app_name}")

