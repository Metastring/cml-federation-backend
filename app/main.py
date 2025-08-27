from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import httpx
import asyncio

# Include other internal modules
from app.db import get_connection
from app.endpoints import metadata
from app.endpoints import categories_router

from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import dataset_master
from app.endpoints import dataset_details

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

# Participant API endpoints
PARTICIPANTS = {
    "kew": "http://134.209.145.106:8000/search",
    "cpmp": "http://139.59.84.243:8050/search"
}

# Updated request payload model
class FederatedSearchRequest(BaseModel):
    category: list[str]
    dataset: list[str]
    fields: list[str]
    search_text: str

async def fetch_from_participant(client, participant_name: str, url: str, field: str, query: str):
    try:
        response = await client.get(url, params={"field": field, "query": query})
        response.raise_for_status()
        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": response.json().get("results", [])
        }
    except Exception as e:
        return {
            "participant_name": participant_name,
            "field": field,
            "api_url": url,
            "results": [],
            "error": str(e)
        }

@app.post("/federated-search")
async def federated_search(payload: FederatedSearchRequest = Body(...)):
    # Check category
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(status_code=400, detail="At least one category must be 'biodiversity'.")

    # Validate datasets
    invalid = [ds for ds in payload.dataset if ds not in PARTICIPANTS]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown datasets: {', '.join(invalid)}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Create one task per dataset-field combination
        tasks = [
            fetch_from_participant(client, participant, PARTICIPANTS[participant], field, payload.search_text)
            for participant in payload.dataset
            for field in payload.fields
        ]
        responses = await asyncio.gather(*tasks)

    # Group results by participant
    results = {}
    for item in responses:
        pname = item["participant_name"]
        if pname not in results:
            results[pname] = {
                "api_url": item["api_url"],
                "field_results": {}
            }
        results[pname]["field_results"][item["field"]] = {
            "results": item["results"],
            "error": item.get("error")
        }

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "fields": payload.fields,
        "search_text": payload.search_text,
        "results": results
    }

@app.get("/ping")
def ping():
    return {"ping": "pong"}
