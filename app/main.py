from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import httpx
import asyncio

# Include other internal modules
from app.db import get_connection
from app.endpoints import fields
from app.endpoints import metadata 
from app.endpoints import categories_router
from app.endpoints import submit_mapping
from fastapi.middleware.cors import CORSMiddleware

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
app.include_router(fields.router)
app.include_router(submit_mapping.router)
app.include_router(categories_router)

# Participant API endpoints
PARTICIPANTS = {
    "kew": "http://134.209.145.106:8000/search",
    "cpmp": "http://139.59.84.243:8050/search"
}

# Request payload model
class FederatedSearchRequest(BaseModel):
    category: list[str]
    dataset: list[str]
    search_text: str

async def fetch_from_participant(client, participant_name: str, url: str, field: str, query: str):
    try:
        response = await client.get(url, params={"field": field, "query": query})
        response.raise_for_status()
        return {
            "participant_name": participant_name,
            "api_url": url,
            "results": response.json().get("results", [])
        }
    except Exception as e:
        return {
            "participant_name": participant_name,
            "api_url": url,
            "results": [],
            "error": str(e)
        }

@app.post("/federated-search")
async def federated_search(payload: FederatedSearchRequest = Body(...)):
    if "biodiversity" not in [c.lower() for c in payload.category]:
        raise HTTPException(status_code=400, detail="At least one category must be 'biodiversity'.")

    # Validate requested datasets
    invalid = [ds for ds in payload.dataset if ds not in PARTICIPANTS]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown datasets: {', '.join(invalid)}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [
            fetch_from_participant(client, participant, PARTICIPANTS[participant], "vernacular_name_common_names", payload.search_text)
            for participant in payload.dataset
        ]
        responses = await asyncio.gather(*tasks)

    results = {
        item["participant_name"]: {
            "api_url": item["api_url"],
            "results": item["results"],
            "error": item.get("error")
        }
        for item in responses
    }

    return {
        "category": payload.category,
        "dataset": payload.dataset,
        "search_text": payload.search_text,
        "results": results
    }

@app.get("/ping")
def ping():
    return {"ping": "pong"}
