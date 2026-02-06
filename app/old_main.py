from fastapi import FastAPI, Query
import httpx
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import fields

app = FastAPI()

# Enable CORS for all origins (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the federated fields API
app.include_router(fields.router)

# Remote agents participating in federated search
REMOTE_AGENTS = {
    "http://134.209.145.106:8000/search": "kew - db",
    "http://139.59.87.3:5002/search": "cpmp - species db"
}

# Fetch search results from one agent
async def fetch_from_agent(client: httpx.AsyncClient, url: str, field: str, query: str):
    try:
        response = await client.get(url, params={"field": field, "query": query})
        response.raise_for_status()
        data = response.json()
        return REMOTE_AGENTS[url], data.get("results", [])
    except Exception as e:
        return REMOTE_AGENTS[url], [{"error": f"Failed to fetch from {url}: {str(e)}"}]

# Federated search endpoint
@app.get("/federated-search")
async def federated_search(field: str = Query(...), query: str = Query(...)):
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [fetch_from_agent(client, url, field, query) for url in REMOTE_AGENTS]
        results = await asyncio.gather(*tasks)

    named_results = {source_name: result_list for source_name, result_list in results}

    return {
        "field": field,
        "query": query,
        "results": named_results
    }
