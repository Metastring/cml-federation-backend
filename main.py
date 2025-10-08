from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from queries.api.api import SpatialQueryAPI1

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(
    SpatialQueryAPI1.router,
    prefix=SpatialQueryAPI1.version,
    tags=["spatial-graphql"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "cmlapis"}


@app.get("/")
async def root():
    return {
        "message": "CML APIs",
        "endpoints": {
            "spatial_graphql": "/v1/graphql",
            "geoserver": "/geoserver",
            "health_check": "/health",
        },
    }
