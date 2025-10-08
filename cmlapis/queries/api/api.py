import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from typing import List

from queries.service.service import (
    fetch_summary_tables,
    fetch_categories,
    fetch_summary_tables_by_category,
)
from queries.models.model import SummaryTableType


class SpatialQueryAPI1:
    version = "/v1"
    router = APIRouter()


@strawberry.type
class Query:
    @strawberry.field
    def summaryTables(self) -> List[SummaryTableType]:
        try:
            rows = fetch_summary_tables()
            return [SummaryTableType(**row) for row in rows]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @strawberry.field
    def categories(self) -> List[str]:
        try:
            return fetch_categories()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @strawberry.field
    def summaryTablesByCategory(self, category: str) -> List[SummaryTableType]:
        try:
            rows = fetch_summary_tables_by_category(category)
            return [SummaryTableType(**row) for row in rows]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


schema = strawberry.Schema(query=Query)
graphql_router = GraphQLRouter(schema)
SpatialQueryAPI1.router.include_router(graphql_router, prefix="/graphql")


