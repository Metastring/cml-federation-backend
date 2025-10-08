from typing import Optional
from pydantic import BaseModel
import strawberry


class SummaryTable(BaseModel):
    id: int
    table_no: Optional[str] = None
    title: Optional[str] = None
    main_fields: Optional[str] = None
    category: Optional[str] = None
    table_slug: Optional[str] = None

    class Config:
        from_attributes = True


@strawberry.type
class SummaryTableType:
    id: int
    table_no: Optional[str] = None
    title: Optional[str] = None
    main_fields: Optional[str] = None
    category: Optional[str] = None
    table_slug: Optional[str] = None


