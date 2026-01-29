from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class Scope(BaseModel):
    temporal_start_date: Optional[date] = None
    temporal_end_date: Optional[date] = None
    geographic_scope: Optional[str] = None
    taxonomic_scope: Optional[str] = None
    taxonomic_authority: Optional[str] = None

class Publisher(BaseModel):
    publisher_name: str
    record_count: int

class Contact(BaseModel):
    name: str
    role: str
    email: str
    organization: str
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

class Mapping(BaseModel):
    field_name: str
    ontology_mapping: str
    data_type: str

class MappingItem(BaseModel):
    field_name: str
    ontology_mapping: Optional[str] = None
    description: Optional[str] = None  # For future use

class DatasetMappingUpdateInput(BaseModel):
    dataset_id: str
    mappings: List[MappingItem]

class Metric(BaseModel):
    metric_name: str
    metric_value: str

class Statistic(BaseModel):
    stat_name: str
    stat_value: str
    measurement_date: Optional[date] = None

class DatasetDetailsInput(BaseModel):
    dataset_id: str
    scopes: Optional[List[Scope]] = []
    publishers: Optional[List[Publisher]] = []
    contacts: Optional[List[Contact]] = []
    mappings: Optional[List[Mapping]] = []
    metrics: Optional[List[Metric]] = []
    statistics: Optional[List[Statistic]] = []

class CategoryInput(BaseModel):
    category_id: Optional[str] = None
    category_name: Optional[str] = None

class SourceInput(BaseModel):
    source_name: str
    base_url: Optional[str] = None
    description: Optional[str] = None

class DatasetRegistryInput(BaseModel):
    category: Optional[CategoryInput] = None
    title: str
    description: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    language: Optional[str] = None
    data_language: Optional[str] = None
    license: Optional[str] = None
    is_active: Optional[bool] = True
    keywords: Optional[str] = None
    dataset_type: Optional[str] = None
    publishers: Optional[List[Publisher]] = []
    contacts: Optional[List[Contact]] = []
    sources: Optional[List[SourceInput]] = []
    statistics: Optional[List[Statistic]] = []
