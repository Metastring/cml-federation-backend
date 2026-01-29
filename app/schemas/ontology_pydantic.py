"""Pydantic models for ontology domain - Request/Response validation."""
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# Domain Models
class DomainBaseModel(BaseModel):
    """Base domain model."""
    domain_code: str = Field(..., min_length=1, max_length=100)
    domain_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class DomainCreateModel(DomainBaseModel):
    """Model for creating a domain."""
    pass


class DomainUpdateModel(BaseModel):
    """Model for updating a domain."""
    domain_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    is_active: Optional[bool] = None


class DomainResponseModel(DomainBaseModel):
    """Model for domain response."""
    domain_id: UUID
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Ontology Version Models
class OntologyVersionBaseModel(BaseModel):
    """Base ontology version model."""
    version_code: str = Field(..., min_length=1, max_length=50)
    status: str = Field(..., pattern="^(draft|active|deprecated)$")
    description: Optional[str] = Field(None, max_length=1000)


class OntologyVersionCreateModel(BaseModel):
    """Model for creating an ontology version."""
    version_code: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = Field(None, max_length=1000)


class OntologyVersionUpdateModel(BaseModel):
    """Model for updating an ontology version."""
    status: Optional[str] = Field(None, pattern="^(draft|active|deprecated)$")
    description: Optional[str] = Field(None, max_length=1000)


class OntologyVersionResponseModel(OntologyVersionBaseModel):
    """Model for ontology version response."""
    ontology_version_id: UUID
    domain_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Concept Models
class ConceptBaseModel(BaseModel):
    """Base concept model."""
    concept_code: str = Field(..., min_length=1, max_length=100)
    concept_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    is_abstract: bool = False


class ConceptCreateModel(ConceptBaseModel):
    """Model for creating a concept."""
    parent_concept_id: Optional[UUID] = None


class ConceptUpdateModel(BaseModel):
    """Model for updating a concept."""
    concept_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    is_abstract: Optional[bool] = None


class ConceptResponseModel(ConceptBaseModel):
    """Model for concept response."""
    concept_id: UUID
    ontology_version_id: UUID
    parent_concept_id: Optional[UUID] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Property Models
class PropertyBaseModel(BaseModel):
    """Base property model."""
    property_code: str = Field(..., min_length=1, max_length=100)
    property_name: str = Field(..., min_length=1, max_length=255)
    data_type: str = Field(..., pattern="^(string|number|date|geometry|boolean)$")
    description: Optional[str] = Field(None, max_length=1000)
    is_required: bool = False
    is_multivalued: bool = False


class PropertyCreateModel(PropertyBaseModel):
    """Model for creating a property."""
    pass


class PropertyUpdateModel(BaseModel):
    """Model for updating a property."""
    property_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    is_required: Optional[bool] = None
    is_multivalued: Optional[bool] = None


class PropertyResponseModel(PropertyBaseModel):
    """Model for property response."""
    property_id: UUID
    concept_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Property Constraint Models
class PropertyConstraintBaseModel(BaseModel):
    """Base property constraint model."""
    constraint_type: str = Field(..., pattern="^(range|enum|regex|unit|geometry|vocab)$")
    constraint_value: Dict[str, Any] = Field(...)


class PropertyConstraintCreateModel(PropertyConstraintBaseModel):
    """Model for creating a property constraint."""
    pass


class PropertyConstraintUpdateModel(BaseModel):
    """Model for updating a property constraint."""
    constraint_type: Optional[str] = Field(None, pattern="^(range|enum|regex|unit|geometry|vocab)$")
    constraint_value: Optional[Dict[str, Any]] = None


class PropertyConstraintResponseModel(PropertyConstraintBaseModel):
    """Model for property constraint response."""
    constraint_id: UUID
    property_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Vocabulary Models
class VocabularyBaseModel(BaseModel):
    """Base vocabulary model."""
    vocab_code: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)


class VocabularyCreateModel(VocabularyBaseModel):
    """Model for creating a vocabulary."""
    pass


class VocabularyUpdateModel(BaseModel):
    """Model for updating a vocabulary."""
    description: Optional[str] = Field(None, max_length=1000)


class VocabularyResponseModel(VocabularyBaseModel):
    """Model for vocabulary response."""
    vocab_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Vocabulary Term Models
class VocabularyTermBaseModel(BaseModel):
    """Base vocabulary term model."""
    term_code: str = Field(..., min_length=1, max_length=100)
    term_label: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class VocabularyTermCreateModel(VocabularyTermBaseModel):
    """Model for creating a vocabulary term."""
    parent_term_id: Optional[UUID] = None


class VocabularyTermUpdateModel(BaseModel):
    """Model for updating a vocabulary term."""
    term_label: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class VocabularyTermResponseModel(VocabularyTermBaseModel):
    """Model for vocabulary term response."""
    term_id: UUID
    vocab_id: UUID
    parent_term_id: Optional[UUID] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Dataset Field Mapping Models
class DatasetFieldMappingBaseModel(BaseModel):
    """Base dataset field mapping model."""
    external_field_name: str = Field(..., min_length=1, max_length=255)
    external_data_type: Optional[str] = Field(None, max_length=50)
    transform_rule: Optional[Dict[str, Any]] = None
    is_exposed: bool = True


class DatasetFieldMappingCreateModel(DatasetFieldMappingBaseModel):
    """Model for creating a dataset field mapping."""
    pass


class DatasetFieldMappingUpdateModel(BaseModel):
    """Model for updating a dataset field mapping."""
    external_field_name: Optional[str] = Field(None, min_length=1, max_length=255)
    external_data_type: Optional[str] = Field(None, max_length=50)
    transform_rule: Optional[Dict[str, Any]] = None
    is_exposed: Optional[bool] = None


class DatasetFieldMappingResponseModel(DatasetFieldMappingBaseModel):
    """Model for dataset field mapping response."""
    mapping_id: UUID
    dataset_id: int
    concept_id: UUID
    property_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Dataset Concept Models
class DatasetConceptCreateModel(BaseModel):
    """Model for creating a dataset concept."""
    pass


class DatasetConceptResponseModel(BaseModel):
    """Model for dataset concept response."""
    dataset_concept_id: UUID
    dataset_id: int
    concept_id: UUID
    created_at: datetime

    class Config:
        from_attributes = True


# Ontology Change Log Models
class OntologyChangeLogBaseModel(BaseModel):
    """Base ontology change log model."""
    change_type: str = Field(...)
    description: str = Field(..., min_length=1, max_length=1000)
    changed_by: Optional[str] = Field(None, max_length=255)


class OntologyChangeLogCreateModel(OntologyChangeLogBaseModel):
    """Model for creating an ontology change log."""
    pass


class OntologyChangeLogResponseModel(OntologyChangeLogBaseModel):
    """Model for ontology change log response."""
    change_id: UUID
    ontology_version_id: UUID
    changed_at: datetime

    class Config:
        from_attributes = True


# List Response Models
class DomainListResponseModel(BaseModel):
    """Model for domain list response."""
    total: int
    domains: List[DomainResponseModel]


class ConceptListResponseModel(BaseModel):
    """Model for concept list response."""
    total: int
    concepts: List[ConceptResponseModel]


class PropertyListResponseModel(BaseModel):
    """Model for property list response."""
    total: int
    properties: List[PropertyResponseModel]


class VocabularyListResponseModel(BaseModel):
    """Model for vocabulary list response."""
    total: int
    vocabularies: List[VocabularyResponseModel]

# Backward compatibility aliases (Schema -> Model naming convention)
DomainCreateSchema = DomainCreateModel
DomainResponseSchema = DomainResponseModel
OntologyVersionCreateSchema = OntologyVersionCreateModel
OntologyVersionResponseSchema = OntologyVersionResponseModel
ConceptCreateSchema = ConceptCreateModel
ConceptResponseSchema = ConceptResponseModel
PropertyCreateSchema = PropertyCreateModel
PropertyResponseSchema = PropertyResponseModel
PropertyConstraintCreateSchema = PropertyConstraintCreateModel
PropertyConstraintResponseSchema = PropertyConstraintResponseModel
VocabularyCreateSchema = VocabularyCreateModel
VocabularyResponseSchema = VocabularyResponseModel
VocabularyTermCreateSchema = VocabularyTermCreateModel
VocabularyTermResponseSchema = VocabularyTermResponseModel
VocabularyTermSchema = VocabularyTermResponseModel
DatasetFieldMappingCreateSchema = DatasetFieldMappingCreateModel
DatasetFieldMappingResponseSchema = DatasetFieldMappingResponseModel
DatasetConceptCreateSchema = DatasetConceptCreateModel
DatasetConceptResponseSchema = DatasetConceptResponseModel
OntologyChangeLogCreateSchema = OntologyChangeLogCreateModel
OntologyChangeLogResponseSchema = OntologyChangeLogResponseModel