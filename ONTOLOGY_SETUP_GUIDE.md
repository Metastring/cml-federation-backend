# Ontology Tables Setup Guide

## Overview

This guide provides everything you need to set up ontology tables in PostgreSQL and use them with Pydantic models.

## Files Included

1. **`SQL_CREATE_TABLES.sql`** - Raw PostgreSQL CREATE TABLE statements
2. **`app/schemas/ontology_pydantic.py`** - Pydantic models for validation
3. **`app/models/ontology_model.py`** - SQLAlchemy ORM models
4. **`app/repositories/ontology_repository.py`** - Data access layer
5. **`app/services/ontology_service.py`** - Business logic layer

## Option 1: Using SQL Directly (Quickest)

### Step 1: Connect to PostgreSQL

```bash
# Using psql command line
psql -U your_username -d your_database_name -h localhost -p 5432

# Or using a PostgreSQL GUI (pgAdmin, DBeaver, etc.)
```

### Step 2: Run SQL Script

**Option A: Execute from file**
```bash
psql -U your_username -d your_database_name -f SQL_CREATE_TABLES.sql
```

**Option B: Copy-paste in your PostgreSQL client**
1. Open `SQL_CREATE_TABLES.sql`
2. Copy the entire content
3. Paste into your PostgreSQL client
4. Execute

**Option C: Run line by line**
```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Then copy each CREATE TABLE statement
```

### Step 3: Verify Tables Created

```sql
-- List all tables
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_name LIKE 'cml_%';

-- Expected output: 10 tables
-- cml_domain
-- cml_ontology_version
-- cml_concept
-- cml_property
-- cml_property_constraint
-- cml_vocab
-- cml_vocab_term
-- cml_dataset_field_mapping
-- cml_dataset_concept
-- cml_ontology_change_log
```

## Option 2: Using Alembic Migrations (Recommended for Production)

```bash
# Navigate to project root
cd /home/metastring/src/central_server

# Apply migrations
alembic upgrade head

# Verify migration applied
alembic current
```

## Pydantic Models

### Location
```
app/schemas/ontology_pydantic.py
```

### Available Models

#### Domain Models
- `DomainBaseModel` - Base fields
- `DomainCreateModel` - For creating domains
- `DomainUpdateModel` - For updating domains
- `DomainResponseModel` - API response
- `DomainListResponseModel` - List response

#### Ontology Version Models
- `OntologyVersionBaseModel`
- `OntologyVersionCreateModel`
- `OntologyVersionUpdateModel`
- `OntologyVersionResponseModel`

#### Concept Models
- `ConceptBaseModel`
- `ConceptCreateModel`
- `ConceptUpdateModel`
- `ConceptResponseModel`
- `ConceptListResponseModel`

#### Property Models
- `PropertyBaseModel`
- `PropertyCreateModel`
- `PropertyUpdateModel`
- `PropertyResponseModel`
- `PropertyListResponseModel`

#### Property Constraint Models
- `PropertyConstraintBaseModel`
- `PropertyConstraintCreateModel`
- `PropertyConstraintUpdateModel`
- `PropertyConstraintResponseModel`

#### Vocabulary Models
- `VocabularyBaseModel`
- `VocabularyCreateModel`
- `VocabularyUpdateModel`
- `VocabularyResponseModel`
- `VocabularyListResponseModel`

#### Vocabulary Term Models
- `VocabularyTermBaseModel`
- `VocabularyTermCreateModel`
- `VocabularyTermUpdateModel`
- `VocabularyTermResponseModel`

#### Dataset Mapping Models
- `DatasetFieldMappingBaseModel`
- `DatasetFieldMappingCreateModel`
- `DatasetFieldMappingUpdateModel`
- `DatasetFieldMappingResponseModel`

#### Change Log Models
- `OntologyChangeLogBaseModel`
- `OntologyChangeLogCreateModel`
- `OntologyChangeLogResponseModel`

### Usage Examples

#### Validate Domain Input

```python
from app.schemas.ontology_pydantic import DomainCreateModel

# This will validate automatically
domain_data = {
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain",
    "description": "Domain for biodiversity data"
}

domain_model = DomainCreateModel(**domain_data)
# Access validated data
print(domain_model.domain_code)  # "biodiversity"
```

#### API Endpoint with Pydantic

```python
from fastapi import APIRouter, HTTPException
from app.schemas.ontology_pydantic import (
    DomainCreateModel, 
    DomainResponseModel
)
from app.services.ontology_service import DomainService

router = APIRouter(prefix="/api/v1/ontology")

@router.post("/domains", response_model=DomainResponseModel, status_code=201)
def create_domain(
    request: DomainCreateModel,
    db: Session = Depends(get_db)
) -> DomainResponseModel:
    """Create a new domain."""
    service = DomainService(db)
    try:
        domain = service.create_domain(
            domain_code=request.domain_code,
            domain_name=request.domain_name,
            description=request.description
        )
        db.commit()
        return DomainResponseModel.from_orm(domain)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
```

#### List Response with Pagination

```python
from app.schemas.ontology_pydantic import DomainListResponseModel

@router.get("/domains", response_model=DomainListResponseModel)
def list_domains(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List all domains."""
    service = DomainService(db)
    domains = service.list_domains(skip=skip, limit=limit)
    
    return DomainListResponseModel(
        total=len(domains),
        domains=[DomainResponseModel.from_orm(d) for d in domains]
    )
```

#### Validation Example

```python
from pydantic import ValidationError
from app.schemas.ontology_pydantic import DomainCreateModel

# Valid data
try:
    domain = DomainCreateModel(
        domain_code="valid_code",
        domain_name="Valid Name"
    )
    print("✓ Valid")
except ValidationError as e:
    print("✗ Invalid:", e)

# Invalid data - code too long
try:
    domain = DomainCreateModel(
        domain_code="a" * 101,  # Exceeds 100 char limit
        domain_name="Valid Name"
    )
    print("✓ Valid")
except ValidationError as e:
    print("✗ Invalid:", e)
    # Output: "ensure this value has at most 100 characters"

# Invalid data - status invalid
try:
    from app.schemas.ontology_pydantic import OntologyVersionCreateModel
    version = OntologyVersionCreateModel(
        version_code="v1.0",
        status="invalid_status"  # Only 'draft', 'active', 'deprecated' allowed
    )
except ValidationError as e:
    print("✗ Invalid status")
```

## Table Structure

### cml_domain
```
┌──────────────────┬──────────┬─────────────────────┐
│ Column           │ Type     │ Constraints         │
├──────────────────┼──────────┼─────────────────────┤
│ domain_id        │ UUID     │ PRIMARY KEY         │
│ domain_code      │ VARCHAR  │ UNIQUE, NOT NULL    │
│ domain_name      │ VARCHAR  │ NOT NULL            │
│ description      │ TEXT     │ NULLABLE            │
│ is_active        │ BOOLEAN  │ DEFAULT TRUE        │
│ created_at       │ TIMESTAMP│ DEFAULT NOW()       │
│ updated_at       │ TIMESTAMP│ NULLABLE            │
└──────────────────┴──────────┴─────────────────────┘
```

### cml_concept (with hierarchy)
```
┌──────────────────┬──────────┬─────────────────────┐
│ Column           │ Type     │ Constraints         │
├──────────────────┼──────────┼─────────────────────┤
│ concept_id       │ UUID     │ PRIMARY KEY         │
│ ontology_version_│ UUID     │ FK, NOT NULL        │
│ concept_code     │ VARCHAR  │ NOT NULL            │
│ concept_name     │ VARCHAR  │ NOT NULL            │
│ description      │ TEXT     │ NULLABLE            │
│ parent_concept_id│ UUID     │ FK, NULLABLE        │
│ is_abstract      │ BOOLEAN  │ DEFAULT FALSE       │
│ created_at       │ TIMESTAMP│ DEFAULT NOW()       │
│ updated_at       │ TIMESTAMP│ NULLABLE            │
└──────────────────┴──────────┴─────────────────────┘
```

### cml_property_constraint (JSON storage)
```
┌──────────────────┬──────────┬─────────────────────┐
│ Column           │ Type     │ Constraints         │
├──────────────────┼──────────┼─────────────────────┤
│ constraint_id    │ UUID     │ PRIMARY KEY         │
│ property_id      │ UUID     │ FK, NOT NULL        │
│ constraint_type  │ VARCHAR  │ NOT NULL            │
│ constraint_value │ JSONB    │ NOT NULL            │
│ created_at       │ TIMESTAMP│ DEFAULT NOW()       │
│ updated_at       │ TIMESTAMP│ NULLABLE            │
└──────────────────┴──────────┴─────────────────────┘
```

## Sample Queries

### Insert Domain
```sql
INSERT INTO cml_domain (domain_code, domain_name, description)
VALUES ('biodiversity', 'Biodiversity Domain', 'For biodiversity data')
RETURNING domain_id;
```

### Create Version
```sql
INSERT INTO cml_ontology_version (domain_id, version_code, status)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 'v1.0', 'draft')
RETURNING ontology_version_id;
```

### Create Concept with Hierarchy
```sql
-- Parent concept
INSERT INTO cml_concept (
    ontology_version_id, 
    concept_code, 
    concept_name, 
    is_abstract
)
VALUES (
    '550e8400-e29b-41d4-a716-446655440010',
    'Organism',
    'Organism',
    TRUE
)
RETURNING concept_id;

-- Child concept
INSERT INTO cml_concept (
    ontology_version_id,
    concept_code,
    concept_name,
    parent_concept_id
)
VALUES (
    '550e8400-e29b-41d4-a716-446655440010',
    'Species',
    'Species',
    '550e8400-e29b-41d4-a716-446655440020'
)
RETURNING concept_id;
```

### Create Property with Constraint
```sql
-- First create property
INSERT INTO cml_property (
    concept_id,
    property_code,
    property_name,
    data_type,
    is_required
)
VALUES (
    '550e8400-e29b-41d4-a716-446655440030',
    'scientificName',
    'Scientific Name',
    'string',
    TRUE
)
RETURNING property_id;

-- Add regex constraint
INSERT INTO cml_property_constraint (
    property_id,
    constraint_type,
    constraint_value
)
VALUES (
    '550e8400-e29b-41d4-a716-446655440040',
    'regex',
    '{"pattern": "^[A-Z][a-z]+ [a-z]+$"}'
);
```

### Query Concept Hierarchy
```sql
-- Get all concepts with their parents
SELECT 
    c.concept_code,
    c.concept_name,
    c.parent_concept_id,
    p.concept_code as parent_code
FROM cml_concept c
LEFT JOIN cml_concept p ON c.parent_concept_id = p.concept_id
WHERE c.ontology_version_id = '550e8400-e29b-41d4-a716-446655440010'
ORDER BY c.concept_code;
```

### List Properties with Constraints
```sql
SELECT 
    p.property_code,
    p.property_name,
    p.data_type,
    p.is_required,
    pc.constraint_type,
    pc.constraint_value
FROM cml_property p
LEFT JOIN cml_property_constraint pc ON p.property_id = pc.property_id
WHERE p.concept_id = '550e8400-e29b-41d4-a716-446655440030'
ORDER BY p.property_code;
```

## Relationships

```
Domain (1) ──────────────┬─────────────── (N) OntologyVersion
                         │
                         ├─────────────── (N) Concept
                                           ├─────────────── (N) Property
                                           │                 ├─────────────── (N) PropertyConstraint
                                           │                 └─────────────── (N) DatasetFieldMapping
                                           └─────────────── Self-referencing (parent-child)

Vocabulary (1) ──────────┬─────────────── (N) VocabularyTerm
                         └─────────────── Self-referencing (parent-child)

DatasetFieldMapping connects:
- Dataset (external table)
- Concept
- Property

DatasetConcept connects:
- Dataset (external table)
- Concept
```

## Performance Considerations

### Indexes Created
- `cml_domain` (domain_code, is_active)
- `cml_ontology_version` (version_code, status, domain_id)
- `cml_concept` (concept_code, ontology_version_id, parent_concept_id)
- `cml_property` (property_code, concept_id, data_type)
- `cml_property_constraint` (property_id, constraint_type)
- `cml_vocab` (vocab_code)
- `cml_vocab_term` (vocab_id, term_code, parent_term_id)
- `cml_dataset_field_mapping` (dataset_id, concept_id, property_id)
- `cml_dataset_concept` (dataset_id, concept_id)
- `cml_ontology_change_log` (ontology_version_id, changed_at)

### Best Practices
1. Use indexed columns in WHERE clauses
2. Use eager loading for relationships (selectinload, joinedload)
3. Implement pagination for list endpoints (LIMIT, OFFSET)
4. Use JSONB indexes for complex constraint queries

## Troubleshooting

### UUID Extension Not Found
```sql
CREATE EXTENSION "uuid-ossp";
```

### Foreign Key Constraint Error
Ensure parent records exist before inserting child records:
```sql
-- Check if domain exists
SELECT domain_id FROM cml_domain WHERE domain_id = 'YOUR_ID';

-- Check if version exists
SELECT ontology_version_id FROM cml_ontology_version 
WHERE ontology_version_id = 'YOUR_ID';
```

### Connection Issues
```bash
# Test PostgreSQL connection
psql -U username -d database_name -h localhost -c "SELECT 1;"

# Check PostgreSQL service
sudo service postgresql status
```

## Next Steps

1. ✅ Run SQL CREATE TABLE statements
2. ✅ Verify tables created with: `\d` command
3. ✅ Use Pydantic models for API validation
4. ✅ Use SQLAlchemy ORM models for queries
5. ✅ Use Services for business logic
6. ✅ Use Repositories for data access

## Support

For issues or questions:
- Check the copilot-instructions.md for coding standards
- Review ONTOLOGY_IMPLEMENTATION.md for architecture details
- See ONTOLOGY_API.md for API endpoint documentation
