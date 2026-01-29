# Ontology System - Quick Start Guide

## What You Have

### 1. Database Setup
📄 **File:** `SQL_CREATE_TABLES.sql` (237 lines)

Contains 10 PostgreSQL CREATE TABLE statements:
- `cml_domain` - Top-level domains
- `cml_ontology_version` - Versioned ontologies (draft/active/deprecated)
- `cml_concept` - Concepts with hierarchy support (parent_concept_id)
- `cml_property` - Properties on concepts
- `cml_property_constraint` - Validation rules (JSON storage)
- `cml_vocab` - Controlled vocabularies
- `cml_vocab_term` - Vocabulary items with hierarchy
- `cml_dataset_field_mapping` - Map external datasets to ontology (JSON transforms)
- `cml_dataset_concept` - Dataset-concept associations
- `cml_ontology_change_log` - Audit trail for all changes

**Ready to use:** Copy entire contents and run in PostgreSQL client

### 2. Pydantic Validation Models
📄 **File:** `app/schemas/ontology_pydantic.py` (324 lines)

Contains 15+ Pydantic model classes:
- `DomainBaseModel`, `DomainCreateModel`, `DomainUpdateModel`, `DomainResponseModel`
- `OntologyVersionBaseModel`, `OntologyVersionCreateModel`, `OntologyVersionResponseModel`
- `ConceptBaseModel`, `ConceptCreateModel`, `ConceptUpdateModel`, `ConceptResponseModel`
- `PropertyBaseModel`, `PropertyCreateModel`, `PropertyUpdateModel`, `PropertyResponseModel`
- `PropertyConstraintBaseModel`, `PropertyConstraintCreateModel`, `PropertyConstraintResponseModel`
- `VocabularyBaseModel`, `VocabularyCreateModel`, `VocabularyResponseModel`
- `VocabularyTermBaseModel`, `VocabularyTermCreateModel`, `VocabularyTermResponseModel`
- `DatasetFieldMappingBaseModel`, `DatasetFieldMappingCreateModel`, `DatasetFieldMappingResponseModel`
- `DatasetConceptBaseModel`, `DatasetConceptCreateModel`, `DatasetConceptResponseModel`
- `OntologyChangeLogBaseModel`, `OntologyChangeLogCreateModel`, `OntologyChangeLogResponseModel`

**Features:**
- ✅ Type hints on all fields
- ✅ Field validation (min/max length, regex)
- ✅ Enum validation (draft/active/deprecated, constraint types)
- ✅ UUID support
- ✅ Timestamp support
- ✅ from_attributes = True (ORM compatible)

### 3. ORM Models
📄 **File:** `app/models/ontology_model.py` (456 lines)

SQLAlchemy models corresponding to all 10 tables.

**Auto-sync:** When you run SQL, SQLAlchemy will recognize the tables.

### 4. Business Logic
📄 **File:** `app/services/ontology_service.py` (499 lines)

Services for:
- Domain management
- Ontology versioning
- Concept management
- Property management
- Vocabulary management
- Dataset field mapping

### 5. Data Access
📄 **File:** `app/repositories/ontology_repository.py` (634 lines)

Repository classes (10 total) with 89+ methods for CRUD operations.

### 6. API Endpoints
📄 **File:** `app/api/ontology_controller.py` (470 lines)

FastAPI routes with 23+ endpoints for all ontology operations.

---

## Getting Started (5 Minutes)

### Step 1: Create Tables in PostgreSQL (2 min)

**Option A: Using psql command**
```bash
psql -U your_username -d your_database -f SQL_CREATE_TABLES.sql
```

**Option B: Copy-paste in pgAdmin or DBeaver**
1. Open `SQL_CREATE_TABLES.sql`
2. Copy all contents
3. Paste into your PostgreSQL client
4. Execute (Ctrl+Enter or Run button)

**Option C: Copy-paste in psql command line**
```bash
psql -U your_username -d your_database
\i SQL_CREATE_TABLES.sql
```

**Verify tables created:**
```bash
psql -U your_username -d your_database -c "\dt cml_*"
```

Expected output:
```
             List of relations
 Schema |           Name            | Type  | Owner
--------+---------------------------+-------+-------
 public | cml_concept               | table | user
 public | cml_dataset_concept       | table | user
 public | cml_dataset_field_mapping | table | user
 public | cml_domain                | table | user
 public | cml_ontology_change_log   | table | user
 public | cml_ontology_version      | table | user
 public | cml_property              | table | user
 public | cml_property_constraint   | table | user
 public | cml_vocab                 | table | user
 public | cml_vocab_term            | table | user
(10 rows)
```

### Step 2: Start the API Server (1 min)

```bash
cd /home/metastring/src/central_server

# Activate virtual environment
source env/bin/activate

# Run the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Output should show:**
```
Uvicorn running on http://0.0.0.0:8000
```

### Step 3: Test an Endpoint (2 min)

**Example: Create a Domain**

```bash
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain",
    "description": "Domain for managing biodiversity data"
  }'
```

**Expected Response:**
```json
{
  "domain_id": "550e8400-e29b-41d4-a716-446655440000",
  "domain_code": "biodiversity",
  "domain_name": "Biodiversity Domain",
  "description": "Domain for managing biodiversity data",
  "is_active": true,
  "created_at": "2024-01-15T10:30:45.123456Z",
  "updated_at": null
}
```

---

## Common Tasks

### Task 1: Create a Domain

**Python/FastAPI:**
```python
from app.schemas.ontology_pydantic import DomainCreateModel
from app.services.ontology_service import DomainService

# Prepare data
domain_data = DomainCreateModel(
    domain_code="plant_taxonomy",
    domain_name="Plant Taxonomy",
    description="Plant taxonomic classification"
)

# Save to database
service = DomainService(db)
domain = service.create_domain(
    domain_data.domain_code,
    domain_data.domain_name,
    domain_data.description
)
```

**cURL:**
```bash
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{
    "domain_code": "plant_taxonomy",
    "domain_name": "Plant Taxonomy"
  }'
```

**Direct SQL:**
```sql
INSERT INTO cml_domain (domain_code, domain_name, description)
VALUES ('plant_taxonomy', 'Plant Taxonomy', 'Plant classification')
RETURNING domain_id;
```

### Task 2: Create an Ontology Version

**Python:**
```python
from app.schemas.ontology_pydantic import OntologyVersionCreateModel
from app.services.ontology_service import OntologyVersionService

version_data = OntologyVersionCreateModel(
    version_code="v1.0.0",
    status="draft"
)

service = OntologyVersionService(db)
version = service.create_version(
    domain_id=domain.domain_id,
    version_data
)
```

**SQL:**
```sql
INSERT INTO cml_ontology_version (domain_id, version_code, status)
VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'v1.0.0',
    'draft'
)
RETURNING ontology_version_id;
```

### Task 3: Create a Concept Hierarchy

**Parent Concept:**
```sql
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
```

**Child Concept:**
```sql
INSERT INTO cml_concept (
    ontology_version_id,
    concept_code,
    concept_name,
    parent_concept_id
)
VALUES (
    '550e8400-e29b-41d4-a716-446655440010',
    'Plant',
    'Plant',
    '550e8400-e29b-41d4-a716-446655440020'  -- Parent ID
);
```

### Task 4: Add Property with Constraints

**Create Property:**
```sql
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
);
```

**Add Constraint (validation rule):**
```sql
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

---

## Database Connection Info

### Update `.env` file
```bash
DATABASE_URL=postgresql://username:password@localhost:5432/your_database_name
```

### Example Configuration
```python
# app/core/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://localhost/ontology_db"
    DEBUG: bool = True

settings = Settings()
```

---

## API Endpoints Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/ontology/domains` | Create domain |
| GET | `/api/v1/ontology/domains` | List domains |
| GET | `/api/v1/ontology/domains/{id}` | Get domain |
| PUT | `/api/v1/ontology/domains/{id}` | Update domain |
| DELETE | `/api/v1/ontology/domains/{id}` | Delete domain |
| POST | `/api/v1/ontology/versions` | Create version |
| GET | `/api/v1/ontology/versions` | List versions |
| POST | `/api/v1/ontology/concepts` | Create concept |
| GET | `/api/v1/ontology/concepts` | List concepts |
| POST | `/api/v1/ontology/properties` | Create property |
| GET | `/api/v1/ontology/properties` | List properties |
| POST | `/api/v1/ontology/vocabularies` | Create vocabulary |
| GET | `/api/v1/ontology/vocabularies` | List vocabularies |

---

## Troubleshooting

### Issue: UUID Extension Not Found
```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

### Issue: Foreign Key Constraint Violation
Make sure parent record exists before inserting child:
```sql
-- Check domain exists
SELECT * FROM cml_domain WHERE domain_id = 'YOUR_ID';

-- Check version exists  
SELECT * FROM cml_ontology_version WHERE ontology_version_id = 'YOUR_ID';
```

### Issue: Pydantic Validation Error
Check field constraints:
```python
# Too long - max 100 chars
DomainCreateModel(domain_code="a" * 101, domain_name="Test")  # ❌

# Correct - within limits
DomainCreateModel(domain_code="test", domain_name="Test")  # ✅

# Invalid status
OntologyVersionCreateModel(version_code="v1", status="invalid")  # ❌

# Correct status
OntologyVersionCreateModel(version_code="v1", status="draft")  # ✅
```

### Issue: Connection Refused
```bash
# Verify PostgreSQL is running
sudo service postgresql status

# Or on macOS
brew services list
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `SQL_CREATE_TABLES.sql` | PostgreSQL CREATE TABLE statements |
| `app/schemas/ontology_pydantic.py` | Pydantic validation models |
| `app/models/ontology_model.py` | SQLAlchemy ORM models |
| `app/services/ontology_service.py` | Business logic services |
| `app/repositories/ontology_repository.py` | Data access layer |
| `app/api/ontology_controller.py` | FastAPI endpoints |
| `tests/unit/test_ontology_repository.py` | Unit tests (31+ tests) |
| `tests/integration/test_ontology.py` | Integration tests (11+ tests) |

---

## Next Steps

1. ✅ Run `SQL_CREATE_TABLES.sql` in PostgreSQL
2. ✅ Start the API server with `uvicorn app.main:app --reload`
3. ✅ Test endpoints with cURL or Postman
4. ✅ Run tests: `pytest tests/unit/test_ontology_repository.py`
5. ✅ Check API docs: `http://localhost:8000/docs` (Swagger UI)

---

## Support Documentation

- 📖 **Architecture:** See `ONTOLOGY_IMPLEMENTATION.md`
- 📖 **API Details:** See `ONTOLOGY_API.md`
- 📖 **Setup Guide:** See `ONTOLOGY_SETUP_GUIDE.md`
- 📖 **Code Standards:** See `.github/copilot-instructions.md`
- 📖 **Project Status:** See `ONTOLOGY_STATUS.md`

---

**You're all set! 🚀 Start with running the SQL file, then launch the server.**
