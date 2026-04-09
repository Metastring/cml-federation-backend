# Ontology System Implementation

## Overview

A complete ontology management system has been implemented for the Central Server, providing comprehensive data modeling, versioning, and mapping capabilities for managing complex domain hierarchies and vocabulary-controlled values.

## Architecture

### Clean Architecture Pattern
- **Models**: SQLAlchemy ORM entities in `app/models/ontology_model.py`
- **Repositories**: Data access layer in `app/repositories/ontology_repository.py`
- **Services**: Business logic layer in `app/services/ontology_service.py`
- **Controllers**: API endpoints in `app/api/ontology_controller.py`
- **Schemas**: Request/response validation in `app/schemas/ontology_schema.py`

### Database Design
- **Database**: PostgreSQL 14+
- **ORM**: SQLAlchemy Core/ORM hybrid
- **Migrations**: Alembic with versioning
- **Constraints**: Foreign keys with CASCADE delete for referential integrity
- **Indexing**: Strategic indexes on frequently queried columns

## Implemented Components

### 1. Data Models (10 Core Tables)

#### Domain (`cml_domain`)
- Top-level categorization for ontologies
- Fields: `domain_id`, `domain_code`, `domain_name`, `description`, `is_active`, `created_at`
- Indexes: `domain_code` (UNIQUE), `is_active`
- Relationships: One-to-many with `cml_ontology_version`

#### OntologyVersion (`cml_ontology_version`)
- Versioned snapshots of ontologies
- Fields: `ontology_version_id`, `domain_id`, `version_code`, `status`, `description`, `created_at`
- Status: `draft`, `active`, `deprecated`
- Constraints: Unique on `(domain_id, version_code)`
- Relationships: One-to-many with `cml_concept`, `cml_ontology_change_log`

#### Concept (`cml_concept`)
- Real-world entities with hierarchical support
- Fields: `concept_id`, `ontology_version_id`, `concept_code`, `concept_name`, `description`, `parent_concept_id`, `is_abstract`, `created_at`
- Parent-child relationships support concept hierarchies
- Constraints: Unique on `(ontology_version_id, concept_code)`
- Relationships: One-to-many with `cml_property`, self-referencing parent-child

#### Property (`cml_property`)
- Attributes/characteristics of concepts
- Fields: `property_id`, `concept_id`, `property_code`, `property_name`, `data_type`, `description`, `is_required`, `is_multivalued`, `created_at`
- Data types: `string`, `number`, `date`, `geometry`, `boolean`
- Constraints: Unique on `(concept_id, property_code)`
- Relationships: One-to-many with `cml_property_constraint`

#### PropertyConstraint (`cml_property_constraint`)
- Validation rules for properties
- Fields: `constraint_id`, `property_id`, `constraint_type`, `constraint_value` (JSON), `created_at`
- Types: `range`, `enum`, `regex`, `unit`, `geometry`, `vocab`
- Flexible JSON storage for constraint values

#### Vocabulary (`cml_vocab`)
- Controlled value lists
- Fields: `vocab_id`, `vocab_code`, `description`, `created_at`
- Indexes: `vocab_code` (UNIQUE)

#### VocabularyTerm (`cml_vocab_term`)
- Terms in vocabularies with hierarchical support
- Fields: `vocab_term_id`, `vocab_id`, `term_code`, `term_label`, `description`, `parent_term_id`, `created_at`
- Parent-child relationships for term hierarchies
- Constraints: Unique on `(vocab_id, term_code)`

#### DatasetFieldMapping (`cml_dataset_field_mapping`)
- Maps dataset columns to ontology properties
- Fields: `mapping_id`, `dataset_id`, `concept_id`, `property_id`, `external_field_name`, `external_data_type`, `transform_rule` (JSON), `is_exposed`, `created_at`
- Flexible JSON transform rules for data transformation
- Constraints: Unique on `(dataset_id, external_field_name)`

#### DatasetConcept (`cml_dataset_concept`)
- Associates datasets with concepts
- Fields: `dataset_concept_id`, `dataset_id`, `concept_id`, `created_at`
- Constraints: Unique on `(dataset_id, concept_id)`

#### OntologyChangeLog (`cml_ontology_change_log`)
- Audit trail for ontology modifications
- Fields: `change_id`, `ontology_version_id`, `change_type`, `description`, `changed_by`, `changed_at`
- Change types: `add_concept`, `add_property`, `add_constraint`, etc.
- Ordered by `changed_at` DESC for chronological access

### 2. Repository Layer

10 repository classes providing CRUD operations:

- **DomainRepository**: Create, read, update, delete domains; get by code
- **OntologyVersionRepository**: Manage versions; get active version; list by domain
- **ConceptRepository**: Full CRUD with hierarchy support; list by version; list root concepts
- **PropertyRepository**: Full CRUD; list by concept
- **PropertyConstraintRepository**: Full CRUD; list by property
- **VocabularyRepository**: Full CRUD; get by code; list all
- **VocabularyTermRepository**: Full CRUD with hierarchy support; list root terms; list by vocabulary
- **DatasetFieldMappingRepository**: Full CRUD; list by dataset; list by concept
- **DatasetConceptRepository**: Full CRUD; list by dataset; list by concept
- **OntologyChangeLogRepository**: Create, read; list by version (descending order)

Each repository:
- Uses SQLAlchemy Session for database operations
- Implements unit-of-work pattern with `flush()`
- Returns None for non-existent entities
- Handles relationships efficiently

### 3. Service Layer

Business logic and validation layer with 7 service classes:

#### DomainService
- Create, retrieve, list, update, delete domains
- Validates domain code uniqueness
- Manages domain lifecycle

#### VocabularyService
- Create, retrieve, list vocabularies
- Add terms to vocabularies
- Manage controlled value lists

#### OntologyVersionService
- Create versions for domains
- Get active version
- Activate/deprecate versions
- Log version changes

#### ConceptService
- Create, retrieve, list, delete concepts
- Add properties to concepts
- Support parent-child hierarchies
- Log concept changes

#### PropertyService
- Retrieve properties
- List properties by concept
- Add constraints to properties
- Validate constraint types

#### DatasetMappingService
- Create field-to-property mappings
- List mappings by dataset/concept
- Support transform rules
- Update mapping configurations

#### DatasetConceptService
- Link datasets to concepts
- List concepts by dataset
- List datasets by concept
- Manage associations

### 4. API Endpoints

RESTful endpoints covering all ontology operations:

#### Domain Endpoints
- `POST /api/v1/ontology/domains` - Create domain
- `GET /api/v1/ontology/domains` - List domains
- `GET /api/v1/ontology/domains/{domain_id}` - Get domain
- `GET /api/v1/ontology/domains/code/{code}` - Get by code
- `PATCH /api/v1/ontology/domains/{domain_id}` - Update domain
- `DELETE /api/v1/ontology/domains/{domain_id}` - Delete domain

#### Ontology Version Endpoints
- `POST /api/v1/ontology/domains/{domain_id}/versions` - Create version
- `GET /api/v1/ontology/domains/{domain_id}/versions` - List versions
- `GET /api/v1/ontology/domains/{domain_id}/versions/active` - Get active
- `GET /api/v1/ontology/versions/{version_id}` - Get version

#### Concept Endpoints
- `POST /api/v1/ontology/versions/{version_id}/concepts` - Create concept
- `GET /api/v1/ontology/versions/{version_id}/concepts` - List concepts
- `GET /api/v1/ontology/concepts/{concept_id}` - Get concept

#### Property Endpoints
- `POST /api/v1/ontology/concepts/{concept_id}/properties` - Create property
- `GET /api/v1/ontology/concepts/{concept_id}/properties` - List properties
- `GET /api/v1/ontology/properties/{property_id}` - Get property

#### Vocabulary Endpoints
- `POST /api/v1/ontology/vocabularies` - Create vocabulary
- `GET /api/v1/ontology/vocabularies` - List vocabularies
- `GET /api/v1/ontology/vocabularies/{vocab_id}` - Get vocabulary
- `GET /api/v1/ontology/vocabularies/code/{code}` - Get by code

#### Vocabulary Term Endpoints
- `POST /api/v1/ontology/vocabularies/{vocab_id}/terms` - Create term
- `GET /api/v1/ontology/vocabularies/{vocab_id}/terms` - List terms
- `GET /api/v1/ontology/terms/{term_id}` - Get term

All endpoints include:
- Proper HTTP status codes (201 for creation, 404 for not found, 409 for conflict)
- Error handling with descriptive messages
- Request/response validation via Pydantic
- Dependency injection for database sessions
- Transaction management with commit/rollback

### 5. Testing

#### Unit Tests (`tests/unit/test_ontology_repository.py`)
- Model instantiation tests
- Repository CRUD operation tests
- Mock database interactions
- Constraint validation tests
- Relationship tests

#### Integration Tests (`tests/integration/test_ontology.py`)
- Domain creation and retrieval
- Version management
- Concept hierarchies
- Property constraints
- Vocabulary term hierarchies
- Dataset field mappings
- Cascade delete behavior
- Database transaction handling

#### Test Coverage
- All 10 model classes tested
- All 10 repository classes tested
- CRUD operations validated
- Relationships verified
- Cascade deletes confirmed

### 6. Database Migration

Complete Alembic migration file (`migrations/001_initial_ontology_schema.py`):

**Upgrade Function**:
- Creates 10 tables with proper schemas
- Establishes foreign key relationships
- Configures cascade delete rules
- Creates indexes on 23+ columns
- Establishes unique constraints
- Creates enum types for status and change_type

**Downgrade Function**:
- Cleanly drops all tables
- Removes enum types
- Reverses all migrations

Run migrations:
```bash
alembic upgrade head
```

Rollback if needed:
```bash
alembic downgrade -1
```

## Key Features

### 1. Hierarchical Data Support
- Concepts with parent-child relationships
- Vocabulary terms with hierarchies
- Recursive queries via SQLAlchemy relationships

### 2. Flexible Constraints
- JSON-based constraint storage
- Multiple constraint types: range, enum, regex, unit, geometry, vocab
- Extensible for new constraint types

### 3. Data Transformation
- JSON-based transform rules in field mappings
- Supports complex data transformations
- Flexible format for various transformation types

### 4. Audit Trail
- Complete change log with timestamps
- Tracks who made changes and when
- Supports versioning and rollback analysis

### 5. Multi-Versioning
- Multiple ontology versions per domain
- Status tracking: draft, active, deprecated
- Active version selection per domain

### 6. Referential Integrity
- CASCADE delete prevents orphaned records
- Foreign key constraints enforce relationships
- Unique constraints ensure data consistency

## Design Patterns

### Repository Pattern
- Abstracts database access
- Enables testing with mocks
- Provides consistent interface
- Separates concerns

### Service Layer Pattern
- Contains business logic
- Validates inputs
- Manages transactions
- Provides clean API

### Dependency Injection
- FastAPI dependencies for database sessions
- Constructor injection in services
- Enables testing and modularity

### Model-Repository-Service-Controller Flow
```
Request → Controller (Validation)
         ↓
         Service (Business Logic)
         ↓
         Repository (Data Access)
         ↓
         Database
```

## Usage Examples

### Create a Domain with Concepts
```python
from app.services.ontology_service import DomainService, OntologyVersionService, ConceptService
from sqlalchemy.orm import Session

# Create domain
domain_service = DomainService(db)
domain = domain_service.create_domain("biology", "Biology Domain")

# Create version
version_service = OntologyVersionService(db)
version = version_service.create_version(domain.domain_id, "v1.0")

# Create concepts
concept_service = ConceptService(db)
species = concept_service.create_concept(
    domain.domain_id,
    version.ontology_version_id,
    "Species",
    "Species"
)
```

### Add Properties and Constraints
```python
# Add property to concept
property_service = PropertyService(db)
prop = property_service.property_repository.create(
    species.concept_id,
    "scientificName",
    "Scientific Name",
    "string",
    is_required=True
)

# Add constraint
constraint = property_service.add_constraint_to_property(
    prop.property_id,
    "regex",
    {"pattern": "^[A-Z][a-z]+ [a-z]+$"}
)
```

### Map Dataset Fields
```python
from app.services.ontology_service import DatasetMappingService

mapping_service = DatasetMappingService(db)
mapping = mapping_service.create_mapping(
    dataset_id=1,
    concept_id=species.concept_id,
    property_id=prop.property_id,
    external_field_name="species_name",
    transform_rule={"type": "uppercase"}
)
```

## Configuration

### Environment Variables
```bash
DATABASE_URL=postgresql://user:password@localhost:5432/central_server
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/central_server_test
```

### Requirements
- Python 3.11+
- PostgreSQL 14+
- SQLAlchemy 2.0+
- FastAPI 0.117+
- Pydantic 2.0+
- Alembic 1.12+

## Testing

### Run Unit Tests
```bash
pytest tests/unit/test_ontology_repository.py -v
```

### Run Integration Tests
```bash
pytest tests/integration/test_ontology.py -v
```

### Run All Tests
```bash
pytest tests/ -v --cov=app
```

## Future Enhancements

1. **Permission Management**: Role-based access control for ontology modifications
2. **Version Control**: Git-like branching for ontology development
3. **Import/Export**: Support for standard ontology formats (OWL, RDF)
4. **Reasoning**: SPARQL queries and semantic reasoning
5. **Visualization**: Graph-based visualization of ontologies
6. **Notifications**: Event-based notifications for ontology changes
7. **Caching**: Redis caching for frequently accessed ontologies

## Troubleshooting

### Common Issues

**Issue**: Foreign key constraint violation
- **Solution**: Ensure referenced entities exist before creating dependent entities

**Issue**: Unique constraint violation
- **Solution**: Check domain_code, vocab_code, property_code uniqueness per parent

**Issue**: Orphaned records
- **Solution**: CASCADE deletes are configured; verify parent deletion cascades correctly

## References

- [SQLAlchemy ORM Documentation](https://docs.sqlalchemy.org/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)

## Code Structure

```
app/
├── models/
│   └── ontology_model.py          # 10 SQLAlchemy ORM models
├── repositories/
│   └── ontology_repository.py     # 10 Repository classes
├── services/
│   └── ontology_service.py        # 7 Service classes
├── api/
│   └── ontology_controller.py     # RESTful API endpoints
└── schemas/
    └── ontology_schema.py          # Pydantic request/response schemas

tests/
├── unit/
│   └── test_ontology_repository.py # Unit tests
└── integration/
    └── test_ontology.py            # Integration tests

migrations/
└── 001_initial_ontology_schema.py # Alembic migration
```

## License

Proprietary - Central Server Project
