# Copilot Instructions

## 1. Project Overview

### Yatra Management System – FastAPI Backend

This is a **FastAPI-based backend** for the Yatra Management System, providing RESTful APIs for managing biodiversity data, species information, datasets, and ontology mappings.

### Scope and Core Responsibilities

- **API Layer**: RESTful endpoints for data access and manipulation
- **Data Management**: PostgreSQL database with PostGIS extensions
- **Ontology System**: Domain-driven vocabulary and concept management
- **Dataset Integration**: Field mapping and data transformation
- **Federated Search**: Cross-participant data discovery

---

## 2. Architecture Reference

### Architecture Documentation

All architectural decisions MUST align with:
- Clean Architecture principles
- Repository Pattern for data access
- Service Layer for business logic
- Dependency Injection for testability

### Code Structure Documentation

```
app/
├── api/           # Controllers/Routes (presentation layer)
├── core/          # Configuration, database, exceptions
├── models/        # SQLAlchemy ORM models
├── repositories/  # Data access layer
├── schemas/       # Pydantic request/response schemas
├── services/      # Business logic layer
└── utils/         # Shared utilities
```

---

## 3. Documentation Maintenance (CRITICAL)

### Documentation Synchronization Rules

1. **Every code change** that affects architecture, APIs, or domain logic MUST be reflected in relevant documentation
2. **README.md** must stay current with setup instructions
3. **API documentation** must match actual endpoint behavior
4. **Schema changes** require migration documentation updates

### Self-Update Requirement

When modifying code:
- Update related docstrings
- Update API documentation if endpoints change
- Update schema descriptions if models change
- Add migration notes for database changes

---

## 5. System Architecture Overview

### Layered Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    API Layer (Controllers)               │
│              FastAPI routes, request validation          │
├─────────────────────────────────────────────────────────┤
│                    Service Layer                         │
│           Business logic, orchestration, validation      │
├─────────────────────────────────────────────────────────┤
│                   Repository Layer                       │
│              Data access, CRUD operations                │
├─────────────────────────────────────────────────────────┤
│                    Model Layer                           │
│              SQLAlchemy ORM, Pydantic schemas            │
├─────────────────────────────────────────────────────────┤
│                    Database Layer                        │
│                 PostgreSQL + PostGIS                     │
└─────────────────────────────────────────────────────────┘
```

### API, Service, Repository, Model Layers

| Layer | Responsibility | Dependencies |
|-------|---------------|--------------|
| API | HTTP handling, validation, routing | Service |
| Service | Business logic, orchestration | Repository |
| Repository | Data persistence, queries | Model |
| Model | Data structure, ORM mapping | Database |

---

## 6. Core Domain Entities

### Entity Definitions

| Entity | Description | Key Fields |
|--------|-------------|------------|
| Domain | Top-level categorization | domain_code, domain_name, is_active |
| OntologyVersion | Versioned ontology snapshots | version_code, status, domain_id |
| Concept | Real-world entity representation | concept_code, parent_concept_id, is_abstract |
| Property | Concept attributes | property_code, data_type, is_required |
| PropertyConstraint | Validation rules | constraint_type, constraint_value |
| Vocabulary | Controlled value lists | vocab_code, description |
| VocabularyTerm | Vocabulary entries | term_code, term_label, parent_term_id |
| DatasetFieldMapping | Dataset-to-ontology mapping | external_field_name, transform_rule |

### Key Relationships

```
Domain (1) ──────< (N) OntologyVersion
OntologyVersion (1) ──────< (N) Concept
Concept (1) ──────< (N) Property
Property (1) ──────< (N) PropertyConstraint
Vocabulary (1) ──────< (N) VocabularyTerm
Concept (1) ──────< (N) DatasetFieldMapping
```

---

## 7. General Engineering Principles

1. **SOLID Principles**: Single responsibility, Open-closed, Liskov substitution, Interface segregation, Dependency inversion
2. **DRY (Don't Repeat Yourself)**: Extract common logic into utilities or base classes
3. **KISS (Keep It Simple, Stupid)**: Prefer simple solutions over complex ones
4. **YAGNI (You Aren't Gonna Need It)**: Don't add functionality until it's needed
5. **Fail Fast**: Validate early, throw exceptions immediately on invalid state
6. **Defensive Programming**: Never trust external input

---

## 8. Code Style & Formatting Standards

### PEP 8

- Follow PEP 8 style guide strictly
- Maximum line length: **88 characters** (Black formatter default)
- Use 4 spaces for indentation (no tabs)

### Type Hints

```python
# ✅ CORRECT - All parameters and return types annotated
def get_domain(self, domain_id: UUID) -> Optional[Domain]:
    """Get domain by ID."""
    return self.repository.get_by_id(domain_id)

# ❌ WRONG - Missing type hints
def get_domain(self, domain_id):
    return self.repository.get_by_id(domain_id)
```

### Imports

```python
# Standard library imports (alphabetically)
from datetime import datetime
from typing import List, Optional
from uuid import UUID

# Third-party imports (alphabetically)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

# Local application imports (alphabetically)
from app.core.database import get_db
from app.models.domain_model import Domain
from app.repositories.domain_repository import DomainRepository
```

### Formatting Rules

- Use **Black** for code formatting
- Use **isort** for import sorting
- Use **flake8** for linting
- Configure in `pyproject.toml`

---

## 9. Project Structure

### Directory Layout

```
central_server/
├── .github/
│   └── copilot-instructions.md
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── api/                    # API controllers
│   │   ├── __init__.py
│   │   ├── ontology_controller.py
│   │   └── species_controller.py
│   ├── core/                   # Core configuration
│   │   ├── __init__.py
│   │   ├── config.py           # Environment configuration
│   │   ├── database.py         # Database connection
│   │   └── exceptions.py       # Custom exceptions
│   ├── models/                 # SQLAlchemy ORM models
│   │   ├── __init__.py
│   │   └── ontology_model.py
│   ├── repositories/           # Data access layer
│   │   ├── __init__.py
│   │   └── ontology_repository.py
│   ├── schemas/                # Pydantic schemas
│   │   ├── __init__.py
│   │   └── ontology_schema.py
│   ├── services/               # Business logic
│   │   ├── __init__.py
│   │   └── ontology_service.py
│   └── utils/                  # Utilities
│       ├── __init__.py
│       └── validators.py
├── migrations/                 # Alembic migrations
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Shared fixtures
│   ├── integration/            # Integration tests
│   └── unit/                   # Unit tests
├── pyproject.toml
├── requirements.txt
└── README.md
```

### Layer Responsibilities

| Layer | Location | Responsibility |
|-------|----------|----------------|
| Controllers | `app/api/` | HTTP request handling, validation, response formatting |
| Services | `app/services/` | Business logic, orchestration, domain validation |
| Repositories | `app/repositories/` | Database operations, query building |
| Models | `app/models/` | ORM definitions, relationships |
| Schemas | `app/schemas/` | Request/response validation |

---

## 10. Naming Conventions

### 10.1 File Naming

```
# Controllers
ontology_controller.py      # ✅ snake_case with _controller suffix
OntologyController.py       # ❌ PascalCase not allowed

# Models
ontology_model.py           # ✅ snake_case with _model suffix

# Repositories
ontology_repository.py      # ✅ snake_case with _repository suffix

# Services
ontology_service.py         # ✅ snake_case with _service suffix

# Schemas
ontology_schema.py          # ✅ snake_case with _schema suffix

# Tests
test_ontology_service.py    # ✅ test_ prefix
```

### 10.2 Class Naming

```python
# Models - PascalCase, singular noun
class Domain(Base):
class OntologyVersion(Base):
class VocabularyTerm(Base):

# Repositories - PascalCase with Repository suffix
class DomainRepository:
class OntologyVersionRepository:

# Services - PascalCase with Service suffix
class DomainService:
class OntologyVersionService:

# Schemas - PascalCase with Schema/Request/Response suffix
class DomainCreateSchema(BaseModel):
class DomainResponseSchema(BaseModel):

# Exceptions - PascalCase with Error suffix
class NotFoundError(Exception):
class ValidationError(Exception):
```

### 10.3 Instance Naming (Singletons)

```python
# Router instances - lowercase snake_case
router = APIRouter()

# Database session - lowercase
db: Session

# Repository instances - snake_case
domain_repo = DomainRepository(db)
concept_repo = ConceptRepository(db)
```

---

## 11. Functions & Class Design

### Function Size Rules

- **Maximum 20 lines** per function (excluding docstring)
- If a function exceeds 20 lines, **refactor into smaller functions**
- Each function should do **one thing only**

```python
# ✅ CORRECT - Small, focused functions
def validate_domain_code(code: str) -> bool:
    """Validate domain code format."""
    return bool(code and len(code) <= 50 and code.isalnum())

def check_domain_exists(repo: DomainRepository, code: str) -> None:
    """Raise error if domain already exists."""
    if repo.get_by_code(code):
        raise ValidationError(f"Domain '{code}' already exists")

# ❌ WRONG - Function doing too many things
def create_domain_with_validation_and_logging_and_notification(...):
    # 50+ lines of mixed concerns
    pass
```

### Class Responsibility Rules

- **Single Responsibility Principle**: One class, one reason to change
- **Maximum 300 lines** per class file
- Split large classes into smaller, focused classes

### Dataclass Usage

```python
from dataclasses import dataclass
from typing import Optional

# Use dataclasses for simple data containers
@dataclass
class DomainDTO:
    domain_code: str
    domain_name: str
    description: Optional[str] = None
    is_active: bool = True
```

---

## 12. Error Handling Standards

### Exception Handling Rules

```python
# ✅ CORRECT - Specific exception handling
try:
    domain = repo.get_by_id(domain_id)
except SQLAlchemyError as e:
    logger.error(f"Database error: {e}")
    raise DatabaseError("Failed to retrieve domain") from e

# ❌ WRONG - Catching all exceptions
try:
    domain = repo.get_by_id(domain_id)
except Exception:
    pass  # Swallowing exceptions
```

### Exception Chaining

```python
# ✅ CORRECT - Chain exceptions with 'from'
try:
    result = external_api.call()
except RequestException as e:
    raise ExternalServiceError("API call failed") from e
```

### Custom Exceptions

```python
# Define in app/core/exceptions.py
class DomainException(Exception):
    """Base exception for domain errors."""
    pass

class NotFoundError(DomainException):
    """Resource not found."""
    pass

class ValidationError(DomainException):
    """Validation failed."""
    pass

class ConflictError(DomainException):
    """Resource conflict (duplicate)."""
    pass
```

---

## 13. Unused Parameters & Variable Shadowing

### Underscore Convention

```python
# ✅ CORRECT - Prefix unused parameters with underscore
def callback(event: Event, _context: Context) -> None:
    """Handle event, context not used."""
    process_event(event)

# For loop unused variables
for _ in range(10):
    perform_action()

# Tuple unpacking with unused values
name, _, age = get_person_data()
```

### Shadowing Prevention

```python
# ❌ WRONG - Shadowing built-in
def process(id: int) -> None:  # 'id' shadows built-in
    pass

# ✅ CORRECT - Use descriptive names
def process(entity_id: int) -> None:
    pass

# ❌ WRONG - Shadowing outer scope
type = "string"  # Shadows built-in 'type'

# ✅ CORRECT
data_type = "string"
```

---

## 14. F-String Usage Guidelines

```python
# ✅ CORRECT - Use f-strings for formatting
name = "Domain"
message = f"Creating {name} with ID {domain_id}"

# ✅ CORRECT - Complex expressions in braces
log.info(f"Found {len(results)} results in {elapsed:.2f}s")

# ❌ WRONG - String concatenation
message = "Creating " + name + " with ID " + str(domain_id)

# ❌ WRONG - .format() method
message = "Creating {} with ID {}".format(name, domain_id)

# ❌ WRONG - % formatting
message = "Creating %s with ID %s" % (name, domain_id)
```

---

## 15. Logging Standards (Mandatory)

```python
import logging

# Get logger for module
logger = logging.getLogger(__name__)

# Log levels usage
logger.debug("Detailed debugging information")
logger.info("General operational messages")
logger.warning("Warning about potential issues")
logger.error("Error that needs attention")
logger.critical("Critical failure")

# ✅ CORRECT - Structured logging with context
logger.info(
    "Domain created",
    extra={
        "domain_id": str(domain.domain_id),
        "domain_code": domain.domain_code,
        "user": current_user.email
    }
)

# ✅ CORRECT - Exception logging
try:
    result = risky_operation()
except Exception as e:
    logger.exception(f"Operation failed: {e}")
    raise

# ❌ WRONG - Using print statements
print(f"Domain created: {domain_id}")  # Never use print!
```

---

## 16. Configuration & Secrets Management

```python
# ✅ CORRECT - Use environment variables
from app.core.config import settings

DATABASE_URL = settings.DATABASE_URL
API_KEY = settings.API_KEY

# ❌ WRONG - Hardcoded secrets
DATABASE_URL = "postgresql://user:password@localhost/db"
API_KEY = "sk-1234567890"

# Configuration class (app/core/config.py)
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings from environment."""
    
    DATABASE_URL: str
    API_KEY: str
    DEBUG: bool = False
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
```

---

## 17. Dependency Management

### Dependency Injection

```python
# ✅ CORRECT - Inject dependencies
from fastapi import Depends

def get_domain_service(db: Session = Depends(get_db)) -> DomainService:
    """Dependency provider for DomainService."""
    return DomainService(db)

@router.get("/domains/{domain_id}")
def get_domain(
    domain_id: UUID,
    service: DomainService = Depends(get_domain_service)
) -> DomainResponse:
    return service.get_domain(domain_id)

# ❌ WRONG - Creating dependencies inside function
@router.get("/domains/{domain_id}")
def get_domain(domain_id: UUID):
    db = get_db()  # Wrong!
    service = DomainService(db)
    return service.get_domain(domain_id)
```

### uv Package Management

```bash
# Install dependencies
uv pip install -r requirements.txt

# Add new dependency
uv pip install package-name
uv pip freeze > requirements.txt

# Create virtual environment
uv venv

# Activate environment
source .venv/bin/activate
```

---

## 18. Testing Requirements (Non-Negotiable)

### Testing Framework

- **pytest** for test framework
- **pytest-mock** for mocking
- **pytest-cov** for coverage
- **pytest-asyncio** for async tests

### Coverage Expectations

| Coverage Type | Minimum |
|--------------|---------|
| Overall | 80% |
| Critical paths | 95% |
| New code | 100% |

```python
# Unit test example
class TestDomainService:
    """Unit tests for DomainService."""

    def test_create_domain_success(self, mock_db):
        """Test successful domain creation."""
        repo = Mock(spec=DomainRepository)
        repo.get_by_code.return_value = None
        repo.create.return_value = Domain(
            domain_code="test",
            domain_name="Test"
        )
        
        service = DomainService(mock_db)
        service.repository = repo
        
        result = service.create_domain("test", "Test")
        
        assert result.domain_code == "test"
        repo.create.assert_called_once()

    def test_create_domain_duplicate_raises_error(self, mock_db):
        """Test duplicate domain raises ValidationError."""
        repo = Mock(spec=DomainRepository)
        repo.get_by_code.return_value = Domain(domain_code="test")
        
        service = DomainService(mock_db)
        service.repository = repo
        
        with pytest.raises(ValidationError):
            service.create_domain("test", "Test")
```

### Test File Structure

```
tests/
├── conftest.py              # Shared fixtures
├── unit/
│   ├── test_domain_service.py
│   ├── test_concept_service.py
│   └── test_validators.py
└── integration/
    ├── test_domain_api.py
    └── test_ontology_flow.py
```

---

## 19. Documentation Standards

### Docstrings

```python
def create_domain(
    self,
    domain_code: str,
    domain_name: str,
    description: Optional[str] = None
) -> Domain:
    """Create a new domain.
    
    Creates a domain with the given code and name. Validates that
    the domain code is unique before creation.
    
    Args:
        domain_code: Unique identifier for the domain (max 50 chars).
        domain_name: Human-readable name for the domain.
        description: Optional description of the domain.
    
    Returns:
        The newly created Domain object.
    
    Raises:
        ValidationError: If domain_code already exists.
        DatabaseError: If database operation fails.
    
    Example:
        >>> service = DomainService(db)
        >>> domain = service.create_domain("bio", "Biology")
        >>> print(domain.domain_code)
        'bio'
    """
    # Implementation
```

### Style Guidelines

- Use **Google-style docstrings**
- Document all public functions, classes, and modules
- Include type hints in signature, not in docstring
- Provide examples for complex functions

---

## 20. Performance & Scalability Considerations

```python
# ✅ CORRECT - Use pagination
@router.get("/domains")
def list_domains(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
) -> List[DomainResponse]:
    return service.list_domains(skip=skip, limit=limit)

# ✅ CORRECT - Eager loading for relationships
def get_concept_with_properties(concept_id: UUID) -> Concept:
    return (
        db.query(Concept)
        .options(selectinload(Concept.properties))
        .filter(Concept.concept_id == concept_id)
        .first()
    )

# ✅ CORRECT - Use database indexes
class Domain(Base):
    __tablename__ = "cml_domain"
    
    domain_code = Column(String(50), unique=True, index=True)  # Indexed!

# ❌ WRONG - N+1 query problem
def list_concepts():
    concepts = db.query(Concept).all()
    for concept in concepts:
        print(concept.properties)  # Triggers N additional queries!
```

---

## 21. Security Best Practices

```python
# ✅ CORRECT - Parameterized queries (SQLAlchemy handles this)
domain = db.query(Domain).filter(Domain.domain_code == code).first()

# ❌ WRONG - SQL injection vulnerable
cursor.execute(f"SELECT * FROM domains WHERE code = '{code}'")

# ✅ CORRECT - Input validation
from pydantic import BaseModel, Field, validator

class DomainCreate(BaseModel):
    domain_code: str = Field(..., min_length=1, max_length=50, regex="^[a-z0-9_]+$")
    
    @validator("domain_code")
    def validate_code(cls, v):
        if v.lower() in RESERVED_WORDS:
            raise ValueError("Reserved word not allowed")
        return v.lower()

# ✅ CORRECT - Secrets in environment
API_KEY = os.getenv("API_KEY")  # Never commit secrets!

# ✅ CORRECT - CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,  # Specific origins, not "*"
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)
```

---

## 22. SQLAlchemy Best Practices

### Boolean & NULL Comparisons

```python
# ✅ CORRECT - Use 'is_' for boolean comparisons
db.query(Domain).filter(Domain.is_active.is_(True))
db.query(Domain).filter(Domain.is_active.is_(False))

# ✅ CORRECT - Use 'is_' for NULL comparisons
db.query(Concept).filter(Concept.parent_concept_id.is_(None))
db.query(Concept).filter(Concept.parent_concept_id.isnot(None))

# ❌ WRONG - Python equality operators
db.query(Domain).filter(Domain.is_active == True)
db.query(Concept).filter(Concept.parent_concept_id == None)
```

### Session Management

```python
# ✅ CORRECT - Use dependency injection for session
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ CORRECT - Commit in controller, not repository
@router.post("/domains")
def create_domain(request: DomainCreate, db: Session = Depends(get_db)):
    domain = service.create_domain(request)
    db.commit()
    return domain

# ✅ CORRECT - Use flush in repository
class DomainRepository:
    def create(self, ...) -> Domain:
        domain = Domain(...)
        self.db.add(domain)
        self.db.flush()  # Get ID without committing
        return domain
```

---

## 23. Audit Logging (MANDATORY)

### Audit Decorators

```python
from functools import wraps
from datetime import datetime

def audit_log(action: str):
    """Decorator for audit logging."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            try:
                result = func(*args, **kwargs)
                logger.info(
                    f"Audit: {action}",
                    extra={
                        "action": action,
                        "status": "success",
                        "duration_ms": (datetime.utcnow() - start_time).total_seconds() * 1000
                    }
                )
                return result
            except Exception as e:
                logger.error(
                    f"Audit: {action} failed",
                    extra={
                        "action": action,
                        "status": "failure",
                        "error": str(e)
                    }
                )
                raise
        return wrapper
    return decorator
```

### Usage Rules

```python
class DomainService:
    @audit_log("domain.create")
    def create_domain(self, code: str, name: str) -> Domain:
        """Create domain with audit logging."""
        return self.repository.create(code, name)
    
    @audit_log("domain.delete")
    def delete_domain(self, domain_id: UUID) -> bool:
        """Delete domain with audit logging."""
        return self.repository.delete(domain_id)
```

### Repository Requirements

- All CREATE operations must be logged
- All UPDATE operations must be logged
- All DELETE operations must be logged
- Log must include: user, timestamp, action, entity_id, changes

### Common Mistakes

```python
# ❌ WRONG - No audit logging
def delete_domain(self, domain_id: UUID):
    self.repository.delete(domain_id)  # Who deleted? When?

# ✅ CORRECT - With audit logging
@audit_log("domain.delete")
def delete_domain(self, domain_id: UUID, user: str):
    self.changelog.create(
        entity_type="domain",
        entity_id=domain_id,
        action="delete",
        changed_by=user
    )
    self.repository.delete(domain_id)
```

---

## 24. API & Service Design Guidelines

### Controller Guidelines

```python
@router.post("/domains", response_model=DomainResponse, status_code=201)
def create_domain(
    request: DomainCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> DomainResponse:
    """Create a new domain.
    
    - Validates request
    - Calls service
    - Commits transaction
    - Returns response
    """
    service = DomainService(db)
    domain = service.create_domain(
        domain_code=request.domain_code,
        domain_name=request.domain_name,
        description=request.description
    )
    db.commit()
    return DomainResponse.from_orm(domain)
```

### Service Guidelines

```python
class DomainService:
    """Business logic for domain operations.
    
    - Contains all business rules
    - Orchestrates repository calls
    - Does NOT handle HTTP concerns
    - Does NOT commit transactions
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repository = DomainRepository(db)
    
    def create_domain(self, domain_code: str, domain_name: str) -> Domain:
        """Create domain with business validation."""
        # Business rule: code must be unique
        if self.repository.get_by_code(domain_code):
            raise ValidationError(f"Domain '{domain_code}' exists")
        
        # Business rule: code format
        if not self._is_valid_code(domain_code):
            raise ValidationError("Invalid domain code format")
        
        return self.repository.create(domain_code, domain_name)
```

---

## 25. Copilot Behavior Expectations

### DO:
- ✅ Always include type hints
- ✅ Always write docstrings for public functions
- ✅ Always follow the layered architecture
- ✅ Always write tests with new code
- ✅ Always use dependency injection
- ✅ Always handle errors explicitly
- ✅ Always use logging (never print)
- ✅ Always validate input
- ✅ Always use parameterized queries

### DON'T:
- ❌ Never put business logic in controllers
- ❌ Never commit secrets or credentials
- ❌ Never swallow exceptions silently
- ❌ Never use `print()` statements
- ❌ Never hardcode configuration
- ❌ Never skip tests
- ❌ Never use raw SQL without parameters
- ❌ Never ignore type hints
- ❌ Never create god classes (>300 lines)

---

## 26. Code Review Mindset

When generating or reviewing code, consider:

1. **Correctness**: Does it do what it's supposed to?
2. **Security**: Are there any vulnerabilities?
3. **Performance**: Are there any obvious inefficiencies?
4. **Maintainability**: Is it easy to understand and modify?
5. **Testability**: Can it be easily tested?
6. **Documentation**: Is it well documented?
7. **Error Handling**: Are errors handled gracefully?
8. **Consistency**: Does it follow project conventions?

---

## 27. Database Migrations (Alembic)

### Migration Creation Rules

```bash
# Generate migration from model changes
alembic revision --autogenerate -m "add_vocabulary_description"

# Create empty migration for manual changes
alembic revision -m "add_custom_index"
```

### Best Practices

```python
# migrations/versions/001_add_vocabulary_description.py

def upgrade() -> None:
    """Add description column to vocabulary."""
    op.add_column(
        'cml_vocab',
        sa.Column('description', sa.Text, nullable=True)
    )

def downgrade() -> None:
    """Remove description column from vocabulary."""
    op.drop_column('cml_vocab', 'description')
```

### Common Patterns

```python
# Adding a column with default
op.add_column('table', sa.Column('col', sa.Boolean, server_default='false'))

# Adding an index
op.create_index('idx_table_col', 'table', ['col'])

# Adding a foreign key
op.add_column('child', sa.Column('parent_id', postgresql.UUID))
op.create_foreign_key(
    'fk_child_parent',
    'child', 'parent',
    ['parent_id'], ['id'],
    ondelete='CASCADE'
)
```

### Migration Execution

```bash
# Apply all pending migrations
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# Show current revision
alembic current

# Show migration history
alembic history
```

---

## 28. Post-Modification Checklist

After every code modification, verify:

- [ ] **Tests Pass**: `pytest` runs successfully
- [ ] **Coverage Met**: Minimum 80% coverage maintained
- [ ] **Linting Clean**: `flake8` shows no errors
- [ ] **Formatting Applied**: `black .` and `isort .` applied
- [ ] **Type Checking**: `mypy` shows no errors
- [ ] **Docstrings Present**: All public functions documented
- [ ] **Migrations Created**: Database changes have migrations
- [ ] **Documentation Updated**: README/API docs reflect changes
- [ ] **No Secrets Exposed**: No credentials in code
- [ ] **Error Handling**: All error paths handled
- [ ] **Logging Added**: Appropriate log statements included
- [ ] **Audit Trail**: CRUD operations logged

```bash
# Quick verification commands
pytest --cov=app --cov-report=term-missing
black --check .
isort --check-only .
flake8 app tests
mypy app
```

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                    QUICK REFERENCE                               │
├─────────────────────────────────────────────────────────────────┤
│ File Naming:     snake_case_suffix.py (e.g., domain_service.py) │
│ Class Naming:    PascalCaseSuffix (e.g., DomainService)         │
│ Function Naming: snake_case (e.g., create_domain)               │
│ Constant Naming: UPPER_SNAKE_CASE (e.g., MAX_RETRIES)           │
├─────────────────────────────────────────────────────────────────┤
│ Max Function Lines:  20                                          │
│ Max Class Lines:     300                                         │
│ Max Line Length:     88                                          │
│ Min Test Coverage:   80%                                         │
├─────────────────────────────────────────────────────────────────┤
│ Layer Order:  Controller → Service → Repository → Model         │
│ Commit In:    Controller (never in repository)                  │
│ Flush In:     Repository (to get IDs)                           │
├─────────────────────────────────────────────────────────────────┤
│ ✅ DO: Type hints, Docstrings, Tests, Logging, DI               │
│ ❌ DON'T: print(), Hardcode secrets, Skip tests, Swallow errors │
└─────────────────────────────────────────────────────────────────┘
```

---

📌 **KEY LINE THAT FORCES AI BEHAVIOR:**

**Do NOT generate code without tests. Every feature MUST include tests.**
