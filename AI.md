# AI Development Rules

This document defines universal rules that apply to ALL AI tools (Copilot, ChatGPT, Claude, etc.) generating code for this project.

## Absolute Rules

1. **No feature is complete without tests**
   - Every function needs a corresponding test
   - Cannot merge code without tests passing

2. **Tests are first-class code**
   - Test code receives same care as production code
   - Tests must be maintainable and clear
   - If test is hard to write, the design needs improvement

3. **If tests cannot be written, explain why**
   - Some code may be legitimately hard to test
   - In such cases, document the reason clearly
   - Consider refactoring to make it testable

## Unit Testing Rules

### Definition
Unit tests verify business logic in isolation without external dependencies.

### Requirements
- **No Database Calls** - Mock all database operations
- **No HTTP Requests** - Mock external API calls
- **No File System Access** - Mock file operations
- **Fast Execution** - Should run in < 1 second
- **Deterministic** - Same input always produces same output

### File Structure
```
tests/unit/
├── test_services.py
├── test_repositories.py
├── test_models.py
├── test_validators.py
└── test_exceptions.py
```

### Example Template
```python
import pytest
from unittest.mock import Mock, patch
from app.services.dataset_service import DatasetService

class TestDatasetService:
    """Test suite for DatasetService."""
    
    @pytest.mark.unit
    def test_fetch_dataset_success(self):
        """Test successfully fetching a dataset."""
        # Arrange
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = {"id": "123", "title": "Test"}
        service = DatasetService(repository=mock_repo)
        
        # Act
        result = service.fetch_dataset("123")
        
        # Assert
        assert result["title"] == "Test"
        mock_repo.get_by_id.assert_called_once_with("123")
    
    @pytest.mark.unit
    def test_fetch_dataset_not_found(self):
        """Test fetching non-existent dataset."""
        # Arrange
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = None
        service = DatasetService(repository=mock_repo)
        
        # Act & Assert
        with pytest.raises(DatasetNotFoundError):
            service.fetch_dataset("999")
    
    @pytest.mark.unit
    def test_fetch_dataset_with_invalid_id(self):
        """Test with invalid dataset ID format."""
        # Arrange
        mock_repo = Mock()
        service = DatasetService(repository=mock_repo)
        
        # Act & Assert
        with pytest.raises(ValueError):
            service.fetch_dataset("")  # Empty ID should fail
```

### Mocking Guidelines
- Mock at boundaries (database, HTTP, file system)
- Use `unittest.mock.Mock` or `pytest-mock`
- Assert mock was called correctly
- Don't over-mock (mock dependencies, not internals)

## Integration Testing Rules

### Definition
Integration tests verify that components work together correctly with real external systems.

### Requirements
- **Real Database** - Must connect to actual PostgreSQL
- **Real Transactions** - Test actual transaction behavior
- **Separate Test Database** - Isolated from production
- **Cleanup After Tests** - Data must be rolled back or deleted
- **No Mocking Database** - Test real SQL, constraints, triggers

### File Structure
```
tests/integration/
├── test_dataset_repository.py
├── test_ontology_repository.py
├── test_mappings_integration.py
└── conftest.py  # Test fixtures
```

### Example Template
```python
import pytest
from app.repositories.dataset_repository import DatasetRepository
from app.db import get_test_connection

class TestDatasetRepository:
    """Integration tests for DatasetRepository."""
    
    @pytest.mark.integration
    def test_save_and_fetch_dataset(self, test_db):
        """Test saving and fetching from actual database."""
        # Arrange
        repo = DatasetRepository(connection=test_db)
        dataset_data = {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "title": "Integration Test Dataset",
            "description": "Test description"
        }
        
        # Act
        repo.save(dataset_data)
        result = repo.get_by_id(dataset_data["id"])
        
        # Assert
        assert result is not None
        assert result["title"] == "Integration Test Dataset"
    
    @pytest.mark.integration
    def test_database_constraints_enforced(self, test_db):
        """Test that database constraints are actually enforced."""
        repo = DatasetRepository(connection=test_db)
        
        # Should fail due to NOT NULL constraint
        with pytest.raises(Exception):  # DB constraint error
            repo.save({"title": None})
    
    @pytest.mark.integration
    def test_transaction_rollback(self, test_db):
        """Test transaction rollback on failure."""
        repo = DatasetRepository(connection=test_db)
        
        try:
            with test_db.begin():
                repo.save({"id": "test-id", "title": "Temp"})
                raise Exception("Simulated error")
        except Exception:
            pass
        
        # Data should not be persisted
        result = repo.get_by_id("test-id")
        assert result is None
```

### Test Database Setup
```python
# conftest.py - pytest fixtures
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for tests."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres

@pytest.fixture
def test_db(postgres_container):
    """Provide test database connection."""
    from app.db import get_connection
    conn = get_connection(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        database="test_db"
    )
    yield conn
    conn.close()
```

## Test Coverage Rules

- **Minimum 80% branch coverage** required
- **Every public function** must have at least one test
- **Error paths** must be tested
- **Edge cases** must be tested
- **Boundaries** must be tested

Coverage command:
```bash
pytest --cov=app --cov-report=html --cov-fail-under=80
```

## Test Organization

### By Feature
```
tests/
├── unit/
│   ├── test_dataset_service.py
│   ├── test_ontology_service.py
│   └── test_mapping_service.py
└── integration/
    ├── test_dataset_repository.py
    ├── test_ontology_repository.py
    └── test_mapping_repository.py
```

### By Type
```
tests/
├── unit/
│   ├── services/
│   ├── repositories/
│   ├── models/
│   └── validators/
└── integration/
    ├── repositories/
    ├── services/
    └── api/
```

## Quick Checklist for AI

Before generating code, ask:
- [ ] Is this testable code?
- [ ] Are tests mocking external dependencies?
- [ ] Are error cases tested?
- [ ] Is edge case coverage included?
- [ ] Are type hints present?
- [ ] Is docstring present?
- [ ] Does this follow Clean Architecture?
- [ ] Is there hardcoded sensitive data?

## Debugging Failed Tests

Common patterns:
1. **Mock not configured correctly** - Verify mock return values
2. **Missing pytest markers** - Add `@pytest.mark.unit` or `@pytest.mark.integration`
3. **Database state pollution** - Ensure cleanup in fixtures
4. **Async issues** - Use `pytest-asyncio` for async tests
5. **Import errors** - Verify imports match actual module structure

## Performance

- Unit tests: < 1 second total
- Integration tests: < 30 seconds total
- CI/CD should run all tests in < 2 minutes

Slow tests indicate:
- Too many external calls in unit test (needs mocking)
- Database queries not optimized (check indexes)
- Unnecessary test setup (use fixtures)
