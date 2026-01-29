"""Unit tests for custom exception classes."""
import pytest

from app.core.exceptions import (
    ConflictError,
    DatabaseError,
    DomainException,
    ExternalServiceError,
    NotFoundError,
    ValidationError,
)


class TestDomainException:
    """Tests for base DomainException."""

    def test_domain_exception_creation(self) -> None:
        """Test creating domain exception with message and error code."""
        exc = DomainException("Test error", "TEST_ERROR")
        assert exc.message == "Test error"
        assert exc.error_code == "TEST_ERROR"
        assert str(exc) == "Test error"

    def test_domain_exception_default_error_code(self) -> None:
        """Test domain exception uses default error code."""
        exc = DomainException("Test error")
        assert exc.error_code == "DOMAIN_ERROR"

    def test_domain_exception_inheritance(self) -> None:
        """Test DomainException is subclass of Exception."""
        exc = DomainException("Test")
        assert isinstance(exc, Exception)


class TestNotFoundError:
    """Tests for NotFoundError."""

    def test_not_found_error_with_id(self) -> None:
        """Test NotFoundError includes resource type and ID."""
        exc = NotFoundError("Domain", "123")
        assert "Domain not found" in str(exc)
        assert "123" in str(exc)
        assert exc.error_code == "NOT_FOUND"

    def test_not_found_error_without_id(self) -> None:
        """Test NotFoundError without resource ID."""
        exc = NotFoundError("Concept")
        assert "Concept not found" in str(exc)
        assert exc.error_code == "NOT_FOUND"


class TestValidationError:
    """Tests for ValidationError."""

    def test_validation_error_creation(self) -> None:
        """Test creating validation error."""
        exc = ValidationError("Invalid field value")
        assert exc.message == "Invalid field value"
        assert exc.error_code == "VALIDATION_ERROR"
        assert isinstance(exc, DomainException)


class TestConflictError:
    """Tests for ConflictError."""

    def test_conflict_error_creation(self) -> None:
        """Test creating conflict error."""
        exc = ConflictError("Domain", "bio")
        assert "Domain 'bio' already exists" in str(exc)
        assert exc.error_code == "CONFLICT"

    def test_conflict_error_inheritance(self) -> None:
        """Test ConflictError inherits from DomainException."""
        exc = ConflictError("Dataset", "test-123")
        assert isinstance(exc, DomainException)


class TestDatabaseError:
    """Tests for DatabaseError."""

    def test_database_error_with_operation(self) -> None:
        """Test database error includes operation type."""
        exc = DatabaseError("Connection timeout", "connect")
        assert "Database connect failed" in str(exc)
        assert exc.error_code == "DATABASE_ERROR"

    def test_database_error_default_operation(self) -> None:
        """Test database error uses default operation type."""
        exc = DatabaseError("Unknown error")
        assert "Database operation failed" in str(exc)


class TestExternalServiceError:
    """Tests for ExternalServiceError."""

    def test_external_service_error_creation(self) -> None:
        """Test creating external service error."""
        exc = ExternalServiceError("Kew API", "Response timeout")
        assert "Kew API" in str(exc)
        assert "Response timeout" in str(exc)
        assert exc.error_code == "EXTERNAL_SERVICE_ERROR"


class TestExceptionHierarchy:
    """Tests for exception inheritance hierarchy."""

    def test_all_custom_exceptions_inherit_from_domain(self) -> None:
        """Test all custom exceptions inherit from DomainException."""
        exceptions = [
            NotFoundError("Test"),
            ValidationError("Test"),
            ConflictError("Test", "test"),
            DatabaseError("Test"),
            ExternalServiceError("Test", "Test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, DomainException)
            assert isinstance(exc, Exception)

    def test_exception_error_codes_unique(self) -> None:
        """Test each exception has unique error code."""
        exceptions = {
            NotFoundError("Test").error_code,
            ValidationError("Test").error_code,
            ConflictError("Test", "test").error_code,
            DatabaseError("Test").error_code,
            ExternalServiceError("Test", "Test").error_code,
        }

        # All should be unique (set size equals number of exceptions)
        assert len(exceptions) == 5
