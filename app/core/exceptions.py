"""Custom exception classes for domain errors."""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DomainException(Exception):
    """Base exception for domain errors."""

    def __init__(self, message: str, error_code: str = "DOMAIN_ERROR") -> None:
        """Initialize domain exception.

        Args:
            message: Human-readable error message.
            error_code: Unique error code for tracking.
        """
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)
        logger.error(f"Domain exception: {error_code} - {message}")


class NotFoundError(DomainException):
    """Resource not found error."""

    def __init__(self, resource_type: str, resource_id: Optional[str] = None) -> None:
        """Initialize not found error.

        Args:
            resource_type: Type of resource not found (e.g., 'Domain', 'Concept').
            resource_id: Optional ID of the resource.
        """
        message = f"{resource_type} not found"
        if resource_id:
            message += f" (ID: {resource_id})"
        super().__init__(message, "NOT_FOUND")


class ValidationError(DomainException):
    """Validation failed error."""

    def __init__(self, message: str) -> None:
        """Initialize validation error.

        Args:
            message: Validation error message.
        """
        super().__init__(message, "VALIDATION_ERROR")


class ConflictError(DomainException):
    """Resource conflict error (duplicate)."""

    def __init__(self, resource_type: str, identifier: str) -> None:
        """Initialize conflict error.

        Args:
            resource_type: Type of resource with conflict.
            identifier: Identifier causing conflict.
        """
        message = f"{resource_type} '{identifier}' already exists"
        super().__init__(message, "CONFLICT")


class DatabaseError(DomainException):
    """Database operation error."""

    def __init__(self, message: str, operation: str = "operation") -> None:
        """Initialize database error.

        Args:
            message: Error details.
            operation: Type of operation that failed.
        """
        full_message = f"Database {operation} failed: {message}"
        super().__init__(full_message, "DATABASE_ERROR")


class ExternalServiceError(DomainException):
    """External service call error."""

    def __init__(self, service_name: str, message: str) -> None:
        """Initialize external service error.

        Args:
            service_name: Name of the external service.
            message: Error details.
        """
        full_message = f"External service '{service_name}' error: {message}"
        super().__init__(full_message, "EXTERNAL_SERVICE_ERROR")