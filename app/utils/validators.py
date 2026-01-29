"""Validation helpers and utility functions."""import logging
import re
from typing import Any, Callable, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def validate_string(
    value: str,
    min_length: int = 0,
    max_length: int = 255,
    pattern: Optional[str] = None,
    field_name: str = "field",
) -> bool:
    """Validate string field with constraints.

    Args:
        value: String value to validate.
        min_length: Minimum allowed length.
        max_length: Maximum allowed length.
        pattern: Optional regex pattern to match.
        field_name: Name of field for error messages.

    Returns:
        True if valid, False otherwise.

    Example:
        >>> validate_string("test", min_length=1, max_length=10)
        True
    """
    if not isinstance(value, str):
        logger.warning(f"{field_name} is not a string")
        return False

    if len(value) < min_length or len(value) > max_length:
        logger.warning(
            f"{field_name} length {len(value)} out of range "
            f"[{min_length}, {max_length}]"
        )
        return False

    if pattern and not re.match(pattern, value):
        logger.warning(f"{field_name} does not match pattern: {pattern}")
        return False

    return True


def validate_required_fields(
    data: dict[str, Any], required_fields: List[str]
) -> bool:
    """Validate that all required fields are present and non-empty.

    Args:
        data: Dictionary to validate.
        required_fields: List of required field names.

    Returns:
        True if all required fields present and non-empty, False otherwise.

    Example:
        >>> data = {"name": "John", "email": "john@example.com"}
        >>> validate_required_fields(data, ["name", "email"])
        True
    """
    for field in required_fields:
        if field not in data:
            logger.warning(f"Required field missing: {field}")
            return False

        if data[field] is None or (isinstance(data[field], str) and not data[field].strip()):
            logger.warning(f"Required field is empty: {field}")
            return False

    return True


def validate_email(email: str) -> bool:
    """Validate email address format.

    Args:
        email: Email address to validate.

    Returns:
        True if email is valid format, False otherwise.

    Example:
        >>> validate_email("user@example.com")
        True
    """
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return validate_string(email, min_length=5, max_length=254, pattern=pattern, field_name="email")


def validate_uuid(value: str, field_name: str = "uuid") -> bool:
    """Validate UUID v4 format.

    Args:
        value: UUID string to validate.
        field_name: Name of field for error messages.

    Returns:
        True if valid UUID v4 format, False otherwise.

    Example:
        >>> validate_uuid("550e8400-e29b-41d4-a716-446655440000")
        True
    """
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    return validate_string(value, min_length=36, max_length=36, pattern=pattern, field_name=field_name)


def validate_enum(
    value: Any, allowed_values: List[Any], field_name: str = "field"
) -> bool:
    """Validate that value is in allowed list.

    Args:
        value: Value to validate.
        allowed_values: List of allowed values.
        field_name: Name of field for error messages.

    Returns:
        True if value is allowed, False otherwise.

    Example:
        >>> validate_enum("active", ["active", "inactive"])
        True
    """
    if value not in allowed_values:
        logger.warning(
            f"{field_name} value '{value}' not in allowed values: {allowed_values}"
        )
        return False

    return True