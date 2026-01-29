"""Unit tests for validator utility functions."""
import pytest

from app.utils.validators import (
    validate_email,
    validate_enum,
    validate_required_fields,
    validate_string,
    validate_uuid,
)


class TestValidateString:
    """Tests for string validation."""

    def test_valid_string(self) -> None:
        """Test validation of valid string."""
        assert validate_string("test", min_length=1, max_length=10) is True

    def test_string_too_short(self) -> None:
        """Test validation fails for string below minimum length."""
        assert validate_string("a", min_length=2, max_length=10) is False

    def test_string_too_long(self) -> None:
        """Test validation fails for string exceeding maximum length."""
        assert validate_string("test", min_length=1, max_length=2) is False

    def test_string_with_pattern_match(self) -> None:
        """Test validation with pattern matching."""
        assert validate_string("ABC123", pattern=r"^[A-Z0-9]+$") is True

    def test_string_with_pattern_no_match(self) -> None:
        """Test validation fails when pattern doesn't match."""
        assert validate_string("abc", pattern=r"^[A-Z0-9]+$") is False

    def test_non_string_input(self) -> None:
        """Test validation fails for non-string input."""
        assert validate_string(123) is False  # type: ignore


class TestValidateRequiredFields:
    """Tests for required fields validation."""

    def test_all_fields_present(self) -> None:
        """Test validation passes when all required fields present."""
        data = {"name": "John", "email": "john@example.com"}
        assert validate_required_fields(data, ["name", "email"]) is True

    def test_missing_field(self) -> None:
        """Test validation fails when required field is missing."""
        data = {"name": "John"}
        assert validate_required_fields(data, ["name", "email"]) is False

    def test_empty_field_string(self) -> None:
        """Test validation fails when required field is empty string."""
        data = {"name": "", "email": "john@example.com"}
        assert validate_required_fields(data, ["name", "email"]) is False

    def test_none_field(self) -> None:
        """Test validation fails when required field is None."""
        data = {"name": None, "email": "john@example.com"}
        assert validate_required_fields(data, ["name", "email"]) is False


class TestValidateEmail:
    """Tests for email validation."""

    def test_valid_email(self) -> None:
        """Test validation of valid email."""
        assert validate_email("user@example.com") is True

    def test_invalid_email_no_domain(self) -> None:
        """Test validation fails for email without domain."""
        assert validate_email("user@") is False

    def test_invalid_email_no_tld(self) -> None:
        """Test validation fails for email without TLD."""
        assert validate_email("user@example") is False

    def test_invalid_email_no_at(self) -> None:
        """Test validation fails for email without @."""
        assert validate_email("userexample.com") is False


class TestValidateUUID:
    """Tests for UUID validation."""

    def test_valid_uuid_v4(self) -> None:
        """Test validation of valid UUID v4."""
        valid_uuid = "550e8400-e29b-41d4-a716-446655440000"
        assert validate_uuid(valid_uuid) is True

    def test_invalid_uuid_wrong_version(self) -> None:
        """Test validation fails for non-v4 UUID."""
        invalid_uuid = "550e8400-e29b-11d4-a716-446655440000"  # v1, not v4
        assert validate_uuid(invalid_uuid) is False

    def test_invalid_uuid_format(self) -> None:
        """Test validation fails for malformed UUID."""
        assert validate_uuid("not-a-uuid") is False

    def test_invalid_uuid_wrong_length(self) -> None:
        """Test validation fails for UUID wrong length."""
        assert validate_uuid("550e8400-e29b-41d4-a716") is False


class TestValidateEnum:
    """Tests for enum validation."""

    def test_valid_enum_value(self) -> None:
        """Test validation of allowed enum value."""
        assert validate_enum("active", ["active", "inactive"]) is True

    def test_invalid_enum_value(self) -> None:
        """Test validation fails for disallowed enum value."""
        assert validate_enum("pending", ["active", "inactive"]) is False

    def test_enum_case_sensitive(self) -> None:
        """Test enum validation is case-sensitive."""
        assert validate_enum("Active", ["active", "inactive"]) is False

    def test_enum_with_none(self) -> None:
        """Test enum validation with None values."""
        assert validate_enum(None, ["active", "inactive", None]) is True
