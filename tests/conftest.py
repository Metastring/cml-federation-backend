"""Shared pytest fixtures and configuration."""

import pytest
from typing import Generator
import os


@pytest.fixture(scope="session")
def test_config():
    """Load test configuration."""
    return {
        "database_url": os.getenv(
            "TEST_DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5432/central_server_test"
        ),
        "api_base_url": os.getenv("API_BASE_URL", "http://localhost:8000"),
    }


@pytest.fixture
def mock_db_connection():
    """Mock database connection for unit tests."""
    from unittest.mock import Mock
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    return mock_conn


# Integration test fixtures can be added here
# Example:
# @pytest.fixture
# def test_db():
#     """Real database connection for integration tests."""
#     from app.db import get_connection
#     conn = get_connection()
#     yield conn
#     conn.close()
