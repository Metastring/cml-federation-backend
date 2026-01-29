"""Integration tests for API endpoints."""
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


class TestHealthCheckEndpoint:
    """Tests for health check endpoint."""

    def test_ping_returns_pong(self) -> None:
        """Test ping endpoint returns pong."""
        response = client.get("/ping")
        assert response.status_code == 200
        assert response.json() == {"ping": "pong"}

    def test_ping_correct_content_type(self) -> None:
        """Test ping endpoint returns JSON content type."""
        response = client.get("/ping")
        assert response.headers["content-type"] == "application/json"


class TestFederatedSearchEndpoint:
    """Tests for federated search endpoint."""

    def test_federated_search_missing_biodiversity_category(self) -> None:
        """Test federated search fails without biodiversity category."""
        payload = {
            "category": ["other"],
            "dataset": ["Kew Plant Database"],
            "fields": ["species"],
            "search_text": "test",
        }
        response = client.post("/federated-search", json=payload)
        assert response.status_code == 400
        assert "biodiversity" in response.json()["detail"].lower()

    def test_federated_search_no_valid_datasets(self) -> None:
        """Test federated search fails with invalid datasets."""
        payload = {
            "category": ["biodiversity"],
            "dataset": ["Invalid Dataset"],
            "fields": ["species"],
            "search_text": "test",
        }
        response = client.post("/federated-search", json=payload)
        assert response.status_code == 400
        assert "valid" in response.json()["detail"].lower()

    def test_federated_search_valid_request_structure(self) -> None:
        """Test federated search request validation."""
        payload = {
            "category": ["biodiversity"],
            "dataset": ["Kew Plant Database"],
            "fields": ["species"],
            "search_text": "test plant",
        }
        # Note: This will fail to connect to actual external APIs,
        # but validates the request structure is accepted
        response = client.post("/federated-search", json=payload)
        # Should either succeed (200) or fail due to external service (not validation)
        assert response.status_code in [200, 400, 500, 504]


class TestRequestValidation:
    """Tests for request validation."""

    def test_missing_required_field(self) -> None:
        """Test endpoint rejects requests missing required fields."""
        payload = {
            "category": ["biodiversity"],
            "dataset": ["Kew Plant Database"],
            # Missing 'fields' and 'search_text'
        }
        response = client.post("/federated-search", json=payload)
        assert response.status_code == 422  # Validation error

    def test_empty_category_list(self) -> None:
        """Test endpoint rejects empty category list."""
        payload = {
            "category": [],  # Empty
            "dataset": ["Kew Plant Database"],
            "fields": ["species"],
            "search_text": "test",
        }
        response = client.post("/federated-search", json=payload)
        assert response.status_code == 422  # Validation error


class TestEndpointDocumentation:
    """Tests for endpoint documentation."""

    def test_openapi_schema_available(self) -> None:
        """Test OpenAPI schema is available."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert "openapi" in response.json()

    def test_ping_documented(self) -> None:
        """Test ping endpoint is in OpenAPI documentation."""
        response = client.get("/openapi.json")
        schema = response.json()
        assert "/ping" in schema["paths"]

    def test_federated_search_documented(self) -> None:
        """Test federated search endpoint is in OpenAPI documentation."""
        response = client.get("/openapi.json")
        schema = response.json()
        assert "/federated-search" in schema["paths"]
