"""Sample unit test demonstrating testing standards."""

import pytest
from unittest.mock import Mock
from datetime import datetime


class ExampleService:
    """Example service to demonstrate testing."""
    
    def __init__(self, repository: Mock) -> None:
        """Initialize service with injected repository."""
        self.repository = repository
    
    def get_dataset(self, dataset_id: str) -> dict:
        """
        Fetch a dataset by ID.
        
        Args:
            dataset_id: The dataset identifier
            
        Returns:
            Dictionary containing dataset information
            
        Raises:
            ValueError: If dataset_id is empty
            DatasetNotFoundError: If dataset doesn't exist
        """
        if not dataset_id:
            raise ValueError("dataset_id cannot be empty")
        
        result = self.repository.get_by_id(dataset_id)
        if not result:
            raise Exception(f"Dataset {dataset_id} not found")
        
        return result


class TestExampleService:
    """Test suite for ExampleService - demonstrates unit testing."""
    
    @pytest.mark.unit
    def test_get_dataset_success(self) -> None:
        """Test successfully retrieving a dataset."""
        # Arrange
        mock_repo = Mock()
        expected_dataset = {
            "id": "123",
            "title": "Test Dataset",
            "description": "Test Description"
        }
        mock_repo.get_by_id.return_value = expected_dataset
        
        service = ExampleService(repository=mock_repo)
        
        # Act
        result = service.get_dataset("123")
        
        # Assert
        assert result == expected_dataset
        assert result["title"] == "Test Dataset"
        mock_repo.get_by_id.assert_called_once_with("123")
    
    @pytest.mark.unit
    def test_get_dataset_not_found(self) -> None:
        """Test retrieving non-existent dataset."""
        # Arrange
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = None
        
        service = ExampleService(repository=mock_repo)
        
        # Act & Assert
        with pytest.raises(Exception, match="not found"):
            service.get_dataset("999")
    
    @pytest.mark.unit
    def test_get_dataset_empty_id(self) -> None:
        """Test with empty dataset ID."""
        # Arrange
        mock_repo = Mock()
        service = ExampleService(repository=mock_repo)
        
        # Act & Assert
        with pytest.raises(ValueError, match="cannot be empty"):
            service.get_dataset("")
    
    @pytest.mark.unit
    def test_get_dataset_none_id(self) -> None:
        """Test with None dataset ID."""
        # Arrange
        mock_repo = Mock()
        service = ExampleService(repository=mock_repo)
        
        # Act & Assert
        with pytest.raises((ValueError, TypeError)):
            service.get_dataset(None)  # type: ignore


# Example of testing with fixtures
@pytest.fixture
def sample_service() -> ExampleService:
    """Fixture providing a service with mocked repository."""
    mock_repo = Mock()
    return ExampleService(repository=mock_repo)


class TestExampleServiceWithFixture:
    """Alternative testing approach using pytest fixtures."""
    
    @pytest.mark.unit
    def test_with_fixture(self, sample_service: ExampleService) -> None:
        """Test using fixture."""
        # Arrange
        sample_service.repository.get_by_id.return_value = {"id": "1"}
        
        # Act
        result = sample_service.get_dataset("1")
        
        # Assert
        assert result["id"] == "1"
