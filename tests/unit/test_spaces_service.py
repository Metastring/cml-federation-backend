"""Unit tests for DigitalOcean Spaces functionality."""
import pytest
from unittest.mock import MagicMock, Mock, patch

from app.core.exceptions import ValidationError, DatabaseError
from app.repositories.spaces_repository import SpacesRepository
from app.services.spaces_service import SpacesService


class TestSpacesRepository:
    """Tests for SpacesRepository."""

    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        return MagicMock()

    @pytest.fixture
    def repository(self, mock_s3_client):
        """Repository with mocked S3 client."""
        with patch("app.repositories.spaces_repository.boto3.client") as mock_boto3:
            mock_boto3.return_value = mock_s3_client
            repo = SpacesRepository()
            repo.s3_client = mock_s3_client
            return repo

    def test_create_folder_success(self, repository, mock_s3_client):
        """Test successful folder creation."""
        mock_s3_client.put_object.return_value = {}

        result = repository.create_folder("test_folder")

        assert result is True
        mock_s3_client.put_object.assert_called_once()

    def test_create_folder_with_slash(self, repository, mock_s3_client):
        """Test folder creation with trailing slash."""
        mock_s3_client.put_object.return_value = {}

        repository.create_folder("test_folder/")

        # Check that slash is preserved
        call_args = mock_s3_client.put_object.call_args
        assert "test_folder/" in call_args[1]["Key"]

    def test_upload_file_success(self, repository, mock_s3_client):
        """Test successful file upload."""
        mock_s3_client.put_object.return_value = {}
        file_content = b"test content"

        result = repository.upload_file("test.txt", file_content)

        assert result["file_name"] == "test.txt"
        assert "url" in result
        mock_s3_client.put_object.assert_called_once()

    def test_upload_file_with_folder(self, repository, mock_s3_client):
        """Test file upload to a specific folder."""
        mock_s3_client.put_object.return_value = {}
        file_content = b"test content"

        result = repository.upload_file("test.txt", file_content, "my_folder")

        call_args = mock_s3_client.put_object.call_args
        assert "my_folder/test.txt" in call_args[1]["Key"]

    def test_upload_multiple_files_success(self, repository, mock_s3_client):
        """Test successful multiple file upload."""
        mock_s3_client.put_object.return_value = {}
        files = [
            ("file1.txt", b"content1"),
            ("file2.txt", b"content2"),
        ]

        result = repository.upload_multiple_files(files)

        assert len(result) == 2
        assert mock_s3_client.put_object.call_count == 2

    def test_list_folders_success(self, repository, mock_s3_client):
        """Test successful folder listing."""
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "folder1/"},
                    {"Prefix": "folder2/"},
                ]
            }
        ]

        result = repository.list_folders()

        assert len(result) == 2
        assert "folder1/" in result
        assert "folder2/" in result

    def test_list_folder_contents_success(self, repository, mock_s3_client):
        """Test successful folder contents listing."""
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [{"Prefix": "subfolder/"}],
                "Contents": [
                    {
                        "Key": "file.txt",
                        "Size": 1024,
                        "LastModified": MagicMock(isoformat=lambda: "2024-01-01T00:00:00"),
                    }
                ],
            }
        ]

        result = repository.list_folder_contents("test_folder")

        assert result["folder_path"] == "test_folder/"
        assert len(result["files"]) == 1
        assert len(result["subfolders"]) == 1

    def test_delete_file_success(self, repository, mock_s3_client):
        """Test successful file deletion."""
        mock_s3_client.delete_object.return_value = {}

        result = repository.delete_file("path/to/file.txt")

        assert result is True
        mock_s3_client.delete_object.assert_called_once()

    def test_get_file_info_success(self, repository, mock_s3_client):
        """Test successful file info retrieval."""
        mock_s3_client.head_object.return_value = {
            "ContentLength": 1024,
            "LastModified": MagicMock(isoformat=lambda: "2024-01-01T00:00:00"),
            "ContentType": "text/plain",
        }

        result = repository.get_file_info("path/to/file.txt")

        assert result["size"] == 1024
        assert "url" in result


class TestSpacesService:
    """Tests for SpacesService."""

    @pytest.fixture
    def mock_repository(self):
        """Mock repository."""
        return MagicMock(spec=SpacesRepository)

    @pytest.fixture
    def service(self, mock_repository):
        """Service with mocked repository."""
        service = SpacesService()
        service.repository = mock_repository
        return service

    def test_create_folder_success(self, service, mock_repository):
        """Test successful folder creation."""
        mock_repository.create_folder.return_value = True

        result = service.create_folder("test_folder")

        assert result["status"] == "success"
        assert "test_folder" in result["folder_path"]

    def test_create_folder_invalid_path_starts_with_slash(self, service):
        """Test folder creation with invalid path starting with slash."""
        with pytest.raises(ValidationError):
            service.create_folder("/test_folder")

    def test_create_folder_path_traversal_attempt(self, service):
        """Test folder creation with path traversal attempt."""
        with pytest.raises(ValidationError):
            service.create_folder("../test_folder")

    def test_create_folder_empty_path(self, service):
        """Test folder creation with empty path."""
        with pytest.raises(ValidationError):
            service.create_folder("")

    def test_upload_file_success(self, service, mock_repository):
        """Test successful file upload."""
        mock_repository.upload_file.return_value = {
            "file_name": "test.txt",
            "url": "http://example.com/test.txt",
        }

        result = service.upload_file("test.txt", b"content")

        assert result["status"] == "success"
        assert result["file_name"] == "test.txt"

    def test_upload_file_empty_content(self, service):
        """Test file upload with empty content."""
        with pytest.raises(ValidationError):
            service.upload_file("test.txt", b"")

    def test_upload_file_invalid_name(self, service):
        """Test file upload with invalid file name."""
        with pytest.raises(ValidationError):
            service.upload_file("", b"content")

    def test_upload_file_invalid_folder_path(self, service):
        """Test file upload with invalid folder path."""
        with pytest.raises(ValidationError):
            service.upload_file("test.txt", b"content", "/invalid/path")

    def test_upload_multiple_files_success(self, service, mock_repository):
        """Test successful multiple file upload."""
        mock_repository.upload_multiple_files.return_value = [
            {"file_name": "file1.txt", "url": "http://example.com/file1.txt"},
            {"file_name": "file2.txt", "url": "http://example.com/file2.txt"},
        ]

        files = [
            {"name": "file1.txt", "content": b"content1"},
            {"name": "file2.txt", "content": b"content2"},
        ]

        result = service.upload_multiple_files(files)

        assert result["status"] == "success"
        assert result["uploaded_count"] == 2

    def test_upload_multiple_files_empty_list(self, service):
        """Test multiple file upload with empty list."""
        with pytest.raises(ValidationError):
            service.upload_multiple_files([])

    def test_upload_multiple_files_missing_keys(self, service):
        """Test multiple file upload with missing keys."""
        files = [{"name": "file1.txt"}]  # Missing 'content' key
        with pytest.raises(ValidationError):
            service.upload_multiple_files(files)

    def test_list_folders_success(self, service, mock_repository):
        """Test successful folder listing."""
        mock_repository.list_folders.return_value = ["folder1/", "folder2/"]

        result = service.list_folders()

        assert result["status"] == "success"
        assert result["folder_count"] == 2

    def test_list_folders_invalid_prefix(self, service):
        """Test folder listing with invalid prefix."""
        with pytest.raises(ValidationError):
            service.list_folders(prefix="/invalid")

    def test_list_folder_contents_success(self, service, mock_repository):
        """Test successful folder contents listing."""
        mock_repository.list_folder_contents.return_value = {
            "folder_path": "test_folder/",
            "files": [],
            "subfolders": [],
            "total_items": 0,
        }

        result = service.list_folder_contents("test_folder")

        assert result["status"] == "success"
        assert result["file_count"] == 0

    def test_list_folder_contents_invalid_path(self, service):
        """Test folder contents listing with invalid path."""
        with pytest.raises(ValidationError):
            service.list_folder_contents("/invalid")

    def test_delete_file_success(self, service, mock_repository):
        """Test successful file deletion."""
        mock_repository.delete_file.return_value = True

        result = service.delete_file("path/to/file.txt")

        assert result["status"] == "success"

    def test_delete_file_invalid_key(self, service):
        """Test file deletion with invalid key."""
        with pytest.raises(ValidationError):
            service.delete_file("")

    def test_get_file_info_success(self, service, mock_repository):
        """Test successful file info retrieval."""
        mock_repository.get_file_info.return_value = {
            "file_key": "path/to/file.txt",
            "size": 1024,
        }

        result = service.get_file_info("path/to/file.txt")

        assert result["status"] == "success"

    def test_get_file_info_invalid_key(self, service):
        """Test file info retrieval with invalid key."""
        with pytest.raises(ValidationError):
            service.get_file_info("")
