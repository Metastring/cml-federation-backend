"""DigitalOcean Spaces service for file operations business logic."""
import logging
from typing import List, Optional

from app.core.exceptions import ValidationError
from app.repositories.spaces_repository import SpacesRepository

logger = logging.getLogger(__name__)


class SpacesService:
    """Service layer for DigitalOcean Spaces operations.

    Handles business logic and validation for file and folder operations.
    """

    def __init__(self):
        """Initialize Spaces service with repository."""
        self.repository = SpacesRepository()

    def create_folder(self, folder_path: str) -> dict:
        """Create a new folder in Spaces.

        Args:
            folder_path: Path to the folder (e.g., 'datasets/project1')

        Returns:
            Dictionary with folder creation status

        Raises:
            ValidationError: If folder path is invalid
        """
        # Validate folder path
        if not folder_path or not isinstance(folder_path, str):
            raise ValidationError("Folder path must be a non-empty string")

        if folder_path.startswith("/"):
            raise ValidationError("Folder path should not start with '/'")

        if ".." in folder_path:
            raise ValidationError("Folder path cannot contain '..'")

        # Ensure path ends with /
        if not folder_path.endswith("/"):
            folder_path += "/"

        try:
            self.repository.create_folder(folder_path)

            logger.info(
                "Folder created successfully",
                extra={"folder_path": folder_path},
            )

            return {
                "status": "success",
                "folder_path": folder_path,
                "message": f"Folder '{folder_path}' created successfully",
            }

        except Exception as e:
            logger.error(
                f"Failed to create folder: {e}",
                extra={"folder_path": folder_path},
            )
            raise

    def upload_file(
        self,
        file_name: str,
        file_content: bytes,
        folder_path: Optional[str] = None,
    ) -> dict:
        """Upload a single file to Spaces.

        Args:
            file_name: Name of the file
            file_content: File content as bytes
            folder_path: Optional folder path

        Returns:
            Dictionary with file upload information

        Raises:
            ValidationError: If file data is invalid
        """
        # Validate file data
        if not file_name or not isinstance(file_name, str):
            raise ValidationError("File name must be a non-empty string")

        if not file_content or not isinstance(file_content, bytes):
            raise ValidationError("File content must be provided as bytes")

        # Validate file name (no path separators)
        if "/" in file_name or "\\" in file_name:
            raise ValidationError("File name cannot contain path separators")

        # Validate folder path if provided
        if folder_path:
            if folder_path.startswith("/"):
                raise ValidationError("Folder path should not start with '/'")
            if ".." in folder_path:
                raise ValidationError("Folder path cannot contain '..'")

        try:
            result = self.repository.upload_file(file_name, file_content, folder_path)

            logger.info(
                "File uploaded successfully",
                extra={
                    "file_name": file_name,
                    "folder": folder_path,
                    "size": len(file_content),
                },
            )

            return {
                "status": "success",
                "file_name": file_name,
                "folder_path": folder_path or "root",
                "url": result["url"],
                "file_size": len(file_content),
                "message": f"File '{file_name}' uploaded successfully",
            }

        except Exception as e:
            logger.error(
                f"Failed to upload file: {e}",
                extra={"file_name": file_name},
            )
            raise

    def upload_multiple_files(
        self,
        files: List[dict],
        folder_path: Optional[str] = None,
    ) -> dict:
        """Upload multiple files to Spaces.

        Args:
            files: List of dictionaries with 'name' and 'content' keys
            folder_path: Optional folder path

        Returns:
            Dictionary with upload results

        Raises:
            ValidationError: If file data is invalid
        """
        # Validate input
        if not files or not isinstance(files, list):
            raise ValidationError("Files must be provided as a non-empty list")

        if len(files) == 0:
            raise ValidationError("At least one file must be provided")

        # Validate each file
        for file_item in files:
            if not isinstance(file_item, dict):
                raise ValidationError("Each file must be a dictionary")
            if "name" not in file_item or "content" not in file_item:
                raise ValidationError("Each file must have 'name' and 'content' keys")

        # Validate folder path if provided
        if folder_path:
            if folder_path.startswith("/"):
                raise ValidationError("Folder path should not start with '/'")
            if ".." in folder_path:
                raise ValidationError("Folder path cannot contain '..'")

        try:
            # Convert to format expected by repository
            file_tuples = [
                (file_item["name"], file_item["content"]) for file_item in files
            ]

            uploaded_files = self.repository.upload_multiple_files(
                file_tuples, folder_path
            )

            logger.info(
                "Multiple files uploaded successfully",
                extra={
                    "count": len(uploaded_files),
                    "folder": folder_path,
                },
            )

            return {
                "status": "success",
                "total_files": len(files),
                "uploaded_count": len(uploaded_files),
                "folder_path": folder_path or "root",
                "files": uploaded_files,
                "message": f"{len(uploaded_files)} out of {len(files)} files uploaded successfully",
            }

        except Exception as e:
            logger.error(
                f"Failed to upload multiple files: {e}",
                extra={"file_count": len(files)},
            )
            raise

    def list_folders(self, prefix: str = "") -> dict:
        """List all visible folders in Spaces.

        Args:
            prefix: Optional prefix to filter folders

        Returns:
            Dictionary with list of folders

        Raises:
            ValidationError: If prefix is invalid
        """
        # Validate prefix
        if prefix and (prefix.startswith("/") or ".." in prefix):
            raise ValidationError("Invalid prefix format")

        try:
            folders = self.repository.list_folders(prefix)

            logger.info(
                "Folders listed successfully",
                extra={"prefix": prefix, "count": len(folders)},
            )

            return {
                "status": "success",
                "prefix": prefix or "root",
                "folder_count": len(folders),
                "folders": folders,
                "message": f"Found {len(folders)} folders",
            }

        except Exception as e:
            logger.error(
                f"Failed to list folders: {e}",
                extra={"prefix": prefix},
            )
            raise

    def list_folder_contents(self, folder_path: str) -> dict:
        """List all contents of a folder.

        Args:
            folder_path: Path to the folder

        Returns:
            Dictionary with folder contents

        Raises:
            ValidationError: If folder path is invalid
        """
        # Validate folder path
        if not folder_path or not isinstance(folder_path, str):
            raise ValidationError("Folder path must be a non-empty string")

        if folder_path.startswith("/"):
            raise ValidationError("Folder path should not start with '/'")

        if ".." in folder_path:
            raise ValidationError("Folder path cannot contain '..'")

        try:
            contents = self.repository.list_folder_contents(folder_path)

            logger.info(
                "Folder contents listed successfully",
                extra={
                    "folder_path": folder_path,
                    "file_count": len(contents["files"]),
                    "subfolder_count": len(contents["subfolders"]),
                },
            )

            return {
                "status": "success",
                "folder_path": folder_path,
                "file_count": len(contents["files"]),
                "subfolder_count": len(contents["subfolders"]),
                "total_items": contents["total_items"],
                "files": contents["files"],
                "subfolders": contents["subfolders"],
                "message": f"Folder contains {contents['total_items']} items",
            }

        except Exception as e:
            logger.error(
                f"Failed to list folder contents: {e}",
                extra={"folder_path": folder_path},
            )
            raise

    def delete_file(self, file_key: str) -> dict:
        """Delete a file from Spaces.

        Args:
            file_key: Full S3 key of the file

        Returns:
            Dictionary with deletion status

        Raises:
            ValidationError: If file key is invalid
        """
        # Validate file key
        if not file_key or not isinstance(file_key, str):
            raise ValidationError("File key must be a non-empty string")

        if ".." in file_key:
            raise ValidationError("File key cannot contain '..'")

        try:
            self.repository.delete_file(file_key)

            logger.info(
                "File deleted successfully",
                extra={"file_key": file_key},
            )

            return {
                "status": "success",
                "file_key": file_key,
                "message": f"File '{file_key}' deleted successfully",
            }

        except Exception as e:
            logger.error(
                f"Failed to delete file: {e}",
                extra={"file_key": file_key},
            )
            raise

    def get_file_info(self, file_key: str) -> dict:
        """Get metadata of a file.

        Args:
            file_key: Full S3 key of the file

        Returns:
            Dictionary with file metadata

        Raises:
            ValidationError: If file key is invalid
        """
        # Validate file key
        if not file_key or not isinstance(file_key, str):
            raise ValidationError("File key must be a non-empty string")

        if ".." in file_key:
            raise ValidationError("File key cannot contain '..'")

        try:
            info = self.repository.get_file_info(file_key)

            logger.info(
                "File info retrieved successfully",
                extra={"file_key": file_key},
            )

            return {"status": "success", "file_info": info}

        except Exception as e:
            logger.error(
                f"Failed to get file info: {e}",
                extra={"file_key": file_key},
            )
            raise
