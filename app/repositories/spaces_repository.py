"""DigitalOcean Spaces repository for file operations."""
import logging
from io import BytesIO
from typing import List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from app.core.config import settings
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class SpacesRepository:
    """Repository for interacting with DigitalOcean Spaces (S3-compatible).

    Handles all direct file and folder operations with the Spaces service.
    """

    def __init__(self):
        """Initialize S3 client for DigitalOcean Spaces."""
        try:
            self.s3_client = boto3.client(
                "s3",
                region_name=settings.spaces_region,
                endpoint_url=settings.spaces_endpoint,
                aws_access_key_id=settings.spaces_access_key,
                aws_secret_access_key=settings.spaces_secret_key,
            )
            self.bucket_name = settings.spaces_bucket_name
            logger.info(
                "S3 client initialized",
                extra={"bucket": self.bucket_name, "endpoint": settings.spaces_endpoint},
            )
        except BotoCoreError as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise DatabaseError("Failed to initialize storage service") from e

    def create_folder(self, folder_path: str) -> bool:
        """Create a folder in Spaces.

        In S3, folders don't exist as objects, but we create an empty marker object
        to represent the folder structure.

        Args:
            folder_path: Path to the folder (e.g., 'datasets/project1')

        Returns:
            True if folder creation was successful

        Raises:
            DatabaseError: If folder creation fails
        """
        try:
            # Ensure folder path ends with /
            if not folder_path.endswith("/"):
                folder_path += "/"

            # Create empty object to represent folder
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=folder_path, Body=b""
            )

            logger.info(
                "Folder created in Spaces",
                extra={"folder_path": folder_path, "bucket": self.bucket_name},
            )
            return True

        except ClientError as e:
            logger.error(
                f"Failed to create folder: {e}",
                extra={"folder_path": folder_path, "error_code": e.response["Error"]["Code"]},
            )
            raise DatabaseError(f"Failed to create folder '{folder_path}'") from e
        except Exception as e:
            logger.error(f"Unexpected error creating folder: {e}")
            raise DatabaseError("Unexpected error creating folder") from e

    def upload_file(
        self, file_path: str, file_content: bytes, folder_path: Optional[str] = None
    ) -> dict:
        """Upload a single file to Spaces.

        Args:
            file_path: Name of the file to upload
            file_content: File content as bytes
            folder_path: Optional folder path (e.g., 'datasets/project1')

        Returns:
            Dictionary with file metadata including url and key

        Raises:
            DatabaseError: If file upload fails
        """
        try:
            # Construct full S3 key
            if folder_path:
                if not folder_path.endswith("/"):
                    folder_path += "/"
                s3_key = f"{folder_path}{file_path}"
            else:
                s3_key = file_path

            # Upload file
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
            )

            # Get file URL
            file_url = f"{settings.spaces_endpoint}/{self.bucket_name}/{s3_key}"

            logger.info(
                "File uploaded to Spaces",
                extra={
                    "file_path": file_path,
                    "s3_key": s3_key,
                    "folder": folder_path,
                    "bucket": self.bucket_name,
                },
            )

            return {
                "file_name": file_path,
                "s3_key": s3_key,
                "url": file_url,
                "bucket": self.bucket_name,
            }

        except ClientError as e:
            logger.error(
                f"Failed to upload file: {e}",
                extra={
                    "file_path": file_path,
                    "folder": folder_path,
                    "error_code": e.response["Error"]["Code"],
                },
            )
            raise DatabaseError(f"Failed to upload file '{file_path}'") from e
        except Exception as e:
            logger.error(f"Unexpected error uploading file: {e}")
            raise DatabaseError("Unexpected error uploading file") from e

    def upload_multiple_files(
        self, files: List[tuple], folder_path: Optional[str] = None
    ) -> List[dict]:
        """Upload multiple files to Spaces.

        Args:
            files: List of tuples (filename, file_content)
            folder_path: Optional folder path

        Returns:
            List of dictionaries with file metadata

        Raises:
            DatabaseError: If any file upload fails
        """
        uploaded_files = []
        failed_uploads = []

        for file_name, file_content in files:
            try:
                result = self.upload_file(file_name, file_content, folder_path)
                uploaded_files.append(result)
            except DatabaseError as e:
                failed_uploads.append({"file": file_name, "error": str(e)})
                logger.warning(
                    f"Failed to upload file: {file_name}",
                    extra={"error": str(e)},
                )

        if failed_uploads and len(failed_uploads) == len(files):
            raise DatabaseError("All file uploads failed")

        logger.info(
            "Batch file upload completed",
            extra={
                "total": len(files),
                "successful": len(uploaded_files),
                "failed": len(failed_uploads),
            },
        )

        return uploaded_files

    def list_folders(self, prefix: str = "") -> List[str]:
        """List all visible folders in Spaces.

        Args:
            prefix: Optional prefix to filter folders

        Returns:
            List of folder paths

        Raises:
            DatabaseError: If listing fails
        """
        try:
            folders = set()

            # List all objects with prefix
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter="/",
            )

            # Extract common prefixes (folders)
            for page in pages:
                if "CommonPrefixes" in page:
                    for prefix_info in page["CommonPrefixes"]:
                        folder = prefix_info["Prefix"]
                        folders.add(folder)

            logger.info(
                "Folders listed from Spaces",
                extra={
                    "prefix": prefix,
                    "folder_count": len(folders),
                    "bucket": self.bucket_name,
                },
            )

            return sorted(list(folders))

        except ClientError as e:
            logger.error(
                f"Failed to list folders: {e}",
                extra={"prefix": prefix, "error_code": e.response["Error"]["Code"]},
            )
            raise DatabaseError("Failed to list folders") from e
        except Exception as e:
            logger.error(f"Unexpected error listing folders: {e}")
            raise DatabaseError("Unexpected error listing folders") from e

    def list_folder_contents(self, folder_path: str) -> dict:
        """List all contents (files and subfolders) of a folder.

        Args:
            folder_path: Path to the folder

        Returns:
            Dictionary with folders and files lists

        Raises:
            DatabaseError: If listing fails
        """
        try:
            # Ensure folder path ends with /
            if not folder_path.endswith("/"):
                folder_path += "/"

            files = []
            folders = set()

            # List all objects in the folder
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=folder_path,
                Delimiter="/",
            )

            # Extract files and subfolders
            for page in pages:
                # Get subfolders
                if "CommonPrefixes" in page:
                    for prefix_info in page["CommonPrefixes"]:
                        subfolder = prefix_info["Prefix"]
                        folders.add(subfolder)

                # Get files
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        # Skip the folder marker itself
                        if key != folder_path:
                            files.append(
                                {
                                    "name": key.split("/")[-1],
                                    "key": key,
                                    "size": obj["Size"],
                                    "last_modified": obj["LastModified"].isoformat(),
                                    "url": f"{settings.spaces_endpoint}/{self.bucket_name}/{key}",
                                }
                            )

            logger.info(
                "Folder contents listed from Spaces",
                extra={
                    "folder_path": folder_path,
                    "file_count": len(files),
                    "subfolder_count": len(folders),
                    "bucket": self.bucket_name,
                },
            )

            return {
                "folder_path": folder_path,
                "files": files,
                "subfolders": sorted(list(folders)),
                "total_items": len(files) + len(folders),
            }

        except ClientError as e:
            logger.error(
                f"Failed to list folder contents: {e}",
                extra={"folder_path": folder_path, "error_code": e.response["Error"]["Code"]},
            )
            raise DatabaseError(f"Failed to list contents of '{folder_path}'") from e
        except Exception as e:
            logger.error(f"Unexpected error listing folder contents: {e}")
            raise DatabaseError("Unexpected error listing folder contents") from e

    def delete_file(self, file_key: str) -> bool:
        """Delete a file from Spaces.

        Args:
            file_key: Full S3 key of the file

        Returns:
            True if deletion was successful

        Raises:
            DatabaseError: If deletion fails
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)

            logger.info(
                "File deleted from Spaces",
                extra={"file_key": file_key, "bucket": self.bucket_name},
            )
            return True

        except ClientError as e:
            logger.error(
                f"Failed to delete file: {e}",
                extra={"file_key": file_key, "error_code": e.response["Error"]["Code"]},
            )
            raise DatabaseError(f"Failed to delete file '{file_key}'") from e
        except Exception as e:
            logger.error(f"Unexpected error deleting file: {e}")
            raise DatabaseError("Unexpected error deleting file") from e

    def get_file_info(self, file_key: str) -> dict:
        """Get metadata of a file in Spaces.

        Args:
            file_key: Full S3 key of the file

        Returns:
            Dictionary with file metadata

        Raises:
            DatabaseError: If retrieving metadata fails
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)

            return {
                "file_key": file_key,
                "size": response["ContentLength"],
                "last_modified": response["LastModified"].isoformat(),
                "content_type": response.get("ContentType", "unknown"),
                "url": f"{settings.spaces_endpoint}/{self.bucket_name}/{file_key}",
            }

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(
                    f"File not found: {file_key}",
                    extra={"file_key": file_key},
                )
                raise DatabaseError(f"File '{file_key}' not found") from e
            logger.error(
                f"Failed to get file info: {e}",
                extra={"file_key": file_key, "error_code": e.response["Error"]["Code"]},
            )
            raise DatabaseError(f"Failed to get info for '{file_key}'") from e
        except Exception as e:
            logger.error(f"Unexpected error getting file info: {e}")
            raise DatabaseError("Unexpected error getting file info") from e
