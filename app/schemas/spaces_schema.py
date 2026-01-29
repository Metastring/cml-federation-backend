"""Pydantic schemas for DigitalOcean Spaces API."""
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class CreateFolderRequest(BaseModel):
    """Request schema for creating a folder."""

    folder_path: str = Field(..., min_length=1, description="Path to the folder")


class FileUploadRequest(BaseModel):
    """Request schema for uploading a single file."""

    file_name: str = Field(..., min_length=1, description="Name of the file")
    folder_path: Optional[str] = Field(
        None, description="Optional folder path for the file"
    )


class MultipleFilesUploadRequest(BaseModel):
    """Request schema for uploading multiple files."""

    files: List[dict] = Field(
        ..., description="List of files with 'name' and 'content' (base64 encoded)"
    )
    folder_path: Optional[str] = Field(
        None, description="Optional folder path for the files"
    )

    @field_validator("files")
    @classmethod
    def validate_files(cls, v: List[dict]) -> List[dict]:
        """Validate files list."""
        if not v or len(v) == 0:
            raise ValueError("At least one file must be provided")
        for file_item in v:
            if "name" not in file_item:
                raise ValueError("Each file must have a 'name' key")
            if "content" not in file_item:
                raise ValueError("Each file must have a 'content' key")
        return v


class ListFoldersRequest(BaseModel):
    """Request schema for listing folders."""

    prefix: Optional[str] = Field(None, description="Optional prefix to filter folders")


class ListFolderContentsRequest(BaseModel):
    """Request schema for listing folder contents."""

    folder_path: str = Field(..., min_length=1, description="Path to the folder")


class DeleteFileRequest(BaseModel):
    """Request schema for deleting a file."""

    file_key: str = Field(..., min_length=1, description="Full S3 key of the file")


class FileInfo(BaseModel):
    """Response schema for file information."""

    name: str = Field(..., description="Name of the file")
    key: str = Field(..., description="S3 key of the file")
    size: int = Field(..., description="Size of the file in bytes")
    last_modified: str = Field(..., description="Last modified timestamp")
    url: str = Field(..., description="URL to access the file")


class FolderContents(BaseModel):
    """Response schema for folder contents."""

    status: str = Field(..., description="Status of the operation")
    folder_path: str = Field(..., description="Path to the folder")
    file_count: int = Field(..., description="Number of files in the folder")
    subfolder_count: int = Field(..., description="Number of subfolders")
    total_items: int = Field(..., description="Total number of items")
    files: List[FileInfo] = Field(..., description="List of files")
    subfolders: List[str] = Field(..., description="List of subfolders")
    message: str = Field(..., description="Message about the operation")


class CreateFolderResponse(BaseModel):
    """Response schema for folder creation."""

    status: str = Field(..., description="Status of the operation")
    folder_path: str = Field(..., description="Path to the created folder")
    message: str = Field(..., description="Success message")


class FileUploadResponse(BaseModel):
    """Response schema for file upload."""

    status: str = Field(..., description="Status of the operation")
    file_name: str = Field(..., description="Name of the file")
    folder_path: str = Field(..., description="Folder path where file is stored")
    url: str = Field(..., description="URL to access the file")
    file_size: int = Field(..., description="Size of the file in bytes")
    message: str = Field(..., description="Success message")


class MultipleFilesUploadResponse(BaseModel):
    """Response schema for multiple files upload."""

    status: str = Field(..., description="Status of the operation")
    total_files: int = Field(..., description="Total files in request")
    uploaded_count: int = Field(..., description="Number of successfully uploaded files")
    folder_path: str = Field(..., description="Folder path where files are stored")
    files: List[dict] = Field(..., description="List of uploaded file information")
    message: str = Field(..., description="Success message")


class ListFoldersResponse(BaseModel):
    """Response schema for listing folders."""

    status: str = Field(..., description="Status of the operation")
    prefix: str = Field(..., description="Prefix used for filtering")
    folder_count: int = Field(..., description="Number of folders found")
    folders: List[str] = Field(..., description="List of folder paths")
    message: str = Field(..., description="Message about the operation")


class DeleteFileResponse(BaseModel):
    """Response schema for file deletion."""

    status: str = Field(..., description="Status of the operation")
    file_key: str = Field(..., description="S3 key of the deleted file")
    message: str = Field(..., description="Success message")


class FileInfoResponse(BaseModel):
    """Response schema for file information."""

    status: str = Field(..., description="Status of the operation")
    file_info: dict = Field(..., description="File metadata")


class ErrorResponse(BaseModel):
    """Error response schema."""

    status: str = Field(default="error", description="Status of the operation")
    error: str = Field(..., description="Error message")
    details: Optional[str] = Field(None, description="Additional error details")
