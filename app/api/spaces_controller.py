"""FastAPI controller for DigitalOcean Spaces operations."""
import base64
import logging
from typing import List

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status

from app.core.exceptions import ValidationError
from app.schemas import spaces_schema
from app.services.spaces_service import SpacesService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/spaces", tags=["DigitalOcean Spaces"])

# Initialize service
spaces_service = SpacesService()


@router.post(
    "/folders/create",
    response_model=spaces_schema.CreateFolderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a folder",
    description="Create a new folder in DigitalOcean Spaces",
)
def create_folder(request: spaces_schema.CreateFolderRequest) -> dict:
    """Create a new folder in DigitalOcean Spaces.

    Args:
        request: Folder creation request with folder_path

    Returns:
        CreateFolderResponse with status and folder information

    Raises:
        HTTPException: If folder creation fails
    """
    try:
        result = spaces_service.create_folder(request.folder_path)
        return result
    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error creating folder: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create folder",
        ) from e


@router.post(
    "/files/upload",
    response_model=spaces_schema.FileUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a single file",
    description="Upload a single file to DigitalOcean Spaces",
)
async def upload_file(
    file: UploadFile = File(...),
    folder_path: str = Form(None),
) -> dict:
    """Upload a single file to DigitalOcean Spaces.

    Args:
        file: File to upload
        folder_path: Optional folder path where file should be stored

    Returns:
        FileUploadResponse with file information and URL

    Raises:
        HTTPException: If file upload fails
    """
    try:
        # Read file content
        content = await file.read()

        if not content:
            raise ValidationError("File content is empty")

        # Upload file
        result = spaces_service.upload_file(
            file_name=file.filename,
            file_content=content,
            folder_path=folder_path,
        )

        logger.info(
            f"File uploaded: {file.filename}",
            extra={"size": len(content), "folder": folder_path},
        )

        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload file",
        ) from e


@router.post(
    "/files/upload-multiple",
    response_model=spaces_schema.MultipleFilesUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload multiple files",
    description="Upload multiple files to DigitalOcean Spaces",
)
async def upload_multiple_files(
    files: List[UploadFile] = File(...),
    folder_path: str = Form(None),
) -> dict:
    """Upload multiple files to DigitalOcean Spaces.

    Args:
        files: List of files to upload
        folder_path: Optional folder path where files should be stored

    Returns:
        MultipleFilesUploadResponse with upload results

    Raises:
        HTTPException: If file uploads fail
    """
    try:
        if not files:
            raise ValidationError("At least one file must be provided")

        # Read all files
        file_list = []
        for file in files:
            content = await file.read()
            if not content:
                logger.warning(f"Skipping empty file: {file.filename}")
                continue
            file_list.append({"name": file.filename, "content": content})

        if not file_list:
            raise ValidationError("No valid files provided")

        # Upload multiple files
        result = spaces_service.upload_multiple_files(
            files=file_list,
            folder_path=folder_path,
        )

        logger.info(
            f"Multiple files uploaded",
            extra={"count": len(file_list), "folder": folder_path},
        )

        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error uploading multiple files: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload files",
        ) from e


@router.get(
    "/folders/list",
    response_model=spaces_schema.ListFoldersResponse,
    summary="List all folders",
    description="List all visible folders in DigitalOcean Spaces",
)
def list_folders(prefix: str = None) -> dict:
    """List all visible folders in DigitalOcean Spaces.

    Args:
        prefix: Optional prefix to filter folders

    Returns:
        ListFoldersResponse with list of folders

    Raises:
        HTTPException: If listing fails
    """
    try:
        result = spaces_service.list_folders(prefix=prefix or "")
        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error listing folders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list folders",
        ) from e


@router.get(
    "/folders/{folder_path}/contents",
    response_model=spaces_schema.FolderContents,
    summary="List folder contents",
    description="List all files and subfolders in a specific folder",
)
def list_folder_contents(folder_path: str) -> dict:
    """List all contents of a folder in DigitalOcean Spaces.

    Args:
        folder_path: Path to the folder

    Returns:
        FolderContents with files and subfolders

    Raises:
        HTTPException: If listing fails
    """
    try:
        result = spaces_service.list_folder_contents(folder_path)
        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error listing folder contents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list folder contents",
        ) from e


@router.delete(
    "/files/delete",
    response_model=spaces_schema.DeleteFileResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete a file",
    description="Delete a file from DigitalOcean Spaces",
)
def delete_file(request: spaces_schema.DeleteFileRequest) -> dict:
    """Delete a file from DigitalOcean Spaces.

    Args:
        request: Delete request with file_key

    Returns:
        DeleteFileResponse with deletion status

    Raises:
        HTTPException: If deletion fails
    """
    try:
        result = spaces_service.delete_file(request.file_key)
        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete file",
        ) from e


@router.get(
    "/files/info",
    response_model=spaces_schema.FileInfoResponse,
    summary="Get file information",
    description="Get metadata about a file in DigitalOcean Spaces",
)
def get_file_info(file_key: str) -> dict:
    """Get metadata of a file in DigitalOcean Spaces.

    Args:
        file_key: Full S3 key of the file

    Returns:
        FileInfoResponse with file metadata

    Raises:
        HTTPException: If retrieving metadata fails
    """
    try:
        result = spaces_service.get_file_info(file_key)
        return result

    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Error getting file info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get file information",
        ) from e


@router.get(
    "/health",
    summary="Check Spaces connection",
    description="Health check endpoint for DigitalOcean Spaces connectivity",
)
def spaces_health() -> dict:
    """Health check for DigitalOcean Spaces connection.

    Returns:
        Dictionary with health status

    Raises:
        HTTPException: If connection fails
    """
    try:
        # Try to list folders as a connectivity check
        spaces_service.list_folders()
        return {
            "status": "healthy",
            "message": "DigitalOcean Spaces connection is active",
        }

    except Exception as e:
        logger.error(f"Spaces health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DigitalOcean Spaces is not accessible",
        ) from e
