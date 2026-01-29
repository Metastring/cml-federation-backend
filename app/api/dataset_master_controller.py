"""Dataset master management API controller."""
import logging
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.database import get_connection
from app.core.exceptions import DatabaseError, ValidationError

logger = logging.getLogger(__name__)

router = APIRouter()


class DatasetMasterInput(BaseModel):
    """Request model for dataset master creation.

    Attributes:
        title: Dataset title (required).
        description: Detailed description of the dataset.
        citation: Citation information for the dataset.
        doi: Digital Object Identifier.
        language: Language of dataset documentation.
        data_language: Language of actual data content.
        license: License type (e.g., CC-BY, CC0).
        publication_date: When dataset was published.
        metadata_modified_date: Last metadata modification date.
        registration_date: When dataset was registered.
        is_active: Whether dataset is currently active.
        keywords: Comma-separated keywords for discovery.
        dataset_type: Classification of dataset type.
        category_id: Reference to parent category.
    """

    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    language: str = Field("en", min_length=2, max_length=5)
    data_language: str = Field("en", min_length=2, max_length=5)
    license: str = Field("CC-BY")
    publication_date: Optional[date] = None
    metadata_modified_date: Optional[date] = None
    registration_date: Optional[date] = None
    is_active: bool = True
    keywords: Optional[str] = None
    dataset_type: Optional[str] = None
    category_id: str = Field(...)


class DatasetMasterResponse(BaseModel):
    """Response model for dataset creation.

    Attributes:
        status: Operation status (success/error).
        dataset_id: ID of created dataset if successful.
        error: Error message if operation failed.
    """

    status: str
    dataset_id: Optional[str] = None
    error: Optional[str] = None


@router.post("/dataset-master", response_model=DatasetMasterResponse, status_code=201)
def create_dataset_master(dataset: DatasetMasterInput) -> DatasetMasterResponse:
    """Create a new dataset master record.

    Inserts a new dataset into the dataset_master table with all provided metadata.

    Args:
        dataset: Dataset information to create.

    Returns:
        Response containing dataset_id if successful, or error details if failed.

    Raises:
        ValidationError: If dataset data is invalid.
        DatabaseError: If database insertion fails.

    Example:
        >>> data = DatasetMasterInput(
        ...     title="Biodiversity Survey 2024",
        ...     category_id="1",
        ...     license="CC-BY"
        ... )
        >>> response = create_dataset_master(data)
        >>> print(response.dataset_id)
        'dataset_123'
    """
    try:
        # Validate required fields
        _validate_dataset_input(dataset)

        conn = get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO dataset_master (
                    title, description, citation, doi, language,
                    data_language, license, publication_date,
                    metadata_modified_date, registration_date, is_active,
                    keywords, dataset_type, category_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING dataset_id;
            """,
                (
                    dataset.title,
                    dataset.description,
                    dataset.citation,
                    dataset.doi,
                    dataset.language,
                    dataset.data_language,
                    dataset.license,
                    dataset.publication_date,
                    dataset.metadata_modified_date,
                    dataset.registration_date,
                    dataset.is_active,
                    dataset.keywords,
                    dataset.dataset_type,
                    dataset.category_id,
                ),
            )

            dataset_id = cursor.fetchone()[0]
            conn.commit()

            logger.info(
                "Dataset created successfully",
                extra={"dataset_id": dataset_id, "title": dataset.title},
            )

            return DatasetMasterResponse(status="success", dataset_id=dataset_id)

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error during dataset creation: {e}")
            raise DatabaseError("Failed to create dataset", "insert") from e

        finally:
            cursor.close()
            conn.close()

    except ValidationError:
        raise
    except DatabaseError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during dataset creation: {e}")
        return DatasetMasterResponse(status="error", error=str(e))


def _validate_dataset_input(dataset: DatasetMasterInput) -> None:
    """Validate dataset input data.

    Args:
        dataset: Dataset to validate.

    Raises:
        ValidationError: If validation fails.
    """
    if not dataset.title or not dataset.title.strip():
        raise ValidationError("Dataset title is required and cannot be empty")

    if not dataset.category_id:
        raise ValidationError("Category ID is required")

    logger.debug(f"Dataset input validation passed for: {dataset.title}")




