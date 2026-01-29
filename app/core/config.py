"""Application configuration management."""
import logging
import os
from typing import List

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application configuration from environment variables.

    All configuration is loaded from environment variables with .env file support.
    Uses pydantic BaseSettings for validation and type safety.
    """

    # Database configuration
    db_name: str = os.getenv("DB_NAME", "biodiversity")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5432")

    # CORS configuration
    cors_origins: List[str] = ["*"]

    # Application configuration
    app_name: str = "Central Server"
    app_version: str = "1.0.0"
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"

    # DigitalOcean Spaces configuration
    spaces_access_key: str = os.getenv("DO_SPACES_ACCESS_KEY", "")
    spaces_secret_key: str = os.getenv("DO_SPACES_SECRET_KEY", "")
    spaces_endpoint: str = os.getenv(
        "DO_SPACES_ENDPOINT", "https://nyc3.digitaloceanspaces.com"
    )
    spaces_region: str = os.getenv("DO_SPACES_REGION", "nyc3")
    spaces_bucket_name: str = os.getenv("DO_SPACES_BUCKET_NAME", "cmldataset")

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        case_sensitive = False

    @property
    def database_url(self) -> str:
        """Get formatted database URL.

        Returns:
            PostgreSQL connection string.
        """
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


# Global settings instance
settings = Settings()

# Legacy constants for backward compatibility (DEPRECATED - use settings object)
DB_NAME = settings.db_name
DB_USER = settings.db_user
DB_PASSWORD = settings.db_password
DB_HOST = settings.db_host
DB_PORT = settings.db_port
CORS_ORIGINS = settings.cors_origins
APP_NAME = settings.app_name
APP_VERSION = settings.app_version

logger.info(
    "Configuration loaded",
    extra={"app_name": APP_NAME, "app_version": APP_VERSION, "debug": settings.debug},
)
