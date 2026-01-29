"""Database connection and session management."""
import logging
from typing import Generator, Optional

import psycopg2
from psycopg2 import OperationalError

from app.core.config import settings
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection management with error handling.

    Manages PostgreSQL connections with proper resource cleanup
    and structured error logging.
    """

    def __init__(self) -> None:
        """Initialize database connection manager."""
        self.connection: Optional[psycopg2.extensions.connection] = None

    def get_connection(self) -> psycopg2.extensions.connection:
        """Get a new database connection.

        Returns:
            PostgreSQL database connection.

        Raises:
            DatabaseError: If connection fails.

        Example:
            >>> db = DatabaseConnection()
            >>> conn = db.get_connection()
            >>> cursor = conn.cursor()
        """
        try:
            db_config = {
                "dbname": settings.db_name,
                "user": settings.db_user,
                "password": settings.db_password,
                "host": settings.db_host,
                "port": settings.db_port,
            }

            connection = psycopg2.connect(**db_config)
            logger.debug(
                "Database connection established",
                extra={"host": settings.db_host, "database": settings.db_name},
            )
            return connection

        except OperationalError as e:
            logger.error(
                "Database connection failed",
                extra={"host": settings.db_host, "error": str(e)},
            )
            raise DatabaseError("Failed to connect to database", "connect") from e
        except Exception as e:
            logger.error(f"Unexpected database error: {e}")
            raise DatabaseError(f"Unexpected error: {e}", "connect") from e

    def close_connection(self, connection: psycopg2.extensions.connection) -> None:
        """Close database connection.

        Args:
            connection: Connection to close.

        Example:
            >>> db = DatabaseConnection()
            >>> conn = db.get_connection()
            >>> db.close_connection(conn)
        """
        try:
            if connection:
                connection.close()
                logger.debug("Database connection closed")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")


# Global database instance
_db_instance = DatabaseConnection()


def get_connection() -> psycopg2.extensions.connection:
    """Get a new database connection.

    Returns:
        PostgreSQL database connection.

    Raises:
        DatabaseError: If connection fails.
    """
    return _db_instance.get_connection()


def close_connection(connection: psycopg2.extensions.connection) -> None:
    """Close database connection.

    Args:
        connection: Connection to close.
    """
    _db_instance.close_connection(connection)


def get_db() -> Generator[psycopg2.extensions.connection, None, None]:
    """Dependency injection for database connections.

    Yields:
        PostgreSQL database connection.

    Raises:
        DatabaseError: If connection fails.

    Example:
        >>> from fastapi import Depends
        >>> @router.get("/data")
        >>> def endpoint(db = Depends(get_db)):
        >>>     cursor = db.cursor()
    """
    connection = get_connection()
    try:
        yield connection
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        raise
    finally:
        close_connection(connection)
