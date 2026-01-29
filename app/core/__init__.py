"""Core module with cross-cutting concerns."""

from app.core.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from app.core.database import get_connection

__all__ = [
    "get_connection",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
]
