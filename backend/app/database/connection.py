"""Database connection helpers.

All databases (halfs, royka, cyber) use SQLite locally or PostgreSQL on Railway.
The ``db_connect`` context manager from ``db_connection`` handles the switch
based on the ``DATABASE_URL`` environment variable.
"""

import os
import sys
from contextlib import contextmanager
from typing import Generator

# Allow importing the top-level db_connection module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from db_connection import db_connect, is_postgres

from backend.app.config import get_settings


def _sqlite_path(name: str) -> str:
    """Return the full path for a named SQLite database."""
    settings = get_settings()
    return str(settings.data_dir / name)


@contextmanager
def get_halfs_connection() -> Generator:
    with db_connect(schema='halfs', sqlite_path=_sqlite_path("halfs.db")) as conn:
        yield conn


@contextmanager
def get_royka_connection() -> Generator:
    with db_connect(schema='royka', sqlite_path=_sqlite_path("royka.db")) as conn:
        yield conn


@contextmanager
def get_cyber_connection() -> Generator:
    with db_connect(schema='cyber', sqlite_path=_sqlite_path("cyber_bases.db")) as conn:
        yield conn
