"""SQLite database connection helpers.

All three databases (halfs, royka, cybers) use SQLite.  Each gets its own
file inside the ``data/`` directory (configurable).  On a VDS with
PostgreSQL you would swap these out for SQLAlchemy sessions – the service
layer is designed to work with both.
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from backend.app.config import get_settings


def _db_path(name: str) -> str:
    """Return the full path for a named SQLite database."""
    settings = get_settings()
    return str(settings.data_dir / name)


@contextmanager
def get_halfs_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(_db_path("halfs.db"))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_royka_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(_db_path("royka.db"))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cybers_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(_db_path("cyber_bases.db"))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
