"""Universal database connection module supporting both SQLite and PostgreSQL.

Set the DATABASE_URL environment variable to a PostgreSQL connection string
(e.g. ``postgresql://user:pass@host:5432/dbname``) to use PostgreSQL.
When DATABASE_URL is empty or absent the module falls back to local SQLite
files in the ``data/`` directory.

In PostgreSQL mode every logical "database" (halfs, royka, cyber) becomes a
separate **schema** inside a single PostgreSQL database.  This keeps table
names unchanged (``matches``, ``cyber_matches``, …) while avoiding conflicts.

Usage
-----
::

    from db_connection import db_connect, adapt_sql, is_postgres

    # Inside a database class:
    with db_connect(schema='halfs', sqlite_path=self.db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM matches WHERE tournament = ?", (name,))

    # pandas read_sql_query also works through the wrapper:
    with db_connect(schema='halfs', sqlite_path=self.db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM matches", conn)

The wrapper automatically translates ``?`` placeholders to ``%s``,
``AUTOINCREMENT`` to ``SERIAL``, and ``instr(…)`` to ``strpos(…)``
when running against PostgreSQL.
"""

import os
import re
import sys
import sqlite3
from contextlib import contextmanager
from typing import Optional

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(
        os.path.dirname(sys.executable) if getattr(sys, 'frozen', False)
        else os.path.dirname(os.path.abspath(__file__)),
        '.env'
    )
    if os.path.isfile(_env_path):
        load_dotenv(_env_path, override=False)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABASE_URL: str = os.getenv("DATABASE_URL", "")


def is_postgres() -> bool:
    """Return *True* when the app is configured to use PostgreSQL."""
    return DATABASE_URL.startswith(("postgresql://", "postgres://"))


# ---------------------------------------------------------------------------
# SQL translation helpers
# ---------------------------------------------------------------------------

def adapt_sql(sql: str) -> str:
    """Translate SQLite-flavoured SQL into PostgreSQL-compatible SQL.

    * ``?`` placeholders  →  ``%s``
    * ``INTEGER PRIMARY KEY AUTOINCREMENT``  →  ``SERIAL PRIMARY KEY``
    * ``instr(…)``  →  ``strpos(…)``

    The function is a no-op when the app uses SQLite.
    """
    if not is_postgres():
        return sql

    # Replace ? → %s (but not inside string literals)
    out: list[str] = []
    in_string = False
    quote_char: Optional[str] = None
    for ch in sql:
        if in_string:
            out.append(ch)
            if ch == quote_char:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote_char = ch
            out.append(ch)
        elif ch == '?':
            out.append('%s')
        else:
            out.append(ch)
    sql = ''.join(out)

    # AUTOINCREMENT → SERIAL
    sql = re.sub(
        r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT',
        'SERIAL PRIMARY KEY',
        sql,
        flags=re.IGNORECASE,
    )
    # instr(x, y) → strpos(x, y)  (same argument order, 1-based return)
    sql = re.sub(r'\binstr\s*\(', 'strpos(', sql, flags=re.IGNORECASE)

    return sql


# ---------------------------------------------------------------------------
# Cursor / connection wrappers for PostgreSQL
# ---------------------------------------------------------------------------

class _PGCursor:
    """Wraps a *psycopg2* cursor so that every ``execute`` call
    automatically translates SQLite SQL to PostgreSQL SQL."""

    def __init__(self, real_cursor):
        self._cur = real_cursor

    # --- execute family ---------------------------------------------------

    def execute(self, sql, params=None):
        sql = adapt_sql(sql)
        if params is not None:
            return self._cur.execute(sql, params)
        return self._cur.execute(sql)

    def executemany(self, sql, params_list):
        sql = adapt_sql(sql)
        return self._cur.executemany(sql, params_list)

    # --- delegate everything else -----------------------------------------

    def __getattr__(self, name):
        return getattr(self._cur, name)

    def __iter__(self):
        return iter(self._cur)


class _PGConn:
    """Wraps a *psycopg2* connection so that ``cursor()`` returns an
    auto-translating :class:`_PGCursor`.  The wrapper also exposes all
    other attributes of the real connection so that *pandas*
    ``read_sql_query`` can use it transparently."""

    def __init__(self, real_conn):
        self._conn = real_conn
        self.row_factory = None  # accept sqlite3.Row assignment silently

    def cursor(self):
        return _PGCursor(self._conn.cursor())

    def execute(self, sql, params=None):
        """Allow ``conn.execute(...)`` pattern (used by some code)."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    # pandas calls conn.cursor() internally — handled above.
    # For any other attribute (encoding, notices, …) fall through:
    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()


# ---------------------------------------------------------------------------
# Public connection context manager
# ---------------------------------------------------------------------------

# Map logical schema → SQLite file name
_SQLITE_FILE_MAP = {
    'halfs': 'halfs.db',
    'royka': 'royka.db',
    'cyber': 'cyber_bases.db',
}


def _default_data_dir() -> str:
    """Return the ``data/`` directory next to the exe / script."""
    if getattr(sys, 'frozen', False):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, "data")
    os.makedirs(d, exist_ok=True)
    return d


@contextmanager
def db_connect(schema: str = 'halfs', sqlite_path: Optional[str] = None):
    """Open a database connection (PostgreSQL **or** SQLite).

    Parameters
    ----------
    schema : str
        Logical schema name (``'halfs'``, ``'royka'``, ``'cyber'``).
        In PostgreSQL mode this becomes an actual ``CREATE SCHEMA``.
    sqlite_path : str | None
        Explicit path to a ``.db`` file.  Used only in SQLite mode.
        If *None*, the path is derived from *schema* automatically.

    Yields
    ------
    connection
        A connection object whose ``cursor().execute()`` auto-translates
        SQL when targeting PostgreSQL.
    """
    if is_postgres():
        import psycopg2  # type: ignore

        raw = psycopg2.connect(DATABASE_URL)
        try:
            with raw.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    (schema,),
                )
                if not cur.fetchone():
                    cur.execute(f'CREATE SCHEMA "{schema}"')
                cur.execute(f'SET search_path TO "{schema}", public')
            raw.commit()
            wrapper = _PGConn(raw)
            yield wrapper
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.close()
    else:
        if sqlite_path is None:
            fname = _SQLITE_FILE_MAP.get(schema, f'{schema}.db')
            sqlite_path = os.path.join(_default_data_dir(), fname)
        conn = sqlite3.connect(sqlite_path)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
