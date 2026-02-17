"""Migrate existing SQLite databases to PostgreSQL.

Usage:
    set DATABASE_URL=postgresql://user:pass@host:5432/dbname
    python migrate_to_postgres.py

The script reads all rows from the local SQLite files and inserts them
into the PostgreSQL database.  Existing data in PostgreSQL is **not**
deleted — the script only appends.  Run it once after setting up the
PostgreSQL instance.
"""

import os
import sys
import sqlite3

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_connection import DATABASE_URL, is_postgres, db_connect


def _get_data_dir() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "data")


def _migrate_table(
    sqlite_path: str,
    table_name: str,
    schema: str,
) -> int:
    """Copy all rows from a SQLite table into PostgreSQL.

    Returns the number of rows migrated.
    """
    if not os.path.exists(sqlite_path):
        print(f"  [skip] {sqlite_path} not found")
        return 0

    src = sqlite3.connect(sqlite_path)
    src_cur = src.cursor()

    # Read column names
    src_cur.execute(f"PRAGMA table_info({table_name})")
    columns_info = src_cur.fetchall()
    if not columns_info:
        print(f"  [skip] table '{table_name}' not found in {sqlite_path}")
        src.close()
        return 0

    # Exclude 'id' column (auto-generated in PostgreSQL)
    columns = [c[1] for c in columns_info if c[1].lower() != 'id']
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))

    # Read all rows
    src_cur.execute(f"SELECT {cols_str} FROM {table_name}")
    rows = src_cur.fetchall()
    src.close()

    if not rows:
        print(f"  [skip] {table_name} is empty in {sqlite_path}")
        return 0

    # Insert into PostgreSQL
    pg_placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f'INSERT INTO {table_name} ({cols_str}) VALUES ({pg_placeholders})'

    with db_connect(schema=schema) as conn:
        # conn is a _PGConn wrapper — get raw psycopg2 connection for executemany
        raw = conn._conn if hasattr(conn, '_conn') else conn
        cur = raw.cursor()
        cur.executemany(insert_sql, rows)
        raw.commit()

    return len(rows)


def main():
    if not is_postgres():
        print("ERROR: DATABASE_URL is not set or does not point to PostgreSQL.")
        print(f"  Current DATABASE_URL = '{DATABASE_URL}'")
        print()
        print("Set it, for example:")
        print("  set DATABASE_URL=postgresql://user:pass@host:5432/dbname")
        sys.exit(1)

    data_dir = _get_data_dir()
    print(f"Data directory: {data_dir}")
    print(f"PostgreSQL URL: {DATABASE_URL[:40]}...")
    print()

    # --- 1. halfs ---
    print("[halfs] Migrating halfs.db → PostgreSQL schema 'halfs'")
    # Ensure target table exists
    with db_connect(schema='halfs') as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                date TEXT NOT NULL,
                tournament TEXT NOT NULL,
                team_home TEXT NOT NULL,
                team_away TEXT NOT NULL,
                q1_home INTEGER, q1_away INTEGER,
                q2_home INTEGER, q2_away INTEGER,
                q3_home INTEGER, q3_away INTEGER,
                q4_home INTEGER, q4_away INTEGER,
                ot_home INTEGER, ot_away INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    count = _migrate_table(
        os.path.join(data_dir, "halfs.db"), "matches", "halfs",
    )
    print(f"  → {count} rows migrated")

    # --- 2. royka ---
    print("[royka] Migrating royka.db → PostgreSQL schema 'royka'")
    with db_connect(schema='royka') as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                date TEXT NOT NULL,
                tournament TEXT NOT NULL,
                team_home TEXT NOT NULL,
                team_away TEXT NOT NULL,
                t1h REAL,
                t2h REAL,
                tim REAL NOT NULL,
                deviation REAL,
                kickoff REAL,
                predict TEXT NOT NULL,
                result REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    count = _migrate_table(
        os.path.join(data_dir, "royka.db"), "matches", "royka",
    )
    print(f"  → {count} rows migrated")

    # --- 3. cyber ---
    print("[cyber] Migrating cyber_bases.db → PostgreSQL schema 'cyber'")
    with db_connect(schema='cyber') as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cyber_matches (
                id SERIAL PRIMARY KEY,
                date TEXT,
                tournament TEXT,
                team TEXT,
                home_away TEXT,
                two_pt_made REAL,
                two_pt_attempt REAL,
                three_pt_made REAL,
                three_pt_attempt REAL,
                fta_made REAL,
                fta_attempt REAL,
                off_rebound REAL,
                turnovers REAL,
                controls REAL,
                points REAL,
                opponent TEXT,
                attak_kef REAL,
                status TEXT
            )
        """)
        conn.commit()

    count = _migrate_table(
        os.path.join(data_dir, "cyber_bases.db"), "cyber_matches", "cyber",
    )
    print(f"  → {count} rows migrated")

    # --- 4. cyber_live_matches ---
    print("[cyber] Migrating cyber_bases.db live_matches → PostgreSQL schema 'cyber'")
    with db_connect(schema='cyber') as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS live_matches (
                id SERIAL PRIMARY KEY,
                tournament TEXT,
                team TEXT,
                opponent TEXT,
                predict REAL,
                attak_kef REAL
            )
        """)
        conn.commit()

    count = _migrate_table(
        os.path.join(data_dir, "cyber_bases.db"), "live_matches", "cyber",
    )
    print(f"  → {count} rows migrated")

    print()
    print("Migration complete!")


if __name__ == "__main__":
    main()
