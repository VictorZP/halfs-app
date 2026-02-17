"""Database initialisation for all databases.

Call ``init_all_databases()`` once at application startup to ensure that
all required tables and indices exist.
"""

from backend.app.database.connection import get_halfs_connection, get_royka_connection


def init_halfs_db() -> None:
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_tournament ON matches(tournament)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_team_home ON matches(team_home)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_team_away ON matches(team_away)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_date ON matches(date)")
        conn.commit()


def init_royka_db() -> None:
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                tournament TEXT NOT NULL,
                team_home TEXT NOT NULL,
                team_away TEXT NOT NULL,
                t1h REAL, t2h REAL,
                tim REAL NOT NULL,
                deviation REAL, kickoff REAL,
                predict TEXT NOT NULL,
                result REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_royka_tournament ON matches(tournament)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_royka_date ON matches(date)")
        conn.commit()


def init_all_databases() -> None:
    """Create all tables in all databases if they don't exist yet."""
    init_halfs_db()
    init_royka_db()
