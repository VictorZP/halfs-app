"""Business logic for the Ройка (Royka) section.

Extracted from the desktop application's RoykaDatabase class and
RoykaPage analysis methods.  Works with both SQLite and PostgreSQL.
"""

from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.app.database.connection import get_royka_connection
from db_connection import is_postgres, adapt_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows_to_dicts(cursor) -> List[dict]:
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _get_royka_df(tournament: Optional[str] = None) -> pd.DataFrame:
    with get_royka_connection() as conn:
        raw = conn._conn if hasattr(conn, '_conn') else conn
        if tournament:
            sql = "SELECT * FROM matches WHERE tournament = ?"
            if is_postgres():
                sql = adapt_sql(sql)
            return pd.read_sql_query(sql, raw, params=(tournament,))
        return pd.read_sql_query("SELECT * FROM matches", raw)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_matches(matches_data: List[tuple]) -> int:
    if not matches_data:
        return 0
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO matches (
                date, tournament, team_home, team_away,
                t1h, t2h, tim, deviation, kickoff, predict, result
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            matches_data,
        )
        conn.commit()
    return len(matches_data)


def get_statistics() -> dict:
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM matches")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT tournament) FROM matches")
        tournaments = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT team) FROM (
                SELECT team_home AS team FROM matches
                UNION SELECT team_away AS team FROM matches
            ) sub
        """)
        teams = cur.fetchone()[0]
        cur.execute("SELECT MAX(date) FROM matches")
        last = cur.fetchone()[0]
    return {
        "total_records": total,
        "tournaments_count": tournaments,
        "teams_count": teams,
        "last_update": last,
    }


def clear_database() -> None:
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM matches")
        conn.commit()


def get_matches(tournament: Optional[str] = None, limit: int = 10000) -> List[dict]:
    query = "SELECT * FROM matches"
    params: list = []
    if tournament:
        query += " WHERE tournament = ?"
        params.append(tournament)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        return _rows_to_dicts(cur)


def get_tournaments() -> List[str]:
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT tournament FROM matches ORDER BY tournament")
        return [r[0] for r in cur.fetchall()]


def delete_matches(ids: List[int]) -> int:
    if not ids:
        return 0
    with get_royka_connection() as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def get_dataframe(tournament: Optional[str] = None) -> pd.DataFrame:
    return _get_royka_df(tournament)


# ---------------------------------------------------------------------------
# Analysis — Royka statistics
# ---------------------------------------------------------------------------

def analyze_tournament(tournament: str) -> dict:
    df = get_dataframe(tournament)
    if df.empty:
        return {"tournament": tournament, "total": 0}

    for col in ("t1h", "t2h", "tim", "deviation", "kickoff", "result"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["predict"] = pd.to_numeric(df["predict"], errors="coerce").fillna(0)

    total = len(df)
    over_count = 0
    under_count = 0
    no_bet = 0
    win = 0
    lose = 0

    for _, row in df.iterrows():
        predict = row["predict"]
        result = row["result"]
        tim = row["tim"]
        deviation = row["deviation"]

        if predict == 0:
            no_bet += 1
            continue

        diff = tim - predict
        if abs(diff) < 0.1:
            no_bet += 1
            continue

        direction = "over" if diff > 0 else "under"

        stage2 = diff + deviation
        if (stage2 > 0 and direction == "under") or (stage2 < 0 and direction == "over"):
            no_bet += 1
            continue

        if direction == "over":
            over_count += 1
        else:
            under_count += 1

        if result:
            if direction == "over" and result > predict:
                win += 1
            elif direction == "under" and result < predict:
                win += 1
            else:
                lose += 1

    return {
        "tournament": tournament,
        "total": total,
        "over": over_count,
        "under": under_count,
        "no_bet": no_bet,
        "win": win,
        "lose": lose,
        "win_rate": round(win / (win + lose) * 100, 1) if (win + lose) > 0 else 0,
    }


def get_all_tournaments_stats() -> List[dict]:
    tournaments = get_tournaments()
    return [analyze_tournament(t) for t in tournaments]
