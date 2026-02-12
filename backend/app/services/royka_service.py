"""Business logic for the Ройка (Royka) section.

Extracted from the desktop application's RoykaDatabase class and
RoykaPage analysis methods.
"""

import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.app.config import get_settings


def _db_path() -> str:
    return str(get_settings().data_dir / "royka.db")


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_matches(matches_data: List[tuple]) -> int:
    """Add matches as tuples: (date, tournament, team_home, team_away,
    t1h, t2h, tim, deviation, kickoff, predict, result)."""
    if not matches_data:
        return 0
    with sqlite3.connect(_db_path()) as conn:
        conn.executemany(
            """INSERT INTO matches (
                date, tournament, team_home, team_away,
                t1h, t2h, tim, deviation, kickoff, predict, result
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            matches_data,
        )
        conn.commit()
    return len(matches_data)


def get_statistics() -> dict:
    with sqlite3.connect(_db_path()) as conn:
        total = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        tournaments = conn.execute("SELECT COUNT(DISTINCT tournament) FROM matches").fetchone()[0]
        teams = conn.execute("""
            SELECT COUNT(DISTINCT team) FROM (
                SELECT team_home AS team FROM matches
                UNION SELECT team_away AS team FROM matches
            )
        """).fetchone()[0]
        last = conn.execute("SELECT MAX(date) FROM matches").fetchone()[0]
    return {
        "total_records": total,
        "tournaments_count": tournaments,
        "teams_count": teams,
        "last_update": last,
    }


def clear_database() -> None:
    with sqlite3.connect(_db_path()) as conn:
        conn.execute("DELETE FROM matches")
        conn.commit()


def get_matches(tournament: Optional[str] = None, limit: int = 10000) -> List[dict]:
    query = "SELECT * FROM matches"
    params = []
    if tournament:
        query += " WHERE tournament = ?"
        params.append(tournament)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with sqlite3.connect(_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_tournaments() -> List[str]:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.execute("SELECT DISTINCT tournament FROM matches ORDER BY tournament")
        return [r[0] for r in cur.fetchall()]


def delete_matches(ids: List[int]) -> int:
    if not ids:
        return 0
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def get_dataframe(tournament: Optional[str] = None) -> pd.DataFrame:
    with sqlite3.connect(_db_path()) as conn:
        if tournament:
            return pd.read_sql_query(
                "SELECT * FROM matches WHERE tournament = ?", conn, params=(tournament,)
            )
        return pd.read_sql_query("SELECT * FROM matches", conn)


# ---------------------------------------------------------------------------
# Analysis — Royka statistics
# ---------------------------------------------------------------------------

def analyze_tournament(tournament: str) -> dict:
    """Compute Royka statistics for a tournament.

    Returns counts/percentages for over/under bets across stages.
    """
    df = get_dataframe(tournament)
    if df.empty:
        return {"tournament": tournament, "total": 0}

    # Ensure numeric
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

        # Stage 1: basic direction
        diff = tim - predict
        if abs(diff) < 0.1:
            no_bet += 1
            continue

        direction = "over" if diff > 0 else "under"

        # Stage 2: add deviation
        stage2 = diff + deviation
        if (stage2 > 0 and direction == "under") or (stage2 < 0 and direction == "over"):
            no_bet += 1
            continue

        if direction == "over":
            over_count += 1
        else:
            under_count += 1

        # Win/lose check
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
    """Run analysis for every tournament."""
    tournaments = get_tournaments()
    return [analyze_tournament(t) for t in tournaments]
