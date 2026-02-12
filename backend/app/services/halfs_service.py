"""Business logic for the Halfs (База половин) section.

This wraps the existing HalfsDatabase class, adapting it for the web
backend with a configurable database path.
"""

import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.app.config import get_settings


def _db_path() -> str:
    return str(get_settings().data_dir / "halfs.db")


# ---------------------------------------------------------------------------
# Data import — line parsing reuses the logic from halfs_database.py
# ---------------------------------------------------------------------------

from datetime import datetime
import re


def _parse_match_line(line: str) -> Optional[Tuple]:
    """Parses a raw match line.

    Simplified version of the desktop parser that handles:
    - 8 scores (4 quarters), 10 scores (4 quarters + OT)
    - 4 or 6 scores (NCAA D1 halves + optional OT)
    - Multi-word team and tournament names
    """
    if not line or not line.strip():
        return None
    tokens = [t.strip() for t in line.strip().split() if t.strip()]
    if len(tokens) < 7:
        return None

    # Date
    start_idx = 0
    try:
        parsed_date = datetime.strptime(tokens[0], "%d.%m.%Y")
        date_iso = parsed_date.strftime("%Y-%m-%d")
        start_idx = 1
    except Exception:
        date_iso = ""

    # Find trailing scores
    total = len(tokens)
    scores = []
    score_start = None
    scores_type = "quarters"

    for count in (10, 8, 6, 4):
        if total - start_idx >= count:
            tail = tokens[total - count:]
            if all(t.lstrip("+-").isdigit() for t in tail):
                scores = [int(t) for t in tail]
                score_start = total - count
                scores_type = "halves" if count in (4, 6) else "quarters"
                break

    if not scores:
        return None

    meta = tokens[start_idx:score_start]
    if len(meta) < 3:
        return None

    # Group (W) tokens
    grouped = []
    i = 0
    while i < len(meta):
        if meta[i] == "(W)" and grouped:
            grouped[-1] += " (W)"
        else:
            grouped.append(meta[i])
        i += 1
    meta = grouped
    if len(meta) < 3:
        return None

    # Split: last two tokens are teams, rest is tournament
    team_away = meta[-1]
    team_home = meta[-2]
    tournament = " ".join(meta[:-2])

    # Build score tuple
    if scores_type == "quarters":
        if len(scores) == 8:
            q1h, q1a, q2h, q2a, q3h, q3a, q4h, q4a = scores
            oth, ota = 0, 0
        else:  # 10
            q1h, q1a, q2h, q2a, q3h, q3a, q4h, q4a, oth, ota = scores
    else:  # halves (NCAA D1)
        if len(scores) == 4:
            q1h, q1a, q2h, q2a = scores
            q3h = q3a = q4h = q4a = oth = ota = 0
        else:  # 6
            q1h, q1a, q2h, q2a, oth, ota = scores
            q3h = q3a = q4h = q4a = 0

    return (
        date_iso, tournament, team_home, team_away,
        q1h, q1a, q2h, q2a, q3h, q3a, q4h, q4a, oth, ota,
    )


def import_matches(raw_text: str) -> Tuple[int, List[str]]:
    """Import matches from raw text lines.

    Returns (imported_count, error_lines).
    """
    parsed = []
    errors = []
    for line in raw_text.strip().splitlines():
        if not line.strip():
            continue
        result = _parse_match_line(line)
        if result:
            parsed.append(result)
        else:
            errors.append(line.strip())

    if parsed:
        with sqlite3.connect(_db_path()) as conn:
            conn.executemany(
                """INSERT INTO matches (
                    date, tournament, team_home, team_away,
                    q1_home, q1_away, q2_home, q2_away,
                    q3_home, q3_away, q4_home, q4_away,
                    ot_home, ot_away
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                parsed,
            )
            conn.commit()

    return len(parsed), errors


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------

def get_all_matches(tournament: Optional[str] = None, limit: int = 10000) -> List[dict]:
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
        cur = conn.execute(
            "SELECT DISTINCT tournament FROM matches WHERE tournament IS NOT NULL ORDER BY tournament"
        )
        return [r[0] for r in cur.fetchall()]


def delete_matches(ids: List[int]) -> int:
    if not ids:
        return 0
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def clear_all() -> None:
    with sqlite3.connect(_db_path()) as conn:
        conn.execute("DELETE FROM matches")
        conn.commit()


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
    return {"total_matches": total, "tournaments": tournaments, "teams": teams}


# ---------------------------------------------------------------------------
# Statistics calculations
# ---------------------------------------------------------------------------

def get_team_statistics(tournament: str) -> List[dict]:
    """Team statistics for a tournament (avg quarter scores, totals, etc.)."""
    with sqlite3.connect(_db_path()) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM matches WHERE tournament = ?",
            conn,
            params=(tournament,),
        )
    if df.empty:
        return []

    results = []
    # Home stats
    for team in df["team_home"].unique():
        home = df[df["team_home"] == team]
        away = df[df["team_away"] == team]
        n_games = len(home) + len(away)
        if n_games == 0:
            continue

        def avg_col(frame, col):
            vals = frame[col].dropna()
            return vals.mean() if len(vals) > 0 else 0

        q1_scored = (avg_col(home, "q1_home") * len(home) + avg_col(away, "q1_away") * len(away)) / n_games
        q2_scored = (avg_col(home, "q2_home") * len(home) + avg_col(away, "q2_away") * len(away)) / n_games
        q3_scored = (avg_col(home, "q3_home") * len(home) + avg_col(away, "q3_away") * len(away)) / n_games
        q4_scored = (avg_col(home, "q4_home") * len(home) + avg_col(away, "q4_away") * len(away)) / n_games

        q1_conceded = (avg_col(home, "q1_away") * len(home) + avg_col(away, "q1_home") * len(away)) / n_games
        q2_conceded = (avg_col(home, "q2_away") * len(home) + avg_col(away, "q2_home") * len(away)) / n_games
        q3_conceded = (avg_col(home, "q3_away") * len(home) + avg_col(away, "q3_home") * len(away)) / n_games
        q4_conceded = (avg_col(home, "q4_away") * len(home) + avg_col(away, "q4_home") * len(away)) / n_games

        h1_scored = q1_scored + q2_scored
        h2_scored = q3_scored + q4_scored
        h1_conceded = q1_conceded + q2_conceded
        h2_conceded = q3_conceded + q4_conceded

        results.append({
            "team": team,
            "games": n_games,
            "q1_scored": round(q1_scored, 1),
            "q2_scored": round(q2_scored, 1),
            "q3_scored": round(q3_scored, 1),
            "q4_scored": round(q4_scored, 1),
            "q1_conceded": round(q1_conceded, 1),
            "q2_conceded": round(q2_conceded, 1),
            "q3_conceded": round(q3_conceded, 1),
            "q4_conceded": round(q4_conceded, 1),
            "h1_scored": round(h1_scored, 1),
            "h2_scored": round(h2_scored, 1),
            "h1_conceded": round(h1_conceded, 1),
            "h2_conceded": round(h2_conceded, 1),
            "total_scored": round(h1_scored + h2_scored, 1),
            "total_conceded": round(h1_conceded + h2_conceded, 1),
        })

    return sorted(results, key=lambda x: x["team"])


def get_tournament_summary() -> List[dict]:
    """Summary statistics per tournament."""
    with sqlite3.connect(_db_path()) as conn:
        df = pd.read_sql_query("SELECT * FROM matches", conn)
    if df.empty:
        return []

    results = []
    for tournament, grp in df.groupby("tournament"):
        n_games = len(grp)
        q_cols_home = ["q1_home", "q2_home", "q3_home", "q4_home"]
        q_cols_away = ["q1_away", "q2_away", "q3_away", "q4_away"]
        avg_total_home = sum(grp[c].mean() for c in q_cols_home)
        avg_total_away = sum(grp[c].mean() for c in q_cols_away)

        teams = set(grp["team_home"].unique()) | set(grp["team_away"].unique())

        results.append({
            "tournament": tournament,
            "games": n_games,
            "teams": len(teams),
            "avg_total": round(avg_total_home + avg_total_away, 1),
            "avg_h1": round(grp["q1_home"].mean() + grp["q2_home"].mean() +
                           grp["q1_away"].mean() + grp["q2_away"].mean(), 1),
            "avg_h2": round(grp["q3_home"].mean() + grp["q4_home"].mean() +
                           grp["q3_away"].mean() + grp["q4_away"].mean(), 1),
        })

    return sorted(results, key=lambda x: x["tournament"])
