"""Business logic for the Halfs (База половин) section.

This wraps the existing HalfsDatabase class, adapting it for the web
backend.  Works with both SQLite and PostgreSQL via db_connect.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re

import pandas as pd

from backend.app.database.connection import get_halfs_connection
from db_connection import is_postgres, adapt_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows_to_dicts(cursor) -> List[dict]:
    """Convert cursor results to a list of dicts using cursor.description."""
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Data import — line parsing reuses the logic from halfs_database.py
# ---------------------------------------------------------------------------

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

    start_idx = 0
    try:
        parsed_date = datetime.strptime(tokens[0], "%d.%m.%Y")
        date_iso = parsed_date.strftime("%Y-%m-%d")
        start_idx = 1
    except Exception:
        date_iso = ""

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

    team_away = meta[-1]
    team_home = meta[-2]
    tournament = " ".join(meta[:-2])

    if scores_type == "quarters":
        if len(scores) == 8:
            q1h, q1a, q2h, q2a, q3h, q3a, q4h, q4a = scores
            oth, ota = 0, 0
        else:
            q1h, q1a, q2h, q2a, q3h, q3a, q4h, q4a, oth, ota = scores
    else:
        if len(scores) == 4:
            q1h, q1a, q2h, q2a = scores
            q3h = q3a = q4h = q4a = oth = ota = 0
        else:
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
        with get_halfs_connection() as conn:
            cur = conn.cursor()
            cur.executemany(
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
    params: list = []
    if tournament:
        query += " WHERE tournament = ?"
        params.append(tournament)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        return _rows_to_dicts(cur)


def get_tournaments() -> List[str]:
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT tournament FROM matches WHERE tournament IS NOT NULL ORDER BY tournament"
        )
        return [r[0] for r in cur.fetchall()]


def delete_matches(ids: List[int]) -> int:
    if not ids:
        return 0
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def clear_all() -> None:
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM matches")
        conn.commit()


def get_statistics() -> dict:
    with get_halfs_connection() as conn:
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
    return {"total_matches": total, "tournaments": tournaments, "teams": teams}


# ---------------------------------------------------------------------------
# Statistics calculations
# ---------------------------------------------------------------------------

def _get_halfs_df(tournament: Optional[str] = None) -> pd.DataFrame:
    """Load matches as DataFrame."""
    with get_halfs_connection() as conn:
        if is_postgres():
            raw = conn._conn if hasattr(conn, '_conn') else conn
            if tournament:
                return pd.read_sql_query(
                    adapt_sql("SELECT * FROM matches WHERE tournament = ?"),
                    raw, params=(tournament,),
                )
            return pd.read_sql_query("SELECT * FROM matches", raw)
        else:
            if tournament:
                return pd.read_sql_query(
                    "SELECT * FROM matches WHERE tournament = ?",
                    conn, params=(tournament,),
                )
            return pd.read_sql_query("SELECT * FROM matches", conn)


def get_team_statistics(tournament: str) -> List[dict]:
    """Team statistics for a tournament (avg quarter scores, totals, etc.)."""
    df = _get_halfs_df(tournament)
    if df.empty:
        return []

    results = []
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
    df = _get_halfs_df()
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


# ---------------------------------------------------------------------------
# Deviations
# ---------------------------------------------------------------------------

def get_team_deviations(tournament: str) -> List[dict]:
    stats = get_team_statistics(tournament)
    if not stats:
        return []
    results = []
    for s in stats:
        h1_total = s["h1_scored"] + s["h1_conceded"]
        h2_total = s["h2_scored"] + s["h2_conceded"]
        deviation = round(h2_total - h1_total, 1)
        avg_total = round(h1_total + h2_total, 1)
        results.append({
            "team": s["team"],
            "games": s["games"],
            "h1_total": round(h1_total, 1),
            "h2_total": round(h2_total, 1),
            "deviation": deviation,
            "average_total": avg_total,
        })
    return sorted(results, key=lambda x: x["team"])


# ---------------------------------------------------------------------------
# Wins / Losses
# ---------------------------------------------------------------------------

def get_wins_losses(tournament: str) -> List[dict]:
    df = _get_halfs_df(tournament)
    if df.empty:
        return []

    df = df.copy()
    for side in ("home", "away"):
        df[f"total_{side}"] = (
            df[f"q1_{side}"].fillna(0) + df[f"q2_{side}"].fillna(0) +
            df[f"q3_{side}"].fillna(0) + df[f"q4_{side}"].fillna(0) +
            df[f"ot_{side}"].fillna(0)
        )

    teams = sorted(set(df["team_home"]) | set(df["team_away"]))
    wins_map: Dict[str, int] = {t: 0 for t in teams}
    losses_map: Dict[str, int] = {t: 0 for t in teams}

    for _, row in df.iterrows():
        th, ta = row["total_home"], row["total_away"]
        if th > ta:
            wins_map[row["team_home"]] += 1
            losses_map[row["team_away"]] += 1
        elif ta > th:
            wins_map[row["team_away"]] += 1
            losses_map[row["team_home"]] += 1

    return [
        {"team": t, "wins": wins_map[t], "losses": losses_map[t],
         "total": wins_map[t] + losses_map[t],
         "win_pct": round(wins_map[t] / (wins_map[t] + losses_map[t]) * 100, 1) if (wins_map[t] + losses_map[t]) > 0 else 0}
        for t in teams
    ]


# ---------------------------------------------------------------------------
# Quarter distribution
# ---------------------------------------------------------------------------

def get_quarter_distribution(tournament: str, team1: str, team2: str, total: float) -> Optional[dict]:
    stats = {s["team"]: s for s in get_team_statistics(tournament)}
    if team1 not in stats or team2 not in stats:
        return None
    t1, t2 = stats[team1], stats[team2]

    q1 = ((t1["q1_scored"] + t1["q1_conceded"]) + (t2["q1_scored"] + t2["q1_conceded"])) / 2.0
    q2 = ((t1["q2_scored"] + t1["q2_conceded"]) + (t2["q2_scored"] + t2["q2_conceded"])) / 2.0
    q3 = ((t1["q3_scored"] + t1["q3_conceded"]) + (t2["q3_scored"] + t2["q3_conceded"])) / 2.0
    q4 = ((t1["q4_scored"] + t1["q4_conceded"]) + (t2["q4_scored"] + t2["q4_conceded"])) / 2.0
    q_total = q1 + q2 + q3 + q4
    if q_total == 0:
        return None

    return {
        "team1": team1,
        "team2": team2,
        "total": total,
        "q1": round(total * q1 / q_total, 1),
        "q2": round(total * q2 / q_total, 1),
        "q3": round(total * q3 / q_total, 1),
        "q4": round(total * q4 / q_total, 1),
        "h1": round(total * (q1 + q2) / q_total, 1),
        "h2": round(total * (q3 + q4) / q_total, 1),
        "q1_pct": round(q1 / q_total * 100, 1),
        "q2_pct": round(q2 / q_total * 100, 1),
        "q3_pct": round(q3 / q_total * 100, 1),
        "q4_pct": round(q4 / q_total * 100, 1),
    }


# ---------------------------------------------------------------------------
# Over/Under coefficients
# ---------------------------------------------------------------------------

def get_coefficients(
    tournament: str, team1: str, team2: str,
    q_threshold: float, h_threshold: float, m_threshold: float,
) -> Optional[dict]:
    df = _get_halfs_df(tournament)
    if df.empty:
        return None

    df = df.copy()
    df["q1_total"] = df["q1_home"].fillna(0) + df["q1_away"].fillna(0)
    df["q2_total"] = df["q2_home"].fillna(0) + df["q2_away"].fillna(0)
    df["q3_total"] = df["q3_home"].fillna(0) + df["q3_away"].fillna(0)
    df["q4_total"] = df["q4_home"].fillna(0) + df["q4_away"].fillna(0)
    df["h1_total"] = df["q1_total"] + df["q2_total"]
    df["h2_total"] = df["q3_total"] + df["q4_total"]
    df["match_total"] = df["h1_total"] + df["h2_total"]

    teams = sorted(set(df["team_home"]) | set(df["team_away"]))
    counts: Dict[str, Dict[str, int]] = {t: {
        "games": 0, "q1": 0, "q2": 0, "q3": 0, "q4": 0, "h1": 0, "h2": 0, "match": 0,
    } for t in teams}

    for _, row in df.iterrows():
        home, away = row["team_home"], row["team_away"]
        counts[home]["games"] += 1
        counts[away]["games"] += 1
        for q, col in [("q1", "q1_total"), ("q2", "q2_total"), ("q3", "q3_total"), ("q4", "q4_total")]:
            if row[col] > q_threshold:
                counts[home][q] += 1
                counts[away][q] += 1
        if row["h1_total"] > h_threshold:
            counts[home]["h1"] += 1
            counts[away]["h1"] += 1
        if row["h2_total"] > h_threshold:
            counts[home]["h2"] += 1
            counts[away]["h2"] += 1
        if row["match_total"] > m_threshold:
            counts[home]["match"] += 1
            counts[away]["match"] += 1

    if team1 not in counts or team2 not in counts:
        return None

    periods = ["q1", "q2", "q3", "q4", "h1", "h2", "match"]
    result: dict = {"over": {}, "under": {}, "counts": {
        "team1": {"name": team1, **counts.get(team1, {})},
        "team2": {"name": team2, **counts.get(team2, {})},
    }}
    for p in periods:
        overs = counts[team1][p] + counts[team2][p]
        games = counts[team1]["games"] + counts[team2]["games"]
        if overs == 0 or games == 0:
            result["over"][p] = 0.0
            result["under"][p] = 0.0
        else:
            oc = games / overs
            result["over"][p] = round(oc, 2)
            result["under"][p] = round(oc / (oc - 1), 2) if oc != 1.0 else 0.0
    return result
