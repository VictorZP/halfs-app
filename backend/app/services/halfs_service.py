"""Business logic for the Halfs (База половин) section.

This wraps the existing HalfsDatabase class, adapting it for the web
backend.  Works with both SQLite and PostgreSQL via db_connect.
"""

from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple

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


def _denormalize_marker(value):
    if isinstance(value, str):
        return value.replace("~", " ").strip()
    return value


def _normalize_match_tuple(row: Tuple) -> Tuple:
    """Replace temporary markers in textual fields of parsed row."""
    return (
        row[0],
        _denormalize_marker(row[1]),
        _denormalize_marker(row[2]),
        _denormalize_marker(row[3]),
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        row[9],
        row[10],
        row[11],
        row[12],
        row[13],
    )


def _to_year_4digits(year_token: str) -> Optional[int]:
    if not year_token.isdigit():
        return None
    if len(year_token) == 4:
        return int(year_token)
    if len(year_token) == 2:
        year = int(year_token)
        return 2000 + year if year <= 69 else 1900 + year
    return None


def _parse_loose_date_to_iso(value: str) -> Optional[str]:
    """Parse many date formats and return YYYY-MM-DD."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Native ISO support first.
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    cleaned = (
        s.replace("/", ".")
        .replace("-", ".")
        .replace("\\", ".")
        .replace(" ", "")
    )
    parts = [p for p in cleaned.split(".") if p]
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        return None

    a, b, c = parts
    if len(a) == 4:
        year = int(a)
        month = int(b)
        day = int(c)
    else:
        year = _to_year_4digits(c)
        if year is None:
            return None
        x, y = int(a), int(b)
        # Prefer dd.mm, but auto-swap when impossible.
        if x > 12 and y <= 12:
            day, month = x, y
        elif y > 12 and x <= 12:
            day, month = y, x
        else:
            # Ambiguous (both <=12): for old records like 02.21.26 month/day is typical.
            if year >= 2000 and y > 12:
                day, month = y, x
            else:
                day, month = x, y

    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except Exception:
        return None


def _to_display_date(value: str) -> str:
    """Convert stored date to dd.mm.yyyy if parseable."""
    iso = _parse_loose_date_to_iso(value)
    if not iso:
        return str(value or "")
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        return str(value or "")


# ---------------------------------------------------------------------------
# Data import — fully aligned with desktop parsing behavior
# ---------------------------------------------------------------------------

def _parse_match_line(line: str) -> Optional[Tuple]:
    """Desktop-compatible parser without external module dependency."""
    if not line or not line.strip():
        return None
    tokens = [tok.strip() for tok in line.strip().split() if tok.strip()]
    if len(tokens) < 7:
        return None

    start_idx = 0
    try:
        parsed_date = _parse_loose_date_to_iso(tokens[0])
        if parsed_date is None:
            raise ValueError("not a date")
        date_iso = parsed_date
        start_idx = 1
    except Exception:
        date_iso = ""
        start_idx = 0

    scores = []
    score_start = None
    scores_type = "quarters"
    total_tokens = len(tokens)

    if total_tokens - start_idx >= 10 and all(
        tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 10:]
    ):
        try:
            scores = [int(tok) for tok in tokens[total_tokens - 10:]]
            score_start = total_tokens - 10
            scores_type = "quarters"
        except Exception:
            scores = []
            score_start = None

    if not scores:
        if total_tokens - start_idx >= 8 and all(
            tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 8:]
        ):
            try:
                scores = [int(tok) for tok in tokens[total_tokens - 8:]]
                score_start = total_tokens - 8
                scores_type = "quarters"
            except Exception:
                return None
        else:
            if total_tokens - start_idx >= 6 and all(
                tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 6:]
            ):
                try:
                    scores = [int(tok) for tok in tokens[total_tokens - 6:]]
                    score_start = total_tokens - 6
                    scores_type = "halves"
                except Exception:
                    return None
            elif total_tokens - start_idx >= 4 and all(
                tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 4:]
            ):
                try:
                    scores = [int(tok) for tok in tokens[total_tokens - 4:]]
                    score_start = total_tokens - 4
                    scores_type = "halves"
                except Exception:
                    return None
            else:
                return None

    meta_tokens = tokens[start_idx:score_start]
    if len(meta_tokens) < 3:
        return None

    grouped: List[str] = []
    i = 0
    while i < len(meta_tokens):
        tok = meta_tokens[i]
        if tok == "(W)":
            if grouped:
                grouped[-1] = f"{grouped[-1]} {tok}"
            else:
                grouped.append(tok)
            i += 1
            continue
        grouped.append(tok)
        i += 1
    meta_tokens = grouped
    if len(meta_tokens) < 3:
        return None

    team_away_tokens: List[str] = []
    team_home_tokens: List[str] = []

    def is_numeric_string(tok: str) -> bool:
        return tok.lstrip("+-").isdigit()

    numeric_case = False
    i = len(meta_tokens) - 1
    if i < 0:
        return None
    if is_numeric_string(meta_tokens[i]):
        if i - 1 < 0:
            return None
        team_away_tokens = [meta_tokens[i - 1], meta_tokens[i]]
        numeric_case = True
        i -= 2
    else:
        team_away_tokens = [meta_tokens[i]]
        i -= 1

    if i < 0:
        return None
    if is_numeric_string(meta_tokens[i]):
        if i - 1 < 0:
            return None
        team_home_tokens = [meta_tokens[i - 1], meta_tokens[i]]
        numeric_case = True
        i -= 2
    else:
        team_home_tokens = [meta_tokens[i]]
        i -= 1

    if i >= 0 and is_numeric_string(meta_tokens[i]):
        team_home_tokens.insert(0, meta_tokens[i])
        numeric_case = True
        i -= 1

    meta_tokens = meta_tokens[: i + 1]

    def looks_like_suffix(token: str) -> bool:
        return bool(
            re.fullmatch(r"[A-Za-z]{1,2}", token)
            or re.fullmatch(r"[A-Za-z]+-\d+", token)
        )

    if (
        looks_like_suffix(team_away_tokens[0])
        and not re.search(r"[-\d()]", team_home_tokens[0])
        and len(meta_tokens) >= 2
    ):
        team_away_tokens.insert(0, team_home_tokens.pop())
        if meta_tokens:
            team_home_tokens = [meta_tokens.pop()]
        else:
            return None

    if (
        not numeric_case
        and len(meta_tokens) >= 2
        and not re.search(r"[-\d()]", team_home_tokens[0])
        and not re.search(r"[-\d()]", meta_tokens[-1])
        and len(meta_tokens[-1]) > 1
        and "+" not in meta_tokens[-1]
        and meta_tokens[-1].upper() not in {
            "EAST", "WEST", "NORTH", "SOUTH", "CENTRAL", "CENTER",
            "NORTHWEST", "NORTHEAST", "SOUTHWEST", "SOUTHEAST",
        }
    ):
        team_home_tokens.insert(0, meta_tokens.pop())

    tournament = " ".join(meta_tokens).strip()
    team_home = " ".join(team_home_tokens).strip()
    team_away = " ".join(team_away_tokens).strip()

    if scores_type == "halves":
        tournament_norm = tournament.replace("~", " ").strip().upper()
        if tournament_norm != "NCAA D1":
            return None
        q1_home, q1_away = scores[0:2]
        q2_home, q2_away = scores[2:4]
        q3_home, q3_away = 0, 0
        q4_home, q4_away = 0, 0
        ot_home, ot_away = None, None
        if len(scores) == 6:
            ot_home, ot_away = scores[4:6]
    else:
        q1_home, q1_away = scores[0:2]
        q2_home, q2_away = scores[2:4]
        q3_home, q3_away = scores[4:6]
        q4_home, q4_away = scores[6:8]
        ot_home, ot_away = None, None
        if len(scores) == 10:
            ot_home, ot_away = scores[8:10]

    return (
        date_iso,
        tournament,
        team_home,
        team_away,
        q1_home,
        q1_away,
        q2_home,
        q2_away,
        q3_home,
        q3_away,
        q4_home,
        q4_away,
        ot_home,
        ot_away,
    )


def _prepare_import_lines(raw_text: str) -> List[str]:
    """Prepare pasted lines exactly like desktop HalfsDatabasePage.import_matches."""
    lines = [ln for ln in raw_text.strip().splitlines() if ln.strip()]
    prepared: List[str] = []

    for line in lines:
        if "\t" in line:
            cells = [c.strip() for c in line.split("\t")]
            new_cells: List[str] = []
            for cell in cells:
                if any(ch.isalpha() for ch in cell):
                    # Preserve multi-word names as a single token for parser.
                    tmp = cell.replace("_", " ").split()
                    new_cells.append("~".join(tmp))
                else:
                    new_cells.append(cell)
            prepared.append(" ".join(new_cells))
        else:
            prepared.append(" ".join(line.split()))

    return prepared


def _parse_import_raw_text(raw_text: str) -> Tuple[List[Tuple], List[str]]:
    parsed: List[Tuple] = []
    errors: List[str] = []
    for line in _prepare_import_lines(raw_text):
        processed_line = line.replace("_", " ")
        result = _parse_match_line(processed_line)
        if result:
            parsed.append(_normalize_match_tuple(result))
        else:
            errors.append(line.strip().replace("~", " "))
    return parsed, errors


def preview_import(raw_text: str) -> dict:
    parsed, errors = _parse_import_raw_text(raw_text)
    rows = []
    for row in parsed:
        rows.append(
            {
                "date": _to_display_date(row[0] or ""),
                "tournament": row[1] or "",
                "team_home": row[2] or "",
                "team_away": row[3] or "",
                "q1_home": row[4],
                "q1_away": row[5],
                "q2_home": row[6],
                "q2_away": row[7],
                "q3_home": row[8],
                "q3_away": row[9],
                "q4_home": row[10],
                "q4_away": row[11],
                "ot_home": row[12],
                "ot_away": row[13],
            }
        )
    return {
        "parsed_count": len(parsed),
        "error_count": len(errors),
        "parsed_rows": rows,
        "errors": errors,
    }


def import_matches(raw_text: str) -> Tuple[int, List[str]]:
    """Import matches from raw text lines.

    Returns (imported_count, error_lines).
    """
    parsed, errors = _parse_import_raw_text(raw_text)

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
        query += " WHERE tournament = ? OR REPLACE(tournament, '~', ' ') = ?"
        params.extend([tournament, tournament])
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = _rows_to_dicts(cur)
        for row in rows:
            row["date"] = _to_display_date(row.get("date"))
            row["tournament"] = _denormalize_marker(row.get("tournament"))
            row["team_home"] = _denormalize_marker(row.get("team_home"))
            row["team_away"] = _denormalize_marker(row.get("team_away"))
        return rows


def get_tournaments() -> List[str]:
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT tournament FROM matches WHERE tournament IS NOT NULL ORDER BY tournament"
        )
        values = [_denormalize_marker(r[0]) for r in cur.fetchall() if r and r[0]]
        return sorted(set(values))


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


def update_match_field(match_id: int, field: str, value: str) -> bool:
    allowed_fields = {
        "date", "tournament", "team_home", "team_away",
        "q1_home", "q1_away", "q2_home", "q2_away",
        "q3_home", "q3_away", "q4_home", "q4_away",
        "ot_home", "ot_away",
    }
    if field not in allowed_fields:
        return False

    prepared_value = value
    if field == "date":
        iso = _parse_loose_date_to_iso(value)
        if iso is None:
            return False
        prepared_value = iso
    elif field in {"tournament", "team_home", "team_away"}:
        prepared_value = _denormalize_marker(value)
    else:
        text = str(value).strip()
        if text == "":
            prepared_value = None
        else:
            try:
                prepared_value = int(float(text))
            except Exception:
                return False

    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE matches SET {field} = ? WHERE id = ?", (prepared_value, match_id))
        conn.commit()
        return cur.rowcount > 0


def normalize_existing_dates() -> int:
    updated = 0
    with get_halfs_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, date FROM matches")
        rows = cur.fetchall()
        updates = []
        for match_id, raw_date in rows:
            iso = _parse_loose_date_to_iso(raw_date)
            if iso and iso != raw_date:
                updates.append((iso, match_id))
        if updates:
            cur.executemany("UPDATE matches SET date = ? WHERE id = ?", updates)
            conn.commit()
            updated = len(updates)
    return updated


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
                df = pd.read_sql_query(
                    adapt_sql("SELECT * FROM matches WHERE tournament = ? OR REPLACE(tournament, '~', ' ') = ?"),
                    raw, params=(tournament, tournament),
                )
            else:
                df = pd.read_sql_query("SELECT * FROM matches", raw)
        else:
            if tournament:
                df = pd.read_sql_query(
                    "SELECT * FROM matches WHERE tournament = ? OR REPLACE(tournament, '~', ' ') = ?",
                    conn, params=(tournament, tournament),
                )
            else:
                df = pd.read_sql_query("SELECT * FROM matches", conn)

    if not df.empty:
        for col in ("tournament", "team_home", "team_away"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace("~", " ", regex=False).str.strip()
    return df


def get_team_statistics(tournament: str) -> List[dict]:
    """Team statistics for a tournament (avg quarter scores, totals, etc.)."""
    df = _get_halfs_df(tournament)
    if df.empty:
        return []

    results = []
    all_teams = sorted(set(df["team_home"].dropna()) | set(df["team_away"].dropna()))
    for team in all_teams:
        team = str(team).strip()
        if not team:
            continue
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
    q_threshold: Optional[float] = None,
    h_threshold: Optional[float] = None,
    m_threshold: Optional[float] = None,
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

    if q_threshold is None and h_threshold is None and m_threshold is None:
        return None

    teams = sorted(set(df["team_home"]) | set(df["team_away"]))
    counts: Dict[str, Dict[str, int]] = {t: {
        "games": 0, "q1": 0, "q2": 0, "q3": 0, "q4": 0, "h1": 0, "h2": 0, "match": 0,
    } for t in teams}

    for _, row in df.iterrows():
        home, away = row["team_home"], row["team_away"]
        counts[home]["games"] += 1
        counts[away]["games"] += 1
        if q_threshold is not None:
            for q, col in [("q1", "q1_total"), ("q2", "q2_total"), ("q3", "q3_total"), ("q4", "q4_total")]:
                if row[col] > q_threshold:
                    counts[home][q] += 1
                    counts[away][q] += 1
        if h_threshold is not None:
            if row["h1_total"] > h_threshold:
                counts[home]["h1"] += 1
                counts[away]["h1"] += 1
            if row["h2_total"] > h_threshold:
                counts[home]["h2"] += 1
                counts[away]["h2"] += 1
        if m_threshold is not None:
            if row["match_total"] > m_threshold:
                counts[home]["match"] += 1
                counts[away]["match"] += 1

    if team1 not in counts or team2 not in counts:
        return None

    periods: List[str] = []
    if q_threshold is not None:
        periods.extend(["q1", "q2", "q3", "q4"])
    if h_threshold is not None:
        periods.extend(["h1", "h2"])
    if m_threshold is not None:
        periods.append("match")

    result: dict = {"over": {}, "under": {}, "counts": {
        "team1": {"name": team1, **counts.get(team1, {})},
        "team2": {"name": team2, **counts.get(team2, {})},
    }, "requested_periods": periods}
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
