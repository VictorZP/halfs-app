"""Business logic for the Cybers section.

This module is a direct extraction of CybersDatabase from the desktop
application.  It provides all the weighted-average and predict logic
without any PyQt5 dependency.
"""

import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from backend.app.config import get_settings


def _db_path() -> str:
    return str(get_settings().data_dir / "cyber_bases.db")


def normalize_key(value: str) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

COLUMNS = [
    "date", "tournament", "team", "home_away",
    "two_pt_made", "two_pt_attempt",
    "three_pt_made", "three_pt_attempt",
    "fta_made", "fta_attempt",
    "off_rebound", "turnovers",
    "controls", "points",
    "opponent", "attak_kef", "status",
]


def add_rows(rows: List[dict]) -> int:
    if not rows:
        return 0
    values = [
        tuple(r.get(c) for c in COLUMNS)
        for r in rows
    ]
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.executemany(
            f"""INSERT INTO cyber_matches ({', '.join(COLUMNS)})
                VALUES ({', '.join('?' * len(COLUMNS))})""",
            values,
        )
        conn.commit()
    return len(rows)


def clear() -> None:
    with sqlite3.connect(_db_path()) as conn:
        conn.execute("DELETE FROM cyber_matches")
        conn.commit()


def get_dataframe() -> pd.DataFrame:
    with sqlite3.connect(_db_path()) as conn:
        return pd.read_sql_query(
            "SELECT * FROM cyber_matches ORDER BY id ASC", conn
        )


def get_dataframe_for_tournament(tournament: str) -> pd.DataFrame:
    with sqlite3.connect(_db_path()) as conn:
        return pd.read_sql_query(
            "SELECT * FROM cyber_matches WHERE tournament = ? ORDER BY id ASC",
            conn,
            params=(tournament,),
        )


def get_tournaments() -> List[str]:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT tournament FROM cyber_matches "
            "WHERE tournament IS NOT NULL AND tournament <> ''"
        )
        return sorted(r[0] for r in cur.fetchall())


def delete_rows(ids: List[int]) -> int:
    if not ids:
        return 0
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM cyber_matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def delete_tournament(tournament: str) -> int:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_matches WHERE tournament = ?", (tournament,))
        conn.commit()
        return cur.rowcount


def update_match_field(row_id: int, field: str, value) -> None:
    if field not in COLUMNS:
        return
    with sqlite3.connect(_db_path()) as conn:
        conn.execute(
            f"UPDATE cyber_matches SET {field} = ? WHERE id = ?", (value, row_id)
        )
        conn.commit()


def find_duplicate_pairs() -> List[List[int]]:
    df = get_dataframe()
    if df.empty:
        return []
    pairs = []
    for i in range(0, len(df), 2):
        if i + 1 >= len(df):
            break
        r1, r2 = df.iloc[i], df.iloc[i + 1]
        sig = (
            tuple(r1.get(c) for c in COLUMNS),
            tuple(r2.get(c) for c in COLUMNS),
        )
        pairs.append((sig, [int(r1["id"]), int(r2["id"])]))
    seen: dict = {}
    duplicates: List[List[int]] = []
    for sig, ids in pairs:
        if sig in seen:
            duplicates.append(ids)
        else:
            seen[sig] = ids
    return duplicates


def replace_in_base(find: str, replace: str, ids: Optional[List[int]] = None) -> int:
    """Find-and-replace across all text columns. Returns count of updated rows."""
    df = get_dataframe()
    if df.empty:
        return 0
    text_cols = ["date", "tournament", "team", "home_away", "opponent", "status"]
    count = 0
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        for _, row in df.iterrows():
            if ids and int(row["id"]) not in ids:
                continue
            for col in text_cols:
                val = str(row.get(col) or "")
                if find in val:
                    new_val = val.replace(find, replace)
                    cur.execute(
                        f"UPDATE cyber_matches SET {col} = ? WHERE id = ?",
                        (new_val, int(row["id"])),
                    )
                    count += 1
        conn.commit()
    return count


# ---------------------------------------------------------------------------
# Live matches
# ---------------------------------------------------------------------------

def load_live_matches() -> List[dict]:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, tournament, team1, team2, total, calc_temp "
            "FROM cyber_live_matches ORDER BY id ASC"
        )
        return [
            {
                "id": r[0],
                "tournament": r[1],
                "team1": r[2],
                "team2": r[3],
                "total": r[4],
                "calc_temp": r[5] or 0.0,
            }
            for r in cur.fetchall()
        ]


def save_live_matches(rows: List[dict]) -> None:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_live_matches")
        if rows:
            cur.executemany(
                "INSERT INTO cyber_live_matches (tournament, team1, team2, total, calc_temp) "
                "VALUES (?, ?, ?, ?, ?)",
                [(r["tournament"], r["team1"], r["team2"], r.get("total"), r.get("calc_temp", 0.0)) for r in rows],
            )
        conn.commit()


def clear_live_matches() -> None:
    with sqlite3.connect(_db_path()) as conn:
        conn.execute("DELETE FROM cyber_live_matches")
        conn.commit()


def add_live_match(tournament: str, team1: str, team2: str, total=None, calc_temp: float = 0.0) -> int:
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cyber_live_matches (tournament, team1, team2, total, calc_temp) "
            "VALUES (?, ?, ?, ?, ?)",
            (tournament, team1, team2, total, calc_temp),
        )
        conn.commit()
        return cur.lastrowid


def delete_live_match(match_id: int) -> None:
    with sqlite3.connect(_db_path()) as conn:
        conn.execute("DELETE FROM cyber_live_matches WHERE id = ?", (match_id,))
        conn.commit()


def update_live_match(match_id: int, **fields) -> None:
    if not fields:
        return
    allowed = {"tournament", "team1", "team2", "total", "calc_temp"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if not sets:
        return
    vals.append(match_id)
    with sqlite3.connect(_db_path()) as conn:
        conn.execute(
            f"UPDATE cyber_live_matches SET {', '.join(sets)} WHERE id = ?", vals
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def get_summary() -> List[dict]:
    """Tournament summary statistics (mirrors the Сводная статистика tab)."""
    df = get_dataframe()
    if df.empty:
        return []
    results = []
    for tournament, grp in df.groupby("tournament"):
        n = len(grp) // 2 or 1
        avg = lambda col: grp[col].astype(float).mean()
        avg_ctrl = avg("controls")
        avg_pts = avg("points")
        pc = (avg_pts / avg_ctrl) if avg_ctrl else 0
        pct2 = (avg("two_pt_made") / avg("two_pt_attempt") * 100) if avg("two_pt_attempt") else 0
        pct3 = (avg("three_pt_made") / avg("three_pt_attempt") * 100) if avg("three_pt_attempt") else 0
        pctf = (avg("fta_made") / avg("fta_attempt") * 100) if avg("fta_attempt") else 0
        results.append({
            "tournament": tournament,
            "games": n,
            "avg_2pta": round(avg("two_pt_made"), 1),
            "avg_2ptm": round(avg("two_pt_attempt"), 1),
            "avg_3pta": round(avg("three_pt_made"), 1),
            "avg_3ptm": round(avg("three_pt_attempt"), 1),
            "avg_fta": round(avg("fta_made"), 1),
            "avg_ftm": round(avg("fta_attempt"), 1),
            "avg_or": round(avg("off_rebound"), 1),
            "avg_to": round(avg("turnovers"), 1),
            "avg_controls": round(avg_ctrl, 2),
            "avg_points": round(avg_pts, 1),
            "pc": round(pc, 2),
            "pct_2pt": round(pct2, 1),
            "pct_3pt": round(pct3, 1),
            "pct_ft": round(pctf, 1),
        })
    return sorted(results, key=lambda x: x["tournament"])


# ---------------------------------------------------------------------------
# Enriched dataframe + Predict calculations
# ---------------------------------------------------------------------------

def _enrich_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Tuple[float, float]]]:
    """Add index / time / match_weight / weighted-stat columns.

    Returns ``(enriched_df, tournament_averages)``.
    """
    if df.empty:
        return df, {}

    df = df.copy()
    df["tournament_key"] = df["tournament"].fillna("").map(normalize_key)
    df["team_key"] = df["team"].fillna("").map(normalize_key)
    df["opponent_key"] = df["opponent"].fillna("").map(normalize_key)

    # Pair diff
    diffs = []
    for i in range(0, len(df), 2):
        if i + 1 < len(df):
            d = (df.iloc[i]["points"] or 0) - (df.iloc[i + 1]["points"] or 0)
            diffs.extend([d, d])
        else:
            diffs.append(0)
    df["pair_diff"] = diffs

    # Tournament averages
    tour_stats = df.groupby("tournament_key", dropna=True).agg(
        avg_controls=("controls", "mean"),
        avg_points=("points", "mean"),
    )
    tournament_avg: Dict[str, Tuple[float, float]] = {}
    for t, row in tour_stats.iterrows():
        ac = round(float(row.get("avg_controls") or 0), 2)
        ap = round(float(row.get("avg_points") or 0), 2)
        aa = round((ap / ac) if ac else 0.0, 2)
        tournament_avg[str(t)] = (ac, aa)

    # Index
    def compute_index(row):
        tk = str(row.get("tournament_key") or "")
        status = str(row.get("status") or "").upper()
        controls = float(row.get("controls") or 0)
        attak = float(row.get("attak_kef") or 0)
        diff = float(row.get("pair_diff") or 0)
        idx = 10.0
        if status == "OT":
            idx -= 5.0
        if status == "FS":
            idx -= 3.0
        ac, aa = tournament_avg.get(tk, (0.0, 0.0))
        if ac > 0:
            if controls < ac * 0.9:
                idx -= 1.0
            if controls > ac * 1.1:
                idx -= 1.0
        else:
            if controls < 72.27:
                idx -= 1.0
            if controls > 88.33:
                idx -= 1.0
        if aa > 0:
            if attak > aa * 1.25:
                idx -= 2.0
            if attak < aa * 0.75:
                idx -= 2.0
        else:
            if attak > 1.237:
                idx -= 2.0
            if attak < 0.742:
                idx -= 2.0
        if diff > 25:
            idx -= 2.0
        if diff < -25:
            idx -= 2.0
        return max(idx, 0.0)

    df["index"] = df.apply(compute_index, axis=1)

    # Time
    today = datetime.now().date()

    def parse_date(ds: str):
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
            try:
                return datetime.strptime(ds.strip().split(" ")[0], fmt)
            except Exception:
                continue
        return None

    def compute_time(row):
        ds = str(row.get("date") or "").strip()
        if not ds:
            return 10.0
        d = parse_date(ds)
        return ((today - d.date()).days / 7.0) if d else 10.0

    df["time"] = df.apply(compute_time, axis=1)
    df["match_weight"] = ((df["index"] * 40.0) / (df["time"] + 10.0)).round(7)

    for src, dst in [
        ("two_pt_made", "x_2pt_made"), ("two_pt_attempt", "x_2pt_att"),
        ("three_pt_made", "x_3pt_made"), ("three_pt_attempt", "x_3pt_att"),
        ("fta_made", "x_fta_made"), ("fta_attempt", "x_fta_att"),
        ("off_rebound", "x_or"), ("turnovers", "x_to"),
        ("controls", "x_controls"), ("points", "x_points"),
        ("attak_kef", "x_attak"),
    ]:
        df[dst] = (df[src].astype(float) * df["match_weight"]).round(7)

    return df, tournament_avg


def _build_aggregates_for_tournament(df: pd.DataFrame, tournament: str) -> Dict[str, Dict[str, Tuple[float, float]]]:
    """Build O/L aggregates restricted to a single tournament."""
    tour_key = normalize_key(tournament)
    df_t = df[df["tournament_key"] == tour_key]
    if df_t.empty:
        return {"team": {}, "opponent": {}, "tournament": {}}
    cache: Dict[str, Dict[str, Tuple[float, float]]] = {"team": {}, "opponent": {}, "tournament": {}}
    for col, key_col in [("team", "team_key"), ("opponent", "opponent_key"), ("tournament", "tournament_key")]:
        for key, subset in df_t.groupby(key_col, dropna=True):
            if not key:
                continue
            sw = subset["match_weight"].sum()
            if sw <= 0:
                cache[col][str(key)] = (0.0, 0.0)
                continue
            a2m = subset["x_2pt_made"].sum() / sw
            a2a = subset["x_2pt_att"].sum() / sw
            a3m = subset["x_3pt_made"].sum() / sw
            a3a = subset["x_3pt_att"].sum() / sw
            afm = subset["x_fta_made"].sum() / sw
            afa = subset["x_fta_att"].sum() / sw
            aor = subset["x_or"].sum() / sw
            ato = subset["x_to"].sum() / sw
            controls = a2a + a3a + (afa / 2.0) + ato - (aor / 2.0)
            points = (a2m * 2.0) + (a3m * 3.0) + afm
            if controls <= 0:
                cache[col][str(key)] = (0.0, 0.0)
            else:
                cache[col][str(key)] = (points / controls, controls)
    return cache


def compute_predict(tournament: str, team1: str, team2: str) -> dict:
    """Compute predict, temp, IT1, IT2 for a match.

    Calculation is restricted to the given tournament only.
    Returns dict with keys: predict, temp, it1, it2.
    """
    df = get_dataframe()
    enriched, _ = _enrich_dataframe(df)
    agg = _build_aggregates_for_tournament(enriched, tournament)

    def get_agg(col: str, value: str) -> Tuple[float, float]:
        key = normalize_key(value)
        return agg.get(col, {}).get(key, (0.0, 0.0))

    o_t1, l_t1 = get_agg("team", team1)
    o_t2, l_t2 = get_agg("team", team2)
    o_op1, l_op1 = get_agg("opponent", team1)
    o_op2, l_op2 = get_agg("opponent", team2)
    o_tour, l_tour = get_agg("tournament", tournament)

    temp = ((l_t1 + l_t2 + l_op1 + l_op2) / 2.0) - l_tour
    it1 = temp * (o_t1 + o_op2 - o_tour) + 2.0
    it2 = temp * (o_t2 + o_op1 - o_tour) - 2.0
    predict = it1 + it2

    return {
        "predict": round(predict, 1),
        "temp": round(temp, 1),
        "it1": round(it1, 1),
        "it2": round(it2, 1),
    }


def compute_live_match(match: dict) -> dict:
    """Full computation for a single live match row."""
    tournament = match.get("tournament", "")
    team1 = match.get("team1", "")
    team2 = match.get("team2", "")
    total = match.get("total")
    calc_temp = float(match.get("calc_temp") or 0)

    pred = compute_predict(tournament, team1, team2)
    predict = pred["predict"]
    temp = pred["temp"]

    if total is None or total == "" or total == 0:
        total = predict
    else:
        total = float(total)

    under = round(total - predict, 1) if (total - predict) > 3 else None
    over = round(predict - total, 1) if (total - predict) < -3 else None

    t2h = 0.0
    if temp != 0:
        z = total / (2.0 * temp)
        t2h = round(z * ((temp + calc_temp) / 2.0), 1)

    return {
        **match,
        "total": total,
        "temp": temp,
        "predict": predict,
        "it1": pred["it1"],
        "it2": pred["it2"],
        "under": under,
        "over": over,
        "t2h": t2h,
    }


def compute_all_live() -> List[dict]:
    """Load all saved live matches and compute predictions for each."""
    matches = load_live_matches()
    return [compute_live_match(m) for m in matches]


# ---------------------------------------------------------------------------
# TSV parsing for import (matches into cyber_matches)
# ---------------------------------------------------------------------------

def parse_import_tsv(raw_text: str) -> Tuple[List[dict], List[str]]:
    """Parse TSV text into rows suitable for ``add_rows``.

    Returns ``(parsed_rows, error_lines)``.
    """
    parsed = []
    errors = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        cells = [c.strip() for c in line.split("\t")]
        if len(cells) < 14:
            errors.append(line)
            continue
        try:
            row = _parse_cells(cells)
            if row:
                parsed.append(row)
            else:
                errors.append(line)
        except Exception:
            errors.append(line)
    return parsed, errors


def _parse_cells(cells: List[str]) -> Optional[dict]:
    """Parse a single row of TSV cells into a dict."""

    def try_float(v):
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return None

    # First cell is always date
    date_str = cells[0]

    # Last cell might be status (FS/OT) or attak_kef
    status = ""
    attak_kef = None
    remaining = cells[1:]

    # Try to find status at the end
    if remaining and str(remaining[-1]).upper() in ("FS", "OT"):
        status = remaining[-1].upper()
        remaining = remaining[:-1]

    # Now from the right: attak_kef, opponent, points, controls, then 8 numeric stats
    # Minimum: tournament, team, H/A, 8 numbers, controls, points, opponent, attak_kef = 14 fields
    # Try to identify the boundary between text and numbers
    # Working from right: attak_kef(float), opponent(text), points(float), controls(float),
    # turnovers(float), off_rebound(float), fta_attempt(float), fta_made(float),
    # 3pt_attempt(float), 3pt_made(float), 2pt_attempt(float), 2pt_made(float),
    # home_away(H/A), team(text), tournament(text)

    if len(remaining) < 13:
        return None

    # The attak_kef is always the last remaining number
    attak_kef = try_float(remaining[-1])
    if attak_kef is None:
        return None
    opponent = str(remaining[-2])
    points = try_float(remaining[-3])
    controls = try_float(remaining[-4])
    turnovers = try_float(remaining[-5])
    off_rebound = try_float(remaining[-6])
    fta_attempt = try_float(remaining[-7])
    fta_made = try_float(remaining[-8])
    three_pt_att = try_float(remaining[-9])
    three_pt_made = try_float(remaining[-10])
    two_pt_att = try_float(remaining[-11])
    two_pt_made = try_float(remaining[-12])

    if any(v is None for v in [points, controls, turnovers, off_rebound,
                                fta_attempt, fta_made, three_pt_att,
                                three_pt_made, two_pt_att, two_pt_made]):
        return None

    # What's left: tournament, team, home_away
    prefix = remaining[:-12]
    if len(prefix) < 2:
        return None

    # home_away is always H or A
    home_away = ""
    for i, v in enumerate(prefix):
        if str(v).upper() in ("H", "A"):
            home_away = str(v).upper()
            tournament = " ".join(prefix[:i - 1]) if i > 1 else prefix[0] if prefix else ""
            team = prefix[i - 1] if i >= 1 else ""
            # If tournament is empty, try to figure it out
            if not tournament and i >= 2:
                tournament = " ".join(prefix[:i - 1])
            elif not tournament:
                tournament = ""
            break
    else:
        # H/A not found — fallback: first token is tournament, second is team
        tournament = prefix[0]
        team = " ".join(prefix[1:])
        home_away = ""

    return {
        "date": date_str,
        "tournament": tournament,
        "team": team,
        "home_away": home_away,
        "two_pt_made": two_pt_made,
        "two_pt_attempt": two_pt_att,
        "three_pt_made": three_pt_made,
        "three_pt_attempt": three_pt_att,
        "fta_made": fta_made,
        "fta_attempt": fta_attempt,
        "off_rebound": off_rebound,
        "turnovers": turnovers,
        "controls": controls,
        "points": points,
        "opponent": opponent,
        "attak_kef": attak_kef,
        "status": status,
    }
