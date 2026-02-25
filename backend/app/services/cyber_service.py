"""Business logic for Cyber sections (Cybers Bases + Cyber LIVE)."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.app.database.connection import get_cyber_connection
from db_connection import adapt_sql, is_postgres


CYBER_COLUMNS = [
    "date",
    "tournament",
    "team",
    "home_away",
    "two_pt_made",
    "two_pt_attempt",
    "three_pt_made",
    "three_pt_attempt",
    "fta_made",
    "fta_attempt",
    "off_rebound",
    "turnovers",
    "controls",
    "points",
    "opponent",
    "attak_kef",
    "status",
]


def _rows_to_dicts(cursor) -> List[dict]:
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def normalize_key(value: str) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def _to_float(value: str) -> float:
    v = str(value).strip().replace(",", ".")
    if not v:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def _normalize_date(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if " " in s:
        s = s.split(" ")[0]
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%d.%m.%Y")
        except Exception:
            continue
    cleaned = s.replace("/", ".").replace("-", ".").replace("\\", ".").replace(" ", "")
    parts = [p for p in cleaned.split(".") if p]
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        a, b, c = parts
        try:
            if len(a) == 4:
                y, m, d = int(a), int(b), int(c)
            else:
                y = int(c)
                if len(c) == 2:
                    y = 2000 + y if y <= 69 else 1900 + y
                x, z = int(a), int(b)
                if x > 12 and z <= 12:
                    d, m = x, z
                elif z > 12 and x <= 12:
                    d, m = z, x
                else:
                    m, d = x, z
            return datetime(y, m, d).strftime("%d.%m.%Y")
        except Exception:
            return s
    return s


def _get_df(tournament: Optional[str] = None) -> pd.DataFrame:
    with get_cyber_connection() as conn:
        raw = conn._conn if hasattr(conn, "_conn") else conn
        if tournament:
            sql = """
            SELECT * FROM cyber_matches
            WHERE tournament = ?
               OR lower(trim(tournament)) = lower(trim(?))
            ORDER BY id ASC
            """
            if is_postgres():
                sql = adapt_sql(sql)
            return pd.read_sql_query(sql, raw, params=(tournament, tournament))
        return pd.read_sql_query("SELECT * FROM cyber_matches ORDER BY id ASC", raw)


def get_matches(tournament: Optional[str] = None, limit: int = 10000) -> List[dict]:
    query = "SELECT * FROM cyber_matches"
    params: list = []
    if tournament:
        query += " WHERE tournament = ?"
        params.append(tournament)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = _rows_to_dicts(cur)
        for row in rows:
            row["date"] = _normalize_date(row.get("date"))
        return rows


def get_tournaments() -> List[str]:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT tournament FROM cyber_matches WHERE tournament IS NOT NULL AND tournament <> '' ORDER BY tournament"
        )
        return [r[0] for r in cur.fetchall()]


def get_statistics() -> dict:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cyber_matches")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT tournament) FROM cyber_matches")
        tournaments = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT team) FROM cyber_matches")
        teams = cur.fetchone()[0]
    return {"total_records": total, "tournaments_count": tournaments, "teams_count": teams}


def import_matches(raw_text: str) -> Tuple[int, int, List[str]]:
    lines = [ln for ln in raw_text.splitlines() if ln.strip()]
    parsed_rows: List[tuple] = []
    skipped_lines: List[str] = []

    normalized: List[List[str]] = []
    for line in lines:
        cells = [c.strip() for c in line.split("\t")]
        if len(cells) == 16:
            cells = cells + [""]
        if len(cells) < 17:
            skipped_lines.append(line)
            continue
        normalized.append(cells[:17])

    i = 0
    while i < len(normalized):
        # Keep pair semantics as in desktop: one match = 2 rows
        if i + 1 >= len(normalized):
            skipped_lines.append("\t".join(normalized[i]))
            break

        r1 = normalized[i]
        r2 = normalized[i + 1]
        t1 = str(r1[1]).strip()
        t2 = str(r2[1]).strip()
        ha1 = str(r1[3]).strip().upper()
        ha2 = str(r2[3]).strip().upper()
        if t1 and t2 and t1 != t2:
            skipped_lines.extend(["\t".join(r1), "\t".join(r2)])
            i += 2
            continue
        if (ha1 and ha2) and not (ha1 == "H" and ha2 == "A"):
            skipped_lines.extend(["\t".join(r1), "\t".join(r2)])
            i += 2
            continue

        for cells in (r1, r2):
            parsed_rows.append(
                (
                    _normalize_date(cells[0]),
                    cells[1],
                    cells[2],
                    cells[3],
                    _to_float(cells[4]),
                    _to_float(cells[5]),
                    _to_float(cells[6]),
                    _to_float(cells[7]),
                    _to_float(cells[8]),
                    _to_float(cells[9]),
                    _to_float(cells[10]),
                    _to_float(cells[11]),
                    _to_float(cells[12]),
                    _to_float(cells[13]),
                    cells[14],
                    _to_float(cells[15]),
                    cells[16],
                )
            )
        i += 2

    if parsed_rows:
        with get_cyber_connection() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO cyber_matches (
                    date, tournament, team, home_away,
                    two_pt_made, two_pt_attempt, three_pt_made, three_pt_attempt,
                    fta_made, fta_attempt, off_rebound, turnovers,
                    controls, points, opponent, attak_kef, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                parsed_rows,
            )
            conn.commit()

    return len(parsed_rows), len(skipped_lines), skipped_lines[:200]


def clear_matches() -> None:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_matches")
        conn.commit()


def delete_matches(ids: List[int]) -> int:
    if not ids:
        return 0
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM cyber_matches WHERE id = ?", [(i,) for i in ids])
        conn.commit()
        return cur.rowcount


def update_match_field(row_id: int, field: str, value) -> bool:
    if field not in CYBER_COLUMNS:
        return False
    prepared = value
    if field == "date":
        prepared = _normalize_date(value)
    elif field in {"two_pt_made", "two_pt_attempt", "three_pt_made", "three_pt_attempt", "fta_made", "fta_attempt", "off_rebound", "turnovers", "controls", "points", "attak_kef"}:
        prepared = _to_float(value)
    elif field in {"tournament", "team", "home_away", "opponent", "status"}:
        prepared = str(value or "").strip()
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE cyber_matches SET {field} = ? WHERE id = ?", (prepared, row_id))
        conn.commit()
        return cur.rowcount > 0


def normalize_existing_dates() -> int:
    updated = 0
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, date FROM cyber_matches")
        rows = cur.fetchall()
        updates = []
        for row_id, value in rows:
            norm = _normalize_date(value)
            if norm and norm != (value or ""):
                updates.append((norm, row_id))
        if updates:
            cur.executemany("UPDATE cyber_matches SET date = ? WHERE id = ?", updates)
            conn.commit()
            updated = len(updates)
    return updated


def replace_values(old_value: str, new_value: str, scope: str = "all", tournament: Optional[str] = None) -> int:
    old_text = str(old_value or "")
    if not old_text:
        return 0
    scope_mode = (scope or "all").strip().lower()
    fields = (
        "date",
        "tournament",
        "team",
        "home_away",
        "two_pt_made",
        "two_pt_attempt",
        "three_pt_made",
        "three_pt_attempt",
        "fta_made",
        "fta_attempt",
        "off_rebound",
        "turnovers",
        "controls",
        "points",
        "opponent",
        "attak_kef",
        "status",
    )

    def _prepare(field: str, text: str):
        if field == "date":
            return _normalize_date(text)
        if field in {"tournament", "team", "home_away", "opponent", "status"}:
            return str(text).strip()
        return _to_float(text)

    with get_cyber_connection() as conn:
        cur = conn.cursor()
        if scope_mode == "tournament" and tournament:
            cur.execute(
                """
                SELECT id, date, tournament, team, home_away,
                       two_pt_made, two_pt_attempt, three_pt_made, three_pt_attempt,
                       fta_made, fta_attempt, off_rebound, turnovers, controls, points,
                       opponent, attak_kef, status
                FROM cyber_matches
                WHERE tournament = ?
                """,
                (tournament,),
            )
        else:
            cur.execute(
                """
                SELECT id, date, tournament, team, home_away,
                       two_pt_made, two_pt_attempt, three_pt_made, three_pt_attempt,
                       fta_made, fta_attempt, off_rebound, turnovers, controls, points,
                       opponent, attak_kef, status
                FROM cyber_matches
                """
            )

        rows = cur.fetchall()
        updates_by_field: Dict[str, List[Tuple[object, int]]] = {field: [] for field in fields}
        replaced = 0
        for row in rows:
            row_id = row[0]
            values = {
                "date": row[1],
                "tournament": row[2],
                "team": row[3],
                "home_away": row[4],
                "two_pt_made": row[5],
                "two_pt_attempt": row[6],
                "three_pt_made": row[7],
                "three_pt_attempt": row[8],
                "fta_made": row[9],
                "fta_attempt": row[10],
                "off_rebound": row[11],
                "turnovers": row[12],
                "controls": row[13],
                "points": row[14],
                "opponent": row[15],
                "attak_kef": row[16],
                "status": row[17],
            }
            for field in fields:
                current = values.get(field)
                current_text = "" if current is None else str(current)
                if old_text not in current_text:
                    continue
                replaced_text = current_text.replace(old_text, str(new_value))
                if replaced_text == current_text:
                    continue
                updated = _prepare(field, replaced_text)
                updates_by_field[field].append((updated, row_id))
                replaced += 1

        for field, payload in updates_by_field.items():
            if payload:
                cur.executemany(f"UPDATE cyber_matches SET {field} = ? WHERE id = ?", payload)
        conn.commit()
    return replaced


def merge_tournaments(source_tournaments: List[str], target_tournament: str) -> int:
    target = str(target_tournament or "").strip()
    if not target:
        return 0
    sources = []
    for value in source_tournaments or []:
        name = str(value or "").strip()
        if name and name not in sources and name != target:
            sources.append(name)
    if not sources:
        return 0

    updated = 0
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        for src in sources:
            cur.execute(
                "UPDATE cyber_matches SET tournament = ? WHERE tournament = ?",
                (target, src),
            )
            updated += max(cur.rowcount, 0)
        conn.commit()
    return updated


def get_summary(tournament: Optional[str] = None) -> List[dict]:
    df = _get_df(tournament=tournament)
    if df.empty:
        return []

    out = []
    for tour_name, grp in df.groupby("tournament"):
        games = int(len(grp) // 2) if len(grp) > 1 else int(len(grp))
        two_pt_att = float(grp["two_pt_attempt"].mean() or 0)
        two_pt_made = float(grp["two_pt_made"].mean() or 0)
        three_pt_att = float(grp["three_pt_attempt"].mean() or 0)
        three_pt_made = float(grp["three_pt_made"].mean() or 0)
        fta_att = float(grp["fta_attempt"].mean() or 0)
        fta_made = float(grp["fta_made"].mean() or 0)
        off_rebound = float(grp["off_rebound"].mean() or 0)
        turnovers = float(grp["turnovers"].mean() or 0)
        controls = float(grp["controls"].mean() or 0)
        points = float(grp["points"].mean() or 0)
        p_per_control = (points / controls) if controls else 0.0
        two_pt_pct = ((two_pt_made / two_pt_att) * 100) if two_pt_att else 0.0
        three_pt_pct = ((three_pt_made / three_pt_att) * 100) if three_pt_att else 0.0
        ft_pct = ((fta_made / fta_att) * 100) if fta_att else 0.0
        out.append(
            {
                "tournament": str(tour_name or ""),
                "games": games,
                "two_pt_attempt": round(two_pt_att, 2),
                "two_pt_made": round(two_pt_made, 2),
                "three_pt_attempt": round(three_pt_att, 2),
                "three_pt_made": round(three_pt_made, 2),
                "fta_attempt": round(fta_att, 2),
                "fta_made": round(fta_made, 2),
                "off_rebound": round(off_rebound, 2),
                "turnovers": round(turnovers, 2),
                "controls": round(controls, 2),
                "points": round(points, 2),
                "p_per_control": round(p_per_control, 3),
                "two_pt_pct": round(two_pt_pct, 2),
                "three_pt_pct": round(three_pt_pct, 2),
                "ft_pct": round(ft_pct, 2),
            }
        )
    return sorted(out, key=lambda x: x["tournament"])


def _enriched_df_for_predict(tournament: str) -> pd.DataFrame:
    tournament_key = normalize_key(tournament)
    df = _get_df(tournament=tournament)
    if df.empty:
        # Fallback to desktop-like behavior: normalize and match tournament key.
        df_all = _get_df()
        if df_all.empty:
            return df_all
        df_all = df_all.copy()
        df_all["tournament_key"] = df_all["tournament"].fillna("").map(normalize_key)
        return df_all[df_all["tournament_key"] == tournament_key].copy()

    df = df.copy()
    df["tournament_key"] = df["tournament"].fillna("").map(normalize_key)
    df = df[df["tournament_key"] == tournament_key].copy()
    if df.empty:
        return df
    df["team_key"] = df["team"].fillna("").map(normalize_key)
    df["opponent_key"] = df["opponent"].fillna("").map(normalize_key)

    diffs = []
    for i in range(0, len(df), 2):
        if i + 1 < len(df):
            diff = (float(df.iloc[i]["points"] or 0)) - (float(df.iloc[i + 1]["points"] or 0))
            diffs.extend([diff, diff])
        else:
            diffs.append(0.0)
    df["pair_diff"] = diffs

    tour_stats = df.groupby("tournament_key", dropna=True).agg(
        avg_controls=("controls", "mean"),
        avg_points=("points", "mean"),
    )
    tournament_avg: Dict[str, Tuple[float, float]] = {}
    for t, row in tour_stats.iterrows():
        avg_controls = float(row.get("avg_controls") or 0)
        avg_points = float(row.get("avg_points") or 0)
        attack_avg = (avg_points / avg_controls) if avg_controls else 0.0
        tournament_avg[str(t)] = (round(avg_controls, 2), round(attack_avg, 2))

    team_tournament_avg: Dict[Tuple[str, str], Tuple[float, float]] = {}
    team_tour_stats = df.groupby(["tournament_key", "team_key"], dropna=True).agg(
        avg_controls=("controls", "mean"),
        avg_points=("points", "mean"),
    )
    for (t, team), row in team_tour_stats.iterrows():
        avg_c = float(row.get("avg_controls") or 0)
        avg_p = float(row.get("avg_points") or 0)
        team_tournament_avg[(str(t), str(team))] = (round(avg_c, 2), round(avg_p, 2))

    def parse_date_str(date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        s = date_str.strip()
        candidates = [s, s.split(" ")[0]] if " " in s else [s]
        for cand in candidates:
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
                try:
                    return datetime.strptime(cand, fmt)
                except Exception:
                    continue
        return None

    today = datetime.now().date()

    def compute_index(row):
        t_key = str(row.get("tournament_key") or "")
        status = str(row.get("status") or "").upper()
        controls = float(row.get("controls") or 0)
        attak = float(row.get("attak_kef") or 0)
        diff = float(row.get("pair_diff") or 0)
        idx = 10.0
        if status == "OT":
            idx -= 5.0
        if status == "FS":
            idx -= 3.0

        team_key = str(row.get("team_key") or "")
        opponent_key = str(row.get("opponent_key") or "")
        team_avg = team_tournament_avg.get((t_key, team_key))
        opp_avg = team_tournament_avg.get((t_key, opponent_key))
        if team_avg and opp_avg and team_avg[0] > 0 and opp_avg[0] > 0:
            avg_controls = round((team_avg[0] + opp_avg[0]) / 2, 2)
            avg_points = round((team_avg[1] + opp_avg[1]) / 2, 2)
            attack_avg = round(avg_points / avg_controls, 2) if avg_controls else 0.0
        else:
            avg_controls, attack_avg = tournament_avg.get(t_key, (0.0, 0.0))

        if avg_controls > 0:
            if controls < avg_controls * 0.9:
                idx -= 1.0
            if controls > avg_controls * 1.1:
                idx -= 1.0
        else:
            if controls < 72.27:
                idx -= 1.0
            if controls > 88.33:
                idx -= 1.0

        if attack_avg > 0:
            if attak > attack_avg * 1.25:
                idx -= 2.0
            if attak < attack_avg * 0.75:
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

    def compute_time(row):
        d = parse_date_str(str(row.get("date") or ""))
        if not d:
            return 10.0
        return (today - d.date()).days / 7.0

    df["index"] = df.apply(compute_index, axis=1)
    df["time"] = df.apply(compute_time, axis=1)
    df["match_weight"] = ((df["index"] * 40.0) / (df["time"] + 10.0)).round(7)

    for src_col, dst_col in (
        ("two_pt_made", "x_2pt_made"),
        ("two_pt_attempt", "x_2pt_att"),
        ("three_pt_made", "x_3pt_made"),
        ("three_pt_attempt", "x_3pt_att"),
        ("fta_made", "x_fta_made"),
        ("fta_attempt", "x_fta_att"),
        ("off_rebound", "x_or"),
        ("turnovers", "x_to"),
    ):
        df[dst_col] = (df[src_col].astype(float) * df["match_weight"]).round(7)

    return df


def compute_predict(tournament: str, team1: str, team2: str) -> Tuple[float, float, float, float]:
    df = _enriched_df_for_predict(tournament=tournament)
    if df.empty:
        return 0.0, 0.0, 0.0, 0.0

    agg: Dict[str, Dict[str, Tuple[float, float]]] = {"team": {}, "opponent": {}, "tournament": {}}
    for col, key_col in (("team", "team_key"), ("opponent", "opponent_key"), ("tournament", "tournament_key")):
        grouped = df.groupby(key_col, dropna=True)
        for key, subset in grouped:
            if key is None or key == "":
                continue
            sum_v = float(subset["match_weight"].sum() or 0)
            if sum_v <= 0:
                agg[col][str(key)] = (0.0, 0.0)
                continue
            avg_2pt_made = subset["x_2pt_made"].sum() / sum_v
            avg_2pt_att = subset["x_2pt_att"].sum() / sum_v
            avg_3pt_made = subset["x_3pt_made"].sum() / sum_v
            avg_3pt_att = subset["x_3pt_att"].sum() / sum_v
            avg_fta_made = subset["x_fta_made"].sum() / sum_v
            avg_fta_att = subset["x_fta_att"].sum() / sum_v
            avg_or = subset["x_or"].sum() / sum_v
            avg_to = subset["x_to"].sum() / sum_v
            controls = avg_2pt_att + avg_3pt_att + (avg_fta_att / 2.0) + avg_to - (avg_or / 2.0)
            points = (avg_2pt_made * 2.0) + (avg_3pt_made * 3.0) + avg_fta_made
            agg[col][str(key)] = (0.0, 0.0) if controls <= 0 else (points / controls, controls)

    def get_agg(col: str, value: str) -> Tuple[float, float]:
        key = normalize_key(value)
        return agg.get(col, {}).get(key, (0.0, 0.0))

    o_team1, l_team1 = get_agg("team", team1)
    o_team2, l_team2 = get_agg("team", team2)
    o_opp_team1, l_opp_team1 = get_agg("opponent", team1)
    o_opp_team2, l_opp_team2 = get_agg("opponent", team2)
    o_tour, l_tour = get_agg("tournament", tournament)

    temp = ((l_team1 + l_team2 + l_opp_team1 + l_opp_team2) / 2.0) - l_tour
    it1 = temp * (o_team1 + o_opp_team2 - o_tour) + 2.0
    it2 = temp * (o_team2 + o_opp_team1 - o_tour) - 2.0
    predict = it1 + it2
    return round(predict, 2), round(temp, 2), round(it1, 2), round(it2, 2)


def get_live_rows() -> List[dict]:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, tournament, team1, team2, total, calc_temp FROM cyber_live_matches ORDER BY id ASC"
        )
        return _rows_to_dicts(cur)


def replace_live_rows(rows: List[tuple]) -> None:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_live_matches")
        if rows:
            cur.executemany(
                "INSERT INTO cyber_live_matches (tournament, team1, team2, total, calc_temp) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        conn.commit()


def clear_live_rows() -> None:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_live_matches")
        conn.commit()


def _has_calc_temp(value) -> bool:
    try:
        return abs(float(value)) > 1e-9
    except Exception:
        return False


def archive_live_row(payload: dict) -> dict:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        tournament = payload.get("tournament") or ""
        team1 = payload.get("team1") or ""
        team2 = payload.get("team2") or ""
        incoming_has_calc = _has_calc_temp(payload.get("calc_temp"))
        today_iso = datetime.now().strftime("%Y-%m-%d")

        # Deduplicate same day + same match. Prefer keeping row with CalcTEMP.
        cur.execute(
            """
            SELECT id, calc_temp
            FROM cyber_live_archive
            WHERE DATE(archived_at) = DATE(?)
              AND tournament = ?
              AND team1 = ?
              AND team2 = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (today_iso, tournament, team1, team2),
        )
        existing = cur.fetchone()
        if existing:
            existing_id, existing_calc_temp = existing
            if _has_calc_temp(existing_calc_temp):
                return {
                    "archived": False,
                    "updated_existing": False,
                    "deleted_from_live": 0,
                    "message": "Дубликат за сегодня уже есть (с CalcTEMP) — пропущено",
                }
            if incoming_has_calc:
                cur.execute(
                    """
                    UPDATE cyber_live_archive
                    SET live_row_id = ?, total = ?, calc_temp = ?, temp = ?, predict = ?,
                        under_value = ?, over_value = ?, t2h = ?, t2h_predict = ?
                    WHERE id = ?
                    """,
                    (
                        payload.get("live_row_id"),
                        payload.get("total"),
                        payload.get("calc_temp"),
                        payload.get("temp"),
                        payload.get("predict"),
                        payload.get("under_value"),
                        payload.get("over_value"),
                        payload.get("t2h"),
                        payload.get("t2h_predict"),
                        existing_id,
                    ),
                )
                deleted = 0
                live_row_id = payload.get("live_row_id")
                if live_row_id:
                    cur.execute("DELETE FROM cyber_live_matches WHERE id = ?", (live_row_id,))
                    deleted = cur.rowcount
                conn.commit()
                return {
                    "archived": False,
                    "updated_existing": True,
                    "deleted_from_live": deleted,
                    "message": "Дубликат обновлен значениями с CalcTEMP",
                }
            return {
                "archived": False,
                "updated_existing": False,
                "deleted_from_live": 0,
                "message": "Дубликат за сегодня без CalcTEMP — пропущено",
            }

        if not incoming_has_calc:
            return {
                "archived": False,
                "updated_existing": False,
                "deleted_from_live": 0,
                "message": "Матч без CalcTEMP не архивируется",
            }

        cur.execute(
            """
            INSERT INTO cyber_live_archive (
                live_row_id, tournament, team1, team2, total, calc_temp,
                temp, predict, under_value, over_value, t2h, t2h_predict
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("live_row_id"),
                tournament,
                team1,
                team2,
                payload.get("total"),
                payload.get("calc_temp"),
                payload.get("temp"),
                payload.get("predict"),
                payload.get("under_value"),
                payload.get("over_value"),
                payload.get("t2h"),
                payload.get("t2h_predict"),
            ),
        )
        deleted = 0
        live_row_id = payload.get("live_row_id")
        if live_row_id:
            cur.execute("DELETE FROM cyber_live_matches WHERE id = ?", (live_row_id,))
            deleted = cur.rowcount
        conn.commit()
        return {
            "archived": True,
            "updated_existing": False,
            "deleted_from_live": deleted,
            "message": "Матч отправлен в архив",
        }


def get_live_archive_rows(limit: int = 5000) -> List[dict]:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, live_row_id, tournament, team1, team2, total, calc_temp,
                temp, predict, under_value, over_value, t2h, t2h_predict, archived_at
            FROM cyber_live_archive
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return _rows_to_dicts(cur)


def clear_live_archive() -> int:
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM cyber_live_archive")
        deleted = cur.rowcount
        conn.commit()
        return deleted


def delete_live_archive_rows(ids: List[int]) -> int:
    if not ids:
        return 0
    with get_cyber_connection() as conn:
        cur = conn.cursor()
        cur.executemany("DELETE FROM cyber_live_archive WHERE id = ?", [(i,) for i in ids])
        deleted = cur.rowcount
        conn.commit()
        return deleted
