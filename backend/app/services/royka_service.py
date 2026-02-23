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


def _to_float(value) -> float:
    try:
        if value is None:
            return 0.0
        return float(str(value).replace(",", "."))
    except Exception:
        return 0.0


def _calculate_win_value(prediction: str, result: float, tim: float) -> int:
    if prediction == "OVER":
        if result > tim:
            return 85
        if result < tim:
            return -100
    elif prediction == "UNDER":
        if result < tim:
            return 85
        if result > tim:
            return -100
    return 0


def _calculate_prediction(match_data: dict, difference: float) -> tuple[str, int]:
    t1h = _to_float(match_data.get("t1h", 0))
    t2h = _to_float(match_data.get("t2h", 0))
    tim = _to_float(match_data.get("tim", 0))
    dev = _to_float(match_data.get("deviation", 0))
    kickoff = _to_float(match_data.get("kickoff", 0))
    predict = match_data.get("predict", "")

    initial_diff = t1h + t2h - tim
    if initial_diff >= difference:
        stage1 = "OVER"
    elif initial_diff <= -difference:
        stage1 = "UNDER"
    else:
        return "No bet", 1

    modified_diff = t1h + t2h + dev - tim
    if (stage1 == "OVER" and modified_diff >= difference) or (
        stage1 == "UNDER" and modified_diff <= -difference
    ):
        stage2 = stage1
    else:
        return "No bet", 2

    try:
        predict_value = _to_float(predict) if predict != "" else 0.0
        kickoff_diff = predict_value - kickoff if kickoff != 0 else 0.0
    except Exception:
        return stage2, 2

    if predict_value == 0:
        return stage2, 2
    if -3 < kickoff_diff < 3:
        return stage2, 2

    if stage2 == "UNDER":
        if kickoff_diff < 0:
            return stage2, 2
    else:
        if kickoff_diff > 0:
            return stage2, 2

    if kickoff == 0:
        return stage2, 2

    modified_value = t1h + t2h * (1 + (kickoff_diff / kickoff)) - tim
    if (stage2 == "OVER" and kickoff_diff <= -3 and modified_value >= difference) or (
        stage2 == "UNDER" and kickoff_diff >= 3 and modified_value <= -difference
    ):
        stage3 = stage2
    else:
        return "No bet", 3

    final_value = t1h + t2h * (1 + (kickoff_diff / kickoff)) + dev - tim
    if (stage3 == "OVER" and kickoff_diff <= -3 and final_value >= difference) or (
        stage3 == "UNDER" and kickoff_diff >= 3 and final_value <= -difference
    ):
        return stage3, 4

    return "No bet", 4


def _calculate_prediction_half(match_data: dict, half_threshold: float = 4.5) -> tuple[str, int]:
    t1h = _to_float(match_data.get("t1h", 0))
    t2h = _to_float(match_data.get("t2h", 0))
    tim = _to_float(match_data.get("tim", 0))
    dev = _to_float(match_data.get("deviation", 0))
    kickoff = _to_float(match_data.get("kickoff", 0))
    predict = match_data.get("predict", "")

    initial_diff = t1h + t2h - tim
    if abs(initial_diff) < 0.1:
        return "No bet", 1
    initial_direction = "OVER" if initial_diff >= 0.1 else "UNDER"

    stage2_value = t1h + t2h + dev - tim
    stage2_direction = "OVER" if stage2_value >= 0.1 else "UNDER"
    if stage2_direction != initial_direction:
        return "No bet", 2
    if abs(stage2_value) < half_threshold:
        return "No bet", 2

    try:
        predict_value = _to_float(predict) if predict != "" else 0.0
        kickoff_diff = predict_value - kickoff if kickoff != 0 else 0.0
    except Exception:
        return stage2_direction, 2

    if predict_value == 0 or (-3 < kickoff_diff < 3):
        return stage2_direction, 2
    if kickoff == 0:
        return stage2_direction, 2

    ratio = kickoff_diff / kickoff
    stage3_value = t1h + t2h * (1 + ratio) - tim
    final_value = t1h + t2h * (1 + ratio) + dev - tim

    if stage2_direction == "OVER" and kickoff_diff <= -3 and stage3_value >= 0.1:
        if final_value >= half_threshold:
            return "OVER", 4
        if final_value <= -half_threshold:
            return "UNDER", 4
        return stage2_direction, 2

    if stage2_direction == "UNDER" and kickoff_diff >= 3 and stage3_value <= -0.1:
        if final_value >= half_threshold:
            return "OVER", 4
        if final_value <= -half_threshold:
            return "UNDER", 4
        return stage2_direction, 2

    return stage2_direction, 2


def _calculate_prediction_half_change(match_data: dict, half_threshold: float = 4.5) -> tuple[str, int]:
    t1h = _to_float(match_data.get("t1h", 0))
    t2h = _to_float(match_data.get("t2h", 0))
    tim = _to_float(match_data.get("tim", 0))
    dev = _to_float(match_data.get("deviation", 0))
    kickoff = _to_float(match_data.get("kickoff", 0))
    predict = match_data.get("predict", "")

    initial_diff = t1h + t2h - tim
    if abs(initial_diff) < 0.1:
        return "No bet", 1

    stage2_value = t1h + t2h + dev - tim
    if abs(stage2_value) < half_threshold:
        return "No bet", 2
    stage2_direction = "OVER" if stage2_value >= 0.1 else "UNDER"

    try:
        predict_value = _to_float(predict) if predict != "" else 0.0
        kickoff_diff = predict_value - kickoff if kickoff != 0 else 0.0
    except Exception:
        return stage2_direction, 2

    if predict_value == 0 or (-3 < kickoff_diff < 3):
        return stage2_direction, 2
    if kickoff == 0:
        return stage2_direction, 2

    ratio = kickoff_diff / kickoff
    stage3_value = t1h + t2h * (1 + ratio) - tim
    final_value = t1h + t2h * (1 + ratio) + dev - tim

    if stage2_direction == "OVER" and kickoff_diff <= -3 and stage3_value >= 0.1:
        if final_value >= half_threshold:
            return "OVER", 4
        if final_value <= -half_threshold:
            return "UNDER", 4
        return stage2_direction, 2

    if stage2_direction == "UNDER" and kickoff_diff >= 3 and stage3_value <= -0.1:
        if final_value >= half_threshold:
            return "OVER", 4
        if final_value <= -half_threshold:
            return "UNDER", 4
        return stage2_direction, 2

    return stage2_direction, 2


def _calculate_prediction_half_ncaa(match_data: dict, half_threshold: float = 4.5) -> tuple[str, int]:
    t1h = _to_float(match_data.get("t1h", 0))
    t2h = _to_float(match_data.get("t2h", 0))
    tim = _to_float(match_data.get("tim", 0))
    dev = _to_float(match_data.get("deviation", 0))
    kickoff = _to_float(match_data.get("kickoff", 0))
    predict = match_data.get("predict", "")

    initial_diff = t1h + t2h - tim
    if initial_diff >= 0.1:
        stage1 = "OVER"
    elif initial_diff <= -0.1:
        stage1 = "UNDER"
    else:
        return "No bet", 1

    stage2_value = t1h + t2h + dev - tim
    if abs(stage2_value) < 0.1:
        return "No bet", 2

    try:
        predict_value = _to_float(predict) if predict != "" else 0.0
        kickoff_diff = predict_value - kickoff if kickoff != 0 else 0.0
    except Exception:
        if stage2_value >= half_threshold:
            return "OVER", 2
        if stage2_value <= -half_threshold:
            return "UNDER", 2
        return "No bet", 2

    if predict_value == 0 or (-3 < kickoff_diff < 3):
        if stage2_value >= half_threshold:
            return "OVER", 2
        if stage2_value <= -half_threshold:
            return "UNDER", 2
        return "No bet", 2

    if stage2_value > 0:
        if kickoff_diff > 0:
            return ("OVER", 2) if stage2_value >= half_threshold else ("No bet", 2)
        if kickoff != 0:
            ratio = kickoff_diff / kickoff
            stage4_value = t1h + t2h * (1 + ratio) + dev - tim
            if kickoff_diff <= -3 and stage4_value >= 0.1 and stage4_value >= half_threshold:
                return "OVER", 4
            return "No bet", 3
        return ("OVER", 2) if stage2_value >= half_threshold else ("No bet", 2)

    if kickoff_diff < 0:
        return ("UNDER", 2) if stage2_value <= -half_threshold else ("No bet", 2)
    if kickoff != 0:
        ratio = kickoff_diff / kickoff
        stage4_value = t1h + t2h * (1 + ratio) + dev - tim
        if kickoff_diff >= 3 and stage4_value <= -0.1 and stage4_value <= -half_threshold:
            return "UNDER", 4
        return "No bet", 3
    return ("UNDER", 2) if stage2_value <= -half_threshold else ("No bet", 2)


def _get_match_dicts(tournament: str) -> List[dict]:
    df = _get_royka_df(tournament)
    if df.empty:
        return []
    return df.fillna(0).to_dict("records")


def analyze_tournament_differences(tournament: str) -> List[dict]:
    matches = _get_match_dicts(tournament)
    if not matches:
        return []
    differences = [0.1] + [x / 2 for x in range(1, 21)]
    stats = {
        diff: {
            "ОБЩЕЕ": {"кол-во": 0, "WIN": 0, "%": 0},
            "OVER": {"кол-во": 0, "WIN": 0, "%": 0},
            "UNDER": {"кол-во": 0, "WIN": 0, "%": 0},
        }
        for diff in differences
    }

    for match in matches:
        tim = _to_float(match.get("tim", 0))
        result = _to_float(match.get("result", 0))
        for diff in differences:
            prediction, _ = _calculate_prediction(match, diff)
            if prediction not in ("OVER", "UNDER"):
                continue
            win_value = _calculate_win_value(prediction, result, tim)
            stats[diff][prediction]["кол-во"] += 1
            stats[diff][prediction]["WIN"] += win_value
            stats[diff]["ОБЩЕЕ"]["кол-во"] += 1
            stats[diff]["ОБЩЕЕ"]["WIN"] += win_value

    rows = []
    for diff in differences:
        for category in ("ОБЩЕЕ", "OVER", "UNDER"):
            count = stats[diff][category]["кол-во"]
            if count > 0:
                stats[diff][category]["%"] = stats[diff][category]["WIN"] / (count * 100)
        rows.append(
            {
                "difference": diff,
                "overall_count": stats[diff]["ОБЩЕЕ"]["кол-во"],
                "overall_win": stats[diff]["ОБЩЕЕ"]["WIN"],
                "overall_roi": round(stats[diff]["ОБЩЕЕ"]["%"] * 100, 2),
                "over_count": stats[diff]["OVER"]["кол-во"],
                "over_win": stats[diff]["OVER"]["WIN"],
                "over_roi": round(stats[diff]["OVER"]["%"] * 100, 2),
                "under_count": stats[diff]["UNDER"]["кол-во"],
                "under_win": stats[diff]["UNDER"]["WIN"],
                "under_roi": round(stats[diff]["UNDER"]["%"] * 100, 2),
            }
        )
    return rows


def analyze_tournament_ranges(tournament: str) -> List[dict]:
    matches = _get_match_dicts(tournament)
    if not matches:
        return []
    boundaries = [0.1] + [0.5 + i * 0.5 for i in range(0, 20)]
    ranges = [(boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1)]
    stats = {
        r: {
            "ОБЩЕЕ": {"кол-во": 0, "WIN": 0, "%": 0},
            "OVER": {"кол-во": 0, "WIN": 0, "%": 0},
            "UNDER": {"кол-во": 0, "WIN": 0, "%": 0},
        }
        for r in ranges
    }

    for match in matches:
        t1h_val = _to_float(match.get("t1h", 0))
        t2h_val = _to_float(match.get("t2h", 0))
        tim_val = _to_float(match.get("tim", 0))
        dev_val = _to_float(match.get("deviation", 0))
        kickoff_val = _to_float(match.get("kickoff", 0))
        predict_val = _to_float(match.get("predict", 0))
        res_val = _to_float(match.get("result", 0))

        initial_diff = t1h_val + t2h_val - tim_val
        if abs(initial_diff) < 0.1:
            continue
        base_pred = "OVER" if initial_diff >= 0.1 else "UNDER"

        modified_diff = initial_diff + dev_val
        if (base_pred == "OVER" and modified_diff < 0.1) or (base_pred == "UNDER" and modified_diff > -0.1):
            continue

        if base_pred == "UNDER":
            stage2_effective_diff = initial_diff if dev_val <= 0 else modified_diff
        else:
            stage2_effective_diff = initial_diff if dev_val >= 0 else modified_diff
        stage2_pred = base_pred

        kickoff_diff = predict_val - kickoff_val if kickoff_val != 0 else 0
        if predict_val == 0 or (-3 < kickoff_diff < 3):
            final_diff = stage2_effective_diff
            final_pred = stage2_pred
        else:
            if stage2_pred == "UNDER":
                if kickoff_diff < 0:
                    final_diff = stage2_effective_diff
                    final_pred = stage2_pred
                else:
                    if kickoff_val == 0:
                        final_diff = stage2_effective_diff
                        final_pred = stage2_pred
                    else:
                        ratio = kickoff_diff / kickoff_val
                        modified_value = t1h_val + t2h_val * (1 + ratio) - tim_val
                        if not (kickoff_diff >= 3 and modified_value <= -0.1):
                            continue
                        final_value = modified_value + dev_val
                        if final_value <= -0.1:
                            final_diff = final_value if dev_val > 0 else modified_value
                            final_pred = stage2_pred
                        else:
                            continue
            else:
                if kickoff_diff > 0:
                    final_diff = stage2_effective_diff
                    final_pred = stage2_pred
                else:
                    if kickoff_val == 0:
                        final_diff = stage2_effective_diff
                        final_pred = stage2_pred
                    else:
                        ratio = kickoff_diff / kickoff_val
                        modified_value = t1h_val + t2h_val * (1 + ratio) - tim_val
                        if not (kickoff_diff <= -3 and modified_value >= 0.1):
                            continue
                        final_value = modified_value + dev_val
                        if final_value >= 0.1:
                            final_diff = final_value if dev_val < 0 else modified_value
                            final_pred = stage2_pred
                        else:
                            continue

        abs_diff = abs(final_diff)
        selected_range = None
        for r_low, r_high in ranges:
            if r_low <= abs_diff < r_high:
                selected_range = (r_low, r_high)
                break
        if selected_range is None:
            continue

        win_value = _calculate_win_value(final_pred, res_val, tim_val)
        stats[selected_range][final_pred]["кол-во"] += 1
        stats[selected_range][final_pred]["WIN"] += win_value
        stats[selected_range]["ОБЩЕЕ"]["кол-во"] += 1
        stats[selected_range]["ОБЩЕЕ"]["WIN"] += win_value

    rows = []
    for r in ranges:
        for category in ("ОБЩЕЕ", "OVER", "UNDER"):
            cnt = stats[r][category]["кол-во"]
            if cnt > 0:
                stats[r][category]["%"] = stats[r][category]["WIN"] / (cnt * 100)
        rows.append(
            {
                "range": f"{r[0]}-{r[1]}",
                "overall_count": stats[r]["ОБЩЕЕ"]["кол-во"],
                "overall_win": stats[r]["ОБЩЕЕ"]["WIN"],
                "overall_roi": round(stats[r]["ОБЩЕЕ"]["%"] * 100, 2),
                "over_count": stats[r]["OVER"]["кол-во"],
                "over_win": stats[r]["OVER"]["WIN"],
                "over_roi": round(stats[r]["OVER"]["%"] * 100, 2),
                "under_count": stats[r]["UNDER"]["кол-во"],
                "under_win": stats[r]["UNDER"]["WIN"],
                "under_roi": round(stats[r]["UNDER"]["%"] * 100, 2),
            }
        )
    return rows


def _analyze_half_matches(matches: List[dict], tournament_name: str, change: bool = False) -> dict:
    stats = {
        "OVER": {"кол-во": 0, "WIN": 0, "%": 0},
        "UNDER": {"кол-во": 0, "WIN": 0, "%": 0},
        "TOTAL": {"кол-во": 0, "WIN": 0, "%": 0},
    }
    is_ncaa_d1 = bool(tournament_name and "NCAA D1" in tournament_name)
    for match in matches:
        if is_ncaa_d1:
            prediction, _ = _calculate_prediction_half_ncaa(match)
        else:
            prediction, _ = (
                _calculate_prediction_half_change(match) if change else _calculate_prediction_half(match)
            )
        if prediction not in ("OVER", "UNDER"):
            continue
        tim = _to_float(match.get("tim", 0))
        result = _to_float(match.get("result", 0))
        win_value = _calculate_win_value(prediction, result, tim)
        stats[prediction]["кол-во"] += 1
        stats[prediction]["WIN"] += win_value
        stats["TOTAL"]["кол-во"] += 1
        stats["TOTAL"]["WIN"] += win_value

    for category in ("OVER", "UNDER", "TOTAL"):
        cnt = stats[category]["кол-во"]
        if cnt > 0:
            stats[category]["%"] = stats[category]["WIN"] / (cnt * 100)
    return stats


def analyze_tournament_half(tournament: str, change: bool = False) -> dict:
    matches = _get_match_dicts(tournament)
    if not matches:
        return {"OVER": {"кол-во": 0, "WIN": 0, "%": 0}, "UNDER": {"кол-во": 0, "WIN": 0, "%": 0}, "TOTAL": {"кол-во": 0, "WIN": 0, "%": 0}}
    return _analyze_half_matches(matches, tournament_name=tournament, change=change)


def analyze_all_tournaments_half(change: bool = False) -> dict:
    tournaments = get_tournaments()
    all_stats: Dict[str, dict] = {}
    total_stats = {
        "OVER": {"кол-во": 0, "WIN": 0, "%": 0},
        "UNDER": {"кол-во": 0, "WIN": 0, "%": 0},
        "TOTAL": {"кол-во": 0, "WIN": 0, "%": 0},
    }
    for tournament_name in tournaments:
        matches = _get_match_dicts(tournament_name)
        tournament_stats = _analyze_half_matches(matches, tournament_name=tournament_name, change=change)
        all_stats[tournament_name] = tournament_stats
        for category in ("OVER", "UNDER", "TOTAL"):
            total_stats[category]["кол-во"] += tournament_stats[category]["кол-во"]
            total_stats[category]["WIN"] += tournament_stats[category]["WIN"]
    for category in ("OVER", "UNDER", "TOTAL"):
        cnt = total_stats[category]["кол-во"]
        if cnt > 0:
            total_stats[category]["%"] = total_stats[category]["WIN"] / (cnt * 100)
    return {"tournaments": all_stats, "total": total_stats}
