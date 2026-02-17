"""
Module for managing the 'База половин' (halves database) in the Excel Analyzer
application.  This module encapsulates all data storage and statistical
operations required to work with half‑time and quarter‑time basketball
statistics.  It uses a SQLite backend located in the user's application
data directory (``AppData/Local/ExcelAnalyzer`` on Windows) and provides
methods to import raw match strings, compute team averages, calculate
deviations between halves, distribute totals by quarter and half, and
derive over/under coefficients based on historical data.

The schema is flexible enough to handle matches with or without an
overtime period.  Each match record stores the date, tournament,
participating teams and the points scored by each team in up to five
periods (four quarters plus an optional overtime).  All computations
exclude rows with missing values and gracefully handle division by zero.

Because the original Excel formulas treat missing or zero values as
non‑contributing when computing averages, this module follows the
same convention: when summing points for a given quarter it only
includes rows where the sum is strictly greater than zero.

Example usage:

>>> db = HalfsDatabase()
>>> lines = [
...     "21.01.2026 China-2 Changsha Jiangxi 23 35 26 18 24 23 23 32",
...     "22.01.2026 China-2 Guangdong Tigers Guangsha 30 28 25 27 27 25 20 21"
... ]
>>> db.add_matches_from_lines(lines)
>>> stats = db.get_team_statistics(tournament="China-2")
>>> deviation = db.get_team_deviations(tournament="China-2")

Note: Team names are parsed heuristically when importing from raw
strings.  The importer assumes the first token is a date in the
``DD.MM.YYYY`` format, the second token is the tournament identifier,
and the trailing eight or ten tokens are numeric scores.  The
remaining tokens between tournament and scores are split equally
between the home and away team names.  If a team name contains a
different number of words from its opponent, the split may not be
perfect; in such cases it is advisable to import data programmatically
to avoid ambiguity.
"""

import os
import sys
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd

from db_connection import db_connect, is_postgres, adapt_sql, _default_data_dir


class HalfsDatabase:
    """Class for storing and analysing basketball half/quarter data."""

    _SCHEMA = 'halfs'

    def __init__(self):
        self.db_path = os.path.join(_default_data_dir(), "halfs.db")
        self.init_database()

    @contextmanager
    def _connect(self):
        """Unified connection: PostgreSQL or SQLite."""
        with db_connect(schema=self._SCHEMA, sqlite_path=self.db_path) as conn:
            yield conn

    def init_database(self) -> None:
        """Initialise the database and create necessary tables."""
        with self._connect() as conn:
            cur = conn.cursor()
            # Matches table stores per‑period points.  Overtime columns are
            # nullable because many games will not have a fifth period.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    tournament TEXT NOT NULL,
                    team_home TEXT NOT NULL,
                    team_away TEXT NOT NULL,
                    q1_home INTEGER,
                    q1_away INTEGER,
                    q2_home INTEGER,
                    q2_away INTEGER,
                    q3_home INTEGER,
                    q3_away INTEGER,
                    q4_home INTEGER,
                    q4_away INTEGER,
                    ot_home INTEGER,
                    ot_away INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            # Indices speed up queries when filtering by tournament or team
            cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_tournament ON matches(tournament)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_team_home ON matches(team_home)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_team_away ON matches(team_away)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_halfs_date ON matches(date)")
            conn.commit()

    # ------------------------------------------------------------------
    # Data import
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_match_line(line: str) -> Optional[Tuple]:
        """Parse a single raw match line into its constituent parts.

        The expected format for ``line`` is::

            ``DD.MM.YYYY TOURNAMENT TEAM_HOME TEAM_AWAY S1 S2 ... S8 [S9 S10]``

        where "S1 S2 ..." represent the numeric scores of each team in
        sequence.  The parser attempts to identify the date at the
        beginning, then separates the tournament and team names from
        the trailing numeric tokens.  Tournament names may contain
        spaces and a trailing ``(W)`` suffix for women's tournaments.
        Team names are assumed to consist of one word each in most
        cases; however, if extra words remain after extracting the
        numeric tokens, they are allocated to the tournament name.

        Parameters
        ----------
        line : str
            The raw string containing match information.

        Returns
        -------
        tuple | None
            A tuple containing all match fields in the order expected
            by the database schema, or ``None`` if parsing fails.
        """
        if not line or not line.strip():
            return None
        tokens = [tok.strip() for tok in line.strip().split() if tok.strip()]
        # Require enough tokens: tournament (and optional date), two team names and scores
        # Without a date there should be at least 7 tokens (tournament + two teams + 4 scores)
        if len(tokens) < 7:
            return None
        # Attempt to parse the first token as a date. If parsing fails,
        # treat the first token as part of the tournament name and use
        # today's date for the record.  This change allows input lines
        # that omit the date entirely.
        start_idx = 0
        try:
            parsed_date = datetime.strptime(tokens[0], "%d.%m.%Y")
            date_iso = parsed_date.strftime("%Y-%m-%d")
            start_idx = 1
        except Exception:
            # If the first token is not a date, leave date empty (None)
            # and treat the first token as part of the tournament name.
            # We do not auto‑assign today's date, as input lines may not
            # include any date information.
            date_iso = ""
            start_idx = 0
        # ----------------------------------------------------------------------
        # Identify the trailing numeric tokens that represent quarter (and optional
        # overtime) scores.  The Excel format specifies either 8 numeric values
        # (four quarters) or 10 numeric values (four quarters plus one overtime
        # pair).  Some team names may contain digits (e.g. "Basket 369"); these
        # digits should not be treated as scores.  To avoid erroneously
        # splitting team names when they contain numbers, we inspect the end
        # of the token list for a suffix of exactly 10 or 8 numeric tokens.
        scores = []
        score_start = None
        scores_type = "quarters"
        total_tokens = len(tokens)
        # Try to match 10 trailing numeric tokens for possible overtime
        if total_tokens - start_idx >= 10 and all(
            tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 10 :]
        ):
            try:
                scores = [int(tok) for tok in tokens[total_tokens - 10 :]]
                score_start = total_tokens - 10
                scores_type = "quarters"
            except Exception:
                scores = []
                score_start = None
        # Otherwise try to match 8 trailing numeric tokens (standard four quarters)
        if not scores:
            if total_tokens - start_idx >= 8 and all(
                tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 8 :]
            ):
                try:
                    scores = [int(tok) for tok in tokens[total_tokens - 8 :]]
                    score_start = total_tokens - 8
                    scores_type = "quarters"
                except Exception:
                    return None
            else:
                # NCAA D1: allow 6 (halves + OT) or 4 (halves) trailing numeric tokens
                if total_tokens - start_idx >= 6 and all(
                    tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 6 :]
                ):
                    try:
                        scores = [int(tok) for tok in tokens[total_tokens - 6 :]]
                        score_start = total_tokens - 6
                        scores_type = "halves"
                    except Exception:
                        return None
                elif total_tokens - start_idx >= 4 and all(
                    tok.lstrip("+-").isdigit() for tok in tokens[total_tokens - 4 :]
                ):
                    try:
                        scores = [int(tok) for tok in tokens[total_tokens - 4 :]]
                        score_start = total_tokens - 4
                        scores_type = "halves"
                    except Exception:
                        return None
                else:
                    return None
        # Now ``scores`` holds either 8 or 10 integers, and ``score_start`` is the
        # index in ``tokens`` where these numeric tokens begin.  Everything
        # before ``score_start`` belongs to the tournament and team names.
        meta_tokens = tokens[start_idx:score_start]
        if len(meta_tokens) < 3:
            # Need at least tournament name and two team names
            return None
        # ------------------------------------------------------------------
        # Improved heuristic for splitting tournament and team names.
        #
        # The original Excel format assumes that team names can contain
        # hyphens, digits and rarely spaces, while tournament names may
        # consist of multiple words (e.g., "Russia-3 (W)" or "China A (W)").
        # A common failure mode of the prior algorithm was incorrectly
        # assigning multi‑token team names such as "Dinamo U-2" or
        # "TSK Ural".  To address this, we perform two additional steps:
        #   1. Group a trailing ``(W)`` token with the preceding token,
        #      so that "A (W)" or "Zhe (W)" are treated as single units.
        #   2. After initially assigning the last two tokens to the teams,
        #      examine whether the away team's token appears to be a suffix
        #      (e.g. ``U-2``, ``NR-2``) and, if so, prepend the previous
        #      token to the away team and adjust the home team accordingly.
        #      Likewise, if the home team appears to be missing a prefix
        #      (e.g. a single letter like ``D``), prepend the previous
        #      token from the tournament to the home team.
        #
        # This heuristic is not perfect but covers common cases where
        # tournaments like "Russia-2" or "China A (W)" are followed by
        # team names that may contain spaces or suffixes.

        # Step 1: Combine standalone "(W)" tokens with the previous token
        grouped: List[str] = []
        i = 0
        while i < len(meta_tokens):
            tok = meta_tokens[i]
            if tok == "(W)":
                # Attach women's marker to previous token if it exists
                if grouped:
                    grouped[-1] = f"{grouped[-1]} {tok}"
                else:
                    # Unexpected position: treat as separate token
                    grouped.append(tok)
                i += 1
                continue
            else:
                grouped.append(tok)
                i += 1
        meta_tokens = grouped
        # After grouping, ensure we still have enough tokens
        if len(meta_tokens) < 3:
            return None

        # Step 2: Assign tokens to home and away teams.
        #
        # Starting from the end of ``meta_tokens``, assemble the team names.  If
        # a token is purely numeric, we treat it as part of the adjacent team name.
        # This is important for names such as "Basket 369", "7 Up", "Junior 06" or
        # "Proleter 023".  We build the away team first and then the home team.  A
        # flag ``numeric_case`` is set when numeric tokens are joined to any team
        # name.  This flag later disables attaching additional tournament tokens
        # to the home team (see Step 4).
        team_away_tokens: List[str] = []
        team_home_tokens: List[str] = []

        def is_numeric_string(tok: str) -> bool:
            return tok.lstrip("+-").isdigit()

        numeric_case = False
        # Index of the last element in meta_tokens
        i = len(meta_tokens) - 1
        # Build the away team
        if i < 0:
            return None
        if is_numeric_string(meta_tokens[i]):
            # Combine numeric suffix with the preceding token (e.g. "Basket 369")
            if i - 1 < 0:
                return None
            # Preserve original order: the preceding word followed by the numeric part
            team_away_tokens = [meta_tokens[i - 1], meta_tokens[i]]
            numeric_case = True
            i -= 2
        else:
            team_away_tokens = [meta_tokens[i]]
            i -= 1
        # Build the home team
        if i < 0:
            return None
        if is_numeric_string(meta_tokens[i]):
            # Combine numeric suffix with the preceding token (e.g. "Proleter 023")
            if i - 1 < 0:
                return None
            team_home_tokens = [meta_tokens[i - 1], meta_tokens[i]]
            numeric_case = True
            i -= 2
        else:
            team_home_tokens = [meta_tokens[i]]
            i -= 1
        # Handle cases like "7 Up" where a numeric token precedes a word.  If the
        # remaining last token is purely numeric, prepend it to the home team.
        # This ensures the correct order (e.g. "7 Up" rather than "Up 7").  We only
        # do this if there is at least one numeric token left; otherwise we risk
        # consuming a legitimate tournament token (e.g. "Russia-2").
        if i >= 0 and is_numeric_string(meta_tokens[i]):
            # Prepend numeric token to home team and mark numeric_case.  This
            # consumes the numeric token from the tournament portion.
            team_home_tokens.insert(0, meta_tokens[i])
            numeric_case = True
            i -= 1
        # Remaining tokens form the tournament name
        meta_tokens = meta_tokens[: i + 1]

        # Step 3: If the away team looks like a suffix (e.g. "U-2", "NR-2")
        # and the tentative home team does not contain digits, hyphens or
        # parentheses, then the away team likely consists of two tokens.
        import re

        def looks_like_suffix(token: str) -> bool:
            """Return True if token resembles a suffix of a team name."""
            # e.g. "U-2", "NR-2", "D", etc.  We consider a suffix to be
            # either a short alphabetic string (<=2 chars) or a hyphenated
            # alphanumeric ending with digits.
            return bool(
                re.fullmatch(r"[A-Za-z]{1,2}", token)  # single or two letters
                or re.fullmatch(r"[A-Za-z]+-\d+", token)  # letters-hyphen-digits
            )

        # If the away token is a suffix and home token lacks digits or hyphen,
        # and there are at least two tokens left in meta_tokens (so that a
        # tournament token remains after reassignment), then the away team
        # likely consists of two tokens.  Without at least two tokens
        # remaining, we risk misclassifying a true tournament token (e.g.,
        # "Russia-2") as a team name.  This refinement prevents
        # cases like "Russia-2 Cheboksary Chel-2" from being split into
        # home="Russia-2" and away="Cheboksary Chel-2".
        if (
            looks_like_suffix(team_away_tokens[0])
            and not re.search(r"[-\d()]", team_home_tokens[0])
            and len(meta_tokens) >= 2
        ):
            # Move the tentative home token to the away team and pick a new
            # home token from the remaining meta tokens
            team_away_tokens.insert(0, team_home_tokens.pop())
            # Assign new home if possible
            if meta_tokens:
                team_home_tokens = [meta_tokens.pop()]
            else:
                return None

        # Step 4: If the home team looks incomplete (no digits/hyphens/parentheses)
        # and the preceding token also looks like a plain word, then prepend
        # that token to the home team.  This captures names like "TSK Ural"
        # and "Jiangsu D".
        #
        # To avoid incorrectly consuming the only remaining tournament token,
        # perform this step only when there are at least two tokens left in
        # ``meta_tokens``.  Without this check, a single tournament token
        # (e.g., "Korea") could be attached to the home team, leaving an
        # empty tournament name and shifting the away team incorrectly.
        if (
            not numeric_case
            and len(meta_tokens) >= 2
            and not re.search(r"[-\d()]", team_home_tokens[0])
            and not re.search(r"[-\d()]", meta_tokens[-1])
            # Avoid attaching single-letter group markers like 'A' or 'B'
            and len(meta_tokens[-1]) > 1
            # Do not attach tokens that are clearly division names or include a plus sign
            and '+' not in meta_tokens[-1]
            and meta_tokens[-1].upper() not in {
                'EAST', 'WEST', 'NORTH', 'SOUTH', 'CENTRAL', 'CENTER',
                'NORTHWEST', 'NORTHEAST', 'SOUTHWEST', 'SOUTHEAST'
            }
        ):
            # Prepend the previous token to the home team (for cases like "TSK Ural" or "Jiangsu D")
            team_home_tokens.insert(0, meta_tokens.pop())

        # The remaining tokens (if any) constitute the tournament name
        tournament_tokens = meta_tokens
        tournament = " ".join(tournament_tokens).strip()
        team_home = " ".join(team_home_tokens).strip()
        team_away = " ".join(team_away_tokens).strip()
        # NCAA D1 uses halves instead of quarters
        if scores_type == "halves":
            tournament_norm = tournament.replace("~", " ").strip().upper()
            if tournament_norm != "NCAA D1":
                return None
            q1_home, q1_away = scores[0:2]
            q2_home, q2_away = scores[2:4]
            q3_home, q3_away = 0, 0
            q4_home, q4_away = 0, 0
            ot_home = None
            ot_away = None
            if len(scores) == 6:
                ot_home, ot_away = scores[4:6]
        else:
            # Assign quarter and overtime scores
            q1_home, q1_away = scores[0:2]
            q2_home, q2_away = scores[2:4]
            q3_home, q3_away = scores[4:6]
            q4_home, q4_away = scores[6:8]
            ot_home = None
            ot_away = None
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

    def add_matches_from_lines(self, lines: List[str]) -> int:
        """Parse and insert multiple match records into the database.

        Parameters
        ----------
        lines : list[str]
            A list of raw strings, each representing one match.

        Returns
        -------
        int
            The number of successfully inserted matches.
        """
        to_insert = []
        for line in lines:
            parsed = self._parse_match_line(line)
            if parsed:
                to_insert.append(parsed)
        if not to_insert:
            return 0
        with self._connect() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO matches (
                    date, tournament, team_home, team_away,
                    q1_home, q1_away, q2_home, q2_away,
                    q3_home, q3_away, q4_home, q4_away,
                    ot_home, ot_away
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                to_insert,
            )
            conn.commit()
        return len(to_insert)

    def import_lines(self, lines: List[str], error_file_path: Optional[str] = None) -> Tuple[int, List[str]]:
        """Import lines and collect errors without throwing per-line exceptions.

        Parameters
        ----------
        lines : list[str]
            Lines to parse and insert.
        error_file_path : str, optional
            If provided, invalid lines will be written to this file.  The
            file will be overwritten.  If no invalid lines are
            encountered, the file will not be created.

        Returns
        -------
        tuple (int, list[str])
            A tuple containing the number of inserted rows and a list of
            lines that failed to parse.
        """
        to_insert = []
        errors = []
        # Перед разбором заменяем символы '_' на пробелы.  Пользователи часто
        # заменяют пробелы в Excel на подчёркивания, чтобы корректно копировать
        # данные.  Здесь мы возвращаем их обратно, чтобы сохранить исходные
        # названия турниров и команд.
        for line in lines:
            if line:
                processed_line = line.replace("_", " ")
            else:
                processed_line = line
            parsed = self._parse_match_line(processed_line)
            if parsed:
                to_insert.append(parsed)
            else:
                errors.append(line)
        inserted = 0
        if to_insert:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.executemany(
                    """
                    INSERT INTO matches (
                        date, tournament, team_home, team_away,
                        q1_home, q1_away, q2_home, q2_away,
                        q3_home, q3_away, q4_home, q4_away,
                        ot_home, ot_away
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    to_insert,
                )
                conn.commit()
            inserted = len(to_insert)
        # Write errors to file if requested
        if error_file_path and errors:
            try:
                with open(error_file_path, "w", encoding="utf-8") as f:
                    for line in errors:
                        f.write(line.rstrip("\n") + "\n")
            except Exception:
                pass
        return inserted, errors

    # ------------------------------------------------------------------
    # Deletion methods
    # ------------------------------------------------------------------
    def delete_matches(self, match_ids: List[int]) -> int:
        """Delete specific matches by their database identifiers.

        Parameters
        ----------
        match_ids : list[int]
            A list of match IDs to delete.  IDs correspond to the
            primary key column ``id`` in the ``matches`` table.

        Returns
        -------
        int
            The number of rows deleted.  If an ID does not exist,
            it is silently ignored.
        """
        if not match_ids:
            return 0
        with self._connect() as conn:
            cur = conn.cursor()
            # Use executemany for efficiency
            cur.executemany("DELETE FROM matches WHERE id = ?", [(mid,) for mid in match_ids])
            conn.commit()
            # rowcount reflects the number of rows deleted by the last execute.
            # To compute total deleted, sum changed row counts is non-trivial.
            # We can compute by checking affected rows via rowcount of each statement,
            # but SQLite does not provide this per row in executemany.  As a
            # compromise, return the length of match_ids assuming they existed.
            return cur.rowcount

    def delete_all_matches(self, tournament: Optional[str] = None) -> int:
        """Delete all matches or all matches belonging to a specific tournament.

        Parameters
        ----------
        tournament : str | None, optional
            If provided, only matches from this tournament are deleted.
            If ``None``, all records are removed.

        Returns
        -------
        int
            The number of rows deleted.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            if tournament:
                cur.execute("DELETE FROM matches WHERE tournament = ?", (tournament,))
            else:
                cur.execute("DELETE FROM matches")
            deleted = cur.rowcount
            conn.commit()
            return deleted

    # ------------------------------------------------------------------
    # Tournament rename
    # ------------------------------------------------------------------
    def rename_tournament(self, old_name: str, new_name: str) -> int:
        """Rename a tournament throughout the matches table.

        Parameters
        ----------
        old_name : str
            The current name of the tournament to rename.
        new_name : str
            The new name to assign to the tournament.

        Returns
        -------
        int
            The number of rows updated.

        Notes
        -----
        This method performs a simple SQL update replacing all occurrences
        of ``old_name`` with ``new_name`` in the ``tournament`` column.
        If no rows match ``old_name``, zero is returned.  Errors from the
        database are propagated to the caller.
        """
        # Guard against invalid input or no‑op renames
        if not old_name or not new_name or old_name == new_name:
            return 0
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE matches SET tournament = ? WHERE tournament = ?",
                (new_name, old_name),
            )
            conn.commit()
            return cur.rowcount

    # ------------------------------------------------------------------
    # Update a single field of a match
    # ------------------------------------------------------------------
    def update_match_field(self, match_id: int, field: str, value: Optional[str]) -> None:
        """Update a specific column of a match record.

        Parameters
        ----------
        match_id : int
            The primary key of the match to update.
        field : str
            The column name to update (e.g. 'tournament', 'team_home', 'q1_home').
        value : str | int | None
            The new value to store.  Use ``None`` to set the column to NULL.

        Raises
        ------
        ValueError
            If ``field`` is not one of the allowed columns.
        sqlite3.DatabaseError
            If a database error occurs.
        """
        allowed_fields = {
            'date', 'tournament', 'team_home', 'team_away',
            'q1_home', 'q1_away', 'q2_home', 'q2_away',
            'q3_home', 'q3_away', 'q4_home', 'q4_away',
            'ot_home', 'ot_away'
        }
        if field not in allowed_fields:
            raise ValueError(f"Недопустимое поле для обновления: {field}")
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE matches SET {field} = ? WHERE id = ?",
                (value, match_id),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Statistics: wins and losses per team
    # ------------------------------------------------------------------
    def get_wins_losses(self, tournament: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Compute the number of wins and losses for each team in the specified tournament.

        Parameters
        ----------
        tournament : str | None
            Name of the tournament to filter by, or ``None`` to include all tournaments.

        Returns
        -------
        pandas.DataFrame | None
            A DataFrame indexed by team with columns ``wins`` and ``losses``.
            Returns ``None`` if there are no matches for the given tournament.
        """
        df = self._load_matches(tournament)
        if df.empty:
            return None
        # Compute total points for each side
        df = df.copy()
        for side in ['home', 'away']:
            df[f'total_{side}'] = (
                df[f'q1_{side}'].fillna(0)
                + df[f'q2_{side}'].fillna(0)
                + df[f'q3_{side}'].fillna(0)
                + df[f'q4_{side}'].fillna(0)
                + df[f'ot_{side}'].fillna(0)
            )
        # Determine winners and losers
        df['winner'] = df.apply(lambda r: r['team_home'] if r['total_home'] > r['total_away'] else (r['team_away'] if r['total_away'] > r['total_home'] else None), axis=1)
        df['loser'] = df.apply(lambda r: r['team_away'] if r['total_home'] > r['total_away'] else (r['team_home'] if r['total_away'] > r['total_home'] else None), axis=1)
        # Count wins and losses
        wins = df['winner'].value_counts()
        losses = df['loser'].value_counts()
        teams = sorted(set(df['team_home']).union(df['team_away']))
        data = []
        for team in teams:
            w = int(wins.get(team, 0))
            l = int(losses.get(team, 0))
            data.append({'team': team, 'wins': w, 'losses': l})
        result_df = pd.DataFrame(data).set_index('team')
        return result_df

    # ------------------------------------------------------------------
    # Games summary per tournament
    # ------------------------------------------------------------------
    def get_games_summary(self) -> pd.DataFrame:
        """
        Compute the actual number of games and normative number of games for each tournament.

        The normative number of games is defined as the floor of half the number of
        unique teams in the tournament (i.e. ``floor(n_teams / 2)``).

        Returns
        -------
        pandas.DataFrame
            A DataFrame with columns ``tournament``, ``actual_games``, ``normative_games``,
            and ``team_count``.  The index is the tournament name.
        """
        # Load all matches
        df = self._load_matches()
        if df.empty:
            return pd.DataFrame(columns=['tournament', 'actual_games', 'normative_games', 'team_count']).set_index('tournament')
        # Count actual games per tournament
        games_counts = df.groupby('tournament').size()
        # Compute unique team count per tournament
        teams_per_tournament = {}
        for tournament, group in df.groupby('tournament'):
            teams = set(group['team_home']).union(set(group['team_away']))
            teams_per_tournament[tournament] = len(teams)
        data = []
        for tournament, actual_games in games_counts.items():
            team_count = teams_per_tournament.get(tournament, 0)
            normative_games = team_count // 2
            data.append({
                'tournament': tournament,
                'actual_games': int(actual_games),
                'normative_games': int(normative_games),
                'team_count': int(team_count)
            })
        result_df = pd.DataFrame(data).set_index('tournament')
        return result_df

    # ------------------------------------------------------------------
    # Statistical computations
    # ------------------------------------------------------------------
    def _load_matches(self, tournament: Optional[str] = None) -> pd.DataFrame:
        """Internal helper to load matches from the database into a DataFrame.

        Parameters
        ----------
        tournament : str | None, optional
            If provided, only matches from this tournament are returned.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of match records.
        """
        query = "SELECT * FROM matches"
        params = []
        if tournament:
            query += " WHERE tournament = ?"
            params.append(tournament)
        with self._connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        return df

    def get_team_statistics(self, tournament: Optional[str] = None) -> pd.DataFrame:
        """Compute per‑team offensive and defensive statistics.

        This method replicates the Excel calculations described in the
        user's original workbook.  For each team it computes the number
        of games played and average points scored and conceded in each
        quarter and half.  Periods with zero total points are excluded
        from the sums (mimicking the ``СУММЕСЛИМН" > 0"`` criteria).

        Parameters
        ----------
        tournament : str | None, optional
            If supplied, only matches belonging to this tournament are
            considered.  When ``None`` all tournaments are included.

        Returns
        -------
        pandas.DataFrame
            A DataFrame indexed by team name with the following
            columns:
                games : int
                    Number of matches the team played.
                avg_scored_q1 .. avg_scored_q4 : float
                    Average points the team scores in the corresponding quarter.
                avg_conceded_q1 .. avg_conceded_q4 : float
                    Average points the team concedes in the corresponding quarter.
                first_half_scored, second_half_scored, total_scored
                    Summations of average scored across quarters.
                first_half_conceded, second_half_conceded, total_conceded
                    Summations of average conceded across quarters.
        """
        df = self._load_matches(tournament)
        if df.empty:
            return pd.DataFrame()
        def is_ncaa_halves(t_name: Optional[str]) -> bool:
            if not t_name:
                return False
            norm = " ".join(str(t_name).replace("~", " ").split()).upper()
            return norm == "NCAA D1"

        # Build a list of all unique teams
        teams = sorted(set(df["team_home"]) | set(df["team_away"]))
        stats = []
        for team in teams:
            # Games where team participated
            home_mask = df["team_home"] == team
            away_mask = df["team_away"] == team
            games = int(home_mask.sum() + away_mask.sum())
            if games == 0:
                continue
            # Helper to sum points scored by the team for the given quarter.
            # Only include matches where the team's own quarter score is > 0.
            def sum_points(col_home: str, col_away: str) -> int:
                """Sum points for the given quarter, counting only positive scores for the team."""
                # Sum when team is home and its own score in this quarter > 0
                s_home = df.loc[home_mask & (df[col_home] > 0), col_home].sum()
                # Sum when team is away and its own score in this quarter > 0
                s_away = df.loc[away_mask & (df[col_away] > 0), col_away].sum()
                return s_home + s_away
            # Helper to sum points conceded by the team for the given quarter.
            # Only include matches where the opponent's quarter score is > 0.
            def sum_conceded(col_home: str, col_away: str) -> int:
                # When team is home, conceded points come from the away column where away score > 0
                c_home = df.loc[home_mask & (df[col_away] > 0), col_away].sum()
                # When team is away, conceded points come from the home column where home score > 0
                c_away = df.loc[away_mask & (df[col_home] > 0), col_home].sum()
                return c_home + c_away
            # Compute totals for each quarter
            scored_q1 = sum_points("q1_home", "q1_away")
            scored_q2 = sum_points("q2_home", "q2_away")
            scored_q3 = sum_points("q3_home", "q3_away")
            scored_q4 = sum_points("q4_home", "q4_away")
            conceded_q1 = sum_conceded("q1_home", "q1_away")
            conceded_q2 = sum_conceded("q2_home", "q2_away")
            conceded_q3 = sum_conceded("q3_home", "q3_away")
            conceded_q4 = sum_conceded("q4_home", "q4_away")
            # Convert to averages
            avg_scored_q1 = scored_q1 / games
            avg_scored_q2 = scored_q2 / games
            avg_scored_q3 = scored_q3 / games
            avg_scored_q4 = scored_q4 / games
            avg_conceded_q1 = conceded_q1 / games
            avg_conceded_q2 = conceded_q2 / games
            avg_conceded_q3 = conceded_q3 / games
            avg_conceded_q4 = conceded_q4 / games
            # Half and total sums
            if is_ncaa_halves(tournament):
                first_half_scored = avg_scored_q1
                second_half_scored = avg_scored_q2
            else:
                first_half_scored = avg_scored_q1 + avg_scored_q2
                second_half_scored = avg_scored_q3 + avg_scored_q4
            total_scored = first_half_scored + second_half_scored
            if is_ncaa_halves(tournament):
                first_half_conceded = avg_conceded_q1
                second_half_conceded = avg_conceded_q2
            else:
                first_half_conceded = avg_conceded_q1 + avg_conceded_q2
                second_half_conceded = avg_conceded_q3 + avg_conceded_q4
            total_conceded = first_half_conceded + second_half_conceded
            stats.append({
                "team": team,
                "games": games,
                "avg_scored_q1": avg_scored_q1,
                "avg_scored_q2": avg_scored_q2,
                "avg_scored_q3": avg_scored_q3,
                "avg_scored_q4": avg_scored_q4,
                "avg_conceded_q1": avg_conceded_q1,
                "avg_conceded_q2": avg_conceded_q2,
                "avg_conceded_q3": avg_conceded_q3,
                "avg_conceded_q4": avg_conceded_q4,
                "first_half_scored": first_half_scored,
                "second_half_scored": second_half_scored,
                "total_scored": total_scored,
                "first_half_conceded": first_half_conceded,
                "second_half_conceded": second_half_conceded,
                "total_conceded": total_conceded,
            })
        return pd.DataFrame(stats).set_index("team")

    def get_team_deviations(self, tournament: Optional[str] = None) -> pd.DataFrame:
        """Compute per‑team deviations between second and first halves.

        Deviation for a team is defined as ``(Z + AJ) - (W + AG)``, where
        ``W`` and ``Z`` are the average points scored in the first and
        second halves respectively, and ``AG`` and ``AJ`` are the
        average points conceded in the first and second halves.  A
        positive deviation indicates that the team tends to perform
        better offensively (or worse defensively) in the second half.

        Parameters
        ----------
        tournament : str | None, optional
            Restrict the calculation to a specific tournament.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with columns ``deviation`` and
            ``average_total`` (sum of points scored and conceded).
        """
        stats = self.get_team_statistics(tournament)
        if stats.empty:
            return pd.DataFrame()
        # Compute AE = W + AG (first half scored + first half conceded)
        ae = stats["first_half_scored"] + stats["first_half_conceded"]
        # Compute AF = Z + AJ (second half scored + second half conceded)
        af = stats["second_half_scored"] + stats["second_half_conceded"]
        deviation = af - ae
        average_total = ae + af
        result = pd.DataFrame({
            "deviation": deviation,
            "average_total": average_total
        }, index=stats.index)
        return result

    def get_quarter_distribution(self, team1: str, team2: str, tournament: Optional[str] = None) -> Optional[Dict[str, float]]:
        """Calculate quarter distribution ratios for a pair of teams.

        Given two team names and an optional tournament, this method
        computes the average scoring of both teams in each quarter and
        expresses it as a fraction of the combined average total.

        The returned dictionary contains the keys ``q1``, ``q2``, ``q3``
        and ``q4`` whose values sum to 1.  These ratios can be used to
        distribute a match total across quarters.

        Parameters
        ----------
        team1, team2 : str
            Names of the competing teams.
        tournament : str | None, optional
            Restrict the calculation to a specific tournament.

        Returns
        -------
        dict | None
            Dictionary of quarter ratios or ``None`` if the teams are
            unknown.
        """
        stats = self.get_team_statistics(tournament)
        if stats.empty or team1 not in stats.index or team2 not in stats.index:
            return None
        t1 = stats.loc[team1]
        t2 = stats.loc[team2]
        # Average quarter totals (scored + conceded) for the pair (mean of both teams)
        q1 = ((t1["avg_scored_q1"] + t1["avg_conceded_q1"]) + (t2["avg_scored_q1"] + t2["avg_conceded_q1"])) / 2.0
        q2 = ((t1["avg_scored_q2"] + t1["avg_conceded_q2"]) + (t2["avg_scored_q2"] + t2["avg_conceded_q2"])) / 2.0
        q3 = ((t1["avg_scored_q3"] + t1["avg_conceded_q3"]) + (t2["avg_scored_q3"] + t2["avg_conceded_q3"])) / 2.0
        q4 = ((t1["avg_scored_q4"] + t1["avg_conceded_q4"]) + (t2["avg_scored_q4"] + t2["avg_conceded_q4"])) / 2.0
        total = q1 + q2 + q3 + q4
        if total == 0:
            return None
        return {
            "q1": q1 / total,
            "q2": q2 / total,
            "q3": q3 / total,
            "q4": q4 / total
        }

    def distribute_total(self, team1: str, team2: str, match_total: float, tournament: Optional[str] = None) -> Optional[Dict[str, float]]:
        """Distribute a match total across quarters for a pair of teams.

        This method uses the quarter distribution ratios computed by
        :func:`get_quarter_distribution` and multiplies them by the
        supplied ``match_total`` value.  It returns a dictionary with
        quarter labels ``q1`` .. ``q4`` and their respective totals.

        Parameters
        ----------
        team1, team2 : str
            Names of the competing teams.
        match_total : float
            The overall total points line to distribute.
        tournament : str | None, optional
            Restrict the calculation to a specific tournament.

        Returns
        -------
        dict | None
            A mapping from quarter labels to distributed totals, or
            ``None`` if distribution is not possible.
        """
        ratios = self.get_quarter_distribution(team1, team2, tournament)
        if ratios is None:
            return None
        return {k: match_total * v for k, v in ratios.items()}

    def get_pair_deviation(self, team1: str, team2: str, tournament: Optional[str] = None) -> Optional[float]:
        """Compute the combined deviation for a pair of teams.

        The deviation for a single team is defined in
        :meth:`get_team_deviations` as the difference between the
        second‑half and first‑half combined scores (points scored and
        conceded).  To compare how two teams might combine, this
        method returns the sum of their deviations divided by 4, as
        specified by the user.

        Parameters
        ----------
        team1, team2 : str
            Names of the teams.
        tournament : str | None, optional
            Restrict the calculation to matches from this tournament.

        Returns
        -------
        float | None
            The combined deviation, or ``None`` if either team is
            absent.
        """
        deviations = self.get_team_deviations(tournament)
        if deviations.empty or team1 not in deviations.index or team2 not in deviations.index:
            return None
        dev1 = deviations.loc[team1, "deviation"]
        dev2 = deviations.loc[team2, "deviation"]
        return (dev1 + dev2) / 4.0

    def get_tournament_summary(self) -> pd.DataFrame:
        """Compute summary statistics for each tournament.

        The summary includes, for each tournament:
            - ``deviation``: the average deviation across all teams in the tournament.
              Deviation for a team is defined as in :meth:`get_team_deviations`.
            - Average first quarter total (Q1): mean of the sum of points scored by both
              teams in the first quarter of each match.  If a match has zero points
              in a quarter, it is still included in the calculation.
            - Average second quarter total (Q2).
            - Average first half total (H1).
            - Average third quarter total (Q3).
            - Average fourth quarter total (Q4).
            - Average second half total (H2).
            - Average match total.
            - ``games_count``: number of matches in the tournament.
            - ``teams_count``: number of unique teams participating in the tournament.

        Returns
        -------
        pandas.DataFrame
            A DataFrame indexed by tournament name with the columns listed above.
        """
        df = self._load_matches()
        if df.empty:
            return pd.DataFrame()
        # Compute per-match totals
        df = df.copy()
        df["q1_total"] = df["q1_home"].fillna(0) + df["q1_away"].fillna(0)
        df["q2_total"] = df["q2_home"].fillna(0) + df["q2_away"].fillna(0)
        df["q3_total"] = df["q3_home"].fillna(0) + df["q3_away"].fillna(0)
        df["q4_total"] = df["q4_home"].fillna(0) + df["q4_away"].fillna(0)
        df["h1_total"] = df["q1_total"] + df["q2_total"]
        df["h2_total"] = df["q3_total"] + df["q4_total"]
        # NCAA D1: q1/q2 columns represent halves
        ncaa_mask = df["tournament"].astype(str).str.replace("~", " ", regex=False)
        ncaa_mask = ncaa_mask.str.split().str.join(" ").str.upper() == "NCAA D1"
        if ncaa_mask.any():
            df.loc[ncaa_mask, "h1_total"] = df.loc[ncaa_mask, "q1_total"]
            df.loc[ncaa_mask, "h2_total"] = df.loc[ncaa_mask, "q2_total"]
        df["match_total"] = df["h1_total"] + df["h2_total"]
        summary_rows = []
        # Unique tournaments
        tournaments = sorted(df["tournament"].unique())
        for t in tournaments:
            t_df = df[df["tournament"] == t]
            games_count = len(t_df)
            if games_count == 0:
                continue
            # Average totals per match
            q1_avg = t_df["q1_total"].sum() / games_count
            q2_avg = t_df["q2_total"].sum() / games_count
            h1_avg = t_df["h1_total"].sum() / games_count
            q3_avg = t_df["q3_total"].sum() / games_count
            q4_avg = t_df["q4_total"].sum() / games_count
            h2_avg = t_df["h2_total"].sum() / games_count
            match_avg = t_df["match_total"].sum() / games_count
            # Unique teams count (NCAA D1: exclude teams below avg games)
            norm_t = " ".join(str(t).replace("~", " ").split()).upper()
            teams_set = set(t_df["team_home"]) | set(t_df["team_away"])
            if norm_t in ("NCAA D1", "NCAA D1 (W)"):
                team_games = {}
                for _, row in t_df.iterrows():
                    home = row["team_home"]
                    away = row["team_away"]
                    team_games[home] = team_games.get(home, 0) + 1
                    team_games[away] = team_games.get(away, 0) + 1
                if team_games:
                    avg_games = sum(team_games.values()) / float(len(team_games))
                    teams_count = sum(1 for g in team_games.values() if g >= avg_games)
                else:
                    teams_count = 0
            else:
                teams_count = len(teams_set)
            # Average deviation across teams in this tournament
            dev_df = self.get_team_deviations(t)
            if dev_df is not None and not dev_df.empty:
                tournament_deviation = dev_df["deviation"].mean()
            else:
                tournament_deviation = 0.0
            summary_rows.append({
                "tournament": t,
                "deviation": tournament_deviation,
                "q1_avg": q1_avg,
                "q2_avg": q2_avg,
                "h1_avg": h1_avg,
                "q3_avg": q3_avg,
                "q4_avg": q4_avg,
                "h2_avg": h2_avg,
                "match_avg": match_avg,
                "games_count": games_count,
                "teams_count": teams_count,
            })
        summary_df = pd.DataFrame(summary_rows)
        return summary_df.set_index("tournament")

    def get_tot_counts(self, q_threshold: float, h_threshold: float, m_threshold: float, tournament: Optional[str] = None) -> Dict[str, Dict[str, int]]:
        """Compute counts of over‑threshold occurrences for each team.

        For every team the method calculates how many times the sum of
        points in each quarter, each half and the entire match exceeded
        the specified thresholds.  It also counts the total number of
        games the team has played.  These counts are used to derive
        over/under coefficients for betting decisions.

        Parameters
        ----------
        q_threshold : float
            Threshold for individual quarters.
        h_threshold : float
            Threshold for halves (first and second).
        m_threshold : float
            Threshold for the entire match.
        tournament : str | None, optional
            Restrict the calculation to a specific tournament.

        Returns
        -------
        dict[str, dict[str, int]]
            A nested dictionary where the outer keys are team names and
            the inner dictionary contains counts:
                ``games`` – number of games played,
                ``q1`` .. ``q4`` – over‑threshold counts for each quarter,
                ``h1`` – over‑threshold count for the first half,
                ``h2`` – over‑threshold count for the second half,
                ``match`` – over‑threshold count for the whole match.
        """
        df = self._load_matches(tournament)
        if df.empty:
            return {}
        # Precompute quarter and half totals
        df = df.copy()
        df["q1_total"] = df["q1_home"].fillna(0) + df["q1_away"].fillna(0)
        df["q2_total"] = df["q2_home"].fillna(0) + df["q2_away"].fillna(0)
        df["q3_total"] = df["q3_home"].fillna(0) + df["q3_away"].fillna(0)
        df["q4_total"] = df["q4_home"].fillna(0) + df["q4_away"].fillna(0)
        df["h1_total"] = df["q1_total"] + df["q2_total"]
        df["h2_total"] = df["q3_total"] + df["q4_total"]
        df["match_total"] = df["h1_total"] + df["h2_total"]
        # Prepare dictionary to accumulate counts
        teams = sorted(set(df["team_home"]) | set(df["team_away"]))
        counts: Dict[str, Dict[str, int]] = {team: {
            "games": 0,
            "q1": 0,
            "q2": 0,
            "q3": 0,
            "q4": 0,
            "h1": 0,
            "h2": 0,
            "match": 0,
        } for team in teams}
        # Iterate through each match and update counts for both teams
        for _, row in df.iterrows():
            home = row["team_home"]
            away = row["team_away"]
            # Increase games count
            counts[home]["games"] += 1
            counts[away]["games"] += 1
            # Update over-threshold counts for each quarter
            for quarter, total_col in [("q1", "q1_total"), ("q2", "q2_total"), ("q3", "q3_total"), ("q4", "q4_total")]:
                if row[total_col] > q_threshold:
                    counts[home][quarter] += 1
                    counts[away][quarter] += 1
            # Halves
            if row["h1_total"] > h_threshold:
                counts[home]["h1"] += 1
                counts[away]["h1"] += 1
            if row["h2_total"] > h_threshold:
                counts[home]["h2"] += 1
                counts[away]["h2"] += 1
            # Match total
            if row["match_total"] > m_threshold:
                counts[home]["match"] += 1
                counts[away]["match"] += 1
        return counts

    def get_tot_coefficients(
        self,
        team1: str,
        team2: str,
        q_threshold: float,
        h_threshold: float,
        m_threshold: float,
        tournament: Optional[str] = None,
    ) -> Optional[Dict[str, Dict[str, float]]]:
        """Compute over/under coefficients for a pair of teams.

        This method mirrors the Excel formulas provided by the user for
        calculating betting coefficients.  For each period (quarters,
        halves and full match) it looks at how many times the combined
        score exceeded a given threshold for each team and divides the
        combined number of games by the combined number of overs.

        In Excel notation the over‑coefficient for a period is

            (games_team1 + games_team2) / (overs_team1 + overs_team2)

        whereas the under‑coefficient is computed as

            over / (over - 1)

        A larger value (>1.0) indicates that overs are less frequent
        relative to the number of games.  If no overs occur for a
        period the over coefficient is defined as 0 and the under
        coefficient is also 0 to avoid division by zero.

        Parameters
        ----------
        team1, team2 : str
            Names of the competing teams.
        q_threshold : float
            Threshold for individual quarters.
        h_threshold : float
            Threshold for halves.
        m_threshold : float
            Threshold for the full match.
        tournament : str | None, optional
            Restrict the calculation to a specific tournament.

        Returns
        -------
        dict | None
            A nested dictionary with keys ``"over"`` and ``"under"``.
            Each contains a mapping from period identifiers
            (``q1``, ``q2``, ``q3``, ``q4``, ``h1``, ``h2``, ``match``)
            to coefficients.  If either team is absent from the
            database ``None`` is returned.
        """
        counts = self.get_tot_counts(q_threshold, h_threshold, m_threshold, tournament)
        # Ensure both teams exist
        if team1 not in counts or team2 not in counts:
            return None
        result: Dict[str, Dict[str, float]] = {"over": {}, "under": {}}
        # Period keys correspond to the same keys used in get_tot_counts
        periods = ["q1", "q2", "q3", "q4", "h1", "h2", "match"]
        for period in periods:
            overs_sum = counts[team1][period] + counts[team2][period]
            games_sum = counts[team1]["games"] + counts[team2]["games"]
            # If no overs occurred or no games, the over coefficient is 0
            if overs_sum == 0 or games_sum == 0:
                over_coeff = 0.0
                under_coeff = 0.0
            else:
                over_coeff = float(games_sum) / float(overs_sum)
                # Under coefficient x/(x - 1).  When over_coeff == 1
                # (overs_sum == games_sum) the denominator is zero; treat
                # the under coefficient as infinite.  This matches the
                # behaviour in Excel where division by zero yields #DIV/0!
                if over_coeff == 1.0:
                    under_coeff = float('inf')
                else:
                    under_coeff = over_coeff / (over_coeff - 1.0)
            result["over"][period] = over_coeff
            result["under"][period] = under_coeff
        return result