"""Pydantic schemas for Halfs API endpoints."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class HalfsMatch(BaseModel):
    id: Optional[int] = None
    date: str = ""
    tournament: str = ""
    team_home: str = ""
    team_away: str = ""
    q1_home: Optional[int] = 0
    q1_away: Optional[int] = 0
    q2_home: Optional[int] = 0
    q2_away: Optional[int] = 0
    q3_home: Optional[int] = 0
    q3_away: Optional[int] = 0
    q4_home: Optional[int] = 0
    q4_away: Optional[int] = 0
    ot_home: Optional[int] = 0
    ot_away: Optional[int] = 0
    created_at: Optional[datetime] = None


class ImportRequest(BaseModel):
    raw_text: str


class ImportResponse(BaseModel):
    imported: int
    errors: List[str]


class ImportPreviewRow(BaseModel):
    date: str = ""
    tournament: str = ""
    team_home: str = ""
    team_away: str = ""
    q1_home: Optional[int] = 0
    q1_away: Optional[int] = 0
    q2_home: Optional[int] = 0
    q2_away: Optional[int] = 0
    q3_home: Optional[int] = 0
    q3_away: Optional[int] = 0
    q4_home: Optional[int] = 0
    q4_away: Optional[int] = 0
    ot_home: Optional[int] = 0
    ot_away: Optional[int] = 0


class ImportPreviewResponse(BaseModel):
    parsed_count: int
    error_count: int
    parsed_rows: List[ImportPreviewRow]
    errors: List[str]


class UpdateMatchFieldRequest(BaseModel):
    field: str
    value: str


class TeamStats(BaseModel):
    team: str
    games: int
    q1_scored: float
    q2_scored: float
    q3_scored: float
    q4_scored: float
    q1_conceded: float
    q2_conceded: float
    q3_conceded: float
    q4_conceded: float
    h1_scored: float
    h2_scored: float
    h1_conceded: float
    h2_conceded: float
    total_scored: float
    total_conceded: float


class TournamentSummary(BaseModel):
    tournament: str
    games: int
    teams: int
    avg_total: float
    avg_h1: float
    avg_h2: float


class DeleteRequest(BaseModel):
    ids: List[int]


class StatisticsResponse(BaseModel):
    total_matches: int
    tournaments: int
    teams: int


class NormalizeDatesResponse(BaseModel):
    updated: int


class ReplaceValuesRequest(BaseModel):
    old_value: str
    new_value: str = ""
    scope: str = "all"  # all | tournament
    tournament: Optional[str] = None


class ReplaceValuesResponse(BaseModel):
    replaced: int


class MergeTournamentsRequest(BaseModel):
    source_tournaments: List[str]
    target_tournament: str


class MergeTournamentsResponse(BaseModel):
    updated: int
