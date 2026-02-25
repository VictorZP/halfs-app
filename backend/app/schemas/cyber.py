"""Pydantic schemas for Cyber API endpoints."""

from typing import List, Optional

from pydantic import BaseModel


class CyberMatch(BaseModel):
    id: Optional[int] = None
    date: str = ""
    tournament: str = ""
    team: str = ""
    home_away: str = ""
    two_pt_made: Optional[float] = 0
    two_pt_attempt: Optional[float] = 0
    three_pt_made: Optional[float] = 0
    three_pt_attempt: Optional[float] = 0
    fta_made: Optional[float] = 0
    fta_attempt: Optional[float] = 0
    off_rebound: Optional[float] = 0
    turnovers: Optional[float] = 0
    controls: Optional[float] = 0
    points: Optional[float] = 0
    opponent: str = ""
    attak_kef: Optional[float] = 0
    status: str = ""


class CyberImportRequest(BaseModel):
    raw_text: str


class CyberImportResponse(BaseModel):
    imported: int
    skipped: int
    skipped_lines: List[str]


class DeleteRequest(BaseModel):
    ids: List[int]


class CyberUpdateFieldRequest(BaseModel):
    field: str
    value: str


class NormalizeDatesResponse(BaseModel):
    updated: int


class CyberLiveRow(BaseModel):
    id: Optional[int] = None
    tournament: str = ""
    team1: str = ""
    team2: str = ""
    total: Optional[float] = None
    calc_temp: Optional[float] = 0


class CyberPredictResponse(BaseModel):
    predict: float
    temp: float
    it1: float
    it2: float


class CyberLiveArchiveRequest(BaseModel):
    live_row_id: Optional[int] = None
    tournament: str = ""
    team1: str = ""
    team2: str = ""
    total: Optional[float] = None
    calc_temp: Optional[float] = 0
    temp: Optional[float] = 0
    predict: Optional[float] = 0
    under_value: Optional[float] = None
    over_value: Optional[float] = None
    t2h: Optional[float] = 0
    t2h_predict: Optional[float] = None


class CyberLiveArchiveRow(BaseModel):
    id: int
    live_row_id: Optional[int] = None
    tournament: str = ""
    team1: str = ""
    team2: str = ""
    total: Optional[float] = None
    calc_temp: Optional[float] = 0
    temp: Optional[float] = 0
    predict: Optional[float] = 0
    under_value: Optional[float] = None
    over_value: Optional[float] = None
    t2h: Optional[float] = 0
    t2h_predict: Optional[float] = None
    archived_at: str = ""


class CyberReplaceValuesRequest(BaseModel):
    old_value: str
    new_value: str = ""
    scope: str = "all"  # all | tournament
    tournament: Optional[str] = None


class CyberReplaceValuesResponse(BaseModel):
    replaced: int


class CyberMergeTournamentsRequest(BaseModel):
    source_tournaments: List[str]
    target_tournament: str


class CyberMergeTournamentsResponse(BaseModel):
    updated: int


class CyberSummaryRow(BaseModel):
    tournament: str
    games: int
    two_pt_attempt: float
    two_pt_made: float
    three_pt_attempt: float
    three_pt_made: float
    fta_attempt: float
    fta_made: float
    off_rebound: float
    turnovers: float
    controls: float
    points: float
    p_per_control: float
    two_pt_pct: float
    three_pt_pct: float
    ft_pct: float
