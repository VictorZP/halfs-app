"""Pydantic schemas for Cybers API endpoints."""

from typing import List, Optional
from pydantic import BaseModel


class CyberMatchRow(BaseModel):
    date: Optional[str] = None
    tournament: Optional[str] = None
    team: Optional[str] = None
    home_away: Optional[str] = None
    two_pt_made: Optional[float] = None
    two_pt_attempt: Optional[float] = None
    three_pt_made: Optional[float] = None
    three_pt_attempt: Optional[float] = None
    fta_made: Optional[float] = None
    fta_attempt: Optional[float] = None
    off_rebound: Optional[float] = None
    turnovers: Optional[float] = None
    controls: Optional[float] = None
    points: Optional[float] = None
    opponent: Optional[str] = None
    attak_kef: Optional[float] = None
    status: Optional[str] = None


class CyberMatchRowWithId(CyberMatchRow):
    id: int


class ImportRequest(BaseModel):
    raw_text: str


class ImportResponse(BaseModel):
    imported: int
    errors: List[str]


class LiveMatchInput(BaseModel):
    tournament: str = ""
    team1: str = ""
    team2: str = ""
    total: Optional[float] = None
    calc_temp: float = 0.0


class LiveMatchUpdate(BaseModel):
    tournament: Optional[str] = None
    team1: Optional[str] = None
    team2: Optional[str] = None
    total: Optional[float] = None
    calc_temp: Optional[float] = None


class LiveMatchComputed(BaseModel):
    id: Optional[int] = None
    tournament: str = ""
    team1: str = ""
    team2: str = ""
    total: Optional[float] = None
    calc_temp: float = 0.0
    temp: float = 0.0
    predict: float = 0.0
    it1: float = 0.0
    it2: float = 0.0
    under: Optional[float] = None
    over: Optional[float] = None
    t2h: float = 0.0


class PredictRequest(BaseModel):
    tournament: str
    team1: str
    team2: str


class PredictResponse(BaseModel):
    predict: float
    temp: float
    it1: float
    it2: float


class SummaryRow(BaseModel):
    tournament: str
    games: int
    avg_2pta: float
    avg_2ptm: float
    avg_3pta: float
    avg_3ptm: float
    avg_fta: float
    avg_ftm: float
    avg_or: float
    avg_to: float
    avg_controls: float
    avg_points: float
    pc: float
    pct_2pt: float
    pct_3pt: float
    pct_ft: float


class ReplaceRequest(BaseModel):
    find: str
    replace: str
    ids: Optional[List[int]] = None


class DeleteRequest(BaseModel):
    ids: List[int]
