"""Pydantic schemas for Royka API endpoints."""

from typing import List, Optional
from pydantic import BaseModel


class RoykaMatch(BaseModel):
    id: Optional[int] = None
    date: str = ""
    tournament: str = ""
    team_home: str = ""
    team_away: str = ""
    t1h: Optional[float] = None
    t2h: Optional[float] = None
    tim: float = 0.0
    deviation: Optional[float] = None
    kickoff: Optional[float] = None
    predict: str = "0"
    result: Optional[float] = None


class RoykaMatchInput(BaseModel):
    """Flat tuple-like input for batch import."""
    date: str
    tournament: str
    team_home: str
    team_away: str
    t1h: Optional[float] = None
    t2h: Optional[float] = None
    tim: float = 0.0
    deviation: Optional[float] = None
    kickoff: Optional[float] = None
    predict: str = "0"
    result: Optional[float] = None


class TournamentStats(BaseModel):
    tournament: str
    total: int
    over: int = 0
    under: int = 0
    no_bet: int = 0
    win: int = 0
    lose: int = 0
    win_rate: float = 0.0


class StatisticsResponse(BaseModel):
    total_records: int
    tournaments_count: int
    teams_count: int
    last_update: Optional[str] = None


class DeleteRequest(BaseModel):
    ids: List[int]
