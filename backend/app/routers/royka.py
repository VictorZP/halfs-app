"""API endpoints for the Ройка (Royka) section."""

from typing import List, Optional

from fastapi import APIRouter, Query

from backend.app.schemas.royka import (
    DeleteRequest,
    RoykaMatch,
    RoykaMatchInput,
    StatisticsResponse,
    TournamentStats,
)
from backend.app.services import royka_service as svc

router = APIRouter(prefix="/royka", tags=["Royka"])


@router.get("/matches", response_model=List[RoykaMatch])
def list_matches(tournament: Optional[str] = Query(None), limit: int = Query(10000)):
    rows = svc.get_matches(tournament=tournament, limit=limit)
    return rows


@router.post("/matches")
def add_matches(matches: List[RoykaMatchInput]):
    data = [
        (m.date, m.tournament, m.team_home, m.team_away,
         m.t1h, m.t2h, m.tim, m.deviation, m.kickoff, m.predict, m.result)
        for m in matches
    ]
    count = svc.add_matches(data)
    return {"added": count}


@router.delete("/matches")
def delete_matches(req: DeleteRequest):
    deleted = svc.delete_matches(req.ids)
    return {"deleted": deleted}


@router.delete("/matches/all")
def clear_all():
    svc.clear_database()
    return {"status": "ok"}


@router.get("/tournaments", response_model=List[str])
def list_tournaments():
    return svc.get_tournaments()


@router.get("/statistics", response_model=StatisticsResponse)
def get_statistics():
    return svc.get_statistics()


@router.get("/analysis/{tournament}", response_model=TournamentStats)
def analyze_tournament(tournament: str):
    return svc.analyze_tournament(tournament)


@router.get("/analysis", response_model=List[TournamentStats])
def analyze_all():
    return svc.get_all_tournaments_stats()
