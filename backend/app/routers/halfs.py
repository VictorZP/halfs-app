"""API endpoints for the Halfs (База половин) section."""

from typing import List, Optional

from fastapi import APIRouter, Query

from backend.app.schemas.halfs import (
    DeleteRequest,
    HalfsMatch,
    ImportRequest,
    ImportResponse,
    StatisticsResponse,
    TeamStats,
    TournamentSummary,
)
from backend.app.services import halfs_service as svc

router = APIRouter(prefix="/halfs", tags=["Halfs"])


@router.get("/matches", response_model=List[HalfsMatch])
def list_matches(tournament: Optional[str] = Query(None), limit: int = Query(10000)):
    return svc.get_all_matches(tournament=tournament, limit=limit)


@router.post("/matches/import", response_model=ImportResponse)
def import_matches(req: ImportRequest):
    count, errors = svc.import_matches(req.raw_text)
    return ImportResponse(imported=count, errors=errors)


@router.delete("/matches")
def delete_matches(req: DeleteRequest):
    deleted = svc.delete_matches(req.ids)
    return {"deleted": deleted}


@router.delete("/matches/all")
def clear_all():
    svc.clear_all()
    return {"status": "ok"}


@router.get("/tournaments", response_model=List[str])
def list_tournaments():
    return svc.get_tournaments()


@router.get("/statistics", response_model=StatisticsResponse)
def get_statistics():
    return svc.get_statistics()


@router.get("/team-stats/{tournament}", response_model=List[TeamStats])
def get_team_stats(tournament: str):
    return svc.get_team_statistics(tournament)


@router.get("/summary", response_model=List[TournamentSummary])
def get_tournament_summary():
    return svc.get_tournament_summary()
