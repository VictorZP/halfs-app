"""API endpoints for the Halfs (База половин) section."""

from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException

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


# ──────────────────────── Deviations (Отклонения) ────────────────────────

@router.get("/deviations/{tournament}")
def get_deviations(tournament: str):
    return svc.get_team_deviations(tournament)


# ──────────────────────── Wins/Losses (Победы/поражения) ────────────────────────

@router.get("/wins-losses/{tournament}")
def get_wins_losses(tournament: str):
    return svc.get_wins_losses(tournament)


# ──────────────────────── Quarter distribution (Средние четверти) ────────────────────────

@router.get("/quarter-distribution/{tournament}")
def get_quarter_distribution(
    tournament: str,
    team1: str = Query(...),
    team2: str = Query(...),
    total: float = Query(...),
):
    result = svc.get_quarter_distribution(tournament, team1, team2, total)
    if result is None:
        raise HTTPException(404, "Teams not found in tournament")
    return result


# ──────────────────────── Coefficients (Коэффициенты) ────────────────────────

@router.get("/coefficients/{tournament}")
def get_coefficients(
    tournament: str,
    team1: str = Query(...),
    team2: str = Query(...),
    q_threshold: float = Query(..., description="Threshold for quarters"),
    h_threshold: float = Query(..., description="Threshold for halves"),
    m_threshold: float = Query(..., description="Threshold for match"),
):
    result = svc.get_coefficients(tournament, team1, team2, q_threshold, h_threshold, m_threshold)
    if result is None:
        raise HTTPException(404, "Teams not found or no data")
    return result
