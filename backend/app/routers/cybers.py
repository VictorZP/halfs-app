"""API endpoints for Cybers Bases and Cyber LIVE sections."""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.app.schemas.cybers import (
    CyberMatchRow,
    CyberMatchRowWithId,
    DeleteRequest,
    ImportRequest,
    ImportResponse,
    LiveMatchComputed,
    LiveMatchInput,
    LiveMatchUpdate,
    PredictRequest,
    PredictResponse,
    ReplaceRequest,
    SummaryRow,
)
from backend.app.services import cybers_service as svc

router = APIRouter(prefix="/cybers", tags=["Cybers"])


# ──────────────────────── Cybers Bases ────────────────────────


@router.get("/matches", response_model=List[dict])
def list_matches(tournament: Optional[str] = Query(None)):
    """Get all matches or filter by tournament."""
    if tournament:
        df = svc.get_dataframe_for_tournament(tournament)
    else:
        df = svc.get_dataframe()
    return df.to_dict(orient="records")


@router.post("/matches/import", response_model=ImportResponse)
def import_matches(req: ImportRequest):
    """Import matches from TSV text."""
    parsed, errors = svc.parse_import_tsv(req.raw_text)
    added = svc.add_rows(parsed)
    return ImportResponse(imported=added, errors=errors)


@router.post("/matches", response_model=dict)
def add_matches(rows: List[CyberMatchRow]):
    """Add matches programmatically."""
    dicts = [r.model_dump() for r in rows]
    count = svc.add_rows(dicts)
    return {"added": count}


@router.delete("/matches")
def delete_matches(req: DeleteRequest):
    deleted = svc.delete_rows(req.ids)
    return {"deleted": deleted}


@router.delete("/matches/all")
def clear_all_matches():
    svc.clear()
    return {"status": "ok"}


@router.delete("/matches/tournament/{tournament}")
def delete_tournament(tournament: str):
    deleted = svc.delete_tournament(tournament)
    return {"deleted": deleted}


@router.patch("/matches/{row_id}")
def update_match(row_id: int, field: str = Query(...), value: str = Query(...)):
    svc.update_match_field(row_id, field, value)
    return {"status": "ok"}


@router.get("/tournaments", response_model=List[str])
def list_tournaments():
    return svc.get_tournaments()


@router.get("/summary", response_model=List[SummaryRow])
def get_summary():
    return svc.get_summary()


@router.post("/duplicates")
def find_duplicates():
    dupes = svc.find_duplicate_pairs()
    return {"duplicates": dupes, "count": len(dupes)}


@router.post("/replace")
def replace_in_base(req: ReplaceRequest):
    count = svc.replace_in_base(req.find, req.replace, req.ids)
    return {"updated": count}


# ──────────────────────── Predict / Compute ────────────────────────


@router.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    """Compute predict for a specific match."""
    result = svc.compute_predict(req.tournament, req.team1, req.team2)
    return PredictResponse(**result)


# ──────────────────────── Cyber LIVE ────────────────────────


@router.get("/live", response_model=List[LiveMatchComputed])
def get_live_matches():
    """Get all live matches with computed values."""
    return svc.compute_all_live()


@router.post("/live", response_model=LiveMatchComputed)
def add_live_match(match: LiveMatchInput):
    """Add a new live match."""
    match_id = svc.add_live_match(
        match.tournament, match.team1, match.team2, match.total, match.calc_temp,
    )
    computed = svc.compute_live_match({
        "id": match_id,
        "tournament": match.tournament,
        "team1": match.team1,
        "team2": match.team2,
        "total": match.total,
        "calc_temp": match.calc_temp,
    })
    return LiveMatchComputed(**computed)


@router.put("/live/{match_id}", response_model=LiveMatchComputed)
def update_live_match(match_id: int, update: LiveMatchUpdate):
    """Update a live match field and recompute."""
    fields = {k: v for k, v in update.model_dump().items() if v is not None}
    svc.update_live_match(match_id, **fields)
    # Reload and recompute
    matches = svc.load_live_matches()
    match = next((m for m in matches if m["id"] == match_id), None)
    if not match:
        raise HTTPException(404, "Match not found")
    computed = svc.compute_live_match(match)
    return LiveMatchComputed(**computed)


@router.delete("/live/{match_id}")
def delete_live_match(match_id: int):
    svc.delete_live_match(match_id)
    return {"status": "ok"}


@router.delete("/live")
def clear_live():
    svc.clear_live_matches()
    return {"status": "ok"}
