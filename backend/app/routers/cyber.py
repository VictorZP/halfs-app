"""API endpoints for Cyber sections (Cybers Bases + Cyber LIVE)."""

from typing import List, Optional

from fastapi import APIRouter, Query

from backend.app.schemas.cyber import (
    CyberImportRequest,
    CyberImportResponse,
    CyberLiveRow,
    CyberMatch,
    CyberPredictResponse,
    CyberSummaryRow,
    CyberUpdateFieldRequest,
    DeleteRequest,
    NormalizeDatesResponse,
)
from backend.app.services import cyber_service as svc

router = APIRouter(prefix="/cyber", tags=["Cyber"])


@router.get("/matches", response_model=List[CyberMatch])
def list_matches(tournament: Optional[str] = Query(None), limit: int = Query(10000)):
    return svc.get_matches(tournament=tournament, limit=limit)


@router.post("/matches/import", response_model=CyberImportResponse)
def import_matches(req: CyberImportRequest):
    imported, skipped, skipped_lines = svc.import_matches(req.raw_text)
    return CyberImportResponse(imported=imported, skipped=skipped, skipped_lines=skipped_lines)


@router.delete("/matches")
def delete_matches(req: DeleteRequest):
    deleted = svc.delete_matches(req.ids)
    return {"deleted": deleted}


@router.patch("/matches/{row_id}")
def update_match(row_id: int, req: CyberUpdateFieldRequest):
    ok = svc.update_match_field(row_id=row_id, field=req.field, value=req.value)
    if not ok:
        return {"status": "error"}
    return {"status": "ok"}


@router.post("/matches/normalize-dates", response_model=NormalizeDatesResponse)
def normalize_dates():
    updated = svc.normalize_existing_dates()
    return NormalizeDatesResponse(updated=updated)


@router.delete("/matches/all")
def clear_all():
    svc.clear_matches()
    return {"status": "ok"}


@router.get("/tournaments", response_model=List[str])
def list_tournaments():
    return svc.get_tournaments()


@router.get("/statistics")
def statistics():
    return svc.get_statistics()


@router.get("/summary", response_model=List[CyberSummaryRow])
def summary(tournament: Optional[str] = Query(None)):
    return svc.get_summary(tournament=tournament)


@router.get("/predict", response_model=CyberPredictResponse)
def predict(
    tournament: str = Query(...),
    team1: str = Query(...),
    team2: str = Query(...),
):
    predict_value, temp, it1, it2 = svc.compute_predict(tournament=tournament, team1=team1, team2=team2)
    return CyberPredictResponse(predict=predict_value, temp=temp, it1=it1, it2=it2)


@router.get("/live", response_model=List[CyberLiveRow])
def get_live():
    rows = svc.get_live_rows()
    return [
        CyberLiveRow(
            tournament=r.get("tournament") or "",
            team1=r.get("team1") or "",
            team2=r.get("team2") or "",
            total=r.get("total"),
            calc_temp=r.get("calc_temp") if r.get("calc_temp") is not None else 0,
        )
        for r in rows
    ]


@router.put("/live")
def replace_live(rows: List[CyberLiveRow]):
    payload = [
        (r.tournament, r.team1, r.team2, r.total, r.calc_temp if r.calc_temp is not None else 0)
        for r in rows
    ]
    svc.replace_live_rows(payload)
    return {"saved": len(payload)}


@router.delete("/live")
def clear_live():
    svc.clear_live_rows()
    return {"status": "ok"}
