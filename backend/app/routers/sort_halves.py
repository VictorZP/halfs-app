"""API endpoints for halfs sorting between two Excel files."""

from __future__ import annotations

import json
import os
from io import BytesIO

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from backend.app.services.sort_halves_service import get_workbook_sheet_names, process_sort_halves

router = APIRouter(prefix="/sort-halves", tags=["Sort Halves"])


@router.post("/process")
async def process_files(
    source_file: UploadFile = File(...),
    destination_file: UploadFile = File(...),
):
    src_name = source_file.filename or "source.xlsx"
    dst_name = destination_file.filename or "destination.xlsx"
    allowed_ext = {".xlsx", ".xlsm"}
    src_ext = os.path.splitext(src_name)[1].lower()
    dst_ext = os.path.splitext(dst_name)[1].lower()
    if src_ext not in allowed_ext or dst_ext not in allowed_ext:
        raise HTTPException(400, "Поддерживаются только файлы .xlsx/.xlsm")

    src_bytes = await source_file.read()
    dst_bytes = await destination_file.read()
    if not src_bytes or not dst_bytes:
        raise HTTPException(400, "Один из файлов пустой")

    try:
        out_bytes, summary = process_sort_halves(src_bytes, dst_bytes)
    except Exception as exc:
        raise HTTPException(400, f"Ошибка обработки файлов: {exc}") from exc

    summary_json = json.dumps(
        [{"tournament": t, "inserted": v[0], "normative": v[1]} for t, v in sorted(summary.items())],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    out_name = f"sorted_{os.path.basename(dst_name)}"

    response = StreamingResponse(
        BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response.headers["Content-Disposition"] = f'attachment; filename="{out_name}"'
    response.headers["X-Games-Summary"] = summary_json
    return response


@router.post("/sheets")
async def get_sheets(destination_file: UploadFile = File(...)):
    name = destination_file.filename or "destination.xlsx"
    ext = os.path.splitext(name)[1].lower()
    if ext not in {".xlsx", ".xlsm"}:
        raise HTTPException(400, "Поддерживаются только файлы .xlsx/.xlsm")

    file_bytes = await destination_file.read()
    if not file_bytes:
        raise HTTPException(400, "Файл пустой")

    try:
        sheets = get_workbook_sheet_names(file_bytes)
    except Exception as exc:
        raise HTTPException(400, f"Не удалось прочитать листы файла: {exc}") from exc

    return {"sheets": sheets}
