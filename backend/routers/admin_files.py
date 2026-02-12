from __future__ import annotations

import json
from io import BytesIO

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from authentication.deps import require_admin
from db.deps import get_db
from models.data_rows import DataRow
from services.analytics_repository import invalidate_dataframe_cache
from services.manual_update_service import mark_manual_update

router = APIRouter(
    prefix="/admin/files",
    tags=["admin-files"],
    dependencies=[Depends(require_admin)],
)


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _clean_json_row(row: dict) -> dict:
    out: dict = {}
    for key, value in row.items():
        if value is None:
            out[key] = None
            continue
        if isinstance(value, float) and (value != value or value == float("inf") or value == float("-inf")):
            out[key] = None
            continue
        out[key] = value
    return out


def _apply_tag_filter(query, source: str, dataset_type: str, job_id: str | None):
    query = query.filter(DataRow.source == source, DataRow.dataset_type == dataset_type)
    if job_id is None:
        query = query.filter(DataRow.job_id.is_(None))
    else:
        query = query.filter(DataRow.job_id == job_id)
    return query


@router.get("")
def list_file_groups(
    source: str | None = Query(None),
    dataset_type: str | None = Query(None),
    job_id: str | None = Query(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)

    query = (
        db.query(
            DataRow.source.label("source"),
            DataRow.dataset_type.label("dataset_type"),
            DataRow.job_id.label("job_id"),
            func.count(DataRow.id).label("rows"),
            func.max(DataRow.id).label("latest_row_id"),
        )
        .group_by(DataRow.source, DataRow.dataset_type, DataRow.job_id)
        .order_by(func.max(DataRow.id).desc())
    )

    if source_norm:
        query = query.filter(DataRow.source == source_norm)
    if dataset_norm:
        query = query.filter(DataRow.dataset_type == dataset_norm)
    if job_id is not None:
        if job_norm is None:
            query = query.filter(DataRow.job_id.is_(None))
        else:
            query = query.filter(DataRow.job_id == job_norm)

    rows = query.all()
    items = [
        {
            "source": r.source,
            "dataset_type": r.dataset_type,
            "job_id": r.job_id,
            "tag": f"{r.source}:{r.dataset_type}:{r.job_id or 'untagged'}",
            "rows": int(r.rows or 0),
            "latest_row_id": int(r.latest_row_id) if r.latest_row_id is not None else None,
        }
        for r in rows
    ]
    return {"items": items}


@router.get("/download")
def download_file_group(
    source: str = Query(...),
    dataset_type: str = Query(...),
    job_id: str | None = Query(None),
    format: str = Query("csv"),
    db: Session = Depends(get_db),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    fmt = (format or "csv").strip().lower()

    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if fmt not in {"csv", "json"}:
        raise HTTPException(status_code=400, detail="format must be csv or json")

    query = _apply_tag_filter(db.query(DataRow.data), source_norm, dataset_norm, job_norm)
    rows = query.all()
    payloads = [r[0] if isinstance(r, tuple) else r.data for r in rows]
    payloads = [p for p in payloads if isinstance(p, dict)]

    if not payloads:
        raise HTTPException(status_code=404, detail="No data found for provided tag")

    file_tag = f"{source_norm}_{dataset_norm}_{job_norm or 'untagged'}"

    if fmt == "json":
        content = json.dumps(payloads).encode("utf-8")
        media_type = "application/json"
        filename = f"{file_tag}.json"
    else:
        df = pd.DataFrame(payloads)
        content = df.to_csv(index=False).encode("utf-8")
        media_type = "text/csv"
        filename = f"{file_tag}.csv"

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("")
def delete_file_group(
    source: str = Query(...),
    dataset_type: str = Query(...),
    job_id: str | None = Query(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")

    query = _apply_tag_filter(db.query(DataRow), source_norm, dataset_norm, job_norm)
    deleted = query.delete(synchronize_session=False)
    db.commit()
    invalidate_dataframe_cache(source=source_norm, dataset_type=dataset_norm, job_id=job_norm)

    return {
        "deleted_rows": int(deleted or 0),
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
    }


@router.post("/replace")
async def replace_file_group(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file")

    filename = (file.filename or "").lower()
    buffer = BytesIO(contents)
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(buffer)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(buffer)
        else:
            raise HTTPException(status_code=400, detail="Only .csv, .xls, and .xlsx are supported")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")

    df = df.astype(object).where(pd.notnull(df), None)
    payloads = [_clean_json_row(row) for row in df.to_dict(orient="records")]

    delete_query = _apply_tag_filter(db.query(DataRow), source_norm, dataset_norm, job_norm)
    deleted = delete_query.delete(synchronize_session=False)

    db.add_all(
        [
            DataRow(
                source=source_norm,
                dataset_type=dataset_norm,
                job_id=job_norm,
                data=payload,
            )
            for payload in payloads
        ]
    )
    db.commit()
    mark_manual_update(
        db=db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
    )
    db.commit()

    invalidate_dataframe_cache(source=source_norm, dataset_type=dataset_norm, job_id=job_norm)
    return {
        "deleted_rows": int(deleted or 0),
        "rows_inserted": len(payloads),
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
    }
