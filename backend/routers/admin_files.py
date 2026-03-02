from __future__ import annotations

import json
import logging
import math
from io import BytesIO
from datetime import date, datetime

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from authentication.deps import require_admin
from db.deps import get_db
from models.data_rows import DataRow
from services.ai_mapper import suggest_reverse_mapping
from services.analytics_repository import invalidate_dataframe_cache
from services.analytics.samsung_engine import invalidate_samsung_load_cache
from services.deck_cache_service import invalidate_deck_cache_for_source_dataset
from services.manual_update_service import mark_manual_update
from services.precompute_service import rebuild_precomputed_analytics, rebuild_precomputed_for_all_tags
from services.precomputed_repository import clear_precomputed_for_source_dataset

router = APIRouter(
    prefix="/admin/files",
    tags=["admin-files"],
    dependencies=[Depends(require_admin)],
)
logger = logging.getLogger(__name__)


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _normalize_source_for_mapper(source: str) -> str:
    source_key = (source or "").strip().lower()
    if source_key in {"samsung_vs", "samsung_vijay_sales", "samsung_croma"}:
        return "samsung"
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"goodrej", "goddrej"}:
        return "godrej"
    return source_key


def _clean_json_row(row: dict) -> dict:
    return {key: _json_safe(value) for key, value in row.items()}


def _json_safe(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)

    return value


def _apply_tag_filter(query, source: str, dataset_type: str, job_id: str | None):
    query = query.filter(DataRow.source == source, DataRow.dataset_type == dataset_type)
    if job_id is None:
        query = query.filter(DataRow.job_id.is_(None))
    else:
        query = query.filter(DataRow.job_id == job_id)
    return query


def _replace_tag_rows(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    payloads: list[dict],
) -> int:
    # Hard overwrite at source+dataset level so latest upload becomes the
    # single source of truth and no legacy job-tag rows remain.
    delete_query = db.query(DataRow).filter(
        DataRow.source == source,
        DataRow.dataset_type == dataset_type,
    )
    deleted = int(delete_query.delete(synchronize_session=False) or 0)
    clear_precomputed_for_source_dataset(db, source=source, dataset_type=dataset_type)
    if payloads:
        db.add_all(
            [
                DataRow(
                    source=source,
                    dataset_type=dataset_type,
                    job_id=job_id,
                    data=payload,
                )
                for payload in payloads
            ]
        )
    return deleted


async def _parse_upload_payloads(file: UploadFile) -> list[dict]:
    df = await _parse_upload_dataframe(file)
    df = df.astype(object).where(pd.notnull(df), None)
    return [_clean_json_row(row) for row in df.to_dict(orient="records")]


async def _parse_upload_dataframe(file: UploadFile) -> pd.DataFrame:
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file")

    filename = (file.filename or "").lower()
    buffer = BytesIO(contents)
    try:
        if filename.endswith(".csv"):
            return pd.read_csv(buffer)
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            return pd.read_excel(buffer)
        raise HTTPException(status_code=400, detail="Only .csv, .xls, and .xlsx are supported")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")


def _post_file_update(
    db: Session,
    source_norm: str,
    dataset_norm: str,
    job_norm: str | None,
    action: str,
):
    invalidate_deck_cache_for_source_dataset(
        db=db,
        source=source_norm,
        dataset_type=dataset_norm,
    )

    refresh_sources: list[str] = [source_norm]
    if source_norm in {"samsung_vs", "samsung_vijay_sales", "samsung_croma"}:
        refresh_sources.append("samsung")
    if source_norm == "samsung_vijay_sales":
        refresh_sources.append("samsung_vs")
    refresh_sources = list(dict.fromkeys(refresh_sources))

    # Refresh both the exact tag and the aggregate ("all tags") view when a job-specific
    # file changes, so dashboard charts/summary without job filter stay in sync.
    refresh_jobs: list[str | None] = [job_norm]
    if job_norm is not None:
        refresh_jobs.append(None)

    for refresh_source in refresh_sources:
        for refresh_job in refresh_jobs:
            mark_manual_update(
                db=db,
                source=refresh_source,
                dataset_type=dataset_norm,
                job_id=refresh_job,
            )
    db.commit()

    for refresh_source in refresh_sources:
        for refresh_job in refresh_jobs:
            invalidate_dataframe_cache(source=refresh_source, dataset_type=dataset_norm, job_id=refresh_job)
            if refresh_source.startswith("samsung"):
                invalidate_samsung_load_cache(
                    source=refresh_source,
                    dataset_type=dataset_norm,
                    job_id=refresh_job,
                )

    for refresh_source in refresh_sources:
        for refresh_job in refresh_jobs:
            try:
                rebuild_precomputed_analytics(
                    db=db,
                    source=refresh_source,
                    dataset_type=dataset_norm,
                    job_id=refresh_job,
                )
            except Exception:
                db.rollback()
                logger.exception(
                    "Failed to rebuild precomputed analytics after %s source=%s dataset=%s job_id=%s",
                    action,
                    refresh_source,
                    dataset_norm,
                    refresh_job,
                )

    # A concurrent read can repopulate samsung's shared in-memory load cache while
    # precompute is rebuilding. Clear once more so subsequent reads are guaranteed fresh.
    for refresh_source in refresh_sources:
        if not refresh_source.startswith("samsung"):
            continue
        for refresh_job in refresh_jobs:
            invalidate_samsung_load_cache(
                source=refresh_source,
                dataset_type=dataset_norm,
                job_id=refresh_job,
            )


def _normalize_col_key(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("_", "")
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )


def _validate_payload_schema(
    *,
    source_norm: str,
    dataset_norm: str,
    payloads: list[dict],
) -> None:
    # Prevent accidental Reliance sales replacement with pivot/summary exports.
    if source_norm != "reliance" or dataset_norm != "sales":
        return
    if not payloads:
        raise HTTPException(status_code=400, detail="Reliance sales upload is empty.")

    sample_rows = payloads[: min(len(payloads), 300)]
    raw_columns = {str(k).strip() for row in sample_rows for k in row.keys()}
    normalized_columns = {_normalize_col_key(k) for k in raw_columns if str(k).strip()}

    date_cols = {
        "planstartdate",
        "warrantystartdate",
        "warrantystartdate",
        "purchasedate",
        "invoicedate",
        "month",
    }
    value_cols = {
        "plansellingprice",
        "totalbillingamount",
        "billingamount",
        "invoicevalue",
        "handsetvalue",
        "zoppersharedtransferprice",
    }

    only_unnamed = bool(normalized_columns) and all(
        col.startswith("unnamed") for col in normalized_columns
    )
    has_date_col = bool(normalized_columns & date_cols)
    has_value_col = bool(normalized_columns & value_cols)

    if only_unnamed or not has_date_col or not has_value_col:
        sample_cols = ", ".join(sorted(raw_columns)[:10]) or "none"
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Reliance sales file format. Upload row-level Reliance sales data (not pivot/summary) "
                "with fields like Warranty Start Date/Plan Start Date and INVOICE_VALUE/Total Billing Amount. "
                f"Detected columns: {sample_cols}"
            ),
        )


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
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="delete")

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
    payloads = await _parse_upload_payloads(file)
    _validate_payload_schema(
        source_norm=source_norm,
        dataset_norm=dataset_norm,
        payloads=payloads,
    )
    deleted = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        payloads=payloads,
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="replace")
    return {
        "deleted_rows": deleted,
        "rows_inserted": len(payloads),
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
    }


@router.post("/update")
async def update_file_group(
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

    payloads = await _parse_upload_payloads(file)
    _validate_payload_schema(
        source_norm=source_norm,
        dataset_norm=dataset_norm,
        payloads=payloads,
    )
    deleted = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        payloads=payloads,
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="update")
    return {
        "deleted_rows": deleted,
        "rows_inserted": len(payloads),
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
    }


@router.post("/reverse-map")
async def reverse_map_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")

    df = await _parse_upload_dataframe(file)
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to map")

    mapped_source = _normalize_source_for_mapper(source_norm)
    suggestion = suggest_reverse_mapping(
        df,
        source=mapped_source,
        dataset_type=dataset_norm,
    )
    suggestion["file_name"] = file.filename or ""
    return suggestion


@router.post("/recompute")
def recompute_precomputed_data(
    source: str | None = Query(None),
    dataset_type: str | None = Query(None),
    job_id: str | None = Query(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    try:
        result = rebuild_precomputed_for_all_tags(
            db=db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=job_norm,
        )
    except Exception:
        db.rollback()
        logger.exception(
            "Failed manual precompute rebuild source=%s dataset=%s job_id=%s",
            source_norm,
            dataset_norm,
            job_norm,
        )
        raise HTTPException(status_code=500, detail="Failed to rebuild precomputed analytics")
    return result
