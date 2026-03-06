from __future__ import annotations

import gzip
import json
import logging
import math
import threading
from io import BytesIO
from datetime import date, datetime
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from authentication.deps import require_admin
from db.session import SessionLocal
from db.deps import get_db
from models.data_rows import DataRow
from services.ai_mapper import suggest_reverse_mapping
from services.data_quality_service import get_primary_key_candidate_order, prepare_rows_for_storage
from services.analytics_repository import invalidate_dataframe_cache
from services.analytics.samsung_engine import invalidate_samsung_load_cache
from services.deck_cache_service import invalidate_deck_cache_for_source_dataset
from services.manual_update_service import mark_manual_update
from services.precompute_service import rebuild_precomputed_analytics, rebuild_precomputed_for_all_tags
from services.precomputed_repository import clear_precomputed_for_source_dataset
from services.partner_filter_service import (
    dataframe_to_payload_rows,
    normalize_partner_dataframe,
)

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


def _normalize_source_key(value: str | None) -> str | None:
    source_key = _normalize(value)
    if source_key is None:
        return None
    if source_key in {"samsung_vs", "samsung_vijay_sales", "samsung vs", "samsung vijay sales", "vijay sales"}:
        return "samsung_vs"
    if source_key in {"samsung_croma", "samsung croma", "croma"}:
        return "samsung_croma"
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return source_key


def _normalize_source_for_mapper(source: str) -> str:
    source_key = _normalize_source_key(source) or ""
    if source_key in {"samsung_vs", "samsung_vijay_sales", "samsung_croma"}:
        return "samsung"
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
    mode: str = "merge",
) -> tuple[int, int, dict[str, Any]]:
    mode_key = str(mode or "merge").strip().lower()
    storage_rows, quality_meta = prepare_rows_for_storage(
        payloads,
        source=source,
        dataset_type=dataset_type,
    )

    def _is_missing(v: Any) -> bool:
        if v is None:
            return True
        text = str(v).strip().lower()
        return text in {"", "nan", "none", "null"}

    def _merge_payload(old_payload: dict[str, Any], new_payload: dict[str, Any]) -> dict[str, Any]:
        merged = dict(old_payload or {})
        for key, value in (new_payload or {}).items():
            if key not in merged:
                merged[key] = value
                continue
            if not _is_missing(value):
                merged[key] = value
        return merged

    deleted = 0
    inserted = 0
    updated = 0

    if mode_key == "replace":
        delete_query = db.query(DataRow).filter(
            DataRow.source == source,
            DataRow.dataset_type == dataset_type,
        )
        if job_id is None:
            delete_query = delete_query.filter(DataRow.job_id.is_(None))
        else:
            delete_query = delete_query.filter(DataRow.job_id == job_id)
        deleted = int(delete_query.delete(synchronize_session=False) or 0)
        insert_payloads: list[dict[str, Any]] = [
            {
                "source": source,
                "dataset_type": dataset_type,
                "job_id": job_id,
                "data": item.get("data") if isinstance(item, dict) else item,
                "record_key": (item or {}).get("record_key") if isinstance(item, dict) else None,
                "primary_key_name": (item or {}).get("primary_key_name") if isinstance(item, dict) else None,
            }
            for item in storage_rows
        ]
        if insert_payloads:
            db.bulk_insert_mappings(DataRow, insert_payloads)
        inserted = int(len(insert_payloads))
    else:
        incoming_keys = [
            str(item.get("record_key") or "").strip()
            for item in storage_rows
            if isinstance(item, dict) and str(item.get("record_key") or "").strip()
        ]
        unique_incoming_keys = list(dict.fromkeys(incoming_keys))
        existing_by_key: dict[str, DataRow] = {}
        if unique_incoming_keys:
            chunk_size = 2000
            for offset in range(0, len(unique_incoming_keys), chunk_size):
                chunk = unique_incoming_keys[offset : offset + chunk_size]
                scoped_query = db.query(DataRow).filter(
                    DataRow.source == source,
                    DataRow.dataset_type == dataset_type,
                    DataRow.record_key.in_(chunk),
                )
                if job_id is None:
                    scoped_query = scoped_query.filter(DataRow.job_id.is_(None))
                else:
                    scoped_query = scoped_query.filter(DataRow.job_id == job_id)
                for row in scoped_query.all():
                    key = str(row.record_key or "").strip()
                    if key:
                        existing_by_key[key] = row

        insert_payloads: list[dict[str, Any]] = []
        for item in storage_rows:
            if not isinstance(item, dict):
                continue
            rk = str(item.get("record_key") or "").strip()
            payload = item.get("data") if isinstance(item.get("data"), dict) else {}
            pk_name = str(item.get("primary_key_name") or "").strip() or None
            existing = existing_by_key.get(rk) if rk else None
            if existing is None:
                insert_payloads.append(
                    {
                        "source": source,
                        "dataset_type": dataset_type,
                        "job_id": job_id,
                        "data": payload,
                        "record_key": rk or None,
                        "primary_key_name": pk_name,
                    }
                )
                inserted += 1
                continue
            existing.data = _merge_payload(existing.data if isinstance(existing.data, dict) else {}, payload)
            if pk_name:
                existing.primary_key_name = pk_name
            if job_id is not None:
                existing.job_id = job_id
            updated += 1
        if insert_payloads:
            db.bulk_insert_mappings(DataRow, insert_payloads)

    clear_precomputed_for_source_dataset(db, source=source, dataset_type=dataset_type)
    quality_meta["merge_mode"] = "replace" if mode_key == "replace" else "upsert"
    quality_meta["updated_rows"] = int(updated)
    quality_meta["inserted_rows"] = int(inserted)
    quality_meta["deleted_rows"] = int(deleted)
    return int(deleted), int(inserted), quality_meta


async def _parse_upload_payloads(
    file: UploadFile,
    *,
    source: str | None = None,
    dataset_type: str | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    df = await _parse_upload_dataframe(file)
    normalize_meta: dict[str, Any] = {
        "source": (source or "").strip().lower(),
        "dataset_type": (dataset_type or "").strip().lower(),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "smart_mapping": {"applied": 0, "required_found": 0, "required_total": 0, "coverage": 0.0},
        "columns_touched": 0,
    }
    if source and dataset_type:
        df, normalize_meta = normalize_partner_dataframe(
            df,
            source=source,
            dataset_type=dataset_type,
        )
    return dataframe_to_payload_rows(df), normalize_meta


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


def _snapshot_rows(rows: list[DataRow]) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for row in rows:
        payload = row.data if isinstance(row.data, dict) else {}
        snapshot.append(
            {
                "job_id": row.job_id,
                "source": row.source,
                "dataset_type": row.dataset_type,
                "record_key": row.record_key,
                "primary_key_name": row.primary_key_name,
                "data": _clean_json_row(payload),
            }
        )
    return snapshot


def _compress_snapshot(snapshot: list[dict[str, Any]]) -> bytes:
    raw = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    return gzip.compress(raw, compresslevel=6)


def _decompress_snapshot(blob: bytes) -> list[dict[str, Any]]:
    try:
        raw = gzip.decompress(blob or b"")
    except Exception:
        raw = blob or b""
    if not raw:
        return []
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _save_filter_revision(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    before_snapshot: list[dict[str, Any]],
    after_rows: int,
) -> int | None:
    params = {
        "source": source,
        "dataset_type": dataset_type,
        "job_key": str(job_id or ""),
        "before_blob": _compress_snapshot(before_snapshot),
        "before_rows": int(len(before_snapshot)),
        "after_rows": int(after_rows),
    }
    try:
        row = (
            db.execute(
                text(
                    """
                    INSERT INTO public.admin_filter_revisions
                    (source, dataset_type, job_key, before_blob, before_rows, after_rows)
                    VALUES (:source, :dataset_type, :job_key, :before_blob, :before_rows, :after_rows)
                    RETURNING id
                    """
                ),
                params,
            )
            .mappings()
            .first()
        )
        if row and row.get("id") is not None:
            return int(row["id"])
    except Exception:
        logger.exception(
            "Failed to persist admin filter revision source=%s dataset=%s job=%s",
            source,
            dataset_type,
            job_id,
        )
    return None


def _build_filter_ai_diagnostics(
    *,
    suggestion: dict[str, Any],
    normalize_meta: dict[str, Any],
    key_meta: dict[str, Any],
) -> dict[str, Any]:
    mappings = suggestion.get("mappings") if isinstance(suggestion, dict) else []
    if not isinstance(mappings, list):
        mappings = []

    right_mappings: list[dict[str, Any]] = []
    wrong_mappings: list[dict[str, Any]] = []
    for item in mappings:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        found = bool(item.get("found"))
        required = bool(item.get("required"))
        confidence = float(item.get("confidence") or 0.0)
        column = str(item.get("suggested_column") or "").strip()
        reason = item.get("reasoning") if isinstance(item.get("reasoning"), list) else []
        reason_text = ", ".join([str(v) for v in reason if str(v).strip()][:2])

        if found and column and confidence >= 0.55:
            right_mappings.append(
                {
                    "field": field,
                    "column": column,
                    "confidence": round(confidence, 3),
                }
            )
            continue

        if required and (not found or confidence < 0.55):
            wrong_mappings.append(
                {
                    "field": field,
                    "column": column or None,
                    "confidence": round(confidence, 3),
                    "issue": "Required mapping is missing" if not found else "Low-confidence mapping",
                    "reason": reason_text or "Insufficient signal in uploaded column values",
                }
            )

    smart_meta = normalize_meta.get("smart_mapping") if isinstance(normalize_meta, dict) else {}
    required_found = int((smart_meta or {}).get("required_found") or 0)
    required_total = int((smart_meta or {}).get("required_total") or 0)
    coverage = float((smart_meta or {}).get("coverage") or 0.0)
    duplicates = int(key_meta.get("duplicate_keys_in_file") or 0)
    missing_keys = int(key_meta.get("missing_key_values") or 0)
    strategy = str(key_meta.get("strategy") or "composite_hash")
    key_column = key_meta.get("key_column")
    key_columns = [
        str(value).strip()
        for value in (key_meta.get("key_columns") or [])
        if str(value).strip()
    ]
    key_columns_text = ", ".join(key_columns[:4])

    issues: list[str] = []
    if required_total > 0 and required_found < required_total:
        issues.append(f"AI mapping found {required_found}/{required_total} required fields.")
    if strategy == "composite_hash":
        issues.append(
            "No reliable unique business key was detected, so fallback row hash matching will be used. "
            "If row values change between uploads, DB may add new rows instead of merging."
        )
    elif strategy == "composite_candidate_columns" and key_columns_text:
        issues.append(f"Rows will be matched using a composite business key built from: {key_columns_text}.")
    if missing_keys > 0:
        issues.append(f"{missing_keys} rows have blank values in the detected primary key column.")
    if duplicates > 0:
        issues.append(f"{duplicates} rows share the same detected business key with another row in this file.")

    planned_changes: list[str] = [
        f"Smart mapping fills canonical columns for {int((smart_meta or {}).get('applied') or 0)} fields.",
        f"Normalization will touch {int(normalize_meta.get('columns_touched') or 0)} columns to align partner schema.",
        f"Record-key staging keeps {int(key_meta.get('rows_out') or 0)} rows from {int(key_meta.get('rows_in') or 0)} input rows.",
        (
            f"Primary key strategy: {strategy} "
            f"({'column ' + str(key_column) if key_column else ('columns ' + key_columns_text if key_columns_text else 'fallback composite key')})."
        ),
    ]
    if duplicates > 0:
        planned_changes.append(f"Duplicate business keys preserved as separate row instances: {duplicates} extra row(s).")

    return {
        "mapping_quality": {
            "required_found": required_found,
            "required_total": required_total,
            "coverage": round(coverage, 4),
        },
        "key_detection": {
            "primary_key_name": key_meta.get("primary_key_name"),
            "key_column": key_column,
            "key_columns": key_columns,
            "strategy": strategy,
            "key_candidates": key_meta.get("key_candidates") or [],
            "missing_key_values": missing_keys,
            "duplicate_keys_in_file": duplicates,
            "uniqueness_ratio": float(key_meta.get("uniqueness_ratio") or 0.0),
        },
        "right_mappings": right_mappings,
        "wrong_mappings": wrong_mappings,
        "issues": issues,
        "planned_changes": planned_changes,
    }


def _build_existing_row_match_preview(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    storage_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    scoped_query = _apply_tag_filter(db.query(DataRow.id), source, dataset_type, job_id)
    rows_in_scope = int(scoped_query.count() or 0)

    incoming_keys = [
        str(item.get("record_key") or "").strip()
        for item in storage_rows
        if isinstance(item, dict) and str(item.get("record_key") or "").strip()
    ]
    unique_incoming_keys = list(dict.fromkeys(incoming_keys))
    if not unique_incoming_keys:
        return {
            "rows_in_scope": rows_in_scope,
            "existing_rows_matched": 0,
            "new_rows_detected": 0,
            "match_ratio": 0.0,
        }

    existing_keys: set[str] = set()
    chunk_size = 2000
    for offset in range(0, len(unique_incoming_keys), chunk_size):
        chunk = unique_incoming_keys[offset : offset + chunk_size]
        query = db.query(DataRow.record_key).filter(
            DataRow.source == source,
            DataRow.dataset_type == dataset_type,
            DataRow.record_key.in_(chunk),
        )
        if job_id is None:
            query = query.filter(DataRow.job_id.is_(None))
        else:
            query = query.filter(DataRow.job_id == job_id)
        for (record_key,) in query.all():
            key = str(record_key or "").strip()
            if key:
                existing_keys.add(key)

    existing_rows_matched = sum(1 for key in incoming_keys if key in existing_keys)
    new_rows_detected = max(0, len(incoming_keys) - existing_rows_matched)
    match_ratio = (existing_rows_matched / max(len(incoming_keys), 1)) if incoming_keys else 0.0
    return {
        "rows_in_scope": rows_in_scope,
        "existing_rows_matched": int(existing_rows_matched),
        "new_rows_detected": int(new_rows_detected),
        "match_ratio": round(float(match_ratio), 4),
    }


def _post_file_update(
    db: Session,
    source_norm: str,
    dataset_norm: str,
    job_norm: str | None,
    action: str,
):
    # Master dashboard payloads are cached separately from source-specific caches.
    # Clear both active and legacy versions so top-level KPI strips always refresh.
    for master_cache_source in ("master_dashboard_v6", "master_dashboard_v7"):
        clear_precomputed_for_source_dataset(
            db,
            source=master_cache_source,
            dataset_type="overview",
        )

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

    def _background_rebuild() -> None:
        worker_db = SessionLocal()
        try:
            for refresh_source in refresh_sources:
                for refresh_job in refresh_jobs:
                    try:
                        rebuild_precomputed_analytics(
                            db=worker_db,
                            source=refresh_source,
                            dataset_type=dataset_norm,
                            job_id=refresh_job,
                        )
                    except Exception:
                        worker_db.rollback()
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
        finally:
            worker_db.close()

    threading.Thread(
        target=_background_rebuild,
        name=f"admin-files-precompute-{source_norm}-{dataset_norm}",
        daemon=True,
    ).start()


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
    source_norm = _normalize_source_key(source)
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
    source_norm = _normalize_source_key(source)
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
    source_norm = _normalize_source_key(source)
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
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    payloads, normalize_meta = await _parse_upload_payloads(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    _validate_payload_schema(
        source_norm=source_norm,
        dataset_norm=dataset_norm,
        payloads=payloads,
    )
    deleted, inserted, quality_meta = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        payloads=payloads,
        mode="replace",
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="replace")
    return {
        "deleted_rows": deleted,
        "rows_inserted": inserted,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
        "normalization": normalize_meta,
        "data_quality": quality_meta,
    }


@router.post("/update")
async def update_file_group(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")

    payloads, normalize_meta = await _parse_upload_payloads(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    _validate_payload_schema(
        source_norm=source_norm,
        dataset_norm=dataset_norm,
        payloads=payloads,
    )
    deleted, inserted, quality_meta = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        payloads=payloads,
        mode="merge",
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="update")
    return {
        "deleted_rows": deleted,
        "rows_inserted": inserted,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
        "normalization": normalize_meta,
        "data_quality": quality_meta,
    }


@router.post("/reverse-map")
async def reverse_map_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
):
    source_norm = _normalize_source_key(source)
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


@router.post("/filter-analyze")
async def analyze_filter_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")

    raw_df = await _parse_upload_dataframe(file)
    if raw_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to analyze")

    mapped_source = _normalize_source_for_mapper(source_norm)
    suggestion = suggest_reverse_mapping(
        raw_df,
        source=mapped_source,
        dataset_type=dataset_norm,
    )
    normalized_df, normalize_meta = normalize_partner_dataframe(
        raw_df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    payloads = dataframe_to_payload_rows(normalized_df)
    storage_rows, key_meta = prepare_rows_for_storage(payloads, source=source_norm, dataset_type=dataset_norm)
    diagnostics = _build_filter_ai_diagnostics(
        suggestion=suggestion,
        normalize_meta=normalize_meta,
        key_meta=key_meta,
    )
    db_match = _build_existing_row_match_preview(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        storage_rows=storage_rows,
    )

    return {
        "file_name": file.filename or "",
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
        "rows_in": int(len(raw_df)),
        "rows_after_filter": int(key_meta.get("rows_out") or 0),
        "ai_mapping": {
            "message": suggestion.get("message"),
            "can_reverse_map": bool(suggestion.get("can_reverse_map")),
        },
        "db_match": db_match,
        **diagnostics,
        "primary_key_candidates": get_primary_key_candidate_order(
            source=source_norm,
            dataset_type=dataset_norm,
        ),
        "can_apply": not diagnostics.get("wrong_mappings"),
    }


@router.post("/filter-revert")
def revert_filter_apply(
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    revision_id: int | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")

    if revision_id is not None:
        revision = (
            db.execute(
                text(
                    """
                    SELECT id, source, dataset_type, job_key, before_blob, before_rows, after_rows, reverted_at
                    FROM public.admin_filter_revisions
                    WHERE id = :revision_id
                    """
                ),
                {"revision_id": int(revision_id)},
            )
            .mappings()
            .first()
        )
    else:
        revision = (
            db.execute(
                text(
                    """
                    SELECT id, source, dataset_type, job_key, before_blob, before_rows, after_rows, reverted_at
                    FROM public.admin_filter_revisions
                    WHERE source = :source
                      AND dataset_type = :dataset_type
                      AND job_key = :job_key
                      AND reverted_at IS NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "source": source_norm,
                    "dataset_type": dataset_norm,
                    "job_key": str(job_norm or ""),
                },
            )
            .mappings()
            .first()
        )

    if not revision:
        raise HTTPException(status_code=404, detail="No saved filter revision found to revert")

    revision_source = str(revision.get("source") or source_norm).strip().lower()
    revision_dataset = str(revision.get("dataset_type") or dataset_norm).strip().lower()
    revision_job = str(revision.get("job_key") or "").strip() or None

    before_blob = revision.get("before_blob")
    snapshot = _decompress_snapshot(before_blob if isinstance(before_blob, (bytes, bytearray)) else b"")
    payloads = [item.get("data") for item in snapshot if isinstance(item.get("data"), dict)]

    deleted, inserted, quality_meta = _replace_tag_rows(
        db,
        source=revision_source,
        dataset_type=revision_dataset,
        job_id=revision_job,
        payloads=payloads,
        mode="replace",
    )
    db.execute(
        text(
            """
            UPDATE public.admin_filter_revisions
            SET reverted_at = NOW()
            WHERE id = :revision_id
            """
        ),
        {"revision_id": int(revision["id"])},
    )
    db.commit()
    _post_file_update(db, revision_source, revision_dataset, revision_job, action="filter-revert")

    return {
        "reverted": True,
        "revision_id": int(revision["id"]),
        "source": revision_source,
        "dataset_type": revision_dataset,
        "job_id": revision_job,
        "deleted_rows": deleted,
        "rows_inserted": inserted,
        "data_quality": quality_meta,
    }


@router.post("/filter-apply")
async def filter_and_apply_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")

    df = await _parse_upload_dataframe(file)
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to filter")

    normalized_df, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    payloads = dataframe_to_payload_rows(normalized_df)
    storage_rows, key_meta = prepare_rows_for_storage(payloads, source=source_norm, dataset_type=dataset_norm)
    filtered_payloads = [item.get("data") for item in storage_rows if isinstance(item.get("data"), dict)]
    if not filtered_payloads:
        raise HTTPException(status_code=400, detail="No rows found after applying filters")

    _validate_payload_schema(
        source_norm=source_norm,
        dataset_norm=dataset_norm,
        payloads=filtered_payloads,
    )
    before_rows = (
        _apply_tag_filter(
            db.query(DataRow),
            source_norm,
            dataset_norm,
            job_norm,
        ).all()
    )
    before_snapshot = _snapshot_rows(before_rows)
    replaced_rows, inserted_rows, quality_meta = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        payloads=filtered_payloads,
        mode="merge",
    )
    revision_id = _save_filter_revision(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        before_snapshot=before_snapshot,
        after_rows=inserted_rows,
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, job_norm, action="filter-apply")

    return {
        "applied": True,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": job_norm,
        "deleted_rows": int(replaced_rows),
        "rows_inserted": int(inserted_rows),
        "rows_updated": int(quality_meta.get("updated_rows") or 0),
        "revision_id": int(revision_id) if revision_id is not None else None,
        "normalization": normalize_meta,
        "data_quality": quality_meta,
        "key_detection": key_meta,
        "summary": (
            f"Applied filtered dataset to DB for {source_norm}:{dataset_norm}. "
            f"New rows {int(inserted_rows)}, existing rows updated {int(quality_meta.get('updated_rows') or 0)}."
        ),
    }


@router.post("/filter-download")
async def filter_and_download_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    dataset_type: str = Form(...),
    output_format: str = Form("csv"),
    apply_to_db: bool = Form(False),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    fmt = (output_format or "csv").strip().lower()

    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")
    if fmt not in {"csv", "xlsx"}:
        raise HTTPException(status_code=400, detail="output_format must be csv or xlsx")

    df = await _parse_upload_dataframe(file)
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to filter")

    normalized_df, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    payloads = dataframe_to_payload_rows(normalized_df)
    storage_rows, key_meta = prepare_rows_for_storage(payloads, source=source_norm, dataset_type=dataset_norm)
    filtered_payloads = [item.get("data") for item in storage_rows if isinstance(item.get("data"), dict)]
    if not filtered_payloads:
        raise HTTPException(status_code=400, detail="No rows found after applying filters")

    replaced_rows = 0
    inserted_rows = int(len(filtered_payloads))
    updated_rows = 0
    revision_id: int | None = None
    if apply_to_db:
        _validate_payload_schema(
            source_norm=source_norm,
            dataset_norm=dataset_norm,
            payloads=filtered_payloads,
        )
        before_rows = (
            _apply_tag_filter(
                db.query(DataRow),
                source_norm,
                dataset_norm,
                job_norm,
            ).all()
        )
        before_snapshot = _snapshot_rows(before_rows)
        replaced_rows, inserted_rows, quality_meta = _replace_tag_rows(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=job_norm,
            payloads=filtered_payloads,
            mode="merge",
        )
        updated_rows = int(quality_meta.get("updated_rows") or 0)
        revision_id = _save_filter_revision(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=job_norm,
            before_snapshot=before_snapshot,
            after_rows=inserted_rows,
        )
        db.commit()
        _post_file_update(db, source_norm, dataset_norm, job_norm, action="filter-download")

    if fmt == "xlsx":
        out_buffer = BytesIO()
        pd.DataFrame(filtered_payloads).to_excel(out_buffer, index=False, sheet_name="filtered")
        content = out_buffer.getvalue()
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        content = pd.DataFrame(filtered_payloads).to_csv(index=False).encode("utf-8")
        media_type = "text/csv"

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"filtered_{source_norm}_{dataset_norm}_{stamp}.{fmt}"
    summary = (
        f"Filtered {len(filtered_payloads)} rows for {source_norm}:{dataset_norm}. "
        f"Smart mapping applied {int((normalize_meta.get('smart_mapping') or {}).get('applied', 0))} fields. "
        f"Columns touched {int(normalize_meta.get('columns_touched') or 0)}. "
        f"Primary key strategy {key_meta.get('strategy')}, key field "
        f"{key_meta.get('key_column') or ', '.join(key_meta.get('key_columns') or []) or key_meta.get('primary_key_name')}."
    )
    if apply_to_db:
        summary += (
            f" Merged into database: {int(inserted_rows)} new rows added, "
            f"{int(updated_rows)} existing rows updated."
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Filter-Summary": summary,
        "X-Filter-Apply-Db": str(bool(apply_to_db)).lower(),
        "X-Filter-Rows": str(len(filtered_payloads)),
    }
    if revision_id is not None:
        headers["X-Filter-Revision-Id"] = str(int(revision_id))

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers=headers,
    )


@router.post("/recompute")
def recompute_precomputed_data(
    source: str | None = Query(None),
    dataset_type: str | None = Query(None),
    job_id: str | None = Query(None),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
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




