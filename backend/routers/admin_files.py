from __future__ import annotations

import gzip
import json
import logging
import math
import re
import threading
from io import BytesIO
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from authentication.deps import get_current_user, require_admin
from db.session import SessionLocal
from db.deps import get_db
from models.data_rows import DataRow
from services.ai_mapper import suggest_reverse_mapping
from services.admin_upload_service import (
    get_latest_upload_batch_map,
    record_upload_batch,
    resolve_job_id,
)
from services.data_quality_service import get_primary_key_candidate_order, prepare_rows_for_storage
from services.analytics_repository import invalidate_dataframe_cache
from services.analytics.goodrej_engine import invalidate_godrej_load_cache
from services.analytics.hitachi_engine import invalidate_hitachi_load_cache
from services.analytics.reliance_engine import invalidate_reliance_load_cache
from services.analytics.samsung_engine import invalidate_samsung_load_cache
from services.deck_cache_service import invalidate_deck_cache_for_source_dataset
from services.maintenance_service import refresh_master_overview_cache, run_daily_refresh
from services.manual_update_service import mark_manual_update
from services.precompute_service import rebuild_precomputed_analytics
from services.precomputed_repository import clear_precomputed_for_source_dataset
from services.partner_filter_service import (
    dataframe_to_payload_rows,
    normalize_partner_dataframe,
)
from services.date_parsing import parse_flexible_datetime
from services.samsung_partner_config import (
    SAMSUNG_PARTNER_SOURCES,
    is_samsung_source,
    normalize_samsung_source,
)

router = APIRouter(
    prefix="/admin/files",
    tags=["admin-files"],
    dependencies=[Depends(require_admin)],
)
logger = logging.getLogger(__name__)

_FILTER_ANALYZE_SAMPLE_THRESHOLD_ROWS = 12000
_FILTER_ANALYZE_SAMPLE_TARGET_CELLS = 100000
_FILTER_ANALYZE_SAMPLE_MAX_ROWS = 8000
_FILTER_ANALYZE_SAMPLE_MIN_ROWS = 1000
_FILTER_ANALYZE_SAMPLE_RANDOM_SEED = 7


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _normalize_source_key(value: str | None) -> str | None:
    source_key = _normalize(value)
    if source_key is None:
        return None
    samsung_source = normalize_samsung_source(source_key)
    if samsung_source:
        return samsung_source
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return source_key


def _normalize_source_for_mapper(source: str) -> str:
    source_key = _normalize_source_key(source) or ""
    if is_samsung_source(source_key):
        return "samsung"
    return source_key


def _actor_email(actor: Any | None) -> str:
    return str(getattr(actor, "username", "") or "").strip()


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


_DATE_NULLS = {"", "nan", "none", "null", "nat", "NaT"}
_DATE_COLUMNS = {
    "Plan Start Date",
    "Warranty Start Date",
    "Plan End Date",
    "Warranty End Date",
    "Start_Date",
    "End_Date",
    "Start Date",
    "End Date",
    "Date",
    "Month",
    "Claim Date",
    "Call Date",
    "Day of Call_Date",
    "Payment Date",
    "Payment_date",
    "Invoice Date",
    "Invoice_Date_",
    "Purchase Date",
    "Warranty Purchase Date",
    "Transaction Date",
}

_DATE_FORMAT_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"), "%Y-%m-%dT%H:%M:%S"),
    (re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), "%Y-%m-%d %H:%M:%S"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
    (re.compile(r"^\d{4}/\d{1,2}/\d{1,2}$"), "%Y/%m/%d"),
    (re.compile(r"^\d{4}\.\d{1,2}\.\d{1,2}$"), "%Y.%m.%d"),
    (re.compile(r"^\d{8}$"), "%Y%m%d"),
    (re.compile(r"^\d{1,2}-[A-Za-z]{3,9}-\d{4}$"), "%d-%b-%Y"),
    (re.compile(r"^\d{1,2}-[A-Za-z]{3,9}-\d{2}$"), "%d-%b-%y"),
    (re.compile(r"^[A-Za-z]{3,9}-\d{4}$"), "%b-%Y"),
    (re.compile(r"^[A-Za-z]{3,9}-\d{2}$"), "%b-%y"),
]


def _infer_numeric_date_format(values: list[str], sep: str) -> tuple[str | None, int]:
    pattern = re.compile(rf"^\d{{1,2}}\\{sep}\d{{1,2}}\\{sep}\d{{2,4}}$")
    matches = [value for value in values if pattern.match(value)]
    if not matches:
        return None, 0

    day_first = 0
    month_first = 0
    year_two = 0
    for value in matches:
        parts = value.split(sep)
        if len(parts) != 3:
            continue
        left, right, year = parts
        try:
            left_num = int(left)
            right_num = int(right)
        except Exception:
            continue
        if len(year) == 2:
            year_two += 1
        if left_num > 12 and right_num <= 12:
            day_first += 1
        elif right_num > 12 and left_num <= 12:
            month_first += 1

    year_fmt = "%y" if year_two >= max(1, len(matches) // 2) else "%Y"
    if month_first > day_first:
        return f"%m{sep}%d{sep}{year_fmt}", len(matches)
    return f"%d{sep}%m{sep}{year_fmt}", len(matches)


def _infer_date_format(values: list[str]) -> str | None:
    if not values:
        return None
    samples = [str(v).strip() for v in values if str(v).strip() and str(v).strip().lower() not in _DATE_NULLS]
    if not samples:
        return None

    counts: dict[str, int] = {}
    for pattern, fmt in _DATE_FORMAT_RULES:
        count = sum(1 for value in samples if pattern.match(value))
        if count:
            counts[fmt] = counts.get(fmt, 0) + count

    for sep in ("/", "-", "."):
        fmt, count = _infer_numeric_date_format(samples, sep)
        if fmt and count:
            counts[fmt] = counts.get(fmt, 0) + count

    if not counts:
        return None

    best_fmt = max(counts.items(), key=lambda item: item[1])[0]
    return best_fmt


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip()
    raw = raw.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA, "null": pd.NA, "NaT": pd.NA})
    try:
        parsed = pd.to_datetime(raw, format="mixed", errors="coerce")
    except TypeError:
        parsed = pd.to_datetime(raw, errors="coerce")

    if parsed.isna().any():
        fallback = raw.where(parsed.isna()).map(parse_flexible_datetime)
        parsed = parsed.fillna(fallback)
    return parsed


def _infer_existing_date_formats(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    sample_limit: int = 200,
) -> dict[str, str]:
    def _collect(job: str | None) -> dict[str, list[str]]:
        query = _apply_tag_filter(
            db.query(DataRow.data),
            source,
            dataset_type,
            job,
        ).limit(sample_limit)
        rows = query.all()
        payloads = [row[0] if isinstance(row, tuple) else row.data for row in rows]
        samples: dict[str, list[str]] = {col: [] for col in _DATE_COLUMNS}
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            for col in _DATE_COLUMNS:
                if col not in payload:
                    continue
                if len(samples[col]) >= 40:
                    continue
                raw = str(payload.get(col, "")).strip()
                if not raw or raw.lower() in _DATE_NULLS:
                    continue
                samples[col].append(raw)
        return samples

    samples = _collect(job_id)
    inferred: dict[str, str] = {}
    for col, values in samples.items():
        fmt = _infer_date_format(values)
        if fmt:
            inferred[col] = fmt

    if not inferred and job_id is not None:
        samples = _collect(None)
        for col, values in samples.items():
            fmt = _infer_date_format(values)
            if fmt:
                inferred[col] = fmt

    return inferred


def _align_date_columns_to_existing_format(
    df: pd.DataFrame,
    *,
    date_formats: dict[str, str],
) -> pd.DataFrame:
    if df is None or df.empty or not date_formats:
        return df

    for col, fmt in date_formats.items():
        if col not in df.columns:
            continue
        series = df[col]
        parsed = _parse_datetime_series(series)
        if parsed.isna().all():
            continue
        formatted = parsed.dt.strftime(fmt)
        original = series.astype(str).replace({"nan": "", "NaT": "", "None": ""})
        df[col] = formatted.where(parsed.notna(), original)

    return df


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
    df = await _parse_upload_dataframe(
        file,
        source=source,
        dataset_type=dataset_type,
    )
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


def _score_excel_sheet_columns(
    columns: list[str],
    *,
    source: str | None,
    dataset_type: str | None,
) -> tuple[int, bool]:
    normalized = {_normalize_col_key(col) for col in columns if str(col).strip()}
    if not normalized:
        return 0, False

    source_norm = _normalize_source_key(source) if source else None
    dataset_norm = _normalize(dataset_type) if dataset_type else None
    if source_norm == "reliance" and dataset_norm == "sales":
        date_cols = {
            "planstartdate",
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
        date_hits = normalized & date_cols
        value_hits = normalized & value_cols
        has_required = bool(date_hits) and bool(value_hits)
        if not date_hits and not value_hits:
            return 0, False

        unnamed_cols = sum(1 for col in normalized if col.startswith("unnamed"))
        richness = max(0, min(len(normalized), 20) - unnamed_cols)
        score = (len(date_hits) * 3) + (len(value_hits) * 3) + richness
        return score, has_required

    return 0, False


def _select_excel_sheet(
    xls: pd.ExcelFile,
    *,
    source: str | None,
    dataset_type: str | None,
) -> tuple[str, int] | None:
    if not source or not dataset_type:
        return None

    best_sheet: str | None = None
    best_score = -1
    best_required = False
    best_header = 0
    for sheet_name in xls.sheet_names:
        for header in range(0, 4):
            try:
                preview = pd.read_excel(xls, sheet_name=sheet_name, nrows=0, header=header)
            except Exception:
                continue
            score, has_required = _score_excel_sheet_columns(
                [str(col).strip() for col in preview.columns],
                source=source,
                dataset_type=dataset_type,
            )
            if has_required and not best_required:
                best_required = True
                best_score = score
                best_sheet = sheet_name
                best_header = header
                continue
            if has_required == best_required:
                if score > best_score or (score == best_score and header < best_header):
                    best_score = score
                    best_sheet = sheet_name
                    best_header = header

    if best_sheet is None or best_score <= 0:
        return None
    return best_sheet, best_header


async def _parse_upload_dataframe(
    file: UploadFile,
    *,
    source: str | None = None,
    dataset_type: str | None = None,
) -> pd.DataFrame:
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file")

    filename = (file.filename or "").lower()
    buffer = BytesIO(contents)
    xls: pd.ExcelFile | None = None
    try:
        if filename.endswith(".csv"):
            return pd.read_csv(buffer)
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            xls = pd.ExcelFile(buffer)
            selection = _select_excel_sheet(
                xls,
                source=source,
                dataset_type=dataset_type,
            )
            if selection:
                sheet_name, header = selection
                return pd.read_excel(xls, sheet_name=sheet_name, header=header)
            return pd.read_excel(xls, sheet_name=0)
        raise HTTPException(status_code=400, detail="Only .csv, .xls, and .xlsx are supported")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")
    finally:
        if xls is not None:
            xls.close()
        buffer.close()
        del contents


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
    canonical_fields = normalize_meta.get("canonical_fields") if isinstance(normalize_meta, dict) else {}
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
        canonical_status = canonical_fields.get(field) if isinstance(canonical_fields, dict) else None
        canonical_column = str((canonical_status or {}).get("column") or "").strip()
        canonical_fill_ratio = float((canonical_status or {}).get("fill_ratio") or 0.0)
        canonical_available = bool((canonical_status or {}).get("available"))

        if found and column and confidence >= 0.55:
            right_mappings.append(
                {
                    "field": field,
                    "column": column,
                    "confidence": round(confidence, 3),
                }
            )
            continue

        if canonical_available and canonical_column and canonical_fill_ratio >= 0.55:
            right_mappings.append(
                {
                    "field": field,
                    "column": canonical_column,
                    "confidence": round(max(confidence, canonical_fill_ratio), 3),
                }
            )
            continue

        if required and (not found or confidence < 0.55):
            wrong_mappings.append(
                {
                    "field": field,
                    "column": column or canonical_column or None,
                    "confidence": round(max(confidence, canonical_fill_ratio), 3),
                    "issue": "Required mapping is missing" if not found else "Low-confidence mapping",
                    "reason": reason_text or "Insufficient signal in uploaded column values",
                }
            )

    smart_meta = normalize_meta.get("smart_mapping") if isinstance(normalize_meta, dict) else {}
    required_found = int(
        suggestion.get("required_fields_found")
        or (smart_meta or {}).get("required_found")
        or 0
    )
    required_total = int(
        suggestion.get("required_fields_total")
        or (smart_meta or {}).get("required_total")
        or 0
    )
    coverage = float(
        suggestion.get("coverage")
        or (smart_meta or {}).get("coverage")
        or 0.0
    )
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


def _merge_effective_mapping_with_normalization(
    *,
    suggestion: dict[str, Any],
    normalize_meta: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(suggestion, dict):
        return {
            "required_fields_found": 0,
            "required_fields_total": 0,
            "coverage": 0.0,
            "can_reverse_map": False,
            "message": "Required fields were not recognized. Please verify source or column headers.",
            "mappings": [],
        }

    canonical_fields = normalize_meta.get("canonical_fields") if isinstance(normalize_meta, dict) else {}
    raw_mappings = suggestion.get("mappings") if isinstance(suggestion.get("mappings"), list) else []
    mappings: list[dict[str, Any]] = []
    required_total = 0
    required_found = 0

    for raw_item in raw_mappings:
        if not isinstance(raw_item, dict):
            continue

        item = dict(raw_item)
        field = str(item.get("field") or "").strip()
        required = bool(item.get("required"))
        found = bool(item.get("found"))
        confidence = float(item.get("confidence") or 0.0)
        suggested_column = str(item.get("suggested_column") or "").strip() or None
        reasoning = item.get("reasoning") if isinstance(item.get("reasoning"), list) else []

        canonical_status = canonical_fields.get(field) if isinstance(canonical_fields, dict) else None
        canonical_column = str((canonical_status or {}).get("column") or "").strip() or None
        canonical_available = bool((canonical_status or {}).get("available"))
        canonical_fill_ratio = float((canonical_status or {}).get("fill_ratio") or 0.0)

        if canonical_available and canonical_column:
            found = True
            if field == "device_plan_category" or not suggested_column or confidence < 0.55:
                suggested_column = canonical_column
            confidence = max(confidence, canonical_fill_ratio, 0.56)
            if "backend canonical normalization populated this field" not in reasoning:
                reasoning = [*reasoning, "backend canonical normalization populated this field"]

        item["found"] = found
        item["suggested_column"] = suggested_column
        item["confidence"] = round(confidence, 4)
        item["reasoning"] = reasoning
        mappings.append(item)

        if required:
            required_total += 1
            if found:
                required_found += 1

    coverage = 1.0 if required_total == 0 else float(required_found) / float(required_total)
    can_reverse_map = coverage >= 0.999
    if can_reverse_map:
        message = "Reverse mapping is possible for this upload."
    elif required_found == 0:
        message = "Required fields were not recognized. Please verify source or column headers."
    else:
        message = "Partial mapping found. Backend normalization will auto-fill the remaining recognized fields."

    merged = dict(suggestion)
    merged["required_fields_found"] = int(required_found)
    merged["required_fields_total"] = int(required_total)
    merged["coverage"] = round(coverage, 4)
    merged["can_reverse_map"] = bool(can_reverse_map)
    merged["message"] = message
    merged["mappings"] = mappings
    return merged


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


def _safe_primary_key_candidates(*, source: str, dataset_type: str) -> list[str]:
    try:
        candidates = get_primary_key_candidate_order(
            source=source,
            dataset_type=dataset_type,
        )
    except Exception:
        logger.exception(
            "Failed to load primary key candidates source=%s dataset=%s",
            source,
            dataset_type,
        )
        if dataset_type == "claims":
            return ["Call No", "Claim ID", "Claim Number", "Case ID", "Ticket ID", "Reference Number"]
        return ["Plan ID", "Policy Number", "Invoice Number", "Order ID", "IMEI", "Serial Number"]

    return [str(value).strip() for value in candidates if str(value).strip()]


def _compose_filter_analyze_response(
    *,
    file_name: str,
    source: str,
    dataset_type: str,
    job_id: str | None,
    rows_in: int,
    key_meta: dict[str, Any],
    effective_suggestion: dict[str, Any],
    diagnostics: dict[str, Any],
    db_match: dict[str, Any] | None,
    primary_key_candidates: list[str],
    can_apply: bool,
) -> dict[str, Any]:
    safe_suggestion = effective_suggestion if isinstance(effective_suggestion, dict) else {}
    safe_diagnostics = diagnostics if isinstance(diagnostics, dict) else {}

    mapping_quality = safe_diagnostics.get("mapping_quality")
    if not isinstance(mapping_quality, dict):
        mapping_quality = {
            "required_found": int(safe_suggestion.get("required_fields_found") or 0),
            "required_total": int(safe_suggestion.get("required_fields_total") or 0),
            "coverage": float(safe_suggestion.get("coverage") or 0.0),
        }

    key_detection = safe_diagnostics.get("key_detection")
    if not isinstance(key_detection, dict):
        key_detection = dict(key_meta or {})

    right_mappings = safe_diagnostics.get("right_mappings")
    if not isinstance(right_mappings, list):
        right_mappings = []

    wrong_mappings = safe_diagnostics.get("wrong_mappings")
    if not isinstance(wrong_mappings, list):
        wrong_mappings = []

    issues = safe_diagnostics.get("issues")
    if not isinstance(issues, list):
        issues = []

    planned_changes = safe_diagnostics.get("planned_changes")
    if not isinstance(planned_changes, list):
        planned_changes = []

    return {
        "file_name": file_name,
        "source": source,
        "dataset_type": dataset_type,
        "job_id": job_id,
        "rows_in": int(rows_in),
        "rows_after_filter": int(key_meta.get("rows_out") or 0),
        "ai_mapping": {
            "message": safe_suggestion.get("message"),
            "can_reverse_map": bool(safe_suggestion.get("can_reverse_map")),
        },
        "db_match": db_match if isinstance(db_match, dict) else None,
        "mapping_quality": mapping_quality,
        "key_detection": key_detection,
        "right_mappings": right_mappings,
        "wrong_mappings": wrong_mappings,
        "issues": issues,
        "planned_changes": planned_changes,
        "primary_key_candidates": list(primary_key_candidates),
        "can_apply": bool(can_apply),
    }


def _sample_filter_analyze_dataframe(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any] | None]:
    total_rows = int(len(df))
    total_cols = max(1, int(len(df.columns)))
    total_cells = total_rows * total_cols
    if (
        total_rows <= _FILTER_ANALYZE_SAMPLE_THRESHOLD_ROWS
        and total_cells <= _FILTER_ANALYZE_SAMPLE_TARGET_CELLS
    ):
        return df.copy(), None

    target_rows = max(
        _FILTER_ANALYZE_SAMPLE_MIN_ROWS,
        _FILTER_ANALYZE_SAMPLE_TARGET_CELLS // total_cols,
    )
    sample_rows = min(_FILTER_ANALYZE_SAMPLE_MAX_ROWS, total_rows, target_rows)
    if sample_rows >= total_rows:
        return df.copy(), None

    edge_rows = max(250, sample_rows // 4)
    head_rows = min(edge_rows, total_rows, sample_rows)
    tail_budget = max(0, sample_rows - head_rows)
    tail_rows = min(edge_rows, max(0, total_rows - head_rows), tail_budget)
    middle_budget = max(0, sample_rows - head_rows - tail_rows)

    frames: list[pd.DataFrame] = []
    if head_rows:
        frames.append(df.head(head_rows))

    middle_start = head_rows
    middle_end = max(middle_start, total_rows - tail_rows)
    if middle_budget and middle_end > middle_start:
        middle = df.iloc[middle_start:middle_end]
        if len(middle) > middle_budget:
            middle = middle.sample(n=middle_budget, random_state=_FILTER_ANALYZE_SAMPLE_RANDOM_SEED)
        frames.append(middle)

    if tail_rows:
        frames.append(df.tail(tail_rows))

    if not frames:
        sampled_df = df.head(sample_rows).copy()
    else:
        sampled_df = pd.concat(frames, ignore_index=True)

    sampled_rows = int(len(sampled_df))
    warning = (
        f"Large file preview analyzed {sampled_rows:,} sampled rows out of {total_rows:,} "
        "to avoid request timeouts. Apply/download still use the full file."
    )
    return sampled_df, {
        "sampled": True,
        "total_rows": total_rows,
        "sample_rows": sampled_rows,
        "warning": warning,
    }


def _post_file_update(
    db: Session,
    source_norm: str,
    dataset_norm: str,
    job_norm: str | None,
    action: str,
):
    # Keep the last successful master overview payload warm while new source data
    # is rebuilding. Manual-update markers will mark it stale, and the background
    # refresh below replaces it once the new overview is ready.

    invalidate_deck_cache_for_source_dataset(
        db=db,
        source=source_norm,
        dataset_type=dataset_norm,
    )

    refresh_sources: list[str] = [source_norm]
    if source_norm in SAMSUNG_PARTNER_SOURCES:
        refresh_sources.append("samsung")
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
            elif refresh_source in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
                invalidate_reliance_load_cache(
                    source=refresh_source,
                    dataset_type=dataset_norm,
                    job_id=refresh_job,
                )
            elif refresh_source in {"godrej", "goodrej", "goddrej"}:
                invalidate_godrej_load_cache(
                    source=refresh_source,
                    dataset_type=dataset_norm,
                    job_id=refresh_job,
                )
            elif refresh_source == "hitachi":
                invalidate_hitachi_load_cache(
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
                for refresh_job in refresh_jobs:
                    if refresh_source.startswith("samsung"):
                        invalidate_samsung_load_cache(
                            source=refresh_source,
                            dataset_type=dataset_norm,
                            job_id=refresh_job,
                        )
                    elif refresh_source in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
                        invalidate_reliance_load_cache(
                            source=refresh_source,
                            dataset_type=dataset_norm,
                            job_id=refresh_job,
                        )
                    elif refresh_source in {"godrej", "goodrej", "goddrej"}:
                        invalidate_godrej_load_cache(
                            source=refresh_source,
                            dataset_type=dataset_norm,
                            job_id=refresh_job,
                        )
                    elif refresh_source == "hitachi":
                        invalidate_hitachi_load_cache(
                            source=refresh_source,
                            dataset_type=dataset_norm,
                            job_id=refresh_job,
                        )
            try:
                refresh_master_overview_cache(db=worker_db)
            except Exception:
                worker_db.rollback()
                logger.exception(
                    "Failed to refresh master overview after %s source=%s dataset=%s",
                    action,
                    source_norm,
                    dataset_norm,
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
    def _has_meaningful_value(rows: list[dict], column: str) -> bool:
        for row in rows:
            value = row.get(column) if isinstance(row, dict) else None
            if value is None:
                continue
            if isinstance(value, str):
                if value.strip() and value.strip().lower() not in {"nan", "none", "null", "nat"}:
                    return True
                continue
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            return True
        return False

    if source_norm == "hitachi" and dataset_norm == "sales":
        if not payloads:
            raise HTTPException(status_code=400, detail="Hitachi sales upload is empty.")

        sample_rows = payloads[: min(len(payloads), 300)]
        raw_columns = {str(k).strip() for row in sample_rows for k in row.keys()}
        normalized_columns = {_normalize_col_key(k) for k in raw_columns if str(k).strip()}
        required_columns = {
            "customerpremium",
            "retailpremium",
            "warrantystartdate",
        }
        missing = sorted(col for col in required_columns if col not in normalized_columns)
        if missing:
            sample_cols = ", ".join(sorted(raw_columns)[:12]) or "none"
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid Hitachi sales file format. Upload row-level Hitachi sales data with "
                    "Customer Premium, Retail Premium, and Warranty Start Date columns. "
                    f"Detected columns: {sample_cols}"
                ),
            )
        required_values = {
            "Customer Premium": _has_meaningful_value(sample_rows, "Customer Premium"),
            "Retail Premium": _has_meaningful_value(sample_rows, "Retail Premium"),
            "Warranty Start Date": _has_meaningful_value(sample_rows, "Warranty Start Date"),
        }
        if not all(required_values.values()):
            missing_values = ", ".join(
                key for key, present in required_values.items() if not present
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid Hitachi sales file format. These required fields are missing values after normalization: "
                    f"{missing_values}."
                ),
            )
        return

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

    # The filter workflow only needs tag suggestions for one source/dataset pair.
    # Keep that path lean so admin uploads do not wait on a full-table group scan.
    latest_upload_map = get_latest_upload_batch_map(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
    )

    if source_norm and dataset_norm:
        query = (
            db.query(
                DataRow.job_id.label("job_id"),
                func.count(DataRow.id).label("rows"),
                func.max(DataRow.id).label("latest_row_id"),
            )
            .filter(
                DataRow.source == source_norm,
                DataRow.dataset_type == dataset_norm,
            )
        )
        if job_id is not None:
            if job_norm is None:
                query = query.filter(DataRow.job_id.is_(None))
            else:
                query = query.filter(DataRow.job_id == job_norm)
        rows = (
            query
            .group_by(DataRow.job_id)
            .order_by(func.max(DataRow.id).desc())
            .all()
        )
        items = [
            {
                **({
                    "source": source_norm,
                    "dataset_type": dataset_norm,
                    "job_id": r.job_id,
                    "tag": f"{source_norm}:{dataset_norm}:{r.job_id or 'untagged'}",
                    "rows": int(r.rows or 0),
                    "latest_row_id": int(r.latest_row_id) if r.latest_row_id is not None else None,
                }),
                **(latest_upload_map.get((source_norm, dataset_norm, str(r.job_id or ""))) or {}),
            }
            for r in rows
        ]
        return {"items": items}

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
            **({
                "source": r.source,
                "dataset_type": r.dataset_type,
                "job_id": r.job_id,
                "tag": f"{r.source}:{r.dataset_type}:{r.job_id or 'untagged'}",
                "rows": int(r.rows or 0),
                "latest_row_id": int(r.latest_row_id) if r.latest_row_id is not None else None,
            }),
            **(latest_upload_map.get((str(r.source), str(r.dataset_type), str(r.job_id or ""))) or {}),
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
    current_user: Any = Depends(get_current_user),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")

    query = _apply_tag_filter(db.query(DataRow), source_norm, dataset_norm, job_norm)
    deleted = query.delete(synchronize_session=False)
    record_upload_batch(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        action="delete_tag",
        uploaded_by=_actor_email(current_user),
        file_name="",
        rows_in=0,
        rows_inserted=0,
        rows_updated=0,
        deleted_rows=int(deleted or 0),
        notes="Tag rows were deleted from admin manual access.",
    )
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
    current_user: Any = Depends(get_current_user),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    uploaded_at = datetime.now(timezone.utc)
    effective_job_id, auto_generated_job_id = resolve_job_id(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        requested_job_id=job_norm,
        uploaded_at=uploaded_at,
    )
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
        job_id=effective_job_id,
        payloads=payloads,
        mode="replace",
    )
    record_upload_batch(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=effective_job_id,
        action="replace",
        uploaded_by=_actor_email(current_user),
        uploaded_at=uploaded_at,
        file_name=file.filename or "",
        rows_in=int(len(payloads)),
        rows_inserted=int(inserted),
        rows_updated=int(quality_meta.get("updated_rows") or 0),
        deleted_rows=int(deleted),
        notes=(
            "Auto-generated job_id was assigned because the upload did not provide one."
            if auto_generated_job_id
            else ""
        ),
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, effective_job_id, action="replace")
    return {
        "deleted_rows": deleted,
        "rows_inserted": inserted,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": effective_job_id,
        "auto_generated_job_id": auto_generated_job_id,
        "uploaded_by": _actor_email(current_user),
        "uploaded_at": uploaded_at.isoformat(),
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
    current_user: Any = Depends(get_current_user),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    uploaded_at = datetime.now(timezone.utc)
    effective_job_id, auto_generated_job_id = resolve_job_id(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        requested_job_id=job_norm,
        uploaded_at=uploaded_at,
        prefer_existing_job_id=True,
    )

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
        job_id=effective_job_id,
        payloads=payloads,
        mode="merge",
    )
    record_upload_batch(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=effective_job_id,
        action="update",
        uploaded_by=_actor_email(current_user),
        uploaded_at=uploaded_at,
        file_name=file.filename or "",
        rows_in=int(len(payloads)),
        rows_inserted=int(inserted),
        rows_updated=int(quality_meta.get("updated_rows") or 0),
        deleted_rows=int(deleted),
        notes=(
            "Auto-generated job_id was assigned because the upload did not provide one."
            if auto_generated_job_id
            else ""
        ),
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, effective_job_id, action="update")
    return {
        "deleted_rows": deleted,
        "rows_inserted": inserted,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": effective_job_id,
        "auto_generated_job_id": auto_generated_job_id,
        "uploaded_by": _actor_email(current_user),
        "uploaded_at": uploaded_at.isoformat(),
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

    df = await _parse_upload_dataframe(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to map")

    mapped_source = _normalize_source_for_mapper(source_norm)
    suggestion = suggest_reverse_mapping(
        df,
        source=mapped_source,
        dataset_type=dataset_norm,
    )
    _, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    effective_suggestion = _merge_effective_mapping_with_normalization(
        suggestion=suggestion,
        normalize_meta=normalize_meta,
    )
    effective_suggestion["file_name"] = file.filename or ""
    del df
    return effective_suggestion


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

    raw_df = await _parse_upload_dataframe(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    if raw_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to analyze")

    analysis_df, analysis_scope = _sample_filter_analyze_dataframe(raw_df)

    mapped_source = _normalize_source_for_mapper(source_norm)
    pipeline_warnings: list[str] = []
    if analysis_scope is not None:
        pipeline_warnings.append(str(analysis_scope.get("warning") or "").strip())
    primary_key_candidates = _safe_primary_key_candidates(
        source=source_norm,
        dataset_type=dataset_norm,
    )
    default_suggestion: dict[str, Any] = {
        "required_fields_found": 0,
        "required_fields_total": 0,
        "coverage": 0.0,
        "can_reverse_map": False,
        "message": "AI mapping could not fully analyze this file, but basic diagnostics are available.",
        "mappings": [],
    }
    default_normalize_meta: dict[str, Any] = {
        "source": source_norm,
        "dataset_type": dataset_norm,
        "rows": int(len(raw_df)),
        "columns": int(len(raw_df.columns)),
        "smart_mapping": {"applied": 0, "required_found": 0, "required_total": 0, "coverage": 0.0},
        "columns_touched": 0,
        "canonical_fields": {},
    }
    default_key_role = "claim_key_fallback" if dataset_norm == "claims" else "plan_key_fallback"
    default_key_meta: dict[str, Any] = {
        "source": source_norm,
        "dataset_type": dataset_norm,
        "primary_key_name": default_key_role,
        "strategy": "composite_hash",
        "key_column": None,
        "key_columns": [],
        "key_candidates": list(primary_key_candidates),
        "rows_in": int(len(raw_df)),
        "rows_out": int(len(raw_df)),
        "duplicate_keys_in_file": 0,
        "missing_key_values": 0,
        "uniqueness_ratio": 0.0,
    }

    effective_suggestion = dict(default_suggestion)
    normalize_meta = dict(default_normalize_meta)
    key_meta = dict(default_key_meta)
    diagnostics: dict[str, Any] = {
        "mapping_quality": {
            "required_found": int(default_suggestion["required_fields_found"]),
            "required_total": int(default_suggestion["required_fields_total"]),
            "coverage": float(default_suggestion["coverage"]),
        },
        "key_detection": dict(default_key_meta),
        "right_mappings": [],
        "wrong_mappings": [],
        "issues": [],
        "planned_changes": [],
    }
    db_match: dict[str, Any] | None = None

    try:
        suggestion = default_suggestion
        try:
            suggestion = suggest_reverse_mapping(
                analysis_df,
                source=mapped_source,
                dataset_type=dataset_norm,
            )
        except Exception:
            logger.exception(
                "filter-analyze reverse mapping failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            pipeline_warnings.append("AI reverse-mapping failed; using basic schema diagnostics.")

        normalized_df = analysis_df.copy()
        try:
            normalized_df, normalize_meta = normalize_partner_dataframe(
                analysis_df,
                source=source_norm,
                dataset_type=dataset_norm,
            )
        except Exception:
            logger.exception(
                "filter-analyze normalization failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            pipeline_warnings.append("Normalization fallback applied because the uploaded file triggered a backend parsing issue.")

        try:
            date_formats = _infer_existing_date_formats(
                db,
                source=source_norm,
                dataset_type=dataset_norm,
                job_id=job_norm,
            )
            normalized_df = _align_date_columns_to_existing_format(
                normalized_df,
                date_formats=date_formats,
            )
        except Exception:
            logger.exception(
                "filter-analyze date alignment failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            pipeline_warnings.append("Date-format alignment was skipped because existing-format inference failed.")

        try:
            effective_suggestion = _merge_effective_mapping_with_normalization(
                suggestion=suggestion,
                normalize_meta=normalize_meta,
            )
        except Exception:
            logger.exception(
                "filter-analyze mapping merge failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            effective_suggestion = dict(suggestion or default_suggestion)
            pipeline_warnings.append("Merged AI mapping could not be computed cleanly; showing the best available analysis.")

        try:
            payloads = dataframe_to_payload_rows(normalized_df)
        except Exception:
            logger.exception(
                "filter-analyze payload conversion failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            payloads = dataframe_to_payload_rows(analysis_df)
            pipeline_warnings.append("Using raw uploaded rows because normalized payload conversion failed.")

        storage_rows: list[dict[str, Any]]
        try:
            storage_rows, key_meta = prepare_rows_for_storage(
                payloads,
                source=source_norm,
                dataset_type=dataset_norm,
            )
        except Exception:
            logger.exception(
                "filter-analyze record-key preparation failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            storage_rows = [
                {
                    "data": payload,
                    "record_key": f"{default_key_role}:row:{index + 1:08d}",
                    "primary_key_name": default_key_role,
                }
                for index, payload in enumerate(payloads)
                if isinstance(payload, dict)
            ]
            key_meta["rows_out"] = int(len(storage_rows))
            pipeline_warnings.append("Business-key detection failed; fallback row keys will be used for preview.")

        try:
            diagnostics = _build_filter_ai_diagnostics(
                suggestion=effective_suggestion,
                normalize_meta=normalize_meta,
                key_meta=key_meta,
            )
        except Exception:
            logger.exception(
                "filter-analyze diagnostics build failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            diagnostics = {
                "mapping_quality": {
                    "required_found": int(effective_suggestion.get("required_fields_found") or 0),
                    "required_total": int(effective_suggestion.get("required_fields_total") or 0),
                    "coverage": float(effective_suggestion.get("coverage") or 0.0),
                },
                "key_detection": dict(key_meta),
                "right_mappings": [],
                "wrong_mappings": [],
                "issues": [],
                "planned_changes": [],
            }
            pipeline_warnings.append("Detailed diagnostics could not be fully computed; showing a simplified result.")

        try:
            db_match = _build_existing_row_match_preview(
                db,
                source=source_norm,
                dataset_type=dataset_norm,
                job_id=job_norm,
                storage_rows=storage_rows,
            )
        except Exception:
            logger.exception(
                "filter-analyze DB match preview failed source=%s dataset=%s file=%s",
                source_norm,
                dataset_norm,
                file.filename,
            )
            pipeline_warnings.append("Existing-row match preview is temporarily unavailable, but the file analysis completed.")
    except Exception:
        logger.exception(
            "filter-analyze unexpected failure source=%s dataset=%s file=%s",
            source_norm,
            dataset_norm,
            file.filename,
        )
        pipeline_warnings.append("Unexpected server fallback applied; returning simplified diagnostics for this file.")
        if not effective_suggestion.get("message"):
            effective_suggestion["message"] = "AI analysis completed with fallback diagnostics."

    if analysis_scope is not None:
        sample_rows = int(analysis_scope.get("sample_rows") or len(analysis_df))
        total_rows = int(analysis_scope.get("total_rows") or len(raw_df))
        sample_note = (
            f"Preview used {sample_rows:,} sampled rows out of {total_rows:,} for AI analysis."
        )
        existing_message = str(effective_suggestion.get("message") or "").strip()
        if sample_note not in existing_message:
            effective_suggestion["message"] = (
                f"{existing_message} {sample_note}".strip()
                if existing_message
                else sample_note
            )
        key_meta["rows_in"] = int(len(raw_df))
        key_meta["rows_out"] = int(len(raw_df))
        key_meta["analysis_scope"] = "sampled_preview"
        key_meta["analysis_sample_rows"] = sample_rows
        if isinstance(db_match, dict):
            db_match["approximate"] = True
            db_match["analysis_sample_rows"] = sample_rows

    if pipeline_warnings:
        diagnostics_issues = diagnostics.get("issues") if isinstance(diagnostics, dict) else None
        if not isinstance(diagnostics_issues, list):
            diagnostics_issues = []
        diagnostics["issues"] = list(dict.fromkeys([*pipeline_warnings, *diagnostics_issues]))

    can_apply = not bool(diagnostics.get("wrong_mappings")) and not any(
        "unexpected server fallback" in str(item).lower()
        for item in diagnostics.get("issues", [])
    )
    rows_in = int(len(raw_df))
    if "normalized_df" in locals():
        del normalized_df
    if "payloads" in locals():
        del payloads
    if "storage_rows" in locals():
        del storage_rows
    del analysis_df
    del raw_df
    return _compose_filter_analyze_response(
        file_name=file.filename or "",
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
        rows_in=rows_in,
        key_meta=key_meta,
        effective_suggestion=effective_suggestion,
        diagnostics=diagnostics,
        db_match=db_match,
        primary_key_candidates=primary_key_candidates,
        can_apply=can_apply,
    )


@router.post("/filter-revert")
def revert_filter_apply(
    source: str = Form(...),
    dataset_type: str = Form(...),
    job_id: str | None = Form(None),
    revision_id: int | None = Form(None),
    db: Session = Depends(get_db),
    current_user: Any = Depends(get_current_user),
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
    record_upload_batch(
        db,
        source=revision_source,
        dataset_type=revision_dataset,
        job_id=revision_job,
        action="filter_revert",
        uploaded_by=_actor_email(current_user),
        file_name="",
        rows_in=int(len(payloads)),
        rows_inserted=int(inserted),
        rows_updated=int(quality_meta.get("updated_rows") or 0),
        deleted_rows=int(deleted),
        notes=f"Reverted filter revision {int(revision['id'])}.",
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
    current_user: Any = Depends(get_current_user),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    if source_norm is None or dataset_norm is None:
        raise HTTPException(status_code=400, detail="source and dataset_type are required")
    if dataset_norm not in {"sales", "claims"}:
        raise HTTPException(status_code=400, detail="dataset_type must be sales or claims")
    uploaded_at = datetime.now(timezone.utc)
    effective_job_id, auto_generated_job_id = resolve_job_id(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        requested_job_id=job_norm,
        uploaded_at=uploaded_at,
        prefer_existing_job_id=True,
    )

    df = await _parse_upload_dataframe(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to filter")

    normalized_df, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    date_formats = _infer_existing_date_formats(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
    )
    normalized_df = _align_date_columns_to_existing_format(
        normalized_df,
        date_formats=date_formats,
    )
    payloads = dataframe_to_payload_rows(normalized_df)
    storage_rows, key_meta = prepare_rows_for_storage(payloads, source=source_norm, dataset_type=dataset_norm)
    filtered_payloads = [item.get("data") for item in storage_rows if isinstance(item.get("data"), dict)]
    if not filtered_payloads:
        raise HTTPException(status_code=400, detail="No rows found after applying filters")
    filtered_rows_count = int(len(filtered_payloads))

    del payloads
    del storage_rows
    del normalized_df
    del df

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
            effective_job_id,
        ).all()
    )
    before_snapshot = _snapshot_rows(before_rows)
    replaced_rows, inserted_rows, quality_meta = _replace_tag_rows(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=effective_job_id,
        payloads=filtered_payloads,
        mode="merge",
    )
    revision_id = _save_filter_revision(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=effective_job_id,
        before_snapshot=before_snapshot,
        after_rows=inserted_rows,
    )
    record_upload_batch(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=effective_job_id,
        action="filter_apply",
        uploaded_by=_actor_email(current_user),
        uploaded_at=uploaded_at,
        file_name=file.filename or "",
        rows_in=filtered_rows_count,
        rows_inserted=int(inserted_rows),
        rows_updated=int(quality_meta.get("updated_rows") or 0),
        deleted_rows=int(replaced_rows),
        notes=(
            "Auto-generated job_id was assigned because the upload did not provide one."
            if auto_generated_job_id
            else ""
        ),
    )
    db.commit()
    _post_file_update(db, source_norm, dataset_norm, effective_job_id, action="filter-apply")

    return {
        "applied": True,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": effective_job_id,
        "auto_generated_job_id": auto_generated_job_id,
        "uploaded_by": _actor_email(current_user),
        "uploaded_at": uploaded_at.isoformat(),
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
    current_user: Any = Depends(get_current_user),
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

    df = await _parse_upload_dataframe(
        file,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows to filter")

    normalized_df, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    date_formats = _infer_existing_date_formats(
        db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_norm,
    )
    normalized_df = _align_date_columns_to_existing_format(
        normalized_df,
        date_formats=date_formats,
    )
    payloads = dataframe_to_payload_rows(normalized_df)
    storage_rows, key_meta = prepare_rows_for_storage(payloads, source=source_norm, dataset_type=dataset_norm)
    filtered_payloads = [item.get("data") for item in storage_rows if isinstance(item.get("data"), dict)]
    if not filtered_payloads:
        raise HTTPException(status_code=400, detail="No rows found after applying filters")
    filtered_rows_count = int(len(filtered_payloads))

    del payloads
    del storage_rows
    del normalized_df
    del df

    replaced_rows = 0
    inserted_rows = filtered_rows_count
    updated_rows = 0
    revision_id: int | None = None
    effective_job_id = job_norm
    auto_generated_job_id = False
    uploaded_at: datetime | None = None
    uploaded_by = _actor_email(current_user)
    if apply_to_db:
        uploaded_at = datetime.now(timezone.utc)
        effective_job_id, auto_generated_job_id = resolve_job_id(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            requested_job_id=job_norm,
            uploaded_at=uploaded_at,
            prefer_existing_job_id=True,
        )
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
            effective_job_id,
        ).all()
        )
        before_snapshot = _snapshot_rows(before_rows)
        replaced_rows, inserted_rows, quality_meta = _replace_tag_rows(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=effective_job_id,
            payloads=filtered_payloads,
            mode="merge",
        )
        updated_rows = int(quality_meta.get("updated_rows") or 0)
        revision_id = _save_filter_revision(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=effective_job_id,
            before_snapshot=before_snapshot,
            after_rows=inserted_rows,
        )
        record_upload_batch(
            db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=effective_job_id,
            action="filter_download_apply",
            uploaded_by=uploaded_by,
            uploaded_at=uploaded_at,
            file_name=file.filename or "",
            rows_in=int(len(filtered_payloads)),
            rows_inserted=int(inserted_rows),
            rows_updated=int(updated_rows),
            deleted_rows=int(replaced_rows),
            notes=(
                "Auto-generated job_id was assigned because the upload did not provide one."
                if auto_generated_job_id
                else ""
            ),
        )
        db.commit()
        _post_file_update(db, source_norm, dataset_norm, effective_job_id, action="filter-download")

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
        if auto_generated_job_id and effective_job_id:
            summary += f" Auto-generated job_id: {effective_job_id}."

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Filter-Summary": summary,
        "X-Filter-Apply-Db": str(bool(apply_to_db)).lower(),
        "X-Filter-Rows": str(len(filtered_payloads)),
    }
    if revision_id is not None:
        headers["X-Filter-Revision-Id"] = str(int(revision_id))
    if apply_to_db and effective_job_id:
        headers["X-Filter-Job-Id"] = str(effective_job_id)
        headers["X-Filter-Job-Auto-Generated"] = str(bool(auto_generated_job_id)).lower()
    if apply_to_db and uploaded_at is not None:
        headers["X-Filter-Uploaded-By"] = uploaded_by
        headers["X-Filter-Uploaded-At"] = uploaded_at.isoformat()

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
    repair_reliance_brands: bool = Query(False),
    db: Session = Depends(get_db),
):
    source_norm = _normalize_source_key(source)
    dataset_norm = _normalize(dataset_type)
    job_norm = _normalize(job_id)
    try:
        result = run_daily_refresh(
            db=db,
            source=source_norm,
            dataset_type=dataset_norm,
            job_id=job_norm,
            repair_reliance_brands=repair_reliance_brands,
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




