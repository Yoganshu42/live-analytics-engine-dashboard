import logging
import asyncio
import json
import os
import re
import difflib
import hashlib
import time
import math
import threading
from pathlib import Path
from typing import Any
from io import BytesIO
from datetime import datetime, date, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

import pandas as pd
from fastapi import (
    FastAPI,
    Depends,
    UploadFile,
    File,
    Form,
    HTTPException,
    Request,
    Response,
)
from fastapi.responses import ORJSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from db.session import SessionLocal, engine
from db.base import Base
from db.deps import get_db

from models.data_rows import DataRow
from models.deck_pptx_cache import DeckPptxCache
from models.manual_updates import ManualUpdateMarker
from models.precomputed_analytics import PrecomputedGraph, PrecomputedInsight, PrecomputedSummary
from authentication import models as auth_models
from authentication.deps import get_current_user
from authentication.router import router as auth_router
from services.manual_update_service import mark_manual_update
from services.precompute_service import rebuild_precomputed_analytics
from services.precomputed_repository import (
    get_precomputed_graph,
    get_precomputed_summary,
    get_precomputed_insights,
    upsert_precomputed_insights,
    clear_precomputed_for_source_dataset,
)
from services.ai_mapper import suggest_reverse_mapping
from services.analytics_engine import filter_by_date_range
from services.admin_upload_service import backfill_missing_job_ids
from services.deck_cache_service import invalidate_deck_cache_for_source_dataset
from services.forecast_service import (
    aggregate_financial_year,
    combine_monthly_history,
    forecast_monthly_points,
    load_monthly_history,
)
from services.maintenance_service import refresh_master_overview_cache, run_daily_refresh_if_due
from services.partner_filter_service import (
    dataframe_to_payload_rows,
    normalize_partner_dataframe,
    normalize_partner_rows,
)
from services.data_quality_service import prepare_rows_for_storage
from services.samsung_partner_config import (
    SAMSUNG_PARTNER_LABELS,
    SAMSUNG_PARTNER_SOURCES,
    normalize_samsung_source,
)

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
_daily_refresh_scheduler_lock = threading.Lock()
_daily_refresh_scheduler_started = False

def _json_safe(value: Any):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        if value != value or value == float("inf") or value == float("-inf"):
            return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

def _clean_json_row(row: dict) -> dict:
    return {k: _json_safe(v) for k, v in row.items()}


def _start_daily_refresh_scheduler() -> None:
    global _daily_refresh_scheduler_started
    with _daily_refresh_scheduler_lock:
        if _daily_refresh_scheduler_started:
            return
        _daily_refresh_scheduler_started = True

    initial_delay_seconds = max(int(os.getenv("AUTO_DAILY_REFRESH_STARTUP_DELAY_SECONDS", "15")), 0)
    check_interval_seconds = max(int(os.getenv("AUTO_DAILY_REFRESH_CHECK_INTERVAL_SECONDS", "900")), 60)

    def _worker() -> None:
        if initial_delay_seconds:
            time.sleep(initial_delay_seconds)
        while True:
            maintenance_db = SessionLocal()
            try:
                result = run_daily_refresh_if_due(db=maintenance_db)
                status = str(result.get("status") or "").strip().lower()
                if status == "success":
                    logger.info(
                        "Auto daily refresh completed run_day=%s",
                        result.get("run_day"),
                    )
                elif status == "failed":
                    logger.error(
                        "Auto daily refresh failed error=%s",
                        result.get("error"),
                    )
            except Exception:
                logger.exception("Unexpected auto daily refresh scheduler error")
            finally:
                maintenance_db.close()
            time.sleep(check_interval_seconds)

    threading.Thread(
        target=_worker,
        name="auto-daily-refresh",
        daemon=True,
    ).start()


def _refresh_jobs(job_id: str | None) -> list[str | None]:
    normalized = (job_id or "").strip() or None
    jobs: list[str | None] = [normalized]
    if normalized is not None:
        jobs.append(None)
    return jobs


def _normalize_data_tag(
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> tuple[str, str, str | None]:
    src = _normalize_source_key((source or "").strip())
    ds = (dataset_type or "").strip().lower()
    jb = (job_id or "").strip() or None
    return src, ds, jb


def _overwrite_rows_for_source_dataset(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    payloads: list[dict[str, Any]],
) -> tuple[int, int, str | None, dict[str, Any]]:
    src, ds, jb = _normalize_data_tag(source=source, dataset_type=dataset_type, job_id=job_id)
    storage_rows, quality_meta = prepare_rows_for_storage(
        payloads,
        source=src,
        dataset_type=ds,
    )
    existing_query = db.query(DataRow).filter(
        DataRow.source == src,
        DataRow.dataset_type == ds,
    )
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
            scoped_query = existing_query.filter(DataRow.record_key.in_(chunk))
            if jb is None:
                scoped_query = scoped_query.filter(DataRow.job_id.is_(None))
            else:
                scoped_query = scoped_query.filter(DataRow.job_id == jb)
            for row in scoped_query.all():
                key = str(row.record_key or "").strip()
                if key:
                    existing_by_key[key] = row

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

    inserted_rows = 0
    updated_rows = 0
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
                    "job_id": jb,
                    "source": src,
                    "dataset_type": ds,
                    "data": payload,
                    "record_key": rk or None,
                    "primary_key_name": pk_name,
                }
            )
            inserted_rows += 1
            continue
        existing.data = _merge_payload(existing.data if isinstance(existing.data, dict) else {}, payload)
        if pk_name:
            existing.primary_key_name = pk_name
        if jb is not None:
            existing.job_id = jb
        updated_rows += 1

    clear_precomputed_for_source_dataset(db, source=src, dataset_type=ds)
    if insert_payloads:
        db.bulk_insert_mappings(
            DataRow,
            insert_payloads,
        )
    db.commit()
    quality_meta["merge_mode"] = "upsert"
    quality_meta["updated_rows"] = int(updated_rows)
    quality_meta["inserted_rows"] = int(inserted_rows)
    quality_meta["deleted_rows"] = 0
    return 0, int(inserted_rows), jb, quality_meta


def _refresh_after_data_change(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    action: str,
) -> None:
    src = _normalize_source_key(source)
    ds = dataset_type.lower().strip()
    refresh_jobs = _refresh_jobs(job_id)
    invalidate_deck_cache_for_source_dataset(db=db, source=src, dataset_type=ds)

    for refresh_job in refresh_jobs:
        mark_manual_update(
            db=db,
            source=src,
            dataset_type=ds,
            job_id=refresh_job,
        )
    db.commit()

    for refresh_job in refresh_jobs:
        invalidate_dataframe_cache(
            source=src,
            dataset_type=ds,
            job_id=refresh_job,
        )
        if src in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
            invalidate_reliance_load_cache(
                source=src,
                dataset_type=ds,
                job_id=refresh_job,
            )
        elif src in {"godrej", "goodrej", "goddrej"}:
            invalidate_godrej_load_cache(
                source=src,
                dataset_type=ds,
                job_id=refresh_job,
            )
        elif src == "hitachi":
            invalidate_hitachi_load_cache(
                source=src,
                dataset_type=ds,
                job_id=refresh_job,
            )

    for refresh_job in refresh_jobs:
        try:
            rebuild_precomputed_analytics(
                db=db,
                source=src,
                dataset_type=ds,
                job_id=refresh_job,
            )
        except Exception:
            db.rollback()
            logger.exception(
                "Failed to rebuild precomputed analytics after %s source=%s dataset=%s job_id=%s",
                action,
                src,
                ds,
                refresh_job,
            )

    try:
        refresh_master_overview_cache(db=db)
    except Exception:
        db.rollback()
        logger.exception(
            "Failed to refresh master overview after %s source=%s dataset=%s",
            action,
            src,
            ds,
        )


# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI(
    title="Live Dashboard API",
    version="1.0.0",
    default_response_class=ORJSONResponse,
    swagger_ui_parameters={
        "persistAuthorization": True,
        "displayRequestDuration": True,
    },
)

# --------------------------------------------------
# DB INIT
# --------------------------------------------------
@app.on_event("startup")
def _init_db():
    try:
        Base.metadata.create_all(bind=engine)
        with engine.begin() as conn:
            # Some DB restores (like analytics.sql) only include `data_rows` + `users`.
            # Create the manual update marker table explicitly so admin "Replace Tag"
            # and upload flows don't fail in production.
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.manual_update_markers (
                        id SERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        dataset_type TEXT NOT NULL,
                        job_key TEXT NOT NULL DEFAULT '',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_update_marker_tag
                    ON public.manual_update_markers (source, dataset_type, job_key);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_source
                    ON public.manual_update_markers (source);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_dataset
                    ON public.manual_update_markers (dataset_type);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_job
                    ON public.manual_update_markers (job_key);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.deck_pptx_cache (
                        id SERIAL PRIMARY KEY,
                        cache_key TEXT NOT NULL,
                        partners_key TEXT NOT NULL DEFAULT '',
                        dataset_type TEXT NOT NULL DEFAULT 'sales',
                        job_key TEXT NOT NULL DEFAULT '',
                        from_date TEXT NOT NULL DEFAULT '',
                        to_date TEXT NOT NULL DEFAULT '',
                        include_tables BOOLEAN NOT NULL DEFAULT TRUE,
                        week_window INTEGER NOT NULL DEFAULT 4,
                        data_fingerprint TEXT NOT NULL DEFAULT '',
                        filename TEXT NOT NULL DEFAULT 'partner_deck_sales.pptx',
                        size_bytes INTEGER NOT NULL DEFAULT 0,
                        pptx_blob BYTEA NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_deck_pptx_cache_key
                    ON public.deck_pptx_cache (cache_key);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_deck_pptx_cache_updated_at
                    ON public.deck_pptx_cache (updated_at);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_data_rows_source_dataset
                    ON public.data_rows (source, dataset_type)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_data_rows_source_dataset_job
                    ON public.data_rows (source, dataset_type, job_id)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    ALTER TABLE public.data_rows
                    ADD COLUMN IF NOT EXISTS record_key TEXT;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    ALTER TABLE public.data_rows
                    ADD COLUMN IF NOT EXISTS primary_key_name TEXT;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_data_rows_record_key
                    ON public.data_rows (record_key);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_data_rows_unique_record
                    ON public.data_rows (source, dataset_type, COALESCE(job_id, ''), record_key)
                    WHERE record_key IS NOT NULL;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.admin_filter_revisions (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        dataset_type TEXT NOT NULL,
                        job_key TEXT NOT NULL DEFAULT '',
                        before_blob BYTEA NOT NULL,
                        before_rows INTEGER NOT NULL DEFAULT 0,
                        after_rows INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        reverted_at TIMESTAMPTZ NULL
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_admin_filter_revisions_tag
                    ON public.admin_filter_revisions (source, dataset_type, job_key, created_at DESC);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.admin_upload_batches (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        dataset_type TEXT NOT NULL,
                        job_key TEXT NOT NULL DEFAULT '',
                        action TEXT NOT NULL DEFAULT 'update',
                        file_name TEXT NOT NULL DEFAULT '',
                        uploaded_by TEXT NOT NULL DEFAULT '',
                        uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        rows_in INTEGER NOT NULL DEFAULT 0,
                        rows_inserted INTEGER NOT NULL DEFAULT 0,
                        rows_updated INTEGER NOT NULL DEFAULT 0,
                        deleted_rows INTEGER NOT NULL DEFAULT 0,
                        notes TEXT NOT NULL DEFAULT ''
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_admin_upload_batches_scope
                    ON public.admin_upload_batches (source, dataset_type, job_key, uploaded_at DESC);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_admin_upload_batches_uploaded_by
                    ON public.admin_upload_batches (uploaded_by);
                    """
                )
            )
    except Exception:
        logger.exception("DB init failed")
    else:
        maintenance_db = SessionLocal()
        try:
            backfill_summary = backfill_missing_job_ids(maintenance_db)
            if int(backfill_summary.get("groups_backfilled") or 0) > 0:
                maintenance_db.commit()
                logger.info(
                    "Auto backfilled missing job_ids groups=%s rows=%s",
                    int(backfill_summary.get("groups_backfilled") or 0),
                    int(backfill_summary.get("rows_backfilled") or 0),
                )
            else:
                maintenance_db.rollback()
        except Exception:
            maintenance_db.rollback()
            logger.exception("Failed to auto backfill missing job_ids on startup")
        finally:
            maintenance_db.close()

    prewarm_raw = os.getenv("LLM_PREWARM", "1").strip()
    prewarm_enabled = prewarm_raw.lower() not in {"0", "false", "no", "off"}
    chatbot_enabled = os.getenv("ENABLE_CHATBOT", "1").strip().lower() not in {"0", "false", "no", "off"}
    insights_enabled = os.getenv("ENABLE_GRAPH_INSIGHTS", "1").strip().lower() not in {"0", "false", "no", "off"}
    auto_daily_refresh_enabled = os.getenv("AUTO_DAILY_REFRESH", "1").strip().lower() not in {"0", "false", "no", "off"}
    if prewarm_enabled and (chatbot_enabled or insights_enabled):
        try:
            threading.Thread(target=_prewarm_llm_model, name="llm-prewarm", daemon=True).start()
        except Exception:
            logger.exception("Failed to schedule LLM prewarm")
    if auto_daily_refresh_enabled:
        try:
            _start_daily_refresh_scheduler()
        except Exception:
            logger.exception("Failed to schedule auto daily refresh")

# --------------------------------------------------
#  CORS  FIXED (DEV SAFE)
# --------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          #  FIX
    allow_credentials=False,      #  FIX
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=[
        "Content-Disposition",
        "X-Transform-Summary",
        "X-Transform-Operations",
        "X-Transform-Rows-Affected",
        "X-Transform-Columns-Touched",
        "X-Transform-Skipped",
        "X-Filter-Summary",
        "X-Filter-Apply-Db",
        "X-Filter-Rows",
        "X-Filter-Revision-Id",
        "X-Filter-Job-Id",
        "X-Filter-Job-Auto-Generated",
        "X-Filter-Uploaded-By",
        "X-Filter-Uploaded-At",
    ],
)


# --------------------------------------------------
# CORS PREFLIGHT (EXPLICIT)
# --------------------------------------------------
@app.options("/{path:path}")
def preflight(path: str, request: Request):
    return Response(status_code=204)


# --------------------------------------------------
# ROUTERS
# --------------------------------------------------
from routers.analytics import analytics_summary, compute_by_dimension_rows, router as analytics_router
from routers.admin_files import router as admin_files_router
from routers.deck_export import router as deck_export_router
from services.analytics.goodrej_engine import invalidate_godrej_load_cache
from services.analytics.hitachi_engine import invalidate_hitachi_load_cache
from services.analytics.reliance_engine import invalidate_reliance_load_cache
from services.analytics_repository import invalidate_dataframe_cache, get_dataframe
app.include_router(auth_router)
app.include_router(analytics_router, dependencies=[Depends(get_current_user)])
app.include_router(admin_files_router)
app.include_router(deck_export_router)

# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "ok"}

# ==================================================
# UPLOAD (CSV/XLSX)
# ==================================================
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source: str | None = Form(None),
    dataset_type: str | None = Form(None),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    if not source or not dataset_type:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: source and dataset_type.",
        )
    source_norm = _normalize_source_key(source)
    dataset_norm = (dataset_type or "").strip().lower()

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file.")

    name = (file.filename or "").lower()
    buf = BytesIO(contents)

    import pandas as pd
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(buf)
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            df = pd.read_excel(buf)
        else:
            raise HTTPException(status_code=400, detail="Only .csv, .xls, and .xlsx files are supported.")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")

    normalized_df, normalize_meta = normalize_partner_dataframe(
        df,
        source=source_norm,
        dataset_type=dataset_norm,
    )
    rows = dataframe_to_payload_rows(normalized_df)

    deleted_rows, inserted_rows, normalized_job_id, quality_meta = _overwrite_rows_for_source_dataset(
        db=db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=job_id,
        payloads=rows,
    )
    _refresh_after_data_change(
        db=db,
        source=source_norm,
        dataset_type=dataset_norm,
        job_id=normalized_job_id,
        action="upload",
    )

    logger.info(
        "UPLOAD overwrite: source=%s dataset=%s deleted_rows=%s inserted_rows=%s",
        source,
        dataset_type,
        deleted_rows,
        inserted_rows,
    )

    return {
        "deleted_rows": deleted_rows,
        "rows_inserted": inserted_rows,
        "source": source_norm,
        "dataset_type": dataset_norm,
        "job_id": normalized_job_id,
        "normalization": normalize_meta,
        "data_quality": quality_meta,
    }

# ==================================================
# INGEST (JSON)
# ==================================================
class IngestPayload(BaseModel):
    source: str = Field(..., min_length=1)
    dataset_type: str = Field(..., min_length=1)
    job_id: str | None = None
    rows: list[dict[str, Any]] = Field(..., min_items=1)


@app.post("/ingest")
def ingest_rows(
    payload: IngestPayload,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    cleaned_rows = [_clean_json_row(row) for row in payload.rows]
    normalized_rows, normalize_meta = normalize_partner_rows(
        cleaned_rows,
        source=payload.source,
        dataset_type=payload.dataset_type,
    )
    deleted_rows, inserted_rows, normalized_job_id, quality_meta = _overwrite_rows_for_source_dataset(
        db=db,
        source=payload.source,
        dataset_type=payload.dataset_type,
        job_id=payload.job_id,
        payloads=normalized_rows,
    )
    _refresh_after_data_change(
        db=db,
        source=payload.source,
        dataset_type=payload.dataset_type,
        job_id=normalized_job_id,
        action="ingest",
    )
    return {
        "deleted_rows": deleted_rows,
        "rows_inserted": inserted_rows,
        "job_id": normalized_job_id,
        "normalization": normalize_meta,
        "data_quality": quality_meta,
    }


_FILE_COLUMN_ALIAS_TARGETS: dict[str, str] = {
    "planprice": "Plan Selling Price",
    "plansellingprice": "Plan Selling Price",
    "grosspremium": "Gross Premium",
    "earnedpremium": "Earned Premium",
    "zopperearnedpremium": "Zopper Earned Premium",
    "zoppershare": "Zopper Share",
    "zoppersharedtransferprice": "Zopper Shared ( Transfer Price )",
    "totalbillingamount": "Total Billing Amount",
    "billingamount": "Billing Amount",
    "invoicevalue": "INVOICE_VALUE",
    "brand": "Brand",
    "articlebrand": "Article_Brand",
    "itembrand": "Item_Brand",
    "planstartdate": "Plan Start Date",
    "planenddate": "Plan End Date",
    "warrantystartdate": "Warranty Start Date",
    "warrantyenddate": "Warranty End Date",
    "claimscost": "Claim_Amount",
    "claimamount": "Claim_Amount",
    "claimvalues": "Claim_Amount",
    "claimdate": "Claim Date",
    "plancategory": "Plan Category",
    "deviceplancategory": "Device Plan Category",
    "state": "State",
    "month": "Month",
    "channel": "Channel",
    "productcategory": "Product_Category",
}

_COPY_INSTRUCTION_PATTERNS = [
    re.compile(r"^(?:copy|map)\s+(?P<source>.+?)\s+(?:to|into|as|->)\s+(?P<target>.+)$", re.IGNORECASE),
    re.compile(r"^(?:map)\s+(?P<target>.+?)\s+from\s+(?P<source>.+)$", re.IGNORECASE),
    re.compile(r"^(?:use)\s+(?P<source>.+?)\s+as\s+(?P<target>.+)$", re.IGNORECASE),
]
_SET_INSTRUCTION_PATTERN = re.compile(
    r"^(?:fill|set|update)\s+(?:(?P<scope>missing|blank|empty|null)\s+)?(?P<target>.+?)\s*(?:with|to|as|=)\s*(?P<value>.+)$",
    re.IGNORECASE,
)
_IS_INSTRUCTION_PATTERN = re.compile(r"^(?P<target>.+?)\s+(?:will\s+be|is|=)\s+(?P<value>.+)$", re.IGNORECASE)
_DUPLICATE_INTENT_PATTERN = re.compile(r"\bduplicate(?:s|d|ing)?\b", re.IGNORECASE)
_DEDUPE_ACTION_PATTERN = re.compile(r"\b(?:remove|drop|delete|dedupe|deduplicate|clean)\b", re.IGNORECASE)
_NEGATED_DEDUPE_PATTERN = re.compile(
    r"\b(?:do\s+not|don't|dont|not|no|without|keep)\b[^.;,\n]{0,80}\b(?:remove|drop|delete|dedupe|deduplicate|clean)\b",
    re.IGNORECASE,
)
_ROW_COUNT_INTENT_PATTERN = re.compile(
    r"\b(?:how\s+many\s+rows(?:\s+are\s+there)?|row\s+count|count\s+rows|number\s+of\s+rows|total\s+rows)\b",
    re.IGNORECASE,
)
_DATE_FILTER_ACTION_PATTERN = re.compile(r"\b(?:keep|filter|include|use|add)\b", re.IGNORECASE)
_DATE_FILTER_RANGE_PATTERN = re.compile(
    r"\b(?:from|between)\s+(?P<start>.+?)\s+(?:to|and)\s+(?P<end>.+)$",
    re.IGNORECASE,
)
_DATE_FILTER_AFTER_PATTERN = re.compile(
    r"\b(?:on\s+or\s+after|after|since)\s+(?P<start>.+)$",
    re.IGNORECASE,
)
_DATE_FILTER_BEFORE_PATTERN = re.compile(
    r"\b(?:on\s+or\s+before|before|until|upto|up\s+to|till)\s+(?P<end>.+)$",
    re.IGNORECASE,
)
_DATE_FILTER_SINGLE_PATTERN = re.compile(r"\b(?:for|in)\s+(?P<value>.+)$", re.IGNORECASE)
_DATE_TOKEN_HINT_PATTERN = re.compile(
    r"(?:\d{4}-\d{2}-\d{2}|\d{4}/\d{2}/\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{6}|\d{4}-\d{2}|[A-Za-z]{3,9}[-\s]\d{2,4})",
    re.IGNORECASE,
)
_DATE_FILTER_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "sales": (
        "Date",
        "Month",
        "Start_Date",
        "Start Date",
        "Plan Start Date",
        "Invoice Date",
        "Invoice_Date_",
        "Purchase Date",
        "Warranty Start Date",
    ),
    "claims": (
        "Claim Date",
        "Date",
        "Month",
        "Payment Date",
        "Payment_date",
        "Call Date",
        "Call_Date",
        "Day of Call_Date",
    ),
}
_UNSAFE_TRANSFORM_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


def _normalize_file_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def _clean_instruction_part(value: str) -> str:
    cleaned = str(value or "").strip()
    cleaned = cleaned.strip().strip("`")
    cleaned = cleaned.strip().strip('"').strip("'")
    cleaned = re.sub(r"^[\[\(\{]+", "", cleaned)
    cleaned = re.sub(r"[\]\)\}]+$", "", cleaned)
    cleaned = re.sub(r"^(?:column|field)\s+", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _sanitize_header_value(value: str, limit: int = 220) -> str:
    out = re.sub(r"[\r\n\t]+", " ", str(value or ""))
    out = re.sub(r"\s+", " ", out).strip()
    if len(out) <= limit:
        return out
    return f"{out[:limit].rstrip()}..."


def _safe_transform_filename(base_name: str, extension: str) -> str:
    base = re.sub(r"\.[^.]+$", "", str(base_name or "").strip())
    base = _UNSAFE_TRANSFORM_FILENAME.sub("_", base).strip("._-")
    if not base:
        base = "chatbot_file"
    ext = (extension or "csv").strip().lower()
    if ext not in {"csv", "xlsx"}:
        ext = "csv"
    return f"{base}_updated.{ext}"


def _parse_file_transform_dataframe(file: UploadFile):
    import pandas as pd

    raw = file.file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file uploaded.")

    filename = (file.filename or "").strip()
    lowered = filename.lower()
    buf = BytesIO(raw)
    try:
        if lowered.endswith(".csv"):
            df = pd.read_csv(buf)
            inferred_ext = "csv"
        elif lowered.endswith(".xlsx") or lowered.endswith(".xls"):
            df = pd.read_excel(buf)
            inferred_ext = "xlsx"
        else:
            raise HTTPException(status_code=400, detail="Only .csv, .xls, and .xlsx files are supported.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")

    df.columns = [str(col).strip() for col in df.columns]
    return df, inferred_ext


def _split_file_instructions(instruction: str) -> list[str]:
    if not instruction or not instruction.strip():
        return []
    parts = re.split(r"[;\n]+", instruction)
    return [part.strip() for part in parts if part.strip()]


def _missing_mask(series):
    as_text = (
        series.astype(str)
        .str.strip()
        .str.lower()
    )
    return series.isna() | as_text.isin({"", "nan", "none", "null", "na"})


def _parse_constant_value(raw_value: str):
    value = _clean_instruction_part(raw_value)
    if not value:
        return ""

    low = value.lower()
    if low in {"none", "null", "nan", "na"}:
        return None
    if low == "true":
        return True
    if low == "false":
        return False

    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except Exception:
            return value
    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except Exception:
            return value
    return value


def _truncate_transform_preview(value: Any, max_len: int = 40) -> str:
    text = str(value)
    return text if len(text) <= max_len else f"{text[:max_len - 1]}..."


def _trim_duplicate_column_phrase(value: str) -> str:
    trimmed = _clean_instruction_part(value)
    trimmed = re.sub(r"[?.,;:!]+$", "", trimmed).strip()
    trimmed = re.split(
        r"(?i)\b(?:from|for|with|and|or|where|that|which|please|dataset|file|values?|duplicates?|duplicate)\b",
        trimmed,
        maxsplit=1,
    )[0].strip()
    return _clean_instruction_part(trimmed)


def _extract_explicit_duplicate_column_label(request_text: str) -> str | None:
    text = (request_text or "").strip()
    if not text:
        return None

    quoted = re.search(r"`(?P<col>[^`]+)`", text)
    if quoted:
        candidate = _trim_duplicate_column_phrase(quoted.group("col"))
        if candidate:
            return candidate

    patterns = [
        re.compile(r"\bcolumn(?:\s+name)?\s*(?:is|=|:)?\s*(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
        re.compile(r"\bin\s+column(?:\s+name)?\s*(?:is|=|:)?\s*(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
        re.compile(r"\bfor\s+column(?:\s+name)?\s*(?:is|=|:)?\s*(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if not match:
            continue
        candidate = _trim_duplicate_column_phrase(match.group("col"))
        if candidate:
            return candidate
    return None


def _closest_duplicate_column_candidates(df, requested_label: str, limit: int = 5) -> list[str]:
    columns = [str(col) for col in df.columns]
    if not columns:
        return []

    normalized_to_column: dict[str, str] = {}
    for col in columns:
        key = _normalize_file_key(col)
        if key and key not in normalized_to_column:
            normalized_to_column[key] = col

    requested_key = _normalize_file_key(requested_label)
    candidates: list[str] = []

    if requested_key:
        close_keys = difflib.get_close_matches(
            requested_key,
            list(normalized_to_column.keys()),
            n=limit,
            cutoff=0.35,
        )
        for key in close_keys:
            col = normalized_to_column[key]
            if col not in candidates:
                candidates.append(col)

    if len(candidates) < limit:
        target_tokens = set(re.findall(r"[a-z0-9]+", requested_label.lower()))
        scored: list[tuple[int, str]] = []
        for col in columns:
            col_tokens = set(re.findall(r"[a-z0-9]+", col.lower()))
            overlap = len(target_tokens & col_tokens)
            if overlap > 0:
                scored.append((overlap, col))
        for _score, col in sorted(scored, key=lambda item: (-item[0], item[1])):
            if col not in candidates:
                candidates.append(col)
            if len(candidates) >= limit:
                break

    return candidates[:limit]


def _extract_deduplicate_column_label(request_text: str) -> str | None:
    explicit = _extract_explicit_duplicate_column_label(request_text)
    if explicit:
        return explicit

    text = (request_text or "").strip()
    if not text:
        return None

    patterns = [
        re.compile(r"\b(?:by|using|on)\s+(?:column(?:\s+name)?\s*)?(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
        re.compile(r"\bfrom\s+(?:column(?:\s+name)?\s*)?(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
        re.compile(r"\bin\s+(?:column(?:\s+name)?\s*)?(?P<col>[A-Za-z0-9_][A-Za-z0-9_ ./-]*)", re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if not match:
            continue
        candidate = _trim_duplicate_column_phrase(match.group("col"))
        if not candidate:
            continue
        lower_candidate = candidate.lower()
        if lower_candidate in {"file", "the file", "dataset", "the dataset", "this file", "this dataset"}:
            continue
        return candidate
    return None


def _is_negated_deduplicate_request(text: str) -> bool:
    request_text = str(text or "").strip()
    if not request_text:
        return False
    if not _DUPLICATE_INTENT_PATTERN.search(request_text):
        return False
    return bool(_NEGATED_DEDUPE_PATTERN.search(request_text))


def _build_noop_instruction_result(df, line: str) -> dict[str, Any] | None:
    text = str(line or "").strip()
    if not text:
        return None

    row_count_requested = bool(_ROW_COUNT_INTENT_PATTERN.search(text))
    dedupe_blocked = _is_negated_deduplicate_request(text)
    duplicate_columns_only = bool(
        re.search(r"\bduplicate\s+columns?\b", text, re.IGNORECASE)
        and not _extract_deduplicate_column_label(text)
    )

    if not any([row_count_requested, dedupe_blocked, duplicate_columns_only]):
        return None

    details: list[str] = []
    if dedupe_blocked:
        details.append("duplicate removal not applied")
    if duplicate_columns_only and not dedupe_blocked:
        details.append("duplicate column cleanup not applied")
    if row_count_requested:
        details.append(f"current row count {int(len(df))}")

    description = "kept uploaded rows unchanged"
    if details:
        description += " (" + "; ".join(details) + ")"

    return {
        "description": description,
        "rows_affected": 0,
        "columns_touched": [],
    }


def _apply_deduplicate_instruction(
    df,
    line: str,
    alias_map: dict[str, str],
) -> dict[str, Any] | None:
    text = (line or "").strip()
    if not text:
        return None

    if _is_negated_deduplicate_request(text):
        return None

    if re.search(r"\bduplicate\s+columns?\b", text, re.IGNORECASE) and not _extract_deduplicate_column_label(text):
        return None

    if not (_DEDUPE_ACTION_PATTERN.search(text) and _DUPLICATE_INTENT_PATTERN.search(text)):
        return None

    requested_column = _extract_deduplicate_column_label(text)
    if requested_column:
        target_col = _resolve_instruction_column(df, requested_column, alias_map, allow_create=False)
        if target_col is None or target_col not in df.columns:
            suggestions = _closest_duplicate_column_candidates(df, requested_column)
            note = f"Dedup skipped: column `{requested_column}` was not found."
            if suggestions:
                note += " Closest columns: " + ", ".join(f"`{col}`" for col in suggestions) + "."
            return {
                "description": note,
                "rows_affected": 0,
                "columns_touched": [],
            }

        before = int(len(df))
        df.drop_duplicates(subset=[target_col], keep="first", inplace=True, ignore_index=True)
        removed = max(0, before - int(len(df)))
        return {
            "description": f"removed duplicate rows by `{target_col}` (removed {removed})",
            "rows_affected": removed,
            "columns_touched": [target_col],
        }

    before = int(len(df))
    df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
    removed = max(0, before - int(len(df)))
    return {
        "description": f"removed fully duplicate rows across all columns (removed {removed})",
        "rows_affected": removed,
        "columns_touched": [],
    }


def _resolve_duplicate_target_column(
    df,
    request_text: str,
    alias_map: dict[str, str],
    explicit_column_label: str | None = None,
) -> str | None:
    if explicit_column_label:
        return _resolve_instruction_column(df, explicit_column_label, alias_map, allow_create=False)

    lowered = (request_text or "").strip().lower()

    preferred_labels: list[str] = []
    if "claim" in lowered:
        preferred_labels.extend([
            "claim values",
            "claim amount",
            "claims cost",
            "claim",
        ])
    if "brand" in lowered:
        preferred_labels.extend(["brand", "article brand", "item brand"])
    if "premium" in lowered:
        preferred_labels.extend(["gross premium", "earned premium", "zopper earned premium"])
    if "quantity" in lowered or "count" in lowered:
        preferred_labels.extend(["quantity", "count", "units sold"])

    for label in preferred_labels:
        column = _resolve_instruction_column(df, label, alias_map, allow_create=False)
        if column is not None:
            return column

    # If no explicit signal is found, pick the column with the highest duplicate footprint.
    best_column: str | None = None
    best_duplicate_rows = -1
    for col in df.columns:
        series = df[col]
        text_series = series.astype(str).str.strip()
        mask = ~series.isna() & ~text_series.str.lower().isin({"", "nan", "none", "null", "na"})
        filtered = text_series[mask]
        if filtered.empty:
            continue
        counts = filtered.value_counts()
        duplicate_rows = int(counts[counts > 1].sum())
        if duplicate_rows > best_duplicate_rows:
            best_duplicate_rows = duplicate_rows
            best_column = str(col)

    if best_column is not None:
        return best_column
    return str(df.columns[0]) if len(df.columns) else None


def _analyze_duplicate_instruction(
    df,
    request_text: str,
    alias_map: dict[str, str],
) -> dict[str, Any] | None:
    if _is_negated_deduplicate_request(request_text):
        return None

    if re.search(r"\bduplicate\s+columns?\b", request_text or "", re.IGNORECASE) and not _extract_explicit_duplicate_column_label(request_text):
        return None

    if not _DUPLICATE_INTENT_PATTERN.search(request_text or ""):
        return None

    explicit_column_label = _extract_explicit_duplicate_column_label(request_text)
    target_col = _resolve_duplicate_target_column(
        df,
        request_text,
        alias_map,
        explicit_column_label=explicit_column_label,
    )

    if explicit_column_label and (not target_col or target_col not in df.columns):
        suggestions = _closest_duplicate_column_candidates(df, explicit_column_label)
        summary = f"Duplicate scan skipped: column `{explicit_column_label}` was not found in the uploaded file."
        if suggestions:
            summary += " Closest columns: " + ", ".join(f"`{col}`" for col in suggestions) + "."
        return {
            "summary": summary,
            "rows_affected": 0,
            "columns_touched": [],
        }

    if not target_col or target_col not in df.columns:
        return None

    series = df[target_col]
    text_series = series.astype(str).str.strip()
    mask = ~series.isna() & ~text_series.str.lower().isin({"", "nan", "none", "null", "na"})
    filtered = text_series[mask]

    if filtered.empty:
        summary = f"Duplicate scan in `{target_col}`: no non-empty values were available to evaluate."
        return {
            "summary": summary,
            "rows_affected": 0,
            "columns_touched": [target_col],
        }

    counts = filtered.value_counts()
    duplicate_counts = counts[counts > 1]
    duplicate_rows = int(duplicate_counts.sum())

    if duplicate_counts.empty:
        summary = f"Duplicate scan in `{target_col}`: no duplicate values found."
    else:
        preview = ", ".join(
            f"{_truncate_transform_preview(value)} ({int(count)}x)"
            for value, count in duplicate_counts.head(12).items()
        )
        summary = (
            f"Duplicate scan in `{target_col}`: found {len(duplicate_counts)} duplicate value(s) "
            f"across {duplicate_rows} row(s). Top duplicates: {preview}."
        )

    return {
        "summary": summary,
        "rows_affected": duplicate_rows,
        "columns_touched": [target_col],
    }


def _build_file_alias_map(df, source: str | None, dataset_type: str | None) -> dict[str, str]:
    alias_map = dict(_FILE_COLUMN_ALIAS_TARGETS)
    for col in df.columns:
        alias_map[_normalize_file_key(col)] = str(col)

    src = _normalize_source_key(source or "") if source else ""
    ds = (dataset_type or "sales").strip().lower()
    if ds not in {"sales", "claims"}:
        ds = "sales"

    if src:
        try:
            suggestions = suggest_reverse_mapping(df, source=src, dataset_type=ds)
            for item in suggestions.get("mappings", []):
                field_key = _normalize_file_key(str(item.get("field") or ""))
                suggested = item.get("suggested_column")
                if not field_key or not isinstance(suggested, str) or suggested not in df.columns:
                    continue
                if not bool(item.get("found")):
                    continue

                try:
                    confidence = float(item.get("confidence") or 0.0)
                except Exception:
                    confidence = 0.0
                required = bool(item.get("required"))
                min_confidence = 0.55 if required else 0.72
                if confidence < min_confidence:
                    continue

                default_target = _FILE_COLUMN_ALIAS_TARGETS.get(field_key)
                if default_target and default_target in df.columns and default_target != suggested:
                    continue

                alias_map[field_key] = suggested
        except Exception:
            logger.exception("File transform alias generation failed source=%s dataset=%s", src, ds)

    return alias_map


def _resolve_instruction_column(df, label: str, alias_map: dict[str, str], *, allow_create: bool = False) -> str | None:
    cleaned = _clean_instruction_part(label)
    if not cleaned:
        return None

    normalized = _normalize_file_key(cleaned)
    if not normalized:
        return None

    alias_hit = alias_map.get(normalized)
    if alias_hit and alias_hit in df.columns:
        return alias_hit

    normalized_cols: dict[str, str] = {}
    for col in df.columns:
        key = _normalize_file_key(str(col))
        if key and key not in normalized_cols:
            normalized_cols[key] = str(col)
    if normalized in normalized_cols:
        return normalized_cols[normalized]

    for key, col in normalized_cols.items():
        if normalized in key or key in normalized:
            return col

    label_tokens = set(re.findall(r"[a-z0-9]+", cleaned.lower()))
    best_col = None
    best_score = 0
    for col in df.columns:
        col_tokens = set(re.findall(r"[a-z0-9]+", str(col).lower()))
        score = len(label_tokens & col_tokens)
        if score > best_score:
            best_score = score
            best_col = str(col)
    if best_col is not None and best_score >= 2:
        return best_col

    if not allow_create:
        return None

    preferred = alias_map.get(normalized)
    if preferred:
        return preferred

    if "_" in cleaned:
        candidate = cleaned
    else:
        candidate = " ".join(token.capitalize() for token in cleaned.split())
    candidate = candidate.strip() or "New Column"
    return candidate


def _resolve_target_column(df, label: str, alias_map: dict[str, str]) -> str | None:
    cleaned = _clean_instruction_part(label)
    if not cleaned:
        return None

    normalized = _normalize_file_key(cleaned)
    if not normalized:
        return None

    normalized_cols: dict[str, str] = {}
    for col in df.columns:
        key = _normalize_file_key(str(col))
        if key and key not in normalized_cols:
            normalized_cols[key] = str(col)
    if normalized in normalized_cols:
        return normalized_cols[normalized]

    for key, col in normalized_cols.items():
        if normalized in key or key in normalized:
            return col

    label_tokens = set(re.findall(r"[a-z0-9]+", cleaned.lower()))
    best_col = None
    best_score = 0
    for col in df.columns:
        col_tokens = set(re.findall(r"[a-z0-9]+", str(col).lower()))
        score = len(label_tokens & col_tokens)
        if score > best_score:
            best_score = score
            best_col = str(col)
    if best_col is not None and best_score >= 2:
        return best_col

    canonical = _FILE_COLUMN_ALIAS_TARGETS.get(normalized)
    if canonical:
        return canonical

    alias_hit = alias_map.get(normalized)
    if alias_hit and alias_hit in df.columns:
        return alias_hit

    if "_" in cleaned:
        candidate = cleaned
    else:
        candidate = " ".join(token.capitalize() for token in cleaned.split())
    candidate = candidate.strip() or "New Column"
    return candidate


def _apply_copy_instruction(
    df,
    *,
    source_label: str,
    target_label: str,
    alias_map: dict[str, str],
    only_missing: bool,
) -> dict[str, Any] | None:
    source_col = _resolve_instruction_column(df, source_label, alias_map, allow_create=False)
    if source_col is None:
        return None
    target_col = _resolve_target_column(df, target_label, alias_map)
    if target_col is None:
        return None
    if target_col not in df.columns:
        df[target_col] = None

    if only_missing:
        mask = _missing_mask(df[target_col])
        df.loc[mask, target_col] = df.loc[mask, source_col]
        rows = int(mask.sum())
    else:
        df[target_col] = df[source_col]
        rows = int(len(df))

    alias_map[_normalize_file_key(target_label)] = target_col
    alias_map[_normalize_file_key(target_col)] = target_col

    mode = "filled missing" if only_missing else "copied"
    return {
        "description": f"{mode} `{target_col}` from `{source_col}`",
        "rows_affected": rows,
        "columns_touched": [target_col],
    }


def _apply_constant_instruction(
    df,
    *,
    target_label: str,
    value_text: str,
    alias_map: dict[str, str],
    only_missing: bool,
) -> dict[str, Any] | None:
    target_col = _resolve_target_column(df, target_label, alias_map)
    if target_col is None:
        return None
    if target_col not in df.columns:
        df[target_col] = None

    source_col = _resolve_instruction_column(df, value_text, alias_map, allow_create=False)
    treat_as_column = source_col is not None and _normalize_file_key(value_text) != ""

    if treat_as_column:
        return _apply_copy_instruction(
            df,
            source_label=source_col,
            target_label=target_col,
            alias_map=alias_map,
            only_missing=only_missing,
        )

    value = _parse_constant_value(value_text)
    if only_missing:
        mask = _missing_mask(df[target_col])
        df.loc[mask, target_col] = value
        rows = int(mask.sum())
        mode = "filled missing"
    else:
        df[target_col] = value
        rows = int(len(df))
        mode = "set"

    alias_map[_normalize_file_key(target_label)] = target_col
    alias_map[_normalize_file_key(target_col)] = target_col
    return {
        "description": f"{mode} `{target_col}` = {repr(value)}",
        "rows_affected": rows,
        "columns_touched": [target_col],
    }


def _parse_instruction_datetime_series(series):
    import pandas as pd

    raw = series.astype(str).str.strip()
    raw = raw.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA, "null": pd.NA})
    cleaned = raw.astype("string").str.replace(r"\.0$", "", regex=True)
    normalized = (
        cleaned.str.replace("/", "-", regex=False)
        .str.replace(r"\s+", "-", regex=True)
        .str.strip("-")
    )
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    mask_ymd = cleaned.str.fullmatch(r"\d{8}", na=False)
    if mask_ymd.any():
        ymd = pd.to_datetime(cleaned.where(mask_ymd), format="%Y%m%d", errors="coerce")
        parsed = parsed.where(parsed.notna(), ymd)

    mask_ym = cleaned.str.fullmatch(r"\d{6}", na=False)
    if mask_ym.any():
        ym = pd.to_datetime(cleaned.where(mask_ym), format="%Y%m", errors="coerce")
        parsed = parsed.where(parsed.notna(), ym)

    mask_mon_yy = normalized.str.fullmatch(r"[A-Za-z]{3,9}-\d{2}", na=False)
    if mask_mon_yy.any():
        mon_yy = pd.to_datetime(normalized.where(mask_mon_yy), format="%b-%y", errors="coerce")
        parsed = parsed.where(parsed.notna(), mon_yy)

    mask_mon_yyyy = normalized.str.fullmatch(r"[A-Za-z]{3,9}-\d{4}", na=False)
    if mask_mon_yyyy.any():
        mon_yyyy = pd.to_datetime(normalized.where(mask_mon_yyyy), format="%b-%Y", errors="coerce")
        parsed = parsed.where(parsed.notna(), mon_yyyy)

    try:
        generic = pd.to_datetime(cleaned, format="mixed", errors="coerce")
        generic_dayfirst = pd.to_datetime(cleaned, format="mixed", errors="coerce", dayfirst=True)
    except TypeError:
        generic = pd.to_datetime(cleaned, errors="coerce")
        generic_dayfirst = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)

    if int(generic_dayfirst.notna().sum()) > int(generic.notna().sum()):
        generic = generic_dayfirst
    parsed = parsed.where(parsed.notna(), generic)
    return parsed


def _clean_date_filter_value(value: str) -> str:
    cleaned = _clean_instruction_part(value)
    cleaned = re.sub(
        r"(?i)\b(?:rows?|data|dataset|period|time|month|date|only|selected|current|the)\b",
        " ",
        cleaned,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,:;")
    return cleaned


def _parse_instruction_date_value(value: str, *, boundary: str):
    import pandas as pd

    raw_cleaned = _clean_instruction_part(value)
    raw_lowered = raw_cleaned.lower()
    if raw_lowered in {"this month", "current month"}:
        today = pd.Timestamp(datetime.utcnow()).normalize()
        parsed = today.replace(day=1)
        return parsed + pd.offsets.MonthEnd(0) if boundary == "end" else parsed

    cleaned = _clean_date_filter_value(value)
    if not cleaned:
        return None

    month_only = bool(
        re.fullmatch(r"[A-Za-z]{3,9}[-\s]\d{2,4}", cleaned)
        or re.fullmatch(r"\d{4}[-/]\d{2}", cleaned)
        or re.fullmatch(r"\d{6}", cleaned)
    )

    format_attempts = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%m-%y",
        "%d/%m/%y",
        "%b-%y",
        "%b-%Y",
        "%b %y",
        "%b %Y",
        "%Y-%m",
        "%Y/%m",
        "%Y%m",
    ]
    parsed = None
    for fmt in format_attempts:
        try:
            parsed = pd.Timestamp(datetime.strptime(cleaned, fmt))
            break
        except ValueError:
            continue

    if parsed is None:
        parsed_direct = pd.to_datetime(cleaned, errors="coerce")
        parsed_dayfirst = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)
        if pd.isna(parsed_direct) or (
            not pd.isna(parsed_dayfirst) and pd.isna(parsed_direct)
        ):
            parsed = parsed_dayfirst if not pd.isna(parsed_dayfirst) else None
        else:
            parsed = parsed_direct

    if parsed is None or pd.isna(parsed):
        return None

    normalized = pd.Timestamp(parsed).normalize()
    if month_only:
        if boundary == "end":
            return normalized + pd.offsets.MonthEnd(0)
        return normalized.replace(day=1)
    return normalized


def _resolve_instruction_period_column(df, dataset_type: str):
    best_col = None
    best_series = None
    best_count = 0
    dataset_key = "claims" if str(dataset_type or "").strip().lower() == "claims" else "sales"

    candidate_columns = list(_DATE_FILTER_COLUMN_CANDIDATES.get(dataset_key, ()))
    candidate_columns.extend(str(col) for col in df.columns if str(col) not in candidate_columns)

    for col in candidate_columns:
        if col not in df.columns:
            continue
        parsed = _parse_instruction_datetime_series(df[col])
        parsed_count = int(parsed.notna().sum())
        if parsed_count > best_count:
            best_col = str(col)
            best_series = parsed
            best_count = parsed_count

    if best_col is None or best_series is None or best_count <= 0:
        return None, None
    return best_col, best_series


def _apply_date_filter_instruction(
    df,
    line: str,
    dataset_type: str,
) -> dict[str, Any] | None:
    text = str(line or "").strip()
    if not text:
        return None

    lower_text = text.lower()
    has_date_hint = bool(_DATE_TOKEN_HINT_PATTERN.search(text)) or any(
        token in lower_text for token in ["month", "period", "date", "time"]
    )
    if not has_date_hint or not _DATE_FILTER_ACTION_PATTERN.search(text):
        return None

    start_value = None
    end_value = None
    label = None

    range_match = _DATE_FILTER_RANGE_PATTERN.search(text)
    if range_match:
        start_token = range_match.group("start")
        end_token = range_match.group("end")
        start_value = _parse_instruction_date_value(start_token, boundary="start")
        end_value = _parse_instruction_date_value(end_token, boundary="end")
        label = f"{_clean_date_filter_value(start_token)} to {_clean_date_filter_value(end_token)}"
    else:
        after_match = _DATE_FILTER_AFTER_PATTERN.search(text)
        before_match = _DATE_FILTER_BEFORE_PATTERN.search(text)
        single_match = _DATE_FILTER_SINGLE_PATTERN.search(text)
        if after_match:
            start_token = after_match.group("start")
            start_value = _parse_instruction_date_value(start_token, boundary="start")
            label = f"after {_clean_date_filter_value(start_token)}"
        elif before_match:
            end_token = before_match.group("end")
            end_value = _parse_instruction_date_value(end_token, boundary="end")
            label = f"before {_clean_date_filter_value(end_token)}"
        elif single_match:
            single_token = single_match.group("value")
            start_value = _parse_instruction_date_value(single_token, boundary="start")
            end_value = _parse_instruction_date_value(single_token, boundary="end")
            label = _clean_date_filter_value(single_token)

    if start_value is None and end_value is None:
        return None

    date_col, parsed_series = _resolve_instruction_period_column(df, dataset_type)
    if date_col is None or parsed_series is None:
        return {
            "description": "Date filter skipped: no usable date or period column was found in the uploaded file.",
            "rows_affected": 0,
            "columns_touched": [],
        }

    mask = parsed_series.notna()
    if start_value is not None:
        mask = mask & (parsed_series >= start_value)
    if end_value is not None:
        mask = mask & (parsed_series <= end_value)

    before = int(len(df))
    df.drop(index=df.index[~mask], inplace=True)
    df.reset_index(drop=True, inplace=True)
    removed = max(0, before - int(len(df)))
    range_label = label or "selected period"
    return {
        "description": f"kept {int(len(df))} row(s) from `{date_col}` for {range_label}",
        "rows_affected": removed,
        "columns_touched": [date_col],
    }


def _coerce_instruction_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _extract_json_object_from_text(text: str) -> dict[str, Any] | None:
    raw_text = str(text or "").strip()
    if not raw_text:
        return None

    candidates: list[str] = []
    fenced_blocks = re.findall(r"```(?:json)?\s*(.*?)```", raw_text, flags=re.IGNORECASE | re.DOTALL)
    candidates.extend(block.strip() for block in fenced_blocks if block.strip())
    candidates.append(raw_text)

    for candidate in candidates:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start < 0 or end < start:
            continue
        snippet = candidate[start : end + 1]
        try:
            parsed = json.loads(snippet)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _should_try_llm_transform_plan(request_text: str, instructions: list[str]) -> bool:
    if not os.getenv("SARVAM_API_KEY", "").strip():
        return False

    text = str(request_text or "").strip().lower()
    if not text:
        return False

    if len(instructions) != 1:
        return False

    compound_markers = [
        " and ",
        " also ",
        " then ",
        " plus ",
        " as well as ",
        " along with ",
        " now ",
        " instead ",
        " rather than ",
        " but ",
    ]
    if any(marker in text for marker in compound_markers):
        return True

    if not (
        _DATE_FILTER_ACTION_PATTERN.search(text)
        or _SET_INSTRUCTION_PATTERN.match(text)
        or _IS_INSTRUCTION_PATTERN.match(text)
        or any(pattern.match(text) for pattern in _COPY_INSTRUCTION_PATTERNS)
        or _DUPLICATE_INTENT_PATTERN.search(text)
        or _ROW_COUNT_INTENT_PATTERN.search(text)
    ):
        return True

    return False


def _plan_transform_instructions_with_llm(
    df,
    *,
    request_text: str,
    source: str | None,
    dataset_type: str,
) -> list[dict[str, Any]] | None:
    if not os.getenv("SARVAM_API_KEY", "").strip():
        return None

    columns = [str(col) for col in list(df.columns)[:80]]
    columns_json = json.dumps(columns, ensure_ascii=True)
    if len(df.columns) > len(columns):
        columns_json = f"{columns_json} (truncated from {len(df.columns)} columns)"

    system_prompt = (
        "You convert spreadsheet file-edit requests into a strict JSON execution plan. "
        "Return only one JSON object and no markdown. "
        "Preserve negations exactly. "
        "Supported operation types are: noop, date_filter, set_value, copy_column, "
        "remove_duplicates, analyze_duplicates. "
        "Use multiple operations when the user asks for multiple actions. "
        "Prefer exact column names from the provided list. "
        "If the user asks to keep data unchanged or only asks a question like row count, use noop. "
        "If the user wants to inspect duplicates without deleting them, use analyze_duplicates. "
        "Schema: "
        "{\"operations\":[{\"type\":\"noop|date_filter|set_value|copy_column|remove_duplicates|analyze_duplicates\","
        "\"target_column\":null,\"source_column\":null,\"column\":null,\"value\":null,"
        "\"only_missing\":false,\"start_date\":null,\"end_date\":null,"
        "\"report_row_count\":false,\"notes\":null}]}"
    )
    prompt = (
        f"User instruction:\n{request_text.strip()}\n\n"
        f"Source: {(source or 'unknown').strip() or 'unknown'}\n"
        f"Dataset type: {(dataset_type or 'sales').strip().lower() or 'sales'}\n"
        f"Available columns: {columns_json}\n\n"
        "Interpret flexible phrasing like 'should actually come from', 'for blanks use', "
        "'only keep January rows', 'do not remove duplicates', or 'tell me how many rows are there'. "
        "Output only valid JSON."
    )

    try:
        model_name, response_text, _ = _call_llm(
            system_prompt,
            prompt,
            model=_resolve_llm_model("CHATBOT_MODEL", "SARVAM_MODEL"),
            temperature=0.0,
            num_predict=420,
            timeout_seconds=25,
        )
    except Exception:
        logger.exception("LLM file instruction planning failed")
        return None

    parsed = _extract_json_object_from_text(response_text)
    if not isinstance(parsed, dict):
        logger.warning("LLM file instruction planner returned non-JSON output model=%s", model_name)
        return None

    operations = parsed.get("operations")
    if not isinstance(operations, list):
        return None

    cleaned_ops = [item for item in operations if isinstance(item, dict)]
    return cleaned_ops or None


def _apply_planned_transform_operation(
    df,
    operation: dict[str, Any],
    *,
    alias_map: dict[str, str],
    dataset_type: str,
) -> dict[str, Any] | None:
    op_type = _normalize_file_key(operation.get("type") or operation.get("op") or "")
    if not op_type:
        return None

    if op_type == "datefilter":
        start_token = str(operation.get("start_date") or operation.get("from_date") or "").strip()
        end_token = str(operation.get("end_date") or operation.get("to_date") or "").strip()
        if start_token and end_token:
            text = f"keep rows from {start_token} to {end_token}"
        elif start_token:
            text = f"keep rows after {start_token}"
        elif end_token:
            text = f"keep rows before {end_token}"
        else:
            return None
        return _apply_date_filter_instruction(df, text, dataset_type)

    if op_type == "copycolumn":
        source_label = _clean_instruction_part(operation.get("source_column") or operation.get("source") or "")
        target_label = _clean_instruction_part(operation.get("target_column") or operation.get("target") or "")
        if not source_label or not target_label:
            return None
        return _apply_copy_instruction(
            df,
            source_label=source_label,
            target_label=target_label,
            alias_map=alias_map,
            only_missing=_coerce_instruction_bool(operation.get("only_missing")),
        )

    if op_type == "setvalue":
        target_label = _clean_instruction_part(operation.get("target_column") or operation.get("target") or "")
        if not target_label:
            return None

        target_col = _resolve_target_column(df, target_label, alias_map)
        if target_col is None:
            return None
        if target_col not in df.columns:
            df[target_col] = None

        raw_value = operation["value"] if "value" in operation else None
        if isinstance(raw_value, str):
            value = _parse_constant_value(raw_value)
        else:
            value = raw_value

        only_missing = _coerce_instruction_bool(operation.get("only_missing"))
        if only_missing:
            mask = _missing_mask(df[target_col])
            df.loc[mask, target_col] = value
            rows = int(mask.sum())
            mode = "filled missing"
        else:
            df[target_col] = value
            rows = int(len(df))
            mode = "set"

        alias_map[_normalize_file_key(target_label)] = target_col
        alias_map[_normalize_file_key(target_col)] = target_col
        return {
            "description": f"{mode} `{target_col}` = {repr(value)}",
            "rows_affected": rows,
            "columns_touched": [target_col],
        }

    if op_type == "removeduplicates":
        column_label = _clean_instruction_part(
            operation.get("column") or operation.get("target_column") or operation.get("target") or ""
        )
        text = f"remove duplicates from column {column_label}" if column_label else "remove duplicates"
        return _apply_deduplicate_instruction(df, text, alias_map)

    if op_type == "analyzeduplicates":
        column_label = _clean_instruction_part(
            operation.get("column") or operation.get("target_column") or operation.get("target") or ""
        )
        text = f"check duplicates in column {column_label}" if column_label else "check duplicates"
        analysis = _analyze_duplicate_instruction(df, text, alias_map)
        if analysis is None:
            return None
        return {
            "description": str(analysis.get("summary") or "Duplicate scan completed."),
            "rows_affected": 0,
            "columns_touched": [str(col) for col in (analysis.get("columns_touched") or []) if str(col).strip()],
        }

    if op_type == "noop":
        notes = str(operation.get("notes") or "").strip()
        report_row_count = _coerce_instruction_bool(operation.get("report_row_count"))
        details: list[str] = []
        if notes:
            details.append(notes)
        if report_row_count:
            details.append(f"current row count {int(len(df))}")

        description = "kept uploaded rows unchanged"
        if details:
            description += " (" + "; ".join(details) + ")"
        return {
            "description": description,
            "rows_affected": 0,
            "columns_touched": [],
        }

    return None


def _execute_planned_transform_operations(
    df,
    operations: list[dict[str, Any]],
    *,
    alias_map: dict[str, str],
    dataset_type: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    applied_ops: list[dict[str, Any]] = []
    skipped_ops: list[str] = []

    for operation in operations:
        result = _apply_planned_transform_operation(
            df,
            operation,
            alias_map=alias_map,
            dataset_type=dataset_type,
        )
        if result is None:
            skipped_ops.append(json.dumps(operation, ensure_ascii=True, default=str))
            continue
        applied_ops.append(result)

    return applied_ops, skipped_ops


def _apply_instruction_line(df, line: str, alias_map: dict[str, str], dataset_type: str) -> dict[str, Any] | None:
    text = line.strip()
    if not text:
        return None

    date_filter_result = _apply_date_filter_instruction(df, text, dataset_type)
    if date_filter_result is not None:
        return date_filter_result

    dedupe_result = _apply_deduplicate_instruction(df, text, alias_map)
    if dedupe_result is not None:
        return dedupe_result

    for pattern in _COPY_INSTRUCTION_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        groups = match.groupdict()
        source = _clean_instruction_part(groups.get("source", ""))
        target = _clean_instruction_part(groups.get("target", ""))
        if not source or not target:
            return None
        return _apply_copy_instruction(
            df,
            source_label=source,
            target_label=target,
            alias_map=alias_map,
            only_missing=False,
        )

    set_match = _SET_INSTRUCTION_PATTERN.match(text)
    if set_match:
        groups = set_match.groupdict()
        target = _clean_instruction_part(groups.get("target", ""))
        value = groups.get("value", "")
        scope = (groups.get("scope") or "").strip().lower()
        only_missing = scope in {"missing", "blank", "empty", "null"}
        if not target or not value.strip():
            return None
        return _apply_constant_instruction(
            df,
            target_label=target,
            value_text=value,
            alias_map=alias_map,
            only_missing=only_missing,
        )

    fallback_match = _IS_INSTRUCTION_PATTERN.match(text)
    if fallback_match:
        groups = fallback_match.groupdict()
        target = _clean_instruction_part(groups.get("target", ""))
        value = groups.get("value", "")
        if not target or not value.strip():
            return None

        normalized_target = _normalize_file_key(target)
        if normalized_target and (
            normalized_target in alias_map
            or any(word in target.lower() for word in ["column", "field", "price", "premium", "brand", "amount", "date", "category", "plan"])
        ):
            return _apply_constant_instruction(
                df,
                target_label=target,
                value_text=value,
                alias_map=alias_map,
                only_missing=False,
            )

        source_col = _resolve_instruction_column(df, value, alias_map, allow_create=False)
        if source_col is not None:
            return _apply_copy_instruction(
                df,
                source_label=source_col,
                target_label=target,
                alias_map=alias_map,
                only_missing=False,
            )

    return _build_noop_instruction_result(df, text)


def _resolve_output_extension(input_ext: str, requested: str | None) -> str:
    preferred = (requested or "").strip().lower()
    if preferred in {"csv", "xlsx"}:
        return preferred
    if input_ext in {"csv", "xlsx"}:
        return input_ext
    return "csv"


@app.post("/chatbot/file-transform")
async def chatbot_file_transform(
    file: UploadFile = File(...),
    instruction: str = Form(...),
    source: str | None = Form(None),
    dataset_type: str | None = Form(None),
    output_format: str | None = Form(None),
    current_user = Depends(get_current_user),
):
    request_text = (instruction or "").strip()
    if not request_text:
        raise HTTPException(status_code=400, detail="Instruction is required.")

    df, input_ext = _parse_file_transform_dataframe(file)
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows.")

    resolved_source = _normalize_source_key(source or "")
    if not resolved_source:
        resolved_source = _normalize_source_key(_detect_source_from_text(request_text) or "")

    resolved_dataset = (dataset_type or "").strip().lower()
    if resolved_dataset not in {"sales", "claims"}:
        inferred_dataset = _detect_dataset_from_text(request_text)
        resolved_dataset = inferred_dataset if inferred_dataset in {"sales", "claims"} else "sales"

    alias_map = _build_file_alias_map(
        df,
        source=resolved_source or None,
        dataset_type=resolved_dataset,
    )
    instructions = _split_file_instructions(request_text)
    if not instructions:
        raise HTTPException(status_code=400, detail="No valid instruction found.")

    applied_ops: list[dict[str, Any]] = []
    skipped_ops: list[str] = []
    used_llm_request_plan = False

    if _should_try_llm_transform_plan(request_text, instructions):
        planned_ops = _plan_transform_instructions_with_llm(
            df,
            request_text=request_text,
            source=resolved_source or None,
            dataset_type=resolved_dataset,
        )
        if planned_ops:
            planned_applied, planned_skipped = _execute_planned_transform_operations(
                df,
                planned_ops,
                alias_map=alias_map,
                dataset_type=resolved_dataset,
            )
            if planned_applied:
                applied_ops.extend(planned_applied)
                skipped_ops.extend(planned_skipped)
                used_llm_request_plan = True

    if not used_llm_request_plan:
        for line in instructions:
            result = _apply_instruction_line(df, line, alias_map, resolved_dataset)
            if result is not None:
                applied_ops.append(result)
                continue

            planned_ops = _plan_transform_instructions_with_llm(
                df,
                request_text=line,
                source=resolved_source or None,
                dataset_type=resolved_dataset,
            )
            if planned_ops:
                planned_applied, planned_skipped = _execute_planned_transform_operations(
                    df,
                    planned_ops,
                    alias_map=alias_map,
                    dataset_type=resolved_dataset,
                )
                if planned_applied:
                    applied_ops.extend(planned_applied)
                    skipped_ops.extend(planned_skipped)
                    continue

            skipped_ops.append(line)

    analysis_result: dict[str, Any] | None = None
    if not applied_ops:
        analysis_result = _analyze_duplicate_instruction(df, request_text, alias_map)
        if analysis_result is None:
            llm_enabled = bool(os.getenv("SARVAM_API_KEY", "").strip())
            if llm_enabled:
                detail = (
                    "Could not interpret the instruction after rule-based and AI parsing. "
                    "Try describing the row filter, fill, copy, duplicate handling, or row-count request in plain English."
                )
            else:
                detail = (
                    "Could not interpret the instruction. AI parsing is unavailable because SARVAM_API_KEY is not configured."
                )
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        skipped_ops = []

    output_ext = _resolve_output_extension(input_ext, output_format)
    output_filename = _safe_transform_filename(file.filename or "chatbot_file", output_ext)

    if output_ext == "xlsx":
        out_buf = BytesIO()
        df.to_excel(out_buf, index=False)
        payload = out_buf.getvalue()
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        payload = df.to_csv(index=False).encode("utf-8")
        media_type = "text/csv"

    if analysis_result is not None:
        total_rows_affected = int(analysis_result.get("rows_affected") or 0)
        columns_touched = sorted(
            {str(col) for col in (analysis_result.get("columns_touched") or []) if str(col).strip()}
        )
        summary = str(analysis_result.get("summary") or "Duplicate scan completed.")
        operations_count = 0
        skipped_count = 0
    else:
        total_rows_affected = int(sum(int(op.get("rows_affected") or 0) for op in applied_ops))
        columns_touched = sorted(
            {str(col) for op in applied_ops for col in (op.get("columns_touched") or []) if str(col).strip()}
        )
        short_descriptions = [str(op.get("description") or "").strip() for op in applied_ops[:3] if str(op.get("description") or "").strip()]
        summary_parts = [
            f"Applied {len(applied_ops)} instruction(s)",
            f"row updates: {total_rows_affected}",
            f"columns touched: {len(columns_touched)}",
        ]
        if short_descriptions:
            summary_parts.append("changes: " + "; ".join(short_descriptions))
        if skipped_ops:
            summary_parts.append(f"skipped {len(skipped_ops)} line(s)")
        summary = " | ".join(summary_parts)
        operations_count = len(applied_ops)
        skipped_count = len(skipped_ops)

    return StreamingResponse(
        iter([payload]),
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{output_filename}"',
            "X-Transform-Summary": _sanitize_header_value(summary),
            "X-Transform-Operations": str(operations_count),
            "X-Transform-Rows-Affected": str(total_rows_affected),
            "X-Transform-Columns-Touched": str(len(columns_touched)),
            "X-Transform-Skipped": str(skipped_count),
        },
    )

# ==================================================
# GRAPH INSIGHTS (LLM)
# ==================================================
class GraphInsightPayload(BaseModel):
    source: str = Field(..., min_length=1)
    dataset_type: str = Field(..., min_length=1)
    dimension: str = Field(..., min_length=1)
    metric: str = Field(..., min_length=1)
    bucket: str | None = None
    job_id: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    compare_mode: bool = False
    rows: list[dict[str, Any]] = Field(default_factory=list)


class ChatbotTurn(BaseModel):
    role: str = Field(..., min_length=1)
    content: str = Field(..., min_length=1, max_length=4000)


class ChatbotPayload(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[ChatbotTurn] = Field(default_factory=list)
    system_prompt: str | None = None
    temperature: float | None = Field(default=None, ge=0.0, le=1.5)
    max_tokens: int | None = Field(default=None, ge=8, le=4096)
    source: str | None = Field(default=None, max_length=64)
    dataset_type: str | None = Field(default=None, max_length=16)
    job_id: str | None = Field(default=None, max_length=128)
    from_date: str | None = Field(default=None, max_length=32)
    to_date: str | None = Field(default=None, max_length=32)
    global_scope: bool = False
    ui_context: dict[str, Any] | None = None


DEFAULT_LLM_MODEL = (
    os.getenv("SARVAM_MODEL", "").strip()
    or os.getenv("CHATBOT_MODEL", "").strip()
    or "sarvam-m"
)
DEFAULT_CHATBOT_SYSTEM_PROMPT = (
    "You are AI Sahyogi, Senior Analytics Advisor for Zopper leadership reviews. "
    "Answer from analytics context built from dashboard metrics and underlying dataset signals. "
    "Do not invent brands, products, numbers, dates, or events. "
    "If key data is insufficient, explicitly state what is missing and provide the closest defensible estimate with assumptions. "
    "For greetings, acknowledgements, or short conversational messages, respond naturally and invite a data question. "
    "Treat source aliases as: reliance/resq -> Reliance ResQ, goodrej/goddrej -> Godrej, hitachi -> Hitachi, "
    "samsung/overview/overall/ -> Samsung Overview, samsung vs/vijay sales -> Samsung Vijay Sales, samsung croma/croma/protect max/protect max croma -> Samsung Croma, reliance digital/reliance_digital -> Samsung Reliance Digital. "
    "Apply source-specific taxonomy and mappings; use Samsung-specific model mapping or Samsung plan abbreviations only when the selected source is Samsung. "
    "Write in a clear executive tone with concise, evidence-backed reasoning. "
    "Lead with the direct answer, then support it with key metrics, trend direction, and business impact. "
    "Answer the exact metric the user asked for; do not switch to premium, revenue, or another metric unless the user asks for that explicitly. "
    "If the user asks for quantity, count, plans sold, or volume and a dedicated quantity field is unavailable, use row count as the operational proxy when the dataset grain supports that and state the assumption briefly. "
    "Vary phrasing and structure across turns; avoid repeating identical templates or sentence openings. "
    "For forecasting questions, derive directional month or financial-year estimates only from historical monthly values in context. "
    "Never expose chain-of-thought, internal reasoning, or <think> tags. "
    "Do not re-introduce yourself unless the user explicitly asks who you are."
)
try:
    CHATBOT_HISTORY_LIMIT = max(1, int(os.getenv("CHATBOT_HISTORY_LIMIT", "10")))
except ValueError:
    CHATBOT_HISTORY_LIMIT = 10

try:
    CHATBOT_HISTORY_CHAR_LIMIT = max(120, int(os.getenv("CHATBOT_HISTORY_CHAR_LIMIT", "650")))
except ValueError:
    CHATBOT_HISTORY_CHAR_LIMIT = 650

try:
    CHATBOT_MESSAGE_CHAR_LIMIT = max(200, int(os.getenv("CHATBOT_MESSAGE_CHAR_LIMIT", "2600")))
except ValueError:
    CHATBOT_MESSAGE_CHAR_LIMIT = 2600

try:
    CHATBOT_CACHE_TTL_SECONDS = max(1, int(os.getenv("CHATBOT_CACHE_TTL_SECONDS", "180")))
except ValueError:
    CHATBOT_CACHE_TTL_SECONDS = 180

try:
    CHATBOT_CACHE_MAX_ITEMS = max(8, int(os.getenv("CHATBOT_CACHE_MAX_ITEMS", "256")))
except ValueError:
    CHATBOT_CACHE_MAX_ITEMS = 256

try:
    CHATBOT_SCOPE_CACHE_TTL_SECONDS = max(5, int(os.getenv("CHATBOT_SCOPE_CACHE_TTL_SECONDS", "60")))
except ValueError:
    CHATBOT_SCOPE_CACHE_TTL_SECONDS = 60

try:
    CHATBOT_LLM_FAILURE_TTL_SECONDS = max(15, int(os.getenv("CHATBOT_LLM_FAILURE_TTL_SECONDS", "180")))
except ValueError:
    CHATBOT_LLM_FAILURE_TTL_SECONDS = 180

GRAPH_INSIGHTS_TTL_SECONDS = int(os.getenv("GRAPH_INSIGHTS_TTL_SECONDS", "300"))
_graph_insights_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_chatbot_response_cache: dict[str, tuple[float, dict[str, str]]] = {}
_chatbot_scope_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_chatbot_cache_lock = threading.Lock()
_chatbot_scope_cache_lock = threading.Lock()
_chatbot_llm_state_lock = threading.Lock()
_chatbot_llm_unavailable_until = 0.0
_chatbot_llm_last_error = ""


def _chatbot_scope_cache_key(*, job_id: str | None) -> str:
    return _normalize_chatbot_job_id(job_id) or "__all__"


def _chatbot_scope_cache_get(*, job_id: str | None) -> list[dict[str, Any]] | None:
    cache_key = _chatbot_scope_cache_key(job_id=job_id)
    now = time.time()
    with _chatbot_scope_cache_lock:
        cached = _chatbot_scope_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _chatbot_scope_cache.pop(cache_key, None)
            return None
        return payload


def _chatbot_scope_cache_set(*, job_id: str | None, scopes: list[dict[str, Any]]) -> None:
    cache_key = _chatbot_scope_cache_key(job_id=job_id)
    with _chatbot_scope_cache_lock:
        _chatbot_scope_cache[cache_key] = (time.time() + CHATBOT_SCOPE_CACHE_TTL_SECONDS, scopes)


def _chatbot_mark_llm_unavailable(*, detail: str, ttl_seconds: int | None = None) -> None:
    cooldown = max(15, int(ttl_seconds or CHATBOT_LLM_FAILURE_TTL_SECONDS))
    with _chatbot_llm_state_lock:
        global _chatbot_llm_unavailable_until, _chatbot_llm_last_error
        _chatbot_llm_unavailable_until = time.time() + cooldown
        _chatbot_llm_last_error = detail.strip()


def _chatbot_mark_llm_available() -> None:
    with _chatbot_llm_state_lock:
        global _chatbot_llm_unavailable_until, _chatbot_llm_last_error
        _chatbot_llm_unavailable_until = 0.0
        _chatbot_llm_last_error = ""


def _chatbot_llm_unavailable_state() -> tuple[bool, str]:
    with _chatbot_llm_state_lock:
        if _chatbot_llm_unavailable_until > time.time():
            return True, _chatbot_llm_last_error
        return False, ""


def _graph_insights_cache_key(payload: GraphInsightPayload) -> str:
    signature = {
        "source": payload.source,
        "dataset_type": payload.dataset_type,
        "dimension": payload.dimension,
        "metric": payload.metric,
        "bucket": payload.bucket,
        "job_id": payload.job_id,
        "from_date": payload.from_date,
        "to_date": payload.to_date,
        "compare_mode": payload.compare_mode,
        "rows": payload.rows[:80],
    }
    raw = json.dumps(signature, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_chatcards_system_prompt() -> str:
    fallback = (
        "You are AI Sahyogi, a business analytics copilot for Zopper leadership reviews. "
        "Generate crisp, decision-ready insights from chart data. Use precise business "
        "language, quantify impact, and avoid filler. Return exactly 3 to 5 bullet points."
    )
    env_path = os.getenv("CHATCARDS_SYSTEM_PROMPT_PATH", "").strip()
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    base_dir = Path(__file__).resolve().parent
    candidates.extend(
        [
            base_dir / "chatcards" / "system_prompt.txt",            # backend-local (works in Docker image)
            base_dir.parent / "chatcards" / "system_prompt.txt",     # repo-root sibling (works in local dev)
        ]
    )

    for prompt_path in candidates:
        if not prompt_path.exists():
            continue
        try:
            content = prompt_path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if content:
            return content
    return fallback


def _extract_bullets(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    bullets: list[str] = []

    for line in lines:
        if re.match(r"^[-*\u2022]\s+", line):
            bullets.append(re.sub(r"^[-*\u2022]\s+", "", line).strip())
            continue
        if re.match(r"^\d+[.)]\s+", line):
            bullets.append(re.sub(r"^\d+[.)]\s+", "", line).strip())
            continue

    if bullets:
        return bullets[:5]

    compact = text.strip()
    if not compact:
        return []

    sentences = re.split(r"(?<=[.!?])\s+", compact)
    return [s.strip() for s in sentences if s.strip()][:5]


def _to_safe_key(key: str) -> str:
    return re.sub(r"[()%'.]", "", re.sub(r"\s+", "_", key.strip().lower()))


def _normalize_source_key(source: str) -> str:
    source_key = (source or "").strip().lower()
    samsung_source = normalize_samsung_source(source_key)
    if samsung_source:
        return samsung_source
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return source_key


_CHATBOT_SOURCE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    (
        "samsung_reliance_digital",
        (
            "samsung reliance digital",
            "samsung_reliance_digital",
            "reliance digital",
            "reliance-digital",
            "reliance_digital",
        ),
    ),
    ("reliance", ("reliance resq", "reliance-resq", "reliance_resq", "resq", "reliance")),
    ("godrej", ("godrej", "goodrej", "goddrej")),
    ("hitachi", ("hitachi",)),
    ("samsung_vs", ("samsung vijay sales", "samsung_vs", "samsung vs", "vijay sales", "vijay")),
    ("samsung_croma", ("samsung croma", "samsung_croma", "croma sales", "croma", "samsung protect max", "samsung protect max croma", "protect max", "protect max croma", "croma protect max")),
    ("samsung", ("samsung",)),
]

_CHATBOT_SOURCE_LABELS: dict[str, str] = {
    "reliance": "Reliance ResQ",
    "godrej": "Godrej",
    "hitachi": "Hitachi",
    "samsung": "Samsung",
    "samsung_vs": "Samsung Vijay Sales",
    "samsung_croma": "Samsung Croma",
    "samsung_reliance_digital": "Samsung Reliance Digital",
}


def _source_display_name(source: str) -> str:
    source_key = _normalize_source_key(source)
    if source_key in _CHATBOT_SOURCE_LABELS:
        return _CHATBOT_SOURCE_LABELS[source_key]
    if source_key:
        return source_key.replace("_", " ").title()
    return "Dashboard"


def _normalize_dataset_type_for_chatbot(value: str | None) -> str:
    token = (value or "").strip().lower()
    return "claims" if token == "claims" else "sales"


def _normalize_chatbot_job_id(value: str | None) -> str | None:
    token = (value or "").strip()
    if not token:
        return None
    if token.lower() in {"all", "null", "undefined"}:
        return None
    return token


def _normalize_chatbot_date(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    candidate = raw[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _normalize_chatbot_date_range(
    from_date: str | None,
    to_date: str | None,
) -> tuple[str | None, str | None]:
    safe_from = _normalize_chatbot_date(from_date)
    safe_to = _normalize_chatbot_date(to_date)
    if safe_from and safe_to and safe_from > safe_to:
        return safe_to, safe_from
    return safe_from, safe_to


def _summarize_ui_context(ui_context: dict[str, Any] | None) -> str:
    if not isinstance(ui_context, dict) or not ui_context:
        return ""
    try:
        compact = json.dumps(ui_context, ensure_ascii=True, default=str, separators=(",", ":"))
    except Exception:
        compact = str(ui_context)
    compact = re.sub(r"\s+", " ", compact).strip()
    if not compact:
        return ""
    max_len = 1200
    if len(compact) > max_len:
        compact = f"{compact[:max_len].rstrip()}..."
    return f"UI context snapshot: {compact}"


def _detect_source_from_text(text: str) -> str | None:
    low = (text or "").strip().lower()
    if not low:
        return None

    for source_key, aliases in _CHATBOT_SOURCE_PATTERNS:
        for alias in aliases:
            pattern = r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b"
            if re.search(pattern, low):
                return source_key
    return None


def _detect_dataset_from_text(text: str) -> str | None:
    low = (text or "").strip().lower()
    if not low:
        return None
    if any(token in low for token in ("claim", "loss ratio", "settlement", "paid out")):
        return "claims"
    if any(token in low for token in ("sale", "premium", "units sold", "earning", "pricing", "price", "mrp")):
        return "sales"
    pricing_tokens = ("plan price", "plan pricing", "price by", "pricing by", "uplift", "rate card")
    if any(token in low for token in pricing_tokens):
        return "sales"
    return None


def _resolve_chatbot_source_with_origin(payload: ChatbotPayload) -> tuple[str, str]:
    from_message = _detect_source_from_text(payload.message)
    if from_message:
        return from_message, "message"

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        inferred = _detect_source_from_text(turn.content)
        if inferred:
            return inferred, "history"

    explicit = _normalize_source_key(payload.source or "")
    if explicit:
        return explicit, "payload"
    return "", "none"


def _resolve_chatbot_source(payload: ChatbotPayload) -> str:
    source, _ = _resolve_chatbot_source_with_origin(payload)
    return source


def _resolve_chatbot_dataset_type(payload: ChatbotPayload) -> str:
    inferred = _detect_dataset_from_text(payload.message)
    if inferred:
        return inferred

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        inferred = _detect_dataset_from_text(turn.content)
        if inferred:
            return inferred

    explicit = (payload.dataset_type or "").strip().lower()
    if explicit in {"sales", "claims"}:
        return explicit
    return "sales"


def _chatbot_message_tokens(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (text or "").strip().lower())
    return [token for token in cleaned.split() if token]


def _chatbot_requested_dimensions_from_text(text: str) -> list[str]:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return []

    hints: list[tuple[str, tuple[str, ...]]] = [
        (
            "state",
            (
                "statewise",
                "state wise",
                "by state",
                "state level",
                "state breakup",
                "state stats",
            ),
        ),
        (
            "month",
            (
                "monthwise",
                "month wise",
                "by month",
                "monthly",
                "which month",
                "month level",
                "month stats",
            ),
        ),
        ("city", ("citywise", "city wise", "by city", "city level", "city stats")),
        ("channel", ("channel wise", "channelwise", "by channel", "channel level", "channel stats")),
        (
            "device_plan_category",
            (
                "device plan category",
                "device category",
                "device wise",
                "device-wise",
            ),
        ),
        (
            "plan_category",
            (
                "plan category",
                "plan wise",
                "plan-wise",
            ),
        ),
        (
            "product_category",
            (
                "product category",
                "product wise",
                "product-wise",
            ),
        ),
        (
            "product_subcategory",
            (
                "product subcategory",
                "sub category",
                "subcategory",
                "model wise",
                "model-wise",
                "by model",
            ),
        ),
        (
            "reason",
            (
                "reason",
                "reasons",
                "cause",
                "causes",
                "root cause",
                "nature of complaint",
                "complaint wise",
                "issue wise",
            ),
        ),
        (
            "operation",
            (
                "major call operation",
                "operation wise",
                "repair action",
            ),
        ),
        (
            "call_type",
            (
                "call type",
                "claim type",
                "service type",
            ),
        ),
        (
            "status",
            (
                "call status",
                "claim status",
                "approval status",
                "status wise",
            ),
        ),
        ("zone", ("zone wise", "by zone", "zone level")),
        ("branch", ("branch wise", "by branch", "branch level")),
        ("dealer", ("dealer wise", "by dealer", "dealer level")),
        ("brand", ("brand wise", "brand-wise", "by brand", "brand level", "brand stats")),
    ]

    requested: list[str] = []
    for dimension, patterns in hints:
        if any(pattern in low for pattern in patterns):
            requested.append(dimension)

    # Keep single-word fallbacks last to avoid false positives.
    single_word_hints: list[tuple[str, str]] = [
        ("state", "state"),
        ("month", "month"),
        ("city", "city"),
        ("channel", "channel"),
        ("brand", "brand"),
        ("reason", "complaint"),
        ("status", "status"),
        ("branch", "branch"),
        ("dealer", "dealer"),
    ]
    for dimension, token in single_word_hints:
        if token in low and dimension not in requested:
            requested.append(dimension)

    return requested


def _chatbot_requested_dimensions(payload: ChatbotPayload) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    text_candidates = [payload.message]
    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        text_candidates.append(turn.content)

    for text in text_candidates:
        for dimension in _chatbot_requested_dimensions_from_text(text):
            if dimension in seen:
                continue
            seen.add(dimension)
            ordered.append(dimension)
    return ordered


def _prioritize_dimensions(base_dimensions: list[str], requested_dimensions: list[str]) -> list[str]:
    ordered: list[str] = []
    for dimension in requested_dimensions + base_dimensions:
        if dimension and dimension not in ordered:
            ordered.append(dimension)
    return ordered


def _is_chatbot_greeting(text: str) -> bool:
    tokens = _chatbot_message_tokens(text)
    if not tokens:
        return False

    greeting_words = {
        "hi",
        "hii",
        "hello",
        "hey",
        "yo",
        "hola",
        "namaste",
        "thanks",
        "thank",
        "ok",
        "okay",
    }
    if len(tokens) <= 3 and all(token in greeting_words for token in tokens):
        return True

    joined = " ".join(tokens)
    greeting_phrases = (
        "good morning",
        "good afternoon",
        "good evening",
        "how are you",
        "who are you",
    )
    return any(phrase in joined for phrase in greeting_phrases)


def _requests_global_scope(text: str) -> bool:
    low = " ".join(_chatbot_message_tokens(text))
    if not low:
        return False
    scope_phrases = (
        "all partners",
        "all partner",
        "across all partners",
        "across partners",
        "all sources",
        "across all sources",
        "across sources",
        "overall",
        "entire database",
        "full database",
        "all datasets",
        "across datasets",
        "look into those datasets",
        "actual datasets",
        "raw datasets",
        "all data",
        "complete database",
        "whole database",
    )
    return any(phrase in low for phrase in scope_phrases)


def _needs_partner_specification_prompt(
    *,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> bool:
    if not bool(context_payload.get("global_scope")):
        return False
    if _detect_source_from_text(payload.message):
        return False
    if _requests_global_scope(payload.message):
        return False
    source_origin = str(context_payload.get("source_origin") or "").strip().lower()
    return source_origin in {"", "none", "payload"}


def _prepend_partner_scope_prompt(
    answer: str,
    *,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str:
    text = (answer or "").strip()
    if not text:
        return text
    if not _needs_partner_specification_prompt(payload=payload, context_payload=context_payload):
        return text

    allowed_labels = context_payload.get("allowed_labels") or []
    partner_labels = [
        str(label).strip()
        for label in allowed_labels
        if str(label).strip() and str(label).strip().lower() != "all sources"
    ]
    if not partner_labels:
        partner_labels = [SAMSUNG_PARTNER_LABELS[source_key] for source_key in SAMSUNG_PARTNER_SOURCES]
        partner_labels.extend(["Reliance ResQ", "Godrej"])
    partner_preview = ", ".join(partner_labels)
    prefix = (
        "Partner is not specified. I am sharing the overall combined view across all available partners. "
        f"If you want a particular reference, mention a partner ({partner_preview}) or narrow it by state, city, product, branch, or date range."
    )
    return f"{prefix}\n{text}"


def _chatbot_available_scopes(
    *,
    db: Session,
    job_id: str | None,
) -> list[dict[str, Any]]:
    cached = _chatbot_scope_cache_get(job_id=job_id)
    if cached is not None:
        return cached

    try:
        query = db.query(
            DataRow.source,
            DataRow.dataset_type,
            func.count(DataRow.id),
        )
        if job_id:
            query = query.filter(DataRow.job_id == job_id)
        rows = query.group_by(DataRow.source, DataRow.dataset_type).all()
    except Exception:
        logger.exception("Failed to load chatbot source coverage from data_rows.")
        return []

    grouped_counts: dict[tuple[str, str], int] = {}
    for source_raw, dataset_raw, row_count_raw in rows:
        source = _normalize_source_key(str(source_raw or ""))
        dataset_type = (str(dataset_raw or "").strip().lower())
        if not source or dataset_type not in {"sales", "claims"}:
            continue
        key = (source, dataset_type)
        grouped_counts[key] = grouped_counts.get(key, 0) + int(row_count_raw or 0)

    scopes = [
        {
            "source": source,
            "dataset_type": dataset_type,
            "row_count": row_count,
        }
        for (source, dataset_type), row_count in grouped_counts.items()
    ]
    scopes.sort(
        key=lambda item: (
            -int(item.get("row_count", 0) or 0),
            _source_display_name(str(item.get("source", ""))),
            str(item.get("dataset_type", "")),
        )
    )
    _chatbot_scope_cache_set(job_id=job_id, scopes=scopes)
    return scopes


def _sum_metric_from_dataframe(
    frame: Any,
    candidates: list[str],
) -> float:
    if frame is None or getattr(frame, "empty", True):
        return 0.0

    safe_to_columns: dict[str, list[str]] = {}
    try:
        columns = list(frame.columns)
    except Exception:
        columns = []
    for col in columns:
        safe_col = _to_safe_key(str(col))
        safe_to_columns.setdefault(safe_col, []).append(str(col))

    for candidate in candidates:
        for col in safe_to_columns.get(_to_safe_key(candidate), []):
            total = 0.0
            found_numeric = False
            try:
                values = frame[col].tolist()
            except Exception:
                values = []
            for raw in values:
                num = _to_number(raw)
                if num is None:
                    continue
                total += float(num)
                found_numeric = True
            if found_numeric:
                return total

    return 0.0


def _build_live_summary_for_scope(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    try:
        frame = get_dataframe(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
        )
    except Exception:
        logger.exception(
            "Chatbot live summary fetch failed source=%s dataset=%s job_id=%s",
            source,
            dataset_type,
            job_id,
        )
        return {}

    if frame is None or getattr(frame, "empty", True):
        return {}

    if from_date or to_date:
        try:
            frame = filter_by_date_range(
                frame,
                dataset_type,
                from_date,
                to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot live date filtering failed source=%s dataset=%s from=%s to=%s",
                source,
                dataset_type,
                from_date,
                to_date,
            )
            return {}

    if frame is None or getattr(frame, "empty", True):
        return {}

    row_count = 0
    try:
        row_count = int(len(frame.index))
    except Exception:
        row_count = 0

    if dataset_type == "claims":
        total_claims_cost = _sum_metric_from_dataframe(
            frame,
            [
                "claims",
                "net_amount",
                "claim_amount",
                "zoppers_cost",
                "gross_premium",
                "amount",
            ],
        )
        net_claims_cost = _sum_metric_from_dataframe(
            frame,
            [
                "net_claims",
                "net_claim",
                "net_amount",
                "earned_premium",
            ],
        )
        claims_count = _sum_metric_from_dataframe(
            frame,
            [
                "quantity",
                "units_sold",
                "claims_count",
                "count",
            ],
        )
        if claims_count <= 0:
            claims_count = float(row_count)
        if net_claims_cost <= 0 and total_claims_cost > 0:
            net_claims_cost = total_claims_cost
        return {
            "gross_premium": float(total_claims_cost),
            "earned_premium": float(net_claims_cost),
            "units_sold": float(claims_count),
        }

    gross_premium = _sum_metric_from_dataframe(
        frame,
        [
            "gross_premium",
            "amount",
            "plan_selling_price",
            "plan_price",
        ],
    )
    earned_premium = _sum_metric_from_dataframe(
        frame,
        [
            "earned_premium",
            "written_premium",
            "earnedpremium",
        ],
    )
    zopper_earned_premium = _sum_metric_from_dataframe(
        frame,
        [
            "zopper_earned_premium",
            "earned_zopper",
            "zopper_shared_transfer_price",
        ],
    )
    units_sold = _sum_metric_from_dataframe(
        frame,
        [
            "quantity",
            "units_sold",
            "units",
            "count",
        ],
    )

    if units_sold <= 0:
        units_sold = float(row_count)
    if earned_premium <= 0 and gross_premium > 0:
        earned_premium = gross_premium

    return {
        "gross_premium": float(gross_premium),
        "earned_premium": float(earned_premium),
        "zopper_earned_premium": float(zopper_earned_premium),
        "units_sold": float(units_sold),
    }


def _resolve_summary_for_scope(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    try:
        summary = analytics_summary(
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
            db=db,
        )
        if isinstance(summary, dict) and summary:
            return summary
    except Exception:
        logger.exception(
            "Failed to resolve chatbot summary via analytics.summary source=%s dataset=%s from=%s to=%s",
            source,
            dataset_type,
            from_date,
            to_date,
        )
    return _build_live_summary_for_scope(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )


def _pick_frame_column(frame: Any, candidates: list[str]) -> str | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        return None
    safe_to_raw: dict[str, str] = {}
    for col in columns:
        safe_to_raw[_to_safe_key(col)] = col
    for candidate in candidates:
        hit = safe_to_raw.get(_to_safe_key(candidate))
        if hit:
            return hit
    return None


_CHATBOT_COLUMN_GROUP_ALIASES: dict[str, tuple[str, ...]] = {
    "reason": (
        "reason",
        "reasons",
        "claim reason",
        "reason code",
        "root cause",
        "cause",
        "nature of complaint",
        "complaint nature",
        "complaint",
        "issue",
        "problem",
        "failure reason",
        "fault",
        "defect",
        "fault code",
    ),
    "operation": (
        "major call operation",
        "operation",
        "service operation",
        "repair action",
        "resolution",
        "action taken",
    ),
    "call_type": (
        "call type",
        "claim type",
        "ticket type",
        "service type",
        "case type",
        "coverage type",
    ),
    "status": (
        "call status",
        "claim status",
        "service status",
        "approval status",
        "status",
        "payout type",
    ),
    "month": (
        "month",
        "month name",
        "month_name",
        "claim date",
        "call date",
        "service closed date",
        "sms closure date",
        "approveddate",
        "date",
        "payment date",
        "plan start date",
        "warranty start date",
    ),
    "state": (
        "state",
        "customer state",
        "state name",
        "region",
        "location",
    ),
    "city": (
        "city",
        "customer city",
        "town",
    ),
    "zone": (
        "zone name",
        "zone",
        "region",
    ),
    "branch": (
        "branch name",
        "new branch",
        "branch",
    ),
    "dealer": (
        "dealer name",
        "dealer type",
        "dealer id",
        "dealer",
        "store name",
    ),
    "channel": (
        "channel",
        "channel name",
        "channel_name",
        "purchase from",
    ),
    "product_category": (
        "product category",
        "product_category",
        "category",
        "brand",
        "article brand",
        "product brand",
    ),
    "product_subcategory": (
        "product subcategory",
        "product subcatergory",
        "subcategory",
        "sub category",
        "model no",
        "model description",
        "model code",
        "item description",
        "appliance model name",
        "device plan category",
        "plan category",
        "item name",
        "care+ plan name",
    ),
    "source": (
        "__chatbot_source",
        "source",
        "partner",
        "brand",
    ),
}

_CHATBOT_ANALYTICS_DIMENSIONS = {
    "month",
    "state",
    "city",
    "channel",
    "brand",
    "plan_category",
    "device_plan_category",
    "product_category",
    "product_subcategory",
    "reason",
}

_CHATBOT_REASON_GROUP_KEYS = ("reason", "operation", "call_type", "status")

_CHATBOT_COLUMN_STOPWORDS = {
    "the",
    "of",
    "and",
    "or",
    "to",
    "for",
    "by",
    "with",
    "from",
    "name",
    "number",
    "no",
    "id",
}


def _unique_preserving_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _frame_columns(frame: Any) -> list[str]:
    if frame is None or getattr(frame, "empty", True):
        return []
    try:
        return [str(col) for col in list(frame.columns)]
    except Exception:
        return []


def _safe_text_parts(value: str) -> list[str]:
    return [
        part
        for part in _to_safe_key(value).split("_")
        if part and part not in _CHATBOT_COLUMN_STOPWORDS
    ]


def _match_frame_columns(
    frame: Any,
    aliases: tuple[str, ...] | list[str],
    *,
    limit: int = 6,
) -> list[str]:
    columns = _frame_columns(frame)
    if not columns:
        return []

    scored: list[tuple[int, int, str]] = []
    for raw_col in columns:
        safe_col = _to_safe_key(raw_col)
        best_score = 0
        for alias in aliases:
            safe_alias = _to_safe_key(alias)
            if not safe_alias:
                continue
            if safe_col == safe_alias:
                best_score = max(best_score, 100)
                continue
            if safe_alias in safe_col:
                best_score = max(best_score, 82)
                continue
            alias_parts = [part for part in safe_alias.split("_") if part]
            if alias_parts and all(part in safe_col for part in alias_parts):
                best_score = max(best_score, 70)
        if best_score > 0:
            scored.append((best_score, len(safe_col), raw_col))

    scored.sort(key=lambda item: (-item[0], item[1], item[2].lower()))
    return [raw_col for _, _, raw_col in scored[:limit]]


def _semantic_field_map_for_frame(frame: Any) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for group_key, aliases in _CHATBOT_COLUMN_GROUP_ALIASES.items():
        mapping[group_key] = _match_frame_columns(frame, aliases, limit=5)
    return mapping


def _message_column_match(
    frame: Any,
    message: str,
    *,
    exclude: set[str] | None = None,
) -> str | None:
    columns = _frame_columns(frame)
    if not columns:
        return None

    excluded = {str(item) for item in (exclude or set())}
    safe_message = _to_safe_key(message)
    if not safe_message:
        return None

    message_parts = set(_safe_text_parts(message))
    scored: list[tuple[int, int, str]] = []
    for raw_col in columns:
        if raw_col in excluded:
            continue
        safe_col = _to_safe_key(raw_col)
        if not safe_col:
            continue

        score = 0
        if safe_col in safe_message:
            score += 70

        col_parts = set(_safe_text_parts(raw_col))
        overlap = len(col_parts & message_parts)
        if overlap:
            score += overlap * 12
            if col_parts and col_parts.issubset(message_parts):
                score += 18

        if score >= 24:
            scored.append((score, len(safe_col), raw_col))

    scored.sort(key=lambda item: (-item[0], item[1], item[2].lower()))
    return scored[0][2] if scored else None


def _semantic_group_for_column(frame: Any, column_name: str) -> str | None:
    for group_key, aliases in _CHATBOT_COLUMN_GROUP_ALIASES.items():
        matches = _match_frame_columns(frame, aliases, limit=3)
        if column_name in matches:
            return group_key
    return None


def _chatbot_scope_sources(
    *,
    db: Session,
    context_payload: dict[str, Any],
    dataset_type: str,
) -> list[str]:
    resolved_dataset = (dataset_type or "").strip().lower()
    if resolved_dataset not in {"sales", "claims"}:
        return []

    if bool(context_payload.get("global_scope")):
        scopes = _chatbot_available_scopes(
            db=db,
            job_id=_normalize_chatbot_job_id(context_payload.get("job_id")),
        )
        sources = [
            _normalize_source_key(str(scope.get("source", "")))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == resolved_dataset
        ]
        return _unique_preserving_order([source for source in sources if source])

    source = _normalize_source_key(str(context_payload.get("source") or ""))
    return [source] if source else []


def _load_chatbot_scope_frame(
    *,
    db: Session,
    sources: list[str],
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frame = get_dataframe(
                db=db,
                job_id=job_id,
                source=source,
                dataset_type=dataset_type,
            )
        except Exception:
            logger.exception(
                "Chatbot scope frame fetch failed source=%s dataset=%s job_id=%s",
                source,
                dataset_type,
                job_id,
            )
            continue

        if frame is None or getattr(frame, "empty", True):
            continue

        try:
            scoped = frame.copy()
        except Exception:
            continue

        if from_date or to_date:
            try:
                scoped = filter_by_date_range(scoped, dataset_type, from_date, to_date)
            except Exception:
                logger.exception(
                    "Chatbot scope frame date filtering failed source=%s dataset=%s from=%s to=%s",
                    source,
                    dataset_type,
                    from_date,
                    to_date,
                )
                continue

        if scoped is None or getattr(scoped, "empty", True):
            continue

        scoped = scoped.copy()
        scoped["__chatbot_source"] = _source_display_name(source)
        frames.append(scoped)

    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True, sort=False)


def _clean_categorical_value(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value or value.lower() in {"nan", "none", "null"}:
        return ""
    return value


def _top_categorical_summary(
    frame: Any,
    column_name: str,
    metric_column: str | None = None,
    *,
    limit: int = 6,
) -> list[dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    if column_name not in _frame_columns(frame):
        return []

    try:
        working = frame.copy()
        working[column_name] = working[column_name].map(_clean_categorical_value)
        working = working[working[column_name].astype(bool)].copy()
        if working.empty:
            return []

        if metric_column and metric_column in working.columns:
            working["__metric_value"] = pd.to_numeric(working[metric_column], errors="coerce").fillna(0.0)
            grouped = working.groupby(column_name).agg(
                count=(column_name, "size"),
                total=("__metric_value", "sum")
            ).sort_values("count", ascending=False).head(limit)
            
            return [
                {
                    "label": str(label),
                    "count": int(row["count"]),
                    "total": float(row["total"])
                }
                for label, row in grouped.iterrows()
            ]
        else:
            counts = working[column_name].value_counts().head(limit)
            return [
                {
                    "label": str(label),
                    "count": int(count),
                    "total": 0.0
                }
                for label, count in counts.items()
            ]
    except Exception:
        logger.exception("Failed to compute categorical summary for %s", column_name)
        return []


def _top_categorical_counts(
    frame: Any,
    column_name: str,
    *,
    limit: int = 6,
) -> list[tuple[str, int]]:
    summary = _top_categorical_summary(frame, column_name, limit=limit)
    return [(item["label"], item["count"]) for item in summary]


def _is_reason_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False

    reason_tokens = (
        "reason",
        "reasons",
        "cause",
        "causes",
        "root cause",
        "root causes",
        "why are claims",
        "why claim",
        "common issue",
        "common issues",
        "nature of complaint",
        "complaint",
    )
    claim_tokens = (
        "claim",
        "claims",
        "breakdown",
        "service",
        "issue",
    )
    return any(token in low for token in reason_tokens) and any(token in low for token in claim_tokens)


def _build_reason_breakdown_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_reason_query(payload.message):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "claims")
    if dataset_type != "claims":
        return None

    sources = _chatbot_scope_sources(
        db=db,
        context_payload=context_payload,
        dataset_type="claims",
    )
    if not sources:
        return None

    frame = _load_chatbot_scope_frame(
        db=db,
        sources=sources,
        dataset_type="claims",
        job_id=_normalize_chatbot_job_id(context_payload.get("job_id")),
        from_date=_normalize_chatbot_date(context_payload.get("from_date")),
        to_date=_normalize_chatbot_date(context_payload.get("to_date")),
    )
    if frame.empty:
        return None

    semantic_map = _semantic_field_map_for_frame(frame)
    reason_columns = _unique_preserving_order(
        [
            *(semantic_map.get("reason") or []),
            *(semantic_map.get("operation") or []),
            *(semantic_map.get("call_type") or []),
            *(semantic_map.get("status") or []),
        ]
    )
    if not reason_columns:
        return _prepend_partner_scope_prompt(
            "I checked the raw claims rows, but I could not find explicit reason or complaint columns in this slice.",
            payload=payload,
            context_payload=context_payload,
        )

    field_labels = {
        "reason": "complaint reason",
        "operation": "service operation",
        "call_type": "call type",
        "status": "call status",
    }
    lines: list[str] = [
        "Yes. The claims data does include reason-like fields, so I can answer this from the dataset directly."
    ]

    payout_column = _pick_frame_column(
        frame,
        [
            "Claims Costing",
            "Claim_Amount",
            "Payout Amount",
            "Amount",
            "Invoice Amount",
            "Payment Amount",
            "net_amount",
            "claim_amount",
        ],
    )

    for column_name in reason_columns[:3]:
        group_key = _semantic_group_for_column(frame, column_name) or "reason"
        summary = _top_categorical_summary(frame, column_name, metric_column=payout_column, limit=5)
        if not summary:
            continue
        label = field_labels.get(group_key, "reason field")
        
        detail_bits = []
        for item in summary:
            bit = f"{item['label']} ({item['count']:,} claims"
            if item.get("total") and item["total"] > 0:
                bit += f", payout {_format_metric_value('claims', item['total'])}"
            bit += ")"
            detail_bits.append(bit)
            
        lines.append(
            f"Top {label} in {column_name}: "
            + "; ".join(detail_bits)
            + "."
        )

    if len(sources) == 1:
        scope_label = _source_display_name(sources[0])
    else:
        scope_label = "all available partners"
    from_date = _normalize_chatbot_date(context_payload.get("from_date"))
    to_date = _normalize_chatbot_date(context_payload.get("to_date"))
    if from_date or to_date:
        lines.append(
            f"Scope used: {scope_label}, {from_date or 'start'} to {to_date or 'latest'}."
        )
    else:
        lines.append(f"Scope used: {scope_label}, all available data.")

    return _prepend_partner_scope_prompt(
        "\n".join(lines),
        payload=payload,
        context_payload=context_payload,
    )


def _is_graph_request(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    graph_tokens = (
        "graph",
        "chart",
        "plot",
        "visualize",
        "visualise",
        "bar chart",
        "line chart",
        "pie chart",
        "donut chart",
        "trend chart",
    )
    return any(token in low for token in graph_tokens)


def _extract_requested_limit(
    message: str,
    *,
    default: int = 8,
    minimum: int = 4,
    maximum: int = 16,
) -> int:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return default
    match = re.search(r"\btop\s+(\d{1,2})\b", low) or re.search(r"\bshow\s+(\d{1,2})\b", low)
    if not match:
        return default
    try:
        value = int(match.group(1))
    except Exception:
        return default
    return max(minimum, min(maximum, value))


def _chart_metric_spec(
    *,
    metric: str,
    label: str,
    aggregation: str,
    fmt: str,
) -> dict[str, str]:
    series_key = _to_safe_key(f"{metric}_{aggregation}") or f"series_{metric}"
    return {
        "metric": (metric or "").strip().lower(),
        "label": label.strip() or _pretty_label(metric),
        "aggregation": (aggregation or "sum").strip().lower(),
        "format": fmt.strip() or (metric or "").strip().lower(),
        "key": series_key,
    }


def _resolve_chart_metric_specs(message: str, dataset_type: str) -> list[dict[str, str]]:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return []

    specs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(metric: str, label: str, aggregation: str = "sum", fmt: str | None = None) -> None:
        key = (metric, aggregation)
        if key in seen:
            return
        seen.add(key)
        specs.append(
            _chart_metric_spec(
                metric=metric,
                label=label,
                aggregation=aggregation,
                fmt=fmt or metric,
            )
        )

    if "loss ratio" in low:
        add("loss_ratio", "Loss Ratio", "sum", "loss_ratio")
    if "net claim" in low or "net amount" in low:
        add("net_claims", "Net Claims", "sum", "net_claims")
    if any(token in low for token in ("claim amount", "claims cost", "claim cost", "payout", "claim value")):
        add("claims", "Claims Cost", "sum", "claims")
    if "zopper earned" in low:
        add("zopper_earned_premium", "Zopper Earned Premium", "sum", "zopper_earned_premium")
    if "earned premium" in low and "zopper earned" not in low:
        add("earned_premium", "Earned Premium", "sum", "earned_premium")
    if any(token in low for token in ("gross premium", "revenue", "sales value")):
        add("gross_premium", "Gross Premium", "sum", "gross_premium")

    count_tokens = (
        "quantity",
        "count",
        "volume",
        "units sold",
        "units",
        "number of claims",
        "no of claims",
        "no. of claims",
        "claim count",
        "number of sales",
        "policy count",
        "most common",
    )
    if any(token in low for token in count_tokens) or _is_reason_query(message):
        if dataset_type == "sales" and not _is_reason_query(message):
            add("quantity", "Quantity", "sum", "quantity")
        else:
            add("count", "Count", "count", "quantity")

    if any(token in low for token in ("average claim", "avg claim", "mean claim")):
        add("claims", "Average Claim Amount", "avg", "claims")
    if any(
        token in low
        for token in (
            "average premium",
            "avg premium",
            "average selling price",
            "avg selling price",
            "asp",
            "average price",
            "avg price",
        )
    ):
        add("gross_premium", "Average Premium", "avg", "gross_premium")

    if not specs:
        if dataset_type == "claims":
            add("count", "Claims Count", "count", "quantity")
        else:
            add("gross_premium", "Gross Premium", "sum", "gross_premium")

    return specs[:2]


def _time_bucket_from_message(message: str) -> str:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if "daily" in low or "day wise" in low or "by day" in low:
        return "day"
    if "weekly" in low or "week wise" in low or "by week" in low:
        return "week"
    return "month"


def _parse_time_dimension_series(series: pd.Series, bucket: str) -> tuple[pd.Series, pd.Series]:
    raw = series.astype(str).str.strip()
    try:
        parsed = pd.to_datetime(raw, format="mixed", errors="coerce")
    except TypeError:
        parsed = pd.to_datetime(raw, errors="coerce")

    if parsed.isna().all():
        for fmt in ("%b-%y", "%b %y", "%b-%Y", "%b %Y", "%Y-%m", "%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y"):
            parsed_try = pd.to_datetime(raw, format=fmt, errors="coerce")
            if parsed_try.notna().any():
                parsed = parsed_try
                break

    if bucket == "day":
        labels = parsed.dt.strftime("%d-%b-%y")
        sort_values = parsed.dt.normalize()
    elif bucket == "week":
        week_start = parsed.dt.to_period("W").dt.start_time
        labels = week_start.dt.strftime("%d-%b-%y")
        sort_values = week_start
    else:
        month_start = parsed.dt.to_period("M").dt.to_timestamp()
        labels = month_start.dt.strftime("%b-%y")
        sort_values = month_start

    return labels, sort_values


def _metric_series_from_frame(
    frame: pd.DataFrame,
    metric: str,
    dataset_type: str,
) -> pd.Series | None:
    metric_key = (metric or "").strip().lower()
    if frame is None or frame.empty:
        return None

    if metric_key in {"count", "quantity"}:
        return pd.Series(1.0, index=frame.index, dtype="float64")

    candidate_map: dict[str, list[str]] = {
        "claims": [
            "Claim_Amount",
            "Claim Amount",
            "Payout Amount",
            "Amount",
            "Invoice Amount",
            "Payment Amount",
        ],
        "net_claims": [
            "Net Amount",
            "Net Claims",
            "Net_Claim_Amount",
            "Claim_Amount",
            "Claim Amount",
        ],
        "gross_premium": [
            "Gross Premium",
            "Customer Premium",
            "Plan Selling Price",
            "Amount",
            "Premium",
        ],
        "earned_premium": [
            "Earned_Premium",
            "Earned Premium",
            "Net Amount",
        ],
        "zopper_earned_premium": [
            "Zopper_Share_EP",
            "Zopper Earned Premium",
            "Zopper Share",
        ],
    }

    candidates = candidate_map.get(metric_key, [])
    column_name = _pick_frame_column(frame, candidates)
    if column_name is None:
        if dataset_type == "claims" and metric_key == "claims":
            column_name = _pick_frame_column(frame, ["Claim_Amount", "Payout Amount", "Amount"])
        elif dataset_type == "sales" and metric_key == "gross_premium":
            column_name = _pick_frame_column(frame, ["Customer Premium", "Premium", "Amount"])
    if column_name is None:
        return None

    series = pd.to_numeric(frame[column_name], errors="coerce")
    if series.notna().any():
        return series.fillna(0.0)
    return None


_CHATBOT_QUANTITY_FIELD_CANDIDATES = [
    "quantity",
    "units_sold",
    "units sold",
    "units",
    "count",
    "claims_count",
    "claim count",
    "no_of_claims",
    "no. of claims",
    "number of claims",
    "no_of_policies",
    "policy count",
]


def _message_requests_row_count_proxy(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False

    explicit_proxy_tokens = (
        "count the rows",
        "count rows",
        "row count",
        "rows month on month",
        "row-wise count",
        "plan count",
        "quantity of plans",
        "number of plans",
        "how many plans",
        "plan volume",
        "monthly activations",
    )
    if any(token in low for token in explicit_proxy_tokens):
        return True

    if any(token in low for token in ("calculate it yourself", "calculate yourself")) and any(
        token in low for token in ("row", "rows", "count", "quantity", "plan", "volume")
    ):
        return True

    if any(token in low for token in ("don't use quantity", "do not use quantity", "quantity column")) and any(
        token in low for token in ("row", "rows", "count", "calculate")
    ):
        return True

    return False


def _chatbot_month_column(frame: pd.DataFrame) -> str | None:
    candidates = [
        *_CHATBOT_COLUMN_GROUP_ALIASES.get("month", ()),
        "sale date",
        "sales date",
        "activation date",
        "booking date",
        "created at",
        "created_on",
        "created date",
        "invoice date",
        "policy issued date",
    ]
    column_name = _pick_frame_column(frame, candidates)
    if column_name:
        return column_name

    for raw_col in frame.columns:
        safe_col = _to_safe_key(str(raw_col))
        if "month" in safe_col or "date" in safe_col:
            return str(raw_col)
    return None


def _quantity_series_from_frame(
    frame: pd.DataFrame,
    *,
    force_row_count: bool = False,
) -> tuple[pd.Series | None, str]:
    if frame is None or frame.empty:
        return None, "none"

    if not force_row_count:
        column_name = _pick_frame_column(frame, _CHATBOT_QUANTITY_FIELD_CANDIDATES)
        if column_name:
            numeric = pd.to_numeric(frame[column_name], errors="coerce")
            if numeric.notna().any() and float(numeric.abs().sum()) > 0:
                return numeric.fillna(0.0), "quantity_column"

    return pd.Series(1.0, index=frame.index, dtype="float64"), "row_count"


def _extract_monthly_metric_series_from_frame(
    frame: pd.DataFrame,
    *,
    metric: str,
    dataset_type: str,
    force_row_count: bool = False,
) -> tuple[list[tuple[date, float]], str]:
    if frame is None or frame.empty:
        return [], "none"

    month_column = _chatbot_month_column(frame)
    if not month_column:
        return [], "none"

    metric_key = (metric or "").strip().lower()
    proxy_mode = "metric"
    if metric_key in {"quantity", "count"}:
        series, proxy_mode = _quantity_series_from_frame(frame, force_row_count=force_row_count)
    else:
        series = _metric_series_from_frame(frame, metric, dataset_type)

    if series is None:
        return [], proxy_mode

    monthly: dict[date, float] = {}
    for raw_month, raw_value in zip(frame[month_column], series):
        month_start = _parse_month_start(raw_month)
        if month_start is None:
            continue
        numeric = _to_number(raw_value)
        if numeric is None:
            continue
        monthly[month_start] = monthly.get(month_start, 0.0) + float(numeric)

    return sorted(monthly.items(), key=lambda item: item[0]), proxy_mode


def _aggregate_graph_metric_across_sources(
    *,
    db: Session,
    sources: list[str],
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    dimension: str,
    metric: str,
) -> dict[str, float]:
    totals: dict[str, float] = {}
    for source in sources:
        rows = _chatbot_graph_rows(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            from_date=from_date,
            to_date=to_date,
        )
        local = _aggregate_metric_by_dimension(rows, dimension=dimension, metric=metric)
        for label, value in local.items():
            totals[label] = totals.get(label, 0.0) + float(value)
    return totals


def _aggregate_raw_metric_across_frame(
    *,
    frame: pd.DataFrame,
    dimension_column: str,
    dataset_type: str,
    metric_spec: dict[str, str],
    time_bucket: str | None,
) -> dict[str, float]:
    if frame is None or frame.empty or dimension_column not in frame.columns:
        return {}

    working = frame.copy()
    if time_bucket:
        labels, sort_values = _parse_time_dimension_series(working[dimension_column], time_bucket)
        working["__chart_label"] = labels
        working["__chart_sort"] = sort_values
        working = working[working["__chart_sort"].notna()].copy()
    else:
        working["__chart_label"] = working[dimension_column].map(_clean_categorical_value)
        working = working[working["__chart_label"].astype(bool)].copy()

    if working.empty:
        return {}

    aggregation = metric_spec.get("aggregation", "sum")
    if aggregation == "count":
        grouped = working.groupby("__chart_label", dropna=False).size()
        return {str(label): float(value) for label, value in grouped.items()}

    series = _metric_series_from_frame(
        working,
        metric_spec.get("metric", ""),
        dataset_type,
    )
    if series is None:
        return {}

    working["__chart_value"] = series
    grouped_obj = working.groupby("__chart_label", dropna=False)["__chart_value"]
    if aggregation == "avg":
        grouped = grouped_obj.mean()
    else:
        grouped = grouped_obj.sum()
    return {str(label): float(value) for label, value in grouped.items()}


def _resolve_chart_dimension(
    *,
    frame: pd.DataFrame,
    message: str,
    dataset_type: str,
) -> tuple[str | None, str | None]:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if frame is None or frame.empty:
        return None, None

    if any(token in low for token in ("partner", "partners", "source", "sources")) and "__chatbot_source" in frame.columns:
        return "__chatbot_source", "source"

    requested_dimensions = _chatbot_requested_dimensions_from_text(message)
    for dimension_key in requested_dimensions:
        aliases = _CHATBOT_COLUMN_GROUP_ALIASES.get(dimension_key)
        if not aliases:
            continue
        matches = _match_frame_columns(frame, aliases, limit=1)
        if matches:
            return matches[0], dimension_key

    if _is_reason_query(message):
        for group_key in _CHATBOT_REASON_GROUP_KEYS:
            matches = _match_frame_columns(frame, _CHATBOT_COLUMN_GROUP_ALIASES[group_key], limit=1)
            if matches:
                return matches[0], group_key

    message_match = _message_column_match(frame, message, exclude={"__chatbot_source"})
    if message_match:
        return message_match, _semantic_group_for_column(frame, message_match)

    if any(token in low for token in ("trend", "timeline", "month", "monthly", "over time", "by date")):
        matches = _match_frame_columns(frame, _CHATBOT_COLUMN_GROUP_ALIASES["month"], limit=1)
        if matches:
            return matches[0], "month"

    fallback_order = (
        ["month", "reason", "product_category", "state", "city", "channel", "source"]
        if dataset_type == "claims"
        else ["month", "product_category", "product_subcategory", "channel", "state", "source"]
    )
    for group_key in fallback_order:
        aliases = _CHATBOT_COLUMN_GROUP_ALIASES.get(group_key)
        if not aliases:
            continue
        matches = _match_frame_columns(frame, aliases, limit=1)
        if matches:
            return matches[0], group_key

    return None, None


def _resolve_chart_type(
    *,
    message: str,
    dimension_key: str | None,
    series_count: int,
) -> str:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if "pie chart" in low or "donut chart" in low:
        return "pie"
    if series_count > 1:
        return "composed"
    if "bar chart" in low or "column chart" in low:
        return "bar"
    if "line chart" in low or "trend" in low or "time series" in low or "timeline" in low:
        return "line"
    if any(token in low for token in ("mix", "split", "share", "distribution")) and dimension_key != "month":
        return "pie"
    if dimension_key == "month":
        return "line"
    return "bar"


def _sort_chart_rows(
    rows: list[dict[str, Any]],
    *,
    chart_type: str,
    primary_key: str,
    ascending: bool = False,
) -> list[dict[str, Any]]:
    if chart_type == "line":
        return sorted(rows, key=lambda row: str(row.get("__sort") or ""))
    return sorted(
        rows,
        key=lambda row: float(row.get(primary_key) or 0.0),
        reverse=not ascending,
    )


def _build_chart_rows(
    *,
    db: Session,
    frame: pd.DataFrame,
    sources: list[str],
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    dimension_column: str,
    dimension_key: str | None,
    metric_specs: list[dict[str, str]],
    message: str,
) -> list[dict[str, Any]]:
    if not metric_specs:
        return []

    time_bucket = _time_bucket_from_message(message) if dimension_key == "month" else None
    label_to_row: dict[str, dict[str, Any]] = {}

    for metric_spec in metric_specs:
        metric_key = metric_spec.get("metric", "")
        if (
            dimension_key in _CHATBOT_ANALYTICS_DIMENSIONS
            and metric_key in {"claims", "net_claims", "loss_ratio", "gross_premium", "earned_premium", "zopper_earned_premium"}
            and metric_spec.get("aggregation") != "avg"
        ):
            metric_rows = _aggregate_graph_metric_across_sources(
                db=db,
                sources=sources,
                dataset_type=dataset_type,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                dimension=dimension_key or "month",
                metric=metric_key,
            )
        else:
            metric_rows = _aggregate_raw_metric_across_frame(
                frame=frame,
                dimension_column=dimension_column,
                dataset_type=dataset_type,
                metric_spec=metric_spec,
                time_bucket=time_bucket,
            )

        for label, value in metric_rows.items():
            row = label_to_row.setdefault(label, {"label": label})
            row[metric_spec["key"]] = float(value)

    rows = list(label_to_row.values())
    if not rows:
        return []

    if time_bucket:
        parsed = pd.to_datetime([row.get("label") for row in rows], format="%d-%b-%y", errors="coerce")
        if parsed.isna().all():
            parsed = pd.to_datetime([row.get("label") for row in rows], format="%b-%y", errors="coerce")
        for row, parsed_value in zip(rows, parsed):
            row["__sort"] = parsed_value.isoformat() if pd.notna(parsed_value) else ""

    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    ascending = any(token in low for token in ("lowest", "least", "bottom", "smallest"))
    chart_type = _resolve_chart_type(
        message=message,
        dimension_key=dimension_key,
        series_count=len(metric_specs),
    )
    rows = _sort_chart_rows(
        rows,
        chart_type=chart_type,
        primary_key=metric_specs[0]["key"],
        ascending=ascending,
    )

    if chart_type != "line":
        rows = rows[: _extract_requested_limit(message, default=8)]

    for row in rows:
        row.pop("__sort", None)
    return rows


def _chart_scope_label(sources: list[str], context_payload: dict[str, Any]) -> str:
    if not sources:
        return "selected scope"
    if len(sources) == 1:
        return _source_display_name(sources[0])
    if bool(context_payload.get("global_scope")):
        return "Overall view across all available partners"
    return ", ".join(_source_display_name(source) for source in sources[:3])


def _chart_download_name(
    *,
    sources: list[str],
    dataset_type: str,
    dimension_column: str,
    metric_specs: list[dict[str, str]],
) -> str:
    source_part = "overall" if len(sources) != 1 else _to_safe_key(_source_display_name(sources[0]))
    metric_part = "-".join(_to_safe_key(spec.get("metric", "")) for spec in metric_specs[:2]) or "metric"
    dimension_part = _to_safe_key(dimension_column) or "dimension"
    dataset_part = _to_safe_key(dataset_type) or "dataset"
    return f"chatbot-{source_part}-{dataset_part}-{dimension_part}-{metric_part}"


def _build_chatbot_chart_response(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if not _is_graph_request(payload.message):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    sources = _chatbot_scope_sources(
        db=db,
        context_payload=context_payload,
        dataset_type=dataset_type,
    )
    if not sources:
        return None

    job_id = _normalize_chatbot_job_id(context_payload.get("job_id"))
    from_date = _normalize_chatbot_date(context_payload.get("from_date"))
    to_date = _normalize_chatbot_date(context_payload.get("to_date"))
    frame = _load_chatbot_scope_frame(
        db=db,
        sources=sources,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    if frame.empty:
        return {
            "response": _prepend_partner_scope_prompt(
                "I could not find rows in the selected scope to build this chart.",
                payload=payload,
                context_payload=context_payload,
            ),
            "model": "rule-based-chart",
        }

    dimension_column, dimension_key = _resolve_chart_dimension(
        frame=frame,
        message=payload.message,
        dataset_type=dataset_type,
    )
    if not dimension_column:
        return {
            "response": _prepend_partner_scope_prompt(
                "I could not determine which field to plot. Mention a field such as month, state, city, product category, reason, call type, or call status.",
                payload=payload,
                context_payload=context_payload,
            ),
            "model": "rule-based-chart",
        }

    metric_specs = _resolve_chart_metric_specs(payload.message, dataset_type)
    rows = _build_chart_rows(
        db=db,
        frame=frame,
        sources=sources,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
        dimension_column=dimension_column,
        dimension_key=dimension_key,
        metric_specs=metric_specs,
        message=payload.message,
    )
    if not rows:
        return {
            "response": _prepend_partner_scope_prompt(
                f"I found the field `{dimension_column}` but there was not enough usable data to render the chart.",
                payload=payload,
                context_payload=context_payload,
            ),
            "model": "rule-based-chart",
        }

    chart_type = _resolve_chart_type(
        message=payload.message,
        dimension_key=dimension_key,
        series_count=len(metric_specs),
    )
    dimension_label = _pretty_label(dimension_column)
    chart_title = f"{', '.join(spec['label'] for spec in metric_specs)} by {dimension_label}"
    date_label = (
        f"{from_date or 'start'} to {to_date or 'latest'}"
        if (from_date or to_date)
        else "all available data"
    )
    chart = {
        "title": chart_title,
        "subtitle": f"{_chart_scope_label(sources, context_payload)} | {dataset_type.title()} | {date_label}",
        "chart_type": chart_type,
        "x_key": "label",
        "series": [
            {
                "key": spec["key"],
                "label": spec["label"],
                "format": spec["format"],
                "render_as": "line" if chart_type == "composed" and idx == 1 else "bar",
            }
            for idx, spec in enumerate(metric_specs)
        ],
        "rows": rows,
        "download_name": _chart_download_name(
            sources=sources,
            dataset_type=dataset_type,
            dimension_column=dimension_column,
            metric_specs=metric_specs,
        ),
    }

    primary_series = metric_specs[0]
    summary_bits: list[str] = []
    for row in rows[:3]:
        raw_value = _to_number(row.get(primary_series["key"]))
        if raw_value is None:
            continue
        summary_bits.append(
            f"{row.get('label')} ({_format_metric_value(primary_series['format'], float(raw_value))})"
        )

    response_lines = [f"I created a {chart_type} chart for {chart_title.lower()}."]
    if summary_bits:
        response_lines.append("Chart highlights: " + "; ".join(summary_bits) + ".")
    response_lines.append(
        "If you want, I can redraw it for a particular partner, state, city, branch, product, or narrower date range."
    )

    return {
        "response": _prepend_partner_scope_prompt(
            "\n".join(response_lines),
            payload=payload,
            context_payload=context_payload,
        ),
        "model": "rule-based-chart",
        "chart": chart,
    }


def _sanitize_chatbot_response_text(response_text: str) -> str:
    text = str(response_text or "").strip()
    if not text:
        return ""
    text = re.sub(r"<think>.*?</think>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"</?think>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _build_dataset_field_profile(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> str | None:
    try:
        frame = get_dataframe(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
        )
    except Exception:
        logger.exception(
            "Chatbot dataset profile fetch failed source=%s dataset=%s job_id=%s",
            source,
            dataset_type,
            job_id,
        )
        return None

    if frame is None or getattr(frame, "empty", True):
        return None

    if from_date or to_date:
        try:
            frame = filter_by_date_range(
                frame,
                dataset_type,
                from_date,
                to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot dataset profile date filtering failed source=%s dataset=%s from=%s to=%s",
                source,
                dataset_type,
                from_date,
                to_date,
            )
            return None

    if frame is None or getattr(frame, "empty", True):
        return None

    try:
        row_count = int(len(frame.index))
    except Exception:
        row_count = 0

    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        columns = []

    if not columns:
        return None

    col_preview = ", ".join(columns[:18])
    if len(columns) > 18:
        col_preview += ", ..."

    semantic_map = _semantic_field_map_for_frame(frame)
    dim_candidates = [
        "month",
        "state",
        "city",
        "zone",
        "branch",
        "channel",
        "dealer",
        "product_category",
        "product_subcategory",
        "reason",
        "operation",
        "call_type",
        "status",
    ]
    detected_dims = [dim for dim in dim_candidates if semantic_map.get(dim)]

    price_col = _pick_frame_column(
        frame,
        [
            "amount",
            "gross_premium",
            "plan_selling_price",
            "plan_price",
            "premium",
            "net_amount",
        ],
    )
    qty_col = _pick_frame_column(
        frame,
        [
            "quantity",
            "units_sold",
            "units",
            "count",
            "claims_count",
            "no_of_claims",
            "no_of_policies",
        ],
    )
    cost_or_margin_col = _pick_frame_column(
        frame,
        [
            "net_amount",
            "net_claims",
            "claims",
            "cost",
            "margin",
            "profit",
            "contribution",
            "zopper_share",
            "zopper_shared_transfer_price",
        ],
    )

    dim_text = ", ".join(detected_dims[:6]) if detected_dims else "none detected"
    price_text = price_col or "not found"
    qty_text = qty_col or "not found"
    cost_text = cost_or_margin_col or "not found"
    semantic_parts: list[str] = []
    if semantic_map.get("reason"):
        semantic_parts.append(f"reason/cause fields={', '.join(semantic_map['reason'][:4])}")
    extra_reason_fields = _unique_preserving_order(
        [
            *(semantic_map.get("operation") or []),
            *(semantic_map.get("call_type") or []),
            *(semantic_map.get("status") or []),
        ]
    )
    if extra_reason_fields:
        semantic_parts.append(f"supporting reason fields={', '.join(extra_reason_fields[:4])}")
    product_fields = _unique_preserving_order(
        [
            *(semantic_map.get("product_category") or []),
            *(semantic_map.get("product_subcategory") or []),
        ]
    )
    if product_fields:
        semantic_parts.append(f"product fields={', '.join(product_fields[:4])}")
    location_fields = _unique_preserving_order(
        [
            *(semantic_map.get("state") or []),
            *(semantic_map.get("city") or []),
            *(semantic_map.get("zone") or []),
            *(semantic_map.get("branch") or []),
        ]
    )
    if location_fields:
        semantic_parts.append(f"location fields={', '.join(location_fields[:4])}")
    semantic_text = "; ".join(semantic_parts[:4]) if semantic_parts else "semantic fields not detected"

    return (
        f"{_source_display_name(source)} {dataset_type} dataset profile: "
        f"rows={row_count:,}; columns total={len(columns):,}; columns sample={col_preview}; "
        f"detected dimensions={dim_text}; pricing field={price_text}; "
        f"quantity field={qty_text}; cost/margin related field={cost_text}; "
        f"{semantic_text}."
    )


def _build_chatbot_global_context(
    *,
    db: Session,
    payload: ChatbotPayload,
    from_date: str | None,
    to_date: str | None,
    job_id: str | None,
    dataset_type: str = "all",
    source_origin: str = "none",
) -> tuple[str, dict[str, Any]]:
    scopes = _chatbot_available_scopes(db=db, job_id=job_id)
    selected_dataset = dataset_type if dataset_type in {"sales", "claims"} else "all"
    date_label = (
        f"{from_date or 'n/a'} to {to_date or 'n/a'}"
        if (from_date or to_date)
        else "all available data"
    )

    context_payload: dict[str, Any] = {
        "source": "",
        "source_label": "All Sources",
        "dataset_type": selected_dataset,
        "source_origin": source_origin,
        "job_id": job_id,
        "from_date": from_date,
        "to_date": to_date,
        "rankings": [],
        "allowed_labels": sorted({_source_display_name(str(scope.get("source", ""))) for scope in scopes}),
        "global_scope": True,
        "ui_context": payload.ui_context if isinstance(payload.ui_context, dict) else {},
        "sales_summary": {},
        "claims_summary": {},
    }

    context_lines = [
        "Scope mode: cross-source analytics context using dashboard summaries plus underlying dataset records.",
        f"Selected dataset: {selected_dataset}",
        f"Selected date range: {date_label}",
    ]
    if job_id:
        context_lines.append(f"Selected job tag: {job_id}")
    ui_context_line = _summarize_ui_context(payload.ui_context)
    if ui_context_line:
        context_lines.append(ui_context_line)

    if not scopes:
        context_lines.append("No rows are available in data_rows for the current filters.")
        return "\n".join(context_lines), context_payload

    total_rows = sum(int(scope.get("row_count", 0) or 0) for scope in scopes)
    context_lines.append(
        f"Available source/dataset slices: {len(scopes)} (total rows: {total_rows:,})."
    )
    context_lines.append(
        "Slices: "
        + "; ".join(
            f"{_source_display_name(str(scope.get('source', '')))} {scope.get('dataset_type')} ({int(scope.get('row_count', 0) or 0):,} rows)"
            for scope in scopes[:12]
        )
    )
    if any(_is_samsung_source(str(scope.get("source", ""))) for scope in scopes):
        context_lines.append(_samsung_model_mapping_context_line())
        context_lines.extend(_samsung_plan_reference_context_lines())

    sales_totals = {
        "gross_premium": 0.0,
        "earned_premium": 0.0,
        "zopper_earned_premium": 0.0,
        "units_sold": 0.0,
    }
    claims_totals = {
        "gross_premium": 0.0,
        "earned_premium": 0.0,
        "units_sold": 0.0,
    }
    summary_lines: list[str] = []
    dataset_profile_lines: list[str] = []
    for scope in scopes[:12]:
        source = str(scope.get("source", ""))
        dataset_type = str(scope.get("dataset_type", ""))
        if dataset_type not in {"sales", "claims"}:
            continue

        summary = _resolve_summary_for_scope(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
        if not summary:
            continue

        if dataset_type == "claims":
            total_claims_cost = float(summary.get("gross_premium", 0) or 0)
            net_claims_cost = float(summary.get("earned_premium", 0) or 0)
            claims_count = float(summary.get("units_sold", 0) or 0)
            claims_totals["gross_premium"] += total_claims_cost
            claims_totals["earned_premium"] += net_claims_cost
            claims_totals["units_sold"] += claims_count
            summary_lines.append(
                f"{_source_display_name(source)} claims summary: "
                f"Total Claims Cost={_format_metric_value('claims', total_claims_cost)}; "
                f"Net Claims Cost Paid={_format_metric_value('net_claims', net_claims_cost)}; "
                f"No. of Claims={int(claims_count):,}"
            )
        else:
            gross_premium = float(summary.get("gross_premium", 0) or 0)
            earned_premium = float(summary.get("earned_premium", 0) or 0)
            zopper_earned_premium = float(summary.get("zopper_earned_premium", 0) or 0)
            units_sold = float(summary.get("units_sold", 0) or 0)
            sales_totals["gross_premium"] += gross_premium
            sales_totals["earned_premium"] += earned_premium
            sales_totals["zopper_earned_premium"] += zopper_earned_premium
            sales_totals["units_sold"] += units_sold
            summary_lines.append(
                f"{_source_display_name(source)} sales summary: "
                f"Gross Premium={_format_metric_value('gross_premium', gross_premium)}; "
                f"Earned Premium={_format_metric_value('earned_premium', earned_premium)}; "
                f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', zopper_earned_premium)}; "
                f"Units Sold={int(units_sold):,}"
            )

        if len(dataset_profile_lines) < 4:
            profile_line = _build_dataset_field_profile(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )
            if profile_line:
                dataset_profile_lines.append(profile_line)

    if summary_lines:
        context_lines.extend(summary_lines)
    else:
        context_lines.append(
            "Summary metrics were not precomputed for these slices, but row-level records are available in data_rows."
        )
    if dataset_profile_lines:
        context_lines.extend(dataset_profile_lines)

    if any(value > 0 for value in sales_totals.values()):
        context_payload["sales_summary"] = dict(sales_totals)
        context_lines.append(
            "All-sources sales total: "
            f"Gross Premium={_format_metric_value('gross_premium', sales_totals['gross_premium'])}; "
            f"Earned Premium={_format_metric_value('earned_premium', sales_totals['earned_premium'])}; "
            f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', sales_totals['zopper_earned_premium'])}; "
            f"Units Sold={int(sales_totals['units_sold']):,}"
        )
    if any(value > 0 for value in claims_totals.values()):
        context_payload["claims_summary"] = dict(claims_totals)
        context_lines.append(
            "All-sources claims total: "
            f"Total Claims Cost={_format_metric_value('claims', claims_totals['gross_premium'])}; "
            f"Net Claims Cost Paid={_format_metric_value('net_claims', claims_totals['earned_premium'])}; "
            f"No. of Claims={int(claims_totals['units_sold']):,}"
        )

    return "\n".join(context_lines), context_payload


def _build_chatbot_greeting_response(
    *,
    db: Session,
    payload: ChatbotPayload,
) -> str:
    job_id = _normalize_chatbot_job_id(payload.job_id)
    scopes = _chatbot_available_scopes(db=db, job_id=job_id)
    if not scopes:
        return (
            "Hi. I'm AI Sahyogi and I can analyze dashboard data, but I don't see any rows in the database right now. "
            "Upload or sync data, then ask about trends, anomalies, or actions."
        )

    total_rows = sum(int(scope.get("row_count", 0) or 0) for scope in scopes)
    preview = "; ".join(
        f"{_source_display_name(str(scope.get('source', '')))} {scope.get('dataset_type')} ({int(scope.get('row_count', 0) or 0):,} rows)"
        for scope in scopes[:5]
    )
    return (
        "Hi. I'm AI Sahyogi and I can analyze dashboard metrics plus underlying dataset records across available sources. "
        f"Current coverage: {len(scopes)} source/dataset slices, {total_rows:,} rows total. "
        f"Examples: {preview}. "
        "Ask any business question and I will answer from the available analytics data."
    )


def _pick_present_key(rows: list[dict[str, Any]], candidates: list[str]) -> str | None:
    if not rows:
        return None
    safe_to_raw: dict[str, str] = {}
    for row in rows[:12]:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            safe_to_raw[_to_safe_key(str(key))] = str(key)

    for candidate in candidates:
        safe_candidate = _to_safe_key(candidate)
        if safe_candidate in safe_to_raw:
            return safe_to_raw[safe_candidate]
    return None


def _guess_numeric_key(rows: list[dict[str, Any]], exclude_keys: set[str]) -> str | None:
    score: dict[str, int] = {}
    for row in rows[:120]:
        if not isinstance(row, dict):
            continue
        for key, raw in row.items():
            if key in exclude_keys:
                continue
            if _to_number(raw) is not None:
                score[key] = score.get(key, 0) + 1
    if not score:
        return None
    return max(score, key=lambda k: score[k])


def _rank_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    metric: str,
) -> dict[str, Any] | None:
    if not rows:
        return None

    dimension_key = _pick_present_key(rows, [dimension])
    metric_key = _pick_present_key(rows, [metric])
    if metric_key is None:
        metric_key = _guess_numeric_key(rows, {dimension_key} if dimension_key else set())
    if metric_key is None:
        return None

    totals: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _to_number(row.get(metric_key))
        if value is None:
            continue

        label = ""
        if dimension_key is not None:
            label = str(row.get(dimension_key, "")).strip()
        if not label:
            for key, raw in row.items():
                if key == metric_key:
                    continue
                if _to_number(raw) is None:
                    label = str(raw or "").strip()
                    if label:
                        break
        if not label:
            continue

        if label.lower() in {"nan", "none", "null"}:
            continue
        totals[label] = totals.get(label, 0.0) + float(value)

    if not totals:
        return None

    ordered_desc = sorted(totals.items(), key=lambda pair: pair[1], reverse=True)
    ordered_asc = sorted(totals.items(), key=lambda pair: pair[1])
    return {
        "dimension": dimension,
        "metric": metric,
        "top": ordered_desc[:4],
        "bottom": ordered_asc[:3],
        "labels": list(totals.keys())[:40],
    }


def _chatbot_dimension_candidates(source: str) -> list[str]:
    source_key = _normalize_source_key(source)
    if source_key == "reliance":
        return ["brand", "device_plan_category", "plan_category", "state", "month"]
    if source_key in {"godrej", "hitachi"}:
        return ["product_category", "product_subcategory", "plan_category", "channel", "state", "month", "reason"]
    return ["brand", "plan_category", "device_plan_category", "state", "month"]


def _chatbot_metric_candidates(dataset_type: str) -> list[str]:
    if dataset_type == "claims":
        return ["loss_ratio", "claims", "net_claims", "quantity"]
    return ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity"]


def _format_rank_pairs(metric: str, pairs: list[tuple[str, float]]) -> str:
    return "; ".join(f"{label} ({_format_metric_value(metric, value)})" for label, value in pairs)


def _chatbot_scope_label_for_answer(context_payload: dict[str, Any]) -> str:
    if bool(context_payload.get("global_scope")):
        return "all available partners"
    return str(context_payload.get("source_label") or "the selected source")


def _chatbot_range_label(context_payload: dict[str, Any]) -> str:
    from_date = _normalize_chatbot_date(context_payload.get("from_date"))
    to_date = _normalize_chatbot_date(context_payload.get("to_date"))
    if from_date or to_date:
        return f"{from_date or 'start'} to {to_date or 'latest'}"
    return "all available data"


def _chatbot_summary_for_context(context_payload: dict[str, Any]) -> dict[str, Any]:
    dataset_type = str(context_payload.get("dataset_type") or "sales").strip().lower()
    if bool(context_payload.get("global_scope")):
        if dataset_type == "claims":
            summary = context_payload.get("claims_summary") or {}
        else:
            summary = context_payload.get("sales_summary") or {}
    else:
        summary = context_payload.get("summary") or {}
    return summary if isinstance(summary, dict) else {}


def _chatbot_summary_metric_answer(
    *,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    dataset_type = str(context_payload.get("dataset_type") or "sales").strip().lower()
    summary = _chatbot_summary_for_context(context_payload)
    if not summary:
        return None

    metric_specs = _resolve_chart_metric_specs(payload.message, dataset_type)
    if not metric_specs:
        return None

    metric = str(metric_specs[0].get("metric") or "").strip().lower()
    scope_label = _chatbot_scope_label_for_answer(context_payload)
    range_label = _chatbot_range_label(context_payload)

    value: float | None = None
    label = str(metric_specs[0].get("label") or _pretty_label(metric) or "Metric").strip()
    format_metric = metric

    if dataset_type == "claims":
        if metric in {"claims", "gross_premium"}:
            value = float(summary.get("gross_premium", 0) or 0)
            label = "Total Claims Cost"
            format_metric = "claims"
        elif metric in {"net_claims", "earned_premium"}:
            value = float(summary.get("earned_premium", 0) or 0)
            label = "Net Claims Cost Paid"
            format_metric = "net_claims"
        elif metric in {"quantity", "count", "units_sold"}:
            value = float(summary.get("units_sold", 0) or 0)
            label = "No. of Claims"
            format_metric = "quantity"
        else:
            return None
    else:
        if metric == "gross_premium":
            value = float(summary.get("gross_premium", 0) or 0)
            label = "Gross Premium"
            format_metric = "gross_premium"
        elif metric == "earned_premium":
            value = float(summary.get("earned_premium", 0) or 0)
            label = "Earned Premium"
            format_metric = "earned_premium"
        elif metric == "zopper_earned_premium":
            value = float(summary.get("zopper_earned_premium", 0) or 0)
            label = "Zopper Earned Premium"
            format_metric = "zopper_earned_premium"
        elif metric in {"quantity", "count", "units_sold"}:
            value = float(summary.get("units_sold", 0) or 0)
            label = "Units Sold"
            format_metric = "quantity"
        else:
            return None

    if value is None:
        return None

    if format_metric == "quantity":
        formatted_value = f"{int(round(value)):,}"
    else:
        formatted_value = _format_metric_value(format_metric, value)

    supporting_bits: list[str] = []
    if dataset_type == "sales":
        if format_metric != "earned_premium":
            supporting_bits.append(
                f"Earned Premium is {_format_metric_value('earned_premium', float(summary.get('earned_premium', 0) or 0))}."
            )
        if format_metric != "zopper_earned_premium":
            supporting_bits.append(
                f"Zopper Earned Premium is {_format_metric_value('zopper_earned_premium', float(summary.get('zopper_earned_premium', 0) or 0))}."
            )
        if format_metric != "quantity":
            supporting_bits.append(
                f"Units Sold are {int(round(float(summary.get('units_sold', 0) or 0))):,}."
            )
    else:
        if format_metric != "net_claims":
            supporting_bits.append(
                f"Net Claims Cost Paid is {_format_metric_value('net_claims', float(summary.get('earned_premium', 0) or 0))}."
            )
        if format_metric != "quantity":
            supporting_bits.append(
                f"No. of Claims are {int(round(float(summary.get('units_sold', 0) or 0))):,}."
            )

    return (
        f"For {scope_label} {dataset_type} in {range_label}, {label} is {formatted_value}. "
        + " ".join(supporting_bits[:3])
    ).strip()


def _chatbot_rankings_fallback_answer(
    *,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    rankings = context_payload.get("rankings") or []
    if not rankings:
        return None

    requested_dimensions = _chatbot_requested_dimensions_from_text(payload.message)
    snapshot: dict[str, Any] | None = None
    for dimension in requested_dimensions:
        snapshot = next((row for row in rankings if row.get("dimension") == dimension), None)
        if snapshot:
            break
    if snapshot is None:
        snapshot = rankings[0] if rankings else None
    if snapshot is None:
        return None

    top = snapshot.get("top", []) or []
    bottom = snapshot.get("bottom", []) or []
    if not top and not bottom:
        return None

    scope_label = _chatbot_scope_label_for_answer(context_payload)
    range_label = _chatbot_range_label(context_payload)
    metric = str(snapshot.get("metric") or "gross_premium")
    dimension = str(snapshot.get("dimension") or "dimension")

    lines = [
        f"Fast dashboard fallback for {scope_label} in {range_label}: {_pretty_label(dimension)} by {_pretty_label(metric)}."
    ]
    if top:
        lines.append(f"Top segments: {_format_rank_pairs(metric, top)}.")
    if bottom:
        lines.append(f"Lowest segments: {_format_rank_pairs(metric, bottom)}.")
    return " ".join(lines)


def _is_direct_summary_metric_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    if any(
        token in low
        for token in (
            "trend",
            "forecast",
            "predict",
            "graph",
            "chart",
            "breakdown",
            "statewise",
            "monthwise",
            "citywise",
            "compare",
            "vs ",
            " versus ",
            "why",
            "reason",
            "underperform",
            "recommend",
        )
    ):
        return False

    metric_tokens = (
        "gross premium",
        "earned premium",
        "zopper earned",
        "quantity",
        "units sold",
        "sales count",
        "claims cost",
        "net claim",
        "claim count",
        "no. of claims",
        "number of claims",
    )
    lookup_tokens = (
        "what is",
        "what are",
        "how much",
        "how many",
        "total",
        "overall",
        "current",
        "summary",
    )
    return any(token in low for token in metric_tokens) and any(token in low for token in lookup_tokens)


def _build_chatbot_service_fallback_response(
    *,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
    error_detail: str = "",
) -> dict[str, str]:
    direct_answer = _chatbot_summary_metric_answer(payload=payload, context_payload=context_payload)
    ranking_answer = _chatbot_rankings_fallback_answer(payload=payload, context_payload=context_payload)

    response_text = direct_answer or ranking_answer
    if not response_text:
        response_text = (
            f"I could not use the live language model right now, but I still have dashboard context for "
            f"{_chatbot_scope_label_for_answer(context_payload)} in {_chatbot_range_label(context_payload)}. "
            "Ask for a specific metric like gross premium, earned premium, quantity, claims cost, or a breakdown by state, month, plan, or channel."
        )
    elif error_detail:
        response_text += " I answered from cached dashboard analytics because the live LLM service is currently unavailable."

    return {
        "response": _prepend_partner_scope_prompt(
            response_text,
            payload=payload,
            context_payload=context_payload,
        ),
        "model": "rule-based-fallback",
        "message": error_detail.strip() or "LLM unavailable; answered from dashboard analytics.",
    }


def _chatbot_graph_rows(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    from_date: str | None,
    to_date: str | None,
    allow_live_fallback: bool = True,
) -> list[dict[str, Any]]:
    rows = get_precomputed_graph(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        from_date=from_date,
        to_date=to_date,
    )
    if rows is None and (from_date or to_date):
        rows = get_precomputed_graph(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
        )

    should_try_live = allow_live_fallback and not rows
    if should_try_live:
        try:
            rows = compute_by_dimension_rows(
                db=db,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                source=source,
                dataset_type=dataset_type,
                from_date=from_date,
                to_date=to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot live dimension fetch failed source=%s dataset=%s dimension=%s metric=%s",
                source,
                dataset_type,
                dimension,
                metric,
            )
            rows = []

    return rows or []


def _build_chatbot_dashboard_context(
    *,
    db: Session,
    payload: ChatbotPayload,
) -> tuple[str, dict[str, Any]]:
    source, source_origin = _resolve_chatbot_source_with_origin(payload)
    dataset_type = _resolve_chatbot_dataset_type(payload)
    from_date, to_date = _normalize_chatbot_date_range(payload.from_date, payload.to_date)
    job_id = _normalize_chatbot_job_id(payload.job_id)
    message_requests_global_scope = _requests_global_scope(payload.message)
    explicit_global_scope = bool(payload.global_scope)
    should_use_global_scope = bool(
        message_requests_global_scope
        or not source
        or (explicit_global_scope and source_origin in {"none", "payload"})
    )

    context_payload: dict[str, Any] = {
        "source": source,
        "source_label": _source_display_name(source),
        "dataset_type": dataset_type,
        "source_origin": source_origin,
        "job_id": job_id,
        "from_date": from_date,
        "to_date": to_date,
        "rankings": [],
        "allowed_labels": [],
        "requested_dimensions": [],
        "global_scope": should_use_global_scope,
        "ui_context": payload.ui_context if isinstance(payload.ui_context, dict) else {},
        "summary": {},
    }

    if should_use_global_scope:
        return _build_chatbot_global_context(
            db=db,
            payload=payload,
            from_date=from_date,
            to_date=to_date,
            job_id=job_id,
            dataset_type=dataset_type,
            source_origin=source_origin,
        )

    context_payload["summary"] = _resolve_summary_for_scope(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    summary = context_payload["summary"] if isinstance(context_payload["summary"], dict) else {}

    metric_candidates = _chatbot_metric_candidates(dataset_type)
    requested_dimensions = _chatbot_requested_dimensions(payload)
    prioritized_dimensions = _prioritize_dimensions(
        _chatbot_dimension_candidates(source),
        requested_dimensions,
    )
    if requested_dimensions:
        dimension_candidates = prioritized_dimensions[: max(1, min(3, len(requested_dimensions) + 1))]
    else:
        dimension_candidates = prioritized_dimensions[:3]
    rankings: list[dict[str, Any]] = []
    allowed_labels: set[str] = set()
    context_payload["requested_dimensions"] = requested_dimensions

    max_rankings = 5
    required_dimensions = set(requested_dimensions)
    for dimension in dimension_candidates:
        snapshot: dict[str, Any] | None = None
        for metric in metric_candidates:
            graph_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                from_date=from_date,
                to_date=to_date,
                allow_live_fallback=False,
            )
            snapshot = _rank_dimension_rows(graph_rows, dimension=dimension, metric=metric)
            if snapshot:
                break
        if snapshot:
            rankings.append(snapshot)
            for label in snapshot.get("labels", [])[:12]:
                if label:
                    allowed_labels.add(str(label))
        if len(rankings) >= max_rankings:
            ranking_dims = {str(item.get("dimension") or "") for item in rankings}
            if required_dimensions.issubset(ranking_dims):
                break

    context_payload["rankings"] = rankings
    context_payload["allowed_labels"] = sorted(allowed_labels)

    date_label = (
        f"{from_date or 'n/a'} to {to_date or 'n/a'}"
        if (from_date or to_date)
        else "all available data"
    )

    context_lines = [
        f"Selected source: {_source_display_name(source)}",
        f"Selected dataset: {dataset_type}",
        f"Selected date range: {date_label}",
    ]
    ui_context_line = _summarize_ui_context(payload.ui_context)
    if ui_context_line:
        context_lines.append(ui_context_line)
    if _is_samsung_source(source):
        context_lines.append(_samsung_model_mapping_context_line())
        context_lines.extend(_samsung_plan_reference_context_lines())
    if job_id:
        context_lines.append(f"Selected job tag: {job_id}")

    if dataset_type == "claims":
        context_lines.append(
            "Summary metrics: "
            f"Total Claims Cost={_format_metric_value('claims', float(summary.get('gross_premium', 0) or 0))}; "
            f"Net Claims Cost Paid={_format_metric_value('net_claims', float(summary.get('earned_premium', 0) or 0))}; "
            f"No. of Claims={int(float(summary.get('units_sold', 0) or 0)):,}"
        )
    else:
        context_lines.append(
            "Summary metrics: "
            f"Gross Premium={_format_metric_value('gross_premium', float(summary.get('gross_premium', 0) or 0))}; "
            f"Earned Premium={_format_metric_value('earned_premium', float(summary.get('earned_premium', 0) or 0))}; "
            f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', float(summary.get('zopper_earned_premium', 0) or 0))}; "
            f"Units Sold={int(float(summary.get('units_sold', 0) or 0)):,}"
        )

    profile_line = _build_dataset_field_profile(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    if profile_line:
        context_lines.append(profile_line)

    if rankings:
        for snapshot in rankings:
            top = snapshot.get("top", [])
            bottom = snapshot.get("bottom", [])
            if not top and not bottom:
                continue
            top_text = _format_rank_pairs(snapshot["metric"], top) if top else "n/a"
            bottom_text = _format_rank_pairs(snapshot["metric"], bottom) if bottom else "n/a"
            context_lines.append(
                f"{_pretty_label(snapshot['dimension'])} by {_pretty_label(snapshot['metric'])}: "
                f"Top={top_text} | Bottom={bottom_text}"
            )
    else:
        context_lines.append("No ranked dimension rows were available for this slice.")

    if allowed_labels:
        context_lines.append(f"Allowed entity labels: {', '.join(sorted(allowed_labels)[:16])}")

    return "\n".join(context_lines), context_payload


def _is_underperformance_query(message: str) -> bool:
    low = (message or "").strip().lower()
    if not low:
        return False
    under_tokens = ("underperform", "under performing", "under-performing", "lagging", "weakest", "worst", "lowest")
    return any(token in low for token in under_tokens)


def _is_dimension_stats_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    stat_tokens = (
        "stats",
        "statistics",
        "breakdown",
        "distribution",
        "wise",
        "statewise",
        "monthwise",
        "citywise",
        "by state",
        "by month",
        "by city",
        "by channel",
    )
    return any(token in low for token in stat_tokens)


def _build_dimension_stats_answer(message: str, context_payload: dict[str, Any]) -> str | None:
    if not _is_dimension_stats_query(message):
        return None

    rankings = context_payload.get("rankings") or []
    if not rankings:
        return None

    requested_dimensions = _chatbot_requested_dimensions_from_text(message)
    if not requested_dimensions:
        return None

    snapshot: dict[str, Any] | None = None
    requested_dimension = requested_dimensions[0]
    for dimension in requested_dimensions:
        snapshot = next((row for row in rankings if row.get("dimension") == dimension), None)
        if snapshot:
            requested_dimension = dimension
            break

    source_label = context_payload.get("source_label") or "the selected source"
    dataset_type = context_payload.get("dataset_type") or "sales"
    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")

    if snapshot is None:
        return (
            f"I can’t confirm {_pretty_label(requested_dimension).lower()}-wise statistics from the current dashboard data "
            f"for {source_label}."
        )

    top = snapshot.get("top") or []
    bottom = snapshot.get("bottom") or []
    if not top and not bottom:
        return (
            f"I can’t confirm {_pretty_label(requested_dimension).lower()}-wise statistics from the current dashboard data "
            f"for {source_label}."
        )

    metric = snapshot.get("metric") or "gross_premium"
    dimension = snapshot.get("dimension") or requested_dimension
    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    top_text = _format_rank_pairs(metric, top[:4]) if top else "n/a"
    low_text = _format_rank_pairs(metric, bottom[:3]) if bottom else "n/a"
    return (
        f"In {source_label} {dataset_type}{range_suffix}, {_pretty_label(dimension).lower()}-wise "
        f"{_pretty_label(metric).lower()} snapshot: Top segments are {top_text}. "
        f"Lowest segments are {low_text}."
    )


def _build_underperformance_answer(message: str, context_payload: dict[str, Any]) -> str | None:
    if not _is_underperformance_query(message):
        return None

    rankings = context_payload.get("rankings") or []
    if not rankings:
        return None

    low_message = (message or "").lower()
    requested_dimensions = _chatbot_requested_dimensions_from_text(message)
    strict_dimension: str | None = requested_dimensions[0] if requested_dimensions else None
    wants_brand = "brand" in low_message or strict_dimension == "brand"

    default_dimensions = (
        "device_plan_category",
        "plan_category",
        "product_category",
        "channel",
        "state",
        "month",
    )
    if wants_brand:
        default_dimensions = ("brand",) + default_dimensions

    preferred_dimensions = tuple(
        _prioritize_dimensions(
            list(default_dimensions),
            requested_dimensions,
        )
    )

    snapshot: dict[str, Any] | None = None
    for dim in preferred_dimensions:
        snapshot = next((row for row in rankings if row.get("dimension") == dim), None)
        if snapshot:
            break

    if snapshot is None and strict_dimension:
        source_label = context_payload.get("source_label") or "the selected source"
        return (
            f"I can’t confirm {_pretty_label(strict_dimension).lower()}-level underperformance from the current dashboard data "
            f"for {source_label}."
        )
    if snapshot is None and wants_brand:
        source_label = context_payload.get("source_label") or "the selected source"
        return f"I can’t confirm brand-level underperformance from the current dashboard data for {source_label}."
    if snapshot is None:
        snapshot = rankings[0]

    bottom = snapshot.get("bottom") or []
    if not bottom:
        return None

    lowest_label, lowest_value = bottom[0]
    source_label = context_payload.get("source_label") or "the selected source"
    dataset_type = context_payload.get("dataset_type") or "sales"
    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    metric = snapshot.get("metric") or "gross_premium"
    dimension = snapshot.get("dimension") or "category"

    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    answer = (
        f"In {source_label} {dataset_type}{range_suffix}, the lowest {_pretty_label(metric).lower()} "
        f"across {_pretty_label(dimension).lower()} is {lowest_label} at {_format_metric_value(metric, float(lowest_value))}. "
    )
    if len(bottom) > 1:
        next_label, next_value = bottom[1]
        answer += (
            f"The next lowest is {next_label} at {_format_metric_value(metric, float(next_value))}. "
        )
    answer += "This is the current underperformer in the dashboard slice."
    return answer


_SAMSUNG_PLAN_REFERENCE_LINES: tuple[str, ...] = (
    "Samsung plan glossary: ADLD = Accidental Damage and Liquid Damage; SP/SPP = Screen Protection Plan; EW = Extended Warranty; CPP = Comprehensive Protection Plan; Combo = ADLD + EW.",
    "Samsung products/devices covered: smartphones, tablets, laptops, and smartwatches (subject to Samsung terms and channel eligibility in India).",
    "Coverage summary: ADLD covers accidental/liquid damage; SPP covers screen/display damage; EW covers mechanical and electrical breakdown; CPP covers accidental damage plus mechanical/electrical breakdown.",
    "Samsung fixed Device Plan Category x Plan Category matrix is treated as Zopper Share reference (used for zopper earned premium derivation when share columns are missing).",
    "Samsung gross premium and earned premium basis uses plan sold price fields (Plan Selling Price/Plan MRP/Amount mapped to gross premium columns).",
    "Claims process summary: login via registered mobile OTP on Samsung unified portal, open Raise Claim for active policy, submit issue/carry-in details, choose service center and visit slot, pay processing fee where applicable, then receive claim ID.",
)


def _samsung_plan_reference_context_lines() -> list[str]:
    return list(_SAMSUNG_PLAN_REFERENCE_LINES)


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower())).strip()


def _extract_location_query_token(message: str, context_payload: dict[str, Any]) -> str | None:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return None

    patterns = (
        r"\bin\s+([a-z][a-z\s\-]{1,40}?)(?=\s*,\s*in\b|\s+in\s+the\s+month\b|\s+in\s+month\b|\s+during\b|\s+for\b|\s+on\b|[?.!,]|$)",
        r"\bfor\s+([a-z][a-z\s\-]{1,40}?)(?=\s*,|\s+in\s+the\s+month\b|\s+in\s+month\b|\s+during\b|\s+on\b|[?.!,]|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, low)
        if not match:
            continue
        token = _normalize_lookup_text(match.group(1))
        if token and token not in {"month", "claims", "claim", "state", "city"}:
            return token

    allowed_labels = [str(label or "") for label in (context_payload.get("allowed_labels") or [])]
    for label in sorted(allowed_labels, key=lambda item: len(item), reverse=True):
        normalized_label = _normalize_lookup_text(label)
        if not normalized_label:
            continue
        pattern = r"\b" + re.escape(normalized_label).replace(r"\ ", r"\s+") + r"\b"
        if re.search(pattern, low):
            return normalized_label
    return None


def _extract_month_window_from_text(message: str) -> tuple[str, str, str] | None:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return None

    month_pattern = (
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\s*[,/\-]?\s*(\d{2}|\d{4})\b"
    )
    match = re.search(month_pattern, low)
    if not match:
        return None

    month_key = match.group(1)[:3]
    month_num = _FORECAST_MONTH_MAP.get(month_key)
    if month_num is None:
        return None

    year_raw = match.group(2)
    year = int(year_raw) + 2000 if len(year_raw) == 2 else int(year_raw)
    if year < 1900 or year > 2200:
        return None

    start_dt = date(year, month_num, 1)
    if month_num == 12:
        end_dt = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(year, month_num + 1, 1) - timedelta(days=1)
    return start_dt.isoformat(), end_dt.isoformat(), start_dt.strftime("%B %Y")


def _present_frame_columns(frame: Any, candidates: list[str]) -> list[str]:
    if frame is None or getattr(frame, "empty", True):
        return []
    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        return []

    safe_candidates = {_to_safe_key(candidate) for candidate in candidates}
    out: list[str] = []
    for col in columns:
        if _to_safe_key(col) in safe_candidates:
            out.append(col)
    return out


def _location_mask_for_column(frame: Any, column: str, location_token: str) -> tuple[list[bool], int]:
    if frame is None or getattr(frame, "empty", True):
        return [], 0

    needle = _normalize_lookup_text(location_token)
    if not needle:
        return [], 0

    try:
        raw_values = frame[column].tolist()
    except Exception:
        return [], 0

    boundary_pattern = re.compile(r"(?:^| )" + re.escape(needle) + r"(?: |$)")
    mask: list[bool] = []
    count = 0
    for raw in raw_values:
        normalized = _normalize_lookup_text(str(raw or ""))
        matched = (
            bool(normalized)
            and (
                normalized == needle
                or bool(boundary_pattern.search(normalized))
                or needle in normalized
            )
        )
        mask.append(bool(matched))
        if matched:
            count += 1
    return mask, count


def _match_frame_by_location(
    frame: Any,
    location_token: str,
) -> tuple[Any, str, bool, bool]:
    if frame is None or getattr(frame, "empty", True):
        return frame, "", False, False

    city_columns = _present_frame_columns(
        frame,
        [
            "city",
            "customer_city",
            "customer city",
            "city_name",
            "location",
            "district",
            "state_city",
            "state/city",
            "state / city",
        ],
    )
    state_columns = _present_frame_columns(
        frame,
        [
            "state",
            "customer_state",
            "customer state",
            "state_name",
            "region",
            "ut",
            "union territory",
            "state_city",
            "state/city",
            "state / city",
        ],
    )
    city_present = bool(city_columns)
    state_present = bool(state_columns)

    best_city_mask: list[bool] | None = None
    best_city_count = 0
    for col in city_columns:
        mask, count = _location_mask_for_column(frame, col, location_token)
        if count > best_city_count:
            best_city_mask = mask
            best_city_count = count
    if best_city_mask is not None and best_city_count > 0:
        return frame[best_city_mask].copy(), "city", city_present, state_present

    best_state_mask: list[bool] | None = None
    best_state_count = 0
    for col in state_columns:
        mask, count = _location_mask_for_column(frame, col, location_token)
        if count > best_state_count:
            best_state_mask = mask
            best_state_count = count
    if best_state_mask is not None and best_state_count > 0:
        return frame[best_state_mask].copy(), "state", city_present, state_present

    return frame.iloc[0:0].copy(), "", city_present, state_present


def _is_claim_average_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    if "claim" not in low:
        return False
    return any(token in low for token in ("average", "avg", "mean"))


def _build_claim_average_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_claim_average_query(payload.message):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    if dataset_type != "claims":
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    global_scope = bool(context_payload.get("global_scope"))

    source_candidates: list[str] = []
    if global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        source_candidates = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == "claims"
        ]
    else:
        source = str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip()
        if source:
            source_candidates = [source]

    source_candidates = [src for src in source_candidates if src]
    if not source_candidates:
        return None

    month_window = _extract_month_window_from_text(payload.message)
    location_token = _extract_location_query_token(payload.message, context_payload)

    total_net_claims = 0.0
    total_claim_count = 0.0
    total_rows = 0
    city_match_hits = 0
    state_match_hits = 0
    city_columns_seen = False
    state_columns_seen = False

    for source in source_candidates:
        try:
            frame = get_dataframe(
                db=db,
                job_id=job_id,
                source=source,
                dataset_type="claims",
            )
        except Exception:
            logger.exception(
                "Chatbot claims average fetch failed source=%s job_id=%s",
                source,
                job_id,
            )
            continue

        if frame is None or getattr(frame, "empty", True):
            continue

        try:
            scoped = frame.copy()
        except Exception:
            continue

        if from_date or to_date:
            scoped = filter_by_date_range(scoped, "claims", from_date, to_date)
        if month_window is not None:
            scoped = filter_by_date_range(scoped, "claims", month_window[0], month_window[1])
        if scoped is None or getattr(scoped, "empty", True):
            continue

        if location_token:
            scoped, match_level, city_present, state_present = _match_frame_by_location(scoped, location_token)
            city_columns_seen = city_columns_seen or city_present
            state_columns_seen = state_columns_seen or state_present
            if match_level == "city":
                city_match_hits += 1
            elif match_level == "state":
                state_match_hits += 1
            if scoped is None or getattr(scoped, "empty", True):
                continue

        net_claims = _sum_metric_from_dataframe(
            scoped,
            [
                "net_claims",
                "net_claim",
                "net_amount",
                "claims",
                "claim_amount",
                "zoppers_cost",
                "amount",
                "earned_premium",
                "gross_premium",
            ],
        )
        claim_count = _sum_metric_from_dataframe(
            scoped,
            [
                "quantity",
                "claims_count",
                "no_of_claims",
                "count",
                "units_sold",
            ],
        )
        if claim_count <= 0:
            try:
                claim_count = float(len(scoped.index))
            except Exception:
                claim_count = 0.0
        if claim_count <= 0:
            continue

        total_net_claims += float(net_claims)
        total_claim_count += float(claim_count)
        try:
            total_rows += int(len(scoped.index))
        except Exception:
            pass

    if total_claim_count <= 0:
        period_label = month_window[2] if month_window else "the selected period"
        if location_token and (city_columns_seen or state_columns_seen):
            return _prepend_partner_scope_prompt(
                f"I can’t confirm average claim raised for {location_token.title()} in {period_label} from current matched rows. "
                "Recommendation: standardize city values and keep a city-level filter in claims data to close this gap.",
                payload=payload,
                context_payload=context_payload,
            )
        if location_token:
            return _prepend_partner_scope_prompt(
                f"I can’t confirm average claim raised for {location_token.title()} in {period_label} because city/state location fields are not consistently available in this claims slice. "
                "Recommendation: add a normalized city column and make it mandatory at claim intake.",
                payload=payload,
                context_payload=context_payload,
            )
        return _prepend_partner_scope_prompt(
            "I don’t have enough claims rows in the selected scope to compute a reliable average claim.",
            payload=payload,
            context_payload=context_payload,
        )

    avg_claim = total_net_claims / total_claim_count if total_claim_count > 0 else 0.0
    scope_label = (
        "all claims sources"
        if global_scope
        else str(context_payload.get("source_label") or _source_display_name(source_candidates[0]))
    )
    period_label = month_window[2] if month_window else (
        f"{from_date or 'start'} to {to_date or 'latest'}" if (from_date or to_date) else "all available data"
    )

    if location_token:
        answer = (
            f"Average net claim raised in {location_token.title()} for {period_label} in {scope_label} is "
            f"{_format_metric_value('net_claims', float(avg_claim))} per claim "
            f"(total net claims {_format_metric_value('net_claims', float(total_net_claims))} across {int(total_claim_count):,} claims)."
        )
    else:
        answer = (
            f"Average net claim for {period_label} in {scope_label} is "
            f"{_format_metric_value('net_claims', float(avg_claim))} per claim "
            f"(total net claims {_format_metric_value('net_claims', float(total_net_claims))} across {int(total_claim_count):,} claims)."
        )

    if location_token and state_match_hits > 0 and city_match_hits == 0:
        answer += " City-level match was unavailable, so this uses state/region-level matching."
    elif location_token and city_match_hits > 0:
        answer += " This is computed from city-level matched claims rows."

    if total_rows > 0:
        answer += f" Rows used: {total_rows:,}."
    return _prepend_partner_scope_prompt(
        answer,
        payload=payload,
        context_payload=context_payload,
    )


_SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY: dict[str, str] = {
    "A06": "Mass",
    "F15": "Mass",
    "A16": "Mid",
    "A17": "Mid",
    "F17": "Mid",
    "A26": "High",
    "A35": "High",
    "A36": "High",
    "F55": "High",
    "A56": "Premium",
    "S24": "Super Premium",
    "S25": "Super Premium",
    "Fold6": "Luxury Fold",
    "Fold7": "Luxury Fold",
    "Flip7": "Luxury Flip",
}

_SAMSUNG_MODEL_CODES_ORDERED: tuple[str, ...] = tuple(
    sorted(_SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.keys(), key=lambda token: (-len(token), token))
)

_SAMSUNG_DEVICE_CATEGORY_ORDER: tuple[str, ...] = (
    "Luxury Fold",
    "Luxury Flip",
    "Super Premium",
    "Premium",
    "High",
    "Mid",
    "Mass",
)

_SAMSUNG_PLAN_CATEGORY_ORDER: tuple[str, ...] = (
    "ADLD",
    "Screen Protection",
    "Combo",
    "Extended Warranty",
)

_SAMSUNG_REFERENCE_PLAN_PRICES: dict[tuple[str, str], int] = {
    ("Luxury Fold", "ADLD"): 5299,
    ("Luxury Fold", "Screen Protection"): 3999,
    ("Luxury Fold", "Combo"): 8587,
    ("Luxury Fold", "Extended Warranty"): 2060,
    ("Luxury Flip", "ADLD"): 4199,
    ("Luxury Flip", "Screen Protection"): 3748,
    ("Luxury Flip", "Combo"): 6800,
    ("Luxury Flip", "Extended Warranty"): 1737,
    ("Super Premium", "ADLD"): 2539,
    ("Super Premium", "Screen Protection"): 1174,
    ("Super Premium", "Combo"): 4694,
    ("Super Premium", "Extended Warranty"): 1064,
    ("Premium", "ADLD"): 1686,
    ("Premium", "Screen Protection"): 523,
    ("Premium", "Combo"): 2299,
    ("Premium", "Extended Warranty"): 410,
    ("High", "ADLD"): 799,
    ("High", "Screen Protection"): 260,
    ("High", "Combo"): 1399,
    ("High", "Extended Warranty"): 242,
    ("Mid", "ADLD"): 563,
    ("Mid", "Screen Protection"): 135,
    ("Mid", "Combo"): 806,
    ("Mid", "Extended Warranty"): 149,
    ("Mass", "ADLD"): 159,
    ("Mass", "Screen Protection"): 53,
    ("Mass", "Combo"): 267,
    ("Mass", "Extended Warranty"): 46,
}

_SAMSUNG_DEVICE_CATEGORY_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("Luxury Fold", ("luxury fold", "fold")),
    ("Luxury Flip", ("luxury flip", "flip")),
    ("Super Premium", ("super premium", "super-premium")),
    ("Premium", ("premium",)),
    ("High", ("high",)),
    ("Mid", ("mid",)),
    ("Mass", ("mass",)),
]

_SAMSUNG_PLAN_CATEGORY_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("ADLD", ("adld",)),
    ("Screen Protection", ("screen protection", "screen-protection")),
    ("Combo", ("combo",)),
    ("Extended Warranty", ("extended warranty", "extended-warranty", "warranty")),
]


def _detect_samsung_model_code_from_text(text: str) -> str | None:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for model_code in _SAMSUNG_MODEL_CODES_ORDERED:
        pattern = r"\b" + re.escape(model_code.lower()) + r"\b"
        if re.search(pattern, low):
            return model_code
    return None


def _samsung_model_mapping_context_line() -> str:
    pairs = "; ".join(
        f"{model_code}->{category}"
        for model_code, category in _SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.items()
    )
    return f"Samsung model-to-device-plan-category mapping: {pairs}."


def _contains_text_alias(text: str, alias: str) -> bool:
    pattern = r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b"
    return re.search(pattern, text) is not None


def _detect_samsung_device_category_from_text(text: str) -> str | None:
    model_code = _detect_samsung_model_code_from_text(text)
    if model_code:
        mapped = _SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.get(model_code)
        if mapped:
            return mapped

    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for category, aliases in _SAMSUNG_DEVICE_CATEGORY_ALIASES:
        for alias in aliases:
            if not _contains_text_alias(low, alias):
                continue
            if category in {"High", "Mid", "Mass"} and alias in {"high", "mid", "mass"}:
                if not re.search(r"\b(device|plan|category|segment|tier)\b", low):
                    continue
            return category
    return None


def _detect_samsung_plan_category_from_text(text: str) -> str | None:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for category, aliases in _SAMSUNG_PLAN_CATEGORY_ALIASES:
        for alias in aliases:
            if _contains_text_alias(low, alias):
                return category
    return None


def _is_samsung_source(source: str) -> bool:
    source_key = _normalize_source_key(source)
    return source_key == "samsung" or source_key in SAMSUNG_PARTNER_SOURCES


def _is_samsung_price_lookup_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False

    direct_lookup_tokens = ("price", "pricing", "cost", "rate", "amount", "mrp")
    has_lookup_intent = any(token in low for token in direct_lookup_tokens)
    if not has_lookup_intent and "how much" in low:
        has_lookup_intent = any(token in low for token in ("plan", "category", "price", "cost", "rate"))
    if not has_lookup_intent:
        return False

    uplift_tokens = ("increase", "hike", "raise", "uplift", "optimize", "optimise", "maximize")
    return not any(token in low for token in uplift_tokens)


def _format_rupee_int(value: int) -> str:
    return f"Rs {int(value):,}"


def _build_samsung_manual_price_answer(
    *,
    message: str,
    source_candidates: list[str],
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_samsung_price_lookup_query(message):
        return None

    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    selected_source_key = _normalize_source_key(str(context_payload.get("source") or ""))
    mentioned_source_key = _normalize_source_key(_detect_source_from_text(message) or "")
    mentions_samsung_family = bool(re.search(r"\b(samsung|croma|vijay)\b", low))

    in_samsung_scope = False
    if _is_samsung_source(selected_source_key) or _is_samsung_source(mentioned_source_key):
        in_samsung_scope = True
    elif mentions_samsung_family and any(_is_samsung_source(source) for source in source_candidates):
        in_samsung_scope = True

    if not in_samsung_scope:
        return None

    device_category = _detect_samsung_device_category_from_text(message)
    plan_category = _detect_samsung_plan_category_from_text(message)

    if device_category and plan_category:
        price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device_category, plan_category))
        if price is None:
            return None
        return (
            f"Samsung reference price for {device_category} in {plan_category} is {_format_rupee_int(price)}."
        )

    if device_category:
        lines = [f"Samsung reference prices for {device_category}:"]
        for plan in _SAMSUNG_PLAN_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device_category, plan))
            if price is None:
                continue
            lines.append(f"- {plan}: {_format_rupee_int(price)}")
        if len(lines) > 1:
            return "\n".join(lines)
        return None

    if plan_category:
        lines = [f"Samsung reference prices for {plan_category}:"]
        for device in _SAMSUNG_DEVICE_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device, plan_category))
            if price is None:
                continue
            lines.append(f"- {device}: {_format_rupee_int(price)}")
        if len(lines) > 1:
            return "\n".join(lines)
        return None

    lines = ["Samsung reference price matrix (Device Plan Category x Plan Category):"]
    for device in _SAMSUNG_DEVICE_CATEGORY_ORDER:
        chunks: list[str] = []
        for plan in _SAMSUNG_PLAN_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device, plan))
            if price is None:
                continue
            chunks.append(f"{plan} {_format_rupee_int(price)}")
        if chunks:
            lines.append(f"- {device}: {'; '.join(chunks)}")
    lines.append("These are fixed category reference prices configured for Samsung queries.")
    return "\n".join(lines)


def _is_pricing_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    price_tokens = ("price", "pricing", "cost", "rate", "amount", "increase", "hike", "raise", "uplift")
    has_price_intent = any(token in low for token in price_tokens)
    if not has_price_intent and "how much" in low:
        has_price_intent = any(
            token in low
            for token in ("plan", "category", "price", "cost", "rate", "increase", "uplift")
        )
    business_tokens = ("revenue", "premium", "sales", "category", "segment", "plan")
    return has_price_intent and any(token in low for token in business_tokens)


def _chatbot_recent_user_messages(payload: ChatbotPayload) -> list[str]:
    messages = [payload.message]
    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        messages.append(turn.content)
    return messages


def _extract_duration_months_from_text(text: str) -> int | None:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    month_match = re.search(r"\b(\d{1,3})\s*(?:month|months|mo)\b", low)
    if month_match:
        try:
            value = int(month_match.group(1))
        except Exception:
            value = 0
        return value if value > 0 else None

    year_match = re.search(r"\b(\d{1,2})\s*(?:year|years|yr|yrs)\b", low)
    if year_match:
        try:
            value = int(year_match.group(1))
        except Exception:
            value = 0
        return value * 12 if value > 0 else None

    return None


def _resolve_duration_months_from_payload(payload: ChatbotPayload) -> int | None:
    for text in _chatbot_recent_user_messages(payload):
        months = _extract_duration_months_from_text(text)
        if months is not None:
            return months
    return None


def _is_duration_asp_query(payload: ChatbotPayload) -> bool:
    current_low = re.sub(r"\s+", " ", (payload.message or "").strip().lower())
    if not current_low:
        return False

    asp_tokens = (
        "asp",
        "average selling price",
        "avg selling price",
        "average price",
        "avg price",
        "average premium",
    )
    duration_tokens = (
        "duration",
        "validity",
        "tenure",
        "zopper plan duration",
        "2 year",
        "2-year",
        "24 month",
        "24-month",
    )
    current_has_asp = any(token in current_low for token in asp_tokens)
    current_has_duration = any(token in current_low for token in duration_tokens)
    if current_has_asp and current_has_duration:
        return True

    if "zopper plan duration" in current_low or ("duration" in current_low and "refer" in current_low):
        for text in _chatbot_recent_user_messages(payload)[1:]:
            low = re.sub(r"\s+", " ", (text or "").strip().lower())
            if any(token in low for token in asp_tokens) and (
                any(token in low for token in duration_tokens) or _extract_duration_months_from_text(low) is not None
            ):
                return True
    return False


def _format_duration_label(duration_months: int) -> str:
    if duration_months > 0 and duration_months % 12 == 0:
        years = duration_months // 12
        return f"{years}-year" if years == 1 else f"{years}-year"
    return f"{duration_months}-month"


def _build_duration_asp_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_duration_asp_query(payload):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    if dataset_type != "sales":
        return None

    duration_months = _resolve_duration_months_from_payload(payload)
    if duration_months is None or duration_months <= 0:
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    global_scope = bool(context_payload.get("global_scope"))

    resolved_source = _normalize_source_key(_resolve_chatbot_source(payload) or "")
    source_candidates: list[str] = []
    if resolved_source:
        source_candidates = [resolved_source]
    elif global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        source_candidates = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == "sales"
        ]
    else:
        source = str(context_payload.get("source") or "").strip()
        if source:
            source_candidates = [source]

    source_candidates = [src for src in source_candidates if src]
    if not source_candidates:
        return None

    total_gross = 0.0
    total_rows = 0
    contributing_sources: list[str] = []

    for source in source_candidates:
        try:
            frame = get_dataframe(
                db=db,
                job_id=job_id,
                source=source,
                dataset_type="sales",
            )
        except Exception:
            logger.exception(
                "Chatbot duration ASP fetch failed source=%s job_id=%s",
                source,
                job_id,
            )
            continue

        if frame is None or getattr(frame, "empty", True):
            continue

        try:
            scoped = frame.copy()
        except Exception:
            continue

        if from_date or to_date:
            scoped = filter_by_date_range(scoped, "sales", from_date, to_date)
        if scoped is None or getattr(scoped, "empty", True):
            continue

        duration_col = _pick_frame_column(scoped, ["Zopper Plan Duration", "zopper_plan_duration", "Plan Duration"])
        if not duration_col:
            continue

        gross_col = _pick_frame_column(
            scoped,
            [
                "Customer Premium",
                "Plan Selling Price",
                "Amount",
                "Gross Premium",
                "gross_premium",
                "premium",
            ],
        )
        if not gross_col:
            continue

        duration_series = pd.to_numeric(scoped[duration_col], errors="coerce")
        mask = duration_series.eq(duration_months)
        if not bool(mask.any()):
            continue

        gross_series = pd.to_numeric(scoped[gross_col], errors="coerce").fillna(0.0)
        gross_value = float(gross_series[mask].sum())
        row_count = int(mask.sum())
        if row_count <= 0:
            continue

        total_gross += gross_value
        total_rows += row_count
        contributing_sources.append(source)

    if total_rows <= 0:
        return _prepend_partner_scope_prompt(
            f"I could not find sales rows with `Zopper Plan Duration = {duration_months}` in the selected scope.",
            payload=payload,
            context_payload=context_payload,
        )

    asp = total_gross / total_rows if total_rows > 0 else 0.0
    duration_label = _format_duration_label(duration_months)
    if len(source_candidates) == 1:
        scope_label = _source_display_name(source_candidates[0])
    else:
        scope_label = "all available partners"
    period_label = (
        f"{from_date or 'start'} to {to_date or 'latest'}"
        if (from_date or to_date)
        else "all available data"
    )

    answer = (
        f"For {scope_label}, ASP for {duration_label} plans "
        f"(using `Zopper Plan Duration = {duration_months}`) is "
        f"{_format_metric_value('gross_premium', float(asp))}. "
        f"That is based on gross premium {_format_metric_value('gross_premium', float(total_gross))} "
        f"across {total_rows:,} plans for {period_label}."
    )
    if len(source_candidates) > 1 and contributing_sources:
        answer += (
            " Sources contributing duration-matched rows: "
            + ", ".join(_source_display_name(source) for source in contributing_sources[:8])
            + "."
        )
    return _prepend_partner_scope_prompt(
        answer,
        payload=payload,
        context_payload=context_payload,
    )


def _aggregate_metric_by_dimension(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    metric: str,
) -> dict[str, float]:
    if not rows:
        return {}

    dim_key = _pick_present_key(rows, [dimension])
    if dim_key is None:
        safe_map = {_to_safe_key(str(k)): str(k) for k in rows[0].keys()}
        if _to_safe_key(dimension) == "plan_category":
            dim_key = safe_map.get("device_plan_category")
        elif _to_safe_key(dimension) == "device_plan_category":
            dim_key = safe_map.get("plan_category")

    metric_key = _pick_present_key(rows, [metric])
    out: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue

        label = ""
        if dim_key is not None:
            label = str(row.get(dim_key, "")).strip()
        if not label:
            continue
        if label.lower() in {"nan", "none", "null"}:
            continue

        value: float | None = None
        if metric_key is not None:
            value = _to_number(row.get(metric_key))

        if value is None:
            partner_values = [
                _to_number(row.get(partner_key))
                for partner_key in SAMSUNG_PARTNER_SOURCES
            ]
            if any(partner_value is not None for partner_value in partner_values):
                value = float(sum(float(partner_value or 0) for partner_value in partner_values))

        if value is None:
            fallback_total = 0.0
            found_numeric = False
            for key, raw in row.items():
                if dim_key is not None and str(key) == dim_key:
                    continue
                safe_key = _to_safe_key(str(key))
                if safe_key.startswith("tooltip_"):
                    continue
                numeric = _to_number(raw)
                if numeric is None:
                    continue
                fallback_total += float(numeric)
                found_numeric = True
            if found_numeric:
                value = fallback_total

        if value is None:
            continue
        out[label] = out.get(label, 0.0) + max(0.0, float(value))
    return out


def _build_pricing_recommendation_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_pricing_query(payload.message):
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    global_scope = bool(context_payload.get("global_scope"))

    source_candidates: list[str] = []
    if global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        source_candidates = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == "sales"
        ]
    else:
        source = str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip()
        if source:
            source_candidates = [source]

    source_candidates = [src for src in source_candidates if src]
    if not source_candidates:
        return None

    samsung_manual_price_answer = _build_samsung_manual_price_answer(
        message=payload.message,
        source_candidates=source_candidates,
        context_payload=context_payload,
    )
    if samsung_manual_price_answer:
        return _prepend_partner_scope_prompt(
            samsung_manual_price_answer,
            payload=payload,
            context_payload=context_payload,
        )

    dimension_used = ""
    revenue_by_category: dict[str, float] = {}
    quantity_by_category: dict[str, float] = {}

    for dimension in ["plan_category", "device_plan_category", "product_category", "brand"]:
        rev_agg: dict[str, float] = {}
        qty_agg: dict[str, float] = {}
        for source in source_candidates:
            rev_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type="sales",
                job_id=job_id,
                dimension=dimension,
                metric="gross_premium",
                from_date=from_date,
                to_date=to_date,
            )
            qty_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type="sales",
                job_id=job_id,
                dimension=dimension,
                metric="quantity",
                from_date=from_date,
                to_date=to_date,
            )
            local_rev = _aggregate_metric_by_dimension(
                rev_rows,
                dimension=dimension,
                metric="gross_premium",
            )
            local_qty = _aggregate_metric_by_dimension(
                qty_rows,
                dimension=dimension,
                metric="quantity",
            )
            for label, value in local_rev.items():
                rev_agg[label] = rev_agg.get(label, 0.0) + float(value)
            for label, value in local_qty.items():
                qty_agg[label] = qty_agg.get(label, 0.0) + float(value)

        valid_count = sum(
            1
            for label, revenue in rev_agg.items()
            if revenue > 0 and qty_agg.get(label, 0.0) > 0
        )
        if valid_count >= 2:
            dimension_used = dimension
            revenue_by_category = rev_agg
            quantity_by_category = qty_agg
            break

    if not dimension_used:
        return _prepend_partner_scope_prompt(
            "I can analyze pricing only when category-level gross premium and quantity are available. "
            "That split is not currently available in the selected dataset scope.",
            payload=payload,
            context_payload=context_payload,
        )

    rows: list[dict[str, float | str]] = []
    total_revenue = 0.0
    for label, revenue in revenue_by_category.items():
        quantity = float(quantity_by_category.get(label, 0.0))
        if revenue <= 0 or quantity <= 0:
            continue
        avg_price = revenue / quantity
        total_revenue += revenue
        rows.append(
            {
                "label": label,
                "revenue": float(revenue),
                "quantity": float(quantity),
                "avg_price": float(avg_price),
            }
        )

    if len(rows) < 2 or total_revenue <= 0:
        return _prepend_partner_scope_prompt(
            "I can analyze pricing only when category-level gross premium and quantity are available. "
            "That split is not currently available in the selected dataset scope.",
            payload=payload,
            context_payload=context_payload,
        )

    rows.sort(key=lambda item: float(item["revenue"]), reverse=True)
    for item in rows:
        share = float(item["revenue"]) / total_revenue
        if share >= 0.30:
            uplift_pct = 3.0
        elif share >= 0.15:
            uplift_pct = 4.0
        elif share >= 0.08:
            uplift_pct = 6.0
        else:
            uplift_pct = 8.0
        item["share"] = share
        item["uplift_pct"] = uplift_pct
        item["estimated_gain"] = float(item["revenue"]) * uplift_pct / 100.0

    shown = rows[:8]
    scope_label = (
        "all sales datasets"
        if global_scope
        else str(context_payload.get("source_label") or _source_display_name(source_candidates[0]))
    )
    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    lines = [
        f"Using {scope_label}{range_suffix} and category-level gross premium + quantity, this is a practical starting price-uplift plan by {_pretty_label(dimension_used).lower()} (assuming volume remains stable):"
    ]
    for idx, item in enumerate(shown, 1):
        lines.append(
            f"{idx}. {item['label']}: +{float(item['uplift_pct']):.0f}% "
            f"(avg premium {_format_metric_value('gross_premium', float(item['avg_price']))}, "
            f"current revenue {_format_metric_value('gross_premium', float(item['revenue']))}, "
            f"estimated gain +{_format_metric_value('gross_premium', float(item['estimated_gain']))})."
        )

    remaining = len(rows) - len(shown)
    if remaining > 0:
        lines.append(
            f"Remaining {remaining} lower-share categories can start with a +8% test band and be tuned weekly based on conversion."
        )
    lines.append(
        "This is a revenue-side scenario. Share margin targets and expected volume elasticity to optimize exact category-wise price changes."
    )
    return _prepend_partner_scope_prompt(
        "\n".join(lines),
        payload=payload,
        context_payload=context_payload,
    )


_FORECAST_MONTH_MAP: dict[str, int] = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _is_forecast_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    tokens = (
        "forecast",
        "predict",
        "prediction",
        "projection",
        "projected",
        "future month",
        "next month",
        "upcoming month",
        "likely next",
        "estimate next",
        "month ahead",
        "time series",
    )
    return any(token in low for token in tokens)


def _forecast_metric_hint_present(message: str, dataset_type: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    if dataset_type == "claims":
        return any(
            token in low
            for token in (
                "gross premium",
                "claim amount",
                "claims cost",
                "loss ratio",
                "net claim",
                "claims",
                "quantity",
                "count",
                "no. of claims",
                "how many claims",
                "row count",
                "count the rows",
            )
        )
    return any(
        token in low
        for token in (
            "gross premium",
            "earned premium",
            "zopper earned",
            "quantity",
            "units sold",
            "units",
            "count",
            "plan count",
            "quantity of plans",
            "number of plans",
            "how many plans",
            "plan volume",
            "row count",
            "count the rows",
            "activations",
            "premium",
            "sales",
        )
    )


def _is_forecast_followup_query(payload: ChatbotPayload) -> bool:
    message = payload.message or ""
    if _is_pricing_query(message):
        return False
    if _is_forecast_query(message):
        return True

    low = re.sub(r"\s+", " ", message.strip().lower())
    if not low:
        return False

    followup_markers = (
        "what about",
        "how about",
        "and what",
        "and for",
        "for croma",
        "for vijay",
        "for samsung",
        "for reliance",
        "for godrej",
        "for this",
        "for that",
        "same for",
        "same question",
        "what will be",
        "what will",
        "how many",
        "count the rows",
        "count rows",
        "row count",
        "calculate it yourself",
        "calculate yourself",
    )
    source_hint = _detect_source_from_text(message) is not None
    metric_hint = _forecast_metric_hint_present(message, "sales") or _forecast_metric_hint_present(message, "claims")
    likely_followup = source_hint or metric_hint or any(marker in low for marker in followup_markers)
    if not likely_followup:
        return False

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        if _is_forecast_query(turn.content):
            return True
    return False


def _forecast_metric_from_text(message: str, dataset_type: str) -> str:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if dataset_type == "claims":
        if any(token in low for token in ("gross premium", "claim amount", "claims cost", "claims cost")):
            return "gross_premium"
        if "loss ratio" in low:
            return "loss_ratio"
        if "net claim" in low:
            return "net_claims"
        if any(
            token in low
            for token in (
                "quantity",
                "count",
                "no. of claims",
                "how many claims",
                "row count",
                "count the rows",
            )
        ):
            return "quantity"
        return "gross_premium"

    if "zopper earned" in low:
        return "zopper_earned_premium"
    if "earned premium" in low:
        return "earned_premium"
    if any(
        token in low
        for token in (
            "quantity",
            "units sold",
            "units",
            "count",
            "plan count",
            "quantity of plans",
            "number of plans",
            "how many plans",
            "plan volume",
            "row count",
            "count the rows",
            "activations",
        )
    ):
        return "quantity"
    if "gross premium" in low or "premium" in low:
        return "gross_premium"
    return "gross_premium"


def _resolve_forecast_metric(payload: ChatbotPayload, dataset_type: str) -> str:
    message = payload.message or ""
    if _forecast_metric_hint_present(message, dataset_type):
        return _forecast_metric_from_text(message, dataset_type)

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        if not _is_forecast_query(turn.content):
            continue
        if _forecast_metric_hint_present(turn.content, dataset_type):
            return _forecast_metric_from_text(turn.content, dataset_type)

    return _forecast_metric_from_text(message, dataset_type)


def _resolve_forecast_horizon_and_grain(payload: ChatbotPayload) -> tuple[int, str]:
    messages = [payload.message or ""]
    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() == "user":
            messages.append(turn.content or "")

    joined = " ".join(messages).lower()
    grain = "financial_year" if any(token in joined for token in ("financial year", "fiscal year", "fy ")) else "month"

    match = re.search(r"\bnext\s+(\d+)\s+(month|months|year|years)\b", joined)
    if not match:
        match = re.search(r"\b(\d+)\s+(month|months|year|years)\b", joined)

    if match:
        count = max(1, int(match.group(1)))
        unit = match.group(2)
        if "year" in unit:
            return min(count * 12, 24), "financial_year"
        return min(count, 24), grain

    if "next month" in joined or "upcoming month" in joined:
        return 1, "month"
    if any(token in joined for token in ("next year", "upcoming year", "financial year", "fiscal year", "yearly", "annual")):
        return 12, "financial_year"
    return (12, grain) if grain == "financial_year" else (6, "month")


def _forecast_metric_label(metric: str, dataset_type: str, message: str) -> str:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if metric == "quantity":
        if dataset_type == "claims":
            return "Claim Count"
        if "activation" in low:
            return "Activation Count"
        if "plan" in low:
            return "Plan Count"
        return "Sales Count"
    if dataset_type == "claims" and metric in {"gross_premium", "claims"}:
        return "Claims Cost"
    return _pretty_label(metric)


def _parse_month_start(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)

    raw = str(value).strip()
    if not raw:
        return None

    short_match = re.match(r"^([A-Za-z]{3,9})[-/\s](\d{2}|\d{4})$", raw)
    if short_match:
        month_key = short_match.group(1)[:3].lower()
        month = _FORECAST_MONTH_MAP.get(month_key)
        if month:
            year_raw = int(short_match.group(2))
            year = year_raw + 2000 if len(short_match.group(2)) == 2 else year_raw
            if 1900 <= year <= 2200:
                return date(year, month, 1)

    year_month_match = re.match(r"^(\d{4})[-/](\d{1,2})$", raw)
    if year_month_match:
        year = int(year_month_match.group(1))
        month = int(year_month_match.group(2))
        if 1 <= month <= 12:
            return date(year, month, 1)

    iso_candidate = raw[:10]
    try:
        parsed = date.fromisoformat(iso_candidate)
        return date(parsed.year, parsed.month, 1)
    except ValueError:
        pass

    for fmt in ("%b-%y", "%b %y", "%b-%Y", "%b %Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            parsed_dt = datetime.strptime(raw, fmt)
            return date(parsed_dt.year, parsed_dt.month, 1)
        except ValueError:
            continue
    return None


def _next_month_start(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _extract_monthly_totals(rows: list[dict[str, Any]], metric: str) -> list[tuple[date, float]]:
    if not rows:
        return []

    dimension_key = _pick_present_key(
        rows,
        [
            "month",
            "date",
            "fiscal_month",
            "month_year",
        ],
    )
    if dimension_key is None:
        for key in rows[0].keys():
            safe_key = _to_safe_key(str(key))
            if "month" in safe_key or "date" in safe_key:
                dimension_key = str(key)
                break
    if dimension_key is None:
        return []

    metric_key = _pick_present_key(rows, [metric])
    monthly: dict[date, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        month_start = _parse_month_start(row.get(dimension_key))
        if month_start is None:
            continue

        value = _to_number(row.get(metric_key)) if metric_key else None
        if value is None:
            fallback_total = 0.0
            found_numeric = False
            for key, raw in row.items():
                if str(key) == dimension_key:
                    continue
                safe_key = _to_safe_key(str(key))
                if safe_key.startswith("tooltip_"):
                    continue
                numeric = _to_number(raw)
                if numeric is None:
                    continue
                fallback_total += float(numeric)
                found_numeric = True
            if not found_numeric:
                continue
            value = fallback_total

        monthly[month_start] = monthly.get(month_start, 0.0) + float(value)

    return sorted(monthly.items(), key=lambda item: item[0])


def _predict_next_month_value(series: list[tuple[date, float]]) -> dict[str, float] | None:
    if len(series) < 2:
        return None

    values = [float(point[1]) for point in series]
    deltas = [values[idx] - values[idx - 1] for idx in range(1, len(values))]
    if not deltas:
        return None

    recent_delta_count = min(6, len(deltas))
    recent_deltas = deltas[-recent_delta_count:]
    delta_weights = list(range(1, recent_delta_count + 1))
    delta_weight_total = float(sum(delta_weights))
    weighted_delta = sum(delta * weight for delta, weight in zip(recent_deltas, delta_weights)) / delta_weight_total

    growth_rates: list[float] = []
    for idx in range(1, len(values)):
        prev = values[idx - 1]
        curr = values[idx]
        if abs(prev) < 1e-9:
            continue
        growth_rates.append((curr - prev) / prev)

    if growth_rates:
        recent_growth_count = min(6, len(growth_rates))
        recent_growth = growth_rates[-recent_growth_count:]
        growth_weights = list(range(1, recent_growth_count + 1))
        growth_weight_total = float(sum(growth_weights))
        weighted_growth = sum(
            growth * weight for growth, weight in zip(recent_growth, growth_weights)
        ) / growth_weight_total
        projected = values[-1] * (1.0 + weighted_growth)
        trend_window = recent_growth_count
    else:
        projected = values[-1] + weighted_delta
        weighted_growth = weighted_delta / values[-1] if abs(values[-1]) > 1e-9 else 0.0
        trend_window = recent_delta_count

    if all(value >= 0 for value in values) and projected < 0:
        projected = 0.0

    return {
        "projected": float(projected),
        "weighted_growth": float(weighted_growth),
        "trend_window": float(trend_window),
    }


def _build_time_series_forecast_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_forecast_followup_query(payload):
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    metric = _resolve_forecast_metric(payload, dataset_type)
    global_scope = bool(context_payload.get("global_scope"))
    force_row_count_proxy = _message_requests_row_count_proxy(payload.message)

    series: list[tuple[date, float]] = []
    scope_label = "selected dashboard scope"
    proxy_mode = "metric"

    sources = (
        _chatbot_scope_sources(
            db=db,
            context_payload=context_payload,
            dataset_type=dataset_type,
        )
        if global_scope
        else [_normalize_source_key(str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip())]
    )
    sources = [source for source in sources if source]
    if not sources:
        return None

    if metric in {"quantity", "count"}:
        frame = _load_chatbot_scope_frame(
            db=db,
            sources=sources,
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
        series, proxy_mode = _extract_monthly_metric_series_from_frame(
            frame,
            metric=metric,
            dataset_type=dataset_type,
            force_row_count=force_row_count_proxy,
        )

    if not series:
        if global_scope:
            aggregated: dict[date, float] = {}
            for source in sources:
                rows = _chatbot_graph_rows(
                    db=db,
                    source=source,
                    dataset_type=dataset_type,
                    job_id=job_id,
                    dimension="month",
                    metric=metric,
                    from_date=from_date,
                    to_date=to_date,
                )
                for month_start, value in _extract_monthly_totals(rows, metric):
                    aggregated[month_start] = aggregated.get(month_start, 0.0) + float(value)
            series = sorted(aggregated.items(), key=lambda item: item[0])
        else:
            source = sources[0]
            rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                dimension="month",
                metric=metric,
                from_date=from_date,
                to_date=to_date,
            )
            series = _extract_monthly_totals(rows, metric)

    if global_scope:
        scope_label = f"all sources ({dataset_type})"
    else:
        source = sources[0]
        scope_label = f"{context_payload.get('source_label') or _source_display_name(source)} {dataset_type}"

    if len(series) < 2:
        return _prepend_partner_scope_prompt(
            "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast.",
            payload=payload,
            context_payload=context_payload,
        )

    forecast = _predict_next_month_value(series)
    if forecast is None:
        return _prepend_partner_scope_prompt(
            "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast.",
            payload=payload,
            context_payload=context_payload,
        )

    last_month, last_value = series[-1]
    next_month = _next_month_start(last_month)
    next_label = next_month.strftime("%b %y")
    last_label = last_month.strftime("%b %y")
    history_window = min(6, len(series))
    history_start = series[-history_window][0].strftime("%b %y")
    growth_pct = float(forecast["weighted_growth"]) * 100.0
    trend_word = "increase" if growth_pct >= 0 else "decline"

    metric_label = _pretty_label(metric)
    low_message = re.sub(r"\s+", " ", (payload.message or "").strip().lower())
    if metric in {"quantity", "count"}:
        if "plan" in low_message:
            metric_label = "Plan Count"
        elif "activation" in low_message:
            metric_label = "Activation Count"
        elif dataset_type == "claims":
            metric_label = "Claim Count"
        else:
            metric_label = "Sales Volume"

    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    proxy_note = ""
    if metric in {"quantity", "count"} and proxy_mode == "row_count":
        proxy_note = " I used monthly row counts as the operational volume proxy in this scope."

    return _prepend_partner_scope_prompt(
        f"Directional forecast for {scope_label}{range_suffix}: "
        f"{metric_label} is most likely around {_format_metric_value(metric, float(forecast['projected']))} in {next_label}, "
        f"based on month-on-month trend from {history_start} to {last_label}. "
        f"Latest observed value is {_format_metric_value(metric, last_value)} and recent momentum implies a {trend_word} of {abs(growth_pct):.1f}% MoM."
        f"{proxy_note}",
        payload=payload,
        context_payload=context_payload,
    )


def _pretty_label(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def _format_metric_value(metric_key: str, value: float) -> str:
    mk = metric_key.lower()
    if "loss_ratio" in mk:
        return f"{value:.2f}%"
    if "quantity" in mk or "count" in mk:
        return f"{value:,.0f}"
    if abs(value) >= 1e7:
        return f"Rs {value / 1e7:.2f} Cr"
    if abs(value) >= 1e5:
        return f"Rs {value / 1e5:.2f} L"
    if abs(value) >= 1e3:
        return f"Rs {value / 1e3:.1f} K"
    return f"Rs {value:,.2f}"


def _build_time_series_forecast_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_forecast_followup_query(payload):
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    metric = _resolve_forecast_metric(payload, dataset_type)
    global_scope = bool(context_payload.get("global_scope"))
    force_row_count_proxy = _message_requests_row_count_proxy(payload.message)
    horizon_months, grain = _resolve_forecast_horizon_and_grain(payload)

    scope_label = "selected dashboard scope"
    proxy_mode = "metric"

    sources = (
        _chatbot_scope_sources(
            db=db,
            context_payload=context_payload,
            dataset_type=dataset_type,
        )
        if global_scope
        else [_normalize_source_key(str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip())]
    )
    sources = [source for source in sources if source]
    if not sources:
        return None

    history: list[Any] = []
    if metric in {"quantity", "count"} and force_row_count_proxy:
        frame = _load_chatbot_scope_frame(
            db=db,
            sources=sources,
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
        series, proxy_mode = _extract_monthly_metric_series_from_frame(
            frame,
            metric=metric,
            dataset_type=dataset_type,
            force_row_count=force_row_count_proxy,
        )
        history = [
            type("ForecastTuple", (), {"period_start": month_start, "value": float(value)})()
            for month_start, value in series
        ]

    if not history:
        if global_scope:
            history = combine_monthly_history(
                [
                    load_monthly_history(
                        db=db,
                        source=source,
                        dataset_type=dataset_type,
                        metric=metric,
                        job_id=job_id,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    for source in sources
                ]
            )
        else:
            history = load_monthly_history(
                db=db,
                source=sources[0],
                dataset_type=dataset_type,
                metric=metric,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )

    if global_scope:
        scope_label = f"all sources ({dataset_type})"
    else:
        source = sources[0]
        scope_label = f"{context_payload.get('source_label') or _source_display_name(source)} {dataset_type}"

    if len(history) < 2:
        return _prepend_partner_scope_prompt(
            "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast.",
            payload=payload,
            context_payload=context_payload,
        )

    forecast = forecast_monthly_points(history, horizon_months=horizon_months)
    if not forecast:
        return _prepend_partner_scope_prompt(
            "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast.",
            payload=payload,
            context_payload=context_payload,
        )

    history_points = aggregate_financial_year(history) if grain == "financial_year" else history
    forecast_points = aggregate_financial_year(forecast) if grain == "financial_year" else forecast
    if not forecast_points:
        return _prepend_partner_scope_prompt(
            "I couldn’t produce a forecast for the requested horizon from the available history.",
            payload=payload,
            context_payload=context_payload,
        )

    latest_observed = history_points[-1]
    first_forecast = forecast_points[0]
    final_forecast = forecast_points[-1]
    latest_label = (
        f"{latest_observed.period_start.year} - {latest_observed.period_start.year + 1}"
        if grain == "financial_year" and latest_observed.period_start.month == 4
        else latest_observed.period_start.strftime("%b %y")
    )
    first_label = (
        f"{first_forecast.period_start.year} - {first_forecast.period_start.year + 1}"
        if grain == "financial_year" and first_forecast.period_start.month == 4
        else first_forecast.period_start.strftime("%b %y")
    )
    final_label = (
        f"{final_forecast.period_start.year} - {final_forecast.period_start.year + 1}"
        if grain == "financial_year" and final_forecast.period_start.month == 4
        else final_forecast.period_start.strftime("%b %y")
    )
    history_window = min(6, len(history))
    history_start = history[-history_window].period_start.strftime("%b %y")
    history_end = history[-1].period_start.strftime("%b %y")
    metric_label = _forecast_metric_label(metric, dataset_type, payload.message)

    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    proxy_note = ""
    if metric in {"quantity", "count"} and proxy_mode == "row_count":
        proxy_note = " I used monthly row counts as the operational volume proxy in this scope."

    forecast_sentence = (
        f"Directional {grain.replace('_', ' ')} forecast for {scope_label}{range_suffix}: "
        f"{metric_label} is projected at {_format_metric_value(metric, first_forecast.value)} in {first_label}"
    )
    if len(forecast_points) > 1 and final_label != first_label:
        forecast_sentence += (
            f", reaching around {_format_metric_value(metric, final_forecast.value)} by {final_label}"
        )
    forecast_sentence += "."

    return _prepend_partner_scope_prompt(
        f"{forecast_sentence} "
        f"Latest observed value is {_format_metric_value(metric, latest_observed.value)} in {latest_label}, "
        f"using historical trend from {history_start} to {history_end}."
        f"{proxy_note}",
        payload=payload,
        context_payload=context_payload,
    )


def _dedupe_insights(lines: list[str], limit: int = 5) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in lines:
        line = re.sub(r"\s+", " ", raw).strip(" -\t")
        if not line:
            continue
        norm = line.lower()
        if norm in seen:
            continue
        seen.add(norm)
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _is_low_signal_line(line: str) -> bool:
    low = line.lower()
    banned = (
        "as an ai",
        "i cannot",
        "i can't",
        "insufficient data",
        "not enough data",
        "unable to",
        "i do not have",
    )
    return any(token in low for token in banned)


def _derive_data_driven_insights(payload: GraphInsightPayload) -> list[str]:
    rows = payload.rows[:80]
    if not rows:
        return []

    dim_key = _to_safe_key(payload.dimension)
    dim_candidates = [dim_key, payload.dimension]
    dimension_key = next((k for k in dim_candidates if any(k in r for r in rows)), None)
    if not dimension_key:
        dimension_key = next(iter(rows[0].keys()), payload.dimension)

    if payload.compare_mode:
        numeric_keys: dict[str, int] = {}
        for row in rows:
            for key, value in row.items():
                if key == dimension_key:
                    continue
                if _to_number(value) is not None:
                    numeric_keys[key] = numeric_keys.get(key, 0) + 1
        if not numeric_keys:
            return []
        ordered = sorted(numeric_keys, key=lambda k: numeric_keys[k], reverse=True)
        series_a = ordered[0]
        series_b = ordered[1] if len(ordered) > 1 else None

        valid = []
        for row in rows:
            va = _to_number(row.get(series_a))
            vb = _to_number(row.get(series_b)) if series_b else None
            if va is None and vb is None:
                continue
            valid.append((row, va, vb))
        if not valid:
            return []

        insights: list[str] = []
        latest_row, latest_a, latest_b = valid[-1]
        latest_label = str(latest_row.get(dimension_key, "latest period"))
        if latest_a is not None:
            insights.append(
                f"In {latest_label}, {_pretty_label(series_a)} stands at {_format_metric_value(payload.metric, latest_a)}."
            )
        if series_b and latest_b is not None:
            insights.append(
                f"In {latest_label}, {_pretty_label(series_b)} stands at {_format_metric_value(payload.metric, latest_b)}."
            )
        if latest_a is not None and latest_b is not None:
            leader = series_a if latest_a >= latest_b else series_b
            gap = abs(latest_a - latest_b)
            insights.append(
                f"{_pretty_label(leader)} leads by {_format_metric_value(payload.metric, gap)} in the latest period, signaling stronger momentum."
            )
        return _dedupe_insights(insights)

    metric_key = _to_safe_key(payload.metric)
    metric_candidates = [metric_key, payload.metric]
    actual_metric_key = next((k for k in metric_candidates if any(k in r for r in rows)), None)
    if not actual_metric_key:
        return []

    points: list[tuple[str, float]] = []
    for row in rows:
        value = _to_number(row.get(actual_metric_key))
        if value is None:
            continue
        label = str(row.get(dimension_key, "Unknown"))
        points.append((label, value))

    if not points:
        return []

    insights = []
    first_label, first_value = points[0]
    last_label, last_value = points[-1]
    peak_label, peak_value = max(points, key=lambda x: x[1])
    low_label, low_value = min(points, key=lambda x: x[1])
    metric_name = _pretty_label(actual_metric_key)

    insights.append(
        f"Latest {metric_name} is {_format_metric_value(actual_metric_key, last_value)} in {last_label}."
    )
    if len(points) > 1:
        delta = last_value - first_value
        direction = "increased" if delta >= 0 else "decreased"
        momentum = "positive momentum" if delta >= 0 else "a contraction trend"
        pct = (abs(delta) / abs(first_value) * 100.0) if first_value else None
        if pct is None:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} from {first_label} to {last_label}, indicating {momentum}."
            )
        else:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} ({pct:.1f}%) from {first_label} to {last_label}, indicating {momentum}."
            )

    insights.append(
        f"Peak {metric_name} reached {_format_metric_value(actual_metric_key, peak_value)} in {peak_label}."
    )
    insights.append(
        f"Lowest {metric_name} was {_format_metric_value(actual_metric_key, low_value)} in {low_label}."
    )

    total = sum(v for _, v in points if v > 0)
    if total > 0:
        top3 = sorted(points, key=lambda x: x[1], reverse=True)[:3]
        share = sum(v for _, v in top3) / total * 100.0
        insights.append(
            f"Top 3 categories contribute {share:.1f}% of total {metric_name}, highlighting concentration risk."
        )

    return _dedupe_insights(insights)

def _build_insight_prompt(payload: GraphInsightPayload) -> str:
    rows = payload.rows[:120]
    serialized_rows = json.dumps(rows, ensure_ascii=True, default=str)
    return (
        "Generate executive-ready insights for the graph below.\n"
        "Return only bullet points.\n"
        f"Source: {payload.source}\n"
        f"Dataset Type: {payload.dataset_type}\n"
        f"Dimension: {payload.dimension}\n"
        f"Metric: {payload.metric}\n"
        f"Bucket: {payload.bucket or 'none'}\n"
        f"Compare Mode: {'yes' if payload.compare_mode else 'no'}\n"
        f"From Date: {payload.from_date or 'n/a'}\n"
        f"To Date: {payload.to_date or 'n/a'}\n"
        "Data rows (JSON):\n"
        f"{serialized_rows}\n"
        "Output requirements:\n"
        "- 4 to 6 bullets.\n"
        "- Each bullet must include at least one concrete number from the data.\n"
        "- Prioritize: current performance, strongest change, top/low contributors, and concentration/risk signal.\n"
        "- Use professional business language (momentum, contribution, concentration, variance, efficiency, risk).\n"
        "- Mention direction and scale (absolute and percent) wherever possible.\n"
        "- Include one implication or action-oriented observation when supported by data.\n"
        "- Do not mention missing data, AI limitations, or generic disclaimers.\n"
        "- Keep each bullet under 34 words.\n"
    )


def _resolve_llm_model(*env_keys: str, default: str = DEFAULT_LLM_MODEL) -> str:
    for key in env_keys:
        if not key:
            continue
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _trim_chat_text(value: str, limit: int) -> str:
    compact = re.sub(r"\s+", " ", value or "").strip()
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit].rstrip()}..."


def _resolve_chatbot_num_predict(payload: ChatbotPayload) -> int:
    hard_cap = max(256, _env_int("CHATBOT_MAX_NUM_PREDICT", 4096))
    if payload.max_tokens is not None:
        return max(8, min(int(payload.max_tokens), hard_cap))

    message = _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT)
    small_prompt_chars = max(1, _env_int("CHATBOT_SMALL_PROMPT_CHARS", 180))
    medium_prompt_chars = max(small_prompt_chars + 1, _env_int("CHATBOT_MEDIUM_PROMPT_CHARS", 520))
    small_tokens = max(220, _env_int("CHATBOT_SMALL_NUM_PREDICT", 420))
    medium_tokens = max(420, _env_int("CHATBOT_MEDIUM_NUM_PREDICT", 900))
    large_tokens = max(760, _env_int("CHATBOT_LARGE_NUM_PREDICT", 1800))

    if len(message) <= small_prompt_chars:
        return min(small_tokens, hard_cap)
    if len(message) <= medium_prompt_chars:
        return min(medium_tokens, hard_cap)
    return min(large_tokens, hard_cap)


def _chatbot_cache_key(
    payload: ChatbotPayload,
    *,
    model: str,
    system_prompt: str,
    temperature: float,
    num_predict: int,
    context_fingerprint: str = "",
) -> str:
    history_signature: list[dict[str, str]] = []
    for turn in payload.history[-CHATBOT_HISTORY_LIMIT:]:
        role = (turn.role or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_chat_text(turn.content, CHATBOT_HISTORY_CHAR_LIMIT)
        if not content:
            continue
        history_signature.append({"role": role, "content": content})

    signature = {
        "model": model,
        "system_prompt": system_prompt.strip(),
        "context_fingerprint": context_fingerprint.strip(),
        "message": _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT),
        "history": history_signature,
        "temperature": round(float(temperature), 3),
        "num_predict": int(num_predict),
        "source": _normalize_source_key(payload.source or ""),
        "dataset_type": _normalize_dataset_type_for_chatbot(payload.dataset_type),
        "job_id": _normalize_chatbot_job_id(payload.job_id) or "",
        "from_date": _normalize_chatbot_date(payload.from_date) or "",
        "to_date": _normalize_chatbot_date(payload.to_date) or "",
        "global_scope": bool(payload.global_scope),
        "ui_context": payload.ui_context if isinstance(payload.ui_context, dict) else {},
    }
    raw = json.dumps(signature, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _chatbot_cache_get(cache_key: str) -> dict[str, str] | None:
    now = time.time()
    with _chatbot_cache_lock:
        cached = _chatbot_response_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _chatbot_response_cache.pop(cache_key, None)
            return None
        return payload


def _chatbot_cache_set(cache_key: str, payload: dict[str, str]) -> None:
    now = time.time()
    with _chatbot_cache_lock:
        if len(_chatbot_response_cache) >= CHATBOT_CACHE_MAX_ITEMS:
            expired = [key for key, (expires, _) in _chatbot_response_cache.items() if expires <= now]
            for key in expired:
                _chatbot_response_cache.pop(key, None)
            if len(_chatbot_response_cache) >= CHATBOT_CACHE_MAX_ITEMS and _chatbot_response_cache:
                oldest_key = min(_chatbot_response_cache, key=lambda key: _chatbot_response_cache[key][0])
                _chatbot_response_cache.pop(oldest_key, None)
        _chatbot_response_cache[cache_key] = (now + CHATBOT_CACHE_TTL_SECONDS, payload)


def _is_timeout_exception(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            return True
        if reason and "timed out" in str(reason).lower():
            return True
    return "timed out" in str(exc).lower() or "timeout" in str(exc).lower()


def _build_chatbot_prompt(payload: ChatbotPayload, dashboard_context: str) -> str:
    lines: list[str] = [
        "Analytics context (authoritative; includes dashboard + dataset-derived signals):",
        dashboard_context.strip(),
        "",
        "Conversation:",
    ]
    for turn in payload.history[-CHATBOT_HISTORY_LIMIT:]:
        role = (turn.role or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_chat_text(turn.content, CHATBOT_HISTORY_CHAR_LIMIT)
        if not content:
            continue
        role_label = "User" if role == "user" else "Assistant"
        lines.append(f"{role_label}: {content}")

    message = _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT)
    lines.append("")
    lines.append("Response quality bar:")
    lines.append("1) Answer the user's question directly in the first sentence.")
    lines.append("2) Use concrete metrics, dates, and comparisons from context when available.")
    lines.append("3) Keep the answer structured, professional, and decision-oriented.")
    lines.append("4) If recommending actions, provide up to 3 prioritized next steps.")
    lines.append("5) Avoid repeating the same phrasing from prior assistant turns; vary sentence openings and structure.")
    lines.append("6) Stay on the asked metric. If quantity/count is requested, do not switch to premium unless the user asks.")
    lines.append("7) If quantity/count is requested and no explicit quantity field is reliable, use row count as the proxy when supported by the dataset grain, and say that briefly.")
    lines.append(f"User: {message}")
    lines.append("Assistant:")
    return "\n".join(lines)


def _call_llm(
    system_prompt: str,
    prompt: str,
    *,
    model: str | None = None,
    temperature: float = 0.2,
    num_predict: int = 480,
    timeout_seconds: int | None = None,
) -> tuple[str, str, dict[str, Any]]:
    resolved_model = (model or "").strip() or _resolve_llm_model("CHATBOT_MODEL", "SARVAM_MODEL")
    sarvam_url = os.getenv("SARVAM_API_URL", "https://api.sarvam.ai/v1/chat/completions").strip() or "https://api.sarvam.ai/v1/chat/completions"
    sarvam_api_key = os.getenv("SARVAM_API_KEY", "").strip()
    if not sarvam_api_key:
        raise ValueError("SARVAM_API_KEY is not configured.")

    resolved_timeout = timeout_seconds if timeout_seconds and timeout_seconds > 0 else _env_int("SARVAM_TIMEOUT_SECONDS", 70)
    resolved_max_tokens = max(8, int(num_predict))
    resolved_temperature = max(0.0, min(1.5, float(temperature)))

    body = {
        "model": resolved_model,
        "messages": [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": prompt.strip()},
        ],
        "temperature": resolved_temperature,
        "max_tokens": resolved_max_tokens,
        "stream": False,
    }
    raw = json.dumps(body).encode("utf-8")
    req = UrlRequest(
        sarvam_url,
        data=raw,
        headers={
            "Content-Type": "application/json",
            "api-subscription-key": sarvam_api_key,
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=resolved_timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            choices = payload.get("choices")
            if not isinstance(choices, list) or not choices:
                raise ValueError("Sarvam response missing choices.")

            first_choice = choices[0] if isinstance(choices[0], dict) else {}
            message_obj = first_choice.get("message")
            if isinstance(message_obj, dict):
                response_text = str(message_obj.get("content") or "").strip()
            else:
                response_text = str(first_choice.get("text") or "").strip()

            if not response_text:
                raise ValueError("Empty LLM response.")

            usage_obj = payload.get("usage")
            completion_tokens = usage_obj.get("completion_tokens") if isinstance(usage_obj, dict) else None
            response_meta = {
                "done_reason": str(first_choice.get("finish_reason") or "").strip().lower(),
                "eval_count": completion_tokens,
                "provider": "sarvam",
            }
            return resolved_model, response_text, response_meta
    except HTTPError as exc:
        detail = ""
        try:
            body = exc.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body) if body else {}
            if isinstance(parsed, dict):
                error_obj = parsed.get("error")
                if isinstance(error_obj, dict):
                    detail = str(error_obj.get("message") or error_obj.get("code") or "").strip()
        except Exception:
            detail = ""
        if exc.code in {401, 403}:
            raise ValueError(detail or f"Sarvam authentication failed with HTTP {exc.code}.")
        raise URLError(detail or f"Sarvam HTTP {exc.code}")


def _looks_truncated_response(
    response_text: str,
    payload_meta: dict[str, Any],
    token_budget: int,
) -> bool:
    done_reason = str(payload_meta.get("done_reason") or "").strip().lower()
    if done_reason == "length":
        return True

    eval_count_raw = payload_meta.get("eval_count")
    try:
        eval_count = int(eval_count_raw) if eval_count_raw is not None else 0
    except (TypeError, ValueError):
        eval_count = 0

    likely_token_limited = eval_count >= max(1, token_budget - 1)
    if not likely_token_limited:
        return False

    tail = (response_text or "").strip()
    if not tail:
        return True
    if tail.endswith((".", "!", "?", "\"", "'", ".)", "!)", "?)")):
        return False
    return True


def _looks_incomplete_response(response_text: str) -> bool:
    text = (response_text or "").strip()
    if not text:
        return True
    if text.endswith((".", "!", "?", "\"", "'", ".)", "!)", "?)", "...")):
        return False

    if text.count("**") % 2 == 1 or text.count("__") % 2 == 1:
        return True
    backtick_count = text.count("`")
    if backtick_count and backtick_count % 2 == 1:
        return True

    if text.endswith((",", ":", ";", "-", "/")):
        return True
    if text.endswith(("(", "[", "{")):
        return True
    if re.search(r"\([^\)]*$", text):
        return True
    if re.search(r"\[[^\]]*$", text):
        return True
    if re.search(r"\{[^}]*$", text):
        return True
    if text.count("(") > text.count(")"):
        return True
    if text.count("[") > text.count("]"):
        return True
    if text.count("{") > text.count("}"):
        return True

    words = text.split()
    if not words:
        return True
    last_word = words[-1].strip(".,:;!?\"'()[]{}").lower()
    dangling_words = {
        "and", "or", "to", "for", "with", "about", "on", "in", "of", "the",
        "a", "an", "this", "that", "your", "our", "their", "better", "more",
        "some", "any", "if", "because", "while", "when", "then",
    }
    if last_word in dangling_words:
        return True

    if len(words) >= 10:
        return True

    last_line = next((line.strip() for line in reversed(text.splitlines()) if line.strip()), "")
    if last_line and re.search(r"[A-Za-z]{1,2}$", last_line):
        return True

    return False


def _repair_incomplete_response(
    *,
    model_name: str,
    response_text: str,
    temperature: float,
    max_num_predict_cap: int,
    retry_timeout_seconds: int,
) -> str:
    if not _looks_incomplete_response(response_text):
        return response_text

    try:
        _, repaired_text, _ = _call_llm(
            "You rewrite incomplete assistant answers into one complete response.",
            (
                "The response below ended mid-thought. Rewrite it as one complete, coherent "
                "answer with the same intent. Do not add new facts.\n\n"
                f"Incomplete response:\n{response_text}\n\nComplete response:"
            ),
            model=model_name,
            temperature=min(temperature, 0.12),
            num_predict=min(max_num_predict_cap, max(128, _env_int("CHATBOT_REPAIR_NUM_PREDICT", 640))),
            timeout_seconds=max(8, min(retry_timeout_seconds, 24)),
        )
        repaired_text = repaired_text.strip()
        if repaired_text and len(repaired_text) >= max(24, len(response_text.strip()) // 2):
            return repaired_text
    except (URLError, TimeoutError, ValueError, OSError):
        pass

    return response_text


def _prewarm_llm_model() -> None:
    model_name = _resolve_llm_model("CHATBOT_MODEL", "SARVAM_MODEL")
    prewarm_timeout = max(10, _env_int("CHATBOT_PREWARM_TIMEOUT_SECONDS", 45))
    try:
        started_at = time.perf_counter()
        _call_llm(
            "You are a concise assistant.",
            "User: Reply with OK.\nAssistant:",
            model=model_name,
            temperature=0.0,
            num_predict=4,
            timeout_seconds=prewarm_timeout,
        )
        duration_ms = (time.perf_counter() - started_at) * 1000
        logger.info("LLM prewarm complete model=%s duration_ms=%.2f", model_name, duration_ms)
    except Exception as exc:
        logger.warning("LLM prewarm failed: %s", exc)


@app.post("/insights/graph")
def generate_graph_insights(
    payload: GraphInsightPayload,
    db: Session = Depends(get_db),
):
    normalized_source = _normalize_source_key(payload.source)
    normalized_dataset = (payload.dataset_type or "").strip().lower()
    cached_db = get_precomputed_insights(
        db=db,
        source=normalized_source,
        dataset_type=normalized_dataset,
        job_id=payload.job_id,
        dimension=payload.dimension,
        metric=payload.metric,
        bucket=payload.bucket,
        compare_mode=payload.compare_mode,
        from_date=payload.from_date,
        to_date=payload.to_date,
    )
    if cached_db is not None:
        return cached_db

    insights_enabled = os.getenv("ENABLE_GRAPH_INSIGHTS", "1").strip().lower() not in {"0", "false", "no", "off"}
    if not insights_enabled:
        return {
            "insights": [],
            "model": "disabled",
            "message": "Graph insights are disabled in this environment.",
        }

    if not payload.rows:
        return {"insights": [], "model": "none", "message": "No graph rows available."}

    cache_key = _graph_insights_cache_key(payload)
    now = time.time()
    if len(_graph_insights_cache) > 256:
        expired_keys = [k for k, (expiry, _) in _graph_insights_cache.items() if expiry <= now]
        for k in expired_keys:
            _graph_insights_cache.pop(k, None)

    cached = _graph_insights_cache.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    system_prompt = _read_chatcards_system_prompt()
    prompt = _build_insight_prompt(payload)
    base_insights = _derive_data_driven_insights(payload)
    insight_tokens = max(220, _env_int("CHATCARDS_NUM_PREDICT", 560))
    insight_timeout = max(12, _env_int("CHATCARDS_TIMEOUT_SECONDS", 55))
    insight_temperature = _env_float("CHATCARDS_TEMPERATURE", 0.2)

    try:
        model, response_text, _ = _call_llm(
            system_prompt,
            prompt,
            model=_resolve_llm_model("CHATCARDS_MODEL", "CHATBOT_MODEL", "SARVAM_MODEL"),
            temperature=insight_temperature,
            num_predict=insight_tokens,
            timeout_seconds=insight_timeout,
        )
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        logger.warning("Graph insights generation failed: %s", exc)
        if base_insights:
            response_payload = {
                "insights": base_insights[:5],
                "model": "rule-based",
                "message": "LLM insights unavailable; showing data-driven insights.",
            }
            _graph_insights_cache[cache_key] = (now + GRAPH_INSIGHTS_TTL_SECONDS, response_payload)
            try:
                upsert_precomputed_insights(
                    db=db,
                    source=normalized_source,
                    dataset_type=normalized_dataset,
                    job_id=payload.job_id,
                    dimension=payload.dimension,
                    metric=payload.metric,
                    bucket=payload.bucket,
                    compare_mode=payload.compare_mode,
                    from_date=payload.from_date,
                    to_date=payload.to_date,
                    insights=response_payload["insights"],
                    model=response_payload.get("model", "rule-based"),
                    message=response_payload.get("message"),
                )
                db.commit()
            except Exception:
                db.rollback()
                logger.exception("Failed to persist precomputed graph insights")
            return response_payload
        raise HTTPException(
            status_code=503,
            detail=(
                "Insights service unavailable. Ensure SARVAM_API_KEY is configured and SARVAM_MODEL is valid."
            ),
        )

    llm_insights = [line for line in _extract_bullets(response_text) if not _is_low_signal_line(line)]
    merged_insights = _dedupe_insights(base_insights + llm_insights, limit=5)
    insights = merged_insights or base_insights
    if not insights:
        trimmed = response_text[:260].strip()
        insights = [trimmed] if trimmed else []

    response_payload = {"insights": insights[:5], "model": model}
    _graph_insights_cache[cache_key] = (now + GRAPH_INSIGHTS_TTL_SECONDS, response_payload)
    try:
        upsert_precomputed_insights(
            db=db,
            source=normalized_source,
            dataset_type=normalized_dataset,
            job_id=payload.job_id,
            dimension=payload.dimension,
            metric=payload.metric,
            bucket=payload.bucket,
            compare_mode=payload.compare_mode,
            from_date=payload.from_date,
            to_date=payload.to_date,
            insights=response_payload["insights"],
            model=response_payload.get("model", "rule-based"),
            message=response_payload.get("message"),
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to persist precomputed graph insights")
    return response_payload


@app.post("/chatbot/message")
def chatbot_message(
    payload: ChatbotPayload,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    chatbot_enabled = os.getenv("ENABLE_CHATBOT", "1").strip().lower() not in {"0", "false", "no", "off"}
    if not chatbot_enabled:
        return {
            "response": "",
            "model": "disabled",
            "message": "Chatbot is disabled in this environment.",
        }

    if _is_chatbot_greeting(payload.message):
        return {
            "response": _build_chatbot_greeting_response(
                db=db,
                payload=payload,
            ),
            "model": "rule-based-greeting",
        }

    dashboard_context, context_payload = _build_chatbot_dashboard_context(db=db, payload=payload)
    chart_response = _build_chatbot_chart_response(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if chart_response:
        return chart_response
    reason_breakdown_answer = _build_reason_breakdown_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if reason_breakdown_answer:
        return {
            "response": reason_breakdown_answer,
            "model": "rule-based-reason-breakdown",
        }
    claims_average_answer = _build_claim_average_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if claims_average_answer:
        return {
            "response": claims_average_answer,
            "model": "rule-based-claims-avg",
        }
    duration_asp_answer = _build_duration_asp_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if duration_asp_answer:
        return {
            "response": duration_asp_answer,
            "model": "rule-based-duration-asp",
        }
    pricing_answer = _build_pricing_recommendation_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if pricing_answer:
        return {
            "response": pricing_answer,
            "model": "rule-based-pricing",
        }
    forecast_answer = _build_time_series_forecast_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if forecast_answer:
        return {
            "response": forecast_answer,
            "model": "rule-based-forecast",
        }
    rule_based_answer = _build_underperformance_answer(payload.message, context_payload)
    if rule_based_answer:
        return {
            "response": _prepend_partner_scope_prompt(
                rule_based_answer,
                payload=payload,
                context_payload=context_payload,
            ),
            "model": "rule-based-dashboard",
        }
    dimension_stats_answer = _build_dimension_stats_answer(payload.message, context_payload)
    if dimension_stats_answer:
        return {
            "response": _prepend_partner_scope_prompt(
                dimension_stats_answer,
                payload=payload,
                context_payload=context_payload,
            ),
            "model": "rule-based-dashboard",
        }
    if _is_direct_summary_metric_query(payload.message):
        direct_summary_answer = _chatbot_summary_metric_answer(
            payload=payload,
            context_payload=context_payload,
        )
        if direct_summary_answer:
            return {
                "response": _prepend_partner_scope_prompt(
                    direct_summary_answer,
                    payload=payload,
                    context_payload=context_payload,
                ),
                "model": "rule-based-summary",
            }
    if _is_dimension_stats_query(payload.message):
        fast_rankings_answer = _chatbot_rankings_fallback_answer(
            payload=payload,
            context_payload=context_payload,
        )
        if fast_rankings_answer:
            return {
                "response": _prepend_partner_scope_prompt(
                    fast_rankings_answer,
                    payload=payload,
                    context_payload=context_payload,
                ),
                "model": "rule-based-rankings",
            }

    prompt = _build_chatbot_prompt(payload, dashboard_context)
    base_system_prompt = (
        (payload.system_prompt or "").strip()
        or os.getenv("CHATBOT_SYSTEM_PROMPT", "").strip()
        or DEFAULT_CHATBOT_SYSTEM_PROMPT
    )
    selected_source_key = _normalize_source_key(str(context_payload.get("source") or ""))
    mentioned_source_key = _normalize_source_key(_detect_source_from_text(payload.message) or "")
    include_samsung_rules = _is_samsung_source(selected_source_key) or _is_samsung_source(mentioned_source_key)
    hard_constraints = [
        "Use the Analytics context block and conversation turns as primary evidence.",
        "Never invent entities or numbers that are not supported by available context.",
        "If key context is missing, state the gap and provide the closest defensible answer with explicit assumptions.",
        "Give a direct answer first, then supporting evidence and implications.",
        "Prefer precise metrics and avoid generic statements.",
        "Never reveal hidden reasoning, chain-of-thought, or <think> blocks.",
        "Do not re-introduce AI Sahyogi unless the user explicitly asks.",
        "End with a complete final sentence and close any opened bracket.",
        "For forecasting questions, estimate future month or financial-year values only from monthly history in context and mark it as directional.",
        "Avoid repetitive templates across turns; vary phrasing while keeping the answer concise and factual.",
        (
            "Honor partner scope from the user query: if a partner is named, answer only for that partner; "
            "if user asks for all partners/sources, include all available partners."
        ),
        (
            "If partner is not specified and scope is cross-partner, first ask user to name a partner for drill-down "
            "and still provide the combined all-partner answer."
        ),
        "If the user asks for sales or claims explicitly, follow the asked dataset even if UI context is on the other dataset.",
        "If dataset profile lists complaint, cause, operation, type, or status fields, use those fields directly before saying reasons are unavailable.",
        (
            "Apply source-specific taxonomy and mappings. Do not use Samsung glossary, Samsung fixed price matrix, "
            "or Samsung model-code mapping unless the selected source is Samsung."
        ),
    ]
    hard_constraints_text = "\n".join(
        f"{idx}) {constraint}" for idx, constraint in enumerate(hard_constraints, start=1)
    )
    system_prompt = (
        f"{base_system_prompt}\n\n"
        "Hard constraints:\n"
        f"{hard_constraints_text}\n"
    )
    model_name = _resolve_llm_model("CHATBOT_MODEL", "SARVAM_MODEL")
    temperature = (
        payload.temperature
        if payload.temperature is not None
        else _env_float("CHATBOT_TEMPERATURE", 0.15)
    )
    max_tokens = _resolve_chatbot_num_predict(payload)
    timeout_seconds = max(12, _env_int("CHATBOT_TIMEOUT_SECONDS", 65))
    retry_timeout_seconds = max(
        8, min(timeout_seconds, _env_int("CHATBOT_RETRY_TIMEOUT_SECONDS", 30))
    )
    retry_num_predict = max(
        128, min(max_tokens, _env_int("CHATBOT_RETRY_NUM_PREDICT", 640))
    )
    max_num_predict_cap = max(max_tokens, _env_int("CHATBOT_MAX_NUM_PREDICT", 4096))
    context_fingerprint = hashlib.sha256(dashboard_context.encode("utf-8")).hexdigest()
    cache_key = _chatbot_cache_key(
        payload,
        model=model_name,
        system_prompt=system_prompt,
        temperature=temperature,
        num_predict=max_tokens,
        context_fingerprint=context_fingerprint,
    )
    cached = _chatbot_cache_get(cache_key)
    if cached is not None:
        return cached

    llm_unavailable, llm_unavailable_reason = _chatbot_llm_unavailable_state()
    if llm_unavailable:
        return _build_chatbot_service_fallback_response(
            payload=payload,
            context_payload=context_payload,
            error_detail=llm_unavailable_reason or "LLM service is temporarily unavailable.",
        )
    started_at = time.perf_counter()

    try:
        model, response_text, response_meta = _call_llm(
            system_prompt,
            prompt,
            model=model_name,
            temperature=temperature,
            num_predict=max_tokens,
            timeout_seconds=timeout_seconds,
        )
        needs_expansion = (
            _looks_truncated_response(response_text, response_meta, max_tokens)
            or _looks_incomplete_response(response_text)
        )
        elapsed_seconds = time.perf_counter() - started_at
        remaining_budget = max(0.0, float(timeout_seconds) - elapsed_seconds)
        allow_expansion = max_tokens > 160 and remaining_budget >= 10.0
        if needs_expansion and max_tokens < max_num_predict_cap and allow_expansion:
            expanded_tokens = min(max_num_predict_cap, max_tokens + max(256, max_tokens // 2))
            try:
                expansion_timeout = max(8, min(retry_timeout_seconds, max(10, int(remaining_budget))))
                model, expanded_text, _ = _call_llm(
                    system_prompt,
                    prompt,
                    model=model_name,
                    temperature=temperature,
                    num_predict=expanded_tokens,
                    timeout_seconds=expansion_timeout,
                )
                if expanded_text and len(expanded_text) >= len(response_text):
                    response_text = expanded_text
                    max_tokens = expanded_tokens
                if _looks_incomplete_response(response_text):
                    response_text = _repair_incomplete_response(
                        model_name=model_name,
                        response_text=response_text,
                        temperature=temperature,
                        max_num_predict_cap=max_num_predict_cap,
                        retry_timeout_seconds=retry_timeout_seconds,
                    )
            except (URLError, TimeoutError, ValueError, OSError):
                response_text = _repair_incomplete_response(
                    model_name=model_name,
                    response_text=response_text,
                    temperature=temperature,
                    max_num_predict_cap=max_num_predict_cap,
                    retry_timeout_seconds=retry_timeout_seconds,
                )
        else:
            response_text = _repair_incomplete_response(
                model_name=model_name,
                response_text=response_text,
                temperature=temperature,
                max_num_predict_cap=max_num_predict_cap,
                retry_timeout_seconds=retry_timeout_seconds,
            )
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        if _is_timeout_exception(exc):
            try:
                model, response_text, _ = _call_llm(
                    system_prompt,
                    prompt,
                    model=model_name,
                    temperature=min(temperature, 0.12),
                    num_predict=retry_num_predict,
                    timeout_seconds=retry_timeout_seconds,
                )
            except (URLError, TimeoutError, ValueError, OSError) as retry_exc:
                logger.warning("Chatbot generation failed after timeout retry: %s", retry_exc)
                _chatbot_mark_llm_unavailable(
                    detail=str(retry_exc) or "LLM timeout retry failed.",
                    ttl_seconds=max(30, retry_timeout_seconds),
                )
                return _build_chatbot_service_fallback_response(
                    payload=payload,
                    context_payload=context_payload,
                    error_detail=str(retry_exc) or "LLM timeout retry failed.",
                )
        else:
            logger.warning("Chatbot generation failed: %s", exc)
            _chatbot_mark_llm_unavailable(
                detail=str(exc) or "LLM request failed.",
            )
            return _build_chatbot_service_fallback_response(
                payload=payload,
                context_payload=context_payload,
                error_detail=str(exc) or "LLM request failed.",
            )

    _chatbot_mark_llm_available()

    if _looks_incomplete_response(response_text):
        response_text = _repair_incomplete_response(
            model_name=model_name,
            response_text=response_text,
            temperature=temperature,
            max_num_predict_cap=max_num_predict_cap,
            retry_timeout_seconds=retry_timeout_seconds,
        )

    response_text = _sanitize_chatbot_response_text(response_text)
    if not response_text:
        response_text = "I could not generate a usable answer for this query. Please try rephrasing the question."
    response_text = _prepend_partner_scope_prompt(
        response_text,
        payload=payload,
        context_payload=context_payload,
    )

    response_payload = {
        "response": response_text,
        "model": model,
    }
    _chatbot_cache_set(cache_key, response_payload)
    duration_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "TIMING chatbot.message model=%s tokens=%s duration_ms=%.2f",
        model,
        max_tokens,
        duration_ms,
    )
    return response_payload


# ==================================================
# PROCESS DISABLED
# ==================================================
@app.post("/process")
def process_disabled():
    return {
        "status": "disabled",
        "reason": "Use /analytics/by-dimension directly",
    }

# ==================================================
# EVENTS (SSE)
# ==================================================

@app.get("/events")
async def events():
    async def event_stream():
        while True:
            await asyncio.sleep(30)
            yield "data: ping"



    return StreamingResponse(event_stream(), media_type="text/event-stream")
