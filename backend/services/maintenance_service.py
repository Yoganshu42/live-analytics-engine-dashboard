from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from models.data_rows import DataRow
from services.analytics.goodrej_engine import invalidate_godrej_load_cache
from services.analytics.hitachi_engine import invalidate_hitachi_load_cache
from services.analytics.reliance_engine import invalidate_reliance_load_cache
from services.analytics.samsung_engine import invalidate_samsung_load_cache
from services.analytics_repository import invalidate_dataframe_cache
from services.precompute_service import rebuild_precomputed_for_all_tags
from services.precomputed_repository import (
    clear_precomputed_for_source_dataset,
    get_precomputed_summary,
    upsert_precomputed_summary,
)
from services.reliance_branding import canonicalize_reliance_payload, is_reliance_source

logger = logging.getLogger(__name__)

_MASTER_CACHE_SOURCES = (
    "master_dashboard_v6",
    "master_dashboard_v7",
    "master_dashboard_v8",
    "master_dashboard_v9",
    "master_dashboard_v12",
    "master_dashboard_v13",
    "master_dashboard_v14",
)
_DAILY_REFRESH_STATUS_SOURCE = "__system__"
_DAILY_REFRESH_STATUS_DATASET = "maintenance_daily_refresh"
_DAILY_REFRESH_RUNNING_TTL_SECONDS = max(
    int(os.getenv("DAILY_REFRESH_RUNNING_TTL_SECONDS", "21600")),
    900,
)


def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off"}


def _parse_status_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _write_daily_refresh_status(
    *,
    db: Session,
    payload: dict[str, Any],
) -> dict[str, Any]:
    clean_payload = payload if isinstance(payload, dict) else {}
    upsert_precomputed_summary(
        db=db,
        source=_DAILY_REFRESH_STATUS_SOURCE,
        dataset_type=_DAILY_REFRESH_STATUS_DATASET,
        job_id=None,
        from_date=None,
        to_date=None,
        summary=clean_payload,
    )
    db.commit()
    return clean_payload


def get_daily_refresh_status(
    *,
    db: Session,
) -> dict[str, Any]:
    payload = get_precomputed_summary(
        db=db,
        source=_DAILY_REFRESH_STATUS_SOURCE,
        dataset_type=_DAILY_REFRESH_STATUS_DATASET,
        job_id=None,
        from_date=None,
        to_date=None,
    )
    return payload if isinstance(payload, dict) else {}


def run_daily_refresh_if_due(
    *,
    db: Session,
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
    repair_reliance_brands: bool = False,
    include_tagged_jobs: bool | None = None,
    force: bool = False,
) -> dict[str, Any]:
    now = datetime.now().astimezone()
    run_day = now.date().isoformat()
    resolved_include_tagged_jobs = (
        include_tagged_jobs
        if include_tagged_jobs is not None
        else _env_flag("AUTO_DAILY_REFRESH_INCLUDE_TAGGED_JOBS", "0")
    )
    status = get_daily_refresh_status(db=db)
    status_state = str(status.get("status") or "").strip().lower()
    status_day = str(status.get("run_day") or "").strip()
    started_at = _parse_status_datetime(status.get("started_at"))

    if not force:
        if status_state == "success" and status_day == run_day:
            return {
                "status": "skipped",
                "reason": "already_completed_today",
                "run_day": run_day,
            }
        if (
            status_state == "running"
            and status_day == run_day
            and started_at is not None
            and (now - started_at.astimezone()).total_seconds() < _DAILY_REFRESH_RUNNING_TTL_SECONDS
        ):
            return {
                "status": "skipped",
                "reason": "already_running",
                "run_day": run_day,
                "started_at": started_at.isoformat(),
            }

    _write_daily_refresh_status(
        db=db,
        payload={
            "status": "running",
            "run_day": run_day,
            "started_at": now.isoformat(),
            "completed_at": None,
            "error": None,
            "source": (source or "").strip().lower() or None,
            "dataset_type": (dataset_type or "").strip().lower() or None,
            "job_id": (job_id or "").strip() or None,
            "include_tagged_jobs": bool(resolved_include_tagged_jobs),
        },
    )

    try:
        result = run_daily_refresh(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            repair_reliance_brands=repair_reliance_brands,
            include_tagged_jobs=bool(resolved_include_tagged_jobs),
        )
        completed_at = datetime.now().astimezone()
        return _write_daily_refresh_status(
            db=db,
            payload={
                "status": "success",
                "run_day": run_day,
                "started_at": now.isoformat(),
                "completed_at": completed_at.isoformat(),
                "error": None,
                "source": (source or "").strip().lower() or None,
                "dataset_type": (dataset_type or "").strip().lower() or None,
                "job_id": (job_id or "").strip() or None,
                "include_tagged_jobs": bool(resolved_include_tagged_jobs),
                "result": result,
            },
        )
    except Exception as exc:
        db.rollback()
        failed_at = datetime.now().astimezone()
        error_payload = {
            "status": "failed",
            "run_day": run_day,
            "started_at": now.isoformat(),
            "completed_at": failed_at.isoformat(),
            "error": str(exc),
            "source": (source or "").strip().lower() or None,
            "dataset_type": (dataset_type or "").strip().lower() or None,
            "job_id": (job_id or "").strip() or None,
            "include_tagged_jobs": bool(resolved_include_tagged_jobs),
        }
        try:
            _write_daily_refresh_status(db=db, payload=error_payload)
        except Exception:
            db.rollback()
            logger.exception("Failed to persist daily refresh status after error")
        return error_payload


def repair_reliance_brand_rows(
    *,
    db: Session,
    dataset_type: str | None = None,
    job_id: str | None = None,
    batch_size: int = 1000,
) -> dict[str, int]:
    scanned = 0
    updated = 0
    changed_tags: set[tuple[str, str, str | None]] = set()
    last_id = 0
    dataset_key = (dataset_type or "").strip().lower()
    normalized_job = (job_id or "").strip() or None if job_id is not None else None

    while True:
        query = (
            db.query(DataRow)
            .filter(DataRow.id > last_id)
            .filter(DataRow.source.in_(sorted({"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"})))
            .order_by(DataRow.id.asc())
            .limit(batch_size)
        )
        if dataset_key:
            query = query.filter(DataRow.dataset_type == dataset_key)
        if job_id is not None:
            if normalized_job is None:
                query = query.filter(DataRow.job_id.is_(None))
            else:
                query = query.filter(DataRow.job_id == normalized_job)

        rows = query.all()
        if not rows:
            break

        for row in rows:
            last_id = max(last_id, int(row.id or 0))
            scanned += 1
            payload = row.data if isinstance(row.data, dict) else {}
            repaired = canonicalize_reliance_payload(payload)
            if repaired != payload:
                row.data = repaired
                flag_modified(row, "data")
                updated += 1
                changed_tags.add(
                    (
                        str(row.source or "").strip().lower(),
                        str(row.dataset_type or "").strip().lower(),
                        (row.job_id or "").strip() or None,
                    )
                )
        db.commit()

    return {
        "rows_scanned": int(scanned),
        "rows_updated": int(updated),
        "tags_touched": int(len(changed_tags)),
    }


def invalidate_all_runtime_caches() -> None:
    invalidate_dataframe_cache()
    invalidate_samsung_load_cache()
    invalidate_reliance_load_cache()
    invalidate_godrej_load_cache()
    invalidate_hitachi_load_cache()


def refresh_master_overview_cache(
    *,
    db: Session,
    job_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    from routers.analytics import _build_master_dashboard_payload
    from services.precomputed_repository import upsert_precomputed_summary

    payload = _build_master_dashboard_payload(
        db=db,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    upsert_precomputed_summary(
        db=db,
        source="master_dashboard_v14",
        dataset_type="overview",
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
        summary=payload,
    )
    db.commit()
    return payload


def run_daily_refresh(
    *,
    db: Session,
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
    repair_reliance_brands: bool = False,
    include_tagged_jobs: bool = True,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "repair": None,
        "precompute": None,
        "overview_refreshed": False,
        "include_tagged_jobs": bool(include_tagged_jobs),
    }

    invalidate_all_runtime_caches()

    if repair_reliance_brands:
        summary["repair"] = repair_reliance_brand_rows(
            db=db,
            dataset_type=dataset_type if is_reliance_source(source) or source is None else None,
            job_id=job_id,
        )
        invalidate_all_runtime_caches()

    for cache_source in _MASTER_CACHE_SOURCES:
        clear_precomputed_for_source_dataset(
            db,
            source=cache_source,
            dataset_type="overview",
        )
    db.commit()

    summary["precompute"] = rebuild_precomputed_for_all_tags(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        include_tagged_jobs=include_tagged_jobs,
    )
    invalidate_all_runtime_caches()
    refresh_master_overview_cache(
        db=db,
        job_id=None if job_id is None else (job_id or "").strip() or None,
        from_date=None,
        to_date=None,
    )
    summary["overview_refreshed"] = True
    return summary
