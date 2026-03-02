from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.data_rows import DataRow
from models.deck_pptx_cache import DeckPptxCache
from services.deck_pptx_service import (
    VALID_WEEK_WINDOWS,
    generate_partner_deck_pptx,
    resolve_partners,
)

DECK_CACHE_MAX_ENTRIES = max(int(os.getenv("DECK_CACHE_MAX_ENTRIES", "24")), 5)
DECK_CACHE_MAX_AGE_DAYS = max(int(os.getenv("DECK_CACHE_MAX_AGE_DAYS", "14")), 0)
DECK_CACHE_MAX_BYTES = max(int(os.getenv("DECK_CACHE_MAX_BYTES", str(8 * 1024 * 1024))), 1)


def _normalize_dataset_type(dataset_type: str) -> str:
    return "claims" if str(dataset_type).strip().lower() == "claims" else "sales"


def _normalize_week_window(week_window: int) -> int:
    try:
        parsed = int(week_window)
    except Exception:
        return 4
    return parsed if parsed in VALID_WEEK_WINDOWS else 4


def _normalize_job_key(value: str | None) -> str:
    return (value or "").strip()


def _normalize_optional(value: str | None) -> str:
    return (value or "").strip()


def _source_variants(source: str) -> list[str]:
    key = (source or "").strip().lower()
    if key in {"samsung", "samsung_vs", "samsung_vijay_sales"}:
        return ["samsung_vs", "samsung_vijay_sales"]
    if key == "samsung_croma":
        return ["samsung_croma"]
    if key in {"reliance", "reliance_resq", "reliance-resq", "reliance resq", "resq"}:
        return ["reliance"]
    if key in {"godrej", "goodrej", "goddrej"}:
        return ["godrej", "goodrej", "goddrej"]
    return [key]


def _partners_for_source(source: str) -> set[str]:
    key = (source or "").strip().lower()
    if key == "samsung":
        return {"samsung_vs", "samsung_croma"}
    if key in {"samsung_vs", "samsung_vijay_sales"}:
        return {"samsung_vs"}
    if key == "samsung_croma":
        return {"samsung_croma"}
    if key in {"reliance", "reliance_resq", "reliance-resq", "reliance resq", "resq"}:
        return {"reliance"}
    if key in {"godrej", "goodrej", "goddrej"}:
        return {"godrej"}
    return {key}


def _compute_data_fingerprint(
    *,
    db: Session,
    partners: list[str],
    dataset_type: str,
    job_id: str | None,
) -> str:
    dataset_key = _normalize_dataset_type(dataset_type)
    job_key = _normalize_job_key(job_id)

    parts: list[str] = []
    for partner in partners:
        variants = _source_variants(partner)
        query = db.query(func.count(DataRow.id), func.max(DataRow.id)).filter(
            DataRow.dataset_type == dataset_key,
        )
        if job_key:
            query = query.filter(DataRow.job_id == job_key)
        if len(variants) == 1:
            query = query.filter(DataRow.source == variants[0])
        else:
            query = query.filter(DataRow.source.in_(variants))
        count_raw, max_id_raw = query.one()
        parts.append(f"{partner}:{int(count_raw or 0)}:{int(max_id_raw or 0)}")

    payload = {
        "dataset_type": dataset_key,
        "job_key": job_key,
        "parts": parts,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_cache_key(
    *,
    partners: list[str],
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    include_tables: bool,
    week_window: int,
) -> str:
    payload = {
        "partners": partners,
        "dataset_type": _normalize_dataset_type(dataset_type),
        "job_id": _normalize_job_key(job_id),
        "from_date": _normalize_optional(from_date),
        "to_date": _normalize_optional(to_date),
        "include_tables": bool(include_tables),
        "week_window": _normalize_week_window(week_window),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _prune_old_cache_rows(db: Session) -> None:
    has_changes = False

    if DECK_CACHE_MAX_AGE_DAYS > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=DECK_CACHE_MAX_AGE_DAYS)
        expired = (
            db.query(DeckPptxCache)
            .filter(DeckPptxCache.updated_at < cutoff)
            .delete(synchronize_session=False)
        )
        if expired:
            has_changes = True

    total = int(db.query(func.count(DeckPptxCache.id)).scalar() or 0)
    overflow = total - DECK_CACHE_MAX_ENTRIES
    if overflow > 0:
        stale_ids = [
            row.id
            for row in db.query(DeckPptxCache.id)
            .order_by(DeckPptxCache.updated_at.asc(), DeckPptxCache.id.asc())
            .limit(overflow)
            .all()
        ]
        if stale_ids:
            (
                db.query(DeckPptxCache)
                .filter(DeckPptxCache.id.in_(stale_ids))
                .delete(synchronize_session=False)
            )
            has_changes = True

    if has_changes:
        db.commit()


def get_or_generate_cached_partner_deck_pptx(
    *,
    db: Session,
    partners: list[str],
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    include_tables: bool,
    week_window: int,
) -> tuple[bytes, str]:
    normalized_dataset = _normalize_dataset_type(dataset_type)
    normalized_week_window = _normalize_week_window(week_window)
    resolved_partners = resolve_partners(partners)
    partners_key = ",".join(resolved_partners)
    cache_key = _build_cache_key(
        partners=resolved_partners,
        dataset_type=normalized_dataset,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
        include_tables=include_tables,
        week_window=normalized_week_window,
    )

    data_fingerprint = _compute_data_fingerprint(
        db=db,
        partners=resolved_partners,
        dataset_type=normalized_dataset,
        job_id=job_id,
    )

    cached = None
    try:
        cached = db.query(DeckPptxCache).filter(DeckPptxCache.cache_key == cache_key).first()
        if (
            cached is not None
            and cached.data_fingerprint == data_fingerprint
            and isinstance(cached.pptx_blob, (bytes, bytearray))
            and len(cached.pptx_blob) > 0
        ):
            filename = (cached.filename or "").strip() or f"partner_deck_{normalized_dataset}.pptx"
            return bytes(cached.pptx_blob), filename
    except Exception:
        db.rollback()
        cached = None

    pptx_bytes, filename = generate_partner_deck_pptx(
        db=db,
        partners=resolved_partners,
        dataset_type=normalized_dataset,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
        include_tables=include_tables,
        week_window=normalized_week_window,
    )

    try:
        if len(pptx_bytes) <= DECK_CACHE_MAX_BYTES:
            target = cached or db.query(DeckPptxCache).filter(DeckPptxCache.cache_key == cache_key).first()
            if target is None:
                target = DeckPptxCache(
                    cache_key=cache_key,
                )
                db.add(target)

            target.partners_key = partners_key
            target.dataset_type = normalized_dataset
            target.job_key = _normalize_job_key(job_id)
            target.from_date = _normalize_optional(from_date)
            target.to_date = _normalize_optional(to_date)
            target.include_tables = bool(include_tables)
            target.week_window = normalized_week_window
            target.data_fingerprint = data_fingerprint
            target.filename = (filename or "").strip() or f"partner_deck_{normalized_dataset}.pptx"
            target.size_bytes = len(pptx_bytes)
            target.pptx_blob = pptx_bytes
            db.commit()
            _prune_old_cache_rows(db)
        elif cached is not None:
            db.delete(cached)
            db.commit()
    except Exception:
        db.rollback()

    return pptx_bytes, filename


def invalidate_deck_cache_for_source_dataset(
    *,
    db: Session,
    source: str,
    dataset_type: str,
) -> int:
    dataset_key = _normalize_dataset_type(dataset_type)
    targets = _partners_for_source(source)
    if not targets:
        return 0

    rows = db.query(DeckPptxCache.id, DeckPptxCache.partners_key).filter(
        DeckPptxCache.dataset_type == dataset_key
    ).all()
    stale_ids: list[int] = []
    for row in rows:
        partner_tokens = {
            token.strip()
            for token in str(row.partners_key or "").split(",")
            if token.strip()
        }
        if partner_tokens.intersection(targets):
            stale_ids.append(int(row.id))

    if not stale_ids:
        return 0

    deleted = (
        db.query(DeckPptxCache)
        .filter(DeckPptxCache.id.in_(stale_ids))
        .delete(synchronize_session=False)
    )
    return int(deleted or 0)
