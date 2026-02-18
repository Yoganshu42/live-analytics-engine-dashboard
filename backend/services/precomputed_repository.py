from __future__ import annotations

import copy
import os
import threading
import time
from typing import Any

from sqlalchemy.orm import Session

from models.precomputed_analytics import (
    PrecomputedGraph,
    PrecomputedInsight,
    PrecomputedSummary,
)

PRECOMPUTED_CACHE_TTL_SECONDS = int(os.getenv("PRECOMPUTED_CACHE_TTL_SECONDS", "180"))
_precomputed_cache_lock = threading.Lock()
_graph_cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]]]] = {}
_summary_cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}
_insights_cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}


def _normalize_key(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_job_key(value: str | None) -> str:
    return (value or "").strip()


def _normalize_optional(value: str | None) -> str:
    return (value or "").strip()


def _base_filters(
    model,
    source: str,
    dataset_type: str,
    job_id: str | None,
):
    return (
        model.source == _normalize_key(source),
        model.dataset_type == _normalize_key(dataset_type),
        model.job_key == _normalize_job_key(job_id),
    )


def _graph_cache_key(
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    bucket: str | None,
    from_date: str | None,
    to_date: str | None,
) -> tuple[str, ...]:
    return (
        _normalize_key(source),
        _normalize_key(dataset_type),
        _normalize_job_key(job_id),
        _normalize_key(dimension),
        _normalize_key(metric),
        _normalize_optional(bucket),
        _normalize_optional(from_date),
        _normalize_optional(to_date),
    )


def _summary_cache_key(
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> tuple[str, ...]:
    return (
        _normalize_key(source),
        _normalize_key(dataset_type),
        _normalize_job_key(job_id),
        _normalize_optional(from_date),
        _normalize_optional(to_date),
    )


def _insights_cache_key(
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    bucket: str | None,
    compare_mode: bool,
    from_date: str | None,
    to_date: str | None,
) -> tuple[str, ...]:
    return (
        _normalize_key(source),
        _normalize_key(dataset_type),
        _normalize_job_key(job_id),
        _normalize_key(dimension),
        _normalize_key(metric),
        _normalize_optional(bucket),
        "1" if compare_mode else "0",
        _normalize_optional(from_date),
        _normalize_optional(to_date),
    )


def _cache_get_list(
    cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]]]],
    key: tuple[str, ...],
) -> list[dict[str, Any]] | None:
    now = time.time()
    with _precomputed_cache_lock:
        cached = cache.get(key)
        if cached is None:
            return None
        expires_at, value = cached
        if expires_at < now:
            cache.pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_set_list(
    cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]]]],
    key: tuple[str, ...],
    value: list[dict[str, Any]],
) -> None:
    with _precomputed_cache_lock:
        cache[key] = (time.time() + PRECOMPUTED_CACHE_TTL_SECONDS, copy.deepcopy(value))


def _cache_get_dict(
    cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]],
    key: tuple[str, ...],
) -> dict[str, Any] | None:
    now = time.time()
    with _precomputed_cache_lock:
        cached = cache.get(key)
        if cached is None:
            return None
        expires_at, value = cached
        if expires_at < now:
            cache.pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_set_dict(
    cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]],
    key: tuple[str, ...],
    value: dict[str, Any],
) -> None:
    with _precomputed_cache_lock:
        cache[key] = (time.time() + PRECOMPUTED_CACHE_TTL_SECONDS, copy.deepcopy(value))


def _invalidate_caches_for_tag(source: str, dataset_type: str, job_id: str | None) -> None:
    source_key = _normalize_key(source)
    dataset_key = _normalize_key(dataset_type)
    job_key = _normalize_job_key(job_id)
    with _precomputed_cache_lock:
        graph_keys = [k for k in _graph_cache if k[0] == source_key and k[1] == dataset_key and k[2] == job_key]
        summary_keys = [k for k in _summary_cache if k[0] == source_key and k[1] == dataset_key and k[2] == job_key]
        insight_keys = [k for k in _insights_cache if k[0] == source_key and k[1] == dataset_key and k[2] == job_key]
        for key in graph_keys:
            _graph_cache.pop(key, None)
        for key in summary_keys:
            _summary_cache.pop(key, None)
        for key in insight_keys:
            _insights_cache.pop(key, None)


def clear_precomputed_for_tag(
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None = None,
) -> None:
    filters_graph = _base_filters(PrecomputedGraph, source, dataset_type, job_id)
    filters_summary = _base_filters(PrecomputedSummary, source, dataset_type, job_id)
    filters_insights = _base_filters(PrecomputedInsight, source, dataset_type, job_id)
    db.query(PrecomputedGraph).filter(*filters_graph).delete(synchronize_session=False)
    db.query(PrecomputedSummary).filter(*filters_summary).delete(synchronize_session=False)
    db.query(PrecomputedInsight).filter(*filters_insights).delete(synchronize_session=False)
    _invalidate_caches_for_tag(source, dataset_type, job_id)


def get_precomputed_graph(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    bucket: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, Any]] | None:
    primary_key = _graph_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        bucket=bucket,
        from_date=from_date,
        to_date=to_date,
    )
    cached = _cache_get_list(_graph_cache, primary_key)
    if cached is not None:
        return cached

    bucket_key = _normalize_optional(bucket)
    from_key = _normalize_optional(from_date)
    to_key = _normalize_optional(to_date)

    def _fetch_row(bk: str, fk: str, tk: str):
        return (
            db.query(PrecomputedGraph)
            .filter(
                *_base_filters(PrecomputedGraph, source, dataset_type, job_id),
                PrecomputedGraph.dimension == _normalize_key(dimension),
                PrecomputedGraph.metric == _normalize_key(metric),
                PrecomputedGraph.bucket == bk,
                PrecomputedGraph.from_date == fk,
                PrecomputedGraph.to_date == tk,
            )
            .first()
        )

    row = _fetch_row(bucket_key, from_key, to_key)
    if row is None and bucket_key:
        row = _fetch_row("", from_key, to_key)

    if row is None:
        return None
    payload = row.rows if isinstance(row.rows, list) else []
    _cache_set_list(_graph_cache, primary_key, payload)
    if bucket_key:
        fallback_key = _graph_cache_key(
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket="",
            from_date=from_date,
            to_date=to_date,
        )
        _cache_set_list(_graph_cache, fallback_key, payload)
    return payload


def upsert_precomputed_graph(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    rows: list[dict[str, Any]],
    bucket: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    filters = (
        *_base_filters(PrecomputedGraph, source, dataset_type, job_id),
        PrecomputedGraph.dimension == _normalize_key(dimension),
        PrecomputedGraph.metric == _normalize_key(metric),
        PrecomputedGraph.bucket == _normalize_optional(bucket),
        PrecomputedGraph.from_date == _normalize_optional(from_date),
        PrecomputedGraph.to_date == _normalize_optional(to_date),
    )
    obj = db.query(PrecomputedGraph).filter(*filters).first()
    payload = rows if isinstance(rows, list) else []
    if obj is None:
        obj = PrecomputedGraph(
            source=_normalize_key(source),
            dataset_type=_normalize_key(dataset_type),
            job_key=_normalize_job_key(job_id),
            dimension=_normalize_key(dimension),
            metric=_normalize_key(metric),
            bucket=_normalize_optional(bucket),
            from_date=_normalize_optional(from_date),
            to_date=_normalize_optional(to_date),
            rows=payload,
        )
        db.add(obj)
        cache_key = _graph_cache_key(
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket=bucket,
            from_date=from_date,
            to_date=to_date,
        )
        _cache_set_list(_graph_cache, cache_key, payload)
        return None
    obj.rows = payload
    cache_key = _graph_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        bucket=bucket,
        from_date=from_date,
        to_date=to_date,
    )
    _cache_set_list(_graph_cache, cache_key, payload)
    return None


def get_precomputed_summary(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any] | None:
    cache_key = _summary_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    cached = _cache_get_dict(_summary_cache, cache_key)
    if cached is not None:
        return cached

    from_key = _normalize_optional(from_date)
    to_key = _normalize_optional(to_date)
    row = (
        db.query(PrecomputedSummary)
        .filter(
            *_base_filters(PrecomputedSummary, source, dataset_type, job_id),
            PrecomputedSummary.from_date == from_key,
            PrecomputedSummary.to_date == to_key,
        )
        .first()
    )
    if row is None:
        return None
    payload = row.summary if isinstance(row.summary, dict) else {}
    _cache_set_dict(_summary_cache, cache_key, payload)
    return payload


def upsert_precomputed_summary(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    summary: dict[str, Any],
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    filters = (
        *_base_filters(PrecomputedSummary, source, dataset_type, job_id),
        PrecomputedSummary.from_date == _normalize_optional(from_date),
        PrecomputedSummary.to_date == _normalize_optional(to_date),
    )
    obj = db.query(PrecomputedSummary).filter(*filters).first()
    payload = summary if isinstance(summary, dict) else {}
    if obj is None:
        obj = PrecomputedSummary(
            source=_normalize_key(source),
            dataset_type=_normalize_key(dataset_type),
            job_key=_normalize_job_key(job_id),
            from_date=_normalize_optional(from_date),
            to_date=_normalize_optional(to_date),
            summary=payload,
        )
        db.add(obj)
        cache_key = _summary_cache_key(
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
        _cache_set_dict(_summary_cache, cache_key, payload)
        return None
    obj.summary = payload
    cache_key = _summary_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    _cache_set_dict(_summary_cache, cache_key, payload)
    return None


def get_precomputed_insights(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    bucket: str | None = None,
    compare_mode: bool = False,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any] | None:
    primary_key = _insights_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        bucket=bucket,
        compare_mode=compare_mode,
        from_date=from_date,
        to_date=to_date,
    )
    cached = _cache_get_dict(_insights_cache, primary_key)
    if cached is not None:
        return cached

    bucket_key = _normalize_optional(bucket)
    from_key = _normalize_optional(from_date)
    to_key = _normalize_optional(to_date)

    def _fetch_row(bk: str, fk: str, tk: str):
        return (
            db.query(PrecomputedInsight)
            .filter(
                *_base_filters(PrecomputedInsight, source, dataset_type, job_id),
                PrecomputedInsight.dimension == _normalize_key(dimension),
                PrecomputedInsight.metric == _normalize_key(metric),
                PrecomputedInsight.bucket == bk,
                PrecomputedInsight.compare_mode == bool(compare_mode),
                PrecomputedInsight.from_date == fk,
                PrecomputedInsight.to_date == tk,
            )
            .first()
        )

    row = _fetch_row(bucket_key, from_key, to_key)
    if row is None and bucket_key:
        row = _fetch_row("", from_key, to_key)

    if row is None:
        return None
    payload = {
        "insights": row.insights if isinstance(row.insights, list) else [],
        "model": row.model or "rule-based",
        "message": row.message,
    }
    _cache_set_dict(_insights_cache, primary_key, payload)
    if bucket_key:
        fallback_key = _insights_cache_key(
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket="",
            compare_mode=compare_mode,
            from_date=from_date,
            to_date=to_date,
        )
        _cache_set_dict(_insights_cache, fallback_key, payload)
    return payload


def upsert_precomputed_insights(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    insights: list[str],
    model: str,
    message: str | None = None,
    bucket: str | None = None,
    compare_mode: bool = False,
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    filters = (
        *_base_filters(PrecomputedInsight, source, dataset_type, job_id),
        PrecomputedInsight.dimension == _normalize_key(dimension),
        PrecomputedInsight.metric == _normalize_key(metric),
        PrecomputedInsight.bucket == _normalize_optional(bucket),
        PrecomputedInsight.compare_mode == bool(compare_mode),
        PrecomputedInsight.from_date == _normalize_optional(from_date),
        PrecomputedInsight.to_date == _normalize_optional(to_date),
    )
    obj = db.query(PrecomputedInsight).filter(*filters).first()
    payload = [str(v).strip() for v in (insights or []) if str(v).strip()]
    if obj is None:
        obj = PrecomputedInsight(
            source=_normalize_key(source),
            dataset_type=_normalize_key(dataset_type),
            job_key=_normalize_job_key(job_id),
            dimension=_normalize_key(dimension),
            metric=_normalize_key(metric),
            bucket=_normalize_optional(bucket),
            compare_mode=bool(compare_mode),
            from_date=_normalize_optional(from_date),
            to_date=_normalize_optional(to_date),
            insights=payload,
            model=(model or "rule-based").strip(),
            message=message,
        )
        db.add(obj)
        cache_key = _insights_cache_key(
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket=bucket,
            compare_mode=compare_mode,
            from_date=from_date,
            to_date=to_date,
        )
        _cache_set_dict(
            _insights_cache,
            cache_key,
            {
                "insights": payload,
                "model": (model or "rule-based").strip(),
                "message": message,
            },
        )
        return None
    obj.insights = payload
    obj.model = (model or "rule-based").strip()
    obj.message = message
    cache_key = _insights_cache_key(
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        bucket=bucket,
        compare_mode=compare_mode,
        from_date=from_date,
        to_date=to_date,
    )
    _cache_set_dict(
        _insights_cache,
        cache_key,
        {
            "insights": payload,
            "model": (model or "rule-based").strip(),
            "message": message,
        },
    )
    return None
