from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from sqlalchemy.orm import Session

from models.data_rows import DataRow
from routers.analytics import (
    _normalize_source,
    analytics_date_bounds,
    compute_by_dimension_rows,
    compute_summary_values,
)
from services.precomputed_repository import (
    clear_precomputed_for_tag,
    upsert_precomputed_graph,
    upsert_precomputed_insights,
    upsert_precomputed_summary,
)
from services.samsung_partner_config import (
    SAMSUNG_PARTNER_LABELS,
    SAMSUNG_PARTNER_SOURCES,
)

logger = logging.getLogger(__name__)

SALES_METRICS = [
    "gross_premium",
    "earned_premium",
    "zopper_earned_premium",
    "quantity",
]

CLAIMS_METRICS = [
    "claims",
    "net_claims",
    "loss_ratio",
    "quantity",
]

BASE_DIMENSIONS = [
    "month",
    "state",
    "plan_category",
    "device_plan_category",
]

APPLIANCE_DIMENSIONS = [
    "channel",
    "product_category",
]


def _normalize_dataset_type(value: str) -> str:
    return (value or "").strip().lower()


def _dedupe_preserve(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _affected_sources(source: str) -> list[str]:
    resolved_source, _ = _normalize_source(source)
    # Keep precompute writes scoped to the exact requested source.
    # Broad fan-out across samsung variants pollutes job-tagged caches
    # (e.g., samsung_vs job keys accidentally written into samsung_croma).
    return [resolved_source]


def _dimensions_for_source(source: str) -> list[str]:
    dimensions = list(BASE_DIMENSIONS)
    if source in SAMSUNG_PARTNER_SOURCES:
        # Deck generation drills into model_code for Samsung partner views;
        # precompute it so large partner uploads stay responsive.
        dimensions.append("model_code")
    if source in {"godrej", "hitachi"}:
        dimensions.extend(APPLIANCE_DIMENSIONS)
    if source == "reliance":
        # Reliance ResQ dashboard uses ARTICLE_BRAND sidecards; precompute to avoid live slowness.
        dimensions.append("article_brand")
    return _dedupe_preserve(dimensions)


def _ranges_for_source(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> list[tuple[str | None, str | None]]:
    ranges: list[tuple[str | None, str | None]] = [(None, None)]
    try:
        bounds = analytics_date_bounds(
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
            db=db,
        )
    except Exception:
        logger.exception(
            "Failed to read date bounds for precompute source=%s dataset=%s",
            source,
            dataset_type,
        )
        return ranges

    min_date = bounds.get("min_date") if isinstance(bounds, dict) else None
    max_date = bounds.get("max_date") if isinstance(bounds, dict) else None
    if min_date and max_date:
        ranges.append((str(min_date), str(max_date)))
        try:
            min_ts = pd.to_datetime(min_date, errors="coerce")
            max_ts = pd.to_datetime(max_date, errors="coerce")
            today = pd.Timestamp.now().normalize()
            if min_ts is not pd.NaT and max_ts is not pd.NaT:
                if max_ts > today and min_ts <= today:
                    ranges.append((str(min_date), today.date().isoformat()))
        except Exception:
            logger.exception(
                "Failed to add capped precompute range source=%s dataset=%s min=%s max=%s",
                source,
                dataset_type,
                min_date,
                max_date,
            )
    return ranges


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if n != n or n == float("inf") or n == float("-inf"):
        return None
    return n


def _derive_rule_based_insights(
    *,
    rows: list[dict[str, Any]],
    dimension: str,
    metric: str,
    compare_mode: bool,
) -> list[str]:
    if not rows:
        return []

    dim_key = str(dimension or "").strip().lower().replace(" ", "_")
    metric_key = str(metric or "").strip().lower().replace(" ", "_")

    if compare_mode:
        points: list[tuple[str, dict[str, float]]] = []
        for row in rows:
            label = str(row.get(dim_key, row.get(dimension, "Unknown")))
            partner_values = {
                partner_key: (_to_number(row.get(partner_key)) or 0.0)
                for partner_key in SAMSUNG_PARTNER_SOURCES
                if row.get(partner_key) is not None
            }
            if not partner_values:
                continue
            points.append((label, partner_values))
        if not points:
            return []
        label, latest_values = points[-1]
        leader_key = max(latest_values, key=latest_values.get)
        leader = SAMSUNG_PARTNER_LABELS.get(leader_key, leader_key).replace("Samsung ", "")
        snapshot = ", ".join(
            f"{SAMSUNG_PARTNER_LABELS.get(partner_key, partner_key).replace('Samsung ', '')} is {value:,.2f}"
            for partner_key, value in latest_values.items()
        )
        return [
            f"In {label}, {snapshot}.",
            f"{leader} leads at {latest_values[leader_key]:,.2f} in the latest period.",
        ]

    values: list[tuple[str, float]] = []
    for row in rows:
        label = str(row.get(dim_key, row.get(dimension, "Unknown")))
        value = _to_number(row.get(metric_key, row.get(metric)))
        if value is None:
            continue
        values.append((label, value))
    if not values:
        return []

    first_label, first_value = values[0]
    latest_label, latest_value = values[-1]
    peak_label, peak_value = max(values, key=lambda t: t[1])
    delta = latest_value - first_value
    direction = "increased" if delta >= 0 else "decreased"
    return [
        f"Latest {metric_key.replace('_', ' ')} is {latest_value:,.2f} in {latest_label}.",
        f"{metric_key.replace('_', ' ').title()} {direction} by {abs(delta):,.2f} from {first_label} to {latest_label}.",
        f"Peak value is {peak_value:,.2f} in {peak_label}.",
    ]


def rebuild_precomputed_analytics(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None = None,
) -> None:
    normalized_dataset = _normalize_dataset_type(dataset_type)
    if normalized_dataset not in {"sales", "claims"}:
        return None

    metrics = SALES_METRICS if normalized_dataset == "sales" else CLAIMS_METRICS
    sources = _affected_sources(source)

    # Remove stale precomputed payloads first and persist that clear in its own
    # transaction. This guarantees APIs don't keep serving old values if a long
    # rebuild fails midway due DB/network interruptions.
    for src in sources:
        clear_precomputed_for_tag(
            db=db,
            source=src,
            dataset_type=normalized_dataset,
            job_id=job_id,
        )
    db.commit()

    for src in sources:
        ranges = _ranges_for_source(
            db=db,
            source=src,
            dataset_type=normalized_dataset,
            job_id=job_id,
        )
        dimensions = _dimensions_for_source(src)

        for from_date, to_date in ranges:
            try:
                summary = compute_summary_values(
                    db=db,
                    job_id=job_id,
                    source=src,
                    dataset_type=normalized_dataset,
                    from_date=from_date,
                    to_date=to_date,
                )
            except Exception:
                logger.exception(
                    "Failed precompute summary source=%s dataset=%s from=%s to=%s",
                    src,
                    normalized_dataset,
                    from_date,
                    to_date,
                )
                summary = {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }

            upsert_precomputed_summary(
                db=db,
                source=src,
                dataset_type=normalized_dataset,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                summary=summary if isinstance(summary, dict) else {},
            )

            for dimension in dimensions:
                bucket_values: list[str | None] = [None]
                if dimension in {"month", "date"}:
                    bucket_values = ["month"]

                for metric in metrics:
                    for bucket in bucket_values:
                        try:
                            rows = compute_by_dimension_rows(
                                db=db,
                                job_id=job_id,
                                source=src,
                                dataset_type=normalized_dataset,
                                dimension=dimension,
                                metric=metric,
                                bucket=bucket,
                                from_date=from_date,
                                to_date=to_date,
                            )
                        except Exception:
                            logger.exception(
                                "Failed precompute graph source=%s dataset=%s dimension=%s metric=%s bucket=%s from=%s to=%s",
                                src,
                                normalized_dataset,
                                dimension,
                                metric,
                                bucket,
                                from_date,
                                to_date,
                            )
                            rows = []

                        upsert_precomputed_graph(
                            db=db,
                            source=src,
                            dataset_type=normalized_dataset,
                            job_id=job_id,
                            dimension=dimension,
                            metric=metric,
                            bucket=bucket,
                            from_date=from_date,
                            to_date=to_date,
                            rows=rows,
                        )

                        compare_mode = src == "samsung"
                        insights = _derive_rule_based_insights(
                            rows=rows,
                            dimension=dimension,
                            metric=metric,
                            compare_mode=compare_mode,
                        )
                        upsert_precomputed_insights(
                            db=db,
                            source=src,
                            dataset_type=normalized_dataset,
                            job_id=job_id,
                            dimension=dimension,
                            metric=metric,
                            bucket=bucket,
                            from_date=from_date,
                            to_date=to_date,
                            compare_mode=compare_mode,
                            insights=insights,
                            model="rule-based-precomputed",
                        )
        # Persist each source chunk independently to reduce long transaction pressure.
        db.commit()

    logger.info(
        "Precompute rebuild complete source=%s dataset=%s job_id=%s",
        source,
        normalized_dataset,
        job_id,
    )
    return None


def rebuild_precomputed_for_all_tags(
    *,
    db: Session,
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    query = db.query(DataRow.source, DataRow.dataset_type, DataRow.job_id).distinct()
    resolved_source = None
    if source:
        resolved_source, _ = _normalize_source(source)
        if resolved_source == "samsung":
            query = query.filter(
                DataRow.source.in_(["samsung", *SAMSUNG_PARTNER_SOURCES, "samsung_vijay_sales"])
            )
        elif resolved_source:
            query = query.filter(DataRow.source == resolved_source)

    normalized_dataset = _normalize_dataset_type(dataset_type) if dataset_type else None
    if normalized_dataset:
        query = query.filter(DataRow.dataset_type == normalized_dataset)
    if job_id is not None:
        query = query.filter(DataRow.job_id == (job_id or None))

    tags = list(query.all())

    # Also rebuild aggregate (no job filter) snapshots so dashboards without a selected tag
    # refresh correctly after tag-specific uploads.
    if job_id is None:
        agg_query = db.query(DataRow.source, DataRow.dataset_type).distinct()
        if resolved_source == "samsung":
            agg_query = agg_query.filter(
                DataRow.source.in_(["samsung", *SAMSUNG_PARTNER_SOURCES, "samsung_vijay_sales"])
            )
        elif resolved_source:
            agg_query = agg_query.filter(DataRow.source == resolved_source)
        if normalized_dataset:
            agg_query = agg_query.filter(DataRow.dataset_type == normalized_dataset)

        existing = {(src, ds, jb) for src, ds, jb in tags}
        for src, ds in agg_query.all():
            key = (src, ds, None)
            if key not in existing:
                tags.append(key)
                existing.add(key)

    completed = 0
    for src, ds, jb in tags:
        try:
            rebuild_precomputed_analytics(
                db=db,
                source=src,
                dataset_type=ds,
                job_id=jb,
            )
            completed += 1
        except Exception:
            db.rollback()
            logger.exception(
                "Failed full precompute rebuild for source=%s dataset=%s job_id=%s",
                src,
                ds,
                jb,
            )
    return {
        "tags_found": len(tags),
        "tags_completed": completed,
    }
