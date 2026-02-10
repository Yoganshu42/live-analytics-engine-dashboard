# routers/analytics.py

from fastapi import APIRouter, Query, Depends
import pandas as pd
from sqlalchemy.orm import Session
from collections import Counter

from db.deps import get_db
from services.analytics import ENGINE_REGISTRY
from services.analytics_repository import get_dataframe
from services.analytics_engine import (
    aggregate_by_dimension,
    get_latest_date,
    filter_by_date_range,
    get_date_bounds,
)
from models.data_rows import DataRow

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _normalize_source(source: str) -> tuple[str, str]:
    source_key = source.lower().strip()
    # normalize known aliases
    if source_key == "samsung_vs":
        resolved = "samsung_vijay_sales"
    elif source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        resolved = "reliance"
    elif source_key in {"goodrej", "goddrej"}:
        resolved = "godrej"
    else:
        resolved = source_key

    # normalize samsung variants for engine lookup only
    engine_key = "samsung" if resolved.startswith("samsung") else resolved
    return resolved, engine_key


@router.get("/by-dimension")
def analytics_by_dimension(
    job_id: str | None = Query(None),
    dimension: str = Query(...),
    metric: str = Query(...),
    source: str = Query(...),
    dataset_type: str = Query(...),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    resolved_source, engine_key = _normalize_source(source)

    # ==============================
    # ENGINE PATH (SAMSUNG SALES)
    # ==============================
    if engine_key in ENGINE_REGISTRY and dataset_type in {"sales", "claims"}:
        engine_cls = ENGINE_REGISTRY[engine_key]
        engine = engine_cls(
            db=db,
            job_id=job_id,
            source=resolved_source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )
        return engine.compute_by_dimension(
            dimension=dimension,
            metric=metric,
        )

    df = get_dataframe(
        db=db,
        job_id=job_id,
        source=resolved_source,
        dataset_type=dataset_type,
    )

    if df is None or df.empty:
        return []

    df = filter_by_date_range(df, dataset_type, from_date, to_date)
    if df is None or df.empty:
        return []

    def normalize(s: str):
        return s.lower().replace("_", "").replace(" ", "").strip()

    col_map = {normalize(c): c for c in df.columns}
    dim_key = normalize(dimension)

    if dim_key not in col_map:
        return []

    real_dimension = col_map[dim_key]

    out_df = aggregate_by_dimension(
        df=df,
        dimension=real_dimension,
        metric=metric,
    )

    if out_df is None or out_df.empty:
        return []

    return out_df.to_dict(orient="records")


@router.get("/summary")
def analytics_summary(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    resolved_source, engine_key = _normalize_source(source)

    if engine_key in ENGINE_REGISTRY and dataset_type in {"sales", "claims"}:
        engine_cls = ENGINE_REGISTRY[engine_key]
        if resolved_source == "samsung" and dataset_type == "sales":
            total = {
                "gross_premium": 0.0,
                "earned_premium": 0.0,
                "zopper_earned_premium": 0.0,
                "units_sold": 0,
            }
            for src in ["samsung_vs", "samsung_croma"]:
                engine = engine_cls(
                    db=db,
                    job_id=job_id,
                    source=src,
                    dataset_type=dataset_type,
                    from_date=from_date,
                    to_date=to_date,
                )
                summary = engine.compute_summary()
                total["gross_premium"] += float(summary.get("gross_premium", 0) or 0)
                total["earned_premium"] += float(summary.get("earned_premium", 0) or 0)
                total["zopper_earned_premium"] += float(summary.get("zopper_earned_premium", 0) or 0)
                total["units_sold"] += int(summary.get("units_sold", 0) or 0)
            return total

        engine = engine_cls(
            db=db,
            job_id=job_id,
            source=resolved_source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )
        return engine.compute_summary()

    df = get_dataframe(
        db=db,
        job_id=job_id,
        source=resolved_source,
        dataset_type=dataset_type,
    )

    if df is None or df.empty:
        return {
            "gross_premium": 0,
            "earned_premium": 0,
            "zopper_earned_premium": 0,
            "units_sold": 0,
        }

    df = filter_by_date_range(df, dataset_type, from_date, to_date)
    if df is None or df.empty:
        return {
            "gross_premium": 0,
            "earned_premium": 0,
            "zopper_earned_premium": 0,
            "units_sold": 0,
        }

    if dataset_type == "claims":
        def _sum_col(*names: str) -> float:
            for name in names:
                if name in df.columns:
                    return float(pd.to_numeric(df[name], errors="coerce").fillna(0).sum())
            return 0.0

        claims_total = _sum_col("Net Amount", "Net_Amount", "Net Claims", "Net_Claims")
        otd_total = _sum_col(
            "OTD Amount",
            "OTD_Amount",
            "One time deductible",
            "One Time Deductible",
        )
        net_claims = claims_total - otd_total

        return {
            "gross_premium": claims_total,
            "earned_premium": net_claims,
            "zopper_earned_premium": net_claims,
            "units_sold": int(len(df)),
        }

    return {
        "gross_premium": float(df.get("Amount", 0).sum()),
        "earned_premium": float(df.get("earned_premium", 0).sum()),
        "zopper_earned_premium": float(df.get("earned_zopper", 0).sum()),
        "units_sold": int(len(df)),
    }


@router.get("/last-updated")
def analytics_last_updated(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    resolved_source, _ = _normalize_source(source)

    def _latest_for_source(src: str):
        if src.startswith("reliance") and dataset_type == "sales":
            engine_cls = ENGINE_REGISTRY.get("reliance")
            if engine_cls is not None:
                engine = engine_cls(
                    db=db,
                    job_id=job_id,
                    source=src,
                    dataset_type=dataset_type,
                    from_date=from_date,
                    to_date=to_date,
                )
                data = engine.load_data()
                df = data.get("sales")
                if df is None or df.empty:
                    return None
                if "_ew" in df.columns:
                    df = df[df["_ew"] != True]
                return get_latest_date(df)

        df = get_dataframe(
            db=db,
            job_id=job_id,
            source=src,
            dataset_type=dataset_type,
        )
        df = filter_by_date_range(df, dataset_type, from_date, to_date)
        return get_latest_date(df)

    if resolved_source == "samsung":
        latest_vs = _latest_for_source("samsung_vijay_sales")
        latest_croma = _latest_for_source("samsung_croma")
        latest = latest_vs
        if latest is None or (latest_croma is not None and latest_croma > latest):
            latest = latest_croma
    else:
        latest = _latest_for_source(resolved_source)

    if latest is None:
        return {"data_upto": None}

    date_str = latest.date().isoformat()
    return {
        "data_upto": date_str,
    }


@router.get("/date-bounds")
def analytics_date_bounds(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    db: Session = Depends(get_db),
):
    resolved_source, _ = _normalize_source(source)

    def _bounds_for_source(src: str):
        if src.startswith("reliance") and dataset_type == "sales":
            engine_cls = ENGINE_REGISTRY.get("reliance")
            if engine_cls is not None:
                engine = engine_cls(
                    db=db,
                    job_id=job_id,
                    source=src,
                    dataset_type=dataset_type,
                    from_date=None,
                    to_date=None,
                )
                data = engine.load_data()
                df = data.get("sales")
                if df is None or df.empty:
                    return None, None
                if "_ew" in df.columns:
                    df = df[df["_ew"] != True]
                return get_date_bounds(df, dataset_type)

        df = get_dataframe(
            db=db,
            job_id=job_id,
            source=src,
            dataset_type=dataset_type,
        )
        return get_date_bounds(df, dataset_type)

    if resolved_source == "samsung":
        vs_min, vs_max = _bounds_for_source("samsung_vijay_sales")
        cr_min, cr_max = _bounds_for_source("samsung_croma")
        min_date = min(d for d in [vs_min, cr_min] if d is not None) if (vs_min or cr_min) else None
        max_date = max(d for d in [vs_max, cr_max] if d is not None) if (vs_max or cr_max) else None
    else:
        min_date, max_date = _bounds_for_source(resolved_source)

    if resolved_source.startswith("reliance") and dataset_type == "sales":
        clamp_min = pd.Timestamp("2025-07-01")
        if min_date is None or min_date < clamp_min:
            min_date = clamp_min
        clamp_max = pd.Timestamp("2025-12-31")
        if max_date is None or max_date > clamp_max:
            max_date = clamp_max

    return {
        "min_date": min_date.date().isoformat() if min_date is not None else None,
        "max_date": max_date.date().isoformat() if max_date is not None else None,
    }


@router.get("/distinct")
def analytics_distinct_values(
    source: str = Query(...),
    dataset_type: str = Query(...),
    field: str = Query(...),
    job_id: str | None = Query(None),
    limit: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    resolved_source, _ = _normalize_source(source)

    q = (
        db.query(DataRow.data)
        .filter(DataRow.source == resolved_source)
        .filter(DataRow.dataset_type == dataset_type)
    )
    if job_id:
        q = q.filter(DataRow.job_id == job_id)

    rows = q.all()
    if not rows:
        return {"field": field, "values": []}

    values = []
    for r in rows:
        data = r[0] if isinstance(r, tuple) else r.data
        if isinstance(data, dict) and field in data:
            values.append(data.get(field))

    counter = Counter(values)
    out = [{"value": k, "count": v} for k, v in counter.most_common(limit)]
    return {"field": field, "values": out}
