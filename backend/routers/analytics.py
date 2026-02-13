# routers/analytics.py

from typing import Any

from fastapi import APIRouter, Query, Depends
import pandas as pd
from sqlalchemy.orm import Session
from collections import Counter
from datetime import datetime

from db.deps import get_db
from services.analytics import ENGINE_REGISTRY
from services.analytics_repository import get_dataframe
from services.analytics_engine import (
    aggregate_by_dimension,
    filter_by_date_range,
)
from models.data_rows import DataRow
router = APIRouter(prefix="/analytics", tags=["analytics"])


def _normalize_source(source: str) -> tuple[str, str]:
    source_key = source.lower().strip()
    # normalize known aliases
    if source_key in {"samsung_vs", "samsung_vijay_sales"}:
        resolved = "samsung_vs"
    elif source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        resolved = "reliance"
    elif source_key in {"godrej", "goodrej", "goddrej"}:
        resolved = "godrej"
    else:
        resolved = source_key

    # normalize samsung variants for engine lookup only
    engine_key = "samsung" if resolved.startswith("samsung") else resolved
    return resolved, engine_key


def _current_month_cap() -> pd.Timestamp:
    now = datetime.now()
    start = pd.Timestamp(year=now.year, month=now.month, day=1)
    return start + pd.offsets.MonthEnd(0)


def _parse_series(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, format="mixed", errors="coerce")
    except TypeError:
        return pd.to_datetime(series, errors="coerce")


def _clip_to_current_month(series: pd.Series) -> pd.Series:
    cap = _current_month_cap()
    return series.where(series <= cap, cap)


def _sanitize_range(
    from_date: str | None,
    to_date: str | None,
) -> tuple[str | None, str | None]:
    cap = _current_month_cap()
    from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
    to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None

    if from_dt is not None and from_dt is not pd.NaT and from_dt > cap:
        from_dt = cap
    if to_dt is not None and to_dt is not pd.NaT and to_dt > cap:
        to_dt = cap

    if from_dt is not None and to_dt is not None and from_dt > to_dt:
        from_dt, to_dt = to_dt, from_dt

    safe_from = from_dt.date().isoformat() if from_dt is not None and from_dt is not pd.NaT else None
    safe_to = to_dt.date().isoformat() if to_dt is not None and to_dt is not pd.NaT else None
    return safe_from, safe_to


def _latest_from_columns(df: pd.DataFrame, columns: list[str]) -> pd.Timestamp | None:
    if df is None or df.empty:
        return None
    best: pd.Timestamp | None = None
    for col in columns:
        if col not in df.columns:
            continue
        series = _parse_series(df[col]).dropna()
        if series.empty:
            continue
        series = _clip_to_current_month(series)
        current = series.max()
        if best is None or current > best:
            best = current
    return best


def _to_safe_key(key: str) -> str:
    return re.sub(r"[()%'.]", "", re.sub(r"\s+", "_", (key or "").strip().lower()))


def _bounds_from_columns(df: pd.DataFrame, columns: list[str]) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if df is None or df.empty:
        return None, None
    min_found: pd.Timestamp | None = None
    max_found: pd.Timestamp | None = None
    for col in columns:
        if col not in df.columns:
            continue
        series = _parse_series(df[col]).dropna()
        if series.empty:
            continue
        series = _clip_to_current_month(series)
        local_min = series.min()
        local_max = series.max()
        if min_found is None or local_min < min_found:
            min_found = local_min
        if max_found is None or local_max > max_found:
            max_found = local_max
    return min_found, max_found


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
    from_date, to_date = _sanitize_range(from_date, to_date)
    resolved_source, engine_key = _normalize_source(source)

    # ==============================
    # ENGINE PATH (SAMSUNG SALES)
    # ==============================
    if engine_key in ENGINE_REGISTRY and dataset_type in {"sales", "claims"}:
        engine_cls = ENGINE_REGISTRY[engine_key]

        # Samsung "overview" should return both partners in one response so the frontend
        # doesn't need to fire 2 requests per graph (vs + croma).
        if resolved_source == "samsung" and engine_key == "samsung":
            dim_key = _to_safe_key(dimension or "")
            metric_key = _to_safe_key(metric or "")
            merged: dict[str, dict[str, Any]] = {}

            def _merge(rows: list[dict[str, Any]], out_key: str):
                for row in rows or []:
                    if not isinstance(row, dict):
                        continue

                    # Engine outputs often use title-cased keys ("Month") while the API
                    # dimension param is lower ("month"). Match by safe-key.
                    safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
                    dim_col = safe_map.get(dim_key) or safe_map.get(_to_safe_key(dimension or ""))
                    metric_col = safe_map.get(metric_key) or safe_map.get(_to_safe_key(metric or ""))

                    dim_val = row.get(dim_col) if dim_col is not None else None
                    if dim_val is None:
                        continue
                    key = str(dim_val)
                    if key not in merged:
                        merged[key] = {dim_key: dim_val}

                    value = row.get(metric_col) if metric_col is not None else row.get(metric, 0)
                    try:
                        merged[key][out_key] = float(value or 0)
                    except Exception:
                        merged[key][out_key] = 0.0

            for src, out_key in [("samsung_vs", "samsung_vs"), ("samsung_croma", "samsung_croma")]:
                engine = engine_cls(
                    db=db,
                    job_id=job_id,
                    source=src,
                    dataset_type=dataset_type,
                    from_date=from_date,
                    to_date=to_date,
                )
                _merge(engine.compute_by_dimension(dimension=dimension, metric=metric) or [], out_key)

            out_rows = list(merged.values())
            for row in out_rows:
                row["samsung_vs"] = row.get("samsung_vs", 0.0)
                row["samsung_croma"] = row.get("samsung_croma", 0.0)

            if dim_key in {"month", "date"}:
                out_rows.sort(key=lambda r: str(r.get(dim_key, "")))
            return out_rows

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
    from_date, to_date = _sanitize_range(from_date, to_date)
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

    def _sum_first_available(*names: str) -> float:
        normalized = {str(col).strip().lower().replace(" ", "_"): col for col in df.columns}
        for raw_name in names:
            key = raw_name.strip().lower().replace(" ", "_")
            actual = normalized.get(key)
            if actual is None:
                continue
            return float(pd.to_numeric(df[actual], errors="coerce").fillna(0).sum())
        return 0.0

    return {
        "gross_premium": _sum_first_available(
            "amount",
            "gross_premium",
            "gross premium",
            "customer_premium",
            "customer premium",
        ),
        "earned_premium": _sum_first_available(
            "earned_premium",
            "earned premium",
            "earned_amount",
            "earned amount",
        ),
        "zopper_earned_premium": _sum_first_available(
            "earned_zopper",
            "zopper_earned_premium",
            "zopper earned premium",
            "zopper_earned_amount",
        ),
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
    from_date, to_date = _sanitize_range(from_date, to_date)
    resolved_source, _ = _normalize_source(source)

    sales_start_columns = [
        "Start_Date",
        "Start Date",
        "Plan Start Date",
        "Warranty Start Date",
        "Warranty Purchase Date",
        "Product Purchased Date",
        "Date",
    ]
    claims_date_columns = [
        "Day of Call_Date",
        "Call_Date",
        "Call Date",
        "Payment_date",
        "Payment Date",
        "Warranty Purchase Date",
        "Warranty Start Date",
        "Product Purchased Date",
        "Date",
    ]

    def _latest_for_source(src: str) -> pd.Timestamp | None:
        df = get_dataframe(
            db=db,
            job_id=job_id,
            source=src,
            dataset_type=dataset_type,
        )
        if dataset_type == "sales":
            return _latest_from_columns(df, sales_start_columns)
        return _latest_from_columns(df, claims_date_columns)

    if resolved_source == "samsung":
        latest_values = [_latest_for_source("samsung_vs"), _latest_for_source("samsung_croma")]
        latest_values = [v for v in latest_values if v is not None]
        latest = max(latest_values) if latest_values else None
    else:
        latest = _latest_for_source(resolved_source)

    return {"data_upto": latest.date().isoformat() if latest is not None else None}


@router.get("/date-bounds")
def analytics_date_bounds(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    db: Session = Depends(get_db),
):
    resolved_source, _ = _normalize_source(source)

    def _bounds_for_source(src: str):
        df = get_dataframe(
            db=db,
            job_id=job_id,
            source=src,
            dataset_type=dataset_type,
        )
        if dataset_type == "sales":
            return _bounds_from_columns(
                df,
                [
                    "Start_Date",
                    "Start Date",
                    "Plan Start Date",
                    "Warranty Start Date",
                    "Date",
                ],
            )
        return _bounds_from_columns(
            df,
            ["Day of Call_Date", "Call_Date", "Call Date", "Date"],
        )

    if resolved_source == "samsung":
        vs_min, vs_max = _bounds_for_source("samsung_vs")
        cr_min, cr_max = _bounds_for_source("samsung_croma")
        min_date = min(d for d in [vs_min, cr_min] if d is not None) if (vs_min or cr_min) else None
        max_date = max(d for d in [vs_max, cr_max] if d is not None) if (vs_max or cr_max) else None
    else:
        min_date, max_date = _bounds_for_source(resolved_source)

    cap = _current_month_cap()
    if max_date is not None and max_date > cap:
        max_date = cap
    if min_date is not None and min_date > cap:
        min_date = cap
    if min_date is not None and max_date is not None and min_date > max_date:
        min_date = max_date

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


@router.get("/data-coverage")
def analytics_data_coverage(
    source: str | None = Query(None),
    dataset_type: str | None = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(DataRow.source, DataRow.dataset_type, DataRow.data)
    if source:
        resolved_source, _ = _normalize_source(source)
        if resolved_source.startswith("godrej"):
            q = q.filter(
                (DataRow.source.ilike("godrej%"))
                | (DataRow.source.ilike("goodrej%"))
                | (DataRow.source.ilike("goddrej%"))
            )
        else:
            q = q.filter(DataRow.source == resolved_source)
    if dataset_type:
        q = q.filter(DataRow.dataset_type == dataset_type)

    rows = q.all()
    if not rows:
        return {"items": []}

    date_candidates = [
        "Date",
        "Start_Date",
        "Start Date",
        "Plan Start Date",
        "Month",
        "Month Name",
        "Month_Name",
        "Warranty Start Date",
        "Day of Call_Date",
        "Call_Date",
        "Payment_date",
    ]
    field_candidates = [
        "Amount",
        "Claim_Amount",
        "Customer Premium",
        "Channel",
        "Product_Category",
        "State",
    ]

    grouped: dict[tuple[str, str], dict] = {}

    def _parse_date(value):
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        for fmt in [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%d/%b/%y",
            "%d/%b/%Y",
            "%d-%b-%y",
            "%d-%b-%Y",
            "%b-%y",
            "%b %y",
            "%Y%m",
            "%Y-%m",
        ]:
            try:
                dt = datetime.strptime(s, fmt)
                if fmt in {"%b-%y", "%b %y", "%Y%m", "%Y-%m"}:
                    dt = dt.replace(day=1)
                return dt.date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(s.replace("Z", "")).date()
        except ValueError:
            return None

    for src, ds, data in rows:
        src_key = (src or "").strip().lower()
        ds_key = (ds or "").strip().lower()
        key = (src_key, ds_key)
        if key not in grouped:
            grouped[key] = {
                "source": src_key,
                "dataset_type": ds_key,
                "rows": 0,
                "dated_rows": 0,
                "min_date": None,
                "max_date": None,
                "fields": {f: 0 for f in field_candidates},
            }

        item = grouped[key]
        item["rows"] += 1

        payload = data if isinstance(data, dict) else {}
        lower_map = {str(k).strip().lower(): k for k in payload.keys()}

        for f in field_candidates:
            if f.lower() in lower_map:
                item["fields"][f] += 1

        parsed = None
        for col in date_candidates:
            if col.lower() in lower_map:
                parsed = _parse_date(payload.get(lower_map[col.lower()]))
                if parsed is not None:
                    break
        if parsed is None:
            continue

        item["dated_rows"] += 1
        if item["min_date"] is None or parsed < item["min_date"]:
            item["min_date"] = parsed
        if item["max_date"] is None or parsed > item["max_date"]:
            item["max_date"] = parsed

    items = []
    for _, v in sorted(grouped.items(), key=lambda kv: (kv[1]["source"], kv[1]["dataset_type"])):
        items.append(
            {
                "source": v["source"],
                "dataset_type": v["dataset_type"],
                "rows": v["rows"],
                "dated_rows": v["dated_rows"],
                "min_date": v["min_date"].isoformat() if v["min_date"] else None,
                "max_date": v["max_date"].isoformat() if v["max_date"] else None,
                "fields": v["fields"],
            }
        )
    return {"items": items}
