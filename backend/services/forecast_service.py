from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from services.precomputed_repository import get_precomputed_graph
from services.samsung_partner_config import normalize_samsung_source

_MONTH_MAP = {
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


@dataclass(frozen=True)
class ForecastPoint:
    period_start: date
    value: float


def _normalize_source_key(source: str | None) -> str:
    source_key = (source or "").strip().lower()
    samsung_source = normalize_samsung_source(source_key)
    if samsung_source:
        return samsung_source
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return source_key


def _normalize_dataset_type(dataset_type: str | None) -> str:
    return (dataset_type or "sales").strip().lower()


def _normalize_metric(metric: str | None, dataset_type: str) -> tuple[str, str]:
    metric_key = (metric or "gross_premium").strip().lower()
    if dataset_type == "claims":
        if metric_key in {"gross_premium", "claims", "claim_amount", "claims_cost"}:
            return "gross_premium", "claims"
        if metric_key in {"quantity", "count", "claim_count"}:
            return "quantity", "quantity"
        if metric_key == "net_claims":
            return "net_claims", "net_claims"
        if metric_key == "loss_ratio":
            return "loss_ratio", "loss_ratio"
        return "gross_premium", "claims"

    if metric_key in {"quantity", "count", "units_sold"}:
        return "quantity", "quantity"
    if metric_key == "earned_premium":
        return "earned_premium", "earned_premium"
    if metric_key == "zopper_earned_premium":
        return "zopper_earned_premium", "zopper_earned_premium"
    return "gross_premium", "gross_premium"


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def _parse_month_start(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)

    raw = str(value).strip()
    if not raw:
        return None

    short_match = re.match(r"^([A-Za-z]{3,9})[-/\s](\d{2}|\d{4})$", raw)
    if short_match:
        month = _MONTH_MAP.get(short_match.group(1)[:3].lower())
        if month:
            year_text = short_match.group(2)
            year = int(year_text) + 2000 if len(year_text) == 2 else int(year_text)
            if 1900 <= year <= 2200:
                return date(year, month, 1)

    year_month_match = re.match(r"^(\d{4})[-/](\d{1,2})(?:[-/]\d{1,2})?$", raw)
    if year_month_match:
        year = int(year_month_match.group(1))
        month = int(year_month_match.group(2))
        if 1 <= month <= 12:
            return date(year, month, 1)

    try:
        parsed = datetime.fromisoformat(raw[:19].replace("Z", "+00:00"))
        return date(parsed.year, parsed.month, 1)
    except ValueError:
        return None


def _next_month_start(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _pick_dimension_key(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    preferred = ("month", "date", "fiscal_month", "month_year")
    first = rows[0]
    for key in preferred:
        if key in first:
            return key
    for raw_key in first.keys():
        safe_key = re.sub(r"[^a-z0-9]", "", str(raw_key).strip().lower())
        if "month" in safe_key or "date" in safe_key:
            return str(raw_key)
    return None


def _extract_monthly_points(rows: list[dict[str, Any]], metric_key: str) -> list[ForecastPoint]:
    dimension_key = _pick_dimension_key(rows)
    if dimension_key is None:
        return []

    aggregated: dict[date, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        month_start = _parse_month_start(row.get(dimension_key))
        if month_start is None:
            continue

        value = _to_number(row.get(metric_key))
        if value is None:
            total = 0.0
            found = False
            for raw_key, raw_value in row.items():
                if raw_key == dimension_key:
                    continue
                safe_key = str(raw_key).strip().lower()
                if safe_key.startswith("tooltip_"):
                    continue
                numeric = _to_number(raw_value)
                if numeric is None:
                    continue
                total += float(numeric)
                found = True
            if not found:
                continue
            value = total

        aggregated[month_start] = aggregated.get(month_start, 0.0) + float(value)

    return [
        ForecastPoint(period_start=month_start, value=float(value))
        for month_start, value in sorted(aggregated.items(), key=lambda item: item[0])
    ]


def load_monthly_history(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    metric: str,
    job_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[ForecastPoint]:
    source_key = _normalize_source_key(source)
    dataset_key = _normalize_dataset_type(dataset_type)
    _public_metric, graph_metric = _normalize_metric(metric, dataset_key)
    rows = get_precomputed_graph(
        db=db,
        source=source_key,
        dataset_type=dataset_key,
        job_id=job_id,
        dimension="month",
        metric=graph_metric,
        bucket="month",
        from_date=from_date,
        to_date=to_date,
    ) or []
    if not rows:
        from routers.analytics import compute_by_dimension_rows

        rows = compute_by_dimension_rows(
            db=db,
            job_id=job_id,
            source=source_key,
            dataset_type=dataset_key,
            dimension="month",
            metric=graph_metric,
            bucket="month",
            from_date=from_date,
            to_date=to_date,
        )
    return _extract_monthly_points(rows, graph_metric)


def combine_monthly_history(series_list: list[list[ForecastPoint]]) -> list[ForecastPoint]:
    aggregated: dict[date, float] = {}
    for series in series_list:
        for point in series:
            aggregated[point.period_start] = aggregated.get(point.period_start, 0.0) + float(point.value)
    return [
        ForecastPoint(period_start=month_start, value=float(value))
        for month_start, value in sorted(aggregated.items(), key=lambda item: item[0])
    ]


def forecast_monthly_points(
    history: list[ForecastPoint],
    *,
    horizon_months: int = 6,
) -> list[ForecastPoint]:
    if len(history) < 2 or horizon_months <= 0:
        return []

    working = list(history)
    forecast: list[ForecastPoint] = []
    horizon = max(1, min(int(horizon_months), 24))

    for _ in range(horizon):
        recent = working[-min(12, len(working)) :]
        values = [float(point.value) for point in recent]
        deltas = [values[idx] - values[idx - 1] for idx in range(1, len(values))]
        delta_count = min(6, len(deltas))
        delta_weights = list(range(1, delta_count + 1))
        weighted_delta = (
            sum(delta * weight for delta, weight in zip(deltas[-delta_count:], delta_weights)) / float(sum(delta_weights))
            if delta_weights
            else 0.0
        )

        growth_rates: list[float] = []
        for idx in range(1, len(values)):
            prev = values[idx - 1]
            curr = values[idx]
            if abs(prev) < 1e-9:
                continue
            growth_rates.append((curr - prev) / prev)
        growth_count = min(6, len(growth_rates))
        growth_weights = list(range(1, growth_count + 1))
        weighted_growth = (
            sum(rate * weight for rate, weight in zip(growth_rates[-growth_count:], growth_weights)) / float(sum(growth_weights))
            if growth_weights
            else None
        )

        last_point = working[-1]
        next_period = _next_month_start(last_point.period_start)
        trend_projection = (
            last_point.value * (1.0 + float(weighted_growth))
            if weighted_growth is not None
            else last_point.value + weighted_delta
        )

        projected = trend_projection
        if len(working) >= 12:
            same_month_values = [point.value for point in working if point.period_start.month == next_period.month]
            if same_month_values:
                seasonal_weights = list(range(1, len(same_month_values) + 1))
                seasonal_baseline = sum(
                    value * weight for value, weight in zip(same_month_values, seasonal_weights)
                ) / float(sum(seasonal_weights))
                projected = (trend_projection * 0.65) + (seasonal_baseline * 0.35)

        projected = max(0.0, float(projected))
        point = ForecastPoint(period_start=next_period, value=projected)
        working.append(point)
        forecast.append(point)

    return forecast


def _financial_year_label(period_start: date) -> str:
    start_year = period_start.year if period_start.month >= 4 else period_start.year - 1
    return f"{start_year} - {start_year + 1}"


def _financial_year_start(period_start: date) -> date:
    start_year = period_start.year if period_start.month >= 4 else period_start.year - 1
    return date(start_year, 4, 1)


def aggregate_financial_year(points: list[ForecastPoint]) -> list[ForecastPoint]:
    aggregated: dict[date, float] = {}
    for point in points:
        fy_start = _financial_year_start(point.period_start)
        aggregated[fy_start] = aggregated.get(fy_start, 0.0) + float(point.value)
    return [
        ForecastPoint(period_start=fy_start, value=float(value))
        for fy_start, value in sorted(aggregated.items(), key=lambda item: item[0])
    ]


def serialize_points(points: list[ForecastPoint], *, grain: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        label = _financial_year_label(point.period_start) if grain == "financial_year" else point.period_start.strftime("%b %y")
        rows.append(
            {
                "label": label,
                "period_start": point.period_start.isoformat(),
                "value": float(point.value),
            }
        )
    return rows


def build_forecast_response(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    metric: str,
    job_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    horizon_months: int = 6,
    grain: str = "month",
) -> dict[str, Any]:
    source_key = _normalize_source_key(source)
    dataset_key = _normalize_dataset_type(dataset_type)
    public_metric, _graph_metric = _normalize_metric(metric, dataset_key)
    normalized_grain = "financial_year" if (grain or "").strip().lower() == "financial_year" else "month"

    history = load_monthly_history(
        db=db,
        source=source_key,
        dataset_type=dataset_key,
        metric=public_metric,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    forecast = forecast_monthly_points(history, horizon_months=horizon_months)

    history_points = aggregate_financial_year(history) if normalized_grain == "financial_year" else history
    forecast_points = aggregate_financial_year(forecast) if normalized_grain == "financial_year" else forecast

    return {
        "source": source_key,
        "dataset_type": dataset_key,
        "metric": public_metric,
        "grain": normalized_grain,
        "horizon_months": max(1, min(int(horizon_months), 24)),
        "model": "seasonal-weighted-trend-v1",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "history": serialize_points(history_points, grain=normalized_grain),
        "forecast": serialize_points(forecast_points, grain=normalized_grain),
        "monthly_history": serialize_points(history, grain="month"),
        "monthly_forecast": serialize_points(forecast, grain="month"),
    }
