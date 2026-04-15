# routers/analytics.py

from typing import Any
import re
import time
import logging
import threading

from fastapi import APIRouter, Query, Depends
from fastapi.responses import ORJSONResponse
import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from collections import Counter
from datetime import datetime

from db.deps import get_db
from db.session import SessionLocal
from services.analytics import ENGINE_REGISTRY
from services.analytics_repository import get_dataframe
from services.analytics_engine import (
    aggregate_by_dimension,
    filter_by_date_range,
)
from models.data_rows import DataRow
from models.manual_updates import ManualUpdateMarker
from models.precomputed_analytics import PrecomputedGraph, PrecomputedSummary
from services.precomputed_repository import (
    get_precomputed_graph,
    get_precomputed_summary,
    upsert_precomputed_graph,
    upsert_precomputed_summary,
)
from services.manual_update_service import mark_manual_update
from services.admin_upload_service import get_latest_available_job_id
from services.forecast_service import build_forecast_response
from services.samsung_partner_config import (
    SAMSUNG_PARTNER_SOURCES,
    SAMSUNG_SOURCE_VARIANTS,
    normalize_samsung_source,
)
router = APIRouter(
    prefix="/analytics",
    tags=["analytics"],
    default_response_class=ORJSONResponse,
)
logger = logging.getLogger(__name__)
APPLIANCE_SOURCES = {"godrej", "hitachi"}

_master_rebuild_lock = threading.Lock()
_master_rebuild_inflight: set[str] = set()
_summary_rebuild_lock = threading.Lock()
_summary_rebuild_inflight: set[str] = set()
_graph_rebuild_lock = threading.Lock()
_graph_rebuild_inflight: set[str] = set()
_batch_engine_state = threading.local()
_DATE_BOUNDS_CACHE_DATASET_SUFFIX = "_date_bounds"
_DATE_BOUNDS_METRIC_CANDIDATES: dict[str, list[str]] = {
    "sales": [
        "quantity",
        "gross_premium",
        "earned_premium",
        "zopper_earned_premium",
    ],
    "claims": [
        "quantity",
        "claims",
        "net_claims",
        "loss_ratio",
    ],
}
_ANNUAL_COMPARISON_DIMENSION = "annual_comparison"
_ANNUAL_COMPARISON_CACHE_BUCKET = "compact_v3"
_ANNUAL_SALES_SUMMARY_METRICS = [
    "gross_premium",
    "earned_premium",
    "zopper_earned_premium",
]
_ANNUAL_SUPPORTED_CLAIMS_METRICS = {
    "claims",
    "net_claims",
    "loss_ratio",
    "quantity",
}
_SALES_SCOPING_CACHE_UPDATED_AT = datetime.fromisoformat("2026-03-29T17:43:21+05:30")
_RELIANCE_BRAND_CACHE_UPDATED_AT = datetime.fromisoformat("2026-03-30T10:43:16+05:30")
_HITACHI_PLAN_CACHE_UPDATED_AT = datetime.fromisoformat("2026-03-30T23:15:00+05:30")
_MASTER_DASHBOARD_CACHE_UPDATED_AT = datetime.fromisoformat("2026-04-13T10:30:00+05:30")
_SUMMARY_SCOPE_REFRESH_SOURCES = {
    "samsung",
    "samsung_vs",
    "samsung_vijay_sales",
    "samsung_croma",
    "samsung_reliance_digital",
    "reliance",
    "godrej",
}
_GRAPH_SCOPE_REFRESH_SOURCES = {
    "reliance",
}


def _annual_comparison_cache_bucket(source: str) -> str:
    # Samsung annual overview payloads before this fix could be built from a
    # stale or duplicated partner merge, so force a fresh cache namespace for
    # that source.
    if source == "samsung":
        return "compact_v7"
    if source == "hitachi":
        return "compact_v5"
    return _ANNUAL_COMPARISON_CACHE_BUCKET


class ByDimensionBatchItem(BaseModel):
    request_key: str | None = None
    job_id: str | None = None
    dimension: str
    metric: str
    source: str
    dataset_type: str
    bucket: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    filter_1_dimension: str | None = None
    filter_1_values: str | None = None
    filter_2_dimension: str | None = None
    filter_2_values: str | None = None


class ByDimensionBatchPayload(BaseModel):
    requests: list[ByDimensionBatchItem] = Field(default_factory=list)


def _get_active_batch_engine_cache() -> dict[tuple[str, ...], Any] | None:
    cache = getattr(_batch_engine_state, "engines", None)
    return cache if isinstance(cache, dict) else None


def _get_or_create_batch_engine(
    *,
    engine_cls: type,
    engine_key: str,
    source: str,
    job_id: str | None,
    dataset_type: str,
    from_date: str | None,
    to_date: str | None,
    allow_shared: bool,
    db: Session,
):
    if not allow_shared:
        return engine_cls(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )

    cache = _get_active_batch_engine_cache()
    if cache is None:
        return engine_cls(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )

    cache_key = (
        engine_key,
        (source or "").strip().lower(),
        (job_id or "").strip(),
        (dataset_type or "").strip().lower(),
        from_date or "",
        to_date or "",
    )
    engine = cache.get(cache_key)
    if engine is None:
        engine = engine_cls(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )
        cache[cache_key] = engine
    return engine


def _normalize_source(source: str) -> tuple[str, str]:
    source_key = source.lower().strip()
    # normalize known aliases
    samsung_source = normalize_samsung_source(source_key)
    if samsung_source:
        resolved = samsung_source
    elif source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        resolved = "reliance"
    elif source_key in {"godrej", "goodrej", "goddrej"}:
        resolved = "godrej"
    elif source_key == "hitachi":
        resolved = "hitachi"
    else:
        resolved = source_key

    # normalize samsung variants for engine lookup only
    engine_key = "samsung" if resolved.startswith("samsung") else resolved
    return resolved, engine_key


def _current_month_cap() -> pd.Timestamp:
    return pd.Timestamp.now().normalize()


def _parse_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.str.replace(r"\.0$", "", regex=True)
    parsed = pd.Series(pd.NaT, index=cleaned.index, dtype="datetime64[ns]")

    month_map = {
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
    month_year = cleaned.str.extract(r"^(?:\d{1,2}[-/\s])?(?P<mon>[A-Za-z]{3,9})[-/\s](?P<yr>\d{2}|\d{4})$")
    month_num = month_year["mon"].str.lower().str.slice(0, 3).map(month_map)
    year_text = month_year["yr"]
    year_num = pd.to_numeric(year_text, errors="coerce")
    is_two_digit = year_text.fillna("").str.len().eq(2)
    year_num = year_num.where(~is_two_digit, year_num + 2000)
    explicit_month = pd.to_datetime({"year": year_num, "month": month_num, "day": 1}, errors="coerce")
    parsed = parsed.fillna(explicit_month)

    yyyymm = cleaned
    yyyymm_mask = yyyymm.str.fullmatch(r"\d{6}")
    normalized = yyyymm.where(
        ~yyyymm_mask,
        yyyymm.str.slice(0, 4) + "-" + yyyymm.str.slice(4, 6) + "-01",
    )
    try:
        generic = pd.to_datetime(normalized, format="mixed", errors="coerce")
    except TypeError:
        generic = pd.to_datetime(normalized, errors="coerce")
    generic = generic.where(generic.dt.year >= 2000)
    parsed = parsed.fillna(generic)

    # Ignore implausible/legacy parsed dates that distort bounds.
    return parsed.where(parsed.dt.year >= 2000)


def _clip_to_current_month(series: pd.Series) -> pd.Series:
    # Do not cap to the current month; uploaded data can legitimately include future periods.
    return series


def _sanitize_range(
    from_date: str | None,
    to_date: str | None,
) -> tuple[str | None, str | None]:
    from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
    to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None

    if from_dt is not None and to_dt is not None and from_dt > to_dt:
        from_dt, to_dt = to_dt, from_dt

    safe_from = from_dt.date().isoformat() if from_dt is not None and from_dt is not pd.NaT else None
    safe_to = to_dt.date().isoformat() if to_dt is not None and to_dt is not pd.NaT else None
    return safe_from, safe_to


def _resolve_job_id_fallback(
    *,
    db: Session,
    resolved_source: str,
    dataset_key: str,
    job_id: str | None,
    context: str,
) -> str | None:
    job_key = (job_id or "").strip()
    if not job_key:
        if (
            resolved_source == "samsung"
            or resolved_source in SAMSUNG_PARTNER_SOURCES
            or resolved_source == "reliance"
            or resolved_source in APPLIANCE_SOURCES
        ):
            logger.info(
                "No job filter supplied for %s/%s (%s); using merged all-tag dataset.",
                resolved_source,
                dataset_key,
                context,
            )
            return None
        implicit_job = get_latest_available_job_id(
            db,
            source=resolved_source,
            dataset_type=dataset_key,
        )
        if implicit_job:
            logger.info(
                "No job filter supplied for %s/%s (%s); defaulting to latest uploaded tag %s",
                resolved_source,
                dataset_key,
                context,
                implicit_job,
            )
            return implicit_job
        return None

    if resolved_source not in {"reliance", *APPLIANCE_SOURCES}:
        return job_key

    def _has_rows(tag: str | None) -> bool:
        query = (
            db.query(func.count(DataRow.id))
            .filter(DataRow.source == resolved_source)
            .filter(DataRow.dataset_type == dataset_key)
        )
        if tag is None:
            query = query.filter(DataRow.job_id.is_(None))
        else:
            query = query.filter(DataRow.job_id == tag)
        return int(query.scalar() or 0) > 0

    if not _has_rows(job_key) and _has_rows(None):
        logger.warning(
            "Job filter %s had no rows for %s/%s; falling back to untagged data.",
            job_key,
            resolved_source,
            dataset_key,
        )
        return None

    return job_key


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


def _normalize_lookup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def _normalize_bucket_value(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _collapse_bucket_value(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", _normalize_bucket_value(value))


_MISSING_LABELS = {"", "nan", "none", "null", "NaN", "None", "NULL"}
_STATE_CODE_TO_NAME: dict[str, str] = {
    "ap": "Andhra Pradesh",
    "ar": "Arunachal Pradesh",
    "as": "Assam",
    "br": "Bihar",
    "cg": "Chhattisgarh",
    "ch": "Chandigarh",
    "dd": "Daman And Diu",
    "dl": "Delhi",
    "dn": "Dadra And Nagar Haveli",
    "ga": "Goa",
    "gj": "Gujarat",
    "hr": "Haryana",
    "hp": "Himachal Pradesh",
    "jh": "Jharkhand",
    "jk": "Jammu And Kashmir",
    "ka": "Karnataka",
    "kl": "Kerala",
    "la": "Ladakh",
    "ld": "Lakshadweep",
    "mh": "Maharashtra",
    "ml": "Meghalaya",
    "mn": "Manipur",
    "mp": "Madhya Pradesh",
    "mz": "Mizoram",
    "nl": "Nagaland",
    "od": "Odisha",
    "or": "Odisha",
    "pb": "Punjab",
    "py": "Puducherry",
    "rj": "Rajasthan",
    "sk": "Sikkim",
    "tg": "Telangana",
    "tn": "Tamil Nadu",
    "tr": "Tripura",
    "ts": "Telangana",
    "uk": "Uttarakhand",
    "up": "Uttar Pradesh",
    "ut": "Uttarakhand",
    "wb": "West Bengal",
}
_STATE_NAME_TO_CANONICAL: dict[str, str] = {
    _normalize_lookup_key(name): name
    for name in sorted(set(_STATE_CODE_TO_NAME.values()))
}
_STATE_NAME_TO_CANONICAL.update(
    {
        "del": "Delhi",
        "delhincr": "Delhi",
        "andamannicobarislands": "Andaman And Nicobar Islands",
        "andamanandnicobarislands": "Andaman And Nicobar Islands",
        "dadraandnagarhavelianddamananddiu": "Dadra And Nagar Haveli And Daman And Diu",
        "jammuandkashmir": "Jammu And Kashmir",
        "ncrdelhi": "Delhi",
        "nctdelhi": "Delhi",
        "newdelhi": "Delhi",
        "orissa": "Odisha",
        "pondicherry": "Puducherry",
        "telengana": "Telangana",
        "uttaranchal": "Uttarakhand",
    }
)
_CITY_TO_STATE_NAME: dict[str, str] = {
    _normalize_lookup_key(city): state
    for city, state in {
        "Agartala": "Tripura",
        "Agra": "Uttar Pradesh",
        "Ahmedabad": "Gujarat",
        "Aizawl": "Mizoram",
        "Ajmer": "Rajasthan",
        "Allahabad": "Uttar Pradesh",
        "Amritsar": "Punjab",
        "Anantapur": "Andhra Pradesh",
        "Asansol": "West Bengal",
        "Aurangabad": "Maharashtra",
        "Belagavi": "Karnataka",
        "Bengaluru": "Karnataka",
        "Bangalore": "Karnataka",
        "Berhampur": "Odisha",
        "Bhilai": "Chhattisgarh",
        "Bhopal": "Madhya Pradesh",
        "Bhubaneswar": "Odisha",
        "Bikaner": "Rajasthan",
        "Bokaro": "Jharkhand",
        "Calicut": "Kerala",
        "Chandigarh": "Chandigarh",
        "Chennai": "Tamil Nadu",
        "Coimbatore": "Tamil Nadu",
        "Cuttack": "Odisha",
        "Dehradun": "Uttarakhand",
        "Dhanbad": "Jharkhand",
        "Dimapur": "Nagaland",
        "Durg": "Chhattisgarh",
        "Durgapur": "West Bengal",
        "Ernakulam": "Kerala",
        "Erode": "Tamil Nadu",
        "Faridabad": "Haryana",
        "Gangtok": "Sikkim",
        "Gandhinagar": "Gujarat",
        "Gaya": "Bihar",
        "Ghaziabad": "Uttar Pradesh",
        "Gorakhpur": "Uttar Pradesh",
        "Greater Noida": "Uttar Pradesh",
        "Guntur": "Andhra Pradesh",
        "Gurgaon": "Haryana",
        "Gurugram": "Haryana",
        "Guwahati": "Assam",
        "Gwalior": "Madhya Pradesh",
        "Haridwar": "Uttarakhand",
        "Howrah": "West Bengal",
        "Hubli": "Karnataka",
        "Hyderabad": "Telangana",
        "Imphal": "Manipur",
        "Indore": "Madhya Pradesh",
        "Itanagar": "Arunachal Pradesh",
        "Jabalpur": "Madhya Pradesh",
        "Jaipur": "Rajasthan",
        "Jalandhar": "Punjab",
        "Jammu": "Jammu And Kashmir",
        "Jamnagar": "Gujarat",
        "Jamshedpur": "Jharkhand",
        "Jodhpur": "Rajasthan",
        "Kadapa": "Andhra Pradesh",
        "Kanpur": "Uttar Pradesh",
        "Karimnagar": "Telangana",
        "Kavaratti": "Lakshadweep",
        "Khammam": "Telangana",
        "Kochi": "Kerala",
        "Kohima": "Nagaland",
        "Kolkata": "West Bengal",
        "Kolhapur": "Maharashtra",
        "Kota": "Rajasthan",
        "Kozhikode": "Kerala",
        "Kurnool": "Andhra Pradesh",
        "Leh": "Ladakh",
        "Lucknow": "Uttar Pradesh",
        "Ludhiana": "Punjab",
        "Madurai": "Tamil Nadu",
        "Mangalore": "Karnataka",
        "Mangaluru": "Karnataka",
        "Margao": "Goa",
        "Meerut": "Uttar Pradesh",
        "Mohali": "Punjab",
        "Mumbai": "Maharashtra",
        "Mumbai Suburban": "Maharashtra",
        "Muzaffarpur": "Bihar",
        "Mysore": "Karnataka",
        "Mysuru": "Karnataka",
        "Nagpur": "Maharashtra",
        "Nashik": "Maharashtra",
        "Nasik": "Maharashtra",
        "Navi Mumbai": "Maharashtra",
        "Nellore": "Andhra Pradesh",
        "New Delhi": "Delhi",
        "Nizamabad": "Telangana",
        "Noida": "Uttar Pradesh",
        "Panaji": "Goa",
        "Panjim": "Goa",
        "Panipat": "Haryana",
        "Patiala": "Punjab",
        "Patna": "Bihar",
        "Port Blair": "Andaman And Nicobar Islands",
        "Prayagraj": "Uttar Pradesh",
        "Puducherry": "Puducherry",
        "Pune": "Maharashtra",
        "Raipur": "Chhattisgarh",
        "Rajkot": "Gujarat",
        "Ranchi": "Jharkhand",
        "Rohtak": "Haryana",
        "Rourkela": "Odisha",
        "Rudrapur": "Uttarakhand",
        "Salem": "Tamil Nadu",
        "Secunderabad": "Telangana",
        "Shillong": "Meghalaya",
        "Shimla": "Himachal Pradesh",
        "Silchar": "Assam",
        "Siliguri": "West Bengal",
        "Silvassa": "Dadra And Nagar Haveli",
        "Solapur": "Maharashtra",
        "Sonipat": "Haryana",
        "Srinagar": "Jammu And Kashmir",
        "Surat": "Gujarat",
        "Thane": "Maharashtra",
        "Thiruvananthapuram": "Kerala",
        "Tiruchirappalli": "Tamil Nadu",
        "Trichy": "Tamil Nadu",
        "Tirunelveli": "Tamil Nadu",
        "Tirupati": "Andhra Pradesh",
        "Trivandrum": "Kerala",
        "Udaipur": "Rajasthan",
        "Ujjain": "Madhya Pradesh",
        "Vadodara": "Gujarat",
        "Baroda": "Gujarat",
        "Varanasi": "Uttar Pradesh",
        "Vasco": "Goa",
        "Vasai": "Maharashtra",
        "Vellore": "Tamil Nadu",
        "Vijayawada": "Andhra Pradesh",
        "Visakhapatnam": "Andhra Pradesh",
        "Vizag": "Andhra Pradesh",
        "Warangal": "Telangana",
    }.items()
}
_TRAILING_GEO_CODE_TOKENS = {
    "br",
    "branch",
    "city",
    "code",
    "dist",
    "district",
    "no",
    "nos",
    "reg",
    "region",
    "st",
    "state",
    "zone",
}
_TRAILING_STATE_CODES = set(_STATE_CODE_TO_NAME.keys())
_TRAILING_CITY_CODES = set(_STATE_CODE_TO_NAME.keys()) | _TRAILING_GEO_CODE_TOKENS


def _resolve_state_from_label(label: str) -> str | None:
    key = _normalize_lookup_key(label)
    if not key:
        return None
    if key in _STATE_CODE_TO_NAME:
        return _STATE_CODE_TO_NAME[key]
    if key in _STATE_NAME_TO_CANONICAL:
        return _STATE_NAME_TO_CANONICAL[key]
    return _CITY_TO_STATE_NAME.get(key)


def _canonical_geo_label(value: Any, *, kind: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    cleaned = (
        raw
        .replace("_", " ")
        .replace("/", " ")
        .replace("\\", " ")
    )
    cleaned = re.sub(r"\([^)]*\)", " ", cleaned)
    cleaned = re.sub(r"\s*-\s*", " ", cleaned)
    cleaned = re.sub(r"[^0-9A-Za-z& ]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    tokens = [token for token in cleaned.split(" ") if token]
    if not tokens:
        return ""

    while len(tokens) > 1 and re.fullmatch(r"\d{1,4}", tokens[0] or ""):
        tokens = tokens[1:]

    if not tokens:
        return ""

    if kind in {"state", "region"} and len(tokens) == 1:
        mapped = _STATE_CODE_TO_NAME.get(tokens[0].lower())
        if mapped:
            return mapped

    if kind in {"state", "region"}:
        trailing_codes = _TRAILING_STATE_CODES | _TRAILING_GEO_CODE_TOKENS
    else:
        trailing_codes = _TRAILING_CITY_CODES

    while len(tokens) > 1:
        last = tokens[-1]
        last_low = last.lower()
        alpha_num_suffix = re.fullmatch(r"([A-Za-z&]{2,})\d{1,4}", last)
        if alpha_num_suffix:
            tokens[-1] = alpha_num_suffix.group(1)
            continue
        if re.fullmatch(r"\d{1,4}", last):
            tokens = tokens[:-1]
            continue
        if re.fullmatch(r"[A-Za-z]{1,3}\d{1,4}", last):
            tokens = tokens[:-1]
            continue
        if last_low in trailing_codes:
            tokens = tokens[:-1]
            continue
        break

    canonical = re.sub(r"\s+", " ", " ".join(tokens)).strip()
    if not canonical or canonical in _MISSING_LABELS:
        return ""

    if kind in {"state", "region"}:
        resolved_state = _resolve_state_from_label(canonical)
        if resolved_state:
            return resolved_state

        parts = [part for part in canonical.split(" ") if part]
        for part in parts:
            resolved_state = _resolve_state_from_label(part)
            if resolved_state:
                return resolved_state

        if len(parts) >= 2:
            pair_candidates = [" ".join(parts[:2]), " ".join(parts[-2:])]
            for candidate in pair_candidates:
                resolved_state = _resolve_state_from_label(candidate)
                if resolved_state:
                    return resolved_state

    return canonical.title()


def _canonical_geo_series(series: pd.Series, *, kind: str) -> pd.Series:
    out = (
        series
        .astype(str)
        .map(lambda value: _canonical_geo_label(value, kind=kind))
        .replace({k: pd.NA for k in _MISSING_LABELS})
    )
    return out.where(out.fillna("").astype(str).str.strip() != "", pd.NA)


def _normalize_geo_for_match(value: Any, *, kind: str) -> str:
    canonical = _canonical_geo_label(value, kind=kind)
    seed = canonical if canonical else str(value or "")
    return _normalize_bucket_value(seed)


def _collapse_geo_for_match(value: Any, *, kind: str) -> str:
    canonical = _canonical_geo_label(value, kind=kind)
    seed = canonical if canonical else str(value or "")
    return _collapse_bucket_value(seed)


def _state_match_mask(series: pd.Series, selected_state: str) -> pd.Series:
    normalized_series = series.map(lambda value: _normalize_geo_for_match(value, kind="state"))
    compact_series = series.map(lambda value: _collapse_geo_for_match(value, kind="state"))
    selected_norm = _normalize_geo_for_match(selected_state, kind="state")
    selected_compact = _collapse_geo_for_match(selected_state, kind="state")

    mask = normalized_series == selected_norm
    if not mask.any() and selected_compact:
        mask = compact_series == selected_compact
    return mask


def _normalize_dimension_rows(
    rows: list[dict[str, Any]] | None,
    *,
    dimension: str,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    dim_key = _to_safe_key(dimension)
    if dim_key in {"state", "region"}:
        kind = "state"
        alias_keys = {"state", "region"}
    elif dim_key == "city":
        kind = "city"
        alias_keys = {"city"}
    else:
        return rows

    merged: dict[str, dict[str, Any]] = {}
    ordered: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        dim_col = next((safe_map.get(key) for key in alias_keys if safe_map.get(key) is not None), None)
        raw_label = row.get(dim_col) if dim_col is not None else row.get(dimension)
        canonical_label = _canonical_geo_label(raw_label, kind=kind)
        if not canonical_label:
            continue

        bucket = _collapse_bucket_value(canonical_label)
        if bucket not in merged:
            merged[bucket] = {dim_key: canonical_label}
            ordered.append(bucket)
        target = merged[bucket]

        for key, value in row.items():
            if _to_safe_key(str(key)) in alias_keys:
                continue
            try:
                numeric = float(value)
                if pd.notna(numeric):
                    target[key] = float(target.get(key, 0.0) or 0.0) + numeric
                    continue
            except Exception:
                pass

            if key not in target and value is not None:
                text = str(value).strip()
                if text:
                    target[key] = value

    return [merged[key] for key in ordered]


def _find_column(
    df: pd.DataFrame,
    candidates: list[str],
    skip: set[str] | None = None,
) -> str | None:
    if df is None or df.empty:
        return None
    skip = skip or set()
    normalized: dict[str, str] = {}
    for col in df.columns:
        col_name = str(col)
        if col_name in skip:
            continue
        normalized[_normalize_lookup_key(col_name)] = col_name

    for candidate in candidates:
        key = _normalize_lookup_key(candidate)
        if key in normalized:
            return normalized[key]
    return None


def _load_city_breakdown_dataframe(
    db: Session,
    job_id: str | None,
    resolved_source: str,
    dataset_type: str,
) -> pd.DataFrame:
    if resolved_source == "samsung":
        frames: list[pd.DataFrame] = []
        for src in SAMSUNG_PARTNER_SOURCES:
            frame = get_dataframe(
                db=db,
                job_id=job_id,
                source=src,
                dataset_type=dataset_type,
            )
            if frame is not None and not frame.empty:
                frames.append(frame)
        if not frames:
            return get_dataframe(
                db=db,
                job_id=job_id,
                source=resolved_source,
                dataset_type=dataset_type,
            )
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, ignore_index=True, sort=False)

    return get_dataframe(
        db=db,
        job_id=job_id,
        source=resolved_source,
        dataset_type=dataset_type,
    )


def _to_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce").fillna(0.0)


def _metric_series_for_city_breakdown(
    df: pd.DataFrame,
    metric: str,
) -> pd.Series | None:
    metric_key = _to_safe_key(metric)

    if metric_key == "quantity":
        return pd.Series(1.0, index=df.index, dtype="float64")

    if metric_key == "gross_premium":
        col = _find_column(
            df,
            [
                "Amount",
                "Gross Premium",
                "gross_premium",
                "Plan Selling Price",
                "Customer Premium",
            ],
        )
        return _to_numeric_series(df, col) if col else None

    if metric_key == "earned_premium":
        col = _find_column(
            df,
            [
                "Earned Premium",
                "earned_premium",
                "Earned_Amount",
                "Earned Amount",
            ],
        )
        if col:
            return _to_numeric_series(df, col)
        fallback = _find_column(df, ["Amount", "Gross Premium", "Plan Selling Price"])
        return _to_numeric_series(df, fallback) if fallback else None

    if metric_key == "zopper_earned_premium":
        col = _find_column(
            df,
            [
                "earned_zopper",
                "Zopper Earned Premium",
                "zopper_earned_premium",
                "Zopper_Share_EP",
                "Zopper Share EP",
                "Zopper Share",
                "Zopper Shared ( Transfer Price )",
            ],
        )
        return _to_numeric_series(df, col) if col else None

    if metric_key in {"claims", "net_claims"}:
        claims_col = _find_column(
            df,
            [
                "Net Amount",
                "Net_Amount",
                "Claim Amount",
                "Claim_Amount",
                "Payout Amount",
                "Payout_Amount",
                "Zopper's Cost",
                "Zoppers Cost",
                "Claims Costing",
                "Claim Costing",
                "Paid Amount",
                "Approved Amount",
                "Claim Cost",
                "Paid_Amount",
            ],
        )
        if not claims_col:
            return None
        claims = _to_numeric_series(df, claims_col)
        if metric_key == "claims":
            return claims

        otd_col = _find_column(
            df,
            [
                "OTD Amount",
                "OTD_Amount",
                "One time deductible",
                "One Time Deductible",
            ],
        )
        if not otd_col:
            return claims
        return claims - _to_numeric_series(df, otd_col)

    # Pie charting by loss ratio can be misleading; skip when source rows do not
    # carry a stable precomputed denominator.
    if metric_key == "loss_ratio":
        return None

    metric_col = _find_column(df, [metric])
    return _to_numeric_series(df, metric_col) if metric_col else None


_STATE_COLUMN_CANDIDATES = [
    "State",
    "State Name",
    "State_Name",
    "State/UT",
    "State_UT",
    "State_UT_Name",
    "Customer_State",
    "Customer State",
    "Region",
    "Region Name",
    "Region_Name",
    "Zone",
    "Location",
    "State / City",
    "State/City",
]


def _filter_df_by_state(df: pd.DataFrame, state: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    state_col = _find_column(df, _STATE_COLUMN_CANDIDATES)
    if not state_col:
        return df.iloc[0:0]

    state_series = df[state_col].astype(str).str.strip()
    mask = _state_match_mask(state_series, state)

    if not mask.any() and "-" in str(state):
        fallback_state = str(state).split("-", 1)[0].strip()
        mask = _state_match_mask(state_series, fallback_state)

    return df[mask].copy()


def _resolve_category_dimension_column(df: pd.DataFrame, dimension: str) -> str | None:
    dim_key = _to_safe_key(dimension)
    if dim_key == "plan_category":
        return _find_column(
            df,
            [
                "Plan Category",
                "Plan_Category",
                "Care+ Plan Name",
                "Care+ Plan",
                "Plan Type",
                "Plan_Type",
                "Warranty Type",
                "Warranty_Type",
                "Product Category",
                "Product_Category",
                "Category",
                "Type",
            ],
        )

    if dim_key == "reason":
        return _find_column(
            df,
            [
                "Nature of Complaint",
                "Major call Operation",
                "Fault Code",
                "Fault_Code",
                "Reason",
                "Complaint Type",
            ],
        )

    if dim_key == "device_plan_category":
        return _find_column(
            df,
            [
                "Device Plan Category",
                "Device_Plan_Category",
                "Device Category",
                "Device_Category",
                "Product Brand(Group)",
                "Product Brand (Group)",
                "Product Brand",
                "Brand",
                "Item_Brand",
                "Item Description",
                "Appliance Model Name",
                "item",
                "Plan_Category",
                "Plan Category",
                "Category",
            ],
        )

    if dim_key in {"article_brand", "brand"}:
        return _find_column(
            df,
            [
                "ARTICLE_BRAND",
                "Article_Brand",
                "Article Brand",
                "Product Brand(Group)",
                "Product Brand (Group)",
                "Product Brand",
                "Brand Category",
                "Brand",
                "Item_Brand",
                "Article Brand Name",
                "Category",
            ],
        )

    if dim_key == "channel":
        return _find_column(
            df,
            [
                "Channel",
                "Sales_Channel",
                "Sales Channel",
                "Dealer Channel",
                "Partner Channel",
                "Distribution Channel",
                "Type",
            ],
        )

    if dim_key == "product_category":
        return _find_column(
            df,
            [
                "Product Category",
                "Product_Category",
                "Product",
                "Category",
                "Type",
                "Item Description",
                "Appliance Model Name",
                "item",
            ],
        )
    if dim_key == "product_subcategory":
        return _find_column(
            df,
            [
                "Appliance Model Name",
                "Model Code",
                "model_code",
                "Item Description",
                "Product Model",
                "Product_Model",
                "display_plan_name",
            ],
        )
    if dim_key in {"state", "region"}:
        return _find_column(
            df,
            [
                "State",
                "STATE3",
                "State Name",
                "State_Name",
                "Customer_State",
                "Customer State",
                "Region",
                "Region Name",
                "Region_Name",
                "Zone",
                "Zone Name",
                "Location",
                "City",
                "Customer City",
                "Customer_City",
                "State/City",
                "State / City",
            ],
        )
    return None


def _parse_filter_values(raw_values: str | None) -> list[str]:
    if raw_values is None:
        return []
    values: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n;|]+", str(raw_values)):
        label = str(part or "").strip()
        if not label:
            continue
        compact = _collapse_bucket_value(label)
        if compact in {"all", "any", "*"}:
            continue
        if compact in seen:
            continue
        seen.add(compact)
        values.append(label)
    return values


def _clean_category_label_series(
    labels: pd.Series,
    *,
    engine: Any | None = None,
) -> pd.Series:
    cleaned = (
        labels.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .replace(
            {
                "": pd.NA,
                "nan": pd.NA,
                "none": pd.NA,
                "null": pd.NA,
                "NaN": pd.NA,
                "None": pd.NA,
                "NULL": pd.NA,
            }
        )
    )
    blocked = {"", "0", "nan", "none", "null", "unknown", "na", "other", "others"}
    cleaned = cleaned.where(
        ~cleaned.fillna("").map(lambda value: _collapse_bucket_value(value) in blocked),
        pd.NA,
    )
    if engine is not None and hasattr(engine, "_is_identifier_like"):
        cleaned = cleaned.where(~cleaned.fillna("").map(engine._is_identifier_like), pd.NA)
    return cleaned


def _canonical_plan_category_value(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not text:
        return ""
    if "combo" in text:
        return "Combo"
    if "adld" in text or "accidental" in text or "liquid" in text:
        return "ADLD"
    if re.search(r"\bsp\b|\bspp\b", text) or "screen" in text or "crack" in text or "protect max" in text:
        return "Screen Protection"
    if re.search(r"\bew\b", text) or "extended warranty" in text:
        return "Extended Warranty"
    return str(value or "").strip()


def _canonical_device_plan_category_value(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not text:
        return ""
    if "luxury" in text and "fold" in text:
        return "Luxury Fold"
    if "luxury" in text and "flip" in text:
        return "Luxury Flip"
    if "fold" in text:
        return "Luxury Fold"
    if "flip" in text:
        return "Luxury Flip"
    if "super" in text and "premium" in text:
        return "Super Premium"
    if text.startswith("premium") or ("premium" in text and "super" not in text):
        return "Premium"
    if text.startswith("high") or text == "high":
        return "High"
    if text.startswith("mid") or text == "mid":
        return "Mid"
    if text.startswith("mass") or text == "mass":
        return "Mass"
    return str(value or "").strip()


def _build_dimension_filters(
    *,
    filter_1_dimension: str | None,
    filter_1_values: str | None,
    filter_2_dimension: str | None,
    filter_2_values: str | None,
) -> list[dict[str, Any]]:
    supported = {
        "plan_category",
        "device_plan_category",
        "article_brand",
        "brand",
        "channel",
        "product_category",
        "product_subcategory",
        "state",
        "region",
        "reason",
    }
    raw_specs = [
        (filter_1_dimension, filter_1_values),
        (filter_2_dimension, filter_2_values),
    ]
    merged: dict[str, list[str]] = {}
    for raw_dimension, raw_values in raw_specs:
        dim_key = _to_safe_key(raw_dimension or "")
        if dim_key not in supported:
            continue
        values = _parse_filter_values(raw_values)
        if not values:
            continue
        merged.setdefault(dim_key, [])
        existing_compact = {_collapse_bucket_value(v) for v in merged[dim_key]}
        for value in values:
            compact = _collapse_bucket_value(value)
            if compact in existing_compact:
                continue
            merged[dim_key].append(value)
            existing_compact.add(compact)
    return [{"dimension": dim, "values": values} for dim, values in merged.items() if values]


def _apply_dimension_filters_to_frame(
    df: pd.DataFrame,
    filters: list[dict[str, Any]] | None,
) -> pd.DataFrame:
    if df is None or df.empty or not filters:
        return df

    scoped = df.copy()
    for filter_spec in filters:
        dim_key = _to_safe_key(filter_spec.get("dimension", ""))
        values = [str(v).strip() for v in (filter_spec.get("values") or []) if str(v).strip()]
        if not dim_key or not values:
            continue

        dim_col = _resolve_category_dimension_column(scoped, dim_key)
        if not dim_col:
            return scoped.iloc[0:0].copy()

        if dim_key == "plan_category":
            allowed_compact = {_collapse_bucket_value(_canonical_plan_category_value(v)) for v in values}
            series = scoped[dim_col].astype(str).str.strip().map(_canonical_plan_category_value)
            compact_series = series.map(_collapse_bucket_value)
        elif dim_key == "device_plan_category":
            allowed_compact = {_collapse_bucket_value(_canonical_device_plan_category_value(v)) for v in values}
            series = scoped[dim_col].astype(str).str.strip().map(_canonical_device_plan_category_value)
            compact_series = series.map(_collapse_bucket_value)
        elif dim_key in {"state", "region"}:
            allowed_compact = {
                _collapse_bucket_value(_canonical_geo_label(v, kind="state"))
                for v in values
            }
            series = scoped[dim_col].astype(str).str.strip().map(
                lambda v: _canonical_geo_label(v, kind="state")
            )
            compact_series = series.map(_collapse_bucket_value)
        else:
            allowed_compact = {_collapse_bucket_value(v) for v in values}
            series = scoped[dim_col].astype(str).str.strip()
            compact_series = series.map(_collapse_bucket_value)
        mask = compact_series.isin(allowed_compact)
        if not mask.any():
            return scoped.iloc[0:0].copy()
        scoped = scoped[mask].copy()
        if scoped.empty:
            return scoped

    return scoped


def _apply_dimension_filters_to_payload(
    payload: dict[str, Any],
    filters: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    if not filters:
        return payload
    out: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, pd.DataFrame):
            out[key] = _apply_dimension_filters_to_frame(value, filters)
        else:
            out[key] = value
    return out


def _rows_have_non_zero_metric(
    rows: list[dict[str, Any]] | None,
    metric: str,
) -> bool:
    if not rows:
        return False
    metric_key = _to_safe_key(metric)
    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        metric_col = safe_map.get(metric_key) or safe_map.get(_to_safe_key(metric))
        raw = row.get(metric_col) if metric_col is not None else row.get(metric)
        try:
            value = float(raw or 0)
        except Exception:
            value = 0.0
        if pd.notna(value) and abs(value) > 1e-12:
            return True
    return False


def _is_samsung_claims_loss_ratio_cache_suspicious(
    rows: list[dict[str, Any]] | None,
    *,
    dimension: str,
    metric: str,
    overview_mode: bool,
) -> bool:
    if not rows:
        return False

    metric_key = _to_safe_key(metric or "")
    dimension_key = _to_safe_key(dimension or "")
    seen_dimension_values: set[str] = set()
    has_duplicate_dimension = False
    max_abs_value = 0.0

    for row in rows:
        if not isinstance(row, dict):
            continue

        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        dim_col = safe_map.get(dimension_key) or safe_map.get(_to_safe_key(dimension or ""))
        if dim_col is None:
            if dimension_key == "plan_category":
                dim_col = safe_map.get("device_plan_category")
            elif dimension_key == "device_plan_category":
                dim_col = safe_map.get("plan_category")

        dim_val = row.get(dim_col) if dim_col is not None else row.get(dimension)
        if dim_val is not None:
            dim_label = str(dim_val).strip().lower()
            if dim_label:
                if dim_label in seen_dimension_values:
                    has_duplicate_dimension = True
                seen_dimension_values.add(dim_label)

        value_candidates: list[Any] = []
        if overview_mode:
            value_candidates.extend([row.get(partner_key) for partner_key in SAMSUNG_PARTNER_SOURCES])
        else:
            metric_col = safe_map.get(metric_key) or safe_map.get(_to_safe_key(metric or ""))
            value_candidates.append(row.get(metric_col) if metric_col is not None else row.get(metric))

        for raw in value_candidates:
            try:
                value = float(raw or 0)
            except Exception:
                continue
            if pd.notna(value):
                max_abs_value = max(max_abs_value, abs(value))

    # Loss ratio above this threshold in samsung claims cache has been observed as stale/corrupt.
    return has_duplicate_dimension or max_abs_value > 500.0


def _is_reliance_brand_cache_suspicious(
    rows: list[dict[str, Any]] | None,
    *,
    dimension: str,
) -> bool:
    if not rows:
        return False

    dimension_key = _to_safe_key(dimension or "")
    label_groups: dict[str, set[str]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        dim_col = (
            safe_map.get(dimension_key)
            or safe_map.get("brand")
            or safe_map.get("article_brand")
            or safe_map.get(_to_safe_key(dimension or ""))
        )
        raw_label = row.get(dim_col) if dim_col is not None else row.get(dimension)
        label = str(raw_label or "").strip()
        if not label:
            continue
        normalized = re.sub(r"[^a-z0-9]+", "", label.lower())
        if not normalized:
            continue
        label_groups.setdefault(normalized, set()).add(label)

    alias_groups = [
        {"vivo"},
        {"realme"},
        {"motorola", "moto"},
        {"mi", "redmi", "xiaomi"},
        {"oneplus", "onepluslite", "onepluslite8"},
    ]

    for group in alias_groups:
        present_keys = [key for key in group if key in label_groups]
        if len(present_keys) > 1:
            return True
        if any(len(label_groups[key]) > 1 for key in present_keys):
            return True

    return False


def _is_hitachi_plan_cache_suspicious(
    rows: list[dict[str, Any]] | None,
    *,
    dimension: str,
) -> bool:
    if not rows:
        return False

    dimension_key = _to_safe_key(dimension or "")
    suspicious_labels = {
        "extendedwarrantywithservice",
        "plan1",
        "plan2",
        "plan3",
        "plan5",
        "newwarrantykit",
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        dim_col = (
            safe_map.get(dimension_key)
            or safe_map.get("plan_category")
            or safe_map.get(_to_safe_key(dimension or ""))
        )
        raw_label = row.get(dim_col) if dim_col is not None else row.get(dimension)
        label = re.sub(r"[^a-z0-9]+", "", str(raw_label or "").strip().lower())
        if label in suspicious_labels:
            return True

    return False


def _load_godrej_claims_dataframe(
    *,
    db: Session,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    source: str = "godrej",
):
    engine_cls = ENGINE_REGISTRY.get(source)
    if engine_cls is None:
        return None, pd.DataFrame()
    engine = engine_cls(
        db=db,
        job_id=job_id,
        source=source,
        dataset_type="claims",
        from_date=from_date,
        to_date=to_date,
    )
    data = engine.load_data(include_sales=False, include_claims=True)
    df = data.get("claims", pd.DataFrame())
    return engine, df


def _godrej_claims_metric_series(df: pd.DataFrame, metric: str, engine) -> pd.Series | None:
    metric_key = _to_safe_key(metric)
    if metric_key == "quantity":
        return pd.Series(1.0, index=df.index, dtype="float64")
    if metric_key in {"claims", "net_claims"}:
        return pd.to_numeric(engine._claim_amount_series(df), errors="coerce").fillna(0.0)
    if metric_key == "loss_ratio":
        return None
    return None


def _godrej_claims_state_series(df: pd.DataFrame, engine) -> pd.Series | None:
    state_col = _find_column(
        df,
        [
            "State",
            "Customer_State",
            "Customer State",
            "Region",
            "Region Name",
            "Region_Name",
            "Branch",
            "Branch Name",
            "City",
            "Customer City",
            "Customer_City",
        ],
    )
    if not state_col:
        return None

    state_series = _canonical_geo_series(engine._canonical_state(df[state_col]), kind="state")
    state_series = state_series.where(~state_series.fillna("").map(engine._is_identifier_like), pd.NA)
    state_series = state_series.where(~state_series.fillna("").str.lower().isin({"unknown", "0"}), pd.NA)
    return state_series


def _godrej_claims_city_series(df: pd.DataFrame, engine) -> pd.Series | None:
    city_col = _find_column(
        df,
        [
            "Customer_City",
            "Customer City",
            "City",
            "Branch",
            "Branch Name",
            "Store Name",
            "State / City",
            "State/City",
            "Location",
        ],
    )
    if not city_col:
        return None

    city_series = _canonical_geo_series(df[city_col], kind="city")
    city_series = city_series.where(~city_series.fillna("").map(engine._is_identifier_like), pd.NA)
    return city_series


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


def _bounds_source_variants(source: str) -> list[str]:
    source_key = (source or "").strip().lower()
    if source_key == "samsung":
        return list(SAMSUNG_SOURCE_VARIANTS)
    if source_key == "samsung_vs":
        return ["samsung_vs", "samsung_vijay_sales"]
    if source_key in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return ["reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"]
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return ["godrej", "goodrej", "goddrej"]
    return [source_key]


def _bounds_from_jsonb_columns(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    columns: list[str],
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    cleaned_columns = [str(col).strip() for col in columns if str(col).strip()]
    if not cleaned_columns:
        return None, None

    source_values = _bounds_source_variants(source)
    source_placeholders = ", ".join([f":source_{idx}" for idx in range(len(source_values))])
    date_exprs = [
        f"NULLIF(jsonb_extract_path_text(data::jsonb, :col_{idx}), '')"
        for idx, _value in enumerate(cleaned_columns)
    ]
    stmt = f"""
        SELECT COALESCE({", ".join(date_exprs)}) AS raw_date
        FROM data_rows
        WHERE source IN ({source_placeholders})
          AND dataset_type = :dataset_type
    """
    params: dict[str, Any] = {
        "dataset_type": (dataset_type or "").strip().lower(),
    }
    for idx, value in enumerate(source_values):
        params[f"source_{idx}"] = value
    for idx, value in enumerate(cleaned_columns):
        params[f"col_{idx}"] = value

    if job_id is not None:
        stmt += " AND job_id = :job_id"
        params["job_id"] = job_id

    try:
        rows = db.execute(text(stmt), params).fetchall()
    except Exception:
        db.rollback()
        logger.exception(
            "Failed raw date-bounds query source=%s dataset=%s job_id=%s columns=%s",
            source,
            dataset_type,
            job_id,
            cleaned_columns,
        )
        return None, None

    raw_values = [
        str(row[0]).strip()
        for row in rows
        if row and row[0] is not None and str(row[0]).strip()
    ]
    if not raw_values:
        return None, None

    series = _parse_series(pd.Series(raw_values, dtype="object")).dropna()
    if series.empty:
        return None, None
    series = _clip_to_current_month(series)
    return series.min(), series.max()


def compute_by_dimension_rows(
    *,
    db: Session,
    job_id: str | None,
    dimension: str,
    metric: str,
    source: str,
    dataset_type: str,
    bucket: str | None = None,
    from_date: str | None,
    to_date: str | None,
    category_filters: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    resolved_source, engine_key = _normalize_source(source)
    active_category_filters = category_filters or []

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
                    if dim_col is None:
                        # Samsung partner engines may fall back between plan/device category
                        # based on available columns. Treat them as aliases while merging.
                        if dim_key == "plan_category":
                            dim_col = safe_map.get("device_plan_category")
                        elif dim_key == "device_plan_category":
                            dim_col = safe_map.get("plan_category")
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

                    # Preserve loss-ratio period window metadata for temporal charts.
                    if metric_key == "loss_ratio" and dim_key in {"month", "date"}:
                        period_start_col = safe_map.get("period_start")
                        period_end_col = safe_map.get("period_end")
                        if period_start_col is not None:
                            start_val = row.get(period_start_col)
                            if start_val not in (None, ""):
                                merged[key]["period_start"] = start_val
                        if period_end_col is not None:
                            end_val = row.get(period_end_col)
                            if end_val not in (None, ""):
                                merged[key]["period_end"] = end_val

            def _fetch_and_merge(src: str, out_key: str):
                dimension_key_local = _to_safe_key(dimension or "")
                force_live_partner_metric = (
                    (
                        dataset_type == "sales"
                        and _to_safe_key(metric or "") in {"earned_premium", "zopper_earned_premium"}
                    )
                    or (
                        dataset_type == "claims"
                        and dimension_key_local in {"month", "date"}
                    )
                )
                if not active_category_filters and not force_live_partner_metric:
                    partner_cached = get_precomputed_graph(
                        db=db,
                        source=src,
                        dataset_type=dataset_type,
                        job_id=job_id,
                        dimension=dimension,
                        metric=metric,
                        bucket=bucket,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    # Empty cached payloads are stale for samsung overview merges.
                    # Fall back to live engine compute so one missing partner cache
                    # does not zero out compare rows for broad date ranges.
                    if partner_cached is not None and len(partner_cached) > 0:
                        suspicious_loss_ratio_cache = (
                            dataset_type == "claims"
                            and _to_safe_key(metric or "") == "loss_ratio"
                            and _is_samsung_claims_loss_ratio_cache_suspicious(
                                partner_cached,
                                dimension=dimension,
                                metric=metric,
                                overview_mode=False,
                            )
                        )
                        if (
                            not suspicious_loss_ratio_cache
                            and dataset_type == "claims"
                            and _to_safe_key(metric or "") == "loss_ratio"
                            and _to_safe_key(dimension or "") in {"month", "date"}
                        ):
                            first_partner_row = next(
                                (row for row in partner_cached if isinstance(row, dict)),
                                None,
                            )
                            if first_partner_row is not None:
                                safe_partner_keys = {
                                    _to_safe_key(str(k))
                                    for k in first_partner_row.keys()
                                }
                                if "period_start" not in safe_partner_keys or "period_end" not in safe_partner_keys:
                                    suspicious_loss_ratio_cache = True
                        if not suspicious_loss_ratio_cache:
                            _merge(partner_cached, out_key)
                            return
                try:
                    engine = _get_or_create_batch_engine(
                        engine_cls=engine_cls,
                        engine_key=engine_key,
                        source=src,
                        job_id=job_id,
                        dataset_type=dataset_type,
                        from_date=from_date,
                        to_date=to_date,
                        allow_shared=not active_category_filters,
                        db=db,
                    )
                    if active_category_filters:
                        needs_sales = dataset_type == "sales" or metric == "loss_ratio"
                        needs_claims = dataset_type == "claims" or metric == "loss_ratio"
                        payload = engine.load_data(
                            include_sales=needs_sales,
                            include_claims=needs_claims,
                        )
                        filtered_payload = _apply_dimension_filters_to_payload(
                            payload,
                            active_category_filters,
                        )
                        engine._loaded_data_cache[(needs_sales, needs_claims)] = filtered_payload
                    rows = engine.compute_by_dimension(dimension=dimension, metric=metric)
                    _merge(rows or [], out_key)
                except Exception as exc:
                    logger.error("Failed to fetch %s: %s", src, exc)

            # Run sequentially in-request: avoids thread-unsafe Session sharing and
            # prevents intermittent DB SSL/connection failures under load.
            for partner_key in SAMSUNG_PARTNER_SOURCES:
                _fetch_and_merge(partner_key, partner_key)

            out_rows = list(merged.values())
            for row in out_rows:
                for partner_key in SAMSUNG_PARTNER_SOURCES:
                    row[partner_key] = row.get(partner_key, 0.0)

            if dim_key in {"month", "date"}:
                out_rows.sort(key=lambda r: str(r.get(dim_key, "")))
            return _normalize_dimension_rows(out_rows, dimension=dimension)

        engine = _get_or_create_batch_engine(
            engine_cls=engine_cls,
            engine_key=engine_key,
            source=resolved_source,
            job_id=job_id,
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
            allow_shared=not active_category_filters,
            db=db,
        )
        if active_category_filters:
            if engine_key in {"samsung", "godrej", "hitachi"}:
                needs_sales = dataset_type == "sales" or metric == "loss_ratio"
                needs_claims = dataset_type == "claims" or metric == "loss_ratio"
                payload = engine.load_data(
                    include_sales=needs_sales,
                    include_claims=needs_claims,
                )
                filtered_payload = _apply_dimension_filters_to_payload(
                    payload,
                    active_category_filters,
                )
                engine._loaded_data_cache[(needs_sales, needs_claims)] = filtered_payload
            else:
                payload = engine.load_data()
                filtered_payload = _apply_dimension_filters_to_payload(
                    payload,
                    active_category_filters,
                )
                engine._loaded_data_cache = filtered_payload
        out = engine.compute_by_dimension(
            dimension=dimension,
            metric=metric,
        )
        return _normalize_dimension_rows(out, dimension=dimension)

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
    if active_category_filters:
        df = _apply_dimension_filters_to_frame(df, active_category_filters)
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

    out_rows = out_df.to_dict(orient="records")
    return _normalize_dimension_rows(out_rows, dimension=dimension)


@router.get("/by-dimension")
def analytics_by_dimension(
    job_id: str | None = Query(None),
    dimension: str = Query(...),
    metric: str = Query(...),
    source: str = Query(...),
    dataset_type: str = Query(...),
    bucket: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    filter_1_dimension: str | None = Query(None),
    filter_1_values: str | None = Query(None),
    filter_2_dimension: str | None = Query(None),
    filter_2_values: str | None = Query(None),
    db: Session = Depends(get_db),
):
    started = time.perf_counter()
    from_date, to_date = _sanitize_range(from_date, to_date)
    resolved_source, _ = _normalize_source(source)
    normalized_dataset = (dataset_type or "").strip().lower()
    job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=normalized_dataset,
        job_id=job_id,
        context="by_dimension",
    )

    bucket_key = (bucket or "").strip().lower() or None
    if bucket_key not in {None, "day", "week", "month"}:
        bucket_key = None
    category_filters = _build_dimension_filters(
        filter_1_dimension=filter_1_dimension,
        filter_1_values=filter_1_values,
        filter_2_dimension=filter_2_dimension,
        filter_2_values=filter_2_values,
    )

    if category_filters:
        try:
            out = compute_by_dimension_rows(
                db=db,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                source=source,
                dataset_type=normalized_dataset,
                bucket=bucket_key,
                from_date=from_date,
                to_date=to_date,
                category_filters=category_filters,
            )
        except Exception:
            logger.exception(
                "Live filtered compute failed for analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s",
                source,
                dataset_type,
                dimension,
                metric,
            )
            out = []
        logger.info(
            "TIMING analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s mode=live_filtered filters=%s rows=%s duration_ms=%.2f",
            source,
            dataset_type,
            dimension,
            metric,
            len(category_filters),
            len(out),
            (time.perf_counter() - started) * 1000,
        )
        return out

    metric_key = _to_safe_key(metric or "")
    dimension_key = _to_safe_key(dimension or "")
    force_live_samsung_sales_earned_graph = (
        resolved_source == "samsung"
        and normalized_dataset == "sales"
        and metric_key in {"earned_premium", "zopper_earned_premium"}
    )
    cached = None if force_live_samsung_sales_earned_graph else get_precomputed_graph(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        bucket=bucket_key,
        from_date=from_date,
        to_date=to_date,
    )
    if cached is not None and len(cached) > 0:
        is_stale_cached_shape = False
        is_stale_manual_update = False
        is_stale_zero_metric = False
        is_stale_samsung_partner_mismatch = False
        is_stale_samsung_loss_ratio_cache = False
        is_stale_godrej_legacy_region = False
        is_stale_godrej_claims_range_mismatch = False
        is_stale_godrej_sales_month_mismatch = False
        is_stale_loss_ratio_period_window = False
        is_stale_reliance_month_floor = False
        is_stale_reliance_brand_alias_cache = False
        is_stale_hitachi_plan_cache = False
        cache_updated_at = _get_graph_cache_updated_at(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket=bucket_key,
            from_date=from_date,
            to_date=to_date,
        )
        manual_cache_marker_updated_at = _latest_manual_update_marker_updated_at(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=job_id,
        )
        latest_cache_marker_updated_at = _latest_cache_marker_updated_at(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            cache_kind="graph",
            dimension=dimension,
        )
        if not _is_precomputed_cache_fresh(
            cache_updated_at=cache_updated_at,
            latest_marker_updated_at=latest_cache_marker_updated_at,
        ):
            is_stale_manual_update = True
        dimension_key = _to_safe_key(dimension or "")
        if isinstance(cached[0], dict):
            row_keys = {_to_safe_key(str(k)) for k in cached[0].keys()}
            has_compare_keys = any(partner_key in row_keys for partner_key in SAMSUNG_PARTNER_SOURCES)
            is_samsung_overview_shape = (
                resolved_source == "samsung"
                and all(partner_key in row_keys for partner_key in SAMSUNG_PARTNER_SOURCES)
            )
            if resolved_source in SAMSUNG_PARTNER_SOURCES and has_compare_keys and metric_key not in row_keys:
                is_stale_cached_shape = True
            if metric_key and metric_key not in row_keys and not is_samsung_overview_shape:
                is_stale_cached_shape = True

        if (
            resolved_source.startswith("samsung")
            and normalized_dataset == "claims"
            and metric_key == "loss_ratio"
        ):
            is_stale_samsung_loss_ratio_cache = _is_samsung_claims_loss_ratio_cache_suspicious(
                cached,
                dimension=dimension,
                metric=metric,
                overview_mode=(resolved_source == "samsung"),
            )

        if (
            resolved_source == "reliance"
            and normalized_dataset == "sales"
            and dimension_key in {"brand", "article_brand"}
        ):
            is_stale_reliance_brand_alias_cache = _is_reliance_brand_cache_suspicious(
                cached,
                dimension=dimension,
            )

        if resolved_source == "hitachi" and dimension_key == "plan_category":
            is_stale_hitachi_plan_cache = _is_hitachi_plan_cache_suspicious(
                cached,
                dimension=dimension,
            )

        if resolved_source == "samsung":
            merged_dim_values: set[str] = set()
            merged_partner_totals = {
                partner_key: 0.0
                for partner_key in SAMSUNG_PARTNER_SOURCES
            }

            for row in cached:
                if not isinstance(row, dict):
                    continue
                safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
                dim_col = safe_map.get(dimension_key) or safe_map.get(_to_safe_key(dimension or ""))
                if dim_col is None:
                    if dimension_key == "plan_category":
                        dim_col = safe_map.get("device_plan_category")
                    elif dimension_key == "device_plan_category":
                        dim_col = safe_map.get("plan_category")
                dim_val = row.get(dim_col) if dim_col is not None else row.get(dimension)
                if dim_val is not None:
                    merged_dim_values.add(str(dim_val))

                for partner_key in SAMSUNG_PARTNER_SOURCES:
                    try:
                        partner_value = float(row.get(partner_key, 0) or 0)
                    except Exception:
                        partner_value = 0.0
                    if pd.notna(partner_value):
                        merged_partner_totals[partner_key] += abs(partner_value)

            def _partner_snapshot(partner_source: str) -> tuple[set[str], bool]:
                partner_rows = get_precomputed_graph(
                    db=db,
                    source=partner_source,
                    dataset_type=normalized_dataset,
                    job_id=job_id,
                    dimension=dimension,
                    metric=metric,
                    bucket=bucket_key,
                    from_date=from_date,
                    to_date=to_date,
                )
                if partner_rows is None:
                    return set(), False

                partner_dims: set[str] = set()
                has_non_zero = False
                for prow in partner_rows:
                    if not isinstance(prow, dict):
                        continue
                    safe_map = {_to_safe_key(str(k)): k for k in prow.keys()}
                    dim_col = safe_map.get(dimension_key) or safe_map.get(_to_safe_key(dimension or ""))
                    if dim_col is None:
                        if dimension_key == "plan_category":
                            dim_col = safe_map.get("device_plan_category")
                        elif dimension_key == "device_plan_category":
                            dim_col = safe_map.get("plan_category")
                    dim_val = prow.get(dim_col) if dim_col is not None else prow.get(dimension)
                    if dim_val is not None:
                        partner_dims.add(str(dim_val))

                    metric_col = safe_map.get(metric_key) or safe_map.get(_to_safe_key(metric or ""))
                    raw_metric = prow.get(metric_col) if metric_col is not None else prow.get(metric)
                    try:
                        metric_value = float(raw_metric or 0)
                    except Exception:
                        metric_value = 0.0
                    if pd.notna(metric_value) and abs(metric_value) > 1e-12:
                        has_non_zero = True

                return partner_dims, has_non_zero

            for partner_key in SAMSUNG_PARTNER_SOURCES:
                partner_dims, partner_has_non_zero = _partner_snapshot(partner_key)
                if partner_dims - merged_dim_values:
                    is_stale_samsung_partner_mismatch = True
                if partner_has_non_zero and merged_partner_totals[partner_key] <= 1e-12:
                    is_stale_samsung_partner_mismatch = True
            if (
                normalized_dataset == "claims"
                and metric_key == "loss_ratio"
                and not is_stale_samsung_loss_ratio_cache
                and _is_samsung_claims_loss_ratio_cache_suspicious(
                    cached,
                    dimension=dimension,
                    metric=metric,
                    overview_mode=True,
                )
            ):
                is_stale_samsung_loss_ratio_cache = True

        if (
            normalized_dataset == "claims"
            and metric_key == "loss_ratio"
            and dimension_key in {"month", "date"}
        ):
            first_cache_row = next((row for row in cached if isinstance(row, dict)), None)
            if first_cache_row is not None:
                safe_keys = {_to_safe_key(str(k)) for k in first_cache_row.keys()}
                if "period_start" not in safe_keys or "period_end" not in safe_keys:
                    is_stale_loss_ratio_period_window = True

        if (
            resolved_source == "godrej"
            and normalized_dataset == "claims"
            and dimension_key in {"channel", "product_category"}
            and metric_key in {"claims", "net_claims", "loss_ratio"}
        ):
            metric_values: list[float] = []
            for row in cached:
                if not isinstance(row, dict):
                    continue
                raw = row.get(metric_key, row.get(metric))
                try:
                    value = float(raw)
                except Exception:
                    continue
                if pd.notna(value):
                    metric_values.append(value)

            has_only_zeros = bool(metric_values) and all(abs(v) < 1e-12 for v in metric_values)
            if has_only_zeros:
                quantity_cached = get_precomputed_graph(
                    db=db,
                    source=resolved_source,
                    dataset_type=normalized_dataset,
                    job_id=job_id,
                    dimension=dimension,
                    metric="quantity",
                    bucket=bucket_key,
                    from_date=from_date,
                    to_date=to_date,
                )
                quantity_values: list[float] = []
                for row in quantity_cached or []:
                    if not isinstance(row, dict):
                        continue
                    raw = row.get("quantity")
                    try:
                        value = float(raw)
                    except Exception:
                        continue
                    if pd.notna(value):
                        quantity_values.append(value)
                if any(v > 0 for v in quantity_values):
                    is_stale_zero_metric = True

        if (
            resolved_source == "godrej"
            and normalized_dataset == "claims"
            and dimension_key in {"state", "region"}
        ):
            godrej_cls = ENGINE_REGISTRY.get("godrej")
            alias_map = {
                _normalize_bucket_value(k): str(v).strip()
                for k, v in getattr(godrej_cls, "STATE_ALIAS_MAP", {}).items()
            }
            for row in cached:
                if not isinstance(row, dict):
                    continue
                safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
                dim_col = safe_map.get(dimension_key) or safe_map.get(_to_safe_key(dimension or ""))
                raw_dim = row.get(dim_col) if dim_col is not None else row.get(dimension)
                if raw_dim is None:
                    continue
                raw_label = str(raw_dim).strip()
                normalized_label = _normalize_bucket_value(raw_label)
                mapped_label = alias_map.get(normalized_label)
                if mapped_label and _normalize_bucket_value(mapped_label) != normalized_label:
                    is_stale_godrej_legacy_region = True
                    break

        if (
            resolved_source == "godrej"
            and normalized_dataset == "claims"
            and (from_date or to_date)
            and dimension_key in {"state", "region", "channel", "product_category"}
            and _rows_have_non_zero_metric(cached, metric)
        ):
            month_quantity_cached = get_precomputed_graph(
                db=db,
                source=resolved_source,
                dataset_type=normalized_dataset,
                job_id=job_id,
                dimension="month",
                metric="quantity",
                bucket=bucket_key,
                from_date=from_date,
                to_date=to_date,
            )
            if month_quantity_cached is not None and not _rows_have_non_zero_metric(month_quantity_cached, "quantity"):
                is_stale_godrej_claims_range_mismatch = True

        if (
            resolved_source == "godrej"
            and normalized_dataset == "sales"
            and dimension_key in {"month", "date"}
        ):
            from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
            to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None
            from_month = (
                pd.Timestamp(from_dt).to_period("M").to_timestamp()
                if from_dt is not None and from_dt is not pd.NaT
                else None
            )
            to_month = (
                pd.Timestamp(to_dt).to_period("M").to_timestamp()
                if to_dt is not None and to_dt is not pd.NaT
                else None
            )

            if _rows_have_temporal_window_mismatch(
                cached,
                dimension_key=dimension_key,
                from_date=from_date,
                to_date=to_date,
            ):
                is_stale_godrej_sales_month_mismatch = True

            if dimension_key == "month":
                month_points: list[pd.Timestamp] = []
                for row in cached:
                    if not isinstance(row, dict):
                        continue
                    safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
                    month_col = safe_map.get("month") or safe_map.get(dimension_key)
                    raw_month = row.get(month_col) if month_col is not None else row.get("month")
                    if raw_month is None:
                        continue
                    parsed = pd.to_datetime(raw_month, errors="coerce")
                    if pd.isna(parsed):
                        continue
                    month_points.append(pd.Timestamp(parsed).to_period("M").to_timestamp())

                if month_points:
                    cached_min_month = min(month_points)
                    cached_max_month = max(month_points)
                    current_month = _current_month_cap().to_period("M").to_timestamp()
                    if cached_max_month > current_month:
                        is_stale_godrej_sales_month_mismatch = True
                    if from_month is not None and from_month < cached_min_month:
                        is_stale_godrej_sales_month_mismatch = True
                    if to_month is not None and cached_max_month > to_month:
                        is_stale_godrej_sales_month_mismatch = True

        if resolved_source == "reliance" and dimension_key in {"month", "date"}:
            month_values: list[str] = []
            for row in cached:
                if not isinstance(row, dict):
                    continue
                safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
                dim_col = safe_map.get(dimension_key) or safe_map.get(_to_safe_key(dimension or ""))
                if dim_col is None:
                    continue
                raw_val = row.get(dim_col)
                if raw_val is None:
                    continue
                month_values.append(str(raw_val))

            if month_values:
                parsed = _parse_series(pd.Series(month_values)).dropna()
                if not parsed.empty:
                    cached_min = parsed.min().to_period("M").to_timestamp()
                    expected_start = pd.to_datetime(from_date, errors="coerce") if from_date else None
                    if expected_start is not None and not pd.isna(expected_start):
                        expected_start = pd.Timestamp(expected_start).to_period("M").to_timestamp()
                    else:
                        expected_start, _ = _date_bounds_for_source_dataset(
                            db=db,
                            job_id=job_id,
                            source=resolved_source,
                            dataset_type=normalized_dataset,
                        )
                        if expected_start is not None and not pd.isna(expected_start):
                            expected_start = pd.Timestamp(expected_start).to_period("M").to_timestamp()
                    if expected_start is not None and cached_min > expected_start:
                        is_stale_reliance_month_floor = True

        has_non_manual_staleness = any(
            [
                is_stale_cached_shape,
                is_stale_zero_metric,
                is_stale_samsung_partner_mismatch,
                is_stale_samsung_loss_ratio_cache,
                is_stale_godrej_legacy_region,
                is_stale_godrej_claims_range_mismatch,
                is_stale_godrej_sales_month_mismatch,
                is_stale_reliance_month_floor,
                is_stale_reliance_brand_alias_cache,
                is_stale_loss_ratio_period_window,
                is_stale_hitachi_plan_cache,
            ]
        )
        manual_marker_only_stale = False
        if (
            is_stale_manual_update
            and not has_non_manual_staleness
            and cache_updated_at is not None
            and manual_cache_marker_updated_at is not None
            and latest_cache_marker_updated_at is not None
        ):
            cache_ts = pd.Timestamp(cache_updated_at)
            manual_ts = pd.Timestamp(manual_cache_marker_updated_at)
            latest_ts = pd.Timestamp(latest_cache_marker_updated_at)
            if cache_ts.tzinfo is not None:
                cache_ts = cache_ts.tz_convert(None)
            if manual_ts.tzinfo is not None:
                manual_ts = manual_ts.tz_convert(None)
            if latest_ts.tzinfo is not None:
                latest_ts = latest_ts.tz_convert(None)
            manual_marker_only_stale = cache_ts < manual_ts and latest_ts == manual_ts

        if (
            is_stale_manual_update
            or
            is_stale_cached_shape
            or is_stale_zero_metric
            or is_stale_samsung_partner_mismatch
            or is_stale_samsung_loss_ratio_cache
            or is_stale_godrej_legacy_region
            or is_stale_godrej_claims_range_mismatch
            or is_stale_godrej_sales_month_mismatch
            or is_stale_reliance_month_floor
            or is_stale_reliance_brand_alias_cache
            or is_stale_loss_ratio_period_window
            or is_stale_hitachi_plan_cache
        ):
            if manual_marker_only_stale:
                normalized_cached = _normalize_dimension_rows(cached, dimension=dimension)
                _schedule_graph_rebuild(
                    job_id=job_id,
                    source=resolved_source,
                    dataset_type=normalized_dataset,
                    dimension=dimension,
                    metric=metric,
                    bucket=bucket_key,
                    from_date=from_date,
                    to_date=to_date,
                )
                logger.info(
                    "TIMING analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s mode=precomputed_stale_background rows=%s duration_ms=%.2f",
                    source,
                    dataset_type,
                    dimension,
                    metric,
                    len(normalized_cached),
                    (time.perf_counter() - started) * 1000,
                )
                return normalized_cached
            if is_stale_manual_update:
                reason = "manual_update_marker"
            elif is_stale_cached_shape:
                reason = "shape"
            elif is_stale_zero_metric:
                reason = "all_zero_metric"
            elif is_stale_samsung_loss_ratio_cache:
                reason = "samsung_loss_ratio_cache"
            elif is_stale_godrej_legacy_region:
                reason = "godrej_legacy_region"
            elif is_stale_godrej_claims_range_mismatch:
                reason = "godrej_range_mismatch"
            elif is_stale_godrej_sales_month_mismatch:
                reason = "godrej_sales_month_mismatch"
            elif is_stale_reliance_month_floor:
                reason = "reliance_month_floor"
            elif is_stale_reliance_brand_alias_cache:
                reason = "reliance_brand_alias_cache"
            elif is_stale_loss_ratio_period_window:
                reason = "loss_ratio_period_window"
            elif is_stale_hitachi_plan_cache:
                reason = "hitachi_plan_cache"
            else:
                reason = "samsung_partner_mismatch"
            logger.warning(
                "Stale precomputed graph detected (%s); recomputing live source=%s dataset=%s dimension=%s metric=%s",
                reason,
                resolved_source,
                normalized_dataset,
                dimension,
                metric,
            )
        else:
            normalized_cached = _normalize_dimension_rows(cached, dimension=dimension)
            logger.info(
                "TIMING analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s mode=precomputed rows=%s duration_ms=%.2f",
                source,
                dataset_type,
                dimension,
                metric,
                len(normalized_cached),
                (time.perf_counter() - started) * 1000,
            )
            return normalized_cached
    if cached == [] and (resolved_source.startswith("samsung") or resolved_source in {"reliance", *APPLIANCE_SOURCES}):
        cache_updated_at = _get_graph_cache_updated_at(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            bucket=bucket_key,
            from_date=from_date,
            to_date=to_date,
        )
        cache_is_fresh = _is_precomputed_cache_fresh(
            cache_updated_at=cache_updated_at,
            latest_marker_updated_at=_latest_cache_marker_updated_at(
                db=db,
                source=resolved_source,
                dataset_type=normalized_dataset,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                cache_kind="graph",
                dimension=dimension,
            ),
        )
        if cache_is_fresh:
            logger.info(
                "TIMING analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s mode=precomputed rows=0 duration_ms=%.2f",
                source,
                dataset_type,
                dimension,
                metric,
                (time.perf_counter() - started) * 1000,
            )
            return []
        logger.warning(
            "Empty precomputed graph detected; recomputing live source=%s dataset=%s dimension=%s metric=%s from=%s to=%s",
            resolved_source,
            normalized_dataset,
            dimension,
            metric,
            from_date,
            to_date,
        )
    try:
        out = compute_by_dimension_rows(
            db=db,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
            source=source,
            dataset_type=normalized_dataset,
            bucket=bucket_key,
            from_date=from_date,
            to_date=to_date,
        )
    except Exception:
        db.rollback()
        logger.exception(
            "Live compute failed for analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s",
            source,
            dataset_type,
            dimension,
            metric,
        )
        out = []

    fallback_job_id: str | None = job_id
    if (
        not out
        and job_id is not None
        and resolved_source == "reliance"
    ):
        def _has_rows(tag: str | None) -> bool:
            query = (
                db.query(func.count(DataRow.id))
                .filter(DataRow.source == resolved_source)
                .filter(DataRow.dataset_type == normalized_dataset)
            )
            if tag is None:
                query = query.filter(DataRow.job_id.is_(None))
            else:
                query = query.filter(DataRow.job_id == tag)
            return int(query.scalar() or 0) > 0

        if not _has_rows(job_id) and _has_rows(None):
            try:
                out = compute_by_dimension_rows(
                    db=db,
                    job_id=None,
                    dimension=dimension,
                    metric=metric,
                    source=source,
                    dataset_type=normalized_dataset,
                    bucket=bucket_key,
                    from_date=from_date,
                    to_date=to_date,
                )
                fallback_job_id = None
                logger.warning(
                    "Reliance job_id %s had no rows; served untagged data instead for dimension=%s metric=%s",
                    job_id,
                    dimension,
                    metric,
                )
            except Exception:
                db.rollback()
                logger.exception(
                    "Fallback compute failed for reliance source=%s dataset=%s dimension=%s metric=%s",
                    source,
                    dataset_type,
                    dimension,
                    metric,
                )

    logger.info(
        "TIMING analytics.by_dimension source=%s dataset=%s dimension=%s metric=%s mode=live rows=%s duration_ms=%.2f",
        source,
        dataset_type,
        dimension,
        metric,
        len(out),
        (time.perf_counter() - started) * 1000,
    )
    try:
        upsert_precomputed_graph(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=fallback_job_id,
            dimension=dimension,
            metric=metric,
            bucket=bucket_key,
            from_date=from_date,
            to_date=to_date,
            rows=out,
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "Failed to upsert precomputed graph source=%s dataset=%s dimension=%s metric=%s",
            resolved_source,
            normalized_dataset,
            dimension,
            metric,
        )
    return out


@router.post("/by-dimension-batch")
def analytics_by_dimension_batch(
    payload: ByDimensionBatchPayload,
    db: Session = Depends(get_db),
):
    requests = list(payload.requests or [])
    if not requests:
        return {"results": []}

    max_requests = 96
    results: list[dict[str, Any]] = []
    previous_engine_cache = getattr(_batch_engine_state, "engines", None)
    _batch_engine_state.engines = {}
    try:
        for index, item in enumerate(requests[:max_requests]):
            rows = analytics_by_dimension(
                job_id=item.job_id,
                dimension=item.dimension,
                metric=item.metric,
                source=item.source,
                dataset_type=item.dataset_type,
                bucket=item.bucket,
                from_date=item.from_date,
                to_date=item.to_date,
                filter_1_dimension=item.filter_1_dimension,
                filter_1_values=item.filter_1_values,
                filter_2_dimension=item.filter_2_dimension,
                filter_2_values=item.filter_2_values,
                db=db,
            )
            results.append({
                "request_key": item.request_key or f"req_{index}",
                "rows": rows if isinstance(rows, list) else [],
            })
    finally:
        if previous_engine_cache is None:
            try:
                delattr(_batch_engine_state, "engines")
            except AttributeError:
                pass
        else:
            _batch_engine_state.engines = previous_engine_cache

    return {
        "results": results,
        "truncated": len(requests) > max_requests,
    }


@router.get("/city-breakdown")
def analytics_city_breakdown(
    state: str = Query(...),
    metric: str = Query(...),
    source: str = Query(...),
    dataset_type: str = Query(...),
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    filter_1_dimension: str | None = Query(None),
    filter_1_values: str | None = Query(None),
    filter_2_dimension: str | None = Query(None),
    filter_2_values: str | None = Query(None),
    limit: int = Query(40, ge=1, le=500),
    db: Session = Depends(get_db),
):
    from_date, to_date = _sanitize_range(from_date, to_date)
    normalized_dataset = (dataset_type or "").strip().lower()
    resolved_source, _ = _normalize_source(source)
    category_filters = _build_dimension_filters(
        filter_1_dimension=filter_1_dimension,
        filter_1_values=filter_1_values,
        filter_2_dimension=filter_2_dimension,
        filter_2_values=filter_2_values,
    )

    if resolved_source in APPLIANCE_SOURCES and normalized_dataset == "claims":
        engine, df = _load_godrej_claims_dataframe(
            db=db,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            source=resolved_source,
        )
        if df is None or df.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "No rows available for this source.",
            }

        scoped = engine._apply_date_filter(df, "claims")
        if scoped is None or scoped.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "No rows available for the selected date range.",
            }

        state_series = _godrej_claims_state_series(scoped, engine)
        if state_series is None:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "State column is not available in this dataset.",
            }

        scoped = scoped.copy()
        scoped["_state"] = state_series
        scoped = scoped[scoped["_state"].notna()].copy()
        if scoped.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "State column is not available in this dataset.",
            }

        mask = _state_match_mask(scoped["_state"].astype(str), state)

        scoped = scoped[mask].copy()
        if scoped.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": f"No rows found for state '{state}'.",
            }

        scoped = _apply_dimension_filters_to_frame(scoped, category_filters)
        if scoped.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "No rows found for the selected filters.",
            }

        city_series = _godrej_claims_city_series(scoped, engine)
        if city_series is None:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": "City column is not available in this dataset.",
            }

        metric_values = _godrej_claims_metric_series(scoped, metric, engine)
        if metric_values is None:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": f"Metric '{metric}' is unavailable for city breakdown.",
            }

        scoped["_city"] = city_series
        scoped["_value"] = pd.to_numeric(metric_values, errors="coerce").fillna(0.0).clip(lower=0.0)
        scoped = scoped[scoped["_city"].notna()].copy()
        if scoped.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": f"No city rows found for state '{state}'.",
            }

        out = (
            scoped.groupby("_city", dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={"_city": "city", "_value": "value"})
            .sort_values("value", ascending=False)
            .head(limit)
        )
        if out.empty:
            return {
                "state": state,
                "metric": metric,
                "rows": [],
                "message": f"No city values found for state '{state}'.",
            }

        rows = [
            {"city": str(row["city"]), "value": float(row["value"] or 0.0)}
            for _, row in out.iterrows()
        ]
        total = float(sum(item["value"] for item in rows))
        return {
            "state": state,
            "metric": metric,
            "rows": rows,
            "total": total,
        }

    df = _load_city_breakdown_dataframe(
        db=db,
        job_id=job_id,
        resolved_source=resolved_source,
        dataset_type=normalized_dataset,
    )
    if df is None or df.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": "No rows available for this source.",
        }

    scoped = filter_by_date_range(df, normalized_dataset, from_date, to_date)
    if scoped is None or scoped.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": "No rows available for the selected date range.",
        }

    state_col = _find_column(scoped, _STATE_COLUMN_CANDIDATES)
    if not state_col:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": "State column is not available in this dataset.",
        }

    city_col = _find_column(
        scoped,
        [
            "City",
            "City Name",
            "City_Name",
            "Customer City",
            "Customer_City",
            "District",
            "Town",
            "Store City",
            "Store_City",
            "Branch City",
            "Branch_City",
            "Location City",
            "Location_City",
            "Branch",
            "Location",
            "State / City",
            "State/City",
        ],
        skip={state_col},
    )
    city_from_state_fallback = False
    if not city_col:
        # Some partner uploads use a single "State/City" style column that
        # stores city tokens (e.g., Mumbai, Pune, Thane). Reuse it for drilldown.
        city_col = state_col
        city_from_state_fallback = True

    scoped = _filter_df_by_state(scoped, state)
    if scoped.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": f"No rows found for state '{state}'.",
        }

    scoped = _apply_dimension_filters_to_frame(scoped, category_filters)
    if scoped.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": "No rows found for the selected filters.",
        }

    metric_values = _metric_series_for_city_breakdown(scoped, metric)
    if metric_values is None:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": f"Metric '{metric}' is unavailable for city breakdown.",
        }

    city_series = _canonical_geo_series(scoped[city_col], kind="city")
    if city_from_state_fallback:
        selected_city_key = _collapse_geo_for_match(state, kind="city")
        city_series = city_series.where(
            city_series.fillna("").map(lambda value: _collapse_geo_for_match(value, kind="city")) != selected_city_key,
            pd.NA,
        )

    scoped["_city"] = city_series
    scoped["_value"] = pd.to_numeric(metric_values, errors="coerce").fillna(0.0)
    scoped = scoped[scoped["_city"].notna()].copy()
    if scoped.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": f"No city rows found for state '{state}'.",
        }

    # Pie charts expect non-negative values for intelligible slices.
    scoped["_value"] = scoped["_value"].clip(lower=0.0)
    out = (
        scoped.groupby("_city", dropna=False)["_value"]
        .sum()
        .reset_index()
        .rename(columns={"_city": "city", "_value": "value"})
        .sort_values("value", ascending=False)
        .head(limit)
    )
    if out.empty:
        return {
            "state": state,
            "metric": metric,
            "rows": [],
            "message": f"No city values found for state '{state}'.",
        }

    rows = [
        {"city": str(row["city"]), "value": float(row["value"] or 0.0)}
        for _, row in out.iterrows()
    ]
    total = float(sum(item["value"] for item in rows))
    return {
        "state": state,
        "metric": metric,
        "rows": rows,
        "total": total,
    }


@router.get("/category-percentage")
def analytics_category_percentage(
    dimension: str = Query(...),
    source: str = Query(...),
    dataset_type: str = Query(...),
    metric: str = Query("quantity"),
    state: str | None = Query(None),
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    filter_1_dimension: str | None = Query(None),
    filter_1_values: str | None = Query(None),
    filter_2_dimension: str | None = Query(None),
    filter_2_values: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    from_date, to_date = _sanitize_range(from_date, to_date)
    normalized_dataset = (dataset_type or "").strip().lower()
    resolved_source, _ = _normalize_source(source)
    dim_key = _to_safe_key(dimension)
    category_filters = _build_dimension_filters(
        filter_1_dimension=filter_1_dimension,
        filter_1_values=filter_1_values,
        filter_2_dimension=filter_2_dimension,
        filter_2_values=filter_2_values,
    )
    if dim_key not in {
        "plan_category",
        "device_plan_category",
        "article_brand",
        "brand",
        "channel",
        "product_category",
        "product_subcategory",
        "reason",
    }:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": "Supported dimensions: plan_category, device_plan_category, article_brand, brand, channel, product_category, product_subcategory, reason.",
        }

    if resolved_source in APPLIANCE_SOURCES and normalized_dataset == "claims":
        engine, df = _load_godrej_claims_dataframe(
            db=db,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            source=resolved_source,
        )
        if df is None or df.empty:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": "No rows available for this source.",
            }

        scoped = engine._apply_date_filter(df, "claims")
        if scoped is None or scoped.empty:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": "No rows available for the selected date range.",
            }

        if state is not None and str(state).strip() != "":
            state_series = _godrej_claims_state_series(scoped, engine)
            if state_series is None:
                return {
                    "dimension": dim_key,
                    "metric": metric,
                    "state": state,
                    "rows": [],
                    "message": "State column is not available in this dataset.",
                }
            scoped = scoped.copy()
            scoped["_state"] = state_series
            scoped = scoped[scoped["_state"].notna()].copy()
            if scoped.empty:
                return {
                    "dimension": dim_key,
                    "metric": metric,
                    "state": state,
                    "rows": [],
                    "message": f"No rows found for state '{state}'.",
                }

            mask = _state_match_mask(scoped["_state"].astype(str), state)

            scoped = scoped[mask].copy()
            if scoped.empty:
                return {
                    "dimension": dim_key,
                    "metric": metric,
                    "state": state,
                    "rows": [],
                    "message": f"No rows found for state '{state}'.",
                }

        scoped = _apply_dimension_filters_to_frame(scoped, category_filters)
        if scoped.empty:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": "No rows found for the selected filters.",
            }

        dim_col = _resolve_category_dimension_column(scoped, dim_key)
        if not dim_col and dim_key == "device_plan_category":
            dim_col = _find_column(
                scoped,
                [
                    "Type",
                ],
            )
        if not dim_col:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": f"{dimension} column is not available in this dataset.",
            }

        metric_values = _godrej_claims_metric_series(scoped, metric, engine)
        if metric_values is None:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": f"Metric '{metric}' is unavailable for this percentage breakdown.",
            }

        labels = _clean_category_label_series(scoped[dim_col], engine=engine)

        scoped = scoped.copy()
        scoped["_label"] = labels
        scoped["_value"] = pd.to_numeric(metric_values, errors="coerce").fillna(0.0).clip(lower=0.0)
        scoped = scoped[scoped["_label"].notna()].copy()
        if scoped.empty:
            return {
                "dimension": dim_key,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": f"No valid values found for {dimension}.",
            }

        out = (
            scoped.groupby("_label", dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={"_label": "label", "_value": "value"})
            .sort_values("value", ascending=False)
            .head(limit)
        )
        total = float(pd.to_numeric(out["value"], errors="coerce").fillna(0).sum()) if not out.empty else 0.0
        rows = [
            {
                "label": str(row["label"]),
                "value": float(row["value"] or 0.0),
                "percentage": float((float(row["value"] or 0.0) / total) * 100) if total > 0 else 0.0,
            }
            for _, row in out.iterrows()
        ]

        return {
            "dimension": dim_key,
            "metric": metric,
            "state": state,
            "total": total,
            "rows": rows,
        }

    df = _load_city_breakdown_dataframe(
        db=db,
        job_id=job_id,
        resolved_source=resolved_source,
        dataset_type=normalized_dataset,
    )
    if df is None or df.empty:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": "No rows available for this source.",
        }

    scoped = filter_by_date_range(df, normalized_dataset, from_date, to_date)
    if scoped is None or scoped.empty:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": "No rows available for the selected date range.",
        }

    if state is not None and str(state).strip() != "":
        scoped = _filter_df_by_state(scoped, str(state))
        if scoped.empty:
            return {
                "dimension": dimension,
                "metric": metric,
                "state": state,
                "rows": [],
                "message": f"No rows found for state '{state}'.",
            }

    scoped = _apply_dimension_filters_to_frame(scoped, category_filters)
    if scoped.empty:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": "No rows found for the selected filters.",
        }

    dim_col = _resolve_category_dimension_column(scoped, dim_key)
    if not dim_col:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": f"{dimension} column is not available in this dataset.",
        }

    metric_values = _metric_series_for_city_breakdown(scoped, metric)
    if metric_values is None:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": f"Metric '{metric}' is unavailable for this percentage breakdown.",
        }

    labels = _clean_category_label_series(scoped[dim_col])

    scoped = scoped.copy()
    scoped["_label"] = labels
    scoped["_value"] = pd.to_numeric(metric_values, errors="coerce").fillna(0.0).clip(lower=0.0)
    scoped = scoped[scoped["_label"].notna()].copy()
    if scoped.empty:
        return {
            "dimension": dimension,
            "metric": metric,
            "state": state,
            "rows": [],
            "message": f"No valid values found for {dimension}.",
        }

    out = (
        scoped.groupby("_label", dropna=False)["_value"]
        .sum()
        .reset_index()
        .rename(columns={"_label": "label", "_value": "value"})
        .sort_values("value", ascending=False)
        .head(limit)
    )
    total = float(pd.to_numeric(out["value"], errors="coerce").fillna(0).sum()) if not out.empty else 0.0
    rows = [
        {
            "label": str(row["label"]),
            "value": float(row["value"] or 0.0),
            "percentage": float((float(row["value"] or 0.0) / total) * 100) if total > 0 else 0.0,
        }
        for _, row in out.iterrows()
    ]

    return {
        "dimension": dim_key,
        "metric": metric,
        "state": state,
        "total": total,
        "rows": rows,
    }


def compute_summary_values(
    *,
    db: Session,
    job_id: str | None,
    source: str,
    dataset_type: str,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
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
            for src in SAMSUNG_PARTNER_SOURCES:
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
        summary = engine.compute_summary()
        return summary

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

        claims_total = _sum_col(
            "Net Amount",
            "Net_Amount",
            "Net Claims",
            "Net_Claims",
            "Claim_Amount",
            "Claim Amount",
            "Payout Amount",
            "Payout_Amount",
        )
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


def _summary_has_signal(summary: dict[str, Any] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    gross = float(summary.get("gross_premium", 0) or 0)
    earned = float(summary.get("earned_premium", 0) or 0)
    zopper = float(summary.get("zopper_earned_premium", 0) or 0)
    units = float(summary.get("units_sold", 0) or 0)
    return any(abs(v) > 0 for v in [gross, earned, zopper]) or units > 0


def _date_bounds_cache_dataset_type(dataset_type: str) -> str:
    dataset_key = (dataset_type or "").strip().lower()
    return f"{dataset_key}{_DATE_BOUNDS_CACHE_DATASET_SUFFIX}" if dataset_key else "date_bounds"


def _month_bounds_from_precomputed_rows(
    rows: list[dict[str, Any]] | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if not rows:
        return None, None

    month_points: list[pd.Timestamp] = []
    for row in rows:
        parsed = _extract_month_from_dimension_row(row)
        if parsed is None:
            continue
        month_points.append(pd.Timestamp(parsed).to_period("M").to_timestamp())

    if not month_points:
        return None, None

    min_dt = min(month_points)
    max_dt = max(month_points) + pd.offsets.MonthEnd(0)
    return min_dt, max_dt


def _date_bounds_from_precomputed_month_graphs(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    dataset_key = (dataset_type or "").strip().lower()
    metric_candidates = _DATE_BOUNDS_METRIC_CANDIDATES.get(dataset_key, [])
    if not metric_candidates:
        return None, None

    source_candidates: list[str] = [source]
    if source == "samsung":
        source_candidates.extend(SAMSUNG_PARTNER_SOURCES)
    source_candidates = list(dict.fromkeys(source_candidates))

    min_candidates: list[pd.Timestamp] = []
    max_candidates: list[pd.Timestamp] = []

    for candidate_source in source_candidates:
        for metric in metric_candidates:
            rows = get_precomputed_graph(
                db=db,
                source=candidate_source,
                dataset_type=dataset_key,
                job_id=job_id,
                dimension="month",
                metric=metric,
                bucket="month",
                from_date=None,
                to_date=None,
            )
            local_min, local_max = _month_bounds_from_precomputed_rows(rows)
            if local_min is None and local_max is None:
                continue
            if local_min is not None:
                min_candidates.append(local_min)
            if local_max is not None:
                max_candidates.append(local_max)
            break

    return (
        min(min_candidates) if min_candidates else None,
        max(max_candidates) if max_candidates else None,
    )


def compute_date_bounds_payload(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> dict[str, Any]:
    resolved_source, _ = _normalize_source(source)
    dataset_key = (dataset_type or "").strip().lower()

    min_date, max_date = _date_bounds_from_precomputed_month_graphs(
        db=db,
        source=resolved_source,
        dataset_type=dataset_key,
        job_id=job_id,
    )

    if min_date is None and max_date is None:
        if resolved_source == "samsung":
            bounds = [
                _date_bounds_for_source_dataset(
                    db=db,
                    source=partner_source,
                    dataset_type=dataset_key,
                    job_id=job_id,
                )
                for partner_source in SAMSUNG_PARTNER_SOURCES
            ]
            min_candidates = [local_min for local_min, _local_max in bounds if local_min is not None]
            max_candidates = [local_max for _local_min, local_max in bounds if local_max is not None]
            min_date = min(min_candidates) if min_candidates else None
            max_date = max(max_candidates) if max_candidates else None
        else:
            min_date, max_date = _date_bounds_for_source_dataset(
                db=db,
                source=resolved_source,
                dataset_type=dataset_key,
                job_id=job_id,
            )

    month_cap = pd.Timestamp.now().normalize()
    if resolved_source == "samsung" and dataset_key == "sales" and min_date is not None:
        if max_date is None or pd.isna(max_date) or max_date < month_cap:
            max_date = month_cap
    if max_date is not None and pd.notna(max_date) and max_date > month_cap:
        max_date = month_cap

    if min_date is not None and max_date is not None and min_date > max_date:
        min_date = max_date

    return {
        "min_date": min_date.date().isoformat() if min_date is not None else None,
        "max_date": max_date.date().isoformat() if max_date is not None else None,
    }


def _get_date_bounds_cache_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> datetime | None:
    row = (
        db.query(PrecomputedSummary)
        .filter(PrecomputedSummary.source == (source or "").strip().lower())
        .filter(PrecomputedSummary.dataset_type == _date_bounds_cache_dataset_type(dataset_type))
        .filter(PrecomputedSummary.job_key == (job_id or "").strip())
        .filter(PrecomputedSummary.from_date == "")
        .filter(PrecomputedSummary.to_date == "")
        .first()
    )
    return row.updated_at if row is not None else None


def _latest_date_bounds_marker_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> datetime | None:
    source_key = (source or "").strip().lower()
    source_variants = list(SAMSUNG_SOURCE_VARIANTS) if source_key == "samsung" else [source_key]
    query = (
        db.query(func.max(ManualUpdateMarker.updated_at))
        .filter(ManualUpdateMarker.source.in_(source_variants))
        .filter(ManualUpdateMarker.dataset_type == (dataset_type or "").strip().lower())
    )
    job_key = (job_id or "").strip()
    if job_key:
        query = query.filter(ManualUpdateMarker.job_key.in_([job_key, ""]))
    return query.scalar()


def _rows_have_values(rows: list[dict[str, Any]] | None, metric: str) -> bool:
    if not rows:
        return False
    metric_key = _to_safe_key(metric)
    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(k)): k for k in row.keys()}
        metric_col = safe_map.get(metric_key) or safe_map.get(_to_safe_key(metric))
        raw_val = row.get(metric_col) if metric_col is not None else row.get(metric)
        try:
            if abs(float(raw_val or 0)) > 0:
                return True
        except Exception:
            continue
    return False


def _extract_month_from_dimension_row(row: dict[str, Any]) -> pd.Timestamp | None:
    if not isinstance(row, dict):
        return None
    for key, raw_value in row.items():
        safe_key = _to_safe_key(str(key))
        if safe_key == "month" or safe_key == "date" or "month" in safe_key:
            parsed = _parse_series(pd.Series([raw_value])).dropna()
            if not parsed.empty:
                return parsed.iloc[0]
    return None


def _rows_have_temporal_window_mismatch(
    rows: list[dict[str, Any]] | None,
    *,
    dimension_key: str,
    from_date: str | None,
    to_date: str | None,
) -> bool:
    if not rows or not (from_date or to_date):
        return False

    parsed_points: list[pd.Timestamp] = []
    for row in rows:
        parsed = _extract_month_from_dimension_row(row)
        if parsed is None:
            continue
        point = pd.Timestamp(parsed)
        if _to_safe_key(dimension_key) == "month":
            point = point.to_period("M").to_timestamp()
        else:
            point = point.normalize()
        parsed_points.append(point)

    if not parsed_points:
        return False

    cached_min = min(parsed_points)
    cached_max = max(parsed_points)
    if _to_safe_key(dimension_key) == "month":
        cached_max = cached_max + pd.offsets.MonthEnd(0)

    from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
    to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None
    if from_dt is not None and not pd.isna(from_dt):
        expected_from = pd.Timestamp(from_dt)
        if _to_safe_key(dimension_key) == "month":
            expected_from = expected_from.to_period("M").to_timestamp()
        else:
            expected_from = expected_from.normalize()
        if cached_min < expected_from:
            return True
    if to_dt is not None and not pd.isna(to_dt):
        expected_to = pd.Timestamp(to_dt)
        if _to_safe_key(dimension_key) == "month":
            expected_to = expected_to.to_period("M").to_timestamp()
        else:
            expected_to = expected_to.normalize()
        if cached_max > expected_to:
            return True

    return False


def _annual_build_current_range(
    from_date: str | None,
    to_date: str | None,
) -> tuple[str, str]:
    fallback_end = pd.to_datetime(to_date, errors="coerce") if to_date else pd.Timestamp.now().normalize()
    if fallback_end is None or pd.isna(fallback_end):
        fallback_end = pd.Timestamp.now().normalize()

    fallback_start = pd.Timestamp(year=fallback_end.year, month=1, day=1)
    current_from = pd.to_datetime(from_date, errors="coerce") if from_date else fallback_start
    current_to = pd.to_datetime(to_date, errors="coerce") if to_date else fallback_end

    if current_from is None or pd.isna(current_from):
        current_from = fallback_start
    if current_to is None or pd.isna(current_to):
        current_to = fallback_end
    if current_from > current_to:
        current_from, current_to = current_to, current_from

    return current_from.date().isoformat(), current_to.date().isoformat()


def _annual_financial_year_start(value: str) -> int:
    parsed = pd.to_datetime(value, errors="coerce")
    if parsed is None or pd.isna(parsed):
        return 0
    return int(parsed.year if parsed.month >= 4 else parsed.year - 1)


def _annual_financial_year_label(financial_year_start: int) -> str:
    return f"{financial_year_start} - {financial_year_start + 1}"


def _annual_shift_year(value: str, delta: int) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if parsed is None or pd.isna(parsed):
        return value
    shifted = pd.Timestamp(parsed) + pd.DateOffset(years=delta)
    return shifted.date().isoformat()


def _annual_build_year_buckets(
    current_from: str,
    current_to: str,
) -> list[dict[str, str]]:
    start_financial_year = _annual_financial_year_start(current_from)
    end_financial_year = _annual_financial_year_start(current_to)
    if not start_financial_year or not end_financial_year:
        return []

    if start_financial_year == end_financial_year:
        return [
            {
                "label": _annual_financial_year_label(start_financial_year - 1),
                "from": _annual_shift_year(current_from, -1),
                "to": _annual_shift_year(current_to, -1),
            },
            {
                "label": _annual_financial_year_label(start_financial_year),
                "from": current_from,
                "to": current_to,
            },
        ]

    buckets: list[dict[str, str]] = []
    for financial_year in range(start_financial_year, end_financial_year + 1):
        buckets.append(
            {
                "label": _annual_financial_year_label(financial_year),
                "from": current_from if financial_year == start_financial_year else f"{financial_year}-04-01",
                "to": current_to if financial_year == end_financial_year else f"{financial_year + 1}-03-31",
            }
        )
    return buckets


def _annual_canonicalize_plan_label(source: str, value: Any) -> str:
    raw = re.sub(r"\s+", " ", str(value or "").strip())
    text = raw.lower()
    if not text:
        return ""

    plan_number_match = re.match(r"^plan\s*(\d+)$", raw, flags=re.IGNORECASE)
    if plan_number_match:
        return f"Plan {plan_number_match.group(1)}"

    if "combo" in text:
        return "Combo"
    if "adld" in text or "accidental" in text or "liquid" in text:
        return "ADLD"
    if source == "reliance" and (
        "crack" in text or "screen" in text or re.search(r"\bsp\b|\bspp\b", text)
    ):
        return "Crack Screen"
    if "screen" in text or "crack" in text or "protect max" in text or re.search(r"\bsp\b|\bspp\b", text):
        return "Screen Protection"
    if "extended" in text or "warranty" in text or re.search(r"\bew\b", text):
        return "Extended Warranty"
    return raw


def _annual_plan_order(source: str) -> list[str]:
    if source == "reliance":
        return ["ADLD", "Crack Screen", "Extended Warranty"]
    if source == "samsung" or source in SAMSUNG_PARTNER_SOURCES:
        return ["Combo", "ADLD", "Screen Protection", "Extended Warranty"]
    return []


def _annual_plan_sort_tuple(source: str, value: str) -> tuple[int, int, str]:
    canonical = _annual_canonicalize_plan_label(source, value)
    order = [candidate.lower() for candidate in _annual_plan_order(source)]
    normalized = canonical.lower()
    if normalized in order:
        return (0, order.index(normalized), canonical)

    plan_number_match = re.match(r"^plan\s+(\d+)$", canonical, flags=re.IGNORECASE)
    if plan_number_match:
        return (1, int(plan_number_match.group(1)), canonical)

    return (2, 999, canonical)


def _annual_extract_plans(
    source: str,
    rows: list[dict[str, Any]] | None,
) -> list[str]:
    if not rows:
        return []

    plans: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_map = {_to_safe_key(str(key)): key for key in row.keys()}
        raw_plan = None
        for candidate in ["plan_category", "device_plan_category", "plan"]:
            actual = safe_map.get(candidate)
            if actual is not None:
                raw_plan = row.get(actual)
                break
        label = _annual_canonicalize_plan_label(source, raw_plan)
        if not label:
            continue
        plan_key = label.lower()
        if plan_key in seen:
            continue
        seen.add(plan_key)
        plans.append(label)

    return sorted(plans, key=lambda value: _annual_plan_sort_tuple(source, value))


def _annual_extract_metric_value(
    row: dict[str, Any],
    metric: str,
) -> float:
    if not isinstance(row, dict):
        return 0.0
    safe_map = {_to_safe_key(str(key)): key for key in row.keys()}
    metric_key = _to_safe_key(metric)
    metric_col = safe_map.get(metric_key)
    raw_value = row.get(metric_col) if metric_col is not None else row.get(metric)
    try:
        numeric = float(raw_value or 0)
    except Exception:
        return 0.0
    return float(numeric) if pd.notna(numeric) else 0.0


def _annual_sum_samsung_partner_rows(
    rows: list[dict[str, Any]] | None,
    *,
    dimension: str,
    metric: str,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    dim_key = _to_safe_key(dimension)
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        safe_map = {_to_safe_key(str(key)): key for key in row.keys()}
        dim_col = safe_map.get(dim_key)
        if dim_col is None:
            if dim_key == "plan_category":
                dim_col = safe_map.get("device_plan_category") or safe_map.get("plan")
            elif dim_key == "device_plan_category":
                dim_col = safe_map.get("plan_category") or safe_map.get("plan")

        dim_value = row.get(dim_col) if dim_col is not None else row.get(dimension)
        if dim_value in (None, ""):
            continue

        total = 0.0
        saw_partner_column = False
        for partner_source in SAMSUNG_PARTNER_SOURCES:
            partner_col = safe_map.get(_to_safe_key(partner_source))
            if partner_col is None:
                continue
            saw_partner_column = True
            try:
                numeric = float(row.get(partner_col) or 0)
            except Exception:
                numeric = 0.0
            if pd.notna(numeric):
                total += float(numeric)

        next_row: dict[str, Any] = {
            dim_key: dim_value,
            metric: total if saw_partner_column else _annual_extract_metric_value(row, metric),
        }
        for metadata_key in ("period_start", "period_end"):
            actual = safe_map.get(metadata_key)
            if actual is None:
                continue
            value = row.get(actual)
            if value not in (None, ""):
                next_row[metadata_key] = value
        out.append(next_row)

    return out


def _annual_empty_rows(
    year_buckets: list[dict[str, str]],
) -> list[dict[str, Any]]:
    return [
        {
            "label": bucket["label"],
            "total": 0.0,
            "values": {},
        }
        for bucket in year_buckets
    ]


def _annual_find_bucket_label(
    month_value: pd.Timestamp | None,
    year_buckets: list[dict[str, str]],
) -> str | None:
    if month_value is None or pd.isna(month_value):
        return None
    month_key = pd.Timestamp(month_value).date().isoformat()
    for bucket in year_buckets:
        if bucket["from"] <= month_key <= bucket["to"]:
            return bucket["label"]
    return None


def _annual_build_total_payload(
    rows: list[dict[str, Any]] | None,
    metric: str,
    year_buckets: list[dict[str, str]],
) -> dict[str, Any]:
    chart_rows = _annual_empty_rows(year_buckets)
    row_map = {str(row["label"]): row for row in chart_rows}

    for row in rows or []:
        month_value = _extract_month_from_dimension_row(row)
        label = _annual_find_bucket_label(month_value, year_buckets)
        if not label:
            continue
        target = row_map.get(label)
        if target is None:
            continue
        target["total"] = float(target.get("total", 0.0) or 0.0) + _annual_extract_metric_value(row, metric)

    return {
        "plans": [],
        "rows": chart_rows,
    }


def _annual_build_plan_payload(
    plans: list[str],
    rows_by_plan: dict[str, list[dict[str, Any]]],
    metric: str,
    year_buckets: list[dict[str, str]],
) -> dict[str, Any]:
    chart_rows = _annual_empty_rows(year_buckets)
    row_map = {str(row["label"]): row for row in chart_rows}

    for plan in plans:
        plan_rows = rows_by_plan.get(plan) or []
        for row in plan_rows:
            month_value = _extract_month_from_dimension_row(row)
            label = _annual_find_bucket_label(month_value, year_buckets)
            if not label:
                continue
            target = row_map.get(label)
            if target is None:
                continue
            values = target.setdefault("values", {})
            metric_value = _annual_extract_metric_value(row, metric)
            values[plan] = float(values.get(plan, 0.0) or 0.0) + metric_value
            target["total"] = float(target.get("total", 0.0) or 0.0) + metric_value

    return {
        "plans": plans,
        "rows": chart_rows,
    }


def _annual_metric_payload_has_plan_signal(metric_payload: dict[str, Any] | None) -> bool:
    if not isinstance(metric_payload, dict):
        return False

    plans = [str(plan).strip() for plan in (metric_payload.get("plans") or []) if str(plan).strip()]
    rows = metric_payload.get("rows") or []
    if not plans or not isinstance(rows, list) or not rows:
        return False

    for row in rows:
        if not isinstance(row, dict):
            continue
        values = row.get("values") or {}
        if not isinstance(values, dict):
            continue
        for raw_value in values.values():
            try:
                if abs(float(raw_value or 0)) > 0:
                    return True
            except Exception:
                continue
    return False


def _annual_payload_has_signal(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    payload_by_metric = payload.get("payload_by_metric")
    if not isinstance(payload_by_metric, dict) or not payload_by_metric:
        return False

    quantity_payload = payload_by_metric.get("quantity")
    has_sales_totals = any(metric in payload_by_metric for metric in _ANNUAL_SALES_SUMMARY_METRICS)
    if has_sales_totals:
        return _annual_metric_payload_has_plan_signal(quantity_payload)

    if _annual_metric_payload_has_plan_signal(quantity_payload):
        return True

    for metric_payload in payload_by_metric.values():
        if _annual_metric_payload_has_plan_signal(metric_payload):
            return True
        if not isinstance(metric_payload, dict):
            continue
        rows = metric_payload.get("rows") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                if abs(float(row.get("total", 0) or 0)) > 0:
                    return True
            except Exception:
                continue
    return False


def _rows_have_month_window_mismatch(
    rows: list[dict[str, Any]] | None,
    *,
    from_date: str | None,
    to_date: str | None,
) -> bool:
    if not rows or not (from_date or to_date):
        return False

    from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
    to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None
    from_month = (
        pd.Timestamp(from_dt).to_period("M").to_timestamp()
        if from_dt is not None and pd.notna(from_dt)
        else None
    )
    to_month = (
        pd.Timestamp(to_dt).to_period("M").to_timestamp()
        if to_dt is not None and pd.notna(to_dt)
        else None
    )

    month_points: list[pd.Timestamp] = []
    for row in rows:
        parsed = _extract_month_from_dimension_row(row)
        if parsed is None:
            continue
        month_points.append(pd.Timestamp(parsed).to_period("M").to_timestamp())

    if not month_points:
        return False

    cached_min_month = min(month_points)
    cached_max_month = max(month_points)
    if from_month is not None and from_month < cached_min_month:
        return True
    if to_month is not None and cached_max_month > to_month:
        return True
    return False


def _rows_have_future_months(rows: list[dict[str, Any]] | None) -> bool:
    if not rows:
        return False

    current_month = _current_month_cap().to_period("M").to_timestamp()
    for row in rows:
        parsed = _extract_month_from_dimension_row(row)
        if parsed is None:
            continue
        if pd.Timestamp(parsed).to_period("M").to_timestamp() > current_month:
            return True
    return False


def _master_payload_has_godrej_sales_month_mismatch(
    payload: dict[str, Any] | None,
    *,
    from_date: str | None,
    to_date: str | None,
) -> bool:
    if not isinstance(payload, dict):
        return False
    row_sets = payload.get("rows")
    if not isinstance(row_sets, dict):
        return False
    for key in ("godrej_gross", "godrej_earned", "godrej_zopper"):
        if _rows_have_future_months(row_sets.get(key)):
            return True
        if _rows_have_month_window_mismatch(
            row_sets.get(key),
            from_date=from_date,
            to_date=to_date,
        ):
            return True
    return False


def _bounds_from_master_rows(row_sets: list[list[dict[str, Any]]]) -> tuple[str | None, str | None]:
    min_dt: pd.Timestamp | None = None
    max_dt: pd.Timestamp | None = None
    for rows in row_sets:
        for row in rows or []:
            dt = _extract_month_from_dimension_row(row)
            if dt is None:
                continue
            if min_dt is None or dt < min_dt:
                min_dt = dt
            if max_dt is None or dt > max_dt:
                max_dt = dt
    return (
        min_dt.date().isoformat() if min_dt is not None else None,
        (max_dt + pd.offsets.MonthEnd(0)).date().isoformat() if max_dt is not None else None,
    )


def _finalize_master_date_bounds(
    min_date: str | None,
    max_date: str | None,
) -> tuple[str | None, str | None]:
    min_dt = pd.to_datetime(min_date, errors="coerce") if min_date else None
    max_dt = pd.to_datetime(max_date, errors="coerce") if max_date else None

    if min_dt is not None and pd.isna(min_dt):
        min_dt = None
    if max_dt is not None and pd.isna(max_dt):
        max_dt = None

    month_cap = _current_month_cap()
    if min_dt is not None and (max_dt is None or (pd.notna(max_dt) and max_dt < month_cap)):
        max_dt = month_cap
    if max_dt is not None and pd.notna(max_dt) and max_dt > month_cap:
        max_dt = month_cap

    if min_dt is not None and max_dt is not None and min_dt > max_dt:
        min_dt = max_dt.replace(day=1)

    return (
        min_dt.date().isoformat() if min_dt is not None else None,
        max_dt.date().isoformat() if max_dt is not None else None,
    )


def _date_bounds_for_source_dataset(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    dataset_key = (dataset_type or "").strip().lower()
    src_key = (source or "").strip().lower()
    df: pd.DataFrame | None = None

    def _load_df() -> pd.DataFrame:
        nonlocal df
        if df is None:
            df = get_dataframe(
                db=db,
                job_id=job_id,
                source=source,
                dataset_type=dataset_key,
            )
        return df

    if dataset_key == "sales" and src_key.startswith(("godrej", "goodrej", "goddrej")):
        # Godrej sales timeline should anchor on warranty purchase first.
        purchase_min, purchase_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=["Warranty Purchase Date"],
        )
        if purchase_min is None and purchase_max is None:
            purchase_min, purchase_max = _bounds_from_columns(_load_df(), ["Warranty Purchase Date"])
        if purchase_min is not None or purchase_max is not None:
            return purchase_min, purchase_max
        product_purchase_min, product_purchase_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=["Product Purchased Date"],
        )
        if product_purchase_min is None and product_purchase_max is None:
            product_purchase_min, product_purchase_max = _bounds_from_columns(_load_df(), ["Product Purchased Date"])
        if product_purchase_min is not None or product_purchase_max is not None:
            return product_purchase_min, product_purchase_max

    if dataset_key == "sales" and src_key == "hitachi":
        # Hitachi sales filters and quick ranges should track the sale/purchase date.
        # Warranty-start dates can extend into future coverage periods and break presets.
        purchase_columns = [
            "Warranty Purchase Date",
            "Plan Start Date",
            "Date",
            "Start_Date",
            "Start Date",
        ]
        purchase_min, purchase_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=purchase_columns,
        )
        if purchase_min is None and purchase_max is None:
            purchase_min, purchase_max = _bounds_from_columns(_load_df(), purchase_columns)
        if purchase_min is not None or purchase_max is not None:
            return purchase_min, purchase_max
        month_min, month_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=["Month"],
        )
        if month_min is None and month_max is None:
            month_min, month_max = _bounds_from_columns(_load_df(), ["Month"])
        if month_min is not None or month_max is not None:
            return month_min, month_max
        warranty_columns = [
            "Warranty Start Date",
            "Warranty Start_Date",
            "Product Purchased Date",
        ]
        warranty_min, warranty_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=warranty_columns,
        )
        if warranty_min is None and warranty_max is None:
            warranty_min, warranty_max = _bounds_from_columns(_load_df(), warranty_columns)
        if warranty_min is not None or warranty_max is not None:
            return warranty_min, warranty_max

    if dataset_key == "sales":
        if src_key.startswith("samsung"):
            # Samsung sales timelines should prefer transaction/sale dates.
            # Some partner files also carry future policy-start dates, which can
            # incorrectly stretch picker bounds far beyond the sales month range.
            sale_columns = [
                "Date",
                "Invoice Date",
                "Invoice_Date_",
                "Bill Created Date",
                "Purchase Date",
                "Transaction Date",
                "Transaction_Date",
            ]
            sale_min, sale_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=sale_columns,
            )
            if sale_min is None and sale_max is None:
                sale_min, sale_max = _bounds_from_columns(_load_df(), sale_columns)
            if sale_min is not None or sale_max is not None:
                return sale_min, sale_max

            # Legacy partner uploads can carry malformed Month labels; prefer
            # canonical start dates before trusting those month strings.
            start_columns = [
                "Start_Date",
                "Start Date",
                "Plan Start Date",
                "Warranty Start Date",
                "Warranty Start_Date",
            ]
            start_min, start_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=start_columns,
            )
            if start_min is None and start_max is None:
                start_min, start_max = _bounds_from_columns(_load_df(), start_columns)
            if start_min is not None or start_max is not None:
                return start_min, start_max
            month_columns = ["Month", "Month Name", "Month_Name", "Month_Year", "Month-Year"]
            month_min, month_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=month_columns,
            )
            if month_min is None and month_max is None:
                month_min, month_max = _bounds_from_columns(_load_df(), month_columns)
            if month_min is not None or month_max is not None:
                return month_min, month_max

        if src_key == "reliance":
            # Reliance sales dashboards should default to the actual sale date
            # window, not future warranty activation dates.
            sale_columns = [
                "Transaction Date",
                "Transaction_Date",
                "Purchase Date",
                "PURCHASE_DATE",
                "Purchase_Date",
                "Invoice Date",
                "Invoice_Date",
                "Bill Created Date",
                "Data Processing Date",
                "Data_Processing_Date",
                "Date",
            ]
            sale_min, sale_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=sale_columns,
            )
            if sale_min is None and sale_max is None:
                sale_min, sale_max = _bounds_from_columns(_load_df(), sale_columns)
            if sale_min is not None or sale_max is not None:
                return sale_min, sale_max
            month_columns = ["Month", "Month Name", "Month_Name", "Month_Year", "Month-Year"]
            month_min, month_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=month_columns,
            )
            if month_min is None and month_max is None:
                month_min, month_max = _bounds_from_columns(_load_df(), month_columns)
            if month_min is not None or month_max is not None:
                return month_min, month_max
            warranty_columns = ["Warranty Start Date", "Warranty_Start_Date", "Plan Start Date", "Start Date", "Start_Date"]
            warranty_min, warranty_max = _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=warranty_columns,
            )
            if warranty_min is None and warranty_max is None:
                warranty_min, warranty_max = _bounds_from_columns(_load_df(), warranty_columns)
            if warranty_min is not None or warranty_max is not None:
                return warranty_min, warranty_max
        fallback_sales_columns = [
            "Start_Date",
            "Start Date",
            "Plan Start Date",
            "Warranty Start Date",
            "Warranty Start_Date",
            "Warranty Purchase Date",
            "Invoice_Date_",
            "Invoice Date",
            "Bill Created Date",
            "Payment_date",
            "Payment Date",
            "Month",
            "Month Name",
            "Month_Name",
            "Date",
        ]
        fast_min, fast_max = _bounds_from_jsonb_columns(
            db=db,
            source=source,
            dataset_type=dataset_key,
            job_id=job_id,
            columns=fallback_sales_columns,
        )
        if fast_min is not None or fast_max is not None:
            return fast_min, fast_max
        return _bounds_from_columns(_load_df(), fallback_sales_columns)

    if dataset_key == "claims" and src_key.startswith("samsung"):
        # Samsung claims files can spread timeline fields across Fiscal/Month/Claim-Date
        # columns. Use a combined bound so newly uploaded claim dates are not hidden by
        # a stale fiscal-month range.
        bound_sets = [
            _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=["Fiscal Month"],
            ),
            _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=["Month", "Month-Year", "Month Year", "Month_Year", "Month Name", "Month_Name"],
            ),
            _bounds_from_jsonb_columns(
                db=db,
                source=source,
                dataset_type=dataset_key,
                job_id=job_id,
                columns=[
                    "Claim Date",
                    "Day of Call_Date",
                    "Call_Date",
                    "Call Date",
                    "Date",
                    "Payment_date",
                    "Payment Date",
                    "Posting Date",
                    "Complete Date",
                    "Bill Created Date",
                ],
            ),
        ]
        if not any(local_min is not None or local_max is not None for local_min, local_max in bound_sets):
            scoped_df = _load_df()
            bound_sets = [
                _bounds_from_columns(scoped_df, ["Fiscal Month"]),
                _bounds_from_columns(
                    scoped_df,
                    ["Month", "Month-Year", "Month Year", "Month_Year", "Month Name", "Month_Name"],
                ),
                _bounds_from_columns(
                    scoped_df,
                    [
                        "Claim Date",
                        "Day of Call_Date",
                        "Call_Date",
                        "Call Date",
                        "Date",
                        "Payment_date",
                        "Payment Date",
                        "Posting Date",
                        "Complete Date",
                        "Bill Created Date",
                    ],
                ),
            ]
        min_candidates = [mn for mn, _mx in bound_sets if mn is not None]
        max_candidates = [mx for _mn, mx in bound_sets if mx is not None]
        if min_candidates or max_candidates:
            return (
                min(min_candidates) if min_candidates else None,
                max(max_candidates) if max_candidates else None,
            )

    fallback_claims_columns = [
        "Day of Call_Date",
        "Call_Date",
        "Call Date",
        "Call_Registered_Date",
        "Call Registered Date",
        "Call_Initiated_Date",
        "Call Initiated Date",
        "Month-Year",
        "Month Year",
        "Month_Year",
        "Month Year",
        "Fiscal Month",
        "Invoice_Date_",
        "Invoice Date",
        "Payment_date",
        "Payment Date",
        "Posting Date",
        "Complete Date",
        "Bill Created Date",
        "Warranty_start_date_",
        "Warranty Start Date",
        "Date",
    ]
    fast_min, fast_max = _bounds_from_jsonb_columns(
        db=db,
        source=source,
        dataset_type=dataset_key,
        job_id=job_id,
        columns=fallback_claims_columns,
    )
    if fast_min is not None or fast_max is not None:
        return fast_min, fast_max
    return _bounds_from_columns(_load_df(), fallback_claims_columns)


def _master_source_bounds(
    *,
    db: Session,
    job_id: str | None,
) -> tuple[str | None, str | None]:
    min_dt: pd.Timestamp | None = None
    max_dt: pd.Timestamp | None = None

    for source_key in [*SAMSUNG_PARTNER_SOURCES, "reliance", "godrej", "hitachi"]:
        for dataset_key in ["sales", "claims"]:
            for candidate_job_id in _master_job_candidates(source_key, job_id):
                local_min, local_max = _date_bounds_for_source_dataset(
                    db=db,
                    source=source_key,
                    dataset_type=dataset_key,
                    job_id=candidate_job_id,
                )
                if local_min is not None and (min_dt is None or local_min < min_dt):
                    min_dt = local_min
                if local_max is not None and (max_dt is None or local_max > max_dt):
                    max_dt = local_max
                if local_min is not None or local_max is not None:
                    break

    return (
        min_dt.date().isoformat() if min_dt is not None else None,
        max_dt.date().isoformat() if max_dt is not None else None,
    )


def _master_job_candidates(source: str, requested_job_id: str | None) -> list[str | None]:
    source_key = (source or "").strip().lower()
    if source_key == "godrej":
        return [None]
    if requested_job_id:
        if source_key in {"reliance", "hitachi"}:
            return [requested_job_id, None]
        return [requested_job_id]
    return [None]


def _load_master_summary(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    candidate_job_ids: list[str | None],
    from_date: str | None,
    to_date: str | None,
) -> tuple[dict[str, Any], str | None]:
    source_key = (source or "").strip().lower()
    force_live_summary = source_key.startswith("samsung") and dataset_type == "sales"
    selected_summary: dict[str, Any] = {}
    selected_job_id: str | None = candidate_job_ids[0] if candidate_job_ids else None
    for candidate_job_id in candidate_job_ids:
        summary = None
        if not force_live_summary:
            summary = get_precomputed_summary(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=candidate_job_id,
                from_date=from_date,
                to_date=to_date,
            )
            if isinstance(summary, dict):
                cache_updated_at = _get_summary_cache_updated_at(
                    db=db,
                    source=source_key,
                    dataset_type=dataset_type,
                    job_id=candidate_job_id,
                    from_date=from_date,
                    to_date=to_date,
                )
                if not _is_precomputed_cache_fresh(
                    cache_updated_at=cache_updated_at,
                    latest_marker_updated_at=_latest_cache_marker_updated_at(
                        db=db,
                        source=source_key,
                        dataset_type=dataset_type,
                        job_id=candidate_job_id,
                        from_date=from_date,
                        to_date=to_date,
                    ),
                ):
                    logger.info(
                        "Master summary cache stale from upload marker; recomputing live source=%s dataset=%s job_id=%s",
                        source_key,
                        dataset_type,
                        candidate_job_id,
                    )
                    summary = None
        if summary is None:
            try:
                summary = compute_summary_values(
                    db=db,
                    job_id=candidate_job_id,
                    source=source,
                    dataset_type=dataset_type,
                    from_date=from_date,
                    to_date=to_date,
                )
                upsert_precomputed_summary(
                    db=db,
                    source=source,
                    dataset_type=dataset_type,
                    job_id=candidate_job_id,
                    from_date=from_date,
                    to_date=to_date,
                    summary=summary if isinstance(summary, dict) else {},
                )
            except Exception:
                logger.exception(
                    "Failed to compute master summary source=%s dataset=%s job_id=%s",
                    source,
                    dataset_type,
                    candidate_job_id,
                )
                summary = {}

        selected_summary = summary if isinstance(summary, dict) else {}
        selected_job_id = candidate_job_id
        if _summary_has_signal(selected_summary):
            break

    return selected_summary, selected_job_id


def _load_master_metric_rows(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    metric: str,
    candidate_job_ids: list[str | None],
    preferred_job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> list[dict[str, Any]]:
    source_key = (source or "").strip().lower()
    force_live_rows = (
        source_key.startswith("samsung")
        and dataset_type == "sales"
        and metric in {"earned_premium", "zopper_earned_premium"}
    )
    ordered_candidates: list[str | None] = []
    if preferred_job_id in candidate_job_ids:
        ordered_candidates.append(preferred_job_id)
    for candidate in candidate_job_ids:
        if candidate not in ordered_candidates:
            ordered_candidates.append(candidate)

    selected_rows: list[dict[str, Any]] = []
    for candidate_job_id in ordered_candidates:
        rows = None
        should_recompute_live = False
        if not force_live_rows:
            rows = get_precomputed_graph(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=candidate_job_id,
                dimension="month",
                metric=metric,
                bucket="month",
                from_date=from_date,
                to_date=to_date,
            )
            if rows is not None:
                cache_updated_at = _get_graph_cache_updated_at(
                    db=db,
                    source=source_key,
                    dataset_type=dataset_type,
                    job_id=candidate_job_id,
                    dimension="month",
                    metric=metric,
                    bucket="month",
                    from_date=from_date,
                    to_date=to_date,
                )
                if not _is_precomputed_cache_fresh(
                    cache_updated_at=cache_updated_at,
                    latest_marker_updated_at=_latest_cache_marker_updated_at(
                        db=db,
                        source=source_key,
                        dataset_type=dataset_type,
                        job_id=candidate_job_id,
                        from_date=from_date,
                        to_date=to_date,
                        cache_kind="graph",
                        dimension="month",
                    ),
                ):
                    logger.info(
                        "Master graph cache stale from upload marker; recomputing live source=%s dataset=%s metric=%s job_id=%s",
                        source_key,
                        dataset_type,
                        metric,
                        candidate_job_id,
                    )
                    should_recompute_live = True
            if rows is not None and not _rows_have_values(rows, metric):
                logger.warning(
                    "Empty master metric graph cache detected; recomputing live source=%s dataset=%s metric=%s job_id=%s",
                    source,
                    dataset_type,
                    metric,
                    candidate_job_id,
                )
                should_recompute_live = True
            if (
                rows is not None
                and not should_recompute_live
                and source_key == "godrej"
                and dataset_type == "sales"
                and (
                    _rows_have_future_months(rows)
                    or _rows_have_month_window_mismatch(
                        rows,
                        from_date=from_date,
                        to_date=to_date,
                    )
                )
            ):
                logger.warning(
                    "Stale master metric graph detected; recomputing live source=%s dataset=%s metric=%s job_id=%s",
                    source,
                    dataset_type,
                    metric,
                    candidate_job_id,
                )
                should_recompute_live = True
        if rows is None or should_recompute_live:
            try:
                rows = compute_by_dimension_rows(
                    db=db,
                    job_id=candidate_job_id,
                    dimension="month",
                    metric=metric,
                    source=source,
                    dataset_type=dataset_type,
                    bucket="month",
                    from_date=from_date,
                    to_date=to_date,
                )
                upsert_precomputed_graph(
                    db=db,
                    source=source,
                    dataset_type=dataset_type,
                    job_id=candidate_job_id,
                    dimension="month",
                    metric=metric,
                    bucket="month",
                    from_date=from_date,
                    to_date=to_date,
                    rows=rows if isinstance(rows, list) else [],
                )
            except Exception:
                logger.exception(
                    "Failed to compute master metric rows source=%s dataset=%s metric=%s job_id=%s",
                    source,
                    dataset_type,
                    metric,
                    candidate_job_id,
                )
                rows = []

        selected_rows = rows if isinstance(rows, list) else []
        if _rows_have_values(selected_rows, metric):
            break
    return selected_rows


def _build_master_dashboard_payload(
    *,
    db: Session,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    summaries: dict[str, dict[str, Any]] = {}
    rows: dict[str, list[dict[str, Any]]] = {}

    samsung_candidates = _master_job_candidates("samsung", job_id)
    samsung_sales_summary, _ = _load_master_summary(
        db=db,
        source="samsung",
        dataset_type="sales",
        candidate_job_ids=samsung_candidates,
        from_date=from_date,
        to_date=to_date,
    )
    samsung_claims_summary, _ = _load_master_summary(
        db=db,
        source="samsung",
        dataset_type="claims",
        candidate_job_ids=samsung_candidates,
        from_date=from_date,
        to_date=to_date,
    )
    summaries["samsung_sales"] = samsung_sales_summary
    summaries["samsung_claims"] = samsung_claims_summary

    source_configs = [
        ("samsung_vs", "samsung_vs"),
        ("samsung_croma", "samsung_croma"),
        ("samsung_reliance_digital", "samsung_reliance_digital"),
        ("reliance", "reliance"),
        ("godrej", "godrej"),
        ("hitachi", "hitachi"),
    ]

    for source_key, prefix in source_configs:
        candidates = _master_job_candidates(source_key, job_id)
        sales_summary, sales_job = _load_master_summary(
            db=db,
            source=source_key,
            dataset_type="sales",
            candidate_job_ids=candidates,
            from_date=from_date,
            to_date=to_date,
        )
        claims_summary, claims_job = _load_master_summary(
            db=db,
            source=source_key,
            dataset_type="claims",
            candidate_job_ids=candidates,
            from_date=from_date,
            to_date=to_date,
        )

        summaries[f"{prefix}_sales"] = sales_summary
        summaries[f"{prefix}_claims"] = claims_summary

        rows[f"{prefix}_gross"] = _load_master_metric_rows(
            db=db,
            source=source_key,
            dataset_type="sales",
            metric="gross_premium",
            candidate_job_ids=candidates,
            preferred_job_id=sales_job,
            from_date=from_date,
            to_date=to_date,
        )
        rows[f"{prefix}_earned"] = _load_master_metric_rows(
            db=db,
            source=source_key,
            dataset_type="sales",
            metric="earned_premium",
            candidate_job_ids=candidates,
            preferred_job_id=sales_job,
            from_date=from_date,
            to_date=to_date,
        )
        rows[f"{prefix}_zopper"] = _load_master_metric_rows(
            db=db,
            source=source_key,
            dataset_type="sales",
            metric="zopper_earned_premium",
            candidate_job_ids=candidates,
            preferred_job_id=sales_job,
            from_date=from_date,
            to_date=to_date,
        )
        rows[f"{prefix}_claims"] = _load_master_metric_rows(
            db=db,
            source=source_key,
            dataset_type="claims",
            metric="claims",
            candidate_job_ids=candidates,
            preferred_job_id=claims_job,
            from_date=from_date,
            to_date=to_date,
        )

    min_date, max_date = _bounds_from_master_rows(list(rows.values()))
    min_date, max_date = _finalize_master_date_bounds(min_date, max_date)

    return {
        "summaries": summaries,
        "rows": rows,
        "date_bounds": {
            "min_date": min_date,
            "max_date": max_date,
        },
    }


def _schedule_master_dashboard_rebuild(
    *,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
    cache_source: str,
) -> None:
    key = f"{job_id or ''}|{from_date or ''}|{to_date or ''}|{cache_source}"
    with _master_rebuild_lock:
        if key in _master_rebuild_inflight:
            return
        _master_rebuild_inflight.add(key)

    def _worker() -> None:
        worker_db = SessionLocal()
        try:
            payload = _build_master_dashboard_payload(
                db=worker_db,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )
            upsert_precomputed_summary(
                db=worker_db,
                source=cache_source,
                dataset_type="overview",
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                summary=payload,
            )
            worker_db.commit()
        except Exception:
            worker_db.rollback()
            logger.exception(
                "Failed background master dashboard rebuild job_id=%s",
                job_id,
            )
        finally:
            worker_db.close()
            with _master_rebuild_lock:
                _master_rebuild_inflight.discard(key)

    threading.Thread(
        target=_worker,
        name=f"master-dashboard-rebuild-{job_id or 'untagged'}",
        daemon=True,
    ).start()


def _schedule_summary_rebuild(
    *,
    job_id: str | None,
    source: str,
    dataset_type: str,
    from_date: str | None,
    to_date: str | None,
) -> None:
    key = "|".join(
        [
            (source or "").strip().lower(),
            (dataset_type or "").strip().lower(),
            (job_id or "").strip(),
            from_date or "",
            to_date or "",
        ]
    )
    with _summary_rebuild_lock:
        if key in _summary_rebuild_inflight:
            return
        _summary_rebuild_inflight.add(key)

    def _worker() -> None:
        worker_db = SessionLocal()
        try:
            payload = compute_summary_values(
                db=worker_db,
                job_id=job_id,
                source=source,
                dataset_type=dataset_type,
                from_date=from_date,
                to_date=to_date,
            )
            upsert_precomputed_summary(
                db=worker_db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                summary=payload if isinstance(payload, dict) else {},
            )
            worker_db.commit()
        except Exception:
            worker_db.rollback()
            logger.exception(
                "Failed background summary rebuild source=%s dataset=%s from=%s to=%s",
                source,
                dataset_type,
                from_date,
                to_date,
            )
        finally:
            worker_db.close()
            with _summary_rebuild_lock:
                _summary_rebuild_inflight.discard(key)

    threading.Thread(
        target=_worker,
        name=f"summary-rebuild-{(source or 'unknown').replace(' ', '-')}",
        daemon=True,
    ).start()


def _schedule_graph_rebuild(
    *,
    job_id: str | None,
    source: str,
    dataset_type: str,
    dimension: str,
    metric: str,
    bucket: str | None,
    from_date: str | None,
    to_date: str | None,
) -> None:
    key = "|".join(
        [
            (source or "").strip().lower(),
            (dataset_type or "").strip().lower(),
            (job_id or "").strip(),
            (dimension or "").strip().lower(),
            (metric or "").strip().lower(),
            (bucket or "").strip().lower(),
            from_date or "",
            to_date or "",
        ]
    )
    with _graph_rebuild_lock:
        if key in _graph_rebuild_inflight:
            return
        _graph_rebuild_inflight.add(key)

    def _worker() -> None:
        worker_db = SessionLocal()
        try:
            rows = compute_by_dimension_rows(
                db=worker_db,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                source=source,
                dataset_type=dataset_type,
                bucket=bucket,
                from_date=from_date,
                to_date=to_date,
            )
            upsert_precomputed_graph(
                db=worker_db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                bucket=bucket,
                from_date=from_date,
                to_date=to_date,
                rows=rows if isinstance(rows, list) else [],
            )
            worker_db.commit()
        except Exception:
            worker_db.rollback()
            logger.exception(
                "Failed background graph rebuild source=%s dataset=%s dimension=%s metric=%s from=%s to=%s",
                source,
                dataset_type,
                dimension,
                metric,
                from_date,
                to_date,
            )
        finally:
            worker_db.close()
            with _graph_rebuild_lock:
                _graph_rebuild_inflight.discard(key)

    threading.Thread(
        target=_worker,
        name=f"graph-rebuild-{(source or 'unknown').replace(' ', '-')}",
        daemon=True,
    ).start()


def _is_valid_master_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    summaries = payload.get("summaries")
    rows = payload.get("rows")
    if not isinstance(summaries, dict) or not isinstance(rows, dict):
        return False
    required_summary_keys = {
        "samsung_sales",
        "samsung_claims",
        "samsung_vs_sales",
        "samsung_croma_sales",
        "samsung_reliance_digital_sales",
        "reliance_sales",
        "godrej_sales",
        "samsung_vs_claims",
        "samsung_croma_claims",
        "samsung_reliance_digital_claims",
        "reliance_claims",
        "godrej_claims",
        "hitachi_sales",
        "hitachi_claims",
    }
    if any(key not in summaries for key in required_summary_keys):
        return False
    required_row_keys = {
        "samsung_vs_gross",
        "samsung_vs_earned",
        "samsung_vs_zopper",
        "samsung_croma_gross",
        "samsung_croma_earned",
        "samsung_croma_zopper",
        "samsung_reliance_digital_gross",
        "samsung_reliance_digital_earned",
        "samsung_reliance_digital_zopper",
        "reliance_gross",
        "reliance_earned",
        "reliance_zopper",
        "godrej_gross",
        "godrej_earned",
        "godrej_zopper",
        "hitachi_gross",
        "hitachi_earned",
        "hitachi_zopper",
        "samsung_vs_claims",
        "samsung_croma_claims",
        "samsung_reliance_digital_claims",
        "reliance_claims",
        "godrej_claims",
        "hitachi_claims",
    }
    if any(key not in rows for key in required_row_keys):
        return False
    return True


def _empty_master_payload(
    *,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    required_summary_keys = {
        "samsung_sales",
        "samsung_claims",
        "samsung_vs_sales",
        "samsung_croma_sales",
        "samsung_reliance_digital_sales",
        "reliance_sales",
        "godrej_sales",
        "samsung_vs_claims",
        "samsung_croma_claims",
        "samsung_reliance_digital_claims",
        "reliance_claims",
        "godrej_claims",
        "hitachi_sales",
        "hitachi_claims",
    }
    required_row_keys = {
        "samsung_vs_gross",
        "samsung_vs_earned",
        "samsung_vs_zopper",
        "samsung_croma_gross",
        "samsung_croma_earned",
        "samsung_croma_zopper",
        "samsung_reliance_digital_gross",
        "samsung_reliance_digital_earned",
        "samsung_reliance_digital_zopper",
        "reliance_gross",
        "reliance_earned",
        "reliance_zopper",
        "godrej_gross",
        "godrej_earned",
        "godrej_zopper",
        "hitachi_gross",
        "hitachi_earned",
        "hitachi_zopper",
        "samsung_vs_claims",
        "samsung_croma_claims",
        "samsung_reliance_digital_claims",
        "reliance_claims",
        "godrej_claims",
        "hitachi_claims",
    }
    empty_summary = {
        "gross_premium": 0.0,
        "earned_premium": 0.0,
        "zopper_earned_premium": 0.0,
        "units_sold": 0,
    }
    summaries = {key: dict(empty_summary) for key in required_summary_keys}
    rows = {key: [] for key in required_row_keys}
    return {
        "summaries": summaries,
        "rows": rows,
        "date_bounds": {
            "min_date": from_date,
            "max_date": to_date,
        },
    }


def _get_master_cache_updated_at(
    *,
    db: Session,
    cache_source: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> datetime | None:
    row = (
        db.query(PrecomputedSummary)
        .filter(PrecomputedSummary.source == (cache_source or "").strip().lower())
        .filter(PrecomputedSummary.dataset_type == "overview")
        .filter(PrecomputedSummary.job_key == (job_id or "").strip())
        .filter(PrecomputedSummary.from_date == (from_date or "").strip())
        .filter(PrecomputedSummary.to_date == (to_date or "").strip())
        .first()
    )
    return row.updated_at if row is not None else None


def _get_summary_cache_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> datetime | None:
    row = (
        db.query(PrecomputedSummary)
        .filter(PrecomputedSummary.source == (source or "").strip().lower())
        .filter(PrecomputedSummary.dataset_type == (dataset_type or "").strip().lower())
        .filter(PrecomputedSummary.job_key == (job_id or "").strip())
        .filter(PrecomputedSummary.from_date == (from_date or "").strip())
        .filter(PrecomputedSummary.to_date == (to_date or "").strip())
        .first()
    )
    return row.updated_at if row is not None else None


def _get_graph_cache_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    bucket: str | None,
    from_date: str | None,
    to_date: str | None,
) -> datetime | None:
    row = (
        db.query(PrecomputedGraph)
        .filter(PrecomputedGraph.source == (source or "").strip().lower())
        .filter(PrecomputedGraph.dataset_type == (dataset_type or "").strip().lower())
        .filter(PrecomputedGraph.job_key == (job_id or "").strip())
        .filter(PrecomputedGraph.dimension == (dimension or "").strip().lower())
        .filter(PrecomputedGraph.metric == (metric or "").strip().lower())
        .filter(PrecomputedGraph.bucket == (bucket or "").strip())
        .filter(PrecomputedGraph.from_date == (from_date or "").strip())
        .filter(PrecomputedGraph.to_date == (to_date or "").strip())
        .first()
    )
    return row.updated_at if row is not None else None


def _latest_manual_update_marker_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> datetime | None:
    source_key = (source or "").strip().lower()
    source_variants = list(SAMSUNG_SOURCE_VARIANTS) if source_key == "samsung" else [source_key]
    query = (
        db.query(func.max(ManualUpdateMarker.updated_at))
        .filter(ManualUpdateMarker.source.in_(source_variants))
        .filter(ManualUpdateMarker.dataset_type == (dataset_type or "").strip().lower())
    )
    job_key = (job_id or "").strip()
    if job_key:
        query = query.filter(ManualUpdateMarker.job_key.in_([job_key, ""]))
    return query.scalar()


def _latest_cache_marker_updated_at(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None = None,
    to_date: str | None = None,
    cache_kind: str = "summary",
    dimension: str | None = None,
) -> datetime | None:
    latest_marker = _latest_manual_update_marker_updated_at(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
    )

    source_key = (source or "").strip().lower()
    dataset_key = (dataset_type or "").strip().lower()
    samsung_source = normalize_samsung_source(source_key)
    canonical_source = samsung_source if samsung_source else source_key
    if canonical_source in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        canonical_source = "reliance"
    if canonical_source in {"goodrej", "goddrej"}:
        canonical_source = "godrej"

    def _needs_scope_refresh() -> bool:
        if dataset_key != "sales":
            return False
        if not from_date and not to_date:
            return False
        if not from_date or not to_date:
            return True
        start = pd.to_datetime(from_date, errors="coerce")
        end = pd.to_datetime(to_date, errors="coerce")
        if pd.isna(start) or pd.isna(end):
            return True
        return abs((end.normalize() - start.normalize()).days) <= 31

    code_marker: datetime | None = None
    if _needs_scope_refresh():
        if cache_kind == "graph" and canonical_source in _GRAPH_SCOPE_REFRESH_SOURCES:
            code_marker = _SALES_SCOPING_CACHE_UPDATED_AT
        elif cache_kind != "graph" and canonical_source in _SUMMARY_SCOPE_REFRESH_SOURCES:
            code_marker = _SALES_SCOPING_CACHE_UPDATED_AT

    if (
        cache_kind == "graph"
        and dataset_key == "sales"
        and canonical_source == "reliance"
        and _to_safe_key(dimension or "") in {"brand", "article_brand"}
    ):
        if code_marker is None:
            code_marker = _RELIANCE_BRAND_CACHE_UPDATED_AT
        else:
            code_marker = max(pd.Timestamp(code_marker), pd.Timestamp(_RELIANCE_BRAND_CACHE_UPDATED_AT)).to_pydatetime()

    if (
        cache_kind == "graph"
        and canonical_source == "hitachi"
        and _to_safe_key(dimension or "") == "plan_category"
    ):
        if code_marker is None:
            code_marker = _HITACHI_PLAN_CACHE_UPDATED_AT
        else:
            code_marker = max(pd.Timestamp(code_marker), pd.Timestamp(_HITACHI_PLAN_CACHE_UPDATED_AT)).to_pydatetime()

    if latest_marker is None:
        return code_marker
    if code_marker is None:
        return latest_marker
    latest_ts = pd.Timestamp(latest_marker)
    code_ts = pd.Timestamp(code_marker)
    if latest_ts.tzinfo is not None:
        latest_ts = latest_ts.tz_convert(None)
    if code_ts.tzinfo is not None:
        code_ts = code_ts.tz_convert(None)
    return max(latest_ts, code_ts).to_pydatetime()


def _is_precomputed_cache_fresh(
    *,
    cache_updated_at: datetime | None,
    latest_marker_updated_at: datetime | None,
) -> bool:
    if cache_updated_at is None:
        return False
    if latest_marker_updated_at is None:
        return True
    cache_ts = pd.Timestamp(cache_updated_at)
    latest_ts = pd.Timestamp(latest_marker_updated_at)
    if cache_ts.tzinfo is not None:
        cache_ts = cache_ts.tz_convert(None)
    if latest_ts.tzinfo is not None:
        latest_ts = latest_ts.tz_convert(None)
    return cache_ts >= latest_ts


def _merge_cache_freshness_markers(
    *markers: datetime | None,
) -> datetime | None:
    latest: pd.Timestamp | None = None
    for marker in markers:
        if marker is None:
            continue
        marker_ts = pd.Timestamp(marker)
        if marker_ts.tzinfo is not None:
            marker_ts = marker_ts.tz_convert(None)
        latest = marker_ts if latest is None else max(latest, marker_ts)
    return latest.to_pydatetime() if latest is not None else None


def _latest_master_marker_updated_at(
    *,
    db: Session,
    job_id: str | None,
) -> datetime | None:
    master_sources = [
        "samsung",
        "samsung_vs",
        "samsung_croma",
        "samsung_reliance_digital",
        "samsung_vijay_sales",
        "reliance",
        "godrej",
        "hitachi",
    ]
    query = (
        db.query(func.max(ManualUpdateMarker.updated_at))
        .filter(ManualUpdateMarker.source.in_(master_sources))
        .filter(ManualUpdateMarker.dataset_type.in_(["sales", "claims"]))
    )
    job_key = (job_id or "").strip()
    if job_key:
        query = query.filter(ManualUpdateMarker.job_key.in_([job_key, ""]))
    return query.scalar()


@router.get("/master-dashboard")
def analytics_master_dashboard(
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    started = time.perf_counter()
    from_date, to_date = _sanitize_range(from_date, to_date)
    cache_source = "master_dashboard_v15"

    cached = get_precomputed_summary(
        db=db,
        source=cache_source,
        dataset_type="overview",
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    cache_updated_at = _get_master_cache_updated_at(
        db=db,
        cache_source=cache_source,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    latest_marker_updated_at = _merge_cache_freshness_markers(
        _latest_master_marker_updated_at(db=db, job_id=job_id),
        _MASTER_DASHBOARD_CACHE_UPDATED_AT,
    )
    cache_is_fresh = False
    if cache_updated_at is not None:
        cache_is_fresh = _is_precomputed_cache_fresh(
            cache_updated_at=cache_updated_at,
            latest_marker_updated_at=latest_marker_updated_at,
        )
    cache_is_code_stale = False
    if cache_updated_at is not None:
        cache_is_code_stale = not _is_precomputed_cache_fresh(
            cache_updated_at=cache_updated_at,
            latest_marker_updated_at=_MASTER_DASHBOARD_CACHE_UPDATED_AT,
        )
    cache_has_godrej_sales_month_mismatch = _master_payload_has_godrej_sales_month_mismatch(
        cached,
        from_date=from_date,
        to_date=to_date,
    )

    if _is_valid_master_payload(cached) and not cache_has_godrej_sales_month_mismatch:
        if cache_is_fresh:
            logger.info(
                "TIMING analytics.master_dashboard mode=precomputed duration_ms=%.2f",
                (time.perf_counter() - started) * 1000,
            )
            return cached
        if cache_is_code_stale:
            logger.info(
                "Master dashboard cache stale from code marker; rebuilding synchronously.",
            )
        else:
            logger.info(
                "Master dashboard cache stale; serving cached payload and rebuilding in background.",
            )
            _schedule_master_dashboard_rebuild(
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                cache_source=cache_source,
            )
            return cached

    if cache_has_godrej_sales_month_mismatch:
        logger.warning(
            "Invalid Godrej master cache detected; rebuilding synchronously source=%s from=%s to=%s",
            cache_source,
            from_date,
            to_date,
        )

    payload_is_fallback = False
    try:
        payload = _build_master_dashboard_payload(
            db=db,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
    except Exception:
        logger.exception(
            "Failed to build master dashboard payload job_id=%s",
            job_id,
        )
        payload = _empty_master_payload(from_date=from_date, to_date=to_date)
        payload_is_fallback = True

    if not payload_is_fallback:
        try:
            upsert_precomputed_summary(
                db=db,
                source=cache_source,
                dataset_type="overview",
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
                summary=payload,
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "Failed to upsert master dashboard precomputed payload job_id=%s",
                job_id,
            )

    logger.info(
        "TIMING analytics.master_dashboard mode=live duration_ms=%.2f",
        (time.perf_counter() - started) * 1000,
    )
    return payload


@router.get("/forecast")
def analytics_forecast(
    source: str = Query(...),
    dataset_type: str = Query(...),
    metric: str = Query("gross_premium"),
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    horizon_months: int = Query(6, ge=1, le=24),
    grain: str = Query("month"),
    db: Session = Depends(get_db),
):
    safe_from, safe_to = _sanitize_range(from_date, to_date)
    resolved_source, _ = _normalize_source(source)
    dataset_key = (dataset_type or "").strip().lower()
    resolved_job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=dataset_key,
        job_id=job_id,
        context="forecast",
    )
    return build_forecast_response(
        db=db,
        source=resolved_source,
        dataset_type=dataset_key,
        metric=metric,
        job_id=resolved_job_id,
        from_date=safe_from,
        to_date=safe_to,
        horizon_months=horizon_months,
        grain=grain,
    )


@router.get("/annual-comparison")
def analytics_annual_comparison(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    metric: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    started = time.perf_counter()
    safe_from, safe_to = _sanitize_range(from_date, to_date)
    resolved_source, engine_key = _normalize_source(source)
    normalized_dataset = (dataset_type or "").strip().lower()
    current_from, current_to = _annual_build_current_range(safe_from, safe_to)
    year_buckets = _annual_build_year_buckets(current_from, current_to)
    if normalized_dataset not in {"sales", "claims"} or not year_buckets:
        return {
            "year_buckets": year_buckets,
            "payload_by_metric": {},
        }

    selected_metric = _to_safe_key(metric or "")
    if normalized_dataset == "claims":
        if selected_metric not in _ANNUAL_SUPPORTED_CLAIMS_METRICS:
            selected_metric = "claims"
        cache_metric = selected_metric
    else:
        selected_metric = "quantity"
        cache_metric = "sales"

    resolved_job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=normalized_dataset,
        job_id=job_id,
        context="annual_comparison",
    )
    cache_bucket = _annual_comparison_cache_bucket(resolved_source)

    cached_rows = get_precomputed_graph(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=resolved_job_id,
        dimension=_ANNUAL_COMPARISON_DIMENSION,
        metric=cache_metric,
        bucket=cache_bucket,
        from_date=current_from,
        to_date=current_to,
    )
    if cached_rows:
        cached_payload = next(
            (
                row
                for row in cached_rows
                if isinstance(row, dict) and isinstance(row.get("payload_by_metric"), dict)
            ),
            None,
        )
        if cached_payload is not None and _annual_payload_has_signal(cached_payload):
            logger.info(
                "TIMING analytics.annual_comparison source=%s dataset=%s metric=%s mode=precomputed duration_ms=%.2f",
                source,
                dataset_type,
                cache_metric,
                (time.perf_counter() - started) * 1000,
            )
            return cached_payload

    fetch_from = year_buckets[0]["from"]
    fetch_to = year_buckets[-1]["to"]
    needs_sales = normalized_dataset == "sales" or selected_metric == "loss_ratio"
    needs_claims = normalized_dataset == "claims"

    engine = None
    base_payload = None
    engine_cls = ENGINE_REGISTRY.get(engine_key)
    if engine_cls is not None:
        engine = engine_cls(
            db=db,
            job_id=resolved_job_id,
            source=resolved_source,
            dataset_type=normalized_dataset,
            from_date=fetch_from,
            to_date=fetch_to,
        )
        if engine_key in {"samsung", "godrej", "hitachi"}:
            base_payload = engine.load_data(include_sales=needs_sales, include_claims=needs_claims)
        else:
            base_payload = engine.load_data()

    def _fetch_rows(
        *,
        dimension: str,
        metric_name: str,
        category_filters: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if resolved_source == "samsung" and normalized_dataset == "sales":
            try:
                rows = compute_by_dimension_rows(
                    db=db,
                    job_id=resolved_job_id,
                    dimension=dimension,
                    metric=metric_name,
                    source=resolved_source,
                    dataset_type=normalized_dataset,
                    bucket="month" if _to_safe_key(dimension) == "month" else None,
                    from_date=fetch_from,
                    to_date=fetch_to,
                    category_filters=category_filters,
                )
                return _annual_sum_samsung_partner_rows(
                    rows,
                    dimension=dimension,
                    metric=metric_name,
                )
            except Exception:
                logger.exception(
                    "Annual comparison samsung partner-merge failed source=%s dataset=%s dimension=%s metric=%s filters=%s",
                    resolved_source,
                    normalized_dataset,
                    dimension,
                    metric_name,
                    category_filters,
                )
                return []

        if engine is None:
            try:
                return compute_by_dimension_rows(
                    db=db,
                    job_id=resolved_job_id,
                    dimension=dimension,
                    metric=metric_name,
                    source=resolved_source,
                    dataset_type=normalized_dataset,
                    bucket="month" if _to_safe_key(dimension) == "month" else None,
                    from_date=fetch_from,
                    to_date=fetch_to,
                    category_filters=category_filters,
                )
            except Exception:
                logger.exception(
                    "Annual comparison fallback compute failed source=%s dataset=%s dimension=%s metric=%s",
                    resolved_source,
                    normalized_dataset,
                    dimension,
                    metric_name,
                )
                return []

        if not category_filters:
            try:
                rows = engine.compute_by_dimension(dimension=dimension, metric=metric_name)
                return _normalize_dimension_rows(rows, dimension=dimension)
            except Exception:
                logger.exception(
                    "Annual comparison engine compute failed source=%s dataset=%s dimension=%s metric=%s",
                    resolved_source,
                    normalized_dataset,
                    dimension,
                    metric_name,
                )
                return []

        original_cache = getattr(engine, "_loaded_data_cache", None)
        try:
            filtered_payload = _apply_dimension_filters_to_payload(base_payload or {}, category_filters)
            if engine_key in {"samsung", "godrej", "hitachi"}:
                cache_map = dict(original_cache or {})
                cache_map[(needs_sales, needs_claims)] = filtered_payload
                engine._loaded_data_cache = cache_map
            else:
                engine._loaded_data_cache = filtered_payload
            rows = engine.compute_by_dimension(dimension=dimension, metric=metric_name)
            return _normalize_dimension_rows(rows, dimension=dimension)
        except Exception:
            logger.exception(
                "Annual comparison filtered engine compute failed source=%s dataset=%s dimension=%s metric=%s filters=%s",
                resolved_source,
                normalized_dataset,
                dimension,
                metric_name,
                category_filters,
            )
            return []
        finally:
            engine._loaded_data_cache = original_cache

    plan_seed_metric = "quantity" if normalized_dataset == "sales" else "claims"
    plan_rows = _fetch_rows(dimension="plan_category", metric_name=plan_seed_metric)
    plans = _annual_extract_plans(resolved_source, plan_rows)
    payload_by_metric: dict[str, Any] = {}

    if normalized_dataset == "sales":
        quantity_rows_by_plan = {
            plan: _fetch_rows(
                dimension="month",
                metric_name="quantity",
                category_filters=[{"dimension": "plan_category", "values": [plan]}],
            )
            for plan in plans
        }
        payload_by_metric["quantity"] = _annual_build_plan_payload(
            plans,
            quantity_rows_by_plan,
            "quantity",
            year_buckets,
        )

        for summary_metric in _ANNUAL_SALES_SUMMARY_METRICS:
            summary_rows = _fetch_rows(dimension="month", metric_name=summary_metric)
            payload_by_metric[summary_metric] = _annual_build_total_payload(
                summary_rows,
                summary_metric,
                year_buckets,
            )
    else:
        rows_by_plan = {
            plan: _fetch_rows(
                dimension="month",
                metric_name=selected_metric,
                category_filters=[{"dimension": "plan_category", "values": [plan]}],
            )
            for plan in plans
        }
        payload_by_metric[selected_metric] = _annual_build_plan_payload(
            plans,
            rows_by_plan,
            selected_metric,
            year_buckets,
        )

    response = {
        "year_buckets": year_buckets,
        "payload_by_metric": payload_by_metric,
    }

    if _annual_payload_has_signal(response):
        try:
            upsert_precomputed_graph(
                db=db,
                source=resolved_source,
                dataset_type=normalized_dataset,
                job_id=resolved_job_id,
                dimension=_ANNUAL_COMPARISON_DIMENSION,
                metric=cache_metric,
                bucket=cache_bucket,
                from_date=current_from,
                to_date=current_to,
                rows=[response],
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "Failed to upsert annual comparison cache source=%s dataset=%s metric=%s",
                resolved_source,
                normalized_dataset,
                cache_metric,
            )

    logger.info(
        "TIMING analytics.annual_comparison source=%s dataset=%s metric=%s plans=%s duration_ms=%.2f",
        source,
        dataset_type,
        cache_metric,
        len(plans),
        (time.perf_counter() - started) * 1000,
    )
    return response


@router.get("/summary")
def analytics_summary(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    started = time.perf_counter()
    from_date, to_date = _sanitize_range(from_date, to_date)
    resolved_source, _ = _normalize_source(source)
    normalized_dataset = (dataset_type or "").strip().lower()
    job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=normalized_dataset,
        job_id=job_id,
        context="summary",
    )
    force_live_summary = resolved_source == "samsung" and normalized_dataset == "sales"

    if resolved_source == "samsung" and not force_live_summary:
        partner_rows: list[dict[str, Any]] = []
        for partner_source in SAMSUNG_PARTNER_SOURCES:
            partner_summary = get_precomputed_summary(
                db=db,
                source=partner_source,
                dataset_type=normalized_dataset,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )
            partner_cache_updated_at = _get_summary_cache_updated_at(
                db=db,
                source=partner_source,
                dataset_type=normalized_dataset,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )
            partner_cache_is_fresh = _is_precomputed_cache_fresh(
                cache_updated_at=partner_cache_updated_at,
                latest_marker_updated_at=_latest_cache_marker_updated_at(
                    db=db,
                    source=partner_source,
                    dataset_type=normalized_dataset,
                    job_id=job_id,
                    from_date=from_date,
                    to_date=to_date,
                ),
            )
            if not isinstance(partner_summary, dict) or not partner_cache_is_fresh:
                partner_rows = []
                break
            partner_rows.append(partner_summary)

        if partner_rows:
            merged = {
                "gross_premium": float(sum(float(row.get("gross_premium", 0) or 0) for row in partner_rows)),
                "earned_premium": float(sum(float(row.get("earned_premium", 0) or 0) for row in partner_rows)),
                "zopper_earned_premium": float(sum(float(row.get("zopper_earned_premium", 0) or 0) for row in partner_rows)),
                "units_sold": int(sum(int(row.get("units_sold", 0) or 0) for row in partner_rows)),
            }
            logger.info(
                "TIMING analytics.summary source=%s dataset=%s mode=precomputed_partner_merge duration_ms=%.2f",
                source,
                dataset_type,
                (time.perf_counter() - started) * 1000,
            )
            return merged

    cached = get_precomputed_summary(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    cache_updated_at = _get_summary_cache_updated_at(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    manual_summary_marker_updated_at = _latest_manual_update_marker_updated_at(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=job_id,
    )
    latest_summary_marker_updated_at = _latest_cache_marker_updated_at(
        db=db,
        source=resolved_source,
        dataset_type=normalized_dataset,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    cache_is_fresh = _is_precomputed_cache_fresh(
        cache_updated_at=cache_updated_at,
        latest_marker_updated_at=latest_summary_marker_updated_at,
    )
    if cached is not None and not force_live_summary:
        if not cache_is_fresh:
            manual_marker_only_stale = False
            if (
                cache_updated_at is not None
                and manual_summary_marker_updated_at is not None
                and latest_summary_marker_updated_at is not None
            ):
                cache_ts = pd.Timestamp(cache_updated_at)
                manual_ts = pd.Timestamp(manual_summary_marker_updated_at)
                latest_ts = pd.Timestamp(latest_summary_marker_updated_at)
                if cache_ts.tzinfo is not None:
                    cache_ts = cache_ts.tz_convert(None)
                if manual_ts.tzinfo is not None:
                    manual_ts = manual_ts.tz_convert(None)
                if latest_ts.tzinfo is not None:
                    latest_ts = latest_ts.tz_convert(None)
                manual_marker_only_stale = cache_ts < manual_ts and latest_ts == manual_ts

            gross_cached = float(cached.get("gross_premium", 0) or 0)
            earned_cached = float(cached.get("earned_premium", 0) or 0)
            zopper_cached = float(cached.get("zopper_earned_premium", 0) or 0)
            all_financial_zero = (
                gross_cached == 0
                and earned_cached == 0
                and zopper_cached == 0
            )

            if manual_marker_only_stale and not all_financial_zero:
                _schedule_summary_rebuild(
                    job_id=job_id,
                    source=resolved_source,
                    dataset_type=normalized_dataset,
                    from_date=from_date,
                    to_date=to_date,
                )
                logger.info(
                    "TIMING analytics.summary source=%s dataset=%s mode=precomputed_stale_background duration_ms=%.2f",
                    source,
                    dataset_type,
                    (time.perf_counter() - started) * 1000,
                )
                return cached

            logger.info(
                "Stale precomputed summary detected; recomputing live source=%s dataset=%s",
                resolved_source,
                normalized_dataset,
            )
        else:
            gross_cached = float(cached.get("gross_premium", 0) or 0)
            earned_cached = float(cached.get("earned_premium", 0) or 0)
            zopper_cached = float(cached.get("zopper_earned_premium", 0) or 0)
            units_cached = int(cached.get("units_sold", 0) or 0)
            all_financial_zero = (
                gross_cached == 0
                and earned_cached == 0
                and zopper_cached == 0
            )

            source_variants = (
                list(SAMSUNG_SOURCE_VARIANTS)
                if resolved_source == "samsung"
                else [resolved_source]
            )

            def _has_rows_in_scope() -> bool:
                if from_date is None and to_date is None:
                    query = (
                        db.query(func.count(DataRow.id))
                        .filter(DataRow.source.in_(source_variants))
                        .filter(DataRow.dataset_type == normalized_dataset)
                    )
                    if job_id is not None:
                        query = query.filter(DataRow.job_id == job_id)
                    count = query.scalar()
                    return int(count or 0) > 0

                for src in source_variants:
                    scoped_df = get_dataframe(
                        db=db,
                        job_id=job_id,
                        source=src,
                        dataset_type=normalized_dataset,
                    )
                    if scoped_df is None or scoped_df.empty:
                        continue
                    scoped_df = filter_by_date_range(scoped_df, normalized_dataset, from_date, to_date)
                    if scoped_df is not None and not scoped_df.empty:
                        return True
                return False

            has_rows_in_scope = _has_rows_in_scope() if all_financial_zero and not cache_is_fresh else False
            is_stale_zero_with_units = all_financial_zero and units_cached > 0
            is_stale_zero_with_rows = False
            if all_financial_zero and units_cached == 0 and not cache_is_fresh:
                is_stale_zero_with_rows = has_rows_in_scope or resolved_source.startswith("samsung")

            if is_stale_zero_with_units or is_stale_zero_with_rows:
                reason = "zero_with_units" if is_stale_zero_with_units else "zero_with_rows"
                logger.warning(
                    "Stale precomputed summary detected (%s); recomputing live source=%s dataset=%s",
                    reason,
                    resolved_source,
                    normalized_dataset,
                )
            else:
                logger.info(
                    "TIMING analytics.summary source=%s dataset=%s mode=precomputed duration_ms=%.2f",
                    source,
                    dataset_type,
                    (time.perf_counter() - started) * 1000,
                )
                return cached
    elif cached is not None and force_live_summary:
        logger.info(
            "Bypassing precomputed summary; recomputing live source=%s dataset=%s from=%s to=%s",
            resolved_source,
            normalized_dataset,
            from_date,
            to_date,
        )

    try:
        out = compute_summary_values(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=normalized_dataset,
            from_date=from_date,
            to_date=to_date,
        )
    except Exception:
        db.rollback()
        logger.exception(
            "Live compute failed for analytics.summary source=%s dataset=%s from=%s to=%s",
            source,
            dataset_type,
            from_date,
            to_date,
        )
        if isinstance(cached, dict):
            return cached
        return {
            "gross_premium": 0,
            "earned_premium": 0,
            "zopper_earned_premium": 0,
            "units_sold": 0,
        }
    logger.info(
        "TIMING analytics.summary source=%s dataset=%s mode=live duration_ms=%.2f",
        source,
        dataset_type,
        (time.perf_counter() - started) * 1000,
    )
    try:
        upsert_precomputed_summary(
            db=db,
            source=resolved_source,
            dataset_type=normalized_dataset,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            summary=out if isinstance(out, dict) else {},
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "Failed to upsert precomputed summary source=%s dataset=%s",
            resolved_source,
            normalized_dataset,
        )
    return out


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
    dataset_key = (dataset_type or "").strip().lower()
    job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=dataset_key,
        job_id=job_id,
        context="last_updated",
    )
    job_key = (job_id or "").strip()

    source_variants = (
        list(SAMSUNG_SOURCE_VARIANTS)
        if resolved_source == "samsung"
        else [resolved_source]
    )

    def _latest_marker_for_sources(sources: list[str], key: str | None) -> datetime | None:
        query = (
            db.query(func.max(ManualUpdateMarker.updated_at))
            .filter(ManualUpdateMarker.dataset_type == dataset_key)
            .filter(ManualUpdateMarker.source.in_(sources))
        )
        if key is not None:
            query = query.filter(ManualUpdateMarker.job_key == key)
        return query.scalar()

    def _source_has_rows(src: str, key: str | None) -> bool:
        query = (
            db.query(func.count(DataRow.id))
            .filter(DataRow.source == src)
            .filter(DataRow.dataset_type == dataset_key)
        )
        if key is not None:
            if key == "":
                query = query.filter(DataRow.job_id.is_(None))
            else:
                query = query.filter(DataRow.job_id == key)
        count = query.scalar()
        return int(count or 0) > 0

    latest = _latest_marker_for_sources(source_variants, job_key if job_key else None)
    if latest is None and job_key:
        # Fallback to aggregate marker for this source/dataset.
        latest = _latest_marker_for_sources(source_variants, "")
    if latest is None and job_key:
        # Final fallback: latest saved date across all tags for this source/dataset.
        latest = _latest_marker_for_sources(source_variants, None)

    # Backfill missing sales marker once for legacy tags so card does not stay "Unknown".
    if latest is None and dataset_key == "sales":
        key_candidates: list[str | None] = [job_key, "", None] if job_key else ["", None]
        marker_source: str | None = None
        marker_job: str | None = None
        for candidate in key_candidates:
            source_with_rows = next(
                (src for src in source_variants if _source_has_rows(src, candidate)),
                None,
            )
            if source_with_rows is not None:
                marker_source = source_with_rows
                marker_job = candidate if candidate not in {"", None} else None
                break

        if marker_source is not None:
            try:
                mark_manual_update(
                    db=db,
                    source=marker_source,
                    dataset_type=dataset_key,
                    job_id=marker_job,
                )
                db.commit()
                latest = _latest_marker_for_sources(source_variants, job_key if job_key else None)
                if latest is None and job_key:
                    latest = _latest_marker_for_sources(source_variants, "")
                if latest is None and job_key:
                    latest = _latest_marker_for_sources(source_variants, None)
            except Exception:
                db.rollback()
                logger.exception(
                    "Failed to initialize manual update marker source=%s dataset=%s job=%s",
                    source,
                    dataset_key,
                    job_key,
                )

    return {"data_upto": latest.date().isoformat() if latest is not None else None}


@router.get("/date-bounds")
def analytics_date_bounds(
    job_id: str | None = Query(None),
    source: str = Query(...),
    dataset_type: str = Query(...),
    db: Session = Depends(get_db),
):
    started = time.perf_counter()
    resolved_source, _ = _normalize_source(source)
    dataset_key = (dataset_type or "").strip().lower()
    job_id = _resolve_job_id_fallback(
        db=db,
        resolved_source=resolved_source,
        dataset_key=dataset_key,
        job_id=job_id,
        context="date_bounds",
    )
    cache_dataset_type = _date_bounds_cache_dataset_type(dataset_key)
    cached = get_precomputed_summary(
        db=db,
        source=resolved_source,
        dataset_type=cache_dataset_type,
        job_id=job_id,
        from_date=None,
        to_date=None,
    )
    cache_updated_at = _get_date_bounds_cache_updated_at(
        db=db,
        source=resolved_source,
        dataset_type=dataset_key,
        job_id=job_id,
    )
    latest_marker_updated_at = _latest_date_bounds_marker_updated_at(
        db=db,
        source=resolved_source,
        dataset_type=dataset_key,
        job_id=job_id,
    )
    cache_is_fresh = False
    if cache_updated_at is not None:
        if latest_marker_updated_at is None:
            cache_is_fresh = True
        else:
            cache_is_fresh = pd.Timestamp(cache_updated_at) >= pd.Timestamp(latest_marker_updated_at)

    force_live_date_bounds = resolved_source == "samsung" and dataset_key == "sales"

    if isinstance(cached, dict) and cache_is_fresh and not force_live_date_bounds:
        min_date = str(cached.get("min_date") or "").strip() or None
        max_date = str(cached.get("max_date") or "").strip() or None
        logger.info(
            "TIMING analytics.date_bounds source=%s dataset=%s mode=precomputed duration_ms=%.2f",
            source,
            dataset_type,
            (time.perf_counter() - started) * 1000,
        )
        return {
            "min_date": min_date,
            "max_date": max_date,
        }

    payload = compute_date_bounds_payload(
        db=db,
        source=resolved_source,
        dataset_type=dataset_key,
        job_id=job_id,
    )
    try:
        upsert_precomputed_summary(
            db=db,
            source=resolved_source,
            dataset_type=cache_dataset_type,
            job_id=job_id,
            from_date=None,
            to_date=None,
            summary=payload,
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "Failed to upsert date bounds cache source=%s dataset=%s job_id=%s",
            resolved_source,
            dataset_key,
            job_id,
        )

    logger.info(
        "TIMING analytics.date_bounds source=%s dataset=%s mode=live duration_ms=%.2f",
        source,
        dataset_type,
        (time.perf_counter() - started) * 1000,
    )
    return payload


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
        elif resolved_source == "hitachi":
            q = q.filter(DataRow.source.ilike("hitachi%"))
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
