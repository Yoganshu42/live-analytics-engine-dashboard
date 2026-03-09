
import json
import threading
import time

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from models.data_rows import DataRow
from services.samsung_partner_config import (
    SAMSUNG_PARTNER_SOURCES,
    SAMSUNG_SOURCE_VARIANTS,
    normalize_samsung_source,
)

_CACHE_TTL_SECONDS = 300
_df_cache_lock = threading.Lock()
_df_cache: dict[tuple[str, str, str], tuple[float, pd.DataFrame]] = {}


def _cache_key(source: str, dataset_type: str, job_id: str | None) -> tuple[str, str, str]:
    return (
        (source or "").strip().lower(),
        (dataset_type or "").strip().lower(),
        (job_id or "").strip(),
    )


def _source_variants(source: str | None) -> list[str]:
    source_key = (source or "").strip().lower()
    if source_key in {"godrej", "goodrej", "goddrej"}:
        # Legacy uploads contain mixed spellings for Godrej.
        return ["godrej", "goodrej", "goddrej"]
    if source_key in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        # Keep legacy Reliance ResQ aliases readable without forcing a migration first.
        return ["reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"]
    samsung_source = normalize_samsung_source(source_key)
    if samsung_source == "samsung":
        return list(SAMSUNG_SOURCE_VARIANTS)
    if samsung_source == "samsung_vs":
        # Keep both aliases readable without requiring a data migration first.
        return ["samsung_vs", "samsung_vijay_sales"]
    if samsung_source in SAMSUNG_PARTNER_SOURCES:
        return [samsung_source]
    return [source_key]


def invalidate_dataframe_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    with _df_cache_lock:
        if source is None and dataset_type is None and job_id is None:
            _df_cache.clear()
            return None

        src_values: set[str] | None = None
        if source is not None:
            src = (source or "").strip().lower()
            samsung_source = normalize_samsung_source(src)
            if samsung_source == "samsung":
                src_values = set(SAMSUNG_SOURCE_VARIANTS)
            elif samsung_source == "samsung_vs":
                src_values = {"samsung_vs", "samsung_vijay_sales"}
            elif samsung_source in SAMSUNG_PARTNER_SOURCES:
                src_values = {samsung_source}
            elif src in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
                src_values = {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}
            elif src in {"godrej", "goodrej", "goddrej"}:
                src_values = {"godrej", "goodrej", "goddrej"}
            else:
                src_values = {src}
        ds = (dataset_type or "").strip().lower() if dataset_type is not None else None
        jb = (job_id or "").strip() if job_id is not None else None

        keys_to_delete = []
        for key in _df_cache.keys():
            key_source, key_dataset, key_job = key
            if src_values is not None and key_source not in src_values:
                continue
            if ds is not None and key_dataset != ds:
                continue
            if jb is not None and key_job != jb:
                continue
            keys_to_delete.append(key)

        for key in keys_to_delete:
            _df_cache.pop(key, None)
    return None


def _extract_data_payload(row) -> dict | None:
    # Row is a tuple from raw SQL: (data, )
    if not row:
        return None
    
    data = row[0]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None

    return data if isinstance(data, dict) else None


def get_data_rows(
    db: Session,
    job_id: str,
    source: str,
    dataset_type: str,
) -> list[dict]:
    """
    Fetch raw rows from data_rows table and return JSON payloads.
    """
    # Use raw SQL for speed here too if needed, but this is less critical than get_dataframe
    source_values = _source_variants(source)
    query = (
        db.query(DataRow.data)
        .filter(
            DataRow.job_id == job_id,
            DataRow.dataset_type == dataset_type,
        )
    )
    if len(source_values) == 1:
        query = query.filter(DataRow.source == source_values[0])
    else:
        query = query.filter(DataRow.source.in_(source_values))
    rows = query.all()

    out = []
    for row in rows:
        payload = _extract_data_payload(row)
        if payload is not None:
            out.append(payload)
    return out


def get_dataframe(
    db: Session,
    job_id: str | None,
    source: str,
    dataset_type: str,
):
    """
    Fetch rows from data_rows using RAW SQL and flatten JSONB `data` into a DataFrame.
    """
    key = _cache_key(source, dataset_type, job_id)
    now = time.time()
    with _df_cache_lock:
        cached = _df_cache.get(key)
        if cached is not None:
            expires_at, cached_df = cached
            if expires_at >= now:
                return cached_df.copy(deep=False)
            _df_cache.pop(key, None)

    # RAW SQL QUERY for performance (bypasses ORM overhead)
    # We select only the 'data' column.
    source_values = _source_variants(source)
    source_placeholders = ", ".join([f":source_{idx}" for idx in range(len(source_values))])
    stmt = f"SELECT data FROM data_rows WHERE source IN ({source_placeholders}) AND dataset_type = :dataset_type"
    params = {"dataset_type": dataset_type}
    for idx, value in enumerate(source_values):
        params[f"source_{idx}"] = value
    
    if job_id:
        stmt += " AND job_id = :job_id"
        params["job_id"] = job_id
        
    try:
        # Execute raw SQL
        result = db.execute(text(stmt), params)
        rows = result.fetchall()
        
    except Exception as e:
        # Fallback or error handling
        print(f"DB Error in get_dataframe: {e}")
        rows = []

    if not rows:
        df = pd.DataFrame()
        with _df_cache_lock:
            _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
        return df

    # Optimize payload extraction
    # rows is list of tuples: [({'col': val},), ({'col': val},), ...]
    # We need list of dicts:  [{'col': val}, {'col': val}, ...]
    
    # Fast path: assuming data is already dict (SQLAlchemy + psycopg2 usually adapts JSONB to dict automatically)
    try:
        payloads = [r[0] for r in rows if r[0] is not None]
    except Exception:
        # Fallback slow path if data needs parsing
        payloads = []
        for row in rows:
            p = _extract_data_payload(row)
            if p:
                payloads.append(p)

    if not payloads:
        df = pd.DataFrame()
        with _df_cache_lock:
            _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
        return df

    # Create DataFrame directly from list of dicts
    df = pd.DataFrame.from_records(payloads)
    
    with _df_cache_lock:
        _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
    return df.copy(deep=False)
