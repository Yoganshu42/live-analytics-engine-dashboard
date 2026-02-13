import json
import threading
import time

import pandas as pd
from sqlalchemy.orm import Session
from models.data_rows import DataRow

_CACHE_TTL_SECONDS = 300
_df_cache_lock = threading.Lock()
_df_cache: dict[tuple[str, str, str], tuple[float, pd.DataFrame]] = {}


def _cache_key(source: str, dataset_type: str, job_id: str | None) -> tuple[str, str, str]:
    return (
        (source or "").strip().lower(),
        (dataset_type or "").strip().lower(),
        (job_id or "").strip(),
    )


def invalidate_dataframe_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    with _df_cache_lock:
        if source is None and dataset_type is None and job_id is None:
            _df_cache.clear()
            return None

        src = (source or "").strip().lower() if source is not None else None
        ds = (dataset_type or "").strip().lower() if dataset_type is not None else None
        jb = (job_id or "").strip() if job_id is not None else None

        keys_to_delete = []
        for key in _df_cache.keys():
            key_source, key_dataset, key_job = key
            if src is not None and key_source != src:
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
    data = getattr(row, "data", None)
    if data is None and isinstance(row, (tuple, list)) and row:
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
    rows = (
        db.query(DataRow.data)
        .filter(
            DataRow.job_id == job_id,
            DataRow.source == source,
            DataRow.dataset_type == dataset_type,
        )
        .all()
    )

    out = []
    for row in rows:
        payload = _extract_data_payload(row)
        if payload is not None:
            out.append(payload)
    return out


def get_dataframe(
    db,
    job_id: str | None,
    source: str,
    dataset_type: str,
):
    """
    Fetch rows from data_rows and flatten JSONB `data` into a DataFrame.
    job_id is kept for compatibility but not required for analytics.
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

    base_query = (
        db.query(DataRow.data)
        .filter(DataRow.source == source)
        .filter(DataRow.dataset_type == dataset_type)
    )
    query = base_query
    if job_id:
        query = query.filter(DataRow.job_id == job_id)

    rows = query.all()
    if job_id and not rows:
        rows = base_query.all()

    if not rows:
        df = pd.DataFrame()
        with _df_cache_lock:
            _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
        return df

    payloads = []
    for row in rows:
        payload = _extract_data_payload(row)
        if payload is not None:
            payloads.append(payload)

    if not payloads:
        df = pd.DataFrame()
        with _df_cache_lock:
            _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
        return df

    df = pd.DataFrame(payloads)
    with _df_cache_lock:
        _df_cache[key] = (now + _CACHE_TTL_SECONDS, df)
    return df.copy(deep=False)
