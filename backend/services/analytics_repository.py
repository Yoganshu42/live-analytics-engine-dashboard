import json

import pandas as pd
from sqlalchemy.orm import Session
from models.data_rows import DataRow

def invalidate_dataframe_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    # Cache removed intentionally; kept for compatibility with callers.
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
        return pd.DataFrame()

    payloads = []
    for row in rows:
        payload = _extract_data_payload(row)
        if payload is not None:
            payloads.append(payload)

    if not payloads:
        return pd.DataFrame()

    return pd.DataFrame(payloads)
