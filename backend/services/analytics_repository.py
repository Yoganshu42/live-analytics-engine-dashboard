import pandas as pd
from sqlalchemy.orm import Session
from models.data_rows import DataRow


def get_data_rows(
    db: Session,
    job_id: str,
    source: str,
    dataset_type: str,
) -> list[dict]:
    """
    Fetch raw rows from data_rows table and return JSON payloads
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

    # rows = [(data,), (data,), ...] → unwrap
    return [r[0] for r in rows]

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

    # 🔑 CRITICAL: flatten JSONB correctly
    df = pd.DataFrame([r.data for r in rows])
    print(df.columns.tolist())
    return df
# rows = get_data_rows(db, job_id, "samsung", "sales")
# print(len(rows), rows[0].keys())
