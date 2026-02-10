from sqlalchemy.orm import Session
from models.data_rows import DataRow


def get_rows(
    db: Session,
    source: str | None = None,
):
    query = db.query(DataRow)

    if source and source != "overall":
        query = query.filter(DataRow.source == source)

    return query.all()