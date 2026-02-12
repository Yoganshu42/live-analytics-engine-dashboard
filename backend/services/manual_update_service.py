from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from models.manual_updates import ManualUpdateMarker


def _job_key(job_id: str | None) -> str:
    if job_id is None:
        return ""
    cleaned = job_id.strip()
    return cleaned


def mark_manual_update(
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
) -> None:
    source_key = (source or "").strip().lower()
    dataset_key = (dataset_type or "").strip().lower()
    key = _job_key(job_id)
    if not source_key or not dataset_key:
        return

    marker = (
        db.query(ManualUpdateMarker)
        .filter(ManualUpdateMarker.source == source_key)
        .filter(ManualUpdateMarker.dataset_type == dataset_key)
        .filter(ManualUpdateMarker.job_key == key)
        .first()
    )

    if marker is None:
        marker = ManualUpdateMarker(source=source_key, dataset_type=dataset_key, job_key=key)
        db.add(marker)
    else:
        marker.updated_at = func.now()

    db.flush()
