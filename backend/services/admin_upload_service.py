from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from models.data_rows import DataRow


def _clean_token(value: str | None) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "dataset"


def job_key(job_id: str | None) -> str:
    return str(job_id or "").strip()


def _job_scope_exists(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str,
) -> bool:
    scope_count = (
        db.query(func.count(DataRow.id))
        .filter(
            DataRow.source == source,
            DataRow.dataset_type == dataset_type,
            DataRow.job_id == job_id,
        )
        .scalar()
    )
    if int(scope_count or 0) > 0:
        return True

    try:
        batch_match = db.execute(
            text(
                """
                SELECT 1
                FROM public.admin_upload_batches
                WHERE source = :source
                  AND dataset_type = :dataset_type
                  AND job_key = :job_key
                LIMIT 1
                """
            ),
            {
                "source": source,
                "dataset_type": dataset_type,
                "job_key": job_id,
            },
        ).first()
        return batch_match is not None
    except Exception:
        return False


def generate_auto_job_id(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    uploaded_at: datetime | None = None,
    legacy: bool = False,
) -> str:
    stamp = (uploaded_at or datetime.now(timezone.utc)).strftime("%Y%m%d_%H%M%S")
    source_key = _clean_token(source)
    dataset_key = _clean_token(dataset_type)
    legacy_marker = "_legacy" if legacy else ""
    base = f"{source_key}_{dataset_key}{legacy_marker}_{stamp}"
    candidate = base
    suffix = 2
    while _job_scope_exists(
        db,
        source=source,
        dataset_type=dataset_type,
        job_id=candidate,
    ):
        candidate = f"{base}_{suffix:02d}"
        suffix += 1
    return candidate


def resolve_job_id(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    requested_job_id: str | None,
    uploaded_at: datetime | None = None,
) -> tuple[str, bool]:
    requested = job_key(requested_job_id)
    if requested:
        return requested, False
    generated = generate_auto_job_id(
        db,
        source=source,
        dataset_type=dataset_type,
        uploaded_at=uploaded_at,
        legacy=False,
    )
    return generated, True


def record_upload_batch(
    db: Session,
    *,
    source: str,
    dataset_type: str,
    job_id: str | None,
    action: str,
    uploaded_by: str | None,
    uploaded_at: datetime | None = None,
    file_name: str | None = None,
    rows_in: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    deleted_rows: int = 0,
    notes: str | None = None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO public.admin_upload_batches (
                source,
                dataset_type,
                job_key,
                action,
                file_name,
                uploaded_by,
                uploaded_at,
                rows_in,
                rows_inserted,
                rows_updated,
                deleted_rows,
                notes
            )
            VALUES (
                :source,
                :dataset_type,
                :job_key,
                :action,
                :file_name,
                :uploaded_by,
                :uploaded_at,
                :rows_in,
                :rows_inserted,
                :rows_updated,
                :deleted_rows,
                :notes
            )
            """
        ),
        {
            "source": source,
            "dataset_type": dataset_type,
            "job_key": job_key(job_id),
            "action": str(action or "").strip().lower() or "update",
            "file_name": str(file_name or "").strip(),
            "uploaded_by": str(uploaded_by or "").strip(),
            "uploaded_at": uploaded_at or datetime.now(timezone.utc),
            "rows_in": int(rows_in or 0),
            "rows_inserted": int(rows_inserted or 0),
            "rows_updated": int(rows_updated or 0),
            "deleted_rows": int(deleted_rows or 0),
            "notes": str(notes or "").strip(),
        },
    )


def get_latest_upload_batch_map(
    db: Session,
    *,
    source: str | None = None,
    dataset_type: str | None = None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    source,
                    dataset_type,
                    job_key,
                    action,
                    file_name,
                    uploaded_by,
                    uploaded_at,
                    rows_in,
                    rows_inserted,
                    rows_updated,
                    deleted_rows,
                    notes
                FROM (
                    SELECT
                        source,
                        dataset_type,
                        job_key,
                        action,
                        file_name,
                        uploaded_by,
                        uploaded_at,
                        rows_in,
                        rows_inserted,
                        rows_updated,
                        deleted_rows,
                        notes,
                        ROW_NUMBER() OVER (
                            PARTITION BY source, dataset_type, job_key
                            ORDER BY uploaded_at DESC, id DESC
                        ) AS rn
                    FROM public.admin_upload_batches
                    WHERE (:source IS NULL OR source = :source)
                      AND (:dataset_type IS NULL OR dataset_type = :dataset_type)
                ) ranked
                WHERE rn = 1
                """
            ),
            {
                "source": source,
                "dataset_type": dataset_type,
            },
        ).mappings().all()
    except Exception:
        return {}

    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        uploaded_at = row.get("uploaded_at")
        out[(str(row["source"]), str(row["dataset_type"]), str(row["job_key"] or ""))] = {
            "action": row.get("action"),
            "file_name": row.get("file_name"),
            "uploaded_by": row.get("uploaded_by"),
            "uploaded_at": uploaded_at.isoformat() if uploaded_at is not None else None,
            "rows_in": int(row.get("rows_in") or 0),
            "rows_inserted": int(row.get("rows_inserted") or 0),
            "rows_updated": int(row.get("rows_updated") or 0),
            "deleted_rows": int(row.get("deleted_rows") or 0),
            "notes": row.get("notes"),
        }

    try:
        legacy_rows = db.execute(
            text(
                """
                SELECT
                    source,
                    dataset_type,
                    job_key,
                    updated_at
                FROM public.manual_update_markers
                WHERE (:source IS NULL OR source = :source)
                  AND (:dataset_type IS NULL OR dataset_type = :dataset_type)
                """
            ),
            {
                "source": source,
                "dataset_type": dataset_type,
            },
        ).mappings().all()
    except Exception:
        legacy_rows = []

    for row in legacy_rows:
        key = (str(row["source"]), str(row["dataset_type"]), str(row["job_key"] or ""))
        if key in out:
            continue
        updated_at = row.get("updated_at")
        out[key] = {
            "action": "legacy_marker",
            "file_name": "",
            "uploaded_by": "legacy-unknown",
            "uploaded_at": updated_at.isoformat() if updated_at is not None else None,
            "rows_in": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "deleted_rows": 0,
            "notes": "Historical upload metadata predates uploader tracking.",
        }
    return out


def backfill_missing_job_ids(db: Session) -> dict[str, Any]:
    groups = db.execute(
        text(
            """
            SELECT source, dataset_type, COUNT(*) AS rows
            FROM public.data_rows
            WHERE job_id IS NULL
            GROUP BY source, dataset_type
            ORDER BY source, dataset_type
            """
        )
    ).mappings().all()

    if not groups:
        return {"groups_backfilled": 0, "rows_backfilled": 0, "items": []}

    uploaded_at = datetime.now(timezone.utc)
    rows_backfilled = 0
    items: list[dict[str, Any]] = []

    for group in groups:
        source = str(group["source"] or "").strip().lower()
        dataset_type = str(group["dataset_type"] or "").strip().lower()
        row_count = int(group.get("rows") or 0)
        if not source or not dataset_type or row_count <= 0:
            continue

        generated_job_id = generate_auto_job_id(
            db,
            source=source,
            dataset_type=dataset_type,
            uploaded_at=uploaded_at,
            legacy=True,
        )
        result = db.execute(
            text(
                """
                UPDATE public.data_rows
                SET job_id = :job_id
                WHERE source = :source
                  AND dataset_type = :dataset_type
                  AND job_id IS NULL
                """
            ),
            {
                "job_id": generated_job_id,
                "source": source,
                "dataset_type": dataset_type,
            },
        )
        updated_rows = int(result.rowcount or 0)
        rows_backfilled += updated_rows
        record_upload_batch(
            db,
            source=source,
            dataset_type=dataset_type,
            job_id=generated_job_id,
            action="job_id_backfill",
            uploaded_by="system-backfill",
            uploaded_at=uploaded_at,
            file_name="",
            rows_in=updated_rows,
            rows_inserted=updated_rows,
            rows_updated=0,
            deleted_rows=0,
            notes="Legacy untagged rows were assigned an auto-generated job_id.",
        )
        items.append(
            {
                "source": source,
                "dataset_type": dataset_type,
                "job_id": generated_job_id,
                "rows_backfilled": updated_rows,
            }
        )

    return {
        "groups_backfilled": len(items),
        "rows_backfilled": rows_backfilled,
        "items": items,
    }
