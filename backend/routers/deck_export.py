from __future__ import annotations

from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from authentication.deps import get_current_user
from db.deps import get_db
from services.deck_cache_service import get_or_generate_cached_partner_deck_pptx
from services.deck_pptx_service import (
    build_partner_deck_preview,
)

router = APIRouter(
    prefix="/deck",
    tags=["deck"],
    dependencies=[Depends(get_current_user)],
)


def _parse_partners(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [value.strip() for value in str(raw).split(",") if value.strip()]


@router.get("/preview")
def preview_partner_deck(
    partners: str | None = Query(None, description="Comma separated partner keys."),
    dataset_type: str = Query("sales"),
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    week_window: int = Query(4, description="Allowed values: 2, 3, 4, 6"),
    db: Session = Depends(get_db),
):
    try:
        items = build_partner_deck_preview(
            db=db,
            partners=_parse_partners(partners),
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            week_window=week_window,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to generate deck preview: {exc}")

    return {
        "items": items,
        "dataset_type": str(dataset_type).strip().lower(),
        "week_window": week_window,
    }


@router.get("/download-pptx")
def download_partner_deck_pptx(
    partners: str | None = Query(None, description="Comma separated partner keys."),
    dataset_type: str = Query("sales"),
    job_id: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    include_tables: bool = Query(True),
    week_window: int = Query(4, description="Allowed values: 2, 3, 4, 6"),
    db: Session = Depends(get_db),
):
    try:
        pptx_bytes, filename = get_or_generate_cached_partner_deck_pptx(
            db=db,
            partners=_parse_partners(partners),
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
            include_tables=include_tables,
            week_window=week_window,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to generate deck: {exc}")

    return StreamingResponse(
        BytesIO(pptx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
