import logging
import asyncio
from typing import Any
from io import BytesIO
from datetime import datetime, date

from fastapi import (
    FastAPI,
    Depends,
    UploadFile,
    File,
    Form,
    HTTPException,
)
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from db.session import engine
from db.base import Base
from db.deps import get_db

from models.data_rows import DataRow
from authentication import models as auth_models
from authentication.deps import get_current_user
from authentication.router import router as auth_router

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _json_safe(value: Any):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        if value != value or value == float("inf") or value == float("-inf"):
            return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

def _clean_json_row(row: dict) -> dict:
    return {k: _json_safe(v) for k, v in row.items()}


# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI(
    title="Live Dashboard API",
    version="1.0.0",
    swagger_ui_parameters={
        "persistAuthorization": True,
        "displayRequestDuration": True,
    },
)

# --------------------------------------------------
# DB INIT
# --------------------------------------------------
Base.metadata.create_all(bind=engine)

# --------------------------------------------------
# ✅ CORS — FIXED (DEV SAFE)
# --------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # 🔥 FIX
    allow_credentials=False,      # 🔥 FIX
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------
# ROUTERS
# --------------------------------------------------
from routers.analytics import router as analytics_router
app.include_router(auth_router)
app.include_router(analytics_router, dependencies=[Depends(get_current_user)])

# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# ==================================================
# UPLOAD (CSV/XLSX)
# ==================================================
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source: str | None = Form(None),
    dataset_type: str | None = Form(None),
    job_id: str | None = Form(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    if not source or not dataset_type:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: source and dataset_type.",
        )

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file.")

    name = (file.filename or "").lower()
    buf = BytesIO(contents)

    import pandas as pd
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(buf)
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            df = pd.read_excel(buf)
        else:
            raise HTTPException(status_code=400, detail="Only .csv or .xlsx files are supported.")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")

    df = df.astype(object).where(pd.notnull(df), None)
    rows = [_clean_json_row(r) for r in df.to_dict(orient="records")]

    db.add_all(
        [
            DataRow(
                job_id=job_id,
                source=source.lower().strip(),
                dataset_type=dataset_type.lower().strip(),
                data=row,
            )
            for row in rows
        ]
    )
    db.commit()

    logger.info(
        "UPLOAD: source=%s dataset=%s rows=%s",
        source,
        dataset_type,
        len(rows),
    )

    return {"rows_inserted": len(rows), "source": source, "dataset_type": dataset_type}

# ==================================================
# INGEST (JSON)
# ==================================================
class IngestPayload(BaseModel):
    source: str = Field(..., min_length=1)
    dataset_type: str = Field(..., min_length=1)
    job_id: str | None = None
    rows: list[dict[str, Any]] = Field(..., min_items=1)


@app.post("/ingest")
def ingest_rows(
    payload: IngestPayload,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    rows = [
        DataRow(
            job_id=payload.job_id,
            source=payload.source.lower().strip(),
            dataset_type=payload.dataset_type.lower().strip(),
            data=row,
        )
        for row in payload.rows
    ]
    db.add_all(rows)
    db.commit()
    return {"rows_inserted": len(rows)}

# ==================================================
# PROCESS DISABLED
# ==================================================
@app.post("/process")
def process_disabled():
    return {
        "status": "disabled",
        "reason": "Use /analytics/by-dimension directly",
    }

# ==================================================
# EVENTS (SSE)
# ==================================================

@app.get("/events")
async def events():
    async def event_stream():
        while True:
            await asyncio.sleep(30)
            yield "data: ping"



    return StreamingResponse(event_stream(), media_type="text/event-stream")
