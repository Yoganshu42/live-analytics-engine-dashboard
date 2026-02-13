import logging
import asyncio
import json
import os
import re
import hashlib
import time
import math
from pathlib import Path
from typing import Any
from io import BytesIO
from datetime import datetime, date
from urllib.error import URLError
from urllib.request import Request as UrlRequest, urlopen

from fastapi import (
    FastAPI,
    Depends,
    UploadFile,
    File,
    Form,
    HTTPException,
    Request,
    Response,
)
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from db.session import engine
from db.base import Base
from db.deps import get_db

from models.data_rows import DataRow
from models.manual_updates import ManualUpdateMarker
from authentication import models as auth_models
from authentication.deps import get_current_user
from authentication.router import router as auth_router
from services.manual_update_service import mark_manual_update

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
@app.on_event("startup")
def _init_db():
    try:
        Base.metadata.create_all(bind=engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_data_rows_source_dataset
                    ON public.data_rows (source, dataset_type)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_data_rows_source_dataset_job
                    ON public.data_rows (source, dataset_type, job_id)
                    """
                )
            )
    except Exception:
        logger.exception("DB init failed")

# --------------------------------------------------
#  CORS  FIXED (DEV SAFE)
# --------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          #  FIX
    allow_credentials=False,      #  FIX
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------
# CORS PREFLIGHT (EXPLICIT)
# --------------------------------------------------
@app.options("/{path:path}")
def preflight(path: str, request: Request):
    return Response(status_code=204)


# --------------------------------------------------
# ROUTERS
# --------------------------------------------------
from routers.analytics import router as analytics_router
from routers.admin_files import router as admin_files_router
from services.analytics_repository import invalidate_dataframe_cache
app.include_router(auth_router)
app.include_router(analytics_router, dependencies=[Depends(get_current_user)])
app.include_router(admin_files_router)

# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def root():
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
    mark_manual_update(
        db=db,
        source=source.lower().strip(),
        dataset_type=dataset_type.lower().strip(),
        job_id=job_id,
    )
    db.commit()
    invalidate_dataframe_cache(
        source=source.lower().strip(),
        dataset_type=dataset_type.lower().strip(),
        job_id=job_id,
    )

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
    mark_manual_update(
        db=db,
        source=payload.source.lower().strip(),
        dataset_type=payload.dataset_type.lower().strip(),
        job_id=payload.job_id,
    )
    db.commit()
    invalidate_dataframe_cache(
        source=payload.source.lower().strip(),
        dataset_type=payload.dataset_type.lower().strip(),
        job_id=payload.job_id,
    )
    return {"rows_inserted": len(rows)}

# ==================================================
# GRAPH INSIGHTS (LLM)
# ==================================================
class GraphInsightPayload(BaseModel):
    source: str = Field(..., min_length=1)
    dataset_type: str = Field(..., min_length=1)
    dimension: str = Field(..., min_length=1)
    metric: str = Field(..., min_length=1)
    bucket: str | None = None
    job_id: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    compare_mode: bool = False
    rows: list[dict[str, Any]] = Field(default_factory=list)

GRAPH_INSIGHTS_TTL_SECONDS = int(os.getenv("GRAPH_INSIGHTS_TTL_SECONDS", "300"))
_graph_insights_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _graph_insights_cache_key(payload: GraphInsightPayload) -> str:
    signature = {
        "source": payload.source,
        "dataset_type": payload.dataset_type,
        "dimension": payload.dimension,
        "metric": payload.metric,
        "bucket": payload.bucket,
        "job_id": payload.job_id,
        "from_date": payload.from_date,
        "to_date": payload.to_date,
        "compare_mode": payload.compare_mode,
        "rows": payload.rows[:80],
    }
    raw = json.dumps(signature, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_chatcards_system_prompt() -> str:
    modelfile_path = Path(__file__).resolve().parent.parent / "chatcards" / "Modelfile"
    fallback = (
        "You are a business insights assistant. Generate concise, factual bullet insights "
        "from chart data. Keep output to 3-5 bullets."
    )
    if not modelfile_path.exists():
        return fallback

    try:
        content = modelfile_path.read_text(encoding="utf-8")
    except Exception:
        return fallback

    match = re.search(r'SYSTEM\s+"""(.*?)"""', content, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return fallback

    system = match.group(1).strip()
    return system or fallback


def _extract_bullets(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    bullets: list[str] = []

    for line in lines:
        if re.match(r"^[-*\u2022]\s+", line):
            bullets.append(re.sub(r"^[-*\u2022]\s+", "", line).strip())
            continue
        if re.match(r"^\d+[.)]\s+", line):
            bullets.append(re.sub(r"^\d+[.)]\s+", "", line).strip())
            continue

    if bullets:
        return bullets[:5]

    compact = text.strip()
    if not compact:
        return []

    sentences = re.split(r"(?<=[.!?])\s+", compact)
    return [s.strip() for s in sentences if s.strip()][:5]


def _to_safe_key(key: str) -> str:
    return re.sub(r"[()%'.]", "", re.sub(r"\s+", "_", key.strip().lower()))


def _pretty_label(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def _format_metric_value(metric_key: str, value: float) -> str:
    mk = metric_key.lower()
    if "loss_ratio" in mk:
        return f"{value:.2f}%"
    if "quantity" in mk or "count" in mk:
        return f"{value:,.0f}"
    if abs(value) >= 1e7:
        return f"Rs {value / 1e7:.2f} Cr"
    if abs(value) >= 1e5:
        return f"Rs {value / 1e5:.2f} L"
    if abs(value) >= 1e3:
        return f"Rs {value / 1e3:.1f} K"
    return f"Rs {value:,.2f}"


def _dedupe_insights(lines: list[str], limit: int = 5) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in lines:
        line = re.sub(r"\s+", " ", raw).strip(" -\t")
        if not line:
            continue
        norm = line.lower()
        if norm in seen:
            continue
        seen.add(norm)
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _is_low_signal_line(line: str) -> bool:
    low = line.lower()
    banned = (
        "as an ai",
        "i cannot",
        "i can't",
        "insufficient data",
        "not enough data",
        "unable to",
        "i do not have",
    )
    return any(token in low for token in banned)


def _derive_data_driven_insights(payload: GraphInsightPayload) -> list[str]:
    rows = payload.rows[:80]
    if not rows:
        return []

    dim_key = _to_safe_key(payload.dimension)
    dim_candidates = [dim_key, payload.dimension]
    dimension_key = next((k for k in dim_candidates if any(k in r for r in rows)), None)
    if not dimension_key:
        dimension_key = next(iter(rows[0].keys()), payload.dimension)

    if payload.compare_mode:
        numeric_keys: dict[str, int] = {}
        for row in rows:
            for key, value in row.items():
                if key == dimension_key:
                    continue
                if _to_number(value) is not None:
                    numeric_keys[key] = numeric_keys.get(key, 0) + 1
        if not numeric_keys:
            return []
        ordered = sorted(numeric_keys, key=lambda k: numeric_keys[k], reverse=True)
        series_a = ordered[0]
        series_b = ordered[1] if len(ordered) > 1 else None

        valid = []
        for row in rows:
            va = _to_number(row.get(series_a))
            vb = _to_number(row.get(series_b)) if series_b else None
            if va is None and vb is None:
                continue
            valid.append((row, va, vb))
        if not valid:
            return []

        insights: list[str] = []
        latest_row, latest_a, latest_b = valid[-1]
        latest_label = str(latest_row.get(dimension_key, "latest period"))
        if latest_a is not None:
            insights.append(
                f"In {latest_label}, {_pretty_label(series_a)} is {_format_metric_value(payload.metric, latest_a)}."
            )
        if series_b and latest_b is not None:
            insights.append(
                f"In {latest_label}, {_pretty_label(series_b)} is {_format_metric_value(payload.metric, latest_b)}."
            )
        if latest_a is not None and latest_b is not None:
            leader = series_a if latest_a >= latest_b else series_b
            gap = abs(latest_a - latest_b)
            insights.append(
                f"{_pretty_label(leader)} leads by {_format_metric_value(payload.metric, gap)} in the latest period."
            )
        return _dedupe_insights(insights)

    metric_key = _to_safe_key(payload.metric)
    metric_candidates = [metric_key, payload.metric]
    actual_metric_key = next((k for k in metric_candidates if any(k in r for r in rows)), None)
    if not actual_metric_key:
        return []

    points: list[tuple[str, float]] = []
    for row in rows:
        value = _to_number(row.get(actual_metric_key))
        if value is None:
            continue
        label = str(row.get(dimension_key, "Unknown"))
        points.append((label, value))

    if not points:
        return []

    insights = []
    first_label, first_value = points[0]
    last_label, last_value = points[-1]
    peak_label, peak_value = max(points, key=lambda x: x[1])
    low_label, low_value = min(points, key=lambda x: x[1])
    metric_name = _pretty_label(actual_metric_key)

    insights.append(f"Latest {metric_name} is {_format_metric_value(actual_metric_key, last_value)} in {last_label}.")
    if len(points) > 1:
        delta = last_value - first_value
        direction = "increased" if delta >= 0 else "decreased"
        pct = (abs(delta) / abs(first_value) * 100.0) if first_value else None
        if pct is None:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} from {first_label} to {last_label}."
            )
        else:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} ({pct:.1f}%) from {first_label} to {last_label}."
            )

    insights.append(f"Peak {metric_name} is {_format_metric_value(actual_metric_key, peak_value)} in {peak_label}.")
    insights.append(f"Lowest {metric_name} is {_format_metric_value(actual_metric_key, low_value)} in {low_label}.")

    total = sum(v for _, v in points if v > 0)
    if total > 0:
        top3 = sorted(points, key=lambda x: x[1], reverse=True)[:3]
        share = sum(v for _, v in top3) / total * 100.0
        insights.append(f"Top 3 categories contribute {share:.1f}% of total {metric_name}.")

    return _dedupe_insights(insights)

def _build_insight_prompt(payload: GraphInsightPayload) -> str:
    rows = payload.rows[:80]
    serialized_rows = json.dumps(rows, ensure_ascii=True, default=str)
    return (
        "Generate concise business insights for the graph below.\n"
        "Return only bullet points.\n"
        f"Source: {payload.source}\n"
        f"Dataset Type: {payload.dataset_type}\n"
        f"Dimension: {payload.dimension}\n"
        f"Metric: {payload.metric}\n"
        f"Bucket: {payload.bucket or 'none'}\n"
        f"Compare Mode: {'yes' if payload.compare_mode else 'no'}\n"
        f"From Date: {payload.from_date or 'n/a'}\n"
        f"To Date: {payload.to_date or 'n/a'}\n"
        "Data rows (JSON):\n"
        f"{serialized_rows}\n"
        "Constraints:\n"
        "- 3 to 5 bullets\n"
        "- Keep each bullet short and specific\n"
        "- Mention trend direction when possible\n"
    )


def _call_ollama(system_prompt: str, prompt: str) -> tuple[str, str]:
    model = os.getenv("CHATCARDS_MODEL", "chatcards")
    ollama_url = os.getenv("OLLAMA_API_URL", "http://127.0.0.1:11434/api/generate")
    timeout_seconds = int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "20"))

    body = {
        "model": model,
        "system": system_prompt,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.2,
            "num_predict": 220,
        },
    }
    raw = json.dumps(body).encode("utf-8")
    req = UrlRequest(
        ollama_url,
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urlopen(req, timeout=timeout_seconds) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
        response_text = (payload.get("response") or "").strip()
        if not response_text:
            raise ValueError("Empty LLM response.")
        return model, response_text


@app.post("/insights/graph")
def generate_graph_insights(
    payload: GraphInsightPayload,
):
    insights_enabled = os.getenv("ENABLE_GRAPH_INSIGHTS", "").strip().lower() in {"1", "true", "yes", "on"}
    if not insights_enabled:
        return {
            "insights": [],
            "model": "disabled",
            "message": "Graph insights are disabled in this environment.",
        }

    if not payload.rows:
        return {"insights": [], "model": "none", "message": "No graph rows available."}

    cache_key = _graph_insights_cache_key(payload)
    now = time.time()
    if len(_graph_insights_cache) > 256:
        expired_keys = [k for k, (expiry, _) in _graph_insights_cache.items() if expiry <= now]
        for k in expired_keys:
            _graph_insights_cache.pop(k, None)

    cached = _graph_insights_cache.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    system_prompt = _read_chatcards_system_prompt()
    prompt = _build_insight_prompt(payload)
    base_insights = _derive_data_driven_insights(payload)

    try:
        model, response_text = _call_ollama(system_prompt, prompt)
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        logger.warning("Graph insights generation failed: %s", exc)
        if base_insights:
            response_payload = {
                "insights": base_insights[:5],
                "model": "rule-based",
                "message": "LLM insights unavailable; showing data-driven insights.",
            }
            _graph_insights_cache[cache_key] = (now + GRAPH_INSIGHTS_TTL_SECONDS, response_payload)
            return response_payload
        raise HTTPException(
            status_code=503,
            detail=(
                "Insights service unavailable. Ensure Ollama is running and the "
                "'chatcards' model is created from chatcards/Modelfile."
            ),
        )

    llm_insights = [line for line in _extract_bullets(response_text) if not _is_low_signal_line(line)]
    merged_insights = _dedupe_insights(base_insights + llm_insights, limit=5)
    insights = merged_insights or base_insights
    if not insights:
        trimmed = response_text[:260].strip()
        insights = [trimmed] if trimmed else []

    response_payload = {"insights": insights[:5], "model": model}
    _graph_insights_cache[cache_key] = (now + GRAPH_INSIGHTS_TTL_SECONDS, response_payload)
    return response_payload

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

