import logging
import asyncio
import json
import os
import re
import hashlib
import time
import math
import threading
from pathlib import Path
from typing import Any
from io import BytesIO
from datetime import datetime, date, timedelta
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
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from db.session import engine
from db.base import Base
from db.deps import get_db

from models.data_rows import DataRow
from models.manual_updates import ManualUpdateMarker
from models.precomputed_analytics import PrecomputedGraph, PrecomputedInsight, PrecomputedSummary
from authentication import models as auth_models
from authentication.deps import get_current_user
from authentication.router import router as auth_router
from services.manual_update_service import mark_manual_update
from services.precompute_service import rebuild_precomputed_analytics
from services.precomputed_repository import (
    get_precomputed_graph,
    get_precomputed_summary,
    get_precomputed_insights,
    upsert_precomputed_insights,
)
from services.analytics_engine import filter_by_date_range

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


def _refresh_jobs(job_id: str | None) -> list[str | None]:
    normalized = (job_id or "").strip() or None
    jobs: list[str | None] = [normalized]
    if normalized is not None:
        jobs.append(None)
    return jobs


def _refresh_after_data_change(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    action: str,
) -> None:
    src = source.lower().strip()
    ds = dataset_type.lower().strip()
    refresh_jobs = _refresh_jobs(job_id)

    for refresh_job in refresh_jobs:
        mark_manual_update(
            db=db,
            source=src,
            dataset_type=ds,
            job_id=refresh_job,
        )
    db.commit()

    for refresh_job in refresh_jobs:
        invalidate_dataframe_cache(
            source=src,
            dataset_type=ds,
            job_id=refresh_job,
        )

    for refresh_job in refresh_jobs:
        try:
            rebuild_precomputed_analytics(
                db=db,
                source=src,
                dataset_type=ds,
                job_id=refresh_job,
            )
        except Exception:
            db.rollback()
            logger.exception(
                "Failed to rebuild precomputed analytics after %s source=%s dataset=%s job_id=%s",
                action,
                src,
                ds,
                refresh_job,
            )


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
            # Some DB restores (like analytics.sql) only include `data_rows` + `users`.
            # Create the manual update marker table explicitly so admin "Replace Tag"
            # and upload flows don't fail in production.
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.manual_update_markers (
                        id SERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        dataset_type TEXT NOT NULL,
                        job_key TEXT NOT NULL DEFAULT '',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_update_marker_tag
                    ON public.manual_update_markers (source, dataset_type, job_key);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_source
                    ON public.manual_update_markers (source);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_dataset
                    ON public.manual_update_markers (dataset_type);
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_manual_update_markers_job
                    ON public.manual_update_markers (job_key);
                    """
                )
            )
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

    prewarm_raw = os.getenv("LLM_PREWARM", "1").strip()
    prewarm_enabled = prewarm_raw.lower() not in {"0", "false", "no", "off"}
    chatbot_enabled = os.getenv("ENABLE_CHATBOT", "1").strip().lower() not in {"0", "false", "no", "off"}
    insights_enabled = os.getenv("ENABLE_GRAPH_INSIGHTS", "1").strip().lower() not in {"0", "false", "no", "off"}
    if prewarm_enabled and (chatbot_enabled or insights_enabled):
        try:
            threading.Thread(target=_prewarm_llm_model, name="llm-prewarm", daemon=True).start()
        except Exception:
            logger.exception("Failed to schedule LLM prewarm")

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
from routers.analytics import compute_by_dimension_rows, router as analytics_router
from routers.admin_files import router as admin_files_router
from services.analytics_repository import invalidate_dataframe_cache, get_dataframe
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
    _refresh_after_data_change(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        action="upload",
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
    _refresh_after_data_change(
        db=db,
        source=payload.source,
        dataset_type=payload.dataset_type,
        job_id=payload.job_id,
        action="ingest",
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


class ChatbotTurn(BaseModel):
    role: str = Field(..., min_length=1)
    content: str = Field(..., min_length=1, max_length=4000)


class ChatbotPayload(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[ChatbotTurn] = Field(default_factory=list)
    system_prompt: str | None = None
    temperature: float | None = Field(default=None, ge=0.0, le=1.5)
    max_tokens: int | None = Field(default=None, ge=8, le=4096)
    source: str | None = Field(default=None, max_length=64)
    dataset_type: str | None = Field(default=None, max_length=16)
    job_id: str | None = Field(default=None, max_length=128)
    from_date: str | None = Field(default=None, max_length=32)
    to_date: str | None = Field(default=None, max_length=32)


DEFAULT_LLM_MODEL = (
    os.getenv("SARVAM_MODEL", "").strip()
    or os.getenv("CHATBOT_MODEL", "").strip()
    or os.getenv("CHATCARDS_MODEL", "").strip()
    or "sarvam-m"
)
DEFAULT_CHATBOT_SYSTEM_PROMPT = (
    "You are AI Sahyogi, Senior Analytics Advisor for Zopper leadership reviews. "
    "Answer from analytics context built from dashboard metrics and underlying dataset signals. "
    "Do not invent brands, products, numbers, dates, or events. "
    "If key data is insufficient, explicitly state what is missing and provide the closest defensible estimate with assumptions. "
    "For greetings, acknowledgements, or short conversational messages, respond naturally and invite a data question. "
    "Treat source aliases as: reliance/resq -> Reliance ResQ, goodrej/goddrej -> Godrej, "
    "samsung/overview/overall/ -> Samsung Overview, samsung vs/vijay sales -> Samsung Vijay Sales, samsung croma/croma -> Samsung Croma. "
    "When Samsung model codes are mentioned (for example A06, S24, Fold7), map them to the provided Samsung device-plan category mapping in context. "
    "Use Samsung plan abbreviations consistently: ADLD = Accidental Damage and Liquid Damage, EW = Extended Warranty, SP/SPP = Screen Protection Plan, CPP = Comprehensive Protection Plan, Combo = ADLD + EW. "
    "Write in a clear executive tone with concise, evidence-backed reasoning. "
    "Lead with the direct answer, then support it with key metrics, trend direction, and business impact. "
    "Vary phrasing and structure across turns; avoid repeating identical templates or sentence openings. "
    "For forecasting questions, derive next-month directional estimates only from historical monthly values in context. "
    "Do not re-introduce yourself unless the user explicitly asks who you are."
)
try:
    CHATBOT_HISTORY_LIMIT = max(1, int(os.getenv("CHATBOT_HISTORY_LIMIT", "10")))
except ValueError:
    CHATBOT_HISTORY_LIMIT = 10

try:
    CHATBOT_HISTORY_CHAR_LIMIT = max(120, int(os.getenv("CHATBOT_HISTORY_CHAR_LIMIT", "650")))
except ValueError:
    CHATBOT_HISTORY_CHAR_LIMIT = 650

try:
    CHATBOT_MESSAGE_CHAR_LIMIT = max(200, int(os.getenv("CHATBOT_MESSAGE_CHAR_LIMIT", "2600")))
except ValueError:
    CHATBOT_MESSAGE_CHAR_LIMIT = 2600

try:
    CHATBOT_CACHE_TTL_SECONDS = max(1, int(os.getenv("CHATBOT_CACHE_TTL_SECONDS", "180")))
except ValueError:
    CHATBOT_CACHE_TTL_SECONDS = 180

try:
    CHATBOT_CACHE_MAX_ITEMS = max(8, int(os.getenv("CHATBOT_CACHE_MAX_ITEMS", "256")))
except ValueError:
    CHATBOT_CACHE_MAX_ITEMS = 256

GRAPH_INSIGHTS_TTL_SECONDS = int(os.getenv("GRAPH_INSIGHTS_TTL_SECONDS", "300"))
_graph_insights_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_chatbot_response_cache: dict[str, tuple[float, dict[str, str]]] = {}
_chatbot_cache_lock = threading.Lock()


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
    fallback = (
        "You are AI Sahyogi, a business analytics copilot for Zopper leadership reviews. "
        "Generate crisp, decision-ready insights from chart data. Use precise business "
        "language, quantify impact, and avoid filler. Return exactly 3 to 5 bullet points."
    )
    env_path = os.getenv("CHATCARDS_MODELFILE_PATH", "").strip()
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    base_dir = Path(__file__).resolve().parent
    candidates.extend(
        [
            base_dir / "chatcards" / "Modelfile",            # backend-local (works in Docker image)
            base_dir.parent / "chatcards" / "Modelfile",     # repo-root sibling (works in local dev)
        ]
    )

    for modelfile_path in candidates:
        if not modelfile_path.exists():
            continue
        try:
            content = modelfile_path.read_text(encoding="utf-8")
        except Exception:
            continue
        match = re.search(r'SYSTEM\s+"""(.*?)"""', content, flags=re.DOTALL | re.IGNORECASE)
        if not match:
            continue
        system = match.group(1).strip()
        if system:
            return system
    return fallback


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


def _normalize_source_key(source: str) -> str:
    source_key = (source or "").strip().lower()
    if source_key in {"samsung_vs", "samsung_vijay_sales", "samsung vs", "samsung vijay sales", "vijay sales"}:
        return "samsung_vs"
    if source_key in {"samsung_croma", "samsung croma", "croma"}:
        return "samsung_croma"
    if source_key in {"reliance resq", "reliance_resq", "reliance-resq", "resq"}:
        return "reliance"
    if source_key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return source_key


_CHATBOT_SOURCE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("reliance", ("reliance resq", "reliance-resq", "reliance_resq", "resq", "reliance")),
    ("godrej", ("godrej", "goodrej", "goddrej")),
    ("samsung_vs", ("samsung vijay sales", "samsung_vs", "samsung vs", "vijay sales", "vijay")),
    ("samsung_croma", ("samsung croma", "samsung_croma", "croma sales", "croma")),
    ("samsung", ("samsung",)),
]

_CHATBOT_SOURCE_LABELS: dict[str, str] = {
    "reliance": "Reliance ResQ",
    "godrej": "Godrej",
    "samsung": "Samsung",
    "samsung_vs": "Samsung Vijay Sales",
    "samsung_croma": "Samsung Croma",
}


def _source_display_name(source: str) -> str:
    source_key = _normalize_source_key(source)
    if source_key in _CHATBOT_SOURCE_LABELS:
        return _CHATBOT_SOURCE_LABELS[source_key]
    if source_key:
        return source_key.replace("_", " ").title()
    return "Dashboard"


def _normalize_dataset_type_for_chatbot(value: str | None) -> str:
    token = (value or "").strip().lower()
    return "claims" if token == "claims" else "sales"


def _normalize_chatbot_job_id(value: str | None) -> str | None:
    token = (value or "").strip()
    if not token:
        return None
    if token.lower() in {"all", "null", "undefined"}:
        return None
    return token


def _normalize_chatbot_date(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    candidate = raw[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _normalize_chatbot_date_range(
    from_date: str | None,
    to_date: str | None,
) -> tuple[str | None, str | None]:
    safe_from = _normalize_chatbot_date(from_date)
    safe_to = _normalize_chatbot_date(to_date)
    if safe_from and safe_to and safe_from > safe_to:
        return safe_to, safe_from
    return safe_from, safe_to


def _detect_source_from_text(text: str) -> str | None:
    low = (text or "").strip().lower()
    if not low:
        return None

    for source_key, aliases in _CHATBOT_SOURCE_PATTERNS:
        for alias in aliases:
            pattern = r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b"
            if re.search(pattern, low):
                return source_key
    return None


def _detect_dataset_from_text(text: str) -> str | None:
    low = (text or "").strip().lower()
    if not low:
        return None
    if any(token in low for token in ("claim", "loss ratio", "settlement", "paid out")):
        return "claims"
    if any(token in low for token in ("sale", "premium", "units sold", "earning")):
        return "sales"
    return None


def _resolve_chatbot_source(payload: ChatbotPayload) -> str:
    explicit = _normalize_source_key(payload.source or "")
    if explicit:
        return explicit

    from_message = _detect_source_from_text(payload.message)
    if from_message:
        return from_message

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        inferred = _detect_source_from_text(turn.content)
        if inferred:
            return inferred
    return ""


def _resolve_chatbot_dataset_type(payload: ChatbotPayload) -> str:
    explicit = (payload.dataset_type or "").strip().lower()
    if explicit in {"sales", "claims"}:
        return explicit

    inferred = _detect_dataset_from_text(payload.message)
    if inferred:
        return inferred

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        inferred = _detect_dataset_from_text(turn.content)
        if inferred:
            return inferred
    return "sales"


def _chatbot_message_tokens(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (text or "").strip().lower())
    return [token for token in cleaned.split() if token]


def _chatbot_requested_dimensions_from_text(text: str) -> list[str]:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return []

    hints: list[tuple[str, tuple[str, ...]]] = [
        (
            "state",
            (
                "statewise",
                "state wise",
                "by state",
                "state level",
                "state breakup",
                "state stats",
            ),
        ),
        (
            "month",
            (
                "monthwise",
                "month wise",
                "by month",
                "monthly",
                "which month",
                "month level",
                "month stats",
            ),
        ),
        ("city", ("citywise", "city wise", "by city", "city level", "city stats")),
        ("channel", ("channel wise", "channelwise", "by channel", "channel level", "channel stats")),
        (
            "device_plan_category",
            (
                "device plan category",
                "device category",
                "device wise",
                "device-wise",
            ),
        ),
        (
            "plan_category",
            (
                "plan category",
                "plan wise",
                "plan-wise",
            ),
        ),
        (
            "product_category",
            (
                "product category",
                "product wise",
                "product-wise",
            ),
        ),
        ("brand", ("brand wise", "brand-wise", "by brand", "brand level", "brand stats")),
    ]

    requested: list[str] = []
    for dimension, patterns in hints:
        if any(pattern in low for pattern in patterns):
            requested.append(dimension)

    # Keep single-word fallbacks last to avoid false positives.
    single_word_hints: list[tuple[str, str]] = [
        ("state", "state"),
        ("month", "month"),
        ("city", "city"),
        ("channel", "channel"),
        ("brand", "brand"),
    ]
    for dimension, token in single_word_hints:
        if token in low and dimension not in requested:
            requested.append(dimension)

    return requested


def _chatbot_requested_dimensions(payload: ChatbotPayload) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    text_candidates = [payload.message]
    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        text_candidates.append(turn.content)

    for text in text_candidates:
        for dimension in _chatbot_requested_dimensions_from_text(text):
            if dimension in seen:
                continue
            seen.add(dimension)
            ordered.append(dimension)
    return ordered


def _prioritize_dimensions(base_dimensions: list[str], requested_dimensions: list[str]) -> list[str]:
    ordered: list[str] = []
    for dimension in requested_dimensions + base_dimensions:
        if dimension and dimension not in ordered:
            ordered.append(dimension)
    return ordered


def _is_chatbot_greeting(text: str) -> bool:
    tokens = _chatbot_message_tokens(text)
    if not tokens:
        return False

    greeting_words = {
        "hi",
        "hii",
        "hello",
        "hey",
        "yo",
        "hola",
        "namaste",
        "thanks",
        "thank",
        "ok",
        "okay",
    }
    if len(tokens) <= 3 and all(token in greeting_words for token in tokens):
        return True

    joined = " ".join(tokens)
    greeting_phrases = (
        "good morning",
        "good afternoon",
        "good evening",
        "how are you",
        "who are you",
    )
    return any(phrase in joined for phrase in greeting_phrases)


def _requests_global_scope(text: str) -> bool:
    low = " ".join(_chatbot_message_tokens(text))
    if not low:
        return False
    scope_phrases = (
        "all sources",
        "across all sources",
        "across sources",
        "overall",
        "entire database",
        "full database",
        "all datasets",
        "across datasets",
        "look into those datasets",
        "actual datasets",
        "raw datasets",
        "all data",
        "complete database",
        "whole database",
    )
    return any(phrase in low for phrase in scope_phrases)


def _chatbot_available_scopes(
    *,
    db: Session,
    job_id: str | None,
) -> list[dict[str, Any]]:
    try:
        query = db.query(
            DataRow.source,
            DataRow.dataset_type,
            func.count(DataRow.id),
        )
        if job_id:
            query = query.filter(DataRow.job_id == job_id)
        rows = query.group_by(DataRow.source, DataRow.dataset_type).all()
    except Exception:
        logger.exception("Failed to load chatbot source coverage from data_rows.")
        return []

    grouped_counts: dict[tuple[str, str], int] = {}
    for source_raw, dataset_raw, row_count_raw in rows:
        source = _normalize_source_key(str(source_raw or ""))
        dataset_type = (str(dataset_raw or "").strip().lower())
        if not source or dataset_type not in {"sales", "claims"}:
            continue
        key = (source, dataset_type)
        grouped_counts[key] = grouped_counts.get(key, 0) + int(row_count_raw or 0)

    scopes = [
        {
            "source": source,
            "dataset_type": dataset_type,
            "row_count": row_count,
        }
        for (source, dataset_type), row_count in grouped_counts.items()
    ]
    scopes.sort(
        key=lambda item: (
            -int(item.get("row_count", 0) or 0),
            _source_display_name(str(item.get("source", ""))),
            str(item.get("dataset_type", "")),
        )
    )
    return scopes


def _sum_metric_from_dataframe(
    frame: Any,
    candidates: list[str],
) -> float:
    if frame is None or getattr(frame, "empty", True):
        return 0.0

    safe_to_columns: dict[str, list[str]] = {}
    try:
        columns = list(frame.columns)
    except Exception:
        columns = []
    for col in columns:
        safe_col = _to_safe_key(str(col))
        safe_to_columns.setdefault(safe_col, []).append(str(col))

    for candidate in candidates:
        for col in safe_to_columns.get(_to_safe_key(candidate), []):
            total = 0.0
            found_numeric = False
            try:
                values = frame[col].tolist()
            except Exception:
                values = []
            for raw in values:
                num = _to_number(raw)
                if num is None:
                    continue
                total += float(num)
                found_numeric = True
            if found_numeric:
                return total

    return 0.0


def _build_live_summary_for_scope(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    try:
        frame = get_dataframe(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
        )
    except Exception:
        logger.exception(
            "Chatbot live summary fetch failed source=%s dataset=%s job_id=%s",
            source,
            dataset_type,
            job_id,
        )
        return {}

    if frame is None or getattr(frame, "empty", True):
        return {}

    if from_date or to_date:
        try:
            frame = filter_by_date_range(
                frame,
                dataset_type,
                from_date,
                to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot live date filtering failed source=%s dataset=%s from=%s to=%s",
                source,
                dataset_type,
                from_date,
                to_date,
            )
            return {}

    if frame is None or getattr(frame, "empty", True):
        return {}

    row_count = 0
    try:
        row_count = int(len(frame.index))
    except Exception:
        row_count = 0

    if dataset_type == "claims":
        total_claims_cost = _sum_metric_from_dataframe(
            frame,
            [
                "claims",
                "net_amount",
                "claim_amount",
                "zoppers_cost",
                "gross_premium",
                "amount",
            ],
        )
        net_claims_cost = _sum_metric_from_dataframe(
            frame,
            [
                "net_claims",
                "net_claim",
                "net_amount",
                "earned_premium",
            ],
        )
        claims_count = _sum_metric_from_dataframe(
            frame,
            [
                "quantity",
                "units_sold",
                "claims_count",
                "count",
            ],
        )
        if claims_count <= 0:
            claims_count = float(row_count)
        if net_claims_cost <= 0 and total_claims_cost > 0:
            net_claims_cost = total_claims_cost
        return {
            "gross_premium": float(total_claims_cost),
            "earned_premium": float(net_claims_cost),
            "units_sold": float(claims_count),
        }

    gross_premium = _sum_metric_from_dataframe(
        frame,
        [
            "gross_premium",
            "amount",
            "plan_selling_price",
            "plan_price",
        ],
    )
    earned_premium = _sum_metric_from_dataframe(
        frame,
        [
            "earned_premium",
            "written_premium",
            "earnedpremium",
        ],
    )
    zopper_earned_premium = _sum_metric_from_dataframe(
        frame,
        [
            "zopper_earned_premium",
            "earned_zopper",
            "zopper_shared_transfer_price",
        ],
    )
    units_sold = _sum_metric_from_dataframe(
        frame,
        [
            "quantity",
            "units_sold",
            "units",
            "count",
        ],
    )

    if units_sold <= 0:
        units_sold = float(row_count)
    if earned_premium <= 0 and gross_premium > 0:
        earned_premium = gross_premium

    return {
        "gross_premium": float(gross_premium),
        "earned_premium": float(earned_premium),
        "zopper_earned_premium": float(zopper_earned_premium),
        "units_sold": float(units_sold),
    }


def _resolve_summary_for_scope(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    summary = get_precomputed_summary(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    if summary is None and (from_date or to_date):
        summary = get_precomputed_summary(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
        )
    if isinstance(summary, dict) and summary:
        return summary
    return _build_live_summary_for_scope(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )


def _pick_frame_column(frame: Any, candidates: list[str]) -> str | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        return None
    safe_to_raw: dict[str, str] = {}
    for col in columns:
        safe_to_raw[_to_safe_key(col)] = col
    for candidate in candidates:
        hit = safe_to_raw.get(_to_safe_key(candidate))
        if hit:
            return hit
    return None


def _build_dataset_field_profile(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    from_date: str | None,
    to_date: str | None,
) -> str | None:
    try:
        frame = get_dataframe(
            db=db,
            job_id=job_id,
            source=source,
            dataset_type=dataset_type,
        )
    except Exception:
        logger.exception(
            "Chatbot dataset profile fetch failed source=%s dataset=%s job_id=%s",
            source,
            dataset_type,
            job_id,
        )
        return None

    if frame is None or getattr(frame, "empty", True):
        return None

    if from_date or to_date:
        try:
            frame = filter_by_date_range(
                frame,
                dataset_type,
                from_date,
                to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot dataset profile date filtering failed source=%s dataset=%s from=%s to=%s",
                source,
                dataset_type,
                from_date,
                to_date,
            )
            return None

    if frame is None or getattr(frame, "empty", True):
        return None

    try:
        row_count = int(len(frame.index))
    except Exception:
        row_count = 0

    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        columns = []

    if not columns:
        return None

    col_preview = ", ".join(columns[:14])
    if len(columns) > 14:
        col_preview += ", ..."

    dim_candidates = [
        "month",
        "state",
        "city",
        "channel",
        "brand",
        "plan_category",
        "device_plan_category",
        "product_category",
    ]
    detected_dims = [
        dim
        for dim in dim_candidates
        if _pick_frame_column(
            frame,
            [
                dim,
                dim.replace("_", " "),
                dim.replace("_", "-"),
            ],
        )
    ]

    price_col = _pick_frame_column(
        frame,
        [
            "amount",
            "gross_premium",
            "plan_selling_price",
            "plan_price",
            "premium",
            "net_amount",
        ],
    )
    qty_col = _pick_frame_column(
        frame,
        [
            "quantity",
            "units_sold",
            "units",
            "count",
            "claims_count",
            "no_of_claims",
            "no_of_policies",
        ],
    )
    cost_or_margin_col = _pick_frame_column(
        frame,
        [
            "net_amount",
            "net_claims",
            "claims",
            "cost",
            "margin",
            "profit",
            "contribution",
            "zopper_share",
            "zopper_shared_transfer_price",
        ],
    )

    dim_text = ", ".join(detected_dims[:6]) if detected_dims else "none detected"
    price_text = price_col or "not found"
    qty_text = qty_col or "not found"
    cost_text = cost_or_margin_col or "not found"

    return (
        f"{_source_display_name(source)} {dataset_type} dataset profile: "
        f"rows={row_count:,}; columns sample={col_preview}; "
        f"detected dimensions={dim_text}; pricing field={price_text}; "
        f"quantity field={qty_text}; cost/margin related field={cost_text}."
    )


def _build_chatbot_global_context(
    *,
    db: Session,
    payload: ChatbotPayload,
    from_date: str | None,
    to_date: str | None,
    job_id: str | None,
) -> tuple[str, dict[str, Any]]:
    scopes = _chatbot_available_scopes(db=db, job_id=job_id)
    date_label = (
        f"{from_date or 'n/a'} to {to_date or 'n/a'}"
        if (from_date or to_date)
        else "all available data"
    )

    context_payload: dict[str, Any] = {
        "source": "",
        "source_label": "All Sources",
        "dataset_type": "all",
        "job_id": job_id,
        "from_date": from_date,
        "to_date": to_date,
        "rankings": [],
        "allowed_labels": sorted({_source_display_name(str(scope.get("source", ""))) for scope in scopes}),
        "global_scope": True,
    }

    context_lines = [
        "Scope mode: cross-source analytics context using dashboard summaries plus underlying dataset records.",
        f"Selected date range: {date_label}",
    ]
    if job_id:
        context_lines.append(f"Selected job tag: {job_id}")

    if not scopes:
        context_lines.append("No rows are available in data_rows for the current filters.")
        return "\n".join(context_lines), context_payload

    total_rows = sum(int(scope.get("row_count", 0) or 0) for scope in scopes)
    context_lines.append(
        f"Available source/dataset slices: {len(scopes)} (total rows: {total_rows:,})."
    )
    context_lines.append(
        "Slices: "
        + "; ".join(
            f"{_source_display_name(str(scope.get('source', '')))} {scope.get('dataset_type')} ({int(scope.get('row_count', 0) or 0):,} rows)"
            for scope in scopes[:12]
        )
    )
    if any(
        _normalize_source_key(str(scope.get("source", ""))) in {"samsung", "samsung_vs", "samsung_croma"}
        for scope in scopes
    ):
        context_lines.append(_samsung_model_mapping_context_line())
        context_lines.extend(_samsung_plan_reference_context_lines())

    sales_totals = {
        "gross_premium": 0.0,
        "earned_premium": 0.0,
        "zopper_earned_premium": 0.0,
        "units_sold": 0.0,
    }
    claims_totals = {
        "gross_premium": 0.0,
        "earned_premium": 0.0,
        "units_sold": 0.0,
    }
    summary_lines: list[str] = []
    dataset_profile_lines: list[str] = []
    for scope in scopes[:12]:
        source = str(scope.get("source", ""))
        dataset_type = str(scope.get("dataset_type", ""))
        if dataset_type not in {"sales", "claims"}:
            continue

        summary = _resolve_summary_for_scope(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            from_date=from_date,
            to_date=to_date,
        )
        if not summary:
            continue

        if dataset_type == "claims":
            total_claims_cost = float(summary.get("gross_premium", 0) or 0)
            net_claims_cost = float(summary.get("earned_premium", 0) or 0)
            claims_count = float(summary.get("units_sold", 0) or 0)
            claims_totals["gross_premium"] += total_claims_cost
            claims_totals["earned_premium"] += net_claims_cost
            claims_totals["units_sold"] += claims_count
            summary_lines.append(
                f"{_source_display_name(source)} claims summary: "
                f"Total Claims Cost={_format_metric_value('claims', total_claims_cost)}; "
                f"Net Claims Cost Paid={_format_metric_value('net_claims', net_claims_cost)}; "
                f"No. of Claims={int(claims_count):,}"
            )
        else:
            gross_premium = float(summary.get("gross_premium", 0) or 0)
            earned_premium = float(summary.get("earned_premium", 0) or 0)
            zopper_earned_premium = float(summary.get("zopper_earned_premium", 0) or 0)
            units_sold = float(summary.get("units_sold", 0) or 0)
            sales_totals["gross_premium"] += gross_premium
            sales_totals["earned_premium"] += earned_premium
            sales_totals["zopper_earned_premium"] += zopper_earned_premium
            sales_totals["units_sold"] += units_sold
            summary_lines.append(
                f"{_source_display_name(source)} sales summary: "
                f"Gross Premium={_format_metric_value('gross_premium', gross_premium)}; "
                f"Earned Premium={_format_metric_value('earned_premium', earned_premium)}; "
                f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', zopper_earned_premium)}; "
                f"Units Sold={int(units_sold):,}"
            )

        if len(dataset_profile_lines) < 4:
            profile_line = _build_dataset_field_profile(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                from_date=from_date,
                to_date=to_date,
            )
            if profile_line:
                dataset_profile_lines.append(profile_line)

    if summary_lines:
        context_lines.extend(summary_lines)
    else:
        context_lines.append(
            "Summary metrics were not precomputed for these slices, but row-level records are available in data_rows."
        )
    if dataset_profile_lines:
        context_lines.extend(dataset_profile_lines)

    if any(value > 0 for value in sales_totals.values()):
        context_lines.append(
            "All-sources sales total: "
            f"Gross Premium={_format_metric_value('gross_premium', sales_totals['gross_premium'])}; "
            f"Earned Premium={_format_metric_value('earned_premium', sales_totals['earned_premium'])}; "
            f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', sales_totals['zopper_earned_premium'])}; "
            f"Units Sold={int(sales_totals['units_sold']):,}"
        )
    if any(value > 0 for value in claims_totals.values()):
        context_lines.append(
            "All-sources claims total: "
            f"Total Claims Cost={_format_metric_value('claims', claims_totals['gross_premium'])}; "
            f"Net Claims Cost Paid={_format_metric_value('net_claims', claims_totals['earned_premium'])}; "
            f"No. of Claims={int(claims_totals['units_sold']):,}"
        )

    return "\n".join(context_lines), context_payload


def _build_chatbot_greeting_response(
    *,
    db: Session,
    payload: ChatbotPayload,
) -> str:
    job_id = _normalize_chatbot_job_id(payload.job_id)
    scopes = _chatbot_available_scopes(db=db, job_id=job_id)
    if not scopes:
        return (
            "Hi. I'm AI Sahyogi and I can analyze dashboard data, but I don't see any rows in the database right now. "
            "Upload or sync data, then ask about trends, anomalies, or actions."
        )

    total_rows = sum(int(scope.get("row_count", 0) or 0) for scope in scopes)
    preview = "; ".join(
        f"{_source_display_name(str(scope.get('source', '')))} {scope.get('dataset_type')} ({int(scope.get('row_count', 0) or 0):,} rows)"
        for scope in scopes[:5]
    )
    return (
        "Hi. I'm AI Sahyogi and I can analyze dashboard metrics plus underlying dataset records across available sources. "
        f"Current coverage: {len(scopes)} source/dataset slices, {total_rows:,} rows total. "
        f"Examples: {preview}. "
        "Ask any business question and I will answer from the available analytics data."
    )


def _pick_present_key(rows: list[dict[str, Any]], candidates: list[str]) -> str | None:
    if not rows:
        return None
    safe_to_raw: dict[str, str] = {}
    for row in rows[:12]:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            safe_to_raw[_to_safe_key(str(key))] = str(key)

    for candidate in candidates:
        safe_candidate = _to_safe_key(candidate)
        if safe_candidate in safe_to_raw:
            return safe_to_raw[safe_candidate]
    return None


def _guess_numeric_key(rows: list[dict[str, Any]], exclude_keys: set[str]) -> str | None:
    score: dict[str, int] = {}
    for row in rows[:120]:
        if not isinstance(row, dict):
            continue
        for key, raw in row.items():
            if key in exclude_keys:
                continue
            if _to_number(raw) is not None:
                score[key] = score.get(key, 0) + 1
    if not score:
        return None
    return max(score, key=lambda k: score[k])


def _rank_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    metric: str,
) -> dict[str, Any] | None:
    if not rows:
        return None

    dimension_key = _pick_present_key(rows, [dimension])
    metric_key = _pick_present_key(rows, [metric])
    if metric_key is None:
        metric_key = _guess_numeric_key(rows, {dimension_key} if dimension_key else set())
    if metric_key is None:
        return None

    totals: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _to_number(row.get(metric_key))
        if value is None:
            continue

        label = ""
        if dimension_key is not None:
            label = str(row.get(dimension_key, "")).strip()
        if not label:
            for key, raw in row.items():
                if key == metric_key:
                    continue
                if _to_number(raw) is None:
                    label = str(raw or "").strip()
                    if label:
                        break
        if not label:
            continue

        if label.lower() in {"nan", "none", "null"}:
            continue
        totals[label] = totals.get(label, 0.0) + float(value)

    if not totals:
        return None

    ordered_desc = sorted(totals.items(), key=lambda pair: pair[1], reverse=True)
    ordered_asc = sorted(totals.items(), key=lambda pair: pair[1])
    return {
        "dimension": dimension,
        "metric": metric,
        "top": ordered_desc[:4],
        "bottom": ordered_asc[:3],
        "labels": list(totals.keys())[:40],
    }


def _chatbot_dimension_candidates(source: str) -> list[str]:
    source_key = _normalize_source_key(source)
    if source_key == "reliance":
        return ["brand", "device_plan_category", "plan_category", "state", "month"]
    if source_key == "godrej":
        return ["product_category", "channel", "state", "month", "plan_category"]
    return ["brand", "plan_category", "device_plan_category", "state", "month"]


def _chatbot_metric_candidates(dataset_type: str) -> list[str]:
    if dataset_type == "claims":
        return ["loss_ratio", "claims", "net_claims", "quantity"]
    return ["gross_premium", "earned_premium", "zopper_earned_premium", "quantity"]


def _format_rank_pairs(metric: str, pairs: list[tuple[str, float]]) -> str:
    return "; ".join(f"{label} ({_format_metric_value(metric, value)})" for label, value in pairs)


def _chatbot_graph_rows(
    *,
    db: Session,
    source: str,
    dataset_type: str,
    job_id: str | None,
    dimension: str,
    metric: str,
    from_date: str | None,
    to_date: str | None,
) -> list[dict[str, Any]]:
    rows = get_precomputed_graph(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        dimension=dimension,
        metric=metric,
        from_date=from_date,
        to_date=to_date,
    )
    if rows is None and (from_date or to_date):
        rows = get_precomputed_graph(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension=dimension,
            metric=metric,
        )

    should_try_live = not rows
    if should_try_live:
        try:
            rows = compute_by_dimension_rows(
                db=db,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                source=source,
                dataset_type=dataset_type,
                from_date=from_date,
                to_date=to_date,
            )
        except Exception:
            logger.exception(
                "Chatbot live dimension fetch failed source=%s dataset=%s dimension=%s metric=%s",
                source,
                dataset_type,
                dimension,
                metric,
            )
            rows = []

    return rows or []


def _build_chatbot_dashboard_context(
    *,
    db: Session,
    payload: ChatbotPayload,
) -> tuple[str, dict[str, Any]]:
    source = _resolve_chatbot_source(payload)
    dataset_type = _resolve_chatbot_dataset_type(payload)
    from_date, to_date = _normalize_chatbot_date_range(payload.from_date, payload.to_date)
    job_id = _normalize_chatbot_job_id(payload.job_id)

    context_payload: dict[str, Any] = {
        "source": source,
        "source_label": _source_display_name(source),
        "dataset_type": dataset_type,
        "job_id": job_id,
        "from_date": from_date,
        "to_date": to_date,
        "rankings": [],
        "allowed_labels": [],
        "requested_dimensions": [],
    }

    if not source or _requests_global_scope(payload.message):
        return _build_chatbot_global_context(
            db=db,
            payload=payload,
            from_date=from_date,
            to_date=to_date,
            job_id=job_id,
        )

    summary = get_precomputed_summary(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    if summary is None and (from_date or to_date):
        summary = get_precomputed_summary(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
        )
    if not isinstance(summary, dict):
        summary = {}

    metric_candidates = _chatbot_metric_candidates(dataset_type)
    requested_dimensions = _chatbot_requested_dimensions(payload)
    dimension_candidates = _prioritize_dimensions(
        _chatbot_dimension_candidates(source),
        requested_dimensions,
    )
    rankings: list[dict[str, Any]] = []
    allowed_labels: set[str] = set()
    context_payload["requested_dimensions"] = requested_dimensions

    max_rankings = 5
    required_dimensions = set(requested_dimensions)
    for dimension in dimension_candidates:
        snapshot: dict[str, Any] | None = None
        for metric in metric_candidates:
            graph_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                dimension=dimension,
                metric=metric,
                from_date=from_date,
                to_date=to_date,
            )
            snapshot = _rank_dimension_rows(graph_rows, dimension=dimension, metric=metric)
            if snapshot:
                break
        if snapshot:
            rankings.append(snapshot)
            for label in snapshot.get("labels", [])[:12]:
                if label:
                    allowed_labels.add(str(label))
        if len(rankings) >= max_rankings:
            ranking_dims = {str(item.get("dimension") or "") for item in rankings}
            if required_dimensions.issubset(ranking_dims):
                break

    context_payload["rankings"] = rankings
    context_payload["allowed_labels"] = sorted(allowed_labels)

    date_label = (
        f"{from_date or 'n/a'} to {to_date or 'n/a'}"
        if (from_date or to_date)
        else "all available data"
    )

    context_lines = [
        f"Selected source: {_source_display_name(source)}",
        f"Selected dataset: {dataset_type}",
        f"Selected date range: {date_label}",
    ]
    if _normalize_source_key(source) in {"samsung", "samsung_vs", "samsung_croma"}:
        context_lines.append(_samsung_model_mapping_context_line())
        context_lines.extend(_samsung_plan_reference_context_lines())
    if job_id:
        context_lines.append(f"Selected job tag: {job_id}")

    if dataset_type == "claims":
        context_lines.append(
            "Summary metrics: "
            f"Total Claims Cost={_format_metric_value('claims', float(summary.get('gross_premium', 0) or 0))}; "
            f"Net Claims Cost Paid={_format_metric_value('net_claims', float(summary.get('earned_premium', 0) or 0))}; "
            f"No. of Claims={int(float(summary.get('units_sold', 0) or 0)):,}"
        )
    else:
        context_lines.append(
            "Summary metrics: "
            f"Gross Premium={_format_metric_value('gross_premium', float(summary.get('gross_premium', 0) or 0))}; "
            f"Earned Premium={_format_metric_value('earned_premium', float(summary.get('earned_premium', 0) or 0))}; "
            f"Zopper Earned Premium={_format_metric_value('zopper_earned_premium', float(summary.get('zopper_earned_premium', 0) or 0))}; "
            f"Units Sold={int(float(summary.get('units_sold', 0) or 0)):,}"
        )

    profile_line = _build_dataset_field_profile(
        db=db,
        source=source,
        dataset_type=dataset_type,
        job_id=job_id,
        from_date=from_date,
        to_date=to_date,
    )
    if profile_line:
        context_lines.append(profile_line)

    if rankings:
        for snapshot in rankings:
            top = snapshot.get("top", [])
            bottom = snapshot.get("bottom", [])
            if not top and not bottom:
                continue
            top_text = _format_rank_pairs(snapshot["metric"], top) if top else "n/a"
            bottom_text = _format_rank_pairs(snapshot["metric"], bottom) if bottom else "n/a"
            context_lines.append(
                f"{_pretty_label(snapshot['dimension'])} by {_pretty_label(snapshot['metric'])}: "
                f"Top={top_text} | Bottom={bottom_text}"
            )
    else:
        context_lines.append("No ranked dimension rows were available for this slice.")

    if allowed_labels:
        context_lines.append(f"Allowed entity labels: {', '.join(sorted(allowed_labels)[:16])}")

    return "\n".join(context_lines), context_payload


def _is_underperformance_query(message: str) -> bool:
    low = (message or "").strip().lower()
    if not low:
        return False
    under_tokens = ("underperform", "under performing", "under-performing", "lagging", "weakest", "worst", "lowest")
    return any(token in low for token in under_tokens)


def _is_dimension_stats_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    stat_tokens = (
        "stats",
        "statistics",
        "breakdown",
        "distribution",
        "wise",
        "statewise",
        "monthwise",
        "citywise",
        "by state",
        "by month",
        "by city",
        "by channel",
    )
    return any(token in low for token in stat_tokens)


def _build_dimension_stats_answer(message: str, context_payload: dict[str, Any]) -> str | None:
    if not _is_dimension_stats_query(message):
        return None

    rankings = context_payload.get("rankings") or []
    if not rankings:
        return None

    requested_dimensions = _chatbot_requested_dimensions_from_text(message)
    if not requested_dimensions:
        return None

    snapshot: dict[str, Any] | None = None
    requested_dimension = requested_dimensions[0]
    for dimension in requested_dimensions:
        snapshot = next((row for row in rankings if row.get("dimension") == dimension), None)
        if snapshot:
            requested_dimension = dimension
            break

    source_label = context_payload.get("source_label") or "the selected source"
    dataset_type = context_payload.get("dataset_type") or "sales"
    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")

    if snapshot is None:
        return (
            f"I can’t confirm {_pretty_label(requested_dimension).lower()}-wise statistics from the current dashboard data "
            f"for {source_label}."
        )

    top = snapshot.get("top") or []
    bottom = snapshot.get("bottom") or []
    if not top and not bottom:
        return (
            f"I can’t confirm {_pretty_label(requested_dimension).lower()}-wise statistics from the current dashboard data "
            f"for {source_label}."
        )

    metric = snapshot.get("metric") or "gross_premium"
    dimension = snapshot.get("dimension") or requested_dimension
    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    top_text = _format_rank_pairs(metric, top[:4]) if top else "n/a"
    low_text = _format_rank_pairs(metric, bottom[:3]) if bottom else "n/a"
    return (
        f"In {source_label} {dataset_type}{range_suffix}, {_pretty_label(dimension).lower()}-wise "
        f"{_pretty_label(metric).lower()} snapshot: Top segments are {top_text}. "
        f"Lowest segments are {low_text}."
    )


def _build_underperformance_answer(message: str, context_payload: dict[str, Any]) -> str | None:
    if not _is_underperformance_query(message):
        return None

    rankings = context_payload.get("rankings") or []
    if not rankings:
        return None

    low_message = (message or "").lower()
    requested_dimensions = _chatbot_requested_dimensions_from_text(message)
    strict_dimension: str | None = requested_dimensions[0] if requested_dimensions else None
    wants_brand = "brand" in low_message or strict_dimension == "brand"

    default_dimensions = (
        "device_plan_category",
        "plan_category",
        "product_category",
        "channel",
        "state",
        "month",
    )
    if wants_brand:
        default_dimensions = ("brand",) + default_dimensions

    preferred_dimensions = tuple(
        _prioritize_dimensions(
            list(default_dimensions),
            requested_dimensions,
        )
    )

    snapshot: dict[str, Any] | None = None
    for dim in preferred_dimensions:
        snapshot = next((row for row in rankings if row.get("dimension") == dim), None)
        if snapshot:
            break

    if snapshot is None and strict_dimension:
        source_label = context_payload.get("source_label") or "the selected source"
        return (
            f"I can’t confirm {_pretty_label(strict_dimension).lower()}-level underperformance from the current dashboard data "
            f"for {source_label}."
        )
    if snapshot is None and wants_brand:
        source_label = context_payload.get("source_label") or "the selected source"
        return f"I can’t confirm brand-level underperformance from the current dashboard data for {source_label}."
    if snapshot is None:
        snapshot = rankings[0]

    bottom = snapshot.get("bottom") or []
    if not bottom:
        return None

    lowest_label, lowest_value = bottom[0]
    source_label = context_payload.get("source_label") or "the selected source"
    dataset_type = context_payload.get("dataset_type") or "sales"
    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    metric = snapshot.get("metric") or "gross_premium"
    dimension = snapshot.get("dimension") or "category"

    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    answer = (
        f"In {source_label} {dataset_type}{range_suffix}, the lowest {_pretty_label(metric).lower()} "
        f"across {_pretty_label(dimension).lower()} is {lowest_label} at {_format_metric_value(metric, float(lowest_value))}. "
    )
    if len(bottom) > 1:
        next_label, next_value = bottom[1]
        answer += (
            f"The next lowest is {next_label} at {_format_metric_value(metric, float(next_value))}. "
        )
    answer += "This is the current underperformer in the dashboard slice."
    return answer


_SAMSUNG_PLAN_REFERENCE_LINES: tuple[str, ...] = (
    "Samsung plan glossary: ADLD = Accidental Damage and Liquid Damage; SP/SPP = Screen Protection Plan; EW = Extended Warranty; CPP = Comprehensive Protection Plan; Combo = ADLD + EW.",
    "Samsung products/devices covered: smartphones, tablets, laptops, and smartwatches (subject to Samsung terms and channel eligibility in India).",
    "Coverage summary: ADLD covers accidental/liquid damage; SPP covers screen/display damage; EW covers mechanical and electrical breakdown; CPP covers accidental damage plus mechanical/electrical breakdown.",
    "Claims process summary: login via registered mobile OTP on Samsung unified portal, open Raise Claim for active policy, submit issue/carry-in details, choose service center and visit slot, pay processing fee where applicable, then receive claim ID.",
)


def _samsung_plan_reference_context_lines() -> list[str]:
    return list(_SAMSUNG_PLAN_REFERENCE_LINES)


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower())).strip()


def _extract_location_query_token(message: str, context_payload: dict[str, Any]) -> str | None:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return None

    patterns = (
        r"\bin\s+([a-z][a-z\s\-]{1,40}?)(?=\s*,\s*in\b|\s+in\s+the\s+month\b|\s+in\s+month\b|\s+during\b|\s+for\b|\s+on\b|[?.!,]|$)",
        r"\bfor\s+([a-z][a-z\s\-]{1,40}?)(?=\s*,|\s+in\s+the\s+month\b|\s+in\s+month\b|\s+during\b|\s+on\b|[?.!,]|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, low)
        if not match:
            continue
        token = _normalize_lookup_text(match.group(1))
        if token and token not in {"month", "claims", "claim", "state", "city"}:
            return token

    allowed_labels = [str(label or "") for label in (context_payload.get("allowed_labels") or [])]
    for label in sorted(allowed_labels, key=lambda item: len(item), reverse=True):
        normalized_label = _normalize_lookup_text(label)
        if not normalized_label:
            continue
        pattern = r"\b" + re.escape(normalized_label).replace(r"\ ", r"\s+") + r"\b"
        if re.search(pattern, low):
            return normalized_label
    return None


def _extract_month_window_from_text(message: str) -> tuple[str, str, str] | None:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return None

    month_pattern = (
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\s*[,/\-]?\s*(\d{2}|\d{4})\b"
    )
    match = re.search(month_pattern, low)
    if not match:
        return None

    month_key = match.group(1)[:3]
    month_num = _FORECAST_MONTH_MAP.get(month_key)
    if month_num is None:
        return None

    year_raw = match.group(2)
    year = int(year_raw) + 2000 if len(year_raw) == 2 else int(year_raw)
    if year < 1900 or year > 2200:
        return None

    start_dt = date(year, month_num, 1)
    if month_num == 12:
        end_dt = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(year, month_num + 1, 1) - timedelta(days=1)
    return start_dt.isoformat(), end_dt.isoformat(), start_dt.strftime("%B %Y")


def _present_frame_columns(frame: Any, candidates: list[str]) -> list[str]:
    if frame is None or getattr(frame, "empty", True):
        return []
    try:
        columns = [str(col) for col in list(frame.columns)]
    except Exception:
        return []

    safe_candidates = {_to_safe_key(candidate) for candidate in candidates}
    out: list[str] = []
    for col in columns:
        if _to_safe_key(col) in safe_candidates:
            out.append(col)
    return out


def _location_mask_for_column(frame: Any, column: str, location_token: str) -> tuple[list[bool], int]:
    if frame is None or getattr(frame, "empty", True):
        return [], 0

    needle = _normalize_lookup_text(location_token)
    if not needle:
        return [], 0

    try:
        raw_values = frame[column].tolist()
    except Exception:
        return [], 0

    boundary_pattern = re.compile(r"(?:^| )" + re.escape(needle) + r"(?: |$)")
    mask: list[bool] = []
    count = 0
    for raw in raw_values:
        normalized = _normalize_lookup_text(str(raw or ""))
        matched = (
            bool(normalized)
            and (
                normalized == needle
                or bool(boundary_pattern.search(normalized))
                or needle in normalized
            )
        )
        mask.append(bool(matched))
        if matched:
            count += 1
    return mask, count


def _match_frame_by_location(
    frame: Any,
    location_token: str,
) -> tuple[Any, str, bool, bool]:
    if frame is None or getattr(frame, "empty", True):
        return frame, "", False, False

    city_columns = _present_frame_columns(
        frame,
        [
            "city",
            "customer_city",
            "customer city",
            "city_name",
            "location",
            "district",
            "state_city",
            "state/city",
            "state / city",
        ],
    )
    state_columns = _present_frame_columns(
        frame,
        [
            "state",
            "customer_state",
            "customer state",
            "state_name",
            "region",
            "ut",
            "union territory",
            "state_city",
            "state/city",
            "state / city",
        ],
    )
    city_present = bool(city_columns)
    state_present = bool(state_columns)

    best_city_mask: list[bool] | None = None
    best_city_count = 0
    for col in city_columns:
        mask, count = _location_mask_for_column(frame, col, location_token)
        if count > best_city_count:
            best_city_mask = mask
            best_city_count = count
    if best_city_mask is not None and best_city_count > 0:
        return frame[best_city_mask].copy(), "city", city_present, state_present

    best_state_mask: list[bool] | None = None
    best_state_count = 0
    for col in state_columns:
        mask, count = _location_mask_for_column(frame, col, location_token)
        if count > best_state_count:
            best_state_mask = mask
            best_state_count = count
    if best_state_mask is not None and best_state_count > 0:
        return frame[best_state_mask].copy(), "state", city_present, state_present

    return frame.iloc[0:0].copy(), "", city_present, state_present


def _is_claim_average_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    if "claim" not in low:
        return False
    return any(token in low for token in ("average", "avg", "mean"))


def _build_claim_average_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_claim_average_query(payload.message):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    if dataset_type != "claims":
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    global_scope = bool(context_payload.get("global_scope"))

    source_candidates: list[str] = []
    if global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        source_candidates = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == "claims"
        ]
    else:
        source = str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip()
        if source:
            source_candidates = [source]

    source_candidates = [src for src in source_candidates if src]
    if not source_candidates:
        return None

    month_window = _extract_month_window_from_text(payload.message)
    location_token = _extract_location_query_token(payload.message, context_payload)

    total_net_claims = 0.0
    total_claim_count = 0.0
    total_rows = 0
    city_match_hits = 0
    state_match_hits = 0
    city_columns_seen = False
    state_columns_seen = False

    for source in source_candidates:
        try:
            frame = get_dataframe(
                db=db,
                job_id=job_id,
                source=source,
                dataset_type="claims",
            )
        except Exception:
            logger.exception(
                "Chatbot claims average fetch failed source=%s job_id=%s",
                source,
                job_id,
            )
            continue

        if frame is None or getattr(frame, "empty", True):
            continue

        try:
            scoped = frame.copy()
        except Exception:
            continue

        if from_date or to_date:
            scoped = filter_by_date_range(scoped, "claims", from_date, to_date)
        if month_window is not None:
            scoped = filter_by_date_range(scoped, "claims", month_window[0], month_window[1])
        if scoped is None or getattr(scoped, "empty", True):
            continue

        if location_token:
            scoped, match_level, city_present, state_present = _match_frame_by_location(scoped, location_token)
            city_columns_seen = city_columns_seen or city_present
            state_columns_seen = state_columns_seen or state_present
            if match_level == "city":
                city_match_hits += 1
            elif match_level == "state":
                state_match_hits += 1
            if scoped is None or getattr(scoped, "empty", True):
                continue

        net_claims = _sum_metric_from_dataframe(
            scoped,
            [
                "net_claims",
                "net_claim",
                "net_amount",
                "claims",
                "claim_amount",
                "zoppers_cost",
                "amount",
                "earned_premium",
                "gross_premium",
            ],
        )
        claim_count = _sum_metric_from_dataframe(
            scoped,
            [
                "quantity",
                "claims_count",
                "no_of_claims",
                "count",
                "units_sold",
            ],
        )
        if claim_count <= 0:
            try:
                claim_count = float(len(scoped.index))
            except Exception:
                claim_count = 0.0
        if claim_count <= 0:
            continue

        total_net_claims += float(net_claims)
        total_claim_count += float(claim_count)
        try:
            total_rows += int(len(scoped.index))
        except Exception:
            pass

    if total_claim_count <= 0:
        period_label = month_window[2] if month_window else "the selected period"
        if location_token and (city_columns_seen or state_columns_seen):
            return (
                f"I can’t confirm average claim raised for {location_token.title()} in {period_label} from current matched rows. "
                "Recommendation: standardize city values and keep a city-level filter in claims data to close this gap."
            )
        if location_token:
            return (
                f"I can’t confirm average claim raised for {location_token.title()} in {period_label} because city/state location fields are not consistently available in this claims slice. "
                "Recommendation: add a normalized city column and make it mandatory at claim intake."
            )
        return "I don’t have enough claims rows in the selected scope to compute a reliable average claim."

    avg_claim = total_net_claims / total_claim_count if total_claim_count > 0 else 0.0
    scope_label = (
        "all claims sources"
        if global_scope
        else str(context_payload.get("source_label") or _source_display_name(source_candidates[0]))
    )
    period_label = month_window[2] if month_window else (
        f"{from_date or 'start'} to {to_date or 'latest'}" if (from_date or to_date) else "all available data"
    )

    if location_token:
        answer = (
            f"Average net claim raised in {location_token.title()} for {period_label} in {scope_label} is "
            f"{_format_metric_value('net_claims', float(avg_claim))} per claim "
            f"(total net claims {_format_metric_value('net_claims', float(total_net_claims))} across {int(total_claim_count):,} claims)."
        )
    else:
        answer = (
            f"Average net claim for {period_label} in {scope_label} is "
            f"{_format_metric_value('net_claims', float(avg_claim))} per claim "
            f"(total net claims {_format_metric_value('net_claims', float(total_net_claims))} across {int(total_claim_count):,} claims)."
        )

    if location_token and state_match_hits > 0 and city_match_hits == 0:
        answer += " City-level match was unavailable, so this uses state/region-level matching."
    elif location_token and city_match_hits > 0:
        answer += " This is computed from city-level matched claims rows."

    if total_rows > 0:
        answer += f" Rows used: {total_rows:,}."
    return answer


_SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY: dict[str, str] = {
    "A06": "Mass",
    "F15": "Mass",
    "A16": "Mid",
    "A17": "Mid",
    "F17": "Mid",
    "A26": "High",
    "A35": "High",
    "A36": "High",
    "F55": "High",
    "A56": "Premium",
    "S24": "Super Premium",
    "S25": "Super Premium",
    "Fold6": "Luxury Fold",
    "Fold7": "Luxury Fold",
    "Flip7": "Luxury Flip",
}

_SAMSUNG_MODEL_CODES_ORDERED: tuple[str, ...] = tuple(
    sorted(_SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.keys(), key=lambda token: (-len(token), token))
)

_SAMSUNG_DEVICE_CATEGORY_ORDER: tuple[str, ...] = (
    "Luxury Fold",
    "Luxury Flip",
    "Super Premium",
    "Premium",
    "High",
    "Mid",
    "Mass",
)

_SAMSUNG_PLAN_CATEGORY_ORDER: tuple[str, ...] = (
    "ADLD",
    "Screen Protection",
    "Combo",
    "Extended Warranty",
)

_SAMSUNG_REFERENCE_PLAN_PRICES: dict[tuple[str, str], int] = {
    ("Luxury Fold", "ADLD"): 5299,
    ("Luxury Fold", "Screen Protection"): 3999,
    ("Luxury Fold", "Combo"): 8587,
    ("Luxury Fold", "Extended Warranty"): 2060,
    ("Luxury Flip", "ADLD"): 4199,
    ("Luxury Flip", "Screen Protection"): 3748,
    ("Luxury Flip", "Combo"): 6800,
    ("Luxury Flip", "Extended Warranty"): 1737,
    ("Super Premium", "ADLD"): 2539,
    ("Super Premium", "Screen Protection"): 1174,
    ("Super Premium", "Combo"): 4694,
    ("Super Premium", "Extended Warranty"): 1064,
    ("Premium", "ADLD"): 1686,
    ("Premium", "Screen Protection"): 523,
    ("Premium", "Combo"): 2299,
    ("Premium", "Extended Warranty"): 410,
    ("High", "ADLD"): 799,
    ("High", "Screen Protection"): 260,
    ("High", "Combo"): 1399,
    ("High", "Extended Warranty"): 242,
    ("Mid", "ADLD"): 563,
    ("Mid", "Screen Protection"): 135,
    ("Mid", "Combo"): 806,
    ("Mid", "Extended Warranty"): 149,
    ("Mass", "ADLD"): 159,
    ("Mass", "Screen Protection"): 53,
    ("Mass", "Combo"): 267,
    ("Mass", "Extended Warranty"): 46,
}

_SAMSUNG_DEVICE_CATEGORY_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("Luxury Fold", ("luxury fold", "fold")),
    ("Luxury Flip", ("luxury flip", "flip")),
    ("Super Premium", ("super premium", "super-premium")),
    ("Premium", ("premium",)),
    ("High", ("high",)),
    ("Mid", ("mid",)),
    ("Mass", ("mass",)),
]

_SAMSUNG_PLAN_CATEGORY_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("ADLD", ("adld",)),
    ("Screen Protection", ("screen protection", "screen-protection")),
    ("Combo", ("combo",)),
    ("Extended Warranty", ("extended warranty", "extended-warranty", "warranty")),
]


def _detect_samsung_model_code_from_text(text: str) -> str | None:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for model_code in _SAMSUNG_MODEL_CODES_ORDERED:
        pattern = r"\b" + re.escape(model_code.lower()) + r"\b"
        if re.search(pattern, low):
            return model_code
    return None


def _samsung_model_mapping_context_line() -> str:
    pairs = "; ".join(
        f"{model_code}->{category}"
        for model_code, category in _SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.items()
    )
    return f"Samsung model-to-device-plan-category mapping: {pairs}."


def _contains_text_alias(text: str, alias: str) -> bool:
    pattern = r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b"
    return re.search(pattern, text) is not None


def _detect_samsung_device_category_from_text(text: str) -> str | None:
    model_code = _detect_samsung_model_code_from_text(text)
    if model_code:
        mapped = _SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.get(model_code)
        if mapped:
            return mapped

    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for category, aliases in _SAMSUNG_DEVICE_CATEGORY_ALIASES:
        for alias in aliases:
            if not _contains_text_alias(low, alias):
                continue
            if category in {"High", "Mid", "Mass"} and alias in {"high", "mid", "mass"}:
                if not re.search(r"\b(device|plan|category|segment|tier)\b", low):
                    continue
            return category
    return None


def _detect_samsung_plan_category_from_text(text: str) -> str | None:
    low = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not low:
        return None

    for category, aliases in _SAMSUNG_PLAN_CATEGORY_ALIASES:
        for alias in aliases:
            if _contains_text_alias(low, alias):
                return category
    return None


def _is_samsung_source(source: str) -> bool:
    source_key = _normalize_source_key(source)
    return source_key in {"samsung", "samsung_vs", "samsung_croma"}


def _is_samsung_price_lookup_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False

    direct_lookup_tokens = ("price", "pricing", "cost", "rate", "amount", "mrp")
    has_lookup_intent = any(token in low for token in direct_lookup_tokens)
    if not has_lookup_intent and "how much" in low:
        has_lookup_intent = any(token in low for token in ("plan", "category", "price", "cost", "rate"))
    if not has_lookup_intent:
        return False

    uplift_tokens = ("increase", "hike", "raise", "uplift", "optimize", "optimise", "maximize")
    return not any(token in low for token in uplift_tokens)


def _format_rupee_int(value: int) -> str:
    return f"Rs {int(value):,}"


def _build_samsung_manual_price_answer(
    *,
    message: str,
    source_candidates: list[str],
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_samsung_price_lookup_query(message):
        return None

    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    selected_source_key = _normalize_source_key(str(context_payload.get("source") or ""))
    mentioned_source_key = _normalize_source_key(_detect_source_from_text(message) or "")
    mentions_samsung_family = bool(re.search(r"\b(samsung|croma|vijay)\b", low))

    in_samsung_scope = False
    if _is_samsung_source(selected_source_key) or _is_samsung_source(mentioned_source_key):
        in_samsung_scope = True
    elif mentions_samsung_family and any(_is_samsung_source(source) for source in source_candidates):
        in_samsung_scope = True

    if not in_samsung_scope:
        return None

    device_category = _detect_samsung_device_category_from_text(message)
    plan_category = _detect_samsung_plan_category_from_text(message)

    if device_category and plan_category:
        price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device_category, plan_category))
        if price is None:
            return None
        return (
            f"Samsung reference price for {device_category} in {plan_category} is {_format_rupee_int(price)}."
        )

    if device_category:
        lines = [f"Samsung reference prices for {device_category}:"]
        for plan in _SAMSUNG_PLAN_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device_category, plan))
            if price is None:
                continue
            lines.append(f"- {plan}: {_format_rupee_int(price)}")
        if len(lines) > 1:
            return "\n".join(lines)
        return None

    if plan_category:
        lines = [f"Samsung reference prices for {plan_category}:"]
        for device in _SAMSUNG_DEVICE_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device, plan_category))
            if price is None:
                continue
            lines.append(f"- {device}: {_format_rupee_int(price)}")
        if len(lines) > 1:
            return "\n".join(lines)
        return None

    lines = ["Samsung reference price matrix (Device Plan Category x Plan Category):"]
    for device in _SAMSUNG_DEVICE_CATEGORY_ORDER:
        chunks: list[str] = []
        for plan in _SAMSUNG_PLAN_CATEGORY_ORDER:
            price = _SAMSUNG_REFERENCE_PLAN_PRICES.get((device, plan))
            if price is None:
                continue
            chunks.append(f"{plan} {_format_rupee_int(price)}")
        if chunks:
            lines.append(f"- {device}: {'; '.join(chunks)}")
    lines.append("These are fixed category reference prices configured for Samsung queries.")
    return "\n".join(lines)


def _is_pricing_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    price_tokens = ("price", "pricing", "cost", "rate", "amount", "increase", "hike", "raise", "uplift")
    has_price_intent = any(token in low for token in price_tokens)
    if not has_price_intent and "how much" in low:
        has_price_intent = any(
            token in low
            for token in ("plan", "category", "price", "cost", "rate", "increase", "uplift")
        )
    business_tokens = ("revenue", "premium", "sales", "category", "segment", "plan")
    return has_price_intent and any(token in low for token in business_tokens)


def _aggregate_metric_by_dimension(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    metric: str,
) -> dict[str, float]:
    if not rows:
        return {}

    dim_key = _pick_present_key(rows, [dimension])
    if dim_key is None:
        safe_map = {_to_safe_key(str(k)): str(k) for k in rows[0].keys()}
        if _to_safe_key(dimension) == "plan_category":
            dim_key = safe_map.get("device_plan_category")
        elif _to_safe_key(dimension) == "device_plan_category":
            dim_key = safe_map.get("plan_category")

    metric_key = _pick_present_key(rows, [metric])
    out: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue

        label = ""
        if dim_key is not None:
            label = str(row.get(dim_key, "")).strip()
        if not label:
            continue
        if label.lower() in {"nan", "none", "null"}:
            continue

        value: float | None = None
        if metric_key is not None:
            value = _to_number(row.get(metric_key))

        if value is None:
            partner_vs = _to_number(row.get("samsung_vs"))
            partner_croma = _to_number(row.get("samsung_croma"))
            if partner_vs is not None or partner_croma is not None:
                value = float(partner_vs or 0) + float(partner_croma or 0)

        if value is None:
            fallback_total = 0.0
            found_numeric = False
            for key, raw in row.items():
                if dim_key is not None and str(key) == dim_key:
                    continue
                safe_key = _to_safe_key(str(key))
                if safe_key.startswith("tooltip_"):
                    continue
                numeric = _to_number(raw)
                if numeric is None:
                    continue
                fallback_total += float(numeric)
                found_numeric = True
            if found_numeric:
                value = fallback_total

        if value is None:
            continue
        out[label] = out.get(label, 0.0) + max(0.0, float(value))
    return out


def _build_pricing_recommendation_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_pricing_query(payload.message):
        return None

    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    if dataset_type != "sales":
        return (
            "Pricing recommendations are meaningful for sales datasets. "
            "Please switch to sales scope or ask a claims-specific optimization question."
        )

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    global_scope = bool(context_payload.get("global_scope"))

    source_candidates: list[str] = []
    if global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        source_candidates = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == "sales"
        ]
    else:
        source = str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip()
        if source:
            source_candidates = [source]

    source_candidates = [src for src in source_candidates if src]
    if not source_candidates:
        return None

    samsung_manual_price_answer = _build_samsung_manual_price_answer(
        message=payload.message,
        source_candidates=source_candidates,
        context_payload=context_payload,
    )
    if samsung_manual_price_answer:
        return samsung_manual_price_answer

    dimension_used = ""
    revenue_by_category: dict[str, float] = {}
    quantity_by_category: dict[str, float] = {}

    for dimension in ["plan_category", "device_plan_category", "product_category", "brand"]:
        rev_agg: dict[str, float] = {}
        qty_agg: dict[str, float] = {}
        for source in source_candidates:
            rev_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type="sales",
                job_id=job_id,
                dimension=dimension,
                metric="gross_premium",
                from_date=from_date,
                to_date=to_date,
            )
            qty_rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type="sales",
                job_id=job_id,
                dimension=dimension,
                metric="quantity",
                from_date=from_date,
                to_date=to_date,
            )
            local_rev = _aggregate_metric_by_dimension(
                rev_rows,
                dimension=dimension,
                metric="gross_premium",
            )
            local_qty = _aggregate_metric_by_dimension(
                qty_rows,
                dimension=dimension,
                metric="quantity",
            )
            for label, value in local_rev.items():
                rev_agg[label] = rev_agg.get(label, 0.0) + float(value)
            for label, value in local_qty.items():
                qty_agg[label] = qty_agg.get(label, 0.0) + float(value)

        valid_count = sum(
            1
            for label, revenue in rev_agg.items()
            if revenue > 0 and qty_agg.get(label, 0.0) > 0
        )
        if valid_count >= 2:
            dimension_used = dimension
            revenue_by_category = rev_agg
            quantity_by_category = qty_agg
            break

    if not dimension_used:
        return (
            "I can analyze pricing only when category-level gross premium and quantity are available. "
            "That split is not currently available in the selected dataset scope."
        )

    rows: list[dict[str, float | str]] = []
    total_revenue = 0.0
    for label, revenue in revenue_by_category.items():
        quantity = float(quantity_by_category.get(label, 0.0))
        if revenue <= 0 or quantity <= 0:
            continue
        avg_price = revenue / quantity
        total_revenue += revenue
        rows.append(
            {
                "label": label,
                "revenue": float(revenue),
                "quantity": float(quantity),
                "avg_price": float(avg_price),
            }
        )

    if len(rows) < 2 or total_revenue <= 0:
        return (
            "I can analyze pricing only when category-level gross premium and quantity are available. "
            "That split is not currently available in the selected dataset scope."
        )

    rows.sort(key=lambda item: float(item["revenue"]), reverse=True)
    for item in rows:
        share = float(item["revenue"]) / total_revenue
        if share >= 0.30:
            uplift_pct = 3.0
        elif share >= 0.15:
            uplift_pct = 4.0
        elif share >= 0.08:
            uplift_pct = 6.0
        else:
            uplift_pct = 8.0
        item["share"] = share
        item["uplift_pct"] = uplift_pct
        item["estimated_gain"] = float(item["revenue"]) * uplift_pct / 100.0

    shown = rows[:8]
    scope_label = (
        "all sales datasets"
        if global_scope
        else str(context_payload.get("source_label") or _source_display_name(source_candidates[0]))
    )
    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    lines = [
        f"Using {scope_label}{range_suffix} and category-level gross premium + quantity, this is a practical starting price-uplift plan by {_pretty_label(dimension_used).lower()} (assuming volume remains stable):"
    ]
    for idx, item in enumerate(shown, 1):
        lines.append(
            f"{idx}. {item['label']}: +{float(item['uplift_pct']):.0f}% "
            f"(avg premium {_format_metric_value('gross_premium', float(item['avg_price']))}, "
            f"current revenue {_format_metric_value('gross_premium', float(item['revenue']))}, "
            f"estimated gain +{_format_metric_value('gross_premium', float(item['estimated_gain']))})."
        )

    remaining = len(rows) - len(shown)
    if remaining > 0:
        lines.append(
            f"Remaining {remaining} lower-share categories can start with a +8% test band and be tuned weekly based on conversion."
        )
    lines.append(
        "This is a revenue-side scenario. Share margin targets and expected volume elasticity to optimize exact category-wise price changes."
    )
    return "\n".join(lines)


_FORECAST_MONTH_MAP: dict[str, int] = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _is_forecast_query(message: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    tokens = (
        "forecast",
        "predict",
        "prediction",
        "projection",
        "projected",
        "future month",
        "next month",
        "upcoming month",
        "likely next",
        "estimate next",
        "month ahead",
        "time series",
    )
    return any(token in low for token in tokens)


def _forecast_metric_hint_present(message: str, dataset_type: str) -> bool:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if not low:
        return False
    if dataset_type == "claims":
        return any(
            token in low
            for token in (
                "loss ratio",
                "net claim",
                "claims",
                "quantity",
                "count",
                "no. of claims",
            )
        )
    return any(
        token in low
        for token in (
            "gross premium",
            "earned premium",
            "zopper earned",
            "quantity",
            "units sold",
            "units",
            "count",
            "premium",
            "sales",
        )
    )


def _is_forecast_followup_query(payload: ChatbotPayload) -> bool:
    message = payload.message or ""
    if _is_pricing_query(message):
        return False
    if _is_forecast_query(message):
        return True

    low = re.sub(r"\s+", " ", message.strip().lower())
    if not low:
        return False

    followup_markers = (
        "what about",
        "how about",
        "and what",
        "and for",
        "for croma",
        "for vijay",
        "for samsung",
        "for reliance",
        "for godrej",
        "for this",
        "for that",
        "same for",
        "same question",
    )
    source_hint = _detect_source_from_text(message) is not None
    likely_followup = source_hint or any(marker in low for marker in followup_markers)
    if not likely_followup:
        return False

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        if _is_forecast_query(turn.content):
            return True
    return False


def _forecast_metric_from_text(message: str, dataset_type: str) -> str:
    low = re.sub(r"\s+", " ", (message or "").strip().lower())
    if dataset_type == "claims":
        if "loss ratio" in low:
            return "loss_ratio"
        if "net claim" in low:
            return "net_claims"
        if "quantity" in low or "count" in low or "no. of claims" in low:
            return "quantity"
        return "claims"

    if "zopper earned" in low:
        return "zopper_earned_premium"
    if "earned premium" in low:
        return "earned_premium"
    if "quantity" in low or "units sold" in low or "units" in low or "count" in low:
        return "quantity"
    if "gross premium" in low or "premium" in low:
        return "gross_premium"
    return "gross_premium"


def _resolve_forecast_metric(payload: ChatbotPayload, dataset_type: str) -> str:
    message = payload.message or ""
    if _forecast_metric_hint_present(message, dataset_type):
        return _forecast_metric_from_text(message, dataset_type)

    for turn in reversed(payload.history[-CHATBOT_HISTORY_LIMIT:]):
        if (turn.role or "").strip().lower() != "user":
            continue
        if not _is_forecast_query(turn.content):
            continue
        if _forecast_metric_hint_present(turn.content, dataset_type):
            return _forecast_metric_from_text(turn.content, dataset_type)

    return _forecast_metric_from_text(message, dataset_type)


def _parse_month_start(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)

    raw = str(value).strip()
    if not raw:
        return None

    short_match = re.match(r"^([A-Za-z]{3,9})[-/\s](\d{2}|\d{4})$", raw)
    if short_match:
        month_key = short_match.group(1)[:3].lower()
        month = _FORECAST_MONTH_MAP.get(month_key)
        if month:
            year_raw = int(short_match.group(2))
            year = year_raw + 2000 if len(short_match.group(2)) == 2 else year_raw
            if 1900 <= year <= 2200:
                return date(year, month, 1)

    year_month_match = re.match(r"^(\d{4})[-/](\d{1,2})$", raw)
    if year_month_match:
        year = int(year_month_match.group(1))
        month = int(year_month_match.group(2))
        if 1 <= month <= 12:
            return date(year, month, 1)

    iso_candidate = raw[:10]
    try:
        parsed = date.fromisoformat(iso_candidate)
        return date(parsed.year, parsed.month, 1)
    except ValueError:
        pass

    for fmt in ("%b-%y", "%b %y", "%b-%Y", "%b %Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            parsed_dt = datetime.strptime(raw, fmt)
            return date(parsed_dt.year, parsed_dt.month, 1)
        except ValueError:
            continue
    return None


def _next_month_start(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _extract_monthly_totals(rows: list[dict[str, Any]], metric: str) -> list[tuple[date, float]]:
    if not rows:
        return []

    dimension_key = _pick_present_key(
        rows,
        [
            "month",
            "date",
            "fiscal_month",
            "month_year",
        ],
    )
    if dimension_key is None:
        for key in rows[0].keys():
            safe_key = _to_safe_key(str(key))
            if "month" in safe_key or "date" in safe_key:
                dimension_key = str(key)
                break
    if dimension_key is None:
        return []

    metric_key = _pick_present_key(rows, [metric])
    monthly: dict[date, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        month_start = _parse_month_start(row.get(dimension_key))
        if month_start is None:
            continue

        value = _to_number(row.get(metric_key)) if metric_key else None
        if value is None:
            fallback_total = 0.0
            found_numeric = False
            for key, raw in row.items():
                if str(key) == dimension_key:
                    continue
                safe_key = _to_safe_key(str(key))
                if safe_key.startswith("tooltip_"):
                    continue
                numeric = _to_number(raw)
                if numeric is None:
                    continue
                fallback_total += float(numeric)
                found_numeric = True
            if not found_numeric:
                continue
            value = fallback_total

        monthly[month_start] = monthly.get(month_start, 0.0) + float(value)

    return sorted(monthly.items(), key=lambda item: item[0])


def _predict_next_month_value(series: list[tuple[date, float]]) -> dict[str, float] | None:
    if len(series) < 2:
        return None

    values = [float(point[1]) for point in series]
    deltas = [values[idx] - values[idx - 1] for idx in range(1, len(values))]
    if not deltas:
        return None

    recent_delta_count = min(6, len(deltas))
    recent_deltas = deltas[-recent_delta_count:]
    delta_weights = list(range(1, recent_delta_count + 1))
    delta_weight_total = float(sum(delta_weights))
    weighted_delta = sum(delta * weight for delta, weight in zip(recent_deltas, delta_weights)) / delta_weight_total

    growth_rates: list[float] = []
    for idx in range(1, len(values)):
        prev = values[idx - 1]
        curr = values[idx]
        if abs(prev) < 1e-9:
            continue
        growth_rates.append((curr - prev) / prev)

    if growth_rates:
        recent_growth_count = min(6, len(growth_rates))
        recent_growth = growth_rates[-recent_growth_count:]
        growth_weights = list(range(1, recent_growth_count + 1))
        growth_weight_total = float(sum(growth_weights))
        weighted_growth = sum(
            growth * weight for growth, weight in zip(recent_growth, growth_weights)
        ) / growth_weight_total
        projected = values[-1] * (1.0 + weighted_growth)
        trend_window = recent_growth_count
    else:
        projected = values[-1] + weighted_delta
        weighted_growth = weighted_delta / values[-1] if abs(values[-1]) > 1e-9 else 0.0
        trend_window = recent_delta_count

    if all(value >= 0 for value in values) and projected < 0:
        projected = 0.0

    return {
        "projected": float(projected),
        "weighted_growth": float(weighted_growth),
        "trend_window": float(trend_window),
    }


def _build_time_series_forecast_answer(
    *,
    db: Session,
    payload: ChatbotPayload,
    context_payload: dict[str, Any],
) -> str | None:
    if not _is_forecast_followup_query(payload):
        return None

    from_date = context_payload.get("from_date")
    to_date = context_payload.get("to_date")
    job_id = context_payload.get("job_id")
    dataset_type = str(context_payload.get("dataset_type") or _resolve_chatbot_dataset_type(payload) or "sales")
    metric = _resolve_forecast_metric(payload, dataset_type)
    global_scope = bool(context_payload.get("global_scope"))

    series: list[tuple[date, float]] = []
    scope_label = "selected dashboard scope"

    if global_scope:
        scopes = _chatbot_available_scopes(db=db, job_id=job_id)
        relevant_sources = [
            str(scope.get("source", ""))
            for scope in scopes
            if str(scope.get("dataset_type", "")).strip().lower() == dataset_type
        ]
        aggregated: dict[date, float] = {}
        for source in relevant_sources:
            rows = _chatbot_graph_rows(
                db=db,
                source=source,
                dataset_type=dataset_type,
                job_id=job_id,
                dimension="month",
                metric=metric,
                from_date=from_date,
                to_date=to_date,
            )
            for month_start, value in _extract_monthly_totals(rows, metric):
                aggregated[month_start] = aggregated.get(month_start, 0.0) + float(value)
        series = sorted(aggregated.items(), key=lambda item: item[0])
        scope_label = f"all sources ({dataset_type})"
    else:
        source = str(context_payload.get("source") or _resolve_chatbot_source(payload) or "").strip()
        if not source:
            return None
        rows = _chatbot_graph_rows(
            db=db,
            source=source,
            dataset_type=dataset_type,
            job_id=job_id,
            dimension="month",
            metric=metric,
            from_date=from_date,
            to_date=to_date,
        )
        series = _extract_monthly_totals(rows, metric)
        scope_label = f"{context_payload.get('source_label') or _source_display_name(source)} {dataset_type}"

    if len(series) < 2:
        return "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast."

    forecast = _predict_next_month_value(series)
    if forecast is None:
        return "I don’t have enough month-level history in the current dataset scope to produce a reliable forecast."

    last_month, last_value = series[-1]
    next_month = _next_month_start(last_month)
    next_label = next_month.strftime("%b %y")
    last_label = last_month.strftime("%b %y")
    history_window = min(6, len(series))
    history_start = series[-history_window][0].strftime("%b %y")
    growth_pct = float(forecast["weighted_growth"]) * 100.0
    trend_word = "increase" if growth_pct >= 0 else "decline"

    range_suffix = ""
    if from_date or to_date:
        range_suffix = f" ({from_date or 'start'} to {to_date or 'latest'})"

    return (
        f"Directional forecast for {scope_label}{range_suffix}: "
        f"{_pretty_label(metric)} is most likely around {_format_metric_value(metric, float(forecast['projected']))} in {next_label}, "
        f"based on month-on-month trend from {history_start} to {last_label}. "
        f"Latest observed value is {_format_metric_value(metric, last_value)} and recent momentum implies a {trend_word} of {abs(growth_pct):.1f}% MoM."
    )


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
                f"In {latest_label}, {_pretty_label(series_a)} stands at {_format_metric_value(payload.metric, latest_a)}."
            )
        if series_b and latest_b is not None:
            insights.append(
                f"In {latest_label}, {_pretty_label(series_b)} stands at {_format_metric_value(payload.metric, latest_b)}."
            )
        if latest_a is not None and latest_b is not None:
            leader = series_a if latest_a >= latest_b else series_b
            gap = abs(latest_a - latest_b)
            insights.append(
                f"{_pretty_label(leader)} leads by {_format_metric_value(payload.metric, gap)} in the latest period, signaling stronger momentum."
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

    insights.append(
        f"Latest {metric_name} is {_format_metric_value(actual_metric_key, last_value)} in {last_label}."
    )
    if len(points) > 1:
        delta = last_value - first_value
        direction = "increased" if delta >= 0 else "decreased"
        momentum = "positive momentum" if delta >= 0 else "a contraction trend"
        pct = (abs(delta) / abs(first_value) * 100.0) if first_value else None
        if pct is None:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} from {first_label} to {last_label}, indicating {momentum}."
            )
        else:
            insights.append(
                f"{metric_name} {direction} by {_format_metric_value(actual_metric_key, abs(delta))} ({pct:.1f}%) from {first_label} to {last_label}, indicating {momentum}."
            )

    insights.append(
        f"Peak {metric_name} reached {_format_metric_value(actual_metric_key, peak_value)} in {peak_label}."
    )
    insights.append(
        f"Lowest {metric_name} was {_format_metric_value(actual_metric_key, low_value)} in {low_label}."
    )

    total = sum(v for _, v in points if v > 0)
    if total > 0:
        top3 = sorted(points, key=lambda x: x[1], reverse=True)[:3]
        share = sum(v for _, v in top3) / total * 100.0
        insights.append(
            f"Top 3 categories contribute {share:.1f}% of total {metric_name}, highlighting concentration risk."
        )

    return _dedupe_insights(insights)

def _build_insight_prompt(payload: GraphInsightPayload) -> str:
    rows = payload.rows[:120]
    serialized_rows = json.dumps(rows, ensure_ascii=True, default=str)
    return (
        "Generate executive-ready insights for the graph below.\n"
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
        "Output requirements:\n"
        "- 4 to 6 bullets.\n"
        "- Each bullet must include at least one concrete number from the data.\n"
        "- Prioritize: current performance, strongest change, top/low contributors, and concentration/risk signal.\n"
        "- Use professional business language (momentum, contribution, concentration, variance, efficiency, risk).\n"
        "- Mention direction and scale (absolute and percent) wherever possible.\n"
        "- Include one implication or action-oriented observation when supported by data.\n"
        "- Do not mention missing data, AI limitations, or generic disclaimers.\n"
        "- Keep each bullet under 34 words.\n"
    )


def _resolve_llm_model(*env_keys: str, default: str = DEFAULT_LLM_MODEL) -> str:
    for key in env_keys:
        if not key:
            continue
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _trim_chat_text(value: str, limit: int) -> str:
    compact = re.sub(r"\s+", " ", value or "").strip()
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit].rstrip()}..."


def _resolve_chatbot_num_predict(payload: ChatbotPayload) -> int:
    hard_cap = max(256, _env_int("CHATBOT_MAX_NUM_PREDICT", 4096))
    if payload.max_tokens is not None:
        return max(8, min(int(payload.max_tokens), hard_cap))

    message = _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT)
    small_prompt_chars = max(1, _env_int("CHATBOT_SMALL_PROMPT_CHARS", 180))
    medium_prompt_chars = max(small_prompt_chars + 1, _env_int("CHATBOT_MEDIUM_PROMPT_CHARS", 520))
    small_tokens = max(220, _env_int("CHATBOT_SMALL_NUM_PREDICT", 420))
    medium_tokens = max(420, _env_int("CHATBOT_MEDIUM_NUM_PREDICT", 900))
    large_tokens = max(760, _env_int("CHATBOT_LARGE_NUM_PREDICT", 1800))

    if len(message) <= small_prompt_chars:
        return min(small_tokens, hard_cap)
    if len(message) <= medium_prompt_chars:
        return min(medium_tokens, hard_cap)
    return min(large_tokens, hard_cap)


def _chatbot_cache_key(
    payload: ChatbotPayload,
    *,
    model: str,
    system_prompt: str,
    temperature: float,
    num_predict: int,
    context_fingerprint: str = "",
) -> str:
    history_signature: list[dict[str, str]] = []
    for turn in payload.history[-CHATBOT_HISTORY_LIMIT:]:
        role = (turn.role or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_chat_text(turn.content, CHATBOT_HISTORY_CHAR_LIMIT)
        if not content:
            continue
        history_signature.append({"role": role, "content": content})

    signature = {
        "model": model,
        "system_prompt": system_prompt.strip(),
        "context_fingerprint": context_fingerprint.strip(),
        "message": _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT),
        "history": history_signature,
        "temperature": round(float(temperature), 3),
        "num_predict": int(num_predict),
        "source": _normalize_source_key(payload.source or ""),
        "dataset_type": _normalize_dataset_type_for_chatbot(payload.dataset_type),
        "job_id": _normalize_chatbot_job_id(payload.job_id) or "",
        "from_date": _normalize_chatbot_date(payload.from_date) or "",
        "to_date": _normalize_chatbot_date(payload.to_date) or "",
    }
    raw = json.dumps(signature, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _chatbot_cache_get(cache_key: str) -> dict[str, str] | None:
    now = time.time()
    with _chatbot_cache_lock:
        cached = _chatbot_response_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _chatbot_response_cache.pop(cache_key, None)
            return None
        return payload


def _chatbot_cache_set(cache_key: str, payload: dict[str, str]) -> None:
    now = time.time()
    with _chatbot_cache_lock:
        if len(_chatbot_response_cache) >= CHATBOT_CACHE_MAX_ITEMS:
            expired = [key for key, (expires, _) in _chatbot_response_cache.items() if expires <= now]
            for key in expired:
                _chatbot_response_cache.pop(key, None)
            if len(_chatbot_response_cache) >= CHATBOT_CACHE_MAX_ITEMS and _chatbot_response_cache:
                oldest_key = min(_chatbot_response_cache, key=lambda key: _chatbot_response_cache[key][0])
                _chatbot_response_cache.pop(oldest_key, None)
        _chatbot_response_cache[cache_key] = (now + CHATBOT_CACHE_TTL_SECONDS, payload)


def _is_timeout_exception(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            return True
        if reason and "timed out" in str(reason).lower():
            return True
    return "timed out" in str(exc).lower() or "timeout" in str(exc).lower()


def _build_chatbot_prompt(payload: ChatbotPayload, dashboard_context: str) -> str:
    lines: list[str] = [
        "Analytics context (authoritative; includes dashboard + dataset-derived signals):",
        dashboard_context.strip(),
        "",
        "Conversation:",
    ]
    for turn in payload.history[-CHATBOT_HISTORY_LIMIT:]:
        role = (turn.role or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_chat_text(turn.content, CHATBOT_HISTORY_CHAR_LIMIT)
        if not content:
            continue
        role_label = "User" if role == "user" else "Assistant"
        lines.append(f"{role_label}: {content}")

    message = _trim_chat_text(payload.message, CHATBOT_MESSAGE_CHAR_LIMIT)
    lines.append("")
    lines.append("Response quality bar:")
    lines.append("1) Answer the user's question directly in the first sentence.")
    lines.append("2) Use concrete metrics, dates, and comparisons from context when available.")
    lines.append("3) Keep the answer structured, professional, and decision-oriented.")
    lines.append("4) If recommending actions, provide up to 3 prioritized next steps.")
    lines.append("5) Avoid repeating the same phrasing from prior assistant turns; vary sentence openings and structure.")
    lines.append(f"User: {message}")
    lines.append("Assistant:")
    return "\n".join(lines)


def _call_llm(
    system_prompt: str,
    prompt: str,
    *,
    model: str | None = None,
    temperature: float = 0.2,
    num_predict: int = 480,
    timeout_seconds: int | None = None,
    keep_alive: str | None = None,
    num_ctx: int | None = None,
    num_thread: int | None = None,
) -> tuple[str, str, dict[str, Any]]:
    resolved_model = (model or "").strip() or _resolve_llm_model("CHATBOT_MODEL", "CHATCARDS_MODEL", "SARVAM_MODEL")
    sarvam_url = os.getenv("SARVAM_API_URL", "https://api.sarvam.ai/v1/chat/completions").strip() or "https://api.sarvam.ai/v1/chat/completions"
    sarvam_api_key = os.getenv("SARVAM_API_KEY", "").strip()
    if not sarvam_api_key:
        raise ValueError("SARVAM_API_KEY is not configured.")

    resolved_timeout = timeout_seconds if timeout_seconds and timeout_seconds > 0 else _env_int("SARVAM_TIMEOUT_SECONDS", 70)
    resolved_max_tokens = max(8, int(num_predict))
    resolved_temperature = max(0.0, min(1.5, float(temperature)))
    _ = keep_alive, num_ctx, num_thread  # kept for backward-compatible call signatures

    body = {
        "model": resolved_model,
        "messages": [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": prompt.strip()},
        ],
        "temperature": resolved_temperature,
        "max_tokens": resolved_max_tokens,
        "stream": False,
    }
    raw = json.dumps(body).encode("utf-8")
    req = UrlRequest(
        sarvam_url,
        data=raw,
        headers={
            "Content-Type": "application/json",
            "api-subscription-key": sarvam_api_key,
        },
        method="POST",
    )

    with urlopen(req, timeout=resolved_timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("Sarvam response missing choices.")

        first_choice = choices[0] if isinstance(choices[0], dict) else {}
        message_obj = first_choice.get("message")
        if isinstance(message_obj, dict):
            response_text = str(message_obj.get("content") or "").strip()
        else:
            response_text = str(first_choice.get("text") or "").strip()

        if not response_text:
            raise ValueError("Empty LLM response.")

        usage_obj = payload.get("usage")
        completion_tokens = usage_obj.get("completion_tokens") if isinstance(usage_obj, dict) else None
        response_meta = {
            "done_reason": str(first_choice.get("finish_reason") or "").strip().lower(),
            "eval_count": completion_tokens,
            "provider": "sarvam",
        }
        return resolved_model, response_text, response_meta


def _looks_truncated_response(
    response_text: str,
    payload_meta: dict[str, Any],
    token_budget: int,
) -> bool:
    done_reason = str(payload_meta.get("done_reason") or "").strip().lower()
    if done_reason == "length":
        return True

    eval_count_raw = payload_meta.get("eval_count")
    try:
        eval_count = int(eval_count_raw) if eval_count_raw is not None else 0
    except (TypeError, ValueError):
        eval_count = 0

    likely_token_limited = eval_count >= max(1, token_budget - 1)
    if not likely_token_limited:
        return False

    tail = (response_text or "").strip()
    if not tail:
        return True
    if tail.endswith((".", "!", "?", "\"", "'", ".)", "!)", "?)")):
        return False
    return True


def _looks_incomplete_response(response_text: str) -> bool:
    text = (response_text or "").strip()
    if not text:
        return True
    if text.endswith((".", "!", "?", "\"", "'", ".)", "!)", "?)", "...")):
        return False

    if text.endswith((",", ":", ";", "-", "/")):
        return True
    if text.endswith(("(", "[", "{")):
        return True
    if re.search(r"\([^\)]*$", text):
        return True
    if re.search(r"\[[^\]]*$", text):
        return True
    if re.search(r"\{[^}]*$", text):
        return True
    if text.count("(") > text.count(")"):
        return True
    if text.count("[") > text.count("]"):
        return True
    if text.count("{") > text.count("}"):
        return True

    words = text.split()
    if not words:
        return True
    last_word = words[-1].strip(".,:;!?\"'()[]{}").lower()
    dangling_words = {
        "and", "or", "to", "for", "with", "about", "on", "in", "of", "the",
        "a", "an", "this", "that", "your", "our", "their", "better", "more",
        "some", "any", "if", "because", "while", "when", "then",
    }
    if last_word in dangling_words:
        return True

    return False


def _repair_incomplete_response(
    *,
    model_name: str,
    response_text: str,
    temperature: float,
    max_num_predict_cap: int,
    retry_timeout_seconds: int,
    keep_alive: str,
    num_ctx: int,
    num_thread: int,
) -> str:
    if not _looks_incomplete_response(response_text):
        return response_text

    try:
        _, repaired_text, _ = _call_llm(
            "You rewrite incomplete assistant answers into one complete response.",
            (
                "The response below ended mid-thought. Rewrite it as one complete, coherent "
                "answer with the same intent. Do not add new facts.\n\n"
                f"Incomplete response:\n{response_text}\n\nComplete response:"
            ),
            model=model_name,
            temperature=min(temperature, 0.12),
            num_predict=min(max_num_predict_cap, max(128, _env_int("CHATBOT_REPAIR_NUM_PREDICT", 640))),
            timeout_seconds=max(8, min(retry_timeout_seconds, 24)),
            keep_alive=keep_alive,
            num_ctx=num_ctx,
            num_thread=num_thread if num_thread > 0 else None,
        )
        repaired_text = repaired_text.strip()
        if repaired_text and len(repaired_text) >= max(24, len(response_text.strip()) // 2):
            return repaired_text
    except (URLError, TimeoutError, ValueError, OSError):
        pass

    return response_text


def _prewarm_llm_model() -> None:
    model_name = _resolve_llm_model("CHATBOT_MODEL", "CHATCARDS_MODEL", "SARVAM_MODEL")
    prewarm_timeout = max(10, _env_int("CHATBOT_PREWARM_TIMEOUT_SECONDS", 45))
    keep_alive = os.getenv("CHATBOT_KEEP_ALIVE", "").strip()
    num_ctx = max(512, _env_int("CHATBOT_NUM_CTX", 1024))
    num_thread = _env_int("CHATBOT_NUM_THREAD", 0)
    try:
        started_at = time.perf_counter()
        _call_llm(
            "You are a concise assistant.",
            "User: Reply with OK.\nAssistant:",
            model=model_name,
            temperature=0.0,
            num_predict=4,
            timeout_seconds=prewarm_timeout,
            keep_alive=keep_alive,
            num_ctx=num_ctx,
            num_thread=num_thread if num_thread > 0 else None,
        )
        duration_ms = (time.perf_counter() - started_at) * 1000
        logger.info("LLM prewarm complete model=%s duration_ms=%.2f", model_name, duration_ms)
    except Exception as exc:
        logger.warning("LLM prewarm failed: %s", exc)


@app.post("/insights/graph")
def generate_graph_insights(
    payload: GraphInsightPayload,
    db: Session = Depends(get_db),
):
    normalized_source = _normalize_source_key(payload.source)
    normalized_dataset = (payload.dataset_type or "").strip().lower()
    cached_db = get_precomputed_insights(
        db=db,
        source=normalized_source,
        dataset_type=normalized_dataset,
        job_id=payload.job_id,
        dimension=payload.dimension,
        metric=payload.metric,
        bucket=payload.bucket,
        compare_mode=payload.compare_mode,
        from_date=payload.from_date,
        to_date=payload.to_date,
    )
    if cached_db is not None:
        return cached_db

    insights_enabled = os.getenv("ENABLE_GRAPH_INSIGHTS", "1").strip().lower() not in {"0", "false", "no", "off"}
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
    insight_tokens = max(220, _env_int("CHATCARDS_NUM_PREDICT", 560))
    insight_timeout = max(12, _env_int("CHATCARDS_TIMEOUT_SECONDS", 55))
    insight_temperature = _env_float("CHATCARDS_TEMPERATURE", 0.2)

    try:
        model, response_text, _ = _call_llm(
            system_prompt,
            prompt,
            model=_resolve_llm_model("CHATCARDS_MODEL", "CHATBOT_MODEL", "SARVAM_MODEL"),
            temperature=insight_temperature,
            num_predict=insight_tokens,
            timeout_seconds=insight_timeout,
        )
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        logger.warning("Graph insights generation failed: %s", exc)
        if base_insights:
            response_payload = {
                "insights": base_insights[:5],
                "model": "rule-based",
                "message": "LLM insights unavailable; showing data-driven insights.",
            }
            _graph_insights_cache[cache_key] = (now + GRAPH_INSIGHTS_TTL_SECONDS, response_payload)
            try:
                upsert_precomputed_insights(
                    db=db,
                    source=normalized_source,
                    dataset_type=normalized_dataset,
                    job_id=payload.job_id,
                    dimension=payload.dimension,
                    metric=payload.metric,
                    bucket=payload.bucket,
                    compare_mode=payload.compare_mode,
                    from_date=payload.from_date,
                    to_date=payload.to_date,
                    insights=response_payload["insights"],
                    model=response_payload.get("model", "rule-based"),
                    message=response_payload.get("message"),
                )
                db.commit()
            except Exception:
                db.rollback()
                logger.exception("Failed to persist precomputed graph insights")
            return response_payload
        raise HTTPException(
            status_code=503,
            detail=(
                "Insights service unavailable. Ensure SARVAM_API_KEY is configured and SARVAM_MODEL is valid."
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
    try:
        upsert_precomputed_insights(
            db=db,
            source=normalized_source,
            dataset_type=normalized_dataset,
            job_id=payload.job_id,
            dimension=payload.dimension,
            metric=payload.metric,
            bucket=payload.bucket,
            compare_mode=payload.compare_mode,
            from_date=payload.from_date,
            to_date=payload.to_date,
            insights=response_payload["insights"],
            model=response_payload.get("model", "rule-based"),
            message=response_payload.get("message"),
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to persist precomputed graph insights")
    return response_payload


@app.post("/chatbot/message")
def chatbot_message(
    payload: ChatbotPayload,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    chatbot_enabled = os.getenv("ENABLE_CHATBOT", "1").strip().lower() not in {"0", "false", "no", "off"}
    if not chatbot_enabled:
        return {
            "response": "",
            "model": "disabled",
            "message": "Chatbot is disabled in this environment.",
        }

    if _is_chatbot_greeting(payload.message):
        return {
            "response": _build_chatbot_greeting_response(
                db=db,
                payload=payload,
            ),
            "model": "rule-based-greeting",
        }

    dashboard_context, context_payload = _build_chatbot_dashboard_context(db=db, payload=payload)
    claims_average_answer = _build_claim_average_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if claims_average_answer:
        return {
            "response": claims_average_answer,
            "model": "rule-based-claims-avg",
        }
    pricing_answer = _build_pricing_recommendation_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if pricing_answer:
        return {
            "response": pricing_answer,
            "model": "rule-based-pricing",
        }
    forecast_answer = _build_time_series_forecast_answer(
        db=db,
        payload=payload,
        context_payload=context_payload,
    )
    if forecast_answer:
        return {
            "response": forecast_answer,
            "model": "rule-based-forecast",
        }
    rule_based_answer = _build_underperformance_answer(payload.message, context_payload)
    if rule_based_answer:
        return {
            "response": rule_based_answer,
            "model": "rule-based-dashboard",
        }
    dimension_stats_answer = _build_dimension_stats_answer(payload.message, context_payload)
    if dimension_stats_answer:
        return {
            "response": dimension_stats_answer,
            "model": "rule-based-dashboard",
        }

    prompt = _build_chatbot_prompt(payload, dashboard_context)
    base_system_prompt = (
        (payload.system_prompt or "").strip()
        or os.getenv("CHATBOT_SYSTEM_PROMPT", "").strip()
        or DEFAULT_CHATBOT_SYSTEM_PROMPT
    )
    system_prompt = (
        f"{base_system_prompt}\n\n"
        "Hard constraints:\n"
        "1) Use the Analytics context block and conversation turns as primary evidence.\n"
        "2) Never invent entities or numbers that are not supported by available context.\n"
        "3) If key context is missing, state the gap and provide the closest defensible answer with explicit assumptions.\n"
        "4) Give a direct answer first, then supporting evidence and implications.\n"
        "5) Prefer precise metrics and avoid generic statements.\n"
        "6) Do not re-introduce AI Sahyogi unless the user explicitly asks.\n"
        "7) End with a complete final sentence and close any opened bracket.\n"
        "8) For forecasting questions, estimate next-month values only from monthly history in context and mark it as directional.\n"
        "9) Avoid repetitive templates across turns; vary phrasing while keeping the answer concise and factual.\n"
        "10) If Samsung model codes appear (A06/F15/A16/A17/F17/A26/A35/A36/F55/A56/S24/S25/Fold6/Fold7/Flip7), use the mapping provided in context to infer device category.\n"
        "11) Use Samsung plan abbreviations consistently: ADLD=Accidental Damage and Liquid Damage, EW=Extended Warranty, SP/SPP=Screen Protection Plan, CPP=Comprehensive Protection Plan, Combo=ADLD + EW.\n"
    )
    model_name = _resolve_llm_model("CHATBOT_MODEL", "CHATCARDS_MODEL", "SARVAM_MODEL")
    temperature = payload.temperature if payload.temperature is not None else _env_float("CHATBOT_TEMPERATURE", 0.15)
    max_tokens = _resolve_chatbot_num_predict(payload)
    timeout_seconds = max(12, _env_int("CHATBOT_TIMEOUT_SECONDS", 65))
    retry_timeout_seconds = max(8, min(timeout_seconds, _env_int("CHATBOT_RETRY_TIMEOUT_SECONDS", 30)))
    retry_num_predict = max(128, min(max_tokens, _env_int("CHATBOT_RETRY_NUM_PREDICT", 640)))
    max_num_predict_cap = max(max_tokens, _env_int("CHATBOT_MAX_NUM_PREDICT", 4096))
    keep_alive = os.getenv("CHATBOT_KEEP_ALIVE", "").strip()
    num_ctx = max(512, _env_int("CHATBOT_NUM_CTX", 1024))
    num_thread = _env_int("CHATBOT_NUM_THREAD", 0)
    context_fingerprint = hashlib.sha256(dashboard_context.encode("utf-8")).hexdigest()
    cache_key = _chatbot_cache_key(
        payload,
        model=model_name,
        system_prompt=system_prompt,
        temperature=temperature,
        num_predict=max_tokens,
        context_fingerprint=context_fingerprint,
    )
    cached = _chatbot_cache_get(cache_key)
    if cached is not None:
        return cached
    started_at = time.perf_counter()

    try:
        model, response_text, response_meta = _call_llm(
            system_prompt,
            prompt,
            model=model_name,
            temperature=temperature,
            num_predict=max_tokens,
            timeout_seconds=timeout_seconds,
            keep_alive=keep_alive,
            num_ctx=num_ctx,
            num_thread=num_thread if num_thread > 0 else None,
        )
        needs_expansion = (
            _looks_truncated_response(response_text, response_meta, max_tokens)
            or _looks_incomplete_response(response_text)
        )
        elapsed_seconds = time.perf_counter() - started_at
        remaining_budget = max(0.0, float(timeout_seconds) - elapsed_seconds)
        allow_expansion = max_tokens > 160 and remaining_budget >= 10.0
        if needs_expansion and max_tokens < max_num_predict_cap and allow_expansion:
            expanded_tokens = min(max_num_predict_cap, max_tokens + max(256, max_tokens // 2))
            try:
                expansion_timeout = max(8, min(retry_timeout_seconds, max(10, int(remaining_budget))))
                model, expanded_text, _ = _call_llm(
                    system_prompt,
                    prompt,
                    model=model_name,
                    temperature=temperature,
                    num_predict=expanded_tokens,
                    timeout_seconds=expansion_timeout,
                    keep_alive=keep_alive,
                    num_ctx=num_ctx,
                    num_thread=num_thread if num_thread > 0 else None,
                )
                if expanded_text and len(expanded_text) >= len(response_text):
                    response_text = expanded_text
                    max_tokens = expanded_tokens
                if _looks_incomplete_response(response_text):
                    response_text = _repair_incomplete_response(
                        model_name=model_name,
                        response_text=response_text,
                        temperature=temperature,
                        max_num_predict_cap=max_num_predict_cap,
                        retry_timeout_seconds=retry_timeout_seconds,
                        keep_alive=keep_alive,
                        num_ctx=num_ctx,
                        num_thread=num_thread,
                    )
            except (URLError, TimeoutError, ValueError, OSError):
                response_text = _repair_incomplete_response(
                    model_name=model_name,
                    response_text=response_text,
                    temperature=temperature,
                    max_num_predict_cap=max_num_predict_cap,
                    retry_timeout_seconds=retry_timeout_seconds,
                    keep_alive=keep_alive,
                    num_ctx=num_ctx,
                    num_thread=num_thread,
                )
        else:
            response_text = _repair_incomplete_response(
                model_name=model_name,
                response_text=response_text,
                temperature=temperature,
                max_num_predict_cap=max_num_predict_cap,
                retry_timeout_seconds=retry_timeout_seconds,
                keep_alive=keep_alive,
                num_ctx=num_ctx,
                num_thread=num_thread,
            )
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        if _is_timeout_exception(exc):
            try:
                model, response_text, _ = _call_llm(
                    system_prompt,
                    prompt,
                    model=model_name,
                    temperature=min(temperature, 0.12),
                    num_predict=retry_num_predict,
                    timeout_seconds=retry_timeout_seconds,
                    keep_alive=keep_alive,
                    num_ctx=num_ctx,
                    num_thread=num_thread if num_thread > 0 else None,
                )
            except (URLError, TimeoutError, ValueError, OSError) as retry_exc:
                logger.warning("Chatbot generation failed after timeout retry: %s", retry_exc)
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Chatbot service unavailable. Ensure SARVAM_API_KEY is configured and SARVAM_MODEL is valid."
                    ),
                )
        else:
            logger.warning("Chatbot generation failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail=(
                    "Chatbot service unavailable. Ensure SARVAM_API_KEY is configured and SARVAM_MODEL is valid."
                ),
            )

    if _looks_incomplete_response(response_text):
        response_text = _repair_incomplete_response(
            model_name=model_name,
            response_text=response_text,
            temperature=temperature,
            max_num_predict_cap=max_num_predict_cap,
            retry_timeout_seconds=retry_timeout_seconds,
            keep_alive=keep_alive,
            num_ctx=num_ctx,
            num_thread=num_thread,
        )

    response_payload = {
        "response": response_text,
        "model": model,
    }
    _chatbot_cache_set(cache_key, response_payload)
    duration_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "TIMING chatbot.message model=%s tokens=%s duration_ms=%.2f",
        model,
        max_tokens,
        duration_ms,
    )
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
