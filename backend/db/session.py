import os
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()


def _normalize_database_url(raw_url: str) -> str:
    cleaned = re.sub(r"\s+", "", (raw_url or "").strip())
    if cleaned.startswith("postgres://"):
        cleaned = "postgresql://" + cleaned[len("postgres://") :]
    return cleaned


def _needs_ssl(url: str) -> bool:
    return ".rds.amazonaws.com" in url or ".aws.neon.tech" in url


DATABASE_URL = _normalize_database_url(
    os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:tech%401234@localhost:5432/analytics_db",
    )
)

engine_kwargs = {
    "pool_pre_ping": True,
}

if DATABASE_URL.startswith("postgresql"):
    engine_kwargs.update(
        {
            "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "1800")),
            "pool_size": int(os.getenv("DB_POOL_SIZE", "5")),
            "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "10")),
        }
    )

    if _needs_ssl(DATABASE_URL) and "sslmode=" not in DATABASE_URL:
        engine_kwargs["connect_args"] = {"sslmode": "require"}

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
