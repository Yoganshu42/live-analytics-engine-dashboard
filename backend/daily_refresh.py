from __future__ import annotations

import argparse
import json
import logging

from db.session import SessionLocal
from services.maintenance_service import run_daily_refresh

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild daily dashboard analytics caches.")
    parser.add_argument("--source", default=None)
    parser.add_argument("--dataset-type", default=None)
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--repair-reliance-brands", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = run_daily_refresh(
            db=db,
            source=args.source,
            dataset_type=args.dataset_type,
            job_id=args.job_id,
            repair_reliance_brands=bool(args.repair_reliance_brands),
        )
        print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception:
        db.rollback()
        logger.exception("Daily refresh failed")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
