
import sys
import os
sys.path.append(os.getcwd())

from db.session import SessionLocal
from routers.analytics import compute_by_dimension_rows
import json

def test():
    db = SessionLocal()
    try:
        # Test 1: Reason by Claims (Amount)
        rows_claims = compute_by_dimension_rows(
            db=db,
            job_id=None,
            dimension="reason",
            metric="claims",
            source="hitachi",
            dataset_type="claims",
            from_date=None,
            to_date=None
        )
        print("--- Reason by Claims (Amount) ---")
        print(json.dumps(rows_claims[:5], indent=2))

        # Test 2: Reason by Quantity (Count)
        rows_qty = compute_by_dimension_rows(
            db=db,
            job_id=None,
            dimension="reason",
            metric="quantity",
            source="hitachi",
            dataset_type="claims",
            from_date=None,
            to_date=None
        )
        print("\n--- Reason by Quantity (Count) ---")
        print(json.dumps(rows_qty[:5], indent=2))

    finally:
        db.close()

if __name__ == "__main__":
    test()
