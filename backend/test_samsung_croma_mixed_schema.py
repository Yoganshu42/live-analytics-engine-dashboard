from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.analytics.samsung_engine import SamsungAnalyticsEngine


class StubSamsungEngine(SamsungAnalyticsEngine):
    def __init__(self, sales_df: pd.DataFrame):
        super().__init__(
            db=None,
            job_id="01",
            source="samsung_croma",
            dataset_type="sales",
        )
        self._sales_df = sales_df

    def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
        sales = self._sales_df.copy() if include_sales else pd.DataFrame()
        claims = pd.DataFrame()
        return {"sales": sales, "claims": claims}


def test_croma_mixed_schema_keeps_legacy_amount_rows_in_premium_metrics():
    sales_df = pd.DataFrame(
        {
            "Month": ["7/25/2026", "2026-02-01T00:00:00"],
            "Date": ["2025-07-31T00:00:00", "2026-02-28T00:00:00"],
            "Start_Date": ["2025-07-31T00:00:00", "2026-02-28T00:00:00"],
            "End_Date": ["2026-07-31T00:00:00", "2027-02-28T00:00:00"],
            "Amount": [100.0, None],
            "Plan Selling Price": [None, 200.0],
            "Zopper Share": [40.0, 60.0],
            "earned_days": [365, 365],
            "policy_days": [365, 365],
        }
    )
    engine = StubSamsungEngine(sales_df)

    gross_rows = engine.compute_by_dimension("month", "gross_premium")
    assert gross_rows == [
        {"gross_premium": 100.0, "Month": "Jul-25"},
        {"gross_premium": 200.0, "Month": "Feb-26"},
    ]

    summary = engine.compute_summary()
    assert summary["gross_premium"] == 300.0
    assert summary["earned_premium"] == 300.0
