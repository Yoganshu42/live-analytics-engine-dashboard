from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import routers.analytics as analytics_router
from routers.analytics import _resolve_job_id_fallback
from services.analytics_repository import get_dataframe, invalidate_dataframe_cache
from services.analytics.samsung_engine import SamsungAnalyticsEngine, invalidate_samsung_load_cache
from services.date_parsing import resolve_samsung_plan_window


def test_samsung_partner_sources_use_merged_history_when_job_id_is_empty():
    assert _resolve_job_id_fallback(
        db=None,
        resolved_source="samsung_vs",
        dataset_key="sales",
        job_id=None,
        context="summary",
    ) is None
    assert _resolve_job_id_fallback(
        db=None,
        resolved_source="samsung_croma",
        dataset_key="sales",
        job_id="",
        context="summary",
    ) is None


def test_resolve_samsung_plan_window_derives_end_dates_for_placeholder_windows():
    combo_start, combo_end = resolve_samsung_plan_window(
        plan_type="Combo",
        start_value="2026-03-01",
        end_value="2026-03-01",
        transaction_value="2026-03-01",
    )
    adld_start, adld_end = resolve_samsung_plan_window(
        plan_type="ADLD",
        start_value="2026-02-01",
        end_value="",
        transaction_value="2026-02-01",
    )

    assert combo_start == pd.Timestamp("2026-03-01")
    assert combo_end == pd.Timestamp("2028-02-29")
    assert adld_start == pd.Timestamp("2026-02-01")
    assert adld_end == pd.Timestamp("2027-01-31")


def test_samsung_load_data_replaces_same_day_and_missing_end_dates(monkeypatch):
    sales_df = pd.DataFrame(
        {
            "Date": ["2026-02-01T00:00:00", "2026-03-01T00:00:00"],
            "Month": ["2026-02-01T00:00:00", "2026-03-01T00:00:00"],
            "Plan Category": ["ADLD", "Combo"],
            "Device Plan Category": ["Mid", "Mid"],
            "Start_Date": ["2026-02-01T00:00:00", "2026-03-01T00:00:00"],
            "End_Date": ["2026-02-01T00:00:00", None],
            "Plan Selling Price": [2249.0, 4199.0],
            "Zopper Share": [563.0, 806.0],
        }
    )

    def fake_get_dataframe(*, db, job_id, source, dataset_type, cache_result=True):
        if dataset_type == "sales":
            return sales_df.copy()
        return pd.DataFrame()

    monkeypatch.setattr("services.analytics.samsung_engine.get_dataframe", fake_get_dataframe)
    invalidate_samsung_load_cache(source="samsung_croma", dataset_type="sales", job_id="upload_fix_test")

    engine = SamsungAnalyticsEngine(
        db=None,
        job_id="upload_fix_test",
        source="samsung_croma",
        dataset_type="sales",
    )
    normalized = engine.load_data(include_sales=True, include_claims=False)["sales"]

    assert normalized.loc[0, "End_Date"] == pd.Timestamp("2027-01-31")
    assert normalized.loc[1, "End_Date"] == pd.Timestamp("2028-02-29")


def test_earned_premium_falls_back_to_dates_when_earned_days_are_blank_for_new_rows():
    class StubSamsungEngine(SamsungAnalyticsEngine):
        def __init__(self, sales_df: pd.DataFrame):
            super().__init__(
                db=None,
                job_id="rowwise_earned_fallback",
                source="samsung_croma",
                dataset_type="sales",
                from_date="2026-03-01",
                to_date="2026-03-31",
            )
            self._sales_df = sales_df

        def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
            sales = self._sales_df.copy() if include_sales else pd.DataFrame()
            claims = pd.DataFrame()
            return {"sales": sales, "claims": claims}

    sales_df = pd.DataFrame(
        {
            "Date": ["2026-03-01T00:00:00"],
            "Month": ["2026-03-01T00:00:00"],
            "Start_Date": ["2026-03-01T00:00:00"],
            "End_Date": ["2026-03-31T00:00:00"],
            "Plan Selling Price": [2400.0],
            "earned_days": [None],
            "policy_days": [None],
        }
    )

    engine = StubSamsungEngine(sales_df)
    earned_rows = engine.compute_by_dimension("month", "earned_premium")
    earned_by_month = {row["Month"]: row["earned_premium"] for row in earned_rows}

    assert earned_by_month["Mar-26"] == 2400.0


def test_samsung_earned_metrics_use_overlap_window_and_live_dates_over_stale_earned_days():
    class StubSamsungEngine(SamsungAnalyticsEngine):
        def __init__(self, sales_df: pd.DataFrame):
            super().__init__(
                db=None,
                job_id="overlap_live_accrual",
                source="samsung_croma",
                dataset_type="sales",
                from_date="2025-02-01",
                to_date="2025-02-28",
            )
            self._sales_df = sales_df

        def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
            sales = self._sales_df.copy() if include_sales else pd.DataFrame()
            claims = pd.DataFrame()
            return {"sales": sales, "claims": claims}

    sales_df = pd.DataFrame(
        {
            "Date": ["2025-01-15T00:00:00"],
            "Month": ["2025-01-15T00:00:00"],
            "Start_Date": ["2025-01-15T00:00:00"],
            "End_Date": ["2025-02-14T00:00:00"],
            "Plan Selling Price": [310.0],
            "Zopper Share": [62.0],
            "earned_days": [5],
            "policy_days": [31],
        }
    )

    engine = StubSamsungEngine(sales_df)

    earned_rows = engine.compute_by_dimension("month", "earned_premium")
    zopper_rows = engine.compute_by_dimension("month", "zopper_earned_premium")
    summary = engine.compute_summary()

    assert earned_rows == [{"Month": "Feb-25", "earned_premium": 140.0}]
    assert zopper_rows == [{"Month": "Feb-25", "zopper_earned_premium": 28.0}]
    assert summary["earned_premium"] == 140.0
    assert summary["zopper_earned_premium"] == 28.0


def test_samsung_temporal_accrual_fallback_handles_invalid_rows_without_series_bool_error():
    class StubSamsungEngine(SamsungAnalyticsEngine):
        def __init__(self, sales_df: pd.DataFrame):
            super().__init__(
                db=None,
                job_id="fallback_invalid_row",
                source="samsung_croma",
                dataset_type="sales",
                from_date="2026-04-01",
                to_date="2026-04-07",
            )
            self._sales_df = sales_df

        def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
            sales = self._sales_df.copy() if include_sales else pd.DataFrame()
            claims = pd.DataFrame()
            return {"sales": sales, "claims": claims}

    sales_df = pd.DataFrame(
        {
            "Date": ["2026-04-01T00:00:00", "2026-04-05T00:00:00"],
            "Month": ["2026-04-01T00:00:00", "2026-04-05T00:00:00"],
            "Start_Date": ["2026-04-01T00:00:00", "2026-04-05T00:00:00"],
            "End_Date": ["2026-04-01T00:00:00", None],
            "Plan Selling Price": [1.0, 1000.0],
            "Earned Premium": [1.0, 125.0],
            "Zopper Share": [1.0, 300.0],
            "Zopper Earned Premium": [1.0, 35.0],
        }
    )

    engine = StubSamsungEngine(sales_df)

    earned_rows = engine.compute_by_dimension("month", "earned_premium")
    zopper_rows = engine.compute_by_dimension("month", "zopper_earned_premium")

    assert earned_rows == [{"Month": "Apr-26", "earned_premium": 1.0}]
    assert zopper_rows == [{"Month": "Apr-26", "zopper_earned_premium": 1.0}]


def test_unscoped_samsung_reliance_digital_uses_record_key_dedup_query():
    class FakeResult:
        def fetchall(self):
            return []

    class FakeDB:
        def __init__(self):
            self.statement = ""
            self.params: dict[str, str] = {}

        def execute(self, stmt, params):
            self.statement = str(stmt)
            self.params = dict(params)
            return FakeResult()

    invalidate_dataframe_cache(source="samsung_reliance_digital", dataset_type="sales", job_id=None)
    db = FakeDB()

    frame = get_dataframe(
        db=db,
        job_id=None,
        source="reliance digital",
        dataset_type="sales",
    )

    assert frame.empty
    assert "ROW_NUMBER() OVER" in db.statement
    assert "PARTITION BY COALESCE(record_key, CONCAT('__row__', id::text))" in db.statement
    assert db.params["source_0"] == "samsung_reliance_digital"


def test_annual_comparison_sums_samsung_partner_rows_for_sales(monkeypatch):
    class FakeDB:
        def commit(self):
            return None

        def rollback(self):
            return None

    def fake_compute_by_dimension_rows(
        *,
        db,
        job_id,
        dimension,
        metric,
        source,
        dataset_type,
        bucket=None,
        from_date=None,
        to_date=None,
        category_filters=None,
    ):
        assert source == "samsung"
        assert dataset_type == "sales"

        if dimension == "plan_category" and metric == "quantity":
            return [
                {"plan_category": "Combo", "samsung_vs": 1.0, "samsung_croma": 0.0, "samsung_reliance_digital": 1.0},
                {"plan_category": "ADLD", "samsung_vs": 2.0, "samsung_croma": 1.0, "samsung_reliance_digital": 0.0},
            ]

        if dimension == "month" and metric == "quantity" and category_filters:
            plan = category_filters[0]["values"][0]
            if plan == "Combo":
                return [
                    {"month": "Mar-25", "samsung_vs": 1.0, "samsung_croma": 0.0, "samsung_reliance_digital": 0.0},
                    {"month": "Mar-26", "samsung_vs": 1.0, "samsung_croma": 0.0, "samsung_reliance_digital": 1.0},
                ]
            if plan == "ADLD":
                return [
                    {"month": "Feb-25", "samsung_vs": 2.0, "samsung_croma": 0.0, "samsung_reliance_digital": 0.0},
                    {"month": "Mar-26", "samsung_vs": 3.0, "samsung_croma": 1.0, "samsung_reliance_digital": 0.0},
                ]

        if dimension == "month" and metric == "gross_premium":
            return [
                {"month": "Feb-25", "samsung_vs": 700.0, "samsung_croma": 0.0, "samsung_reliance_digital": 0.0},
                {"month": "Mar-26", "samsung_vs": 3000.0, "samsung_croma": 5000.0, "samsung_reliance_digital": 214.0},
            ]

        if dimension == "month" and metric == "earned_premium":
            return [
                {"month": "Feb-25", "samsung_vs": 500.0, "samsung_croma": 0.0, "samsung_reliance_digital": 0.0},
                {"month": "Mar-26", "samsung_vs": 1000.0, "samsung_croma": 2000.0, "samsung_reliance_digital": 214.0},
            ]

        if dimension == "month" and metric == "zopper_earned_premium":
            return [
                {"month": "Feb-25", "samsung_vs": 50.0, "samsung_croma": 0.0, "samsung_reliance_digital": 0.0},
                {"month": "Mar-26", "samsung_vs": 100.0, "samsung_croma": 300.0, "samsung_reliance_digital": 44.0},
            ]

        return []

    monkeypatch.setattr(analytics_router, "_resolve_job_id_fallback", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "get_precomputed_graph", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "upsert_precomputed_graph", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "compute_by_dimension_rows", fake_compute_by_dimension_rows)
    monkeypatch.setitem(analytics_router.ENGINE_REGISTRY, "samsung", None)

    response = analytics_router.analytics_annual_comparison(
        db=FakeDB(),
        source="samsung",
        dataset_type="sales",
        metric=None,
        job_id=None,
        from_date="2025-02-01",
        to_date="2026-04-07",
    )

    quantity_rows = response["payload_by_metric"]["quantity"]["rows"]
    earned_rows = response["payload_by_metric"]["earned_premium"]["rows"]
    zopper_rows = response["payload_by_metric"]["zopper_earned_premium"]["rows"]

    assert quantity_rows == [
        {"label": "2024 - 2025", "total": 3.0, "values": {"Combo": 1.0, "ADLD": 2.0}},
        {"label": "2025 - 2026", "total": 6.0, "values": {"Combo": 2.0, "ADLD": 4.0}},
        {"label": "2026 - 2027", "total": 0.0, "values": {}},
    ]
    assert earned_rows == [
        {"label": "2024 - 2025", "total": 500.0, "values": {}},
        {"label": "2025 - 2026", "total": 3214.0, "values": {}},
        {"label": "2026 - 2027", "total": 0.0, "values": {}},
    ]
    assert zopper_rows == [
        {"label": "2024 - 2025", "total": 50.0, "values": {}},
        {"label": "2025 - 2026", "total": 444.0, "values": {}},
        {"label": "2026 - 2027", "total": 0.0, "values": {}},
    ]


def test_compute_date_bounds_payload_extends_samsung_sales_to_today(monkeypatch):
    today_iso = pd.Timestamp.now().normalize().date().isoformat()

    monkeypatch.setattr(
        analytics_router,
        "_date_bounds_from_precomputed_month_graphs",
        lambda **kwargs: (pd.Timestamp("2025-02-01"), pd.Timestamp("2026-03-31")),
    )

    payload = analytics_router.compute_date_bounds_payload(
        db=None,
        source="samsung",
        dataset_type="sales",
        job_id=None,
    )

    assert payload == {
        "min_date": "2025-02-01",
        "max_date": today_iso,
    }


def test_analytics_date_bounds_bypasses_cached_samsung_sales_bounds(monkeypatch):
    class FakeDB:
        def commit(self):
            return None

        def rollback(self):
            return None

    today_iso = pd.Timestamp.now().normalize().date().isoformat()
    live_payload = {
        "min_date": "2025-02-01",
        "max_date": today_iso,
    }

    monkeypatch.setattr(analytics_router, "_resolve_job_id_fallback", lambda **kwargs: None)
    monkeypatch.setattr(
        analytics_router,
        "get_precomputed_summary",
        lambda **kwargs: {"min_date": "2025-02-01", "max_date": "2026-03-31"},
    )
    monkeypatch.setattr(
        analytics_router,
        "_get_date_bounds_cache_updated_at",
        lambda **kwargs: pd.Timestamp("2026-04-07"),
    )
    monkeypatch.setattr(
        analytics_router,
        "_latest_date_bounds_marker_updated_at",
        lambda **kwargs: pd.Timestamp("2026-04-06"),
    )
    monkeypatch.setattr(analytics_router, "compute_date_bounds_payload", lambda **kwargs: live_payload)
    monkeypatch.setattr(analytics_router, "upsert_precomputed_summary", lambda **kwargs: None)

    response = analytics_router.analytics_date_bounds(
        db=FakeDB(),
        source="samsung",
        dataset_type="sales",
        job_id=None,
    )

    assert response == live_payload


def test_analytics_summary_forces_live_samsung_sales_recompute(monkeypatch):
    class FakeDB:
        def commit(self):
            return None

        def rollback(self):
            return None

    live_summary = {
        "gross_premium": 1234.0,
        "earned_premium": 567.0,
        "zopper_earned_premium": 89.0,
        "units_sold": 10,
    }

    monkeypatch.setattr(analytics_router, "_resolve_job_id_fallback", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "get_precomputed_summary", lambda **kwargs: {"gross_premium": 0.0, "earned_premium": 0.0, "zopper_earned_premium": 0.0, "units_sold": 0})
    monkeypatch.setattr(analytics_router, "_get_summary_cache_updated_at", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "_latest_manual_update_marker_updated_at", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "_latest_cache_marker_updated_at", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "_is_precomputed_cache_fresh", lambda **kwargs: True)
    monkeypatch.setattr(analytics_router, "compute_summary_values", lambda **kwargs: live_summary)
    monkeypatch.setattr(analytics_router, "upsert_precomputed_summary", lambda **kwargs: None)

    response = analytics_router.analytics_summary(
        db=FakeDB(),
        source="samsung",
        dataset_type="sales",
        job_id=None,
        from_date="2025-02-01",
        to_date="2026-04-07",
    )

    assert response == live_summary


def test_analytics_by_dimension_bypasses_cached_samsung_earned_graph(monkeypatch):
    class FakeDB:
        def commit(self):
            return None

        def rollback(self):
            return None

    live_rows = [{"month": "Apr-26", "samsung_vs": 1.0, "samsung_croma": 2.0, "samsung_reliance_digital": 3.0}]

    monkeypatch.setattr(analytics_router, "_resolve_job_id_fallback", lambda **kwargs: None)
    monkeypatch.setattr(analytics_router, "get_precomputed_graph", lambda **kwargs: [{"month": "Mar-26", "samsung_vs": 9.0, "samsung_croma": 9.0, "samsung_reliance_digital": 9.0}])
    monkeypatch.setattr(analytics_router, "compute_by_dimension_rows", lambda **kwargs: live_rows)
    monkeypatch.setattr(analytics_router, "upsert_precomputed_graph", lambda **kwargs: None)

    response = analytics_router.analytics_by_dimension(
        db=FakeDB(),
        source="samsung",
        dataset_type="sales",
        dimension="month",
        metric="earned_premium",
        bucket="month",
        job_id=None,
        from_date="2025-02-01",
        to_date="2026-04-07",
        filter_1_dimension=None,
        filter_1_values=None,
        filter_2_dimension=None,
        filter_2_values=None,
    )

    assert response == live_rows
