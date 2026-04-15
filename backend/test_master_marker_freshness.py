from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from routers import admin_files
from routers import analytics
from services import precompute_service


class _QueryStub:
    def __init__(self):
        self.filter_calls: list[tuple] = []

    def filter(self, *args):
        self.filter_calls.append(args)
        return self

    def scalar(self):
        return None


class _DbStub:
    def __init__(self):
        self.query_stub = _QueryStub()

    def query(self, *args):
        return self.query_stub


def test_master_marker_freshness_checks_all_jobs_for_unscoped_cache():
    db = _DbStub()

    analytics._latest_master_marker_updated_at(db=db, job_id=None)

    assert len(db.query_stub.filter_calls) == 2


def test_master_marker_freshness_keeps_job_filter_for_scoped_cache():
    db = _DbStub()

    analytics._latest_master_marker_updated_at(db=db, job_id="02")

    assert len(db.query_stub.filter_calls) == 3


class _DistinctQueryStub:
    def __init__(self, rows):
        self.rows = rows
        self.filter_calls: list[tuple] = []

    def distinct(self):
        return self

    def filter(self, *args):
        self.filter_calls.append(args)
        return self

    def all(self):
        return list(self.rows)


class _PrecomputeDbStub:
    def __init__(self):
        self.tag_query = _DistinctQueryStub(
            [
                ("samsung_vs", "sales", None),
                ("samsung_croma", "sales", None),
            ]
        )
        self.aggregate_query = _DistinctQueryStub(
            [
                ("samsung_vs", "sales"),
                ("samsung_croma", "sales"),
            ]
        )

    def query(self, *args):
        return self.tag_query if len(args) == 3 else self.aggregate_query


def test_samsung_precompute_rebuild_includes_aggregate_source(monkeypatch):
    db = _PrecomputeDbStub()
    rebuilt: list[tuple[str, str, str | None]] = []

    def fake_rebuild_precomputed_analytics(*, db, source, dataset_type, job_id=None):
        rebuilt.append((source, dataset_type, job_id))

    monkeypatch.setattr(precompute_service, "rebuild_precomputed_analytics", fake_rebuild_precomputed_analytics)

    result = precompute_service.rebuild_precomputed_for_all_tags(
        db=db,
        source="samsung",
        dataset_type="sales",
        job_id=None,
    )

    assert ("samsung", "sales", None) in rebuilt
    assert result["tags_completed"] == len(rebuilt)


class _AggregateOnlyDbStub:
    def __init__(self):
        self.tag_query = _DistinctQueryStub(
            [
                ("reliance", "sales", "batch_01"),
                ("reliance", "sales", "batch_02"),
            ]
        )
        self.aggregate_query = _DistinctQueryStub(
            [
                ("reliance", "sales"),
                ("godrej", "claims"),
            ]
        )

    def query(self, *args):
        return self.tag_query if len(args) == 3 else self.aggregate_query


def test_precompute_rebuild_can_skip_tagged_jobs(monkeypatch):
    db = _AggregateOnlyDbStub()
    rebuilt: list[tuple[str, str, str | None]] = []

    def fake_rebuild_precomputed_analytics(*, db, source, dataset_type, job_id=None):
        rebuilt.append((source, dataset_type, job_id))

    monkeypatch.setattr(precompute_service, "rebuild_precomputed_analytics", fake_rebuild_precomputed_analytics)

    result = precompute_service.rebuild_precomputed_for_all_tags(
        db=db,
        include_tagged_jobs=False,
    )

    assert rebuilt == [
        ("reliance", "sales", None),
        ("godrej", "claims", None),
    ]
    assert result == {
        "tags_found": 2,
        "tags_completed": 2,
    }


def test_master_bounds_use_month_end_and_cap_future_dates(monkeypatch):
    monkeypatch.setattr(analytics, "_current_month_cap", lambda: pd.Timestamp("2026-03-31"))

    min_date, max_date = analytics._bounds_from_master_rows(
        [
            [{"Month": "Apr-22", "gross_premium": 1}],
            [{"Month": "Dec-29", "gross_premium": 2}],
        ]
    )
    final_min, final_max = analytics._finalize_master_date_bounds(min_date, max_date)

    assert min_date == "2022-04-01"
    assert max_date == "2029-12-31"
    assert final_min == "2022-04-01"
    assert final_max == "2026-03-31"


def test_master_bounds_extend_default_max_to_today(monkeypatch):
    monkeypatch.setattr(analytics, "_current_month_cap", lambda: pd.Timestamp("2026-04-07"))

    final_min, final_max = analytics._finalize_master_date_bounds("2025-02-01", "2026-03-31")

    assert final_min == "2025-02-01"
    assert final_max == "2026-04-07"


def test_master_hitachi_sales_summary_uses_precomputed_cache(monkeypatch):
    sentinel = {"gross_premium": 123.0}

    monkeypatch.setattr(analytics, "get_precomputed_summary", lambda **kwargs: sentinel)
    monkeypatch.setattr(
        analytics,
        "_get_summary_cache_updated_at",
        lambda **kwargs: pd.Timestamp("2026-04-07 10:00:00"),
    )
    monkeypatch.setattr(
        analytics,
        "_latest_cache_marker_updated_at",
        lambda **kwargs: pd.Timestamp("2026-04-07 09:00:00"),
    )

    def fail_compute_summary_values(**kwargs):
        raise AssertionError("master summary should not recompute hitachi sales live when cache exists")

    monkeypatch.setattr(analytics, "compute_summary_values", fail_compute_summary_values)

    summary, job_id = analytics._load_master_summary(
        db=None,
        source="hitachi",
        dataset_type="sales",
        candidate_job_ids=[None],
        from_date=None,
        to_date=None,
    )

    assert summary is sentinel
    assert job_id is None


def test_master_samsung_sales_summary_bypasses_precomputed_cache(monkeypatch):
    live_summary = {
        "gross_premium": 1000.0,
        "earned_premium": 250.0,
        "zopper_earned_premium": 80.0,
        "units_sold": 5,
    }

    def fail_get_precomputed_summary(**kwargs):
        raise AssertionError("master samsung sales summary should recompute live")

    monkeypatch.setattr(analytics, "get_precomputed_summary", fail_get_precomputed_summary)
    monkeypatch.setattr(analytics, "compute_summary_values", lambda **kwargs: live_summary)
    monkeypatch.setattr(analytics, "upsert_precomputed_summary", lambda **kwargs: None)

    summary, job_id = analytics._load_master_summary(
        db=None,
        source="samsung_croma",
        dataset_type="sales",
        candidate_job_ids=[None],
        from_date="2025-02-01",
        to_date="2026-04-07",
    )

    assert summary == live_summary
    assert job_id is None


def test_master_samsung_earned_rows_bypass_precomputed_graph(monkeypatch):
    live_rows = [{"month": "Apr-26", "earned_premium": 123.0}]

    def fail_get_precomputed_graph(**kwargs):
        raise AssertionError("master samsung earned graph should recompute live")

    monkeypatch.setattr(analytics, "get_precomputed_graph", fail_get_precomputed_graph)
    monkeypatch.setattr(analytics, "compute_by_dimension_rows", lambda **kwargs: live_rows)
    monkeypatch.setattr(analytics, "upsert_precomputed_graph", lambda **kwargs: None)

    rows = analytics._load_master_metric_rows(
        db=None,
        source="samsung_croma",
        dataset_type="sales",
        metric="earned_premium",
        candidate_job_ids=[None],
        preferred_job_id=None,
        from_date="2025-02-01",
        to_date="2026-04-07",
    )

    assert rows == live_rows


def test_compute_date_bounds_payload_uses_precomputed_month_rows(monkeypatch):
    def fake_get_precomputed_graph(**kwargs):
        assert kwargs["source"] == "reliance"
        assert kwargs["dataset_type"] == "sales"
        return [
            {"Month": "Feb-26", "quantity": 10},
            {"Month": "Mar-26", "quantity": 20},
        ]

    def fail_live_bounds(**kwargs):
        raise AssertionError("date bounds should use precomputed month rows before raw dataframe scans")

    monkeypatch.setattr(analytics, "get_precomputed_graph", fake_get_precomputed_graph)
    monkeypatch.setattr(analytics, "_date_bounds_for_source_dataset", fail_live_bounds)
    monkeypatch.setattr(analytics, "_current_month_cap", lambda: pd.Timestamp("2026-03-31"))

    payload = analytics.compute_date_bounds_payload(
        db=None,
        source="reliance",
        dataset_type="sales",
        job_id=None,
    )

    assert payload == {
        "min_date": "2026-02-01",
        "max_date": "2026-03-31",
    }


def test_analytics_date_bounds_uses_cached_summary_when_fresh(monkeypatch):
    sentinel = {
        "min_date": "2025-04-01",
        "max_date": "2026-03-31",
    }

    monkeypatch.setattr(analytics, "get_precomputed_summary", lambda **kwargs: sentinel)
    monkeypatch.setattr(
        analytics,
        "_get_date_bounds_cache_updated_at",
        lambda **kwargs: pd.Timestamp("2026-03-27 10:00:00"),
    )
    monkeypatch.setattr(
        analytics,
        "_latest_date_bounds_marker_updated_at",
        lambda **kwargs: pd.Timestamp("2026-03-27 09:00:00"),
    )

    def fail_compute_date_bounds_payload(**kwargs):
        raise AssertionError("fresh date-bounds cache should not recompute live")

    monkeypatch.setattr(analytics, "compute_date_bounds_payload", fail_compute_date_bounds_payload)
    monkeypatch.setattr(
        analytics,
        "_resolve_job_id_fallback",
        lambda **kwargs: kwargs.get("job_id"),
    )

    payload = analytics.analytics_date_bounds(
        job_id=None,
        source="reliance",
        dataset_type="sales",
        db=None,
    )

    assert payload == sentinel


def test_master_dashboard_rebuilds_synchronously_when_code_marker_is_newer(monkeypatch):
    cached = analytics._empty_master_payload(from_date=None, to_date=None)
    fresh = analytics._empty_master_payload(from_date=None, to_date=None)
    fresh["summaries"]["samsung_sales"] = {
        "gross_premium": 399152171.99,
        "earned_premium": 129890973.33,
        "zopper_earned_premium": 34120655.97,
        "units_sold": 68409,
    }

    scheduled: list[dict] = []
    upserts: list[dict] = []

    class _MasterDbStub:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    db = _MasterDbStub()

    monkeypatch.setattr(analytics, "get_precomputed_summary", lambda **kwargs: cached)
    monkeypatch.setattr(
        analytics,
        "_get_master_cache_updated_at",
        lambda **kwargs: pd.Timestamp("2026-04-12 10:00:00"),
    )
    monkeypatch.setattr(analytics, "_latest_master_marker_updated_at", lambda **kwargs: None)
    monkeypatch.setattr(
        analytics,
        "_master_payload_has_godrej_sales_month_mismatch",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        analytics,
        "_build_master_dashboard_payload",
        lambda **kwargs: fresh,
    )
    monkeypatch.setattr(
        analytics,
        "upsert_precomputed_summary",
        lambda **kwargs: upserts.append(kwargs),
    )
    monkeypatch.setattr(
        analytics,
        "_schedule_master_dashboard_rebuild",
        lambda **kwargs: scheduled.append(kwargs),
    )
    monkeypatch.setattr(
        analytics,
        "_MASTER_DASHBOARD_CACHE_UPDATED_AT",
        pd.Timestamp("2026-04-13 18:45:00+05:30").to_pydatetime(),
    )

    payload = analytics.analytics_master_dashboard(db=db)

    assert payload == fresh
    assert db.commits == 1
    assert db.rollbacks == 0
    assert len(upserts) == 1
    assert scheduled == []


class _AdminFilesDbStub:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_post_file_update_keeps_master_cache_warm_during_background_refresh(monkeypatch):
    cleared: list[tuple[str, str]] = []
    marked: list[tuple[str, str, str | None]] = []
    rebuilt: list[tuple[str, str, str | None]] = []
    refreshed: list[dict] = []

    primary_db = _AdminFilesDbStub()
    worker_db = _AdminFilesDbStub()

    monkeypatch.setattr(
        admin_files,
        "clear_precomputed_for_source_dataset",
        lambda db, *, source, dataset_type: cleared.append((source, dataset_type)),
    )
    monkeypatch.setattr(
        admin_files,
        "invalidate_deck_cache_for_source_dataset",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        admin_files,
        "mark_manual_update",
        lambda **kwargs: marked.append((kwargs["source"], kwargs["dataset_type"], kwargs["job_id"])),
    )
    monkeypatch.setattr(admin_files, "invalidate_dataframe_cache", lambda **kwargs: None)
    monkeypatch.setattr(admin_files, "invalidate_samsung_load_cache", lambda **kwargs: None)
    monkeypatch.setattr(admin_files, "invalidate_reliance_load_cache", lambda **kwargs: None)
    monkeypatch.setattr(admin_files, "invalidate_godrej_load_cache", lambda **kwargs: None)
    monkeypatch.setattr(admin_files, "invalidate_hitachi_load_cache", lambda **kwargs: None)
    monkeypatch.setattr(
        admin_files,
        "rebuild_precomputed_analytics",
        lambda **kwargs: rebuilt.append((kwargs["source"], kwargs["dataset_type"], kwargs.get("job_id"))),
    )
    monkeypatch.setattr(
        admin_files,
        "refresh_master_overview_cache",
        lambda **kwargs: refreshed.append(kwargs),
    )
    monkeypatch.setattr(admin_files, "SessionLocal", lambda: worker_db)

    class _ImmediateThread:
        def __init__(self, *, target=None, **kwargs):
            self._target = target

        def start(self):
            if self._target is not None:
                self._target()

    monkeypatch.setattr(admin_files.threading, "Thread", _ImmediateThread)

    admin_files._post_file_update(
        db=primary_db,
        source_norm="samsung_croma",
        dataset_norm="sales",
        job_norm="01",
        action="upload",
    )

    assert cleared == []
    assert ("samsung_croma", "sales", "01") in marked
    assert ("samsung_croma", "sales", None) in marked
    assert ("samsung", "sales", "01") in marked
    assert ("samsung", "sales", None) in marked
    assert ("samsung_croma", "sales", "01") in rebuilt
    assert ("samsung_croma", "sales", None) in rebuilt
    assert ("samsung", "sales", "01") in rebuilt
    assert ("samsung", "sales", None) in rebuilt
    assert refreshed and refreshed[0]["db"] is worker_db
