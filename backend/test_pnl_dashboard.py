from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from routers import analytics


def test_pnl_store_rollup_computes_profit_and_loss_ratio():
    sales = pd.DataFrame(
        {
            "State": ["Maharashtra", "Maharashtra"],
            "City": ["Mumbai", "Mumbai"],
            "Store Name": ["Store A", "Store B"],
            "Store ID": ["A1", "B1"],
            "Product Category": ["Mobile", "Mobile"],
            "Plan Category": ["ADLD", "EW"],
            "Channel": ["Retail", "Retail"],
            "Date": ["2026-03-01", "2026-03-02"],
            "Gross Premium": [10000, 8000],
            "Earned Premium": [6000, 5000],
            "Zopper_Share_EP": [2500, 2100],
        }
    )
    claims = pd.DataFrame(
        {
            "State": ["Maharashtra", "Maharashtra"],
            "City": ["Mumbai", "Mumbai"],
            "Store Name": ["Store A", "Store B"],
            "Store ID": ["A1", "B1"],
            "Reason": ["Screen", "Battery"],
            "Model Description": ["Galaxy", "Galaxy"],
            "Plan Category": ["ADLD", "EW"],
            "Day of Call_Date": ["2026-03-10", "2026-03-11"],
            "Net Claims": [800, 2600],
        }
    )

    sales_prepared = analytics._prepare_pnl_frame(sales, dataset_type="sales")
    claims_prepared = analytics._prepare_pnl_frame(claims, dataset_type="claims")
    rows = analytics._pnl_store_rollup(sales_prepared, claims_prepared)

    by_store = {row["store_name"]: row for _, row in rows.iterrows()}
    assert round(float(by_store["Store A"]["profit"]), 2) == 1700.00
    assert round(float(by_store["Store A"]["loss_ratio"]), 2) == 32.00
    assert round(float(by_store["Store B"]["profit"]), 2) == -500.00
    assert round(float(by_store["Store B"]["loss_ratio"]), 2) == 123.81


def test_pnl_board_payload_builds_filters_and_default_store(monkeypatch):
    sales = pd.DataFrame(
        {
            "State": ["Delhi", "Delhi"],
            "City": ["Delhi", "Delhi"],
            "Store Name": ["Store A", "Store B"],
            "Store ID": ["A1", "B1"],
            "Product Category": ["Mobile", "Tablet"],
            "Plan Category": ["ADLD", "EW"],
            "Channel": ["Retail", "Online"],
            "Date": ["2026-03-01", "2026-03-02"],
            "Gross Premium": [10000, 8000],
            "Earned Premium": [6000, 5000],
            "Zopper_Share_EP": [2500, 2100],
        }
    )
    claims = pd.DataFrame(
        {
            "State": ["Delhi"],
            "City": ["Delhi"],
            "Store Name": ["Store B"],
            "Store ID": ["B1"],
            "Reason": ["Battery"],
            "Model Description": ["Galaxy"],
            "Plan Category": ["EW"],
            "Day of Call_Date": ["2026-03-11"],
            "Net Claims": [2600],
        }
    )

    monkeypatch.setattr(
        analytics,
        "_pnl_load_source_frames",
        lambda **kwargs: (sales.copy(), claims.copy()),
    )

    payload = analytics._pnl_build_board_payload(
        db=None,
        resolved_source="samsung",
        job_id=None,
        from_date=None,
        to_date=None,
        state="Delhi",
        city=None,
        limit=10,
    )

    assert payload["source"] == "samsung"
    assert payload["state_options"][0]["label"] == "Delhi"
    assert payload["rows"]
    assert payload["default_store_key"] == payload["rows"][0]["store_key"]


def test_pnl_board_summary_uses_full_rollup_before_limit(monkeypatch):
    sales = pd.DataFrame(
        {
            "State": ["Delhi", "Delhi", "Maharashtra"],
            "City": ["Delhi", "Delhi", "Mumbai"],
            "Store Name": ["Loss Store", "Profit Store", "Stable Store"],
            "Store ID": ["L1", "P1", "S1"],
            "Product Category": ["Mobile", "Mobile", "Mobile"],
            "Plan Category": ["ADLD", "EW", "ADLD"],
            "Channel": ["Retail", "Retail", "Retail"],
            "Date": ["2026-03-01", "2026-03-02", "2026-03-03"],
            "Gross Premium": [5000, 12000, 7000],
            "Earned Premium": [2400, 6000, 3000],
            "Zopper_Share_EP": [1000, 5500, 1800],
        }
    )
    claims = pd.DataFrame(
        {
            "State": ["Delhi", "Maharashtra"],
            "City": ["Delhi", "Mumbai"],
            "Store Name": ["Loss Store", "Stable Store"],
            "Store ID": ["L1", "S1"],
            "Reason": ["Screen", "Battery"],
            "Model Description": ["Galaxy", "iPhone"],
            "Plan Category": ["ADLD", "ADLD"],
            "Day of Call_Date": ["2026-03-10", "2026-03-11"],
            "Net Claims": [2200, 400],
        }
    )

    monkeypatch.setattr(
        analytics,
        "_pnl_load_source_frames",
        lambda **kwargs: (sales.copy(), claims.copy()),
    )

    payload = analytics._pnl_build_board_payload(
        db=None,
        resolved_source="samsung",
        job_id=None,
        from_date=None,
        to_date=None,
        state=None,
        city=None,
        limit=1,
    )

    assert len(payload["rows"]) == 1
    assert payload["summary"]["total_stores"] == 3
    assert round(float(payload["summary"]["total_zopper_earned_premium"]), 2) == 8300.00
    assert round(float(payload["summary"]["total_claims_cost"]), 2) == 2600.00
    assert round(float(payload["summary"]["total_profit"]), 2) == 5700.00


def test_prepare_reliance_pnl_frame_uses_city_state_store_bucket():
    sales = pd.DataFrame(
        {
            "State": ["Telangana"],
            "City": ["Hyderabad"],
            "Store No": ["101"],
            "Plan Type": ["ADLD"],
            "Purchase Date": ["2026-03-01"],
            "Gross Premium": [10000],
            "Earned Premium": [4000],
            "Zopper Earned Premium": [1200],
        }
    )
    claims = pd.DataFrame(
        {
            "Customer_State": ["Telangana"],
            "Customer_City": ["Hyderabad"],
            "Store Name": ["Reliance ResQ"],
            "Warranty Type": ["ADLD"],
            "Day of Call_Date": ["2026-03-05"],
            "Net Claims": [300],
        }
    )

    sales_prepared = analytics._prepare_pnl_frame(sales, dataset_type="sales", source="reliance")
    claims_prepared = analytics._prepare_pnl_frame(claims, dataset_type="claims", source="reliance")

    assert sales_prepared.iloc[0]["store_key"] == claims_prepared.iloc[0]["store_key"]
    assert sales_prepared.iloc[0]["store_name"] == "Hyderabad"
    assert claims_prepared.iloc[0]["store_name"] == "Hyderabad"


def test_hitachi_pnl_attributes_claims_back_to_sales_store(monkeypatch):
    sales = pd.DataFrame(
        {
            "Warranty Activation Code": ["HIT-001"],
            "State": ["Delhi"],
            "City": ["New Delhi"],
            "Store Name": ["Cool Air Condition"],
            "Store ID": ["472174"],
            "Product Category": ["Air Conditioner"],
            "Plan Category": ["Care Plus"],
            "Channel": ["Hitachi"],
            "Warranty Purchase Date": ["2026-03-01"],
            "Customer Premium": [6999],
            "Earned Premium": [4200],
            "Zopper_Share_EP": [1500],
        }
    )
    claims = pd.DataFrame(
        {
            "Care+ Plan ID": ["HIT-001"],
            "State": ["Haryana"],
            "City": ["Gurgaon"],
            "Dealer Name": ["Third Party Service"],
            "Dealer ID": ["9001"],
            "Nature of Complaint": ["Noise issue"],
            "Model Description": ["AC Split"],
            "Plan Category": ["PLAN 1"],
            "Claim Date": ["2026-03-10"],
            "Net Claims": [700],
        }
    )

    monkeypatch.setattr(
        analytics,
        "_pnl_load_source_frames",
        lambda **kwargs: (sales.copy(), claims.copy()),
    )

    payload = analytics._pnl_build_board_payload(
        db=None,
        resolved_source="hitachi",
        job_id=None,
        from_date=None,
        to_date=None,
        state=None,
        city=None,
        limit=10,
    )

    assert payload["summary"]["total_stores"] == 1
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["store_name"] == "Cool Air Condition"
    assert row["store_id"] == "472174"
    assert row["state"] == "Delhi"
    assert row["city"] == "New Delhi"
    assert round(float(row["profit"]), 2) == 800.00
    assert row["top_claim_reason"] == "Noise issue / AC Split"
