from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.data_quality_service import prepare_rows_for_storage


def _claim_row(
    *,
    call_no: str,
    month: str,
    payout_amount: float,
    approval_status: str,
) -> dict[str, object]:
    return {
        "Month": month,
        "Call No": call_no,
        "Call Date": month,
        "Payout Amount": payout_amount,
        "Approval Status": approval_status,
        "Care+ Plan ID": "PLAN-001",
        "Care+ Plan Name": "PLAN 1",
        "Plan Start Date": "2025-01-01",
        "Plan End Date": "2028-01-01",
    }


def test_hitachi_claim_rows_with_same_call_number_keep_distinct_record_keys():
    rows = [
        _claim_row(
            call_no="CALL-1001",
            month="2025-01-01",
            payout_amount=120.0,
            approval_status="APPROVED",
        ),
        _claim_row(
            call_no="CALL-1001",
            month="2025-01-01",
            payout_amount=245.0,
            approval_status="REOPENED",
        ),
        _claim_row(
            call_no="CALL-1002",
            month="2025-01-01",
            payout_amount=180.0,
            approval_status="APPROVED",
        ),
    ]

    storage_rows, meta = prepare_rows_for_storage(rows, source="hitachi", dataset_type="claims")

    assert meta["strategy"] == "natural_column_row_fingerprint"
    assert len(storage_rows) == 3
    assert len({item["record_key"] for item in storage_rows}) == 3


def test_hitachi_claim_row_keys_do_not_collide_across_separate_uploads():
    base_rows = [
        _claim_row(
            call_no="CALL-2001",
            month="2025-02-01",
            payout_amount=300.0,
            approval_status="APPROVED",
        )
    ]
    delta_rows = [
        _claim_row(
            call_no="CALL-2001",
            month="2025-02-01",
            payout_amount=410.0,
            approval_status="SETTLED",
        )
    ]

    base_storage, _ = prepare_rows_for_storage(base_rows, source="hitachi", dataset_type="claims")
    delta_storage, _ = prepare_rows_for_storage(delta_rows, source="hitachi", dataset_type="claims")

    assert base_storage[0]["record_key"] != delta_storage[0]["record_key"]
