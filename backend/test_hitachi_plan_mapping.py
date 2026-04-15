from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.hitachi_plan_mapping import (
    canonicalize_hitachi_claim_plan_category,
    canonicalize_hitachi_sales_plan_category,
)


def test_hitachi_sales_extended_warranty_maps_to_care_plus():
    assert canonicalize_hitachi_sales_plan_category("Extended Warranty (with service)") == "Care Plus"


def test_hitachi_claim_plan_codes_map_to_canonical_labels():
    assert canonicalize_hitachi_claim_plan_category(plan_name="PLAN 1") == "Care Plus"
    assert canonicalize_hitachi_claim_plan_category(plan_name="PLAN 2") == "New Warranty Kit"
    assert canonicalize_hitachi_claim_plan_category(plan_name="PLAN 3") == "Complete Care"
    assert canonicalize_hitachi_claim_plan_category(plan_name="PLAN 5") == "Complete Care"


def test_hitachi_claim_description_rules_override_generic_codes():
    assert (
        canonicalize_hitachi_claim_plan_category(
            plan_name="PLAN 1",
            plan_description="PARTS COVERAGE AS PER STANDARD WARRANTY + GAS CHARGING + UPTO 3 CLEANING SERVICE PER YEAR ON REQUEST",
        )
        == "Care Plus"
    )
    assert (
        canonicalize_hitachi_claim_plan_category(
            plan_name="PLAN 2",
            plan_description="PARTS COVERAGE AS PER STANDARD WARRANTY + GAS CHARGING",
        )
        == "New Warranty Kit"
    )
