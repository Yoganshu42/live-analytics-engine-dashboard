from __future__ import annotations

from typing import Any


def _normalize_key(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("&", "and")
        .replace("+", "plus")
        .replace("_", "")
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )


_HITACHI_DIRECT_PLAN_MAP = {
    "careplus": "Care Plus",
    "extendedwarranty": "Care Plus",
    "extendedwarrantywithservice": "Care Plus",
    "completecare": "Complete Care",
    "completecareplus": "Complete Care Plus",
    "newwarrantykit": "New Warranty Kit",
    "plan1": "Care Plus",
    "plan2": "New Warranty Kit",
    "plan3": "Complete Care",
    # PLAN 5 has no stable description in current claims rows and no direct serial
    # join back to sales yet. Recent model-plan mix is overwhelmingly Complete Care,
    # so use that as the best available canonical label for now.
    "plan5": "Complete Care",
}


def canonicalize_hitachi_sales_plan_category(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = _normalize_key(raw)
    direct = _HITACHI_DIRECT_PLAN_MAP.get(normalized)
    if direct:
        return direct
    return raw


def canonicalize_hitachi_claim_plan_category(
    *,
    plan_name: Any = None,
    plan_description: Any = None,
    product_category: Any = None,
    model_description: Any = None,
) -> str:
    description = str(plan_description or "").strip()
    description_key = _normalize_key(description)
    if description_key:
        if "upto3cleaningserviceperyearonrequest" in description_key:
            return "Care Plus"
        if "gascharging" in description_key and "cleaning" not in description_key:
            return "New Warranty Kit"
        if "upto2cleaningserviceperyearonrequest" in description_key:
            return "Complete Care"
        direct_desc = _HITACHI_DIRECT_PLAN_MAP.get(description_key)
        if direct_desc:
            return direct_desc

    direct_name = _HITACHI_DIRECT_PLAN_MAP.get(_normalize_key(plan_name))
    if direct_name:
        return direct_name

    for candidate in (product_category, model_description):
        direct_other = _HITACHI_DIRECT_PLAN_MAP.get(_normalize_key(candidate))
        if direct_other:
            return direct_other

    return str(plan_name or "").strip()
