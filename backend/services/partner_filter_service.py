from __future__ import annotations

import math
import re
from datetime import date, datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from services.ai_mapper import suggest_reverse_mapping

SAMSUNG_REFERENCE_PLAN_PRICES: dict[tuple[str, str], int] = {
    ("Luxury Fold", "ADLD"): 5299,
    ("Luxury Fold", "Screen Protection"): 3999,
    ("Luxury Fold", "Combo"): 8587,
    ("Luxury Fold", "Extended Warranty"): 2060,
    ("Luxury Flip", "ADLD"): 4199,
    ("Luxury Flip", "Screen Protection"): 3748,
    ("Luxury Flip", "Combo"): 6800,
    ("Luxury Flip", "Extended Warranty"): 1737,
    ("Super Premium", "ADLD"): 2539,
    ("Super Premium", "Screen Protection"): 1174,
    ("Super Premium", "Combo"): 4694,
    ("Super Premium", "Extended Warranty"): 1064,
    ("Premium", "ADLD"): 1686,
    ("Premium", "Screen Protection"): 523,
    ("Premium", "Combo"): 2299,
    ("Premium", "Extended Warranty"): 410,
    ("High", "ADLD"): 799,
    ("High", "Screen Protection"): 260,
    ("High", "Combo"): 1399,
    ("High", "Extended Warranty"): 242,
    ("Mid", "ADLD"): 563,
    ("Mid", "Screen Protection"): 135,
    ("Mid", "Combo"): 806,
    ("Mid", "Extended Warranty"): 149,
    ("Mass", "ADLD"): 159,
    ("Mass", "Screen Protection"): 53,
    ("Mass", "Combo"): 267,
    ("Mass", "Extended Warranty"): 46,
}

SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY: dict[str, str] = {
    "A06": "Mass",
    "F15": "Mass",
    "A16": "Mid",
    "A17": "Mid",
    "F17": "Mid",
    "A26": "High",
    "A35": "High",
    "A36": "High",
    "F55": "High",
    "A56": "Premium",
    "S24": "Super Premium",
    "S25": "Super Premium",
    "Fold6": "Luxury Fold",
    "Fold7": "Luxury Fold",
    "Flip7": "Luxury Flip",
}

RELIANCE_TRANSFER_RATE: dict[tuple[str, int], float] = {
    ("ADLD", 12): 0.022,
    ("Crack Screen", 12): 0.014,
    ("Crack Screen", 24): 0.028,
    ("Extended Warranty", 6): 0.003,
    ("Extended Warranty", 12): 0.005,
}

REVENUE_SPLIT: dict[str, dict[str, float]] = {
    "D2D": {"Channel": 0.25, "Godrej": 0.35, "Zopper": 0.40},
    "POS": {"Channel": 0.25, "Godrej": 0.35, "Zopper": 0.40},
    "Calling Process": {"Channel": 0.30, "Godrej": 0.35, "Zopper": 0.35},
    "POD": {"Channel": 0.20, "Godrej": 0.35, "Zopper": 0.45},
    "Amazon": {"Channel": 0.40, "Godrej": 0.35, "Zopper": 0.25},
}

SOURCE_ALIASES: dict[str, str] = {
    "samsung": "samsung",
    "samsungvs": "samsung",
    "samsungvijaysales": "samsung",
    "samsungcroma": "samsung",
    "reliance": "reliance",
    "relianceresq": "reliance",
    "resq": "reliance",
    "godrej": "godrej",
    "goodrej": "godrej",
    "goddrej": "godrej",
}

FIELD_TO_CANONICAL: dict[str, str] = {
    "gross_premium": "Plan Selling Price",
    "zopper_transfer_price": "Zopper Shared ( Transfer Price )",
    "zopper_share": "Zopper Share",
    "plan_category": "Plan Category",
    "device_plan_category": "Device Plan Category",
    "brand": "Brand",
    "start_date": "Plan Start Date",
    "plan_start_date": "Plan Start Date",
    "warranty_start_date": "Warranty Start Date",
    "end_date": "Plan End Date",
    "plan_end_date": "Plan End Date",
    "warranty_end_date": "Warranty End Date",
    "channel": "Channel",
    "product_category": "Product_Category",
    "claims_cost": "Claim_Amount",
    "claim_amount": "Claim_Amount",
    "claim_date": "Claim Date",
    "claim_month": "Month",
    "month": "Month",
}

COMMON_ALIAS_TARGETS: dict[str, tuple[str, ...]] = {
    "Plan Selling Price": (
        "plan selling price",
        "plan price",
        "plan mrp",
        "plan mrp rs",
        "plan mrp rupees",
        "plan mrp ₹",
        "gross premium",
        "amount",
        "customer premium",
        "invoice value",
        "invoice_value",
        "inr amount",
    ),
    "Amount": (
        "amount",
        "gross premium",
        "plan selling price",
        "customer premium",
    ),
    "Gross Premium": (
        "gross premium",
        "plan selling price",
        "amount",
        "customer premium",
    ),
    "Zopper Shared ( Transfer Price )": (
        "zopper shared transfer price",
        "zopper shared ( transfer price )",
        "zopper share",
        "transfer price",
        "transfer_price",
        "total billing amount",
        "billing amount",
    ),
    "Zopper Share": (
        "zopper share",
        "zopper shared transfer price",
        "zopper shared ( transfer price )",
        "transfer price",
        "transfer_price",
    ),
    "Plan Category": (
        "plan category",
        "plan_category",
        "plan type",
        "warranty type",
        "pack type",
    ),
    "Device Plan Category": (
        "device plan category",
        "device_plan_category",
        "device category",
        "article brick",
        "article_brick",
    ),
    "Model Code": (
        "model code",
        "model code-1",
        "model",
        "article_model_desc",
        "appliance model name",
        "item description",
    ),
    "State": (
        "state",
        "state name",
        "state ut",
        "state / city",
        "state/city",
        "customer state",
        "region",
        "region name",
        "zone",
        "location",
    ),
    "City": (
        "city",
        "customer city",
        "branch",
        "branch name",
        "location",
        "state / city",
        "state/city",
    ),
    "Month": (
        "month",
        "month-year",
        "month year",
        "month_name",
        "month name",
        "fiscal month",
    ),
    "Plan Start Date": (
        "plan start date",
        "start date",
        "start_date",
        "purchase date",
        "invoice date",
        "date",
    ),
    "Plan End Date": (
        "plan end date",
        "end date",
        "end_date",
        "warranty end date",
    ),
    "Warranty Start Date": (
        "warranty start date",
        "warranty_start_date",
        "plan start date",
        "start date",
    ),
    "Warranty End Date": (
        "warranty end date",
        "warranty_end_date",
        "plan end date",
        "end date",
    ),
    "Claim_Amount": (
        "claim amount",
        "claim_amount",
        "net amount",
        "net_claim_amount",
        "zopper's cost",
        "zoppers cost",
    ),
    "Claim Date": (
        "claim date",
        "call_date",
        "day of call_date",
        "call registered date",
        "payment date",
        "date",
    ),
    "Channel": (
        "channel",
        "channel name",
        "channel_name",
    ),
    "Product_Category": (
        "product category",
        "product_category",
        "prodcut category",
        "category",
    ),
}

_STATE_FALLBACK_MAP: dict[str, str] = {
    "delhi": "Delhi",
    "newdelhi": "Delhi",
    "mumbai": "Maharashtra",
    "thane": "Maharashtra",
    "navi mumbai": "Maharashtra",
    "pune": "Maharashtra",
    "hyderabad": "Telangana",
    "secunderabad": "Telangana",
    "bangalore": "Karnataka",
    "bengaluru": "Karnataka",
    "chennai": "Tamil Nadu",
    "coimbatore": "Tamil Nadu",
    "kolkata": "West Bengal",
    "ahmedabad": "Gujarat",
    "baroda": "Gujarat",
    "vadodara": "Gujarat",
    "surat": "Gujarat",
    "lucknow": "Uttar Pradesh",
    "noida": "Uttar Pradesh",
    "greater noida": "Uttar Pradesh",
    "ghaziabad": "Uttar Pradesh",
    "gurgaon": "Haryana",
    "gurugram": "Haryana",
    "faridabad": "Haryana",
    "jaipur": "Rajasthan",
    "bhopal": "Madhya Pradesh",
    "indore": "Madhya Pradesh",
    "patna": "Bihar",
    "ranchi": "Jharkhand",
    "kochi": "Kerala",
    "trivandrum": "Kerala",
    "guwahati": "Assam",
    "vijayawada": "Andhra Pradesh",
    "visakhapatnam": "Andhra Pradesh",
    "vizag": "Andhra Pradesh",
    "odisha": "Odisha",
    "orissa": "Odisha",
}


def _normalize_source(source: str) -> str:
    key = _clean_key(source)
    if key in SOURCE_ALIASES:
        return SOURCE_ALIASES[key]
    if key.startswith("samsung"):
        return "samsung"
    return key


def _normalize_dataset_type(dataset_type: str) -> str:
    key = _clean_key(dataset_type)
    return "claims" if "claim" in key else "sales"


def _clean_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def _missing_mask(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.lower()
    return series.isna() | text.isin({"", "nan", "none", "null", "na"})


def _as_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0.0)
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("INR", "", regex=False)
        .str.replace("Rs.", "", regex=False)
        .str.replace("Rs", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)


def _build_norm_index(columns: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for col in columns:
        out.setdefault(_clean_key(col), []).append(col)
    return out


def _resolve_candidate_columns(df: pd.DataFrame, aliases: tuple[str, ...]) -> list[str]:
    norm_index = _build_norm_index([str(c) for c in df.columns])
    matched: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        key = _clean_key(alias)
        for col in norm_index.get(key, []):
            if col in seen:
                continue
            seen.add(col)
            matched.append(col)
    if matched:
        return matched

    alias_keys = [_clean_key(alias) for alias in aliases]
    generic_partial_keys = {
        "amount",
        "date",
        "month",
        "state",
        "city",
        "region",
        "channel",
        "category",
        "brand",
        "model",
        "price",
        "premium",
        "value",
    }
    for col in df.columns:
        col_key = _clean_key(col)
        if not col_key:
            continue
        hit = False
        for alias_key in alias_keys:
            if not alias_key:
                continue
            if alias_key in col_key:
                shorter = alias_key
            elif col_key in alias_key:
                shorter = col_key
            else:
                continue
            if shorter in generic_partial_keys:
                continue
            if min(len(alias_key), len(col_key)) < 6:
                continue
            hit = True
            break
        if hit:
            text_col = str(col)
            if text_col not in seen:
                seen.add(text_col)
                matched.append(text_col)
    return matched


def _coalesce_text(df: pd.DataFrame, aliases: tuple[str, ...], default: str = "") -> pd.Series:
    out = pd.Series(default, index=df.index, dtype="object")
    candidates = _resolve_candidate_columns(df, aliases)
    for col in candidates:
        text = df[col].astype(str).str.strip()
        text = text.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "none": pd.NA, "null": pd.NA})
        mask = _missing_mask(out) & text.notna()
        out = out.where(~mask, text)
    return out.fillna(default)


def _coalesce_numeric(df: pd.DataFrame, aliases: tuple[str, ...], default: float = 0.0) -> pd.Series:
    out = pd.Series(float(default), index=df.index, dtype="float64")
    candidates = _resolve_candidate_columns(df, aliases)
    for col in candidates:
        numeric = _as_numeric(df[col])
        mask = out.eq(0.0) & numeric.ne(0.0)
        out = out.where(~mask, numeric)
    return out.fillna(float(default))


def _apply_smart_reverse_mapping(df: pd.DataFrame, *, source: str, dataset_type: str) -> dict[str, Any]:
    mapper_source = "samsung" if source == "samsung" else source
    try:
        suggestion = suggest_reverse_mapping(df, source=mapper_source, dataset_type=dataset_type)
    except Exception:
        return {"applied": 0, "required_found": 0, "required_total": 0, "coverage": 0.0}

    applied = 0
    for item in suggestion.get("mappings", []):
        if not isinstance(item, dict) or not item.get("found"):
            continue
        field = str(item.get("field") or "").strip()
        suggested_col = str(item.get("suggested_column") or "").strip()
        confidence = float(item.get("confidence") or 0.0)
        canonical = FIELD_TO_CANONICAL.get(field)
        if not canonical or not suggested_col or suggested_col not in df.columns:
            continue
        if confidence < 0.45:
            continue

        if canonical not in df.columns:
            df[canonical] = df[suggested_col]
            applied += 1
            continue

        current = df[canonical]
        candidate = df[suggested_col]
        mask = _missing_mask(current) & ~_missing_mask(candidate)
        if mask.any():
            df.loc[mask, canonical] = candidate.loc[mask]
            applied += 1

    return {
        "applied": int(applied),
        "required_found": int(suggestion.get("required_fields_found") or 0),
        "required_total": int(suggestion.get("required_fields_total") or 0),
        "coverage": float(suggestion.get("coverage") or 0.0),
    }


def _apply_common_alias_coalescing(df: pd.DataFrame) -> int:
    touched = 0
    for canonical, aliases in COMMON_ALIAS_TARGETS.items():
        if canonical in df.columns:
            existing = df[canonical]
            candidates = _coalesce_text(df, aliases, default="")
            mask = _missing_mask(existing) & ~_missing_mask(candidates)
            if mask.any():
                df.loc[mask, canonical] = candidates.loc[mask]
                touched += 1
            continue

        candidates = _coalesce_text(df, aliases, default="")
        if (~_missing_mask(candidates)).any():
            df[canonical] = candidates
            touched += 1
    return touched


def _canonical_samsung_plan_category(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not text or text in {"nan", "none", "null"}:
        return ""
    if "combo" in text:
        return "Combo"
    if "adld" in text or "accidental" in text or "liquid" in text:
        return "ADLD"
    if re.search(r"\bsp\b|\bspp\b", text) or "screen" in text or "crack" in text:
        return "Screen Protection"
    if re.search(r"\bew\b", text) or "extended warranty" in text or text.startswith("ew"):
        return "Extended Warranty"
    if "warranty" in text:
        return "Extended Warranty"
    return ""


def _extract_samsung_model_code(text: str) -> str | None:
    low = re.sub(r"\s+", " ", str(text or "").strip().lower())
    if not low:
        return None
    for model in sorted(SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY.keys(), key=lambda token: (-len(token), token)):
        if re.search(r"\b" + re.escape(model.lower()) + r"\b", low):
            return model
    return None


def _canonical_samsung_device_category(value: Any, model_text: str = "") -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    model_code = _extract_samsung_model_code(model_text or text)
    if model_code and model_code in SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY:
        return SAMSUNG_MODEL_TO_DEVICE_PLAN_CATEGORY[model_code]

    if "luxury" in text and "fold" in text:
        return "Luxury Fold"
    if "luxury" in text and "flip" in text:
        return "Luxury Flip"
    if "fold" in text:
        return "Luxury Fold"
    if "flip" in text:
        return "Luxury Flip"
    if "super" in text and "premium" in text:
        return "Super Premium"
    if text.startswith("premium") or ("premium" in text and "super" not in text):
        return "Premium"
    if text.startswith("high") or text == "high":
        return "High"
    if text.startswith("mid") or text == "mid":
        return "Mid"
    if text.startswith("mass") or text == "mass":
        return "Mass"
    return ""


def _build_reliance_transfer_bands() -> list[tuple[int, int, float]]:
    bands: list[tuple[int, int, float]] = []
    for upper in range(5000, 30001, 5000):
        lower = 0 if upper == 5000 else upper - 4999
        avg = upper - 2500
        bands.append((lower, upper, float(avg)))
    for upper in range(40000, 210001, 10000):
        lower = upper - 9999
        avg = upper - 5000
        bands.append((lower, upper, float(avg)))
    return bands


RELIANCE_TRANSFER_BANDS = _build_reliance_transfer_bands()


def _nearest_reliance_band_average(value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        return 0.0
    midpoint = float(value)
    best_avg = 0.0
    best_gap = float("inf")
    for lower, upper, avg in RELIANCE_TRANSFER_BANDS:
        if lower <= midpoint <= upper:
            return float(avg)
        gap = min(abs(midpoint - lower), abs(midpoint - upper))
        if gap < best_gap:
            best_gap = gap
            best_avg = float(avg)
    return best_avg


def _canonical_reliance_plan_type(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not text:
        return ""
    if "adld" in text or "accidental" in text or "liquid" in text:
        return "ADLD"
    if "crack" in text or "screen" in text or re.search(r"\bsp\b|\bspp\b", text):
        return "Crack Screen"
    if re.search(r"\bew\b", text) or "extended" in text or "warranty" in text:
        return "Extended Warranty"
    return ""


def _infer_tenure_months(text: Any, default: int = 12) -> int:
    low = str(text or "").strip().lower()
    if not low:
        return default
    if re.search(r"2\s*yr|24\s*m|24\s*month", low):
        return 24
    if re.search(r"6\s*m|6\s*month", low):
        return 6
    if re.search(r"1\s*yr|12\s*m|12\s*month", low):
        return 12
    return default


def _canonical_godrej_channel(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not text:
        return "Unknown"
    if text in {"d2d", "door to door", "door2door"}:
        return "D2D"
    if text in {"pos", "point of sale"}:
        return "POS"
    if "calling" in text:
        return "Calling Process"
    if text in {"pod"}:
        return "POD"
    if "amazon" in text:
        return "Amazon"
    return str(value or "").strip() or "Unknown"


@lru_cache(maxsize=4096)
def _canonical_state_cached(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        from routers.analytics import _canonical_geo_label  # local import avoids hard dependency at startup

        resolved = _canonical_geo_label(raw, kind="state")
        if str(resolved or "").strip():
            return str(resolved).strip()
    except Exception:
        pass

    key = re.sub(r"\s+", " ", raw.lower()).strip()
    compact = _clean_key(key)
    if compact in _STATE_FALLBACK_MAP:
        return _STATE_FALLBACK_MAP[compact]
    if key in _STATE_FALLBACK_MAP:
        return _STATE_FALLBACK_MAP[key]
    return raw.title()


def _apply_state_city_mapping(df: pd.DataFrame) -> int:
    touched = 0
    state_candidates = COMMON_ALIAS_TARGETS["State"]
    city_candidates = COMMON_ALIAS_TARGETS["City"]

    state_text = _coalesce_text(df, state_candidates, default="")
    city_text = _coalesce_text(df, city_candidates, default="")

    state_from_state = state_text.map(_canonical_state_cached)
    state_from_city = city_text.map(_canonical_state_cached)

    out_state = state_from_state.where(state_from_state.astype(str).str.strip().ne(""), state_from_city)
    out_state = out_state.fillna("")

    if "State" not in df.columns:
        if out_state.astype(str).str.strip().ne("").any():
            df["State"] = out_state
            touched += 1
    else:
        existing = df["State"].astype(str).str.strip()
        canonical_existing = existing.map(_canonical_state_cached)
        resolved = out_state.where(out_state.astype(str).str.strip().ne(""), canonical_existing)
        mask = resolved.astype(str).str.strip().ne("")
        if mask.any():
            df.loc[mask, "State"] = resolved.loc[mask]
            touched += 1

    if "City" not in df.columns and city_text.astype(str).str.strip().ne("").any():
        df["City"] = city_text
        touched += 1

    return touched


def _normalize_samsung_sales(df: pd.DataFrame) -> int:
    touched = 0
    plan_raw = _coalesce_text(df, COMMON_ALIAS_TARGETS["Plan Category"], default="")
    model_raw = _coalesce_text(df, COMMON_ALIAS_TARGETS["Model Code"], default="")
    device_raw = _coalesce_text(df, COMMON_ALIAS_TARGETS["Device Plan Category"], default="")

    plan_category = plan_raw.map(_canonical_samsung_plan_category)
    device_category = pd.Series(
        [
            _canonical_samsung_device_category(device_value, model_value)
            for device_value, model_value in zip(device_raw.tolist(), model_raw.tolist())
        ],
        index=df.index,
        dtype="object",
    )

    plan_mask = plan_category.ne("")
    if plan_mask.any():
        df.loc[plan_mask, "Plan Category"] = plan_category[plan_mask]
        touched += 1

    device_mask = device_category.ne("")
    if device_mask.any():
        df.loc[device_mask, "Device Plan Category"] = device_category[device_mask]
        touched += 1

    plan_price = _coalesce_numeric(
        df,
        (
            "Plan Selling Price",
            "Plan Price",
            "Plan MRP",
            "Plan MRP ₹",
            "Plan MRP (₹)",
            "Amount",
            "Gross Premium",
            "Customer Premium",
            "Invoice Value",
            "INVOICE_VALUE",
        ),
        default=0.0,
    )
    existing_plan = _as_numeric(df["Plan Selling Price"]) if "Plan Selling Price" in df.columns else pd.Series(0.0, index=df.index)
    plan_price = existing_plan.where(existing_plan.ne(0.0), plan_price)
    if plan_price.ne(0.0).any():
        df["Plan Selling Price"] = plan_price
        touched += 1

    amount = _as_numeric(df["Amount"]) if "Amount" in df.columns else pd.Series(0.0, index=df.index)
    amount = amount.where(amount.ne(0.0), plan_price)
    df["Amount"] = amount

    gross = _as_numeric(df["Gross Premium"]) if "Gross Premium" in df.columns else pd.Series(0.0, index=df.index)
    gross = gross.where(gross.ne(0.0), plan_price)
    df["Gross Premium"] = gross

    raw_zopper_share = _coalesce_numeric(
        df,
        (
            "Zopper Share",
            "Zopper Shared ( Transfer Price )",
            "Transfer Price",
            "Transfer_Price",
            "Total Billing Amount",
            "Billing Amount",
        ),
        default=0.0,
    )

    mapped_share = pd.Series(
        [
            float(SAMSUNG_REFERENCE_PLAN_PRICES.get((str(device or ""), str(plan or "")), 0.0))
            for device, plan in zip(device_category.tolist(), plan_category.tolist())
        ],
        index=df.index,
        dtype="float64",
    )

    zopper_share = raw_zopper_share.where(raw_zopper_share.ne(0.0), mapped_share)
    if zopper_share.ne(0.0).any():
        df["Zopper Share"] = zopper_share
        touched += 1

    transfer_existing = _as_numeric(df["Zopper Shared ( Transfer Price )"]) if "Zopper Shared ( Transfer Price )" in df.columns else pd.Series(0.0, index=df.index)
    transfer = transfer_existing.where(transfer_existing.ne(0.0), zopper_share)
    if transfer.ne(0.0).any():
        df["Zopper Shared ( Transfer Price )"] = transfer
        touched += 1

    return touched


def _normalize_samsung_claims(df: pd.DataFrame) -> int:
    touched = 0
    model_raw = _coalesce_text(df, ("Model Code", "Model Code-1", "Model"), default="")

    # Claims plan category should come from pack/plan type columns (ADLD/Combo/SP/EW),
    # not from device-segment category fields.
    plan_from_pack = _coalesce_text(
        df,
        (
            "Pack type",
            "Pack Type",
            "Plan type",
            "Plan Type",
            "Warranty Type",
            "Plan_Category",
            "Plan Category",
        ),
        default="",
    )
    plan_category = plan_from_pack.map(_canonical_samsung_plan_category)
    if plan_category.ne("").any():
        df.loc[plan_category.ne(""), "Plan Category"] = plan_category[plan_category.ne("")]
        touched += 1

    device_raw = _coalesce_text(
        df,
        (
            "Device Plan Category",
            "Device_Plan_Category",
            "Category",
            "Device Category",
            "Device_Category",
        ),
        default="",
    )
    device_category = pd.Series(
        [
            _canonical_samsung_device_category(device_value, model_value)
            for device_value, model_value in zip(device_raw.tolist(), model_raw.tolist())
        ],
        index=df.index,
        dtype="object",
    )
    if device_category.ne("").any():
        df.loc[device_category.ne(""), "Device Plan Category"] = device_category[device_category.ne("")]
        touched += 1

    return touched


def _normalize_reliance_sales(df: pd.DataFrame) -> int:
    touched = 0
    plan_text = _coalesce_text(df, COMMON_ALIAS_TARGETS["Plan Category"], default="")
    tenure_text = _coalesce_text(df, ("plan tenure", "tenure", "plan type", "plan category", "article_model_desc"), default="")

    plan_type = plan_text.map(_canonical_reliance_plan_type)
    tenure = tenure_text.map(_infer_tenure_months)

    plan_price = _coalesce_numeric(
        df,
        (
            "Plan Selling Price",
            "Plan Price",
            "Invoice Value",
            "INVOICE_VALUE",
            "Gross Premium",
            "Amount",
        ),
        default=0.0,
    )
    transfer = _coalesce_numeric(
        df,
        (
            "Zopper Shared ( Transfer Price )",
            "Zopper Share",
            "Transfer Price",
            "Total Billing Amount",
            "Billing Amount",
        ),
        default=0.0,
    )

    inferred_transfer = pd.Series(0.0, index=df.index, dtype="float64")
    inferred_price = pd.Series(0.0, index=df.index, dtype="float64")

    for idx in df.index:
        p_type = str(plan_type.loc[idx] or "")
        ten = int(tenure.loc[idx] or 12)
        rate = float(RELIANCE_TRANSFER_RATE.get((p_type, ten), 0.0))
        if rate <= 0:
            continue

        price_value = float(plan_price.loc[idx] or 0.0)
        transfer_value = float(transfer.loc[idx] or 0.0)

        if transfer_value <= 0 and price_value > 0:
            band_avg = _nearest_reliance_band_average(price_value)
            inferred_transfer.loc[idx] = band_avg * rate
        if price_value <= 0 and transfer_value > 0:
            band_avg = _nearest_reliance_band_average(transfer_value / rate)
            inferred_price.loc[idx] = band_avg

    transfer = transfer.where(transfer.ne(0.0), inferred_transfer)
    plan_price = plan_price.where(plan_price.ne(0.0), inferred_price)

    if plan_price.ne(0.0).any():
        df["Plan Selling Price"] = plan_price
        touched += 1
    if transfer.ne(0.0).any():
        df["Zopper Shared ( Transfer Price )"] = transfer
        df["Zopper Share"] = transfer
        touched += 1

    if plan_type.ne("").any():
        df.loc[plan_type.ne(""), "Plan Category"] = plan_type[plan_type.ne("")]
        touched += 1

    if tenure.notna().any():
        df["Plan Tenure Months"] = tenure
        touched += 1

    return touched


def _normalize_godrej_sales(df: pd.DataFrame) -> int:
    touched = 0
    channel = _coalesce_text(df, COMMON_ALIAS_TARGETS["Channel"], default="")
    canonical_channel = channel.map(_canonical_godrej_channel)

    if canonical_channel.astype(str).str.strip().ne("").any():
        df["Channel"] = canonical_channel
        touched += 1

    split = canonical_channel.map(REVENUE_SPLIT)
    zopper_pct = split.map(lambda item: float(item.get("Zopper", 0.0) * 100.0) if isinstance(item, dict) else 0.0)
    godrej_pct = split.map(lambda item: float(item.get("Godrej", 0.0) * 100.0) if isinstance(item, dict) else 0.0)
    channel_pct = split.map(lambda item: float(item.get("Channel", 0.0) * 100.0) if isinstance(item, dict) else 0.0)

    df["Zopper Share %"] = zopper_pct
    df["Godrej Share %"] = godrej_pct
    df["Channel Share %"] = channel_pct
    touched += 1
    return touched


def _ensure_deck_compat_columns(df: pd.DataFrame, *, source: str, dataset_type: str) -> int:
    touched = 0
    if df is None or df.empty:
        return touched

    def _parse_datetime_series(series: pd.Series) -> pd.Series:
        raw = series.astype(str).str.strip()
        raw = raw.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA, "null": pd.NA})
        cleaned = raw.astype("string").str.replace(r"\.0$", "", regex=True)
        parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

        # Normalize separators so explicit format parsing is stable.
        normalized = (
            cleaned.str.replace("/", "-", regex=False)
            .str.replace(r"\s+", "-", regex=True)
            .str.strip("-")
        )

        mask_ymd = cleaned.str.fullmatch(r"\d{8}", na=False)
        if mask_ymd.any():
            ymd = pd.to_datetime(cleaned.where(mask_ymd), format="%Y%m%d", errors="coerce")
            parsed = parsed.where(parsed.notna(), ymd)

        mask_ym = cleaned.str.fullmatch(r"\d{6}", na=False)
        if mask_ym.any():
            ym = pd.to_datetime(cleaned.where(mask_ym), format="%Y%m", errors="coerce")
            parsed = parsed.where(parsed.notna(), ym)

        # Explicit month-year labels (e.g. Aug-25 / Aug 25) should be treated as month-year,
        # not "month day in current year".
        mask_mon_yy = normalized.str.fullmatch(r"[A-Za-z]{3,9}-\d{2}", na=False)
        if mask_mon_yy.any():
            mon_yy = pd.to_datetime(normalized.where(mask_mon_yy), format="%b-%y", errors="coerce")
            parsed = parsed.where(parsed.notna(), mon_yy)

        mask_mon_yyyy = normalized.str.fullmatch(r"[A-Za-z]{3,9}-\d{4}", na=False)
        if mask_mon_yyyy.any():
            mon_yyyy = pd.to_datetime(normalized.where(mask_mon_yyyy), format="%b-%Y", errors="coerce")
            parsed = parsed.where(parsed.notna(), mon_yyyy)

        # Day-month labels without year are ambiguous for analytics monthly buckets.
        # Do not let pandas/dateutil inject current year for these strings.
        ambiguous_day_month = normalized.str.fullmatch(r"\d{1,2}-[A-Za-z]{3,9}", na=False)

        try:
            generic = pd.to_datetime(cleaned.mask(ambiguous_day_month), format="mixed", errors="coerce")
            generic_dayfirst = pd.to_datetime(cleaned.mask(ambiguous_day_month), format="mixed", errors="coerce", dayfirst=True)
        except TypeError:
            generic = pd.to_datetime(cleaned.mask(ambiguous_day_month), errors="coerce")
            generic_dayfirst = pd.to_datetime(cleaned.mask(ambiguous_day_month), errors="coerce", dayfirst=True)

        # Prefer day-first interpretation when it clearly parses more values.
        if int(generic_dayfirst.notna().sum()) > int(generic.notna().sum()):
            generic = generic_dayfirst

        generic = generic.where(generic.dt.year >= 2000)
        parsed = parsed.where(parsed.notna(), generic)
        return parsed

    def _coalesce_datetime(candidates: tuple[str, ...]) -> pd.Series:
        out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        for col in candidates:
            if col not in df.columns:
                continue
            parsed = _parse_datetime_series(df[col])
            if parsed.notna().any():
                out = out.where(out.notna(), parsed)
        return out

    def _upsert_datetime_column(target: str, values: pd.Series) -> None:
        nonlocal touched
        if values is None or values.notna().sum() == 0:
            return
        if target not in df.columns:
            df[target] = values
            touched += 1
            return
        current = _parse_datetime_series(df[target])
        mask = current.isna() & values.notna()
        if mask.any():
            current.loc[mask] = values.loc[mask]
            df[target] = current
            touched += 1

    def _upsert_numeric_column(target: str, values: pd.Series) -> None:
        nonlocal touched
        if values is None:
            return
        numeric = _as_numeric(values)
        if target not in df.columns:
            if numeric.ne(0.0).any():
                df[target] = numeric
                touched += 1
            return
        current = _as_numeric(df[target])
        mask = current.eq(0.0) & numeric.ne(0.0)
        if mask.any():
            current.loc[mask] = numeric.loc[mask]
            df[target] = current
            touched += 1
        elif not pd.api.types.is_numeric_dtype(df[target]):
            df[target] = current

    def _copy_alias_if_missing(target: str, source_col: str) -> None:
        nonlocal touched
        if source_col not in df.columns:
            return
        if target not in df.columns:
            df[target] = df[source_col]
            touched += 1

    start_dt = _coalesce_datetime(
        (
            "Start_Date",
            "Plan Start Date",
            "Warranty Start Date",
            "Warranty_Start_Date",
            "Start Date",
            "Date",
            "Invoice Date",
            "Invoice_Date_",
            "Warranty Purchase Date",
            "Purchase Date",
        )
    )
    end_dt = _coalesce_datetime(
        (
            "End_Date",
            "Plan End Date",
            "Warranty End Date",
            "Warranty_End_Date",
            "End Date",
        )
    )
    month_dt = _coalesce_datetime(
        (
            "Month",
            "Month Name",
            "Month_Name",
            "Month-Year",
            "Month Year",
            "Month_Year",
            "Fiscal Month",
        )
    )
    if month_dt.notna().sum() == 0:
        month_dt = start_dt
    month_bucket = month_dt.dt.to_period("M").dt.to_timestamp()

    _upsert_datetime_column("Start_Date", start_dt)
    _upsert_datetime_column("Plan Start Date", start_dt)
    _upsert_datetime_column("Start Date", start_dt)
    _upsert_datetime_column("Date", start_dt.where(start_dt.notna(), month_bucket))
    _upsert_datetime_column("End_Date", end_dt)
    _upsert_datetime_column("Plan End Date", end_dt)
    _upsert_datetime_column("End Date", end_dt)
    _upsert_datetime_column("Month", month_bucket)

    if dataset_type == "sales":
        plan_price = _coalesce_numeric(
            df,
            (
                "Plan Selling Price",
                "Plan Price",
                "Plan MRP",
                "Amount",
                "Gross Premium",
                "Customer Premium",
                "Invoice Value",
                "INVOICE_VALUE",
            ),
            default=0.0,
        )
        _upsert_numeric_column("Plan Selling Price", plan_price)
        _upsert_numeric_column("Amount", plan_price)
        _upsert_numeric_column("Gross Premium", plan_price)

        if source == "godrej" and "Customer Premium" in df.columns:
            premium = _as_numeric(df["Customer Premium"])
            _upsert_numeric_column("Plan Selling Price", premium)
            _upsert_numeric_column("Amount", premium)
            _upsert_numeric_column("Gross Premium", premium)

    if dataset_type == "claims":
        claim_value = _coalesce_numeric(
            df,
            (
                "Claim_Amount",
                "Net Amount",
                "Net_Amount",
                "Claim Amount",
                "Claims Cost",
                "Claim Cost",
                "Zopper's Cost",
                "Zoppers Cost",
            ),
            default=0.0,
        )
        _upsert_numeric_column("Claim_Amount", claim_value)
        _upsert_numeric_column("Net Amount", claim_value)

        claim_date = _coalesce_datetime(
            (
                "Claim Date",
                "Call_Date",
                "Call Date",
                "Day of Call_Date",
                "Payment_date",
                "Payment Date",
                "Date",
                "Month",
            )
        )
        _upsert_datetime_column("Claim Date", claim_date)
        # Claims month must be derived from claim dates to avoid fiscal/posting
        # month collapsing all records into one bucket.
        claim_month = claim_date.dt.to_period("M").dt.to_timestamp()
        if claim_month.notna().any():
            df["Month"] = claim_month
            touched += 1

    if "Month" in df.columns:
        month_text = _parse_datetime_series(df["Month"]).dt.strftime("%b-%y").fillna("")
        if month_text.astype(str).str.strip().ne("").any():
            if "Month Name" not in df.columns or not df["Month Name"].astype(str).str.strip().eq(month_text.astype(str).str.strip()).all():
                df["Month Name"] = month_text
                touched += 1
            if "Month_Name" not in df.columns or not df["Month_Name"].astype(str).str.strip().eq(month_text.astype(str).str.strip()).all():
                df["Month_Name"] = month_text
                touched += 1

    # Dimension compatibility used by deck studio / deck export.
    _copy_alias_if_missing("Plan_Category", "Plan Category")
    _copy_alias_if_missing("Device_Plan_Category", "Device Plan Category")
    _copy_alias_if_missing("model_code", "Model Code")
    _copy_alias_if_missing("Model Code", "model_code")
    _copy_alias_if_missing("Product Category", "Product_Category")
    _copy_alias_if_missing("Product_Category", "Product Category")

    # Keep alias columns synchronized to canonical columns so downstream
    # engines that prioritize alias fields do not pick stale values.
    if "Plan Category" in df.columns:
        canonical = df["Plan Category"].astype(str).fillna("").str.strip()
        alias = df["Plan_Category"].astype(str).fillna("").str.strip() if "Plan_Category" in df.columns else pd.Series("", index=df.index)
        if "Plan_Category" not in df.columns or not alias.eq(canonical).all():
            df["Plan_Category"] = df["Plan Category"]
            touched += 1
    if "Device Plan Category" in df.columns:
        canonical = df["Device Plan Category"].astype(str).fillna("").str.strip()
        alias = df["Device_Plan_Category"].astype(str).fillna("").str.strip() if "Device_Plan_Category" in df.columns else pd.Series("", index=df.index)
        if "Device_Plan_Category" not in df.columns or not alias.eq(canonical).all():
            df["Device_Plan_Category"] = df["Device Plan Category"]
            touched += 1

    if source == "reliance":
        if "ARTICLE_BRAND" not in df.columns:
            brand = _coalesce_text(
                df,
                (
                    "ARTICLE_BRAND",
                    "Article Brand",
                    "Article_Brand",
                    "Brand",
                    "Item_Brand",
                ),
                default="",
            )
            if brand.astype(str).str.strip().ne("").any():
                df["ARTICLE_BRAND"] = brand
                touched += 1

    return touched


def normalize_partner_dataframe(
    df: pd.DataFrame,
    *,
    source: str,
    dataset_type: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_key = _normalize_source(source)
    dataset_key = _normalize_dataset_type(dataset_type)

    if df is None or df.empty:
        return pd.DataFrame(), {
            "source": source_key,
            "dataset_type": dataset_key,
            "rows": 0,
            "columns": 0,
            "smart_mapping": {"applied": 0, "required_found": 0, "required_total": 0, "coverage": 0.0},
            "columns_touched": 0,
        }

    work = df.copy()
    work.columns = [str(col).strip() for col in work.columns]

    smart_meta = _apply_smart_reverse_mapping(work, source=source_key, dataset_type=dataset_key)
    touched = _apply_common_alias_coalescing(work)
    touched += _apply_state_city_mapping(work)

    if dataset_key == "sales" and source_key == "samsung":
        touched += _normalize_samsung_sales(work)
    elif dataset_key == "claims" and source_key == "samsung":
        touched += _normalize_samsung_claims(work)
    elif dataset_key == "sales" and source_key == "reliance":
        touched += _normalize_reliance_sales(work)
    elif dataset_key == "sales" and source_key == "godrej":
        touched += _normalize_godrej_sales(work)
    touched += _ensure_deck_compat_columns(work, source=source_key, dataset_type=dataset_key)

    metadata = {
        "source": source_key,
        "dataset_type": dataset_key,
        "rows": int(len(work)),
        "columns": int(len(work.columns)),
        "smart_mapping": smart_meta,
        "columns_touched": int(touched),
    }
    return work, metadata


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)

    return value


def dataframe_to_payload_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []

    safe_df = df.astype(object).where(pd.notnull(df), None)
    out: list[dict[str, Any]] = []
    for row in safe_df.to_dict(orient="records"):
        cleaned = {str(key): _json_safe(value) for key, value in row.items()}
        out.append(cleaned)
    return out


def normalize_partner_rows(
    rows: list[dict[str, Any]],
    *,
    source: str,
    dataset_type: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not rows:
        source_key = _normalize_source(source)
        dataset_key = _normalize_dataset_type(dataset_type)
        return [], {
            "source": source_key,
            "dataset_type": dataset_key,
            "rows": 0,
            "columns": 0,
            "smart_mapping": {"applied": 0, "required_found": 0, "required_total": 0, "coverage": 0.0},
            "columns_touched": 0,
        }

    df = pd.DataFrame(rows)
    normalized_df, metadata = normalize_partner_dataframe(df, source=source, dataset_type=dataset_type)
    return dataframe_to_payload_rows(normalized_df), metadata
