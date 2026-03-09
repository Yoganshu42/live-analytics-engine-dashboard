from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from services.date_parsing import parse_flexible_datetime


def normalize(col: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(col or "").lower())


def _tokenize(col: str) -> list[str]:
    base = str(col or "")
    base = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", base)
    base = base.replace("_", " ").replace("/", " ").replace("-", " ")
    tokens = [t.strip().lower() for t in re.split(r"\s+", base) if t.strip()]
    return tokens


def _clean_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("INR", "", regex=False)
        .str.replace("Rs.", "", regex=False)
        .str.replace("Rs", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _datetime_ratio(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    try:
        parsed = pd.to_datetime(series, format="mixed", errors="coerce")
    except TypeError:
        parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        fallback = series.where(parsed.isna()).map(parse_flexible_datetime)
        parsed = parsed.where(parsed.notna(), fallback)
    return float(parsed.notna().sum()) / float(len(series))


def _text_ratio(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
    )
    return float(cleaned.notna().sum()) / float(len(series))


def _samsung_device_category_ratio(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "null": pd.NA})
        .dropna()
    )
    if cleaned.empty:
        return 0.0

    normalized = (
        cleaned
        .str.replace("_", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    matches = (
        normalized.eq("mass")
        | normalized.eq("mid")
        | normalized.eq("high")
        | normalized.eq("premium")
        | normalized.eq("super premium")
        | normalized.eq("luxury flip")
        | normalized.eq("luxury fold")
    )
    return float(matches.mean()) if len(normalized) else 0.0


def _samsung_plan_like_ratio(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "null": pd.NA})
        .dropna()
    )
    if cleaned.empty:
        return 0.0

    normalized = (
        cleaned
        .str.replace("_", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    matches = (
        normalized.str.contains(r"\bcombo\b", na=False)
        | normalized.str.contains(r"\badld\b", na=False)
        | normalized.str.contains(r"\bew\b", na=False)
        | normalized.str.contains(r"\bextended warranty\b", na=False)
        | normalized.str.contains(r"\bscreen protection\b", na=False)
        | normalized.str.contains(r"\bprotect max\b", na=False)
    )
    return float(matches.mean()) if len(normalized) else 0.0


def _sample_values(series: pd.Series, limit: int = 3) -> list[str]:
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
        .dropna()
    )
    values: list[str] = []
    for value in cleaned.head(25).tolist():
        if value in values:
            continue
        values.append(value)
        if len(values) >= limit:
            break
    return values


@dataclass(frozen=True)
class FieldRule:
    field: str
    aliases: tuple[str, ...]
    keywords: tuple[str, ...]
    expected_type: str = "text"  # text | numeric | date
    required: bool = False


def _rules(*items: FieldRule) -> list[FieldRule]:
    return list(items)


SOURCE_ALIASES = {
    "relianceresq": "reliance",
    "resq": "reliance",
    "goodrej": "godrej",
    "goddrej": "godrej",
    "samsungvs": "samsung",
    "samsungvijaysales": "samsung",
    "samsungcroma": "samsung",
    "samsungreliancedigital": "samsung",
    "reliancedigital": "samsung",
}


PROFILE_RULES: dict[tuple[str, str], list[FieldRule]] = {
    ("reliance", "sales"): _rules(
        FieldRule(
            field="gross_premium",
            aliases=("plan price", "plan selling price", "gross premium", "invoice value", "invoice_value"),
            keywords=("plan", "price", "premium", "gross", "invoice"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="brand",
            aliases=("article_brand", "article brand", "item_brand", "product brand", "brand"),
            keywords=("brand", "article", "product"),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="plan_start_date",
            aliases=("plan start date", "warranty start date", "purchase date", "invoice date", "month"),
            keywords=("start", "purchase", "invoice", "month", "date"),
            expected_type="date",
            required=True,
        ),
        FieldRule(
            field="plan_end_date",
            aliases=("plan end date", "warranty end date", "end date"),
            keywords=("end", "expiry", "date"),
            expected_type="date",
            required=False,
        ),
        FieldRule(
            field="zopper_transfer_price",
            aliases=("total billing amount", "billing amount", "zopper shared transfer price", "zopper shared ( transfer price )", "zopper share"),
            keywords=("zopper", "transfer", "share", "billing", "amount"),
            expected_type="numeric",
            required=False,
        ),
        FieldRule(
            field="plan_category",
            aliases=("plan type", "plan category", "warranty type", "plan_name"),
            keywords=("plan", "warranty", "type", "category"),
            expected_type="text",
            required=False,
        ),
    ),
    ("reliance", "claims"): _rules(
        FieldRule(
            field="claims_cost",
            aliases=("zopper's cost", "claim_amount", "claim amount", "payment amount", "last_estimation_amount"),
            keywords=("claim", "cost", "payment", "amount"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="brand",
            aliases=("product brand(group)", "product brand", "article_brand", "item_brand", "brand"),
            keywords=("brand", "product", "article"),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="claim_month",
            aliases=("month", "month_year", "day of call_date", "call_registered_date"),
            keywords=("month", "call", "date", "registered"),
            expected_type="date",
            required=True,
        ),
        FieldRule(
            field="deductible",
            aliases=("one time deductible", "otd amount", "deductible"),
            keywords=("deductible", "otd"),
            expected_type="numeric",
            required=False,
        ),
    ),
    ("samsung", "sales"): _rules(
        FieldRule(
            field="gross_premium",
            aliases=("amount", "gross premium", "plan selling price", "customer premium"),
            keywords=("amount", "premium", "plan", "price", "gross"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="brand",
            aliases=("brand", "article_brand", "item_brand", "product brand"),
            keywords=("brand", "article", "product"),
            expected_type="text",
            required=False,
        ),
        FieldRule(
            field="plan_category",
            aliases=("plan category", "plan_category", "plan type", "warranty type", "pack type"),
            keywords=("plan", "category", "type", "warranty", "pack"),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="device_plan_category",
            aliases=("device plan category", "device_plan_category", "device category", "article_brick"),
            keywords=("device", "plan", "category", "brick"),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="start_date",
            aliases=("start_date", "start date", "plan start date", "warranty start date", "transaction date", "date"),
            keywords=("start", "purchase", "invoice", "transaction", "date", "warranty"),
            expected_type="date",
            required=True,
        ),
        FieldRule(
            field="end_date",
            aliases=("end_date", "end date", "plan end date", "warranty end date"),
            keywords=("end", "expiry", "date", "warranty"),
            expected_type="date",
            required=False,
        ),
        FieldRule(
            field="zopper_share",
            aliases=("zopper share", "zopper shared ( transfer price )", "transfer_price"),
            keywords=("zopper", "transfer", "share"),
            expected_type="numeric",
            required=False,
        ),
    ),
    ("samsung", "claims"): _rules(
        FieldRule(
            field="claims_cost",
            aliases=("net amount", "claim amount", "claim_amount"),
            keywords=("claim", "net", "amount"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="deductible",
            aliases=("otd amount", "otd_amount", "one time deductible"),
            keywords=("otd", "deductible"),
            expected_type="numeric",
            required=False,
        ),
        FieldRule(
            field="claim_date",
            aliases=("day of call_date", "call_date", "month", "month_year"),
            keywords=("day", "call", "month", "date"),
            expected_type="date",
            required=True,
        ),
        FieldRule(
            field="plan_category",
            aliases=("plan category", "warranty type"),
            keywords=("plan", "warranty", "category"),
            expected_type="text",
            required=False,
        ),
        FieldRule(
            field="brand",
            aliases=("brand", "product brand", "item_brand"),
            keywords=("brand", "product"),
            expected_type="text",
            required=False,
        ),
    ),
    ("godrej", "sales"): _rules(
        FieldRule(
            field="gross_premium",
            aliases=("customer premium", "customer_premium", "premium"),
            keywords=("customer", "premium", "amount"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="channel",
            aliases=("channel", "channel name", "channel_name"),
            keywords=("channel",),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="product_category",
            aliases=("product category", "product_category", "category"),
            keywords=("product", "category"),
            expected_type="text",
            required=False,
        ),
        FieldRule(
            field="warranty_start_date",
            aliases=("warranty start date", "start date", "start_date"),
            keywords=("start", "warranty", "date"),
            expected_type="date",
            required=True,
        ),
        FieldRule(
            field="warranty_end_date",
            aliases=("warranty end date", "end date", "end_date"),
            keywords=("end", "warranty", "date"),
            expected_type="date",
            required=False,
        ),
        FieldRule(
            field="activation_code",
            aliases=("warranty activation code", "activation code", "activation_code"),
            keywords=("activation", "code", "warranty"),
            expected_type="text",
            required=False,
        ),
    ),
    ("godrej", "claims"): _rules(
        FieldRule(
            field="claim_amount",
            aliases=("claim_amount", "claim amount", "net claim amount", "payment amount"),
            keywords=("claim", "amount", "payment"),
            expected_type="numeric",
            required=True,
        ),
        FieldRule(
            field="channel",
            aliases=("channel", "channel name", "channel_name"),
            keywords=("channel",),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="product_category",
            aliases=("product_category", "product category", "prodcut category", "category"),
            keywords=("product", "category"),
            expected_type="text",
            required=True,
        ),
        FieldRule(
            field="month",
            aliases=("month", "month name", "month_name", "payment date", "claim date"),
            keywords=("month", "date", "claim", "payment"),
            expected_type="date",
            required=True,
        ),
    ),
}


DEFAULT_PROFILE = _rules(
    FieldRule(
        field="gross_premium",
        aliases=("gross premium", "total billing amount", "amount", "premium"),
        keywords=("gross", "billing", "amount", "premium"),
        expected_type="numeric",
        required=True,
    ),
    FieldRule(
        field="brand",
        aliases=("brand", "article_brand", "product brand"),
        keywords=("brand", "article", "product"),
        expected_type="text",
        required=True,
    ),
)


def _resolve_profile(source: str, dataset_type: str) -> tuple[str, str, list[FieldRule]]:
    source_key = normalize(source)
    dataset_key = normalize(dataset_type)
    source_key = SOURCE_ALIASES.get(source_key, source_key)
    dataset_key = "claims" if "claim" in dataset_key else "sales"
    profile = PROFILE_RULES.get((source_key, dataset_key), DEFAULT_PROFILE)
    return source_key, dataset_key, profile


def _score_candidate(rule: FieldRule, col_name: str, series: pd.Series) -> dict[str, Any]:
    norm_col = normalize(col_name)
    tokens = set(_tokenize(col_name))
    score = 0.0
    reasons: list[str] = []

    alias_hit = False
    for alias in rule.aliases:
        alias_norm = normalize(alias)
        if not alias_norm:
            continue
        if norm_col == alias_norm:
            score += 10.0
            alias_hit = True
            reasons.append(f"exact alias: {alias}")
            break
    if not alias_hit:
        for alias in rule.aliases:
            alias_norm = normalize(alias)
            if not alias_norm:
                continue
            if alias_norm in norm_col or norm_col in alias_norm:
                score += 4.0
                reasons.append(f"partial alias: {alias}")
                break

    keyword_hits = 0
    for keyword in rule.keywords:
        keyword_norm = normalize(keyword)
        if not keyword_norm:
            continue
        if keyword_norm in norm_col or keyword.lower() in tokens:
            keyword_hits += 1
    if keyword_hits > 0:
        score += min(4.0, keyword_hits * 1.0)
        reasons.append(f"keyword hits: {keyword_hits}")

    if rule.expected_type == "numeric":
        numeric = _clean_numeric(series)
        ratio = float(numeric.notna().sum()) / float(max(len(series), 1))
        if ratio >= 0.75:
            score += 3.0
            reasons.append("mostly numeric values")
        elif ratio >= 0.35:
            score += 1.5
            reasons.append("partially numeric values")
        else:
            score -= 1.0
            reasons.append("low numeric signal")
    elif rule.expected_type == "date":
        ratio = _datetime_ratio(series)
        if ratio >= 0.75:
            score += 3.0
            reasons.append("mostly date-like values")
        elif ratio >= 0.35:
            score += 1.5
            reasons.append("partially date-like values")
        else:
            score -= 1.0
            reasons.append("low date signal")
    else:
        ratio = _text_ratio(series)
        if ratio >= 0.7:
            score += 1.5
            reasons.append("strong text signal")
        elif ratio >= 0.35:
            score += 0.5
            reasons.append("moderate text signal")

    if rule.field == "device_plan_category":
        device_ratio = _samsung_device_category_ratio(series)
        plan_like_ratio = _samsung_plan_like_ratio(series)
        if device_ratio >= 0.6:
            score += 3.0
            reasons.append("values match samsung device segments")
        elif device_ratio >= 0.25:
            score += 1.5
            reasons.append("some values match samsung device segments")
        if plan_like_ratio >= 0.5 and device_ratio < 0.35:
            score -= 10.0
            reasons.append("values look like plan labels, not device segments")

    fill_ratio = _text_ratio(series)
    score += fill_ratio
    if fill_ratio >= 0.75:
        reasons.append("high fill ratio")

    confidence = max(0.0, min(0.99, (score + 1.0) / 16.0))

    return {
        "column": col_name,
        "score": round(score, 4),
        "confidence": round(confidence, 4),
        "reasons": reasons,
        "sample_values": _sample_values(series),
    }


def suggest_reverse_mapping(
    df: pd.DataFrame,
    *,
    source: str,
    dataset_type: str,
) -> dict[str, Any]:
    source_key, dataset_key, rules = _resolve_profile(source, dataset_type)
    safe_df = df.copy()
    safe_df.columns = [str(c).strip() for c in safe_df.columns]

    mappings: list[dict[str, Any]] = []
    required_total = 0
    required_found = 0

    for rule in rules:
        if rule.required:
            required_total += 1

        scored: list[dict[str, Any]] = []
        for col in safe_df.columns:
            scored.append(_score_candidate(rule, col, safe_df[col]))

        scored.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)
        top = scored[0] if scored else None
        found = bool(top and top["score"] >= 2.0)

        if rule.required and found:
            required_found += 1

        mappings.append(
            {
                "field": rule.field,
                "required": rule.required,
                "found": found,
                "suggested_column": top["column"] if found and top else None,
                "confidence": float(top["confidence"]) if found and top else 0.0,
                "reasoning": top["reasons"] if top else [],
                "sample_values": top["sample_values"] if top else [],
                "candidates": scored[:4],
            }
        )

    coverage = 1.0 if required_total == 0 else float(required_found) / float(required_total)
    can_reverse_map = coverage >= 0.999

    if can_reverse_map:
        message = "Reverse mapping is possible for this upload."
    elif required_found == 0:
        message = "Required fields were not recognized. Please verify source or column headers."
    else:
        message = "Partial mapping found. Please confirm missing required fields."

    return {
        "source": source_key,
        "dataset_type": dataset_key,
        "total_rows": int(len(safe_df)),
        "total_columns": int(len(safe_df.columns)),
        "required_fields_found": int(required_found),
        "required_fields_total": int(required_total),
        "coverage": round(coverage, 4),
        "can_reverse_map": can_reverse_map,
        "message": message,
        "mappings": mappings,
    }


def suggest_gross_premium(df: pd.DataFrame) -> dict[str, Any]:
    rule = FieldRule(
        field="gross_premium",
        aliases=("total billing amount", "billing amount", "gross premium", "plan selling price", "amount", "premium"),
        keywords=("gross", "billing", "amount", "premium", "plan"),
        expected_type="numeric",
        required=True,
    )

    scored: list[dict[str, Any]] = []
    for col in df.columns:
        scored.append(_score_candidate(rule, str(col), df[col]))
    scored.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)

    top = scored[0] if scored else None
    found = bool(top and top["score"] >= 2.0)
    confidence = float(top["confidence"]) if found and top else 0.0

    return {
        "operation": "gross_premium",
        "confidence": confidence,
        "suggested_column": top["column"] if found and top else None,
        "null_strategy": "fill_zero",
        "reasoning": top["reasons"] if top else [],
        "candidates": scored[:5],
    }
