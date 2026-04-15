from __future__ import annotations

import re
from typing import Any

import pandas as pd

RELIANCE_SOURCE_ALIASES = {
    "reliance",
    "reliance resq",
    "reliance_resq",
    "reliance-resq",
    "resq",
}

RELIANCE_BRAND_FIELDS: tuple[str, ...] = (
    "ARTICLE_BRAND",
    "Article_Brand",
    "Article Brand",
    "Brand",
    "Item_Brand",
    "Product Brand(Group)",
    "Product Brand (Group)",
    "Product Brand",
)

_RELIANCE_BRAND_EXACT_MAP = {
    "idea": "Lenovo",
    "pad": "Redmi",
    "googlepixel": "Google",
    "oppo": "Oppo",
    "op": "Oppo",
    "vivo": "Vivo",
    "moto": "Motorola",
    "motorola": "Motorola",
    "realme": "Realme",
    "mi": "Redmi",
    "redmi": "Redmi",
    "xiaomi": "Redmi",
    "apple": "Apple",
    "iphone": "Apple",
    "samsung": "Samsung",
    "nothing": "Nothing",
    "oneplus": "OnePlus",
    "onepluslite": "OnePlus",
    "onepluslite8": "OnePlus",
    "len": "Lenovo",
    "lenovo": "Lenovo",
    "iqoo": "iQOO",
}


def is_reliance_source(source: str | None) -> bool:
    return (source or "").strip().lower() in RELIANCE_SOURCE_ALIASES


def _canonical_brand_from_text(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return ""

    normalized = re.sub(r"[^a-z0-9]", "", cleaned.lower())
    if not normalized:
        return cleaned
    mapped = _RELIANCE_BRAND_EXACT_MAP.get(normalized)
    if mapped:
        return mapped
    prefix_rules = (
        ("googlepixel", "Google"),
        ("oneplus", "OnePlus"),
        ("samsung", "Samsung"),
        ("apple", "Apple"),
        ("iphone", "Apple"),
        ("oppo", "Oppo"),
        ("vivo", "Vivo"),
        ("motorola", "Motorola"),
        ("moto", "Motorola"),
        ("realme", "Realme"),
        ("redmi", "Redmi"),
        ("xiaomi", "Redmi"),
        ("nothing", "Nothing"),
        ("lenovo", "Lenovo"),
        ("iqoo", "iQOO"),
    )
    for prefix, canonical in prefix_rules:
        if normalized.startswith(prefix):
            return canonical
    return cleaned


def canonicalize_reliance_brand_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return value
    except Exception:
        pass

    if isinstance(value, str):
        canonical = _canonical_brand_from_text(value)
        return canonical or value
    return value


def canonicalize_reliance_brand_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    if series.empty:
        return series
    return series.map(canonicalize_reliance_brand_value)


def canonicalize_reliance_brand_columns(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    touched = 0
    for column in RELIANCE_BRAND_FIELDS:
        if column not in df.columns:
            continue
        current = df[column]
        canonical = canonicalize_reliance_brand_series(current)
        current_text = current.astype(str).fillna("").str.strip()
        canonical_text = canonical.astype(str).fillna("").str.strip()
        if not current_text.eq(canonical_text).all():
            df[column] = canonical
            touched += 1
    return touched


def canonicalize_reliance_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    cleaned = dict(payload)
    for field in RELIANCE_BRAND_FIELDS:
        if field in cleaned:
            cleaned[field] = canonicalize_reliance_brand_value(cleaned.get(field))
    return cleaned
