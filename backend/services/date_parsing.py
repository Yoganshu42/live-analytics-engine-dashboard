from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import pandas as pd

_NULL_TEXT = {"", "nan", "none", "null", "nat"}
_DATE_TOKEN_PATTERN = re.compile(
    r"\d{4}[-/]\d{1,2}[-/]\d{1,2}"
    r"|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}"
    r"|\d{8}"
)
_LABELED_SAMSUNG_DATE_PATTERN = re.compile(
    r"(?P<label>extended warranty|screen protection|accidental(?: damage)?(?: and)? liquid|adld|combo|ew|sp)"
    r"\s*[:=\-]?\s*"
    r"(?P<date>\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{8})",
    re.IGNORECASE,
)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _parse_candidate_date(text: str) -> pd.Timestamp:
    candidate = (text or "").strip()
    if not candidate:
        return pd.NaT
    try:
        parsed = pd.to_datetime(candidate, format="mixed", errors="coerce")
    except TypeError:
        parsed = pd.to_datetime(candidate, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    timestamp = pd.Timestamp(parsed)
    return timestamp if timestamp.year >= 2000 else pd.NaT


def parse_flexible_datetime(value: Any) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value if not pd.isna(value) else pd.NaT
    if isinstance(value, (datetime, date)):
        timestamp = pd.Timestamp(value)
        return timestamp if timestamp.year >= 2000 else pd.NaT

    text = _clean_text(value)
    if not text or text.lower() in _NULL_TEXT:
        return pd.NaT

    direct = _parse_candidate_date(re.sub(r"\.0$", "", text))
    if not pd.isna(direct):
        return direct

    for token in _DATE_TOKEN_PATTERN.findall(text):
        parsed = _parse_candidate_date(token)
        if not pd.isna(parsed):
            return parsed
    return pd.NaT


def classify_samsung_plan_key(value: Any) -> str:
    text = re.sub(r"\s+", " ", _clean_text(value).lower())
    if not text or text in _NULL_TEXT:
        return ""
    if "combo" in text:
        return "combo"
    if "adld" in text or "accidental" in text or "liquid" in text:
        return "adld"
    if re.search(r"\bsp\b|\bspp\b", text) or "screen" in text or "crack" in text:
        return "screen_protection"
    if re.search(r"\bew\b", text) or "extended warranty" in text or text.startswith("ew") or "warranty" in text:
        return "ew"
    return ""


def canonicalize_samsung_plan_category(value: Any) -> str:
    key = classify_samsung_plan_key(value)
    return {
        "combo": "Combo",
        "adld": "ADLD",
        "screen_protection": "Screen Protection",
        "ew": "Extended Warranty",
    }.get(key, "")


def extract_labeled_dates(value: Any) -> dict[str, list[pd.Timestamp]]:
    text = _clean_text(value)
    if not text or text.lower() in _NULL_TEXT:
        return {}

    out: dict[str, list[pd.Timestamp]] = {}
    for match in _LABELED_SAMSUNG_DATE_PATTERN.finditer(text):
        label = classify_samsung_plan_key(match.group("label"))
        parsed = _parse_candidate_date(match.group("date"))
        if not label or pd.isna(parsed):
            continue
        out.setdefault(label, []).append(parsed)

    if out:
        return out

    for segment in re.split(r"[|;]+", text):
        label = classify_samsung_plan_key(segment)
        parsed = parse_flexible_datetime(segment)
        if not label or pd.isna(parsed):
            continue
        out.setdefault(label, []).append(parsed)
    return out


def _pick_labeled_date(
    labeled: dict[str, list[pd.Timestamp]],
    keys: tuple[str, ...],
    *,
    mode: str = "first",
) -> pd.Timestamp:
    candidates: list[pd.Timestamp] = []
    for key in keys:
        candidates.extend(labeled.get(key, []))
    if not candidates:
        return pd.NaT
    if mode == "min":
        return min(candidates)
    if mode == "max":
        return max(candidates)
    return candidates[0]


def resolve_samsung_plan_window(
    *,
    plan_type: Any,
    start_value: Any,
    end_value: Any,
    transaction_value: Any = None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    plan_key = classify_samsung_plan_key(plan_type)
    start_direct = parse_flexible_datetime(start_value)
    end_direct = parse_flexible_datetime(end_value)
    transaction_dt = parse_flexible_datetime(transaction_value)

    start_labeled = extract_labeled_dates(start_value)
    end_labeled = extract_labeled_dates(end_value)

    if plan_key == "combo":
        start_dt = _pick_labeled_date(start_labeled, ("adld", "screen_protection", "ew", "combo"), mode="min")
        end_dt = _pick_labeled_date(end_labeled, ("adld", "screen_protection", "ew", "combo"), mode="max")
        if pd.isna(start_dt):
            start_dt = start_direct if not pd.isna(start_direct) else transaction_dt
        if pd.isna(end_dt):
            end_dt = end_direct
        return start_dt, end_dt

    preferred_keys = {
        "adld": ("adld",),
        "screen_protection": ("screen_protection",),
        "ew": ("ew",),
    }.get(plan_key, ("adld", "screen_protection", "ew", "combo"))

    start_dt = _pick_labeled_date(start_labeled, preferred_keys)
    end_dt = _pick_labeled_date(end_labeled, preferred_keys)

    if pd.isna(start_dt):
        start_dt = start_direct
    if pd.isna(end_dt):
        end_dt = end_direct
    if pd.isna(start_dt) and plan_key in {"adld", "screen_protection"} and not pd.isna(transaction_dt):
        start_dt = transaction_dt

    return start_dt, end_dt
