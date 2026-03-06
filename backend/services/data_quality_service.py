from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import pandas as pd


def _clean_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def _normalize_source(source: str) -> str:
    key = _clean_key(source)
    if key in {"samsungvs", "samsungvijaysales", "samsungcroma", "samsung"}:
        return "samsung"
    if key in {"relianceresq", "reliance", "resq"}:
        return "reliance"
    if key in {"godrej", "goodrej", "goddrej"}:
        return "godrej"
    return key


def _normalize_dataset_type(dataset_type: str) -> str:
    key = _clean_key(dataset_type)
    return "claims" if "claim" in key else "sales"


def _missing_mask(series: pd.Series) -> pd.Series:
    as_text = series.astype(str).str.strip().str.lower()
    return series.isna() | as_text.isin({"", "nan", "none", "null", "na"})


def _resolve_columns(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    normalized_to_cols: dict[str, list[str]] = {}
    for col in df.columns:
        normalized_to_cols.setdefault(_clean_key(col), []).append(str(col))

    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        for col in normalized_to_cols.get(_clean_key(candidate), []):
            if col in seen:
                continue
            seen.add(col)
            out.append(col)
    return out


BASE_KEY_CANDIDATES: dict[str, list[str]] = {
    "sales": [
        "Plan ID",
        "Plan_Id",
        "PlanID",
        "Policy Number",
        "Policy No",
        "Policy ID",
        "Policy_Number",
        "Certificate Number",
        "Contract ID",
        "Invoice Number",
        "Invoice No",
        "Order ID",
        "Transaction ID",
        "Serial Number",
        "IMEI",
    ],
    "claims": [
        "Claim ID",
        "Claim_Id",
        "ClaimID",
        "Claim Number",
        "Claim No",
        "Claim_No",
        "Case ID",
        "Ticket ID",
        "Reference Number",
        "Reference No",
        "SR Number",
        "SR No",
        "Complaint ID",
    ],
}

SOURCE_KEY_CANDIDATES: dict[str, dict[str, list[str]]] = {
    "samsung": {
        "sales": [
            "Plan ID",
            "Policy Number",
            "Policy ID",
            "Certificate Number",
            "Invoice Number",
            "Serial Number",
            "IMEI",
        ],
        "claims": [
            "Claim ID",
            "Claim Number",
            "Case ID",
            "Ticket ID",
            "SR Number",
        ],
    },
    "reliance": {
        "sales": [
            "Plan ID",
            "Policy Number",
            "Policy ID",
            "Contract ID",
            "Invoice Number",
            "Order ID",
        ],
        "claims": [
            "Claim ID",
            "Claim Number",
            "Case ID",
            "Ticket ID",
            "Reference Number",
        ],
    },
    "godrej": {
        "sales": [
            "Plan ID",
            "Policy Number",
            "Contract ID",
            "Invoice Number",
            "Order ID",
        ],
        "claims": [
            "Claim ID",
            "Claim Number",
            "Case ID",
            "Ticket ID",
            "Reference Number",
        ],
    },
}


def get_primary_key_candidate_order(*, source: str, dataset_type: str) -> list[str]:
    source_key = _normalize_source(source)
    dataset_key = _normalize_dataset_type(dataset_type)
    source_candidates = SOURCE_KEY_CANDIDATES.get(source_key, {}).get(dataset_key, [])
    merged = source_candidates + [c for c in BASE_KEY_CANDIDATES.get(dataset_key, []) if c not in source_candidates]
    return list(merged)


def _classify_key_role(dataset_type: str, column_name: str | None) -> str:
    norm = _clean_key(column_name or "")
    if dataset_type == "claims":
        if "claim" in norm:
            return "claim_id"
        return "claim_key_fallback"
    if "plan" in norm or "policy" in norm:
        return "plan_id"
    return "plan_key_fallback"


def _key_score(series: pd.Series) -> tuple[float, int, int, float]:
    mask = ~_missing_mask(series)
    non_null = int(mask.sum())
    if non_null <= 0:
        return (0.0, 0, 0, 0.0)
    normalized = series[mask].astype(str).str.strip()
    unique_count = int(normalized.nunique(dropna=True))
    duplicate_count = max(0, non_null - unique_count)
    ratio = (unique_count / non_null) if non_null else 0.0
    score = (ratio * 1000.0) - (duplicate_count * 0.25)
    return (score, non_null, unique_count, ratio)


def _choose_primary_key_column(
    df: pd.DataFrame,
    *,
    source: str,
    dataset_type: str,
) -> tuple[str | None, dict[str, Any]]:
    source_key = _normalize_source(source)
    dataset_key = _normalize_dataset_type(dataset_type)

    source_candidates = SOURCE_KEY_CANDIDATES.get(source_key, {}).get(dataset_key, [])
    merged_candidates = source_candidates + [c for c in BASE_KEY_CANDIDATES.get(dataset_key, []) if c not in source_candidates]
    resolved = _resolve_columns(df, merged_candidates)
    if not resolved:
        return None, {
            "strategy": "composite_hash",
            "key_column": None,
            "primary_key_name": "claim_id_fallback" if dataset_key == "claims" else "plan_id_fallback",
            "non_null": 0,
            "unique": 0,
            "uniqueness_ratio": 0.0,
        }

    preferred_token = "claim" if dataset_key == "claims" else "plan"
    preferred_cols = [col for col in resolved if preferred_token in _clean_key(col)]
    evaluation_pool = preferred_cols or resolved

    best_col: str | None = None
    best_stats: tuple[float, int, int, float] = (0.0, 0, 0, 0.0)
    for col in evaluation_pool:
        stats = _key_score(df[col])
        if best_col is None or stats[0] > best_stats[0]:
            best_col = col
            best_stats = stats

    if best_col is None:
        return None, {
            "strategy": "composite_hash",
            "key_column": None,
            "primary_key_name": "claim_id_fallback" if dataset_key == "claims" else "plan_id_fallback",
            "non_null": 0,
            "unique": 0,
            "uniqueness_ratio": 0.0,
        }

    _, non_null, unique_count, ratio = best_stats
    if non_null <= 0:
        return None, {
            "strategy": "composite_hash",
            "key_column": None,
            "primary_key_name": "claim_id_fallback" if dataset_key == "claims" else "plan_id_fallback",
            "non_null": 0,
            "unique": 0,
            "uniqueness_ratio": 0.0,
        }

    strategy = "natural_column" if ratio >= 0.55 else "composite_hash"
    return best_col, {
        "strategy": strategy,
        "key_column": best_col if strategy == "natural_column" else None,
        "primary_key_name": _classify_key_role(dataset_key, best_col if strategy == "natural_column" else None),
        "non_null": non_null,
        "unique": unique_count,
        "uniqueness_ratio": round(float(ratio), 4),
    }


FALLBACK_HASH_CANDIDATES: dict[str, list[str]] = {
    "sales": [
        "Plan Category",
        "Device Plan Category",
        "Model Code",
        "State",
        "City",
        "Month",
        "Plan Selling Price",
        "Amount",
        "Gross Premium",
        "Date",
    ],
    "claims": [
        "Claim Date",
        "State",
        "City",
        "Plan Category",
        "Device Plan Category",
        "Month",
        "Claim_Amount",
        "Net Amount",
        "Date",
    ],
}


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _build_row_hash_payload(row: dict[str, Any], candidate_cols: list[str]) -> dict[str, Any]:
    if not candidate_cols:
        return row
    out: dict[str, Any] = {}
    normalized_map = {_clean_key(k): k for k in row.keys()}
    for candidate in candidate_cols:
        col = normalized_map.get(_clean_key(candidate))
        if col is None:
            continue
        out[col] = row.get(col)
    return out or row


def prepare_rows_for_storage(
    rows: list[dict[str, Any]],
    *,
    source: str,
    dataset_type: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_key = _normalize_source(source)
    dataset_key = _normalize_dataset_type(dataset_type)

    if not rows:
        return [], {
            "source": source_key,
            "dataset_type": dataset_key,
            "primary_key_name": "claim_id" if dataset_key == "claims" else "plan_id",
            "strategy": "composite_hash",
            "key_column": None,
            "key_candidates": get_primary_key_candidate_order(source=source_key, dataset_type=dataset_key),
            "rows_in": 0,
            "rows_out": 0,
            "duplicate_keys_in_file": 0,
            "missing_key_values": 0,
            "uniqueness_ratio": 0.0,
        }

    working_df = pd.DataFrame(rows)
    key_col, key_meta = _choose_primary_key_column(
        working_df,
        source=source_key,
        dataset_type=dataset_key,
    )

    normalized_rows = rows
    record_keys: list[str] = []
    missing_key_values = 0
    hash_candidates = FALLBACK_HASH_CANDIDATES.get(dataset_key, [])
    role = str(key_meta.get("primary_key_name") or ("claim_id" if dataset_key == "claims" else "plan_id"))

    if key_meta.get("strategy") == "natural_column" and key_col:
        series = working_df[key_col]
        missing_mask = _missing_mask(series)
        missing_key_values = int(missing_mask.sum())
        for idx, row in enumerate(normalized_rows):
            raw_value = "" if missing_mask.iloc[idx] else str(series.iloc[idx]).strip()
            if raw_value:
                key_payload = {"v": raw_value}
                digest = _stable_hash(key_payload)
                record_keys.append(f"{role}:{digest[:24]}")
            else:
                fallback_payload = _build_row_hash_payload(row, hash_candidates)
                record_keys.append(f"{role}:row:{_stable_hash(fallback_payload)[:24]}")
    else:
        for row in normalized_rows:
            fallback_payload = _build_row_hash_payload(row, hash_candidates)
            record_keys.append(f"{role}:row:{_stable_hash(fallback_payload)[:24]}")

    with_keys: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_keys = 0
    # keep latest row for duplicate record keys to make overwrite deterministic
    for idx in range(len(normalized_rows) - 1, -1, -1):
        rk = str(record_keys[idx] or "").strip()
        if not rk:
            continue
        if rk in seen:
            duplicate_keys += 1
            continue
        seen.add(rk)
        with_keys.append(
            {
                "data": normalized_rows[idx],
                "record_key": rk,
                "primary_key_name": role,
            }
        )
    with_keys.reverse()

    metadata = {
        "source": source_key,
        "dataset_type": dataset_key,
        "primary_key_name": role,
        "strategy": str(key_meta.get("strategy") or "composite_hash"),
        "key_column": key_meta.get("key_column"),
        "key_candidates": get_primary_key_candidate_order(source=source_key, dataset_type=dataset_key),
        "rows_in": int(len(normalized_rows)),
        "rows_out": int(len(with_keys)),
        "duplicate_keys_in_file": int(duplicate_keys),
        "missing_key_values": int(missing_key_values),
        "uniqueness_ratio": float(key_meta.get("uniqueness_ratio") or 0.0),
    }
    return with_keys, metadata


def inspect_primary_key_profile(
    rows: list[dict[str, Any]],
    *,
    source: str,
    dataset_type: str,
) -> dict[str, Any]:
    _, meta = prepare_rows_for_storage(rows, source=source, dataset_type=dataset_type)
    return meta
