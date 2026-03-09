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
    if key in {"samsungvs", "samsungvijaysales", "samsungcroma", "samsungreliancedigital", "reliancedigital", "samsung"}:
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


COMPOSITE_KEY_SUPPORT_CANDIDATES: dict[str, list[str]] = {
    "sales": [
        "Invoice Number",
        "Invoice No",
        "Order ID",
        "Transaction ID",
        "Contract ID",
        "Serial Number",
        "IMEI",
        "Model Code",
        "Date",
        "Month",
        "Start Date",
        "Plan Start Date",
        "State",
        "City",
    ],
    "claims": [
        "Claim Date",
        "Date",
        "Month",
        "Case ID",
        "Ticket ID",
        "Reference Number",
        "SR Number",
        "Serial Number",
        "IMEI",
        "State",
        "City",
    ],
}


def _normalize_key_component(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "na"}:
        return ""
    return lowered


def _combined_key_score(
    df: pd.DataFrame,
    columns: list[str],
) -> tuple[float, int, int, float, float]:
    if not columns:
        return (0.0, 0, 0, 0.0, 0.0)

    normalized_parts: list[pd.Series] = []
    mask = pd.Series(False, index=df.index, dtype=bool)
    for col in columns:
        if col not in df.columns:
            continue
        part = df[col].map(_normalize_key_component)
        normalized_parts.append(part)
        mask = mask | part.ne("")

    if not normalized_parts:
        return (0.0, 0, 0, 0.0, 0.0)

    non_null = int(mask.sum())
    if non_null <= 0:
        return (0.0, 0, 0, 0.0, 0.0)

    combined = normalized_parts[0].astype(str)
    for part in normalized_parts[1:]:
        combined = combined + "|" + part.astype(str)

    filtered = combined[mask]
    unique_count = int(filtered.nunique(dropna=True))
    duplicate_count = max(0, non_null - unique_count)
    ratio = (unique_count / non_null) if non_null else 0.0
    coverage = (non_null / max(len(df), 1)) if len(df) else 0.0
    score = (ratio * 1000.0) + (coverage * 40.0) - (duplicate_count * 0.15)
    return (score, non_null, unique_count, ratio, coverage)


def _choose_composite_key_columns(
    df: pd.DataFrame,
    *,
    source: str,
    dataset_type: str,
) -> tuple[list[str], tuple[float, int, int, float, float]]:
    source_key = _normalize_source(source)
    dataset_key = _normalize_dataset_type(dataset_type)

    primary_candidates = _resolve_columns(
        df,
        get_primary_key_candidate_order(source=source_key, dataset_type=dataset_key),
    )
    support_candidates = [
        col
        for col in _resolve_columns(df, COMPOSITE_KEY_SUPPORT_CANDIDATES.get(dataset_key, []))
        if col not in primary_candidates
    ]
    candidate_pool = primary_candidates + support_candidates
    if len(candidate_pool) < 2:
        return [], (0.0, 0, 0, 0.0, 0.0)

    ranked_columns: list[tuple[float, str, tuple[float, int, int, float, float]]] = []
    for idx, col in enumerate(candidate_pool):
        stats = _combined_key_score(df, [col])
        if stats[1] <= 0:
            continue
        priority_bonus = max(0, len(candidate_pool) - idx) * 0.01
        ranked_columns.append((stats[0] + priority_bonus, col, stats))

    if not ranked_columns:
        return [], (0.0, 0, 0, 0.0, 0.0)

    ranked_columns.sort(key=lambda item: item[0], reverse=True)
    selected = [ranked_columns[0][1]]
    selected_stats = ranked_columns[0][2]
    remaining = [col for col in candidate_pool if col not in selected]

    while remaining and len(selected) < 4:
        best_next: tuple[float, str, tuple[float, int, int, float, float]] | None = None
        for col in remaining:
            trial = selected + [col]
            trial_stats = _combined_key_score(df, trial)
            if trial_stats[1] <= 0:
                continue
            ratio_gain = trial_stats[3] - selected_stats[3]
            score_gain = trial_stats[0] - selected_stats[0]
            if ratio_gain <= 0.01 and score_gain <= 1.0:
                continue
            candidate_score = trial_stats[0] + (8.0 if col in primary_candidates else 0.0)
            if best_next is None or candidate_score > best_next[0]:
                best_next = (candidate_score, col, trial_stats)

        if best_next is None:
            break

        _, next_col, next_stats = best_next
        selected.append(next_col)
        selected_stats = next_stats
        remaining = [col for col in remaining if col != next_col]
        if selected_stats[3] >= 0.98:
            break

    can_use_composite = (
        len(selected) >= 2
        and (
            selected_stats[3] >= 0.72
            or (
                selected_stats[3] >= 0.62
                and any(col in primary_candidates for col in selected)
            )
        )
    )
    return (selected if can_use_composite else []), selected_stats


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


def _assign_record_instances(
    base_record_keys: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[str], int]:
    if not base_record_keys:
        return [], 0

    grouped_indexes: dict[str, list[int]] = {}
    for idx, key in enumerate(base_record_keys):
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        grouped_indexes.setdefault(normalized_key, []).append(idx)

    row_fingerprints = [
        _stable_hash(row if isinstance(row, dict) else {"value": row})
        for row in rows
    ]

    resolved_keys = [str(key or "").strip() for key in base_record_keys]
    duplicate_keys = 0
    for base_key, indexes in grouped_indexes.items():
        if len(indexes) == 1:
            resolved_keys[indexes[0]] = base_key
            continue

        duplicate_keys += len(indexes) - 1
        ordered_indexes = sorted(indexes, key=lambda idx: (row_fingerprints[idx], idx))
        for occurrence, row_idx in enumerate(ordered_indexes, start=1):
            if occurrence == 1:
                resolved_keys[row_idx] = base_key
                continue
            resolved_keys[row_idx] = f"{base_key}:dup:{occurrence:04d}"

    return resolved_keys, int(duplicate_keys)


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
            "key_columns": [],
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
    composite_key_columns, composite_stats = _choose_composite_key_columns(
        working_df,
        source=source_key,
        dataset_type=dataset_key,
    )

    normalized_rows = rows
    record_keys: list[str] = []
    missing_key_values = 0
    hash_candidates = FALLBACK_HASH_CANDIDATES.get(dataset_key, [])
    natural_key_selected = key_meta.get("strategy") == "natural_column" and bool(key_col)
    active_strategy = "natural_column"
    active_key_columns: list[str] = [str(key_col)] if natural_key_selected and key_col else []
    active_uniqueness_ratio = float(key_meta.get("uniqueness_ratio") or 0.0)

    if not active_key_columns and composite_key_columns:
        active_strategy = "composite_candidate_columns"
        active_key_columns = list(composite_key_columns)
        active_uniqueness_ratio = round(float(composite_stats[3] or 0.0), 4)
    elif not active_key_columns:
        active_strategy = "composite_hash"

    role = str(
        _classify_key_role(dataset_key, active_key_columns[0] if active_key_columns else None)
        or ("claim_id" if dataset_key == "claims" else "plan_id")
    )

    if active_strategy == "natural_column" and key_col:
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
    elif active_strategy == "composite_candidate_columns" and active_key_columns:
        normalized_key_frame = working_df[active_key_columns].copy()
        for col in active_key_columns:
            normalized_key_frame[col] = normalized_key_frame[col].map(_normalize_key_component)
        missing_mask = normalized_key_frame.eq("").all(axis=1)
        missing_key_values = int(missing_mask.sum())
        for idx, row in enumerate(normalized_rows):
            if not bool(missing_mask.iloc[idx]):
                key_payload = _build_row_hash_payload(row, active_key_columns)
                digest = _stable_hash(key_payload)
                record_keys.append(f"{role}:composite:{digest[:24]}")
            else:
                fallback_payload = _build_row_hash_payload(row, hash_candidates)
                record_keys.append(f"{role}:row:{_stable_hash(fallback_payload)[:24]}")
    else:
        for row in normalized_rows:
            fallback_payload = _build_row_hash_payload(row, hash_candidates)
            record_keys.append(f"{role}:row:{_stable_hash(fallback_payload)[:24]}")

    record_keys, duplicate_keys = _assign_record_instances(record_keys, normalized_rows)
    with_keys: list[dict[str, Any]] = []
    for idx, row in enumerate(normalized_rows):
        rk = str(record_keys[idx] or "").strip()
        if not rk:
            continue
        with_keys.append(
            {
                "data": row,
                "record_key": rk,
                "primary_key_name": role,
            }
        )

    metadata = {
        "source": source_key,
        "dataset_type": dataset_key,
        "primary_key_name": role,
        "strategy": active_strategy,
        "key_column": key_col if active_strategy == "natural_column" else None,
        "key_columns": active_key_columns,
        "key_candidates": get_primary_key_candidate_order(source=source_key, dataset_type=dataset_key),
        "rows_in": int(len(normalized_rows)),
        "rows_out": int(len(with_keys)),
        "duplicate_keys_in_file": int(duplicate_keys),
        "missing_key_values": int(missing_key_values),
        "uniqueness_ratio": float(active_uniqueness_ratio or 0.0),
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
