from __future__ import annotations

from collections import OrderedDict
from typing import Generic, TypeVar

import pandas as pd

K = TypeVar("K")
V = TypeVar("V")

_FLOAT64_PRESERVE_TOKENS = {
    "amount",
    "billing",
    "claim",
    "commission",
    "cost",
    "deduct",
    "earned",
    "gross",
    "gst",
    "loss_ratio",
    "net",
    "premium",
    "price",
    "rate",
    "revenue",
    "share",
    "tax",
    "transfer",
    "value",
}


class BoundedTTLCache(Generic[K, V]):
    def __init__(self, *, max_items: int):
        self.max_items = max(1, int(max_items))
        self._items: OrderedDict[K, tuple[float, V]] = OrderedDict()

    def get(self, key: K, *, now: float) -> V | None:
        cached = self._items.get(key)
        if cached is None:
            return None
        expires_at, value = cached
        if expires_at < now:
            self._items.pop(key, None)
            return None
        self._items.move_to_end(key)
        return value

    def set(self, key: K, value: V, *, expires_at: float, now: float) -> None:
        self._evict_expired(now)
        if key in self._items:
            self._items.pop(key, None)
        self._items[key] = (expires_at, value)
        self._items.move_to_end(key)
        while len(self._items) > self.max_items:
            self._items.popitem(last=False)

    def pop(self, key: K, default: V | None = None) -> V | None:
        cached = self._items.pop(key, None)
        if cached is None:
            return default
        return cached[1]

    def clear(self) -> None:
        self._items.clear()

    def keys(self) -> list[K]:
        return list(self._items.keys())

    def _evict_expired(self, now: float) -> None:
        expired_keys = [key for key, (expires_at, _value) in self._items.items() if expires_at < now]
        for key in expired_keys:
            self._items.pop(key, None)


def _safe_key(value: object) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())


def _preserve_float64(column_name: object) -> bool:
    safe_name = _safe_key(column_name)
    return any(token in safe_name for token in _FLOAT64_PRESERVE_TOKENS)


def _maybe_category(series: pd.Series) -> pd.Series | None:
    non_null = series.dropna()
    if non_null.empty or len(series) < 128:
        return None

    sample = non_null.head(128)
    if not sample.map(lambda value: isinstance(value, str)).all():
        return None

    unique_count = int(non_null.nunique(dropna=True))
    if unique_count <= 1:
        return series.astype("category")

    unique_ratio = unique_count / max(len(non_null), 1)
    if unique_count > 128 or unique_ratio > 0.25:
        return None

    return series.astype("category")


def compact_dataframe_memory(
    df: pd.DataFrame,
    *,
    allow_category: bool = True,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    for column in df.columns:
        series = df[column]
        if pd.api.types.is_bool_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
            continue

        if pd.api.types.is_integer_dtype(series):
            downcast = "unsigned" if bool(series.min(skipna=True) >= 0) else "integer"
            compacted = pd.to_numeric(series, downcast=downcast)
            if getattr(compacted.dtype, "itemsize", 8) < getattr(series.dtype, "itemsize", 8):
                df[column] = compacted
            continue

        if pd.api.types.is_float_dtype(series):
            if _preserve_float64(column):
                continue
            compacted = pd.to_numeric(series, downcast="float")
            if getattr(compacted.dtype, "itemsize", 8) < getattr(series.dtype, "itemsize", 8):
                df[column] = compacted
            continue

        if allow_category and pd.api.types.is_object_dtype(series):
            compacted = _maybe_category(series)
            if compacted is not None:
                df[column] = compacted

    return df


def compact_dataframe_mapping(
    frames: dict[str, pd.DataFrame],
    *,
    allow_category: bool = True,
) -> dict[str, pd.DataFrame]:
    for key, frame in list(frames.items()):
        if isinstance(frame, pd.DataFrame):
            frames[key] = compact_dataframe_memory(frame, allow_category=allow_category)
    return frames
