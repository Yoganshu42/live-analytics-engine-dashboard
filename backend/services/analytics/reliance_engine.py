# services/analytics/reliance_engine.py

import logging
import os
import threading
import time
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.orm import Session

from models.data_rows import DataRow
from services.analytics.base_engine import BaseAnalyticsEngine
from services.analytics_repository import get_dataframe
from services.dataframe_optimization import BoundedTTLCache, compact_dataframe_mapping
from services.reliance_branding import canonicalize_reliance_brand_columns

logger = logging.getLogger(__name__)
RELIANCE_LOAD_CACHE_TTL_SECONDS = 300
RELIANCE_LOAD_CACHE_MAX_ITEMS = max(1, int(os.getenv("RELIANCE_LOAD_CACHE_MAX_ITEMS", "3")))
_reliance_load_cache_lock = threading.Lock()
_reliance_load_cache: BoundedTTLCache[
    tuple[str, str, str, str, str],
    dict[str, pd.DataFrame],
] = BoundedTTLCache(max_items=RELIANCE_LOAD_CACHE_MAX_ITEMS)
_reliance_load_inflight: dict[
    tuple[str, str, str, str, str],
    threading.Event,
] = {}

GST_MULTIPLIER = 1.18
LOSS_RATIO_CAP_PERCENT = 300.0
RELIANCE_TRANSFER_RATE = {
    ("ADLD", 12): 0.022,
    ("Crack Screen", 12): 0.014,
    ("Crack Screen", 24): 0.028,
    ("Extended Warranty", 6): 0.003,
    ("Extended Warranty", 12): 0.005,
}


def _build_reliance_transfer_bands() -> list[tuple[int, int, float]]:
    bands: list[tuple[int, int, float]] = []

    # 0-30k in 5k slabs
    for upper in range(5000, 30001, 5000):
        lower = 0 if upper == 5000 else upper - 4999
        avg = upper - 2500
        bands.append((lower, upper, avg))

    # 30k-210k in 10k slabs
    for upper in range(40000, 210001, 10000):
        lower = upper - 9999
        avg = upper - 5000
        bands.append((lower, upper, avg))

    return bands


RELIANCE_TRANSFER_BANDS = _build_reliance_transfer_bands()


def invalidate_reliance_load_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    source_scope: set[str] | None = None
    if source is not None:
        source_key = (source or "").strip().lower()
        if source_key in {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}:
            source_scope = {"reliance", "reliance resq", "reliance_resq", "reliance-resq", "resq"}
        else:
            source_scope = {source_key}

    dataset_scope = (dataset_type or "").strip().lower() if dataset_type is not None else None
    job_scope = (job_id or "").strip() if job_id is not None else None

    with _reliance_load_cache_lock:
        if source_scope is None and dataset_scope is None and job_scope is None:
            _reliance_load_cache.clear()
            return None

        keys_to_delete: list[tuple[str, str, str, str, str]] = []
        for key in _reliance_load_cache.keys():
            key_source, key_dataset, key_job, _from_key, _to_key = key
            if source_scope is not None and key_source not in source_scope:
                continue
            if dataset_scope is not None and key_dataset != dataset_scope:
                continue
            if job_scope is not None and key_job != job_scope:
                continue
            keys_to_delete.append(key)

        for key in keys_to_delete:
            _reliance_load_cache.pop(key, None)
    return None


class RelianceAnalyticsEngine(BaseAnalyticsEngine):
    """
    STRICTLY aligned with Reliance notebook logic.
    """

    def __init__(
        self,
        db: Session,
        job_id: str | None,
        source: str | None = "reliance",
        dataset_type: str | None = "sales",
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        super().__init__(db=db, job_id=job_id, source=source)
        self.dataset_type = dataset_type or "sales"
        self._loaded_data_cache: dict[str, pd.DataFrame] | None = None
        # The original notebook logic hard-coded a Jul-Dec 2025 window.
        # For the dashboard deployment we must respect the user-provided range,
        # and otherwise default to "no extra filtering" (let the API date-bounds drive it).
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else None
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else None
        if self.report_start is not None and pd.isna(self.report_start):
            self.report_start = None
        if self.report_end is not None and pd.isna(self.report_end):
            self.report_end = None
        if self.report_start is not None and self.report_end is not None and self.report_end < self.report_start:
            self.report_end = self.report_start
        today = pd.Timestamp.now().normalize()
        if self.report_end is None or self.report_end > today:
            self.report_end = today
        self.valuation_date = self._resolve_valuation_date(self.report_end)

    def _shared_load_cache_key(self) -> tuple[str, str, str, str, str]:
        valuation_key = self.valuation_date.date().isoformat() if self.valuation_date is not None else ""
        return (
            (self.source or "").strip().lower(),
            (self.dataset_type or "").strip().lower(),
            (self.job_id or "").strip(),
            valuation_key,
            "",
        )

    def run_analytics(self, df: pd.DataFrame, valuation_date: pd.Timestamp | None = None) -> dict:
        if df.empty:
            return {}

        df = self.normalize_sales(df)
        df = self.compute_premiums(df, valuation_date=valuation_date)
        # Assuming normalize_sales and compute_premiums are methods of the class,
        # or global functions that need to be imported.
        # If they are methods, they should be called with `self.`.
        # If they are global, they need to be defined or imported.
        # For now, assuming they are methods based on the context of an analytics engine.
        return {} # Placeholder, as the original snippet was incomplete for the return value.

    def _clone_loaded_data(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        return {
            "sales": data.get("sales", pd.DataFrame()).copy(deep=False),
            "claims": data.get("claims", pd.DataFrame()).copy(deep=False),
            "sales_ew": data.get("sales_ew", pd.DataFrame()).copy(deep=False),
        }

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------

    def _clean_number(self, series: pd.Series) -> pd.Series:
        if series is None:
            return pd.Series(dtype=float)
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce").fillna(0)
        return (
            series.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("INR", "", regex=False)
            .str.replace("Rs.", "", regex=False)
            .str.replace("Rs", "", regex=False)
            .str.strip()
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
        )

    @staticmethod
    def _resolve_valuation_date(report_end: pd.Timestamp | None) -> pd.Timestamp:
        today = pd.Timestamp.now().normalize()
        if report_end is None:
            return today
        candidate = pd.to_datetime(report_end, errors="coerce")
        if candidate is None or pd.isna(candidate):
            return today
        candidate_ts = pd.Timestamp(candidate).normalize()
        return candidate_ts if candidate_ts <= today else today

    def _is_ew_plan(self, df: pd.DataFrame) -> pd.Series:
        candidates = ["Plan Category", "Plan Type", "Device Plan Category"]
        for col in candidates:
            if col in df.columns:
                raw = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r"[^a-z0-9]+", " ", regex=True)
                    .str.replace(r"\s+", " ", regex=True)
                )
                return raw.isin({"ew", "extended warranty", "extendedwarranty"})
        return pd.Series(False, index=df.index)

    def _parse_month_series(self, series: pd.Series) -> pd.Series:
        raw = series.astype(str).str.strip()
        cleaned = raw.str.replace(r"\.0$", "", regex=True)

        parsed = pd.Series(pd.NaT, index=cleaned.index, dtype="datetime64[ns]")

        # Parse explicit month tokens first (e.g., Apr-25, Apr 2025) to avoid
        # pandas mixed-parser interpreting Apr-25 as year=0001/day=25.
        month_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        month_year = cleaned.str.extract(r"^(?:\d{1,2}[-/\s])?(?P<mon>[A-Za-z]{3,9})[-/\s](?P<yr>\d{2}|\d{4})$")
        month_num = month_year["mon"].str.lower().str.slice(0, 3).map(month_map)
        year_text = month_year["yr"]
        year_num = pd.to_numeric(year_text, errors="coerce")
        is_two_digit = year_text.fillna("").str.len().eq(2)
        year_num = year_num.where(~is_two_digit, year_num + 2000)
        explicit_month = pd.to_datetime(
            {"year": year_num, "month": month_num, "day": 1},
            errors="coerce",
        )
        parsed = parsed.fillna(explicit_month)

        # yyyymm numeric month (e.g., 202504)
        yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
        if yyyymm_mask.any():
            yyyymm_year = pd.to_numeric(cleaned.where(yyyymm_mask).str.slice(0, 4), errors="coerce")
            yyyymm_month = pd.to_numeric(cleaned.where(yyyymm_mask).str.slice(4, 6), errors="coerce")
            yyyymm_parsed = pd.to_datetime(
                {"year": yyyymm_year, "month": yyyymm_month, "day": 1},
                errors="coerce",
            )
            parsed = parsed.fillna(yyyymm_parsed)

        # Full datetime/date formats.
        if parsed.isna().any():
            for fmt in (
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%d-%b-%y",
                "%d-%b-%Y",
                "%d/%b/%y",
                "%d/%b/%Y",
                "%b-%y",
                "%b-%Y",
                "%b %y",
                "%b %Y",
                "%m-%Y",
                "%Y-%m",
            ):
                parsed_try = pd.to_datetime(cleaned, format=fmt, errors="coerce")
                parsed_try = parsed_try.where(parsed_try.dt.year >= 2000)
                parsed = parsed.fillna(parsed_try)
                if parsed.notna().all():
                    break
        if parsed.isna().any():
            try:
                parsed_try = pd.to_datetime(cleaned, format="mixed", errors="coerce")
            except TypeError:
                parsed_try = pd.to_datetime(cleaned, errors="coerce")
            parsed_try = parsed_try.where(parsed_try.dt.year >= 2000)
            parsed = parsed.fillna(parsed_try)

        # Do not guess year for ambiguous month-only labels like "Jun" or "6".
        # Those rows should be resolved from another explicit date column.
        ambiguous_month_only = cleaned.str.fullmatch(r"[A-Za-z]{3,9}") | cleaned.str.fullmatch(r"\d{1,2}")
        parsed = parsed.where(~ambiguous_month_only, pd.NaT)

        # Drop implausible years.
        parsed = parsed.where(parsed.dt.year >= 2000)

        return parsed

    def _month_key(self, series: pd.Series) -> pd.Series:
        dt = pd.to_datetime(series, errors="coerce")
        return dt.dt.to_period("M").dt.to_timestamp()

    def _finalize_month_dimension_output(self, out: pd.DataFrame) -> pd.DataFrame:
        if out.empty or "month" not in out.columns:
            return out

        month_series = pd.to_datetime(out["month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        out = out.assign(month=month_series)
        out = out[out["month"].notna()].sort_values("month").copy()
        if out.empty:
            return out

        start = self.report_start
        end = self.report_end or month_series.max()
        if start is None or pd.isna(start):
            start = out["month"].min()
        if end is None or pd.isna(end):
            end = out["month"].max()

        start = pd.Timestamp(start).to_period("M").to_timestamp()
        end = pd.Timestamp(end).to_period("M").to_timestamp()
        if end < start:
            end = start

        full_index = pd.date_range(start, end, freq="MS")
        out = (
            out.set_index("month")
            .reindex(full_index, fill_value=0)
            .rename_axis("month")
            .reset_index()
        )
        out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")
        return out

    def _parse_date_with_month_fallback(self, series: pd.Series) -> pd.Series:
        normalized = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        try:
            parsed = pd.to_datetime(normalized, format="mixed", errors="coerce")
        except TypeError:
            parsed = pd.to_datetime(normalized, errors="coerce")
        if parsed.isna().any():
            parsed = parsed.fillna(self._parse_month_series(normalized))
        parsed = parsed.where(parsed.dt.year >= 2000)
        return parsed

    def _apply_report_range(self, frame: pd.DataFrame, date_series: pd.Series) -> pd.DataFrame:
        if frame.empty:
            return frame
        if self.report_start is None and self.report_end is None:
            return frame
        if date_series is None:
            return frame

        series = pd.to_datetime(date_series, errors="coerce")
        mask = series.notna()
        if self.report_start is not None:
            mask &= series >= self.report_start
        if self.report_end is not None:
            mask &= series <= self.report_end
        return frame[mask]

    def _apply_reliance_gross_units_window(self, frame: pd.DataFrame, date_series: pd.Series) -> pd.DataFrame:
        # Reliance sales gross premium / quantity KPIs should follow the actual
        # sale date timeline so freshly uploaded sales land in the dashboard
        # month they were sold, even when coverage starts later.
        return self._apply_report_range(frame, date_series)

    def _apply_sales_metric_report_window(self, frame: pd.DataFrame, metric: str) -> pd.DataFrame:
        if frame.empty:
            return frame
        date_metric = "gross_premium" if metric in {"gross_premium", "quantity"} else "earned_premium"
        return self._apply_report_range(frame, self._prepared_sales_metric_date_series(frame, date_metric))

    @staticmethod
    def _normalize_date_key(value: str) -> str:
        return (
            str(value)
            .lower()
            .replace("_", "")
            .replace(" ", "")
            .replace("/", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
            .strip()
        )

    def _coalesce_parsed_dates(self, frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
        if frame.empty:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

        normalized: dict[str, list[str]] = {}
        for col in frame.columns:
            normalized.setdefault(self._normalize_date_key(col), []).append(col)

        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = self._normalize_date_key(candidate)
            for matched in normalized.get(key, []):
                if matched in seen:
                    continue
                seen.add(matched)
                ordered.append(matched)

        month_like = {"month", "monthname", "monthyear"}
        ranked: list[tuple[int, int, pd.Series]] = []
        for order, col in enumerate(ordered):
            key = self._normalize_date_key(col)
            if key in month_like:
                parsed = self._parse_month_series(frame[col])
            else:
                parsed = self._parse_date_with_month_fallback(frame[col])
            valid = int(parsed.notna().sum())
            if valid <= 0:
                continue
            ranked.append((-valid, order, parsed))

        ranked.sort(key=lambda item: (item[0], item[1]))
        series = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        for _neg_valid, _order, parsed in ranked:
            series = series.where(series.notna(), parsed)
        return series

    def _sales_metric_end_series(self, frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

        series = self._coalesce_parsed_dates(
            frame,
            [
                "Warranty End Date",
                "Warranty_End_Date",
                "Plan End Date",
                "End Date",
                "End_Date",
            ],
        )
        if series.isna().any():
            series = series.fillna(self._sales_metric_date_series(frame, "earned_premium"))
        return series

    def _prepared_sales_metric_date_series(self, frame: pd.DataFrame, metric: str) -> pd.Series:
        if frame.empty:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

        preferred_columns = (
            ["Purchase Date", "Transaction Date", "Transaction_Date", "Date"]
            if metric in {"gross_premium", "quantity"}
            else ["Warranty Start Date", "Plan Start Date", "Purchase Date", "Date"]
        )
        resolved = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        for col in preferred_columns:
            if col not in frame.columns:
                continue
            parsed = pd.to_datetime(frame[col], errors="coerce")
            if not parsed.notna().any():
                continue
            resolved = resolved.where(resolved.notna(), parsed)
            if resolved.notna().all():
                return resolved
        fallback = self._sales_metric_date_series(frame, metric)
        if fallback is not None:
            fallback = pd.to_datetime(fallback, errors="coerce")
            resolved = resolved.where(resolved.notna(), fallback)
        return resolved

    def _prepared_sales_metric_end_series(self, frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

        for col in ["Warranty End Date", "Plan End Date", "End Date", "End_Date"]:
            if col not in frame.columns:
                continue
            parsed = pd.to_datetime(frame[col], errors="coerce")
            if not parsed.notna().any():
                continue
            if parsed.isna().any():
                parsed = parsed.where(parsed.notna(), self._prepared_sales_metric_date_series(frame, "earned_premium"))
            return parsed
        return self._sales_metric_end_series(frame)

    def _filter_claims_frame_by_report_range(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        if self.report_start is None and self.report_end is None:
            return frame

        if "Day of Call_Date" in frame.columns:
            claim_dates = pd.to_datetime(frame["Day of Call_Date"], errors="coerce")
            if claim_dates.notna().any():
                mask = claim_dates.notna()
                if self.report_start is not None:
                    mask &= claim_dates >= self.report_start
                if self.report_end is not None:
                    mask &= claim_dates <= self.report_end
                return frame[mask]

        if "Month" in frame.columns:
            month_series = pd.to_datetime(frame["Month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
            mask = month_series.notna()
            if self.report_start is not None:
                start_month = pd.Timestamp(self.report_start).to_period("M").to_timestamp()
                mask &= month_series >= start_month
            if self.report_end is not None:
                end_month = pd.Timestamp(self.report_end).to_period("M").to_timestamp()
                mask &= month_series <= end_month
            return frame[mask]

        return frame

    def _apply_sales_overlap_range(
        self,
        frame: pd.DataFrame,
        start_series: pd.Series,
        end_series: pd.Series | None,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        if self.report_start is None and self.report_end is None:
            return frame
        if start_series is None:
            return frame

        start = pd.to_datetime(start_series, errors="coerce")
        if end_series is None:
            end = start
        else:
            end = pd.to_datetime(end_series, errors="coerce")
            end = end.where(end.notna(), start)

        mask = start.notna()
        if self.report_start is not None:
            mask &= end >= self.report_start
        if self.report_end is not None:
            mask &= start <= self.report_end
        return frame[mask]

    def _sales_metric_date_series(self, frame: pd.DataFrame, metric: str) -> pd.Series:
        if frame.empty:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

        if metric in {"gross_premium", "quantity"}:
            return self._coalesce_parsed_dates(
                frame,
                [
                    "Transaction Date",
                    "Transaction_Date",
                    "Data Processing Date",
                    "Data_Processing_Date",
                    "Purchase Date",
                    "PURCHASE_DATE",
                    "Purchase_Date",
                    "Invoice Date",
                    "Invoice_Date",
                    "Bill Created Date",
                    "Date",
                    "Plan Start Date",
                    "Start Date",
                    "Start_Date",
                    "Warranty Start Date",
                    "Warranty_Start_Date",
                    "Month",
                    "Month Name",
                    "Month_Name",
                    "Month_Year",
                    "Month-Year",
                ],
            )

        return self._coalesce_parsed_dates(
            frame,
            [
                "Warranty Start Date",
                "Warranty_Start_Date",
                "Plan Start Date",
                "Start Date",
                "Start_Date",
                "Purchase Date",
                "PURCHASE_DATE",
                "Purchase_Date",
                "Invoice Date",
                "Invoice_Date",
                "Bill Created Date",
                "Date",
                "Month",
                "Month Name",
                "Month_Name",
                "Month_Year",
                "Month-Year",
            ],
        )

    def _coalesce_text_series(self, df: pd.DataFrame, candidates: list[str], default: str = "") -> pd.Series:
        out = pd.Series(default, index=df.index, dtype="object")
        for col in candidates:
            if col not in df.columns:
                continue
            text = df[col].astype(str).str.strip()
            text = text.replace({"nan": "", "None": "", "none": ""})
            mask = out.astype(str).str.strip().eq("") & text.ne("")
            out = out.where(~mask, text)
        return out.fillna(default)

    def _is_unknown_like_text(self, series: pd.Series) -> pd.Series:
        normalized = (
            series
            .astype(str)
            .str.strip()
            .str.lower()
        )
        return normalized.isin({"", "unknown", "nan", "none", "null", "na"})

    def _infer_plan_type(self, df: pd.DataFrame) -> pd.Series:
        text = self._coalesce_text_series(
            df,
            [
                "Plan Type",
                "Plan Category",
                "ARTICLE_MODEL_DESC",
                "ARTICLE_BRICK",
                "Device Plan Category",
                "ARTICLE_FAMILY",
                "Warranty Status",
            ],
            default="",
        )
        normalized = (
            text
            .str.upper()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        plan = pd.Series("Unknown", index=df.index, dtype="object")

        is_ew = normalized.str.contains(r"EXT(?:ENDED)?\s*(?:WTY|WARRANTY)|\bEW\b", regex=True, na=False)
        is_adld = normalized.str.contains(r"\bADLD\b|ACCIDENTAL|LIQUID", regex=True, na=False)
        is_crack = normalized.str.contains(r"CRACK|SCREEN|\bPP\b|PROTECTION PLAN", regex=True, na=False)

        plan = plan.mask(is_ew, "Extended Warranty")
        plan = plan.mask(is_adld, "ADLD")
        plan = plan.mask(~is_adld & is_crack, "Crack Screen")
        return plan

    def _infer_plan_tenure_months(self, df: pd.DataFrame, plan_type: pd.Series) -> pd.Series:
        text = self._coalesce_text_series(
            df,
            ["ARTICLE_MODEL_DESC", "Plan Type", "Plan Category", "ARTICLE_BRICK", "Device Plan Category"],
            default="",
        )
        compact = text.str.upper().str.replace(" ", "", regex=False)

        tenure = pd.Series(pd.NA, index=df.index, dtype="Int64")
        tenure = tenure.mask(compact.str.contains(r"2YR|2YEAR|24MONTH|24M", regex=True, na=False), 24)
        tenure = tenure.mask(compact.str.contains(r"1YEAR|12MONTH|12M", regex=True, na=False), 12)
        tenure = tenure.mask(compact.str.contains(r"6MONTH|6M", regex=True, na=False), 6)

        start = pd.to_datetime(df.get("Plan Start Date"), errors="coerce")
        end = pd.to_datetime(df.get("Plan End Date"), errors="coerce")
        months_from_days = ((end - start).dt.days / 30.0).round()

        for target in (6, 12, 24):
            inferred = months_from_days.sub(target).abs().le(2)
            tenure = tenure.mask(tenure.isna() & inferred, target)

        tenure = tenure.mask(tenure.isna() & plan_type.eq("ADLD"), 12)
        tenure = tenure.mask(tenure.isna() & plan_type.eq("Crack Screen"), 12)
        tenure = tenure.mask(tenure.isna() & plan_type.eq("Extended Warranty"), 12)
        return tenure

    def _map_value_to_reliance_band_average(self, value) -> float:
        try:
            v = float(value)
        except Exception:
            return float("nan")
        if pd.isna(v):
            return float("nan")
        for lower, upper, avg in RELIANCE_TRANSFER_BANDS:
            if lower <= v <= upper:
                return float(avg)
        return float("nan")

    def _infer_reliance_band_average(self, df: pd.DataFrame) -> pd.Series:
        model_text = self._coalesce_text_series(df, ["ARTICLE_MODEL_DESC", "Plan Type", "Plan Category"], default="")
        cleaned = model_text.str.replace(",", "", regex=False)
        pair = cleaned.str.extract(r"(?P<min>\d{4,6})\D+(?P<max>\d{4,6})")
        min_val = pd.to_numeric(pair["min"], errors="coerce")
        max_val = pd.to_numeric(pair["max"], errors="coerce")
        swap_mask = min_val.gt(max_val)
        min_adj = min_val.where(~swap_mask, max_val)
        max_adj = max_val.where(~swap_mask, min_val)
        band_span = (max_adj - min_adj)
        valid_span = band_span.between(4000, 10000)
        mid_from_desc = ((min_adj + max_adj) / 2.0).where(min_adj.notna() & max_adj.notna() & valid_span)
        avg_from_desc = mid_from_desc.map(self._map_value_to_reliance_band_average)

        handset_col = None
        for col in ["Handset Value", "Handset_Value", "Device Value", "INVOICE_VALUE"]:
            if col in df.columns:
                handset_col = col
                break
        if handset_col is None:
            return avg_from_desc

        handset_value = self._clean_number(df[handset_col])
        avg_from_handset = handset_value.map(self._map_value_to_reliance_band_average)
        return avg_from_desc.where(avg_from_desc.notna(), avg_from_handset)

    def _infer_transfer_price_from_slabs(self, df: pd.DataFrame, plan_type: pd.Series, tenure_months: pd.Series) -> pd.Series:
        avg_band_value = self._infer_reliance_band_average(df)
        tenure_int = pd.to_numeric(tenure_months, errors="coerce").fillna(-1).astype(int)
        keys = pd.Series(list(zip(plan_type.astype(str), tenure_int)), index=df.index)
        rates = keys.map(RELIANCE_TRANSFER_RATE)
        return (avg_band_value * rates).fillna(0)

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------

    def load_data(self) -> dict[str, pd.DataFrame]:
        if self._loaded_data_cache is not None:
            return self._loaded_data_cache

        shared_key = self._shared_load_cache_key()
        now = time.time()
        with _reliance_load_cache_lock:
            cached_value = _reliance_load_cache.get(shared_key, now=now)
            if cached_value is not None:
                cloned = self._clone_loaded_data(cached_value)
                self._loaded_data_cache = cloned
                return cloned

            result = self._load_data_uncached()
            cloned = self._clone_loaded_data(result)
            cache_now = time.time()
            _reliance_load_cache.set(
                shared_key,
                cloned,
                expires_at=cache_now + RELIANCE_LOAD_CACHE_TTL_SECONDS,
                now=cache_now,
            )
            self._loaded_data_cache = self._clone_loaded_data(cloned)
            return self._loaded_data_cache

    def _load_data_uncached(self) -> dict[str, pd.DataFrame]:
        if self._loaded_data_cache is not None:
            return self._loaded_data_cache
        started = time.perf_counter()
        sales_df = get_dataframe(
            db=self.db,
            job_id=self.job_id,
            source="reliance",
            dataset_type="sales",
            cache_result=False,
        )
        claims_df = get_dataframe(
            db=self.db,
            job_id=self.job_id,
            source="reliance",
            dataset_type="claims",
            cache_result=False,
        )

        if sales_df is None:
            sales_df = pd.DataFrame()
        if claims_df is None:
            claims_df = pd.DataFrame()

        if not sales_df.empty:
            sales_df.columns = [str(c).strip() for c in sales_df.columns]

        if not claims_df.empty:
            claims_df.columns = [str(c).strip() for c in claims_df.columns]

        sales_ew_df = pd.DataFrame()

        # -----------------------------
        # SALES CLEANING (NOTEBOOK)
        # -----------------------------
        if not sales_df.empty:
            def _normalize_key(value: str) -> str:
                return (
                    str(value)
                    .lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_columns(frame: pd.DataFrame, candidates: list[str]) -> list[str]:
                normalized: dict[str, list[str]] = {}
                for col in frame.columns:
                    normalized.setdefault(_normalize_key(col), []).append(col)

                ordered: list[str] = []
                seen: set[str] = set()
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    for matched in normalized.get(key, []):
                        if matched in seen:
                            continue
                        seen.add(matched)
                        ordered.append(matched)
                return ordered

            def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
                matched = _pick_columns(frame, candidates)
                return matched[0] if matched else None

            def _pick_best_text_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
                matched = _pick_columns(frame, candidates)
                if not matched:
                    return None

                best_col = matched[0]
                best_score = -1.0
                total_rows = max(len(frame), 1)
                for rank, col in enumerate(matched):
                    text = frame[col].astype(str).str.strip()
                    clean = text.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
                    present_ratio = float(clean.notna().sum()) / total_rows
                    unique_ratio = float(clean.nunique(dropna=True)) / total_rows
                    score = (present_ratio * 0.85) + (unique_ratio * 0.15) - (rank * 0.01)
                    if score > best_score:
                        best_score = score
                        best_col = col
                return best_col

            def _pick_best_numeric_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
                matched = _pick_columns(frame, candidates)
                if not matched:
                    return None

                best_col = matched[0]
                best_score = -1.0
                total_rows = max(len(frame), 1)
                for rank, col in enumerate(matched):
                    numeric = self._clean_number(frame[col])
                    non_zero_ratio = float((numeric.abs() > 0).sum()) / total_rows
                    non_null_ratio = float(frame[col].notna().sum()) / total_rows
                    score = (non_zero_ratio * 0.75) + (non_null_ratio * 0.25) - (rank * 0.02)
                    if score > best_score:
                        best_score = score
                        best_col = col
                return best_col

            if "Plan Selling Price " in sales_df.columns and "Plan Selling Price" not in sales_df.columns:
                sales_df = sales_df.rename(columns={"Plan Selling Price ": "Plan Selling Price"})

            article_brand_col = _pick_best_text_column(
                sales_df,
                [
                    "ARTICLE_BRAND",
                    "Article_Brand",
                    "Article Brand",
                ],
            )
            if article_brand_col is not None:
                article_brand = (
                    sales_df[article_brand_col]
                    .astype(str)
                    .str.strip()
                    .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
                )
            else:
                article_brand = pd.Series(pd.NA, index=sales_df.index, dtype="object")

            brand_col = _pick_best_text_column(
                sales_df,
                [
                    "Item_Brand",
                    "Product Brand",
                    "Brand",
                ],
            )
            if brand_col is not None:
                fallback_brand = (
                    sales_df[brand_col]
                    .astype(str)
                    .str.strip()
                    .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
                )
            else:
                fallback_brand = pd.Series(pd.NA, index=sales_df.index, dtype="object")

            sales_df["Brand"] = article_brand.fillna(fallback_brand).fillna("Unknown")
            sales_df["Brand"] = (
                sales_df["Brand"]
                .astype(str)
                .str.strip()
                .replace({"": "Unknown", "nan": "Unknown", "none": "Unknown", "None": "Unknown"})
            )
            canonicalize_reliance_brand_columns(sales_df)

            state_col = _pick_column(
                sales_df,
                ["State", "STATE3", "State Name", "Customer State", "Region", "Zone", "CITY"],
            )
            if state_col is not None and state_col != "State":
                sales_df["State"] = sales_df[state_col]

            sales_df["Purchase Date"] = self._coalesce_parsed_dates(
                sales_df,
                [
                    "Transaction Date",
                    "Transaction_Date",
                    "Data Processing Date",
                    "Data_Processing_Date",
                    "PURCHASE_DATE",
                    "Purchase Date",
                    "Purchase_Date",
                    "Invoice Date",
                    "Invoice_Date",
                    "Bill Created Date",
                ],
            )
            sales_df["Warranty Start Date"] = self._coalesce_parsed_dates(
                sales_df,
                [
                    "Warranty Start Date",
                    "Warranty_Start_Date",
                    "Plan Start Date",
                    "Start Date",
                    "Start_Date",
                    "Warranty_start_date_",
                    "Date",
                ],
            )
            sales_df["Warranty End Date"] = self._coalesce_parsed_dates(
                sales_df,
                [
                    "Warranty End Date",
                    "Warranty_End_Date",
                    "Plan End Date",
                    "End Date",
                    "End_Date",
                ],
            )

            if sales_df["Purchase Date"].isna().any():
                sales_df["Purchase Date"] = sales_df["Purchase Date"].fillna(sales_df["Warranty Start Date"])

            if sales_df["Warranty Start Date"].isna().any():
                sales_df["Warranty Start Date"] = sales_df["Warranty Start Date"].fillna(sales_df["Purchase Date"])

            # When explicit purchase/warranty dates are unavailable, fall back to month labels row by row.
            month_fallback = self._coalesce_parsed_dates(
                sales_df,
                ["Month", "Month Name", "Month_Name", "Month_Year", "Month-Year"],
            )
            if sales_df["Purchase Date"].isna().any():
                sales_df["Purchase Date"] = sales_df["Purchase Date"].fillna(month_fallback)
            if sales_df["Warranty Start Date"].isna().any():
                sales_df["Warranty Start Date"] = sales_df["Warranty Start Date"].fillna(month_fallback)

            missing_end = sales_df["Warranty End Date"].isna()
            sales_df.loc[missing_end, "Warranty End Date"] = (
                sales_df.loc[missing_end, "Warranty Start Date"] + pd.to_timedelta(365, unit="D")
            )

            # Premium earning logic is warranty-period based.
            sales_df["Plan Start Date"] = sales_df["Warranty Start Date"]
            sales_df["Plan End Date"] = sales_df["Warranty End Date"]

            plan_price_col = _pick_best_numeric_column(
                sales_df,
                [
                    "Plan Price",
                    "Plan Selling Price",
                    "INVOICE_VALUE",
                    "Invoice Value",
                    "Billing Amount",
                    "Total Billing Amount",
                ],
            )
            if plan_price_col is not None:
                sales_df["Plan Selling Price"] = self._clean_number(sales_df[plan_price_col])
            else:
                sales_df["Plan Selling Price"] = 0

            transfer_price_col = _pick_column(
                sales_df,
                [
                    "Total Billing Amount",
                    "Billing Amount",
                    "Zopper Shared ( Transfer Price )",
                    "Zopper Shared Transfer Price",
                    "Zopper Share",
                ],
            )
            if transfer_price_col is not None:
                sales_df["_raw_transfer_price"] = self._clean_number(sales_df[transfer_price_col])
            else:
                sales_df["_raw_transfer_price"] = 0

            device_plan_col = _pick_column(
                sales_df,
                [
                    "Device Plan Category",
                    "ARTICLE_BRICK",
                    "ARTICLE_MODEL_DESC",
                    "ARTICLE_FAMILY",
                    "Brand",
                ],
            )
            if device_plan_col is not None:
                sales_df["Device Plan Category"] = sales_df[device_plan_col].astype(str).str.strip()
            else:
                sales_df["Device Plan Category"] = "Unknown"
            sales_df["Device Plan Category"] = sales_df["Device Plan Category"].replace({"": "Unknown", "nan": "Unknown"})

            plan_type = self._infer_plan_type(sales_df)
            tenure_months = self._infer_plan_tenure_months(sales_df, plan_type=plan_type)
            sales_df["Plan Type"] = plan_type
            sales_df["Plan Tenure"] = tenure_months

            mapped_transfer_price = self._infer_transfer_price_from_slabs(
                sales_df,
                plan_type=plan_type,
                tenure_months=tenure_months,
            )
            fallback_transfer_price = sales_df["Plan Selling Price"] / GST_MULTIPLIER
            raw_transfer_price = self._clean_number(sales_df["_raw_transfer_price"])
            sales_df["Zopper Shared ( Transfer Price )"] = raw_transfer_price.where(
                raw_transfer_price > 0,
                mapped_transfer_price.where(mapped_transfer_price > 0, fallback_transfer_price),
            )
            sales_df = sales_df.drop(columns=["_raw_transfer_price"], errors="ignore")

            # Time-series analysis defaults to warranty-start timeline.
            if "Warranty Start Date" in sales_df.columns and sales_df["Warranty Start Date"].notna().any():
                sales_df["Month"] = self._month_key(sales_df["Warranty Start Date"])
            elif "Plan Start Date" in sales_df.columns and sales_df["Plan Start Date"].notna().any():
                sales_df["Month"] = self._month_key(sales_df["Plan Start Date"])
            elif "Month" in sales_df.columns:
                sales_df["Month"] = self._month_key(self._parse_month_series(sales_df["Month"]))

            # Split EW rows so we can selectively include them per-metric downstream.
            sales_df["_ew"] = self._is_ew_plan(sales_df)
            sales_ew_df = sales_df[sales_df["_ew"] == True].copy()
            sales_df = sales_df[sales_df["_ew"] != True]

        # -----------------------------
        # CLAIMS CLEANING (NOTEBOOK)
        # -----------------------------
        if not claims_df.empty:
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in frame.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            def _pick_best_text_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in frame.columns}
                matched: list[str] = []
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    col = normalized.get(key)
                    if col is None or col in matched:
                        continue
                    matched.append(col)
                if not matched:
                    return None

                best_col = matched[0]
                best_score = -1.0
                total_rows = max(len(frame), 1)
                for rank, col in enumerate(matched):
                    text = frame[col].astype(str).str.strip()
                    clean = text.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
                    present_ratio = float(clean.notna().sum()) / total_rows
                    unique_ratio = float(clean.nunique(dropna=True)) / total_rows
                    score = (present_ratio * 0.85) + (unique_ratio * 0.15) - (rank * 0.01)
                    if score > best_score:
                        best_score = score
                        best_col = col
                return best_col

            call_date_col = _pick_column(
                claims_df,
                [
                    "Day of Call_Date",
                    "Day of Call Date",
                    "Call_Date",
                    "Call Date",
                    "Month_Year",
                    "Call_Registered_Date",
                    "Call_Initiated_Date",
                    "Claim Redeemed Date",
                    "Warranty_start_date_",
                    "Invoice_Date_",
                    "Date",
                ],
            )
            if call_date_col is not None:
                claims_df["Day of Call_Date"] = pd.to_datetime(
                    claims_df[call_date_col], errors="coerce"
                )

            if "Month" in claims_df.columns:
                claims_df["Month"] = self._parse_month_series(claims_df["Month"])
            elif "Month_Year" in claims_df.columns:
                claims_df["Month"] = self._parse_month_series(claims_df["Month_Year"])
            elif "Day of Call_Date" in claims_df.columns:
                claims_df["Month"] = self._month_key(claims_df["Day of Call_Date"])

            if "Month" in claims_df.columns:
                claims_df = claims_df[claims_df["Month"].notna()]
            elif "Day of Call_Date" in claims_df.columns:
                claims_df = claims_df[claims_df["Day of Call_Date"].notna()]
            else:
                # No recognizable claims date column; keep rows rather than fail hard.
                claims_df = claims_df.copy()

            warranty_col = _pick_column(claims_df, ["Warranty Type", "Warranty_Type", "Plan_Name"])
            if warranty_col is not None:
                claims_df[warranty_col] = claims_df[warranty_col].replace(
                    {"Screen Protection": "Cracked Screen"}
                )
                if warranty_col != "Warranty Type":
                    claims_df["Warranty Type"] = claims_df[warranty_col]

            brand_col = _pick_best_text_column(
                claims_df,
                [
                    "Product Brand(Group)",
                    "Product Brand (Group)",
                    "Product Brand",
                    "ARTICLE_BRAND",
                    "Article_Brand",
                    "Article Brand",
                    "Brand",
                    "Item_Brand",
                ],
            )
            if brand_col is not None:
                claims_df[brand_col] = claims_df[brand_col].replace({"OPPO": "Oppo"})
                if brand_col != "Product Brand(Group)":
                    claims_df["Product Brand(Group)"] = claims_df[brand_col]
            canonicalize_reliance_brand_columns(claims_df)

            cost_col = _pick_column(
                claims_df,
                [
                    "Zopper's Cost",
                    "Claim_Amount",
                    "Claim Amount",
                    "Payment_Amount",
                    "Payment Amount",
                    "last_estimation_amount",
                ],
            )
            deductible_col = _pick_column(
                claims_df,
                [
                    "One time deductible",
                    "One Time Deductible",
                    "OTD Amount",
                    "OTD_Amount",
                    "Deductible",
                ],
            )
            customer_paid_col = _pick_column(
                claims_df,
                [
                    "Customer Paid",
                    "Customer Paid Amount",
                    "Customer_Paid_Amount",
                ],
            )
            state_col = _pick_column(claims_df, ["State", "Customer_State", "Customer State"])
            if state_col is not None and state_col != "State":
                claims_df["State"] = claims_df[state_col]

            claims_df["One time deductible"] = (
                self._clean_number(claims_df[deductible_col])
                if deductible_col is not None
                else 0
            )

            claims_df["Zopper's Cost"] = (
                self._clean_number(claims_df[cost_col])
                if cost_col is not None
                else 0
            )

            if customer_paid_col is not None:
                claims_df["Customer Paid"] = self._clean_number(
                    claims_df[customer_paid_col]
                )
            else:
                claims_df["Customer Paid"] = 0

            claims_df["Net Claims"] = (
                claims_df["Zopper's Cost"]
                - claims_df["One time deductible"]
                - claims_df["Customer Paid"]
            )

        # -----------------------------
        # PREMIUM CALCULATION (NOTEBOOK)
        # -----------------------------
        if not sales_df.empty:
            sales_df = sales_df.copy()

            if "Plan Start Date" not in sales_df.columns:
                sales_df["Plan Start Date"] = pd.NaT
            if "Plan End Date" not in sales_df.columns:
                sales_df["Plan End Date"] = pd.NaT
            if "Plan Selling Price" not in sales_df.columns:
                sales_df["Plan Selling Price"] = 0
            if "Zopper Shared ( Transfer Price )" not in sales_df.columns:
                sales_df["Zopper Shared ( Transfer Price )"] = 0

            coverage_days = (sales_df["Plan End Date"] - sales_df["Plan Start Date"]).dt.days
            coverage_days = coverage_days.fillna(365).clip(lower=1)

            exposure_days_raw = (
                self.valuation_date - sales_df["Plan Start Date"]
            ).dt.days
            # Future-start policies should not generate negative earned premium.
            # Also cap exposure at coverage days.
            exposure_days = exposure_days_raw.fillna(0).clip(lower=0)
            exposure_days = pd.concat([exposure_days, coverage_days], axis=1).min(axis=1)

            sales_df["Coverage Days"] = coverage_days
            sales_df["Exposure Days"] = exposure_days

            transfer_price = self._clean_number(
                sales_df["Zopper Shared ( Transfer Price )"]
            )
            selling_price = self._clean_number(
                sales_df["Plan Selling Price"]
            )

            sales_df["Written Premium"] = transfer_price * GST_MULTIPLIER
            sales_df["Zopper Earned Premium"] = (
                sales_df["Written Premium"]
                * sales_df["Exposure Days"]
                / sales_df["Coverage Days"]
            ).fillna(0)

            sales_df["Gross Premium"] = selling_price
            sales_df["Earned Premium"] = (
                sales_df["Gross Premium"]
                * sales_df["Exposure Days"]
                / sales_df["Coverage Days"]
            ).fillna(0)
        self._loaded_data_cache = compact_dataframe_mapping(
            {"sales": sales_df, "claims": claims_df, "sales_ew": sales_ew_df}
        )
        logger.info(
            "TIMING reliance.load_data source=%s dataset=%s sales_rows=%s claims_rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            len(sales_df),
            len(claims_df),
            (time.perf_counter() - started) * 1000,
        )
        return self._loaded_data_cache

    # --------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------


    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        total_started = time.perf_counter()
        data = self.load_data()
        df = data["claims"] if self.dataset_type == "claims" else data["sales"]
        ew_df = data.get("sales_ew") if self.dataset_type == "sales" else None

        include_ew_sales = (
            self.dataset_type == "sales"
            and metric in {"quantity", "gross_premium"}
            and ew_df is not None
            and not ew_df.empty
        )
        if df.empty and not include_ew_sales:
            return []

        if self.dataset_type == "claims":
            df = self._filter_claims_frame_by_report_range(df)
            if df.empty:
                return []

        df = df.copy() if not df.empty else pd.DataFrame()

        if metric == "quantity":
            df["_value"] = 1
        elif metric == "gross_premium":
            if "Gross Premium" not in df.columns:
                return []
            df["_value"] = df["Gross Premium"]
        elif metric == "earned_premium":
            if "Earned Premium" not in df.columns:
                return []
            df["_value"] = df["Earned Premium"]
        elif metric == "zopper_earned_premium":
            if "Zopper Earned Premium" not in df.columns:
                return []
            df["_value"] = df["Zopper Earned Premium"]
        elif metric == "net_claims":
            if "Net Claims" not in df.columns:
                return []
            df["_value"] = df["Net Claims"]
        elif metric == "claims":
            if "Zopper's Cost" not in df.columns:
                return []
            df["_value"] = df["Zopper's Cost"]
        elif metric == "loss_ratio":
            out = self._compute_loss_ratio(dimension, data=data)
            logger.info(
                "TIMING reliance.compute_by_dimension source=%s dataset=%s dimension=%s metric=%s out_rows=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                dimension,
                metric,
                len(out),
                (time.perf_counter() - total_started) * 1000,
            )
            return out
        else:
            return []

        if self.dataset_type == "sales":
            if not df.empty:
                df = self._apply_sales_metric_report_window(df, metric)
            if df.empty and not include_ew_sales:
                return []

            if include_ew_sales:
                ew_df = self._apply_sales_metric_report_window(ew_df, metric)

        dim_map = {
            "month": "Month",
            "date": "Date",
            "state": "State",
            "brand": "Brand"
            if self.dataset_type == "sales"
            else "Product Brand(Group)",
            "article_brand": "Brand"
            if self.dataset_type == "sales"
            else "Product Brand(Group)",
            "plan_category": "Plan Type"
            if self.dataset_type == "sales"
            else "Warranty Type",
            "device_plan_category": "Device Plan Category"
            if self.dataset_type == "sales"
            else "Product Brand(Group)",
        }

        def resolve_dimension(local_df: pd.DataFrame):
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_column(candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in local_df.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            if dimension == "date" and self.dataset_type == "sales":
                local_df = local_df.copy()
                local_df["Date"] = pd.to_datetime(
                    self._prepared_sales_metric_date_series(local_df, metric),
                    errors="coerce",
                ).dt.normalize()
                local_df = local_df[local_df["Date"].notna()]
                if local_df.empty:
                    return None, None
                return local_df, "Date"

            if dimension == "month" and self.dataset_type == "sales":
                local_df = local_df.copy()
                local_df["Month"] = self._month_key(self._prepared_sales_metric_date_series(local_df, metric))
                local_df = local_df[local_df["Month"].notna()]
                if local_df.empty:
                    return None, None
                return local_df, "Month"

            dim_col = dim_map.get(dimension)
            if dim_col not in local_df.columns:
                if dimension == "state":
                    dim_col = _pick_column([
                        "State",
                        "Customer_State",
                        "Customer State",
                        "State Name",
                        "State_Name",
                        "State/City",
                        "State / City",
                        "Region",
                        "Region Name",
                        "Region_Name",
                        "Zone",
                        "Zone Name",
                        "Location",
                    ])
                elif dimension == "plan_category":
                    dim_col = _pick_column([
                        "Plan Type",
                        "Plan Category",
                        "Plan_Category",
                        "Warranty Type",
                        "Product Category",
                        "Product_Category",
                    ])
                elif dimension == "device_plan_category":
                    dim_col = _pick_column([
                        "Device Plan Category",
                        "Device Category",
                        "Product Brand(Group)",
                        "Product Brand (Group)",
                        "Product Brand",
                        "Brand",
                        "Item_Brand",
                        "Plan_Category",
                        "Plan Category",
                    ])

                if dim_col not in local_df.columns:
                    if dimension in {"month", "date"}:
                        date_col = _pick_column(
                            [
                                "Month",
                                "Month_Year",
                                "Day of Call_Date",
                                "Call_Registered_Date",
                                "Call_Initiated_Date",
                                "Call_Date",
                                "Call Date",
                                "Date",
                            ]
                        )
                        if date_col in local_df.columns:
                            if dimension == "month" and _normalize_key(date_col) in {"month", "monthyear", "monthname"}:
                                local_df["Month"] = self._parse_month_series(local_df[date_col])
                            elif dimension == "month":
                                local_df["Month"] = self._month_key(local_df[date_col])
                            else:
                                local_df["Date"] = pd.to_datetime(
                                    self._parse_date_with_month_fallback(local_df[date_col]),
                                    errors="coerce",
                                ).dt.normalize()
                            dim_col = "Month" if dimension == "month" else "Date"
                        else:
                            return None, None
                    else:
                        return None, None

            if dimension == "month":
                if dim_col != "Month":
                    local_df["Month"] = self._month_key(local_df[dim_col])
                if "Month" in local_df.columns:
                    local_df["Month"] = self._parse_month_series(local_df["Month"])
                    local_df["Month"] = self._month_key(local_df["Month"])
                    local_df = local_df[local_df["Month"].notna()]
                    dim_col = "Month"
            elif dimension == "date":
                if dim_col != "Date":
                    local_df["Date"] = pd.to_datetime(
                        self._parse_date_with_month_fallback(local_df[dim_col]),
                        errors="coerce",
                    ).dt.normalize()
                if "Date" in local_df.columns:
                    local_df["Date"] = pd.to_datetime(local_df["Date"], errors="coerce").dt.normalize()
                    local_df = local_df[local_df["Date"].notna()]
                    dim_col = "Date"

            return local_df, dim_col

        def prepare_dimension_frame(local_df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
            if local_df.empty:
                return pd.DataFrame(), None

            local_df, dim_col = resolve_dimension(local_df)
            if dim_col is None or local_df is None or local_df.empty:
                return pd.DataFrame(), None

            if dimension in {"brand", "article_brand", "device_plan_category"}:
                dim_values = local_df[dim_col].astype(str).str.strip()
                keep_mask = ~self._is_unknown_like_text(dim_values)
                local_df = local_df.loc[keep_mask].copy()
                if local_df.empty:
                    return pd.DataFrame(), None
                local_df[dim_col] = dim_values.loc[keep_mask]

            return local_df, dim_col

        df, dim_col = prepare_dimension_frame(df)
        if dim_col is None and not include_ew_sales:
            return []

        if dim_col is not None:
            out = (
                df.groupby(dim_col, dropna=False)["_value"]
                .sum()
                .reset_index()
                .rename(columns={dim_col: dimension, "_value": metric})
            )
        else:
            out = pd.DataFrame(columns=[dimension, metric])

        if include_ew_sales and ew_df is not None and not ew_df.empty:
            ew_df = ew_df.copy()
            if metric == "quantity":
                ew_df["_value"] = 1
                ew_metric_col = "ew_quantity"
            else:
                if "Gross Premium" in ew_df.columns:
                    ew_df["_value"] = self._clean_number(ew_df["Gross Premium"])
                elif "Plan Selling Price" in ew_df.columns:
                    ew_df["_value"] = self._clean_number(ew_df["Plan Selling Price"])
                else:
                    ew_df["_value"] = 0
                ew_metric_col = "ew_gross_premium"
            ew_df, ew_dim_col = prepare_dimension_frame(ew_df)
            if ew_dim_col is not None:
                ew_out = (
                    ew_df.groupby(ew_dim_col, dropna=False)["_value"]
                    .sum()
                    .reset_index()
                    .rename(columns={ew_dim_col: dimension, "_value": ew_metric_col})
                )
                if out.empty:
                    out = ew_out.rename(columns={ew_metric_col: metric})
                else:
                    out = out.merge(ew_out, on=dimension, how="outer").fillna(0)
                    out[metric] = pd.to_numeric(out.get(metric), errors="coerce").fillna(0) + pd.to_numeric(
                        out.get(ew_metric_col), errors="coerce"
                    ).fillna(0)
                    out = out.drop(columns=[ew_metric_col], errors="ignore")

        if out.empty:
            return []
        if dimension == "month" and "month" in out.columns:
            out = self._finalize_month_dimension_output(out)
        elif dimension == "date" and "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            out = out[pd.Series(out["date"]).notna()].copy()
            out = out.sort_values("date")
        result = out.fillna(0).to_dict(orient="records")
        logger.info(
            "TIMING reliance.compute_by_dimension source=%s dataset=%s dimension=%s metric=%s out_rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            dimension,
            metric,
            len(result),
            (time.perf_counter() - total_started) * 1000,
        )
        return result

    # --------------------------------------------------
    # LOSS RATIO
    # --------------------------------------------------

    def _compute_loss_ratio(self, dimension: str, data: dict[str, pd.DataFrame] | None = None) -> list[dict]:
        if data is None:
            data = self.load_data()
        sales = data["sales"]
        claims = data["claims"]

        if sales.empty or claims.empty:
            return []

        sales_dates = self._prepared_sales_metric_date_series(sales, "earned_premium")
        sales = self._apply_report_range(sales, sales_dates)
        if sales.empty:
            return []

        # Keep claims scope aligned with report range to avoid numerator/denominator
        # month drift when explicit date filters are used.
        claims = self._filter_claims_frame_by_report_range(claims)
        if claims.empty:
            return []

        if dimension == "month":
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in df.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            def _best_month_key(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
                month_like = {"month", "monthyear", "monthname"}
                best_key: pd.Series | None = None
                best_valid = -1
                best_years = -1
                for candidate in candidates:
                    col = _pick_column(df, [candidate])
                    if col is None:
                        continue
                    key_name = _normalize_key(col)
                    if key_name in month_like:
                        parsed = self._parse_month_series(df[col])
                    else:
                        parsed = self._parse_date_with_month_fallback(df[col])
                    month_key = self._month_key(parsed)
                    valid = int(month_key.notna().sum())
                    if valid <= 0:
                        continue
                    years = int(month_key.dropna().dt.year.nunique())
                    if valid > best_valid or (valid == best_valid and years > best_years):
                        best_key = month_key
                        best_valid = valid
                        best_years = years
                return best_key

            claims = claims.copy()
            claims_month = _best_month_key(
                claims,
                [
                    "Day of Call_Date",
                    "Call_Registered_Date",
                    "Call_Initiated_Date",
                    "Call_Date",
                    "Call Date",
                    "Month_Year",
                    "Month-Year",
                    "Month",
                ],
            )
            if claims_month is None:
                return []
            claims["Month"] = claims_month
            claims = claims[claims["Month"].notna()].copy()
            claims = self._apply_report_range(claims, claims["Month"])
            if claims.empty:
                return []

            claims_g = (
                claims.groupby("Month", dropna=False)["Net Claims"]
                .sum()
                .reset_index()
            )
            month_index = pd.DatetimeIndex(sorted(pd.to_datetime(claims_g["Month"], errors="coerce").dropna().unique()))
            if month_index.empty:
                return []

            sales = sales.copy()
            transfer_col = _pick_column(
                sales,
                [
                    "Zopper Shared ( Transfer Price )",
                    "Zopper Shared (Transfer Price)",
                    "Zopper Shared",
                    "Transfer Price",
                    "Zopper Share",
                ],
            )
            if transfer_col is not None:
                zopper_total = self._clean_number(sales[transfer_col]) * GST_MULTIPLIER
            elif "Zopper Earned Premium" in sales.columns:
                zopper_total = self._clean_number(sales["Zopper Earned Premium"])
            else:
                return []

            start_col = _pick_column(
                sales,
                ["Plan Start Date", "Warranty Start Date", "Start Date", "Start_Date", "Purchase Date", "Month"],
            )
            end_col = _pick_column(
                sales,
                ["Plan End Date", "Warranty End Date", "End Date", "End_Date"],
            )
            if start_col is None:
                return []

            start_dt = pd.to_datetime(sales[start_col], errors="coerce")
            end_dt = pd.to_datetime(sales[end_col], errors="coerce") if end_col is not None else pd.Series(pd.NaT, index=sales.index)
            coverage_days = (end_dt - start_dt).dt.days + 1
            valid = start_dt.notna() & end_dt.notna() & coverage_days.gt(0) & zopper_total.ne(0)

            monthly_denominator = pd.Series(0.0, index=month_index, dtype="float64")
            for month_start in month_index:
                month_start = pd.Timestamp(month_start)
                month_end = (month_start + MonthEnd(1)).normalize()
                overlap_start = start_dt.clip(lower=month_start)
                overlap_end = end_dt.clip(upper=month_end)
                overlap_days = (overlap_end - overlap_start).dt.days + 1
                overlap_days = overlap_days.clip(lower=0)
                accrual = (
                    zopper_total
                    * (overlap_days / coverage_days.where(coverage_days.gt(0), pd.NA))
                ).fillna(0.0)
                monthly_denominator.loc[month_start] = float(accrual[valid].sum())

            # Fallback for rows without usable policy dates: bucket by explicit sales month.
            invalid = (~valid) & zopper_total.ne(0)
            if invalid.any():
                fallback_month = _best_month_key(
                    sales.loc[invalid],
                    ["Month", "Plan Start Date", "Warranty Start Date", "Start Date", "Purchase Date"],
                )
                if fallback_month is not None:
                    fallback_df = pd.DataFrame(
                        {
                            "Month": pd.to_datetime(fallback_month, errors="coerce"),
                            "Zopper Earned Premium": zopper_total.loc[invalid],
                        }
                    )
                    fallback_df = fallback_df[fallback_df["Month"].notna()]
                    if not fallback_df.empty:
                        fallback_g = fallback_df.groupby("Month", dropna=False)["Zopper Earned Premium"].sum()
                        for month_key, value in fallback_g.items():
                            if month_key in monthly_denominator.index:
                                monthly_denominator.loc[month_key] += float(value or 0.0)

            sales_g = (
                monthly_denominator
                .rename("Zopper Earned Premium")
                .reset_index()
                .rename(columns={"index": "Month"})
            )

            merged = claims_g.merge(sales_g, on="Month", how="left")
            merged["Month"] = pd.to_datetime(merged["Month"], errors="coerce")
            merged = merged[merged["Month"].notna()].sort_values("Month").copy()
            if merged.empty:
                return []

            numerator = pd.to_numeric(merged["Net Claims"], errors="coerce").fillna(0.0)
            denominator = pd.to_numeric(merged["Zopper Earned Premium"], errors="coerce").fillna(0.0)
            merged["_cum_claims"] = numerator.cumsum()
            merged["_cum_zp"] = denominator.cumsum()
            merged["loss_ratio"] = (
                merged["_cum_claims"] / merged["_cum_zp"].replace(0, pd.NA) * 100
            ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)
            merged["period_start"] = merged["Month"].iloc[0]
            merged["period_end"] = merged["Month"]

            out = merged[["Month", "loss_ratio", "period_start", "period_end"]].rename(columns={"Month": "month"})
            out = out.sort_values("month")
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")
            out["period_start"] = pd.to_datetime(out["period_start"], errors="coerce").dt.strftime("%b-%y")
            out["period_end"] = pd.to_datetime(out["period_end"], errors="coerce").dt.strftime("%b-%y")
            return out.to_dict(orient="records")
        elif dimension == "state":
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_dim(df: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in df.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            state_candidates = [
                "State",
                "Customer_State",
                "Customer State",
                "State Name",
                "State_Name",
                "State/City",
                "State / City",
                "Region",
                "Region Name",
                "Region_Name",
                "Zone",
                "Zone Name",
                "Location",
            ]

            dim_sales = _pick_dim(sales, state_candidates)
            dim_claims = _pick_dim(claims, state_candidates)

            if dim_sales is None or dim_claims is None:
                return []

            sales = sales.copy()
            claims = claims.copy()
            sales[dim_sales] = sales[dim_sales].astype(str).str.strip()
            claims[dim_claims] = claims[dim_claims].astype(str).str.strip()
        elif dimension == "device_plan_category":
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_dim(df: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in df.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            sales_candidates = [
                "Device Plan Category",
                "Device Category",
                "Brand",
                "Product_Category",
                "Plan_Category",
                "Plan Category",
            ]
            claims_candidates = [
                "Product Brand(Group)",
                "Product Brand (Group)",
                "Product Brand",
                "Brand",
                "Item_Brand",
                "Device Plan Category",
                "Device Category",
            ]

            dim_sales = _pick_dim(sales, sales_candidates)
            dim_claims = _pick_dim(claims, claims_candidates)

            if dim_sales is None or dim_claims is None:
                return []

            sales = sales.copy()
            claims = claims.copy()
            sales[dim_sales] = sales[dim_sales].astype(str).str.strip()
            claims[dim_claims] = claims[dim_claims].astype(str).str.strip()
        elif dimension == "plan_category":
            def _normalize_key(value: str) -> str:
                return (
                    value.lower()
                    .replace("_", "")
                    .replace(" ", "")
                    .replace("/", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

            def _pick_dim(df: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {_normalize_key(c): c for c in df.columns}
                for candidate in candidates:
                    key = _normalize_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            sales_candidates = [
                "Plan Type",
                "Plan Category",
                "Plan_Category",
                "Product Category",
                "Product_Category",
                "Brand",
            ]
            claims_candidates = [
                "Warranty Type",
                "Plan Type",
                "Plan Category",
                "Plan_Category",
                "Product Category",
                "Product_Category",
            ]

            dim_sales = _pick_dim(sales, sales_candidates)
            dim_claims = _pick_dim(claims, claims_candidates)
            if dim_sales is None or dim_claims is None:
                return []

            sales = sales.copy()
            claims = claims.copy()
            sales[dim_sales] = sales[dim_sales].astype(str).str.strip()
            claims[dim_claims] = claims[dim_claims].astype(str).str.strip()
        else:
            dim_sales = "Plan Type" if dimension == "plan_category" else "Brand"
            dim_claims = "Warranty Type" if dimension == "plan_category" else "Product Brand(Group)"

        sales_g = (
            sales.groupby(dim_sales)["Zopper Earned Premium"]
            .sum()
            .reset_index()
        )

        claims_g = (
            claims.groupby(dim_claims)["Net Claims"]
            .sum()
            .reset_index()
        )

        def _norm_dim(series: pd.Series) -> pd.Series:
            return (
                series
                .astype(str)
                .str.strip()
                .str.lower()
                .str.replace("_", " ", regex=False)
                .str.replace(r"\s+", " ", regex=True)
            )

        sales_g["_k"] = _norm_dim(sales_g[dim_sales])
        claims_g["_k"] = _norm_dim(claims_g[dim_claims])

        merged = sales_g.merge(
            claims_g[["_k", "Net Claims"]],
            on="_k",
            how="left",
        ).fillna({"Net Claims": 0})

        numerator = pd.to_numeric(merged["Net Claims"], errors="coerce").fillna(0)
        denominator = pd.to_numeric(merged["Zopper Earned Premium"], errors="coerce").fillna(0)
        merged["loss_ratio"] = (
            numerator / denominator.replace(0, pd.NA) * 100
        ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)

        out = merged[[dim_sales, "loss_ratio"]].rename(
            columns={dim_sales: dimension}
        )
        if dimension == "month" and "month" in out.columns:
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")
        return out.to_dict(orient="records")

    # --------------------------------------------------
    # SUMMARY
    # --------------------------------------------------

    def compute_summary(self) -> dict:
        started = time.perf_counter()
        data = self.load_data()

        if self.dataset_type == "claims":
            df = self._filter_claims_frame_by_report_range(data["claims"])
            if df.empty:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }
            if "Zopper's Cost" not in df.columns or "Net Claims" not in df.columns:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": int(len(df)),
                }
            result = {
                "gross_premium": float(df["Zopper's Cost"].sum()),
                "earned_premium": float(df["Net Claims"].sum()),
                "zopper_earned_premium": float(df["Net Claims"].sum()),
                "units_sold": int(len(df)),
            }
            logger.info(
                "TIMING reliance.compute_summary source=%s dataset=%s rows=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                len(df),
                (time.perf_counter() - started) * 1000,
            )
            return result

        df = data["sales"]
        ew_df = data.get("sales_ew")
        ew_gross_df = pd.DataFrame()
        if ew_df is not None and not ew_df.empty:
            ew_gross_df = self._apply_sales_metric_report_window(ew_df, "gross_premium")

        if df.empty and ew_gross_df.empty:
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        gross_df = self._apply_sales_metric_report_window(df, "gross_premium")
        earned_df = self._apply_sales_metric_report_window(df, "earned_premium")

        gross_base = (
            self._clean_number(gross_df["Gross Premium"]).sum()
            if "Gross Premium" in gross_df.columns
            else (
                self._clean_number(gross_df["Plan Selling Price"]).sum()
                if "Plan Selling Price" in gross_df.columns
                else 0
            )
        )
        gross_ew = (
            self._clean_number(ew_gross_df["Gross Premium"]).sum()
            if "Gross Premium" in ew_gross_df.columns
            else (
                self._clean_number(ew_gross_df["Plan Selling Price"]).sum()
                if "Plan Selling Price" in ew_gross_df.columns
                else 0
            )
        )
        result = {
            "gross_premium": float(gross_base + gross_ew),
            "earned_premium": float(self._clean_number(earned_df["Earned Premium"]).sum()) if "Earned Premium" in earned_df.columns else 0.0,
            "zopper_earned_premium": float(self._clean_number(earned_df["Zopper Earned Premium"]).sum()) if "Zopper Earned Premium" in earned_df.columns else 0.0,
            "units_sold": int(len(gross_df)) + int(len(ew_gross_df)),
        }
        logger.info(
            "TIMING reliance.compute_summary source=%s dataset=%s rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            len(df),
            (time.perf_counter() - started) * 1000,
        )
        return result

    def compute(self) -> dict:
        return {}
