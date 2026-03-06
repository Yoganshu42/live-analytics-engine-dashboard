# services/analytics/samsung_engine.py

import logging
import time
import threading
import re
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.orm import Session

from services.analytics.base_engine import BaseAnalyticsEngine
from services.analytics_repository import get_dataframe

REPORT_START = pd.Timestamp("2000-01-01")
REPORT_END = pd.Timestamp("2100-12-31")
ZOPPER_GST_MULTIPLIER = 1.18
LOSS_RATIO_CAP_PERCENT = 300.0
logger = logging.getLogger(__name__)
SAMSUNG_SOURCE_VARIANTS = (
    "samsung_vs",
    "samsung_croma",
    "samsung_vijay_sales",
    "samsung",
)
SAMSUNG_LOAD_CACHE_TTL_SECONDS = 300
_samsung_load_cache_lock = threading.Lock()
_samsung_load_cache: dict[
    tuple[str, str, str, str, str, bool, bool],
    tuple[float, dict[str, pd.DataFrame]],
] = {}
_samsung_load_inflight: dict[
    tuple[str, str, str, str, str, bool, bool],
    threading.Event,
] = {}


def invalidate_samsung_load_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    source_scope: set[str] | None = None
    if source is not None:
        source_key = (source or "").strip().lower()
        if source_key.startswith("samsung") or source_key in SAMSUNG_SOURCE_VARIANTS:
            source_scope = set(SAMSUNG_SOURCE_VARIANTS)
        else:
            source_scope = {source_key}

    dataset_scope = (dataset_type or "").strip().lower() if dataset_type is not None else None
    job_scope = (job_id or "").strip() if job_id is not None else None

    with _samsung_load_cache_lock:
        if source_scope is None and dataset_scope is None and job_scope is None:
            _samsung_load_cache.clear()
            return None

        keys_to_delete: list[tuple[str, str, str, str, str, bool, bool]] = []
        for key in _samsung_load_cache.keys():
            key_source, key_dataset, key_job, _from_key, _to_key, _sales, _claims = key
            if source_scope is not None and key_source not in source_scope:
                continue
            if dataset_scope is not None and key_dataset != dataset_scope:
                continue
            if job_scope is not None and key_job != job_scope:
                continue
            keys_to_delete.append(key)

        for key in keys_to_delete:
            _samsung_load_cache.pop(key, None)
    return None


class SamsungAnalyticsEngine(BaseAnalyticsEngine):
    def __init__(
        self,
        db: Session,
        job_id: str | None,
        source: str | None = "samsung",
        dataset_type: str | None = "sales",
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        super().__init__(db=db, job_id=job_id, source=source)
        self.dataset_type = dataset_type or "sales"
        self._loaded_data_cache: dict[tuple[bool, bool], dict[str, pd.DataFrame]] = {}
        self.apply_date_filter = bool(from_date or to_date)
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else REPORT_START
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else REPORT_END
        if pd.isna(self.report_start):
            self.report_start = REPORT_START
        if pd.isna(self.report_end):
            self.report_end = REPORT_END

    def _shared_load_cache_key(self, include_sales: bool, include_claims: bool) -> tuple[str, str, str, str, str, bool, bool]:
        # Sales data loading is range-agnostic (date slicing happens in compute stage),
        # so keep one shared cache across date-range switches for much faster reuse.
        # Claims loading applies date filters inside load_data, so keep range in key.
        needs_range_key = include_claims and self.apply_date_filter
        from_key = self.report_start.date().isoformat() if needs_range_key and self.report_start is not None else ""
        to_key = self.report_end.date().isoformat() if needs_range_key and self.report_end is not None else ""
        return (
            (self.source or "").strip().lower(),
            (self.dataset_type or "").strip().lower(),
            (self.job_id or "").strip(),
            from_key,
            to_key,
            include_sales,
            include_claims,
        )

    def _clone_loaded_data(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        return {
            "sales": data.get("sales", pd.DataFrame()).copy(deep=False),
            "claims": data.get("claims", pd.DataFrame()).copy(deep=False),
        }

    # --------------------------------------------------
    # MONTH PARSING (CONSISTENT)
    # --------------------------------------------------
    def _parse_month_series(
        self,
        month_series: pd.Series,
        start_date_series: pd.Series | None = None,
    ) -> pd.Series | None:
        # Normalize to string for robust parsing
        month_series = month_series.astype(str).str.strip()

        # Handle yyyymm like 202507 (or 202507.0)
        cleaned = month_series.str.replace(r"\.0$", "", regex=True)
        yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
        if yyyymm_mask.any():
            parsed_yyyymm = pd.to_datetime(
                cleaned.where(~yyyymm_mask, cleaned.str.slice(0, 4) + "-" + cleaned.str.slice(4, 6) + "-01"),
                errors="coerce",
            )
            if not parsed_yyyymm.isna().all():
                return parsed_yyyymm

        month_dt = pd.to_datetime(month_series, errors="coerce")

        if month_dt.isna().all():
            for fmt in ["%d-%b", "%d-%b-%y", "%d-%b-%Y", "%b-%y", "%b-%Y", "%m-%Y", "%Y-%m", "%Y-%m-%d"]:
                month_dt = pd.to_datetime(month_series, format=fmt, errors="coerce")
                if not month_dt.isna().all():
                    break

        # If Month is numeric (1-12), build dates using report year
        if month_dt.isna().all():
            month_num = pd.to_numeric(month_series, errors="coerce")
            if not month_num.isna().all():
                if start_date_series is not None:
                    start_dt = pd.to_datetime(start_date_series, errors="coerce")
                    year_vals = start_dt.dt.year.where(start_dt.notna(), self.report_start.year)
                    month_dt = pd.to_datetime(
                        {
                            "year": year_vals,
                            "month": month_num.clip(1, 12),
                            "day": 1,
                        },
                        errors="coerce",
                    )
                else:
                    month_dt = pd.Series(pd.NaT, index=month_series.index, dtype="datetime64[ns]")

        # Month-only labels without explicit year are ambiguous; keep them null
        # unless a start-date series was provided to infer year.
        if start_date_series is None:
            cleaned = month_series.astype(str).str.strip()
            ambiguous_month_only = cleaned.str.fullmatch(r"[A-Za-z]{3,9}") | cleaned.str.fullmatch(r"\d{1,2}")
            month_dt = month_dt.where(~ambiguous_month_only, pd.NaT)

        # If parsed years are bogus (e.g., 0001 or 1900), fix year using start date or report year
        if month_dt.notna().any():
            bad_year = month_dt.dt.year < 2000
            if bad_year.any():
                if start_date_series is not None:
                    start_dt = pd.to_datetime(start_date_series, errors="coerce")
                    year_vals = start_dt.dt.year.where(start_dt.notna(), REPORT_START.year)
                else:
                    year_vals = pd.Series(REPORT_START.year, index=month_dt.index)

                month_dt = month_dt.where(
                    ~bad_year,
                    pd.to_datetime(
                        {
                            "year": year_vals,
                            "month": month_dt.dt.month.clip(1, 12),
                            "day": 1,
                        },
                        errors="coerce",
                    ),
                )

        if month_dt.isna().all() and start_date_series is not None:
            month_dt = pd.to_datetime(start_date_series, errors="coerce")

        if month_dt.isna().all():
            return None

        return month_dt

    def _coalesce_columns(
        self,
        df: pd.DataFrame,
        target: str,
        candidates: list[str],
    ) -> pd.DataFrame:
        available = [c for c in candidates if c in df.columns]
        if not available:
            return df
        combined = df[available].bfill(axis=1).iloc[:, 0]
        df[target] = combined
        return df

    def _claims_amount_series(self, df: pd.DataFrame) -> pd.Series:
        col = self._find_column_by_alias(
            df,
            "Net Amount",
            "Net_Amount",
            "Claim_Amount",
            "Claim Amount",
            "Claims Cost",
            "Claim Cost",
        )
        if col is None:
            return pd.Series(0.0, index=df.index, dtype="float64")
        return self._numeric_series(df[col])

    def _clean_text_series(self, series: pd.Series) -> pd.Series:
        cleaned = series.astype(str).str.strip()
        return cleaned.replace(
            {
                "": pd.NA,
                "nan": pd.NA,
                "none": pd.NA,
                "null": pd.NA,
                "NaN": pd.NA,
                "None": pd.NA,
                "NULL": pd.NA,
            }
        )

    def _canonicalize_device_plan_category(
        self,
        series: pd.Series,
        model_series: pd.Series | None = None,
    ) -> pd.Series:
        raw = self._clean_text_series(series)
        if model_series is not None:
            model_ref = (
                self._clean_text_series(model_series)
                .fillna("")
                .astype(str)
                .str.lower()
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )
        else:
            model_ref = pd.Series("", index=raw.index, dtype="object")

        normalized = (
            raw.fillna("")
            .astype(str)
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.replace("superpremium", "super premium", regex=False)
            .str.replace("flip luxury", "luxury flip", regex=False)
            .str.replace("fold luxury", "luxury fold", regex=False)
        )

        def _label(value: str, model_value: str):
            if not value:
                return pd.NA
            if "super" in value and "premium" in value:
                return "Super Premium"
            if "luxury" in value and "flip" in value:
                return "Luxury Flip"
            if "luxury" in value and "fold" in value:
                return "Luxury Fold"
            if value.startswith("mass"):
                return "Mass"
            if value.startswith("mid"):
                return "Mid"
            if value == "high" or value.startswith("high "):
                return "High"
            if value == "premium" or ("premium" in value and "super" not in value):
                return "Premium"
            if value == "luxury" or "luxury" in value:
                if "flip" in model_value or re.search(r"\bf7\d{2}\b", model_value):
                    return "Luxury Flip"
                if "fold" in model_value or re.search(r"\bf9\d{2}\b", model_value):
                    return "Luxury Fold"
                return "Luxury Fold"
            return pd.NA

        return pd.Series(
            [_label(v, m) for v, m in zip(normalized.tolist(), model_ref.tolist())],
            index=series.index,
            dtype="object",
        )

    def _filter_claims_partner_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not self.source:
            return df

        src = (self.source or "").strip().lower()
        # Partner-specific queries already scope rows at source level; do not
        # rely on marketplace text because vendor files are often inconsistent.
        if src in {"samsung_vs", "samsung_vijay_sales", "samsung_croma"}:
            return df
        is_vs = ("vijay" in src) or src in {"samsung_vs", "samsung_vijay_sales"}
        is_croma = "croma" in src
        if not is_vs and not is_croma:
            return df

        candidate_columns = [
            "Claim Marketplace",
            "Partner Name",
            "Partner",
            "Channel",
            "Market Name",
        ]
        present_columns = [col for col in candidate_columns if col in df.columns]
        if not present_columns:
            return df

        mask = pd.Series(False, index=df.index)
        for col in present_columns:
            values = df[col].astype(str).str.lower().str.strip()
            if is_vs:
                mask |= values.str.contains("vijay", na=False)
                mask |= values.str.fullmatch(r"v\\.?s\\.?|vs", na=False)
            elif is_croma:
                mask |= values.str.contains("croma", na=False)

        filtered = df[mask]
        # If mapping mismatches for some partner files, avoid dropping to empty.
        return filtered if not filtered.empty else df

    def _trim_sparse_croma_claims_tail_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop a one-row terminal month artifact in Samsung Croma claims.

        Some Croma claim uploads contain a single trailing month row that skews
        month-on-month charts (sharp artificial drop). Keep the dataset intact
        unless the latest month is clearly a sparse outlier.
        """
        if df.empty or "Month" not in df.columns:
            return df
        src = (self.source or "").strip().lower()
        if src != "samsung_croma":
            return df

        month_key = pd.to_datetime(df["Month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        valid_months = month_key.dropna()
        if valid_months.empty:
            return df

        counts = valid_months.value_counts().sort_index()
        if len(counts) < 4:
            return df

        last_month = counts.index.max()
        previous = counts[counts.index < last_month]
        if previous.empty:
            return df

        prev_median = float(previous.median())
        last_count = float(counts.loc[last_month])
        # Trim only when latest bucket is a single-row outlier vs historical months.
        if prev_median >= 3.0 and last_count <= 1.0 and last_count <= max(1.0, prev_median * 0.08):
            keep_mask = month_key.ne(last_month) | month_key.isna()
            trimmed = df.loc[keep_mask].copy()
            logger.info(
                "Trimmed sparse croma tail month source=%s last_month=%s removed_rows=%s",
                self.source,
                str(last_month),
                int((~keep_mask).sum()),
            )
            return trimmed
        return df

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------
    def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
        cache_key = (include_sales, include_claims)
        if cache_key in self._loaded_data_cache:
            return self._loaded_data_cache[cache_key]

        shared_key = self._shared_load_cache_key(include_sales, include_claims)
        is_loader = False
        while True:
            now = time.time()
            wait_event: threading.Event | None = None
            with _samsung_load_cache_lock:
                shared_cached = _samsung_load_cache.get(shared_key)
                if shared_cached is not None:
                    expires_at, cached_value = shared_cached
                    if expires_at >= now:
                        cloned = self._clone_loaded_data(cached_value)
                        self._loaded_data_cache[cache_key] = cloned
                        return cloned
                    _samsung_load_cache.pop(shared_key, None)

                wait_event = _samsung_load_inflight.get(shared_key)
                if wait_event is None:
                    wait_event = threading.Event()
                    _samsung_load_inflight[shared_key] = wait_event
                    is_loader = True
                    break

            # Another request is computing the same payload; wait for it and
            # then re-check cache before doing duplicate heavy dataframe work.
            wait_event.wait(timeout=20)

        try:
            total_started = time.perf_counter()
            source_key = (self.source or "").strip().lower()

            def _canonical_source(value: str) -> str:
                key = (value or "").strip().lower()
                if key in {"samsung_vs", "samsung_vijay_sales"}:
                    return "samsung_vs"
                return key

            def _dedupe_sources(values: list[str]) -> list[str]:
                out: list[str] = []
                seen: set[str] = set()
                for value in values:
                    key = _canonical_source(value)
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    out.append(key)
                return out

            def _sources_for(dataset_type: str) -> list[str]:
                if dataset_type == "sales":
                    if not include_sales:
                        return []
                    if source_key == "samsung":
                        return list(SAMSUNG_SOURCE_VARIANTS)
                    return [self.source] if self.source else []

                if dataset_type == "claims":
                    if not include_claims:
                        return []
                    if source_key == "samsung":
                        # Overview mode merges both samsung partners.
                        return list(SAMSUNG_SOURCE_VARIANTS)
                    return [self.source] if self.source else []

                return []

            def _load_frames(dataset_type: str, sources: list[str]) -> pd.DataFrame:
                frames: list[pd.DataFrame] = []
                for src in _dedupe_sources(sources):
                    frame = get_dataframe(
                        db=self.db,
                        job_id=self.job_id,
                        source=src,
                        dataset_type=dataset_type,
                    )
                    if frame is None or frame.empty:
                        continue
                    frames.append(frame.copy(deep=False))

                if not frames:
                    return pd.DataFrame()
                if len(frames) == 1:
                    return frames[0].copy(deep=False)
                return pd.concat(frames, ignore_index=True, sort=False)

            dataframe_started = time.perf_counter()
            sales_df = _load_frames("sales", _sources_for("sales"))
            claims_df = _load_frames("claims", _sources_for("claims"))
            dataframe_duration_ms = (time.perf_counter() - dataframe_started) * 1000
            logger.info(
                "TIMING samsung.load_data.fetch_rows source=%s dataset=%s job_id=%s sales_rows=%s claims_rows=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                self.job_id,
                len(sales_df),
                len(claims_df),
                dataframe_duration_ms,
            )

            logger.info(
                "TIMING samsung.load_data.dataframes source=%s dataset=%s sales_shape=%s claims_shape=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                sales_df.shape,
                claims_df.shape,
                dataframe_duration_ms,
            )

            if sales_df.empty and claims_df.empty:
                logger.info(
                    "TIMING samsung.load_data.total source=%s dataset=%s duration_ms=%.2f",
                    self.source,
                    self.dataset_type,
                    (time.perf_counter() - total_started) * 1000,
                )
                result = {"sales": sales_df, "claims": claims_df}
                self._loaded_data_cache[cache_key] = result
                with _samsung_load_cache_lock:
                    _samsung_load_cache[shared_key] = (time.time() + SAMSUNG_LOAD_CACHE_TTL_SECONDS, self._clone_loaded_data(result))
                return result

            # normalize column names (trim)
            sales_normalization_started = time.perf_counter()
            if not sales_df.empty:
                sales_df.columns = [str(c).strip() for c in sales_df.columns]

            # Coalesce common variants into canonical columns (works even when both variants exist).
            if not sales_df.empty:
                sales_df = self._coalesce_columns(sales_df, "Start_Date", ["Start_Date", "Start Date", "Plan Start Date"])
                sales_df = self._coalesce_columns(sales_df, "End_Date", ["End_Date", "End Date", "Plan End Date"])
                sales_df = self._coalesce_columns(sales_df, "Month", ["Month", "Month ", "Month Name", "Month_Name"])
                sales_df = self._coalesce_columns(sales_df, "State", ["State", "State / City", "State/City"])

            if not sales_df.empty:
                if "Start_Date" in sales_df.columns:
                    try:
                        sales_df["Start_Date"] = pd.to_datetime(sales_df["Start_Date"], format="mixed", errors="coerce")
                    except TypeError:
                        sales_df["Start_Date"] = pd.to_datetime(sales_df["Start_Date"], errors="coerce")
                if "End_Date" in sales_df.columns:
                    try:
                        sales_df["End_Date"] = pd.to_datetime(sales_df["End_Date"], format="mixed", errors="coerce")
                    except TypeError:
                        sales_df["End_Date"] = pd.to_datetime(sales_df["End_Date"], errors="coerce")
                if "Date" in sales_df.columns:
                    try:
                        sales_df["Date"] = pd.to_datetime(sales_df["Date"], format="mixed", errors="coerce")
                    except TypeError:
                        sales_df["Date"] = pd.to_datetime(sales_df["Date"], errors="coerce")
                # Keep raw Month values; parsing is handled centrally in _parse_month_series


            # Flag Extended Warranty (EW) rows for downstream logic
            if not sales_df.empty:
                sales_df["_ew"] = self._is_ew_plan(sales_df)
                if "Start_Date" in sales_df.columns:
                    sales_df["_adj_start_date"] = sales_df["Start_Date"].where(~sales_df["_ew"])
                    sales_df.loc[sales_df["_ew"], "_adj_start_date"] = sales_df.loc[
                        sales_df["_ew"], "Start_Date"
                    ] + pd.DateOffset(years=1)
                if "End_Date" in sales_df.columns:
                    sales_df["_adj_end_date"] = sales_df["End_Date"].where(~sales_df["_ew"])
                    sales_df.loc[sales_df["_ew"], "_adj_end_date"] = sales_df.loc[
                        sales_df["_ew"], "End_Date"
                    ] + pd.DateOffset(years=1)
            logger.info(
                "TIMING samsung.load_data.sales_normalization source=%s dataset=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                (time.perf_counter() - sales_normalization_started) * 1000,
            )

            # Normalize claims columns
            claims_normalization_started = time.perf_counter()
            if not claims_df.empty:
                claims_df.columns = [str(c).strip() for c in claims_df.columns]
                has_explicit_device_category = any(
                    col in claims_df.columns
                    for col in ["Device Plan Category", "Device_Plan_Category", "Category", "Device Category"]
                )
                rename_map = {
                    "Partner Name": "Partner Name",
                    "Partner_Name": "Partner Name",
                    "Claim Marketplace": "Claim Marketplace",
                    "Claim_Marketplace": "Claim Marketplace",
                    "Net Amount": "Net Amount",
                    "Net_Amount": "Net Amount",
                    "otd amount": "OTD Amount",
                    "OTD Amount": "OTD Amount",
                    "One time deductible": "OTD Amount",
                "One Time Deductible": "OTD Amount",
                "Plan Category": "Plan Category",
                "Plan_Category": "Plan Category",
                "Device Plan Category": "Device Plan Category",
                "Device_Plan_Category": "Device Plan Category",
                "Day of Call_Date": "Day of Call_Date",
                    "Call Date": "Call_Date",
                    "Call_Date": "Call_Date",
                    "Month": "Month",
                    "Month-Year": "Month",
                    "Month Year": "Month",
                    "Month_Year": "Month",
                    "Month Name": "Month",
                    "Month_Name": "Month",
                    "Fiscal Month": "Fiscal Month",
                    "State / City": "State",
                    "State/City": "State",
                    "Pack type": "Plan Category",
                    "Category": "Device Plan Category",
                    "Device Category": "Device Plan Category",
                }
                col_renames = {}
                for src, dest in rename_map.items():
                    if src in claims_df.columns and dest not in claims_df.columns:
                        col_renames[src] = dest
                if col_renames:
                    claims_df = claims_df.rename(columns=col_renames)

                # Canonicalize claim categories:
                # - Plan Category comes from plan type fields (ADLD/Combo/SP...)
                # - Device Plan Category comes from device-segment fields (Mass/Mid/High...)
                if "Plan Category" in claims_df.columns:
                    claims_df["Plan Category"] = self._clean_text_series(claims_df["Plan Category"])
                else:
                    claims_df["Plan Category"] = pd.NA
                if "Pack type" in claims_df.columns:
                    claims_df["Pack type"] = self._clean_text_series(claims_df["Pack type"])
                    claims_df["Plan Category"] = claims_df["Plan Category"].fillna(claims_df["Pack type"])

                if "Device Plan Category" in claims_df.columns:
                    claims_df["Device Plan Category"] = self._clean_text_series(claims_df["Device Plan Category"])
                else:
                    claims_df["Device Plan Category"] = pd.NA
                for device_fallback_col in ["Category", "Device Category"]:
                    if device_fallback_col in claims_df.columns:
                        claims_df[device_fallback_col] = self._clean_text_series(claims_df[device_fallback_col])
                        claims_df["Device Plan Category"] = claims_df["Device Plan Category"].fillna(
                            claims_df[device_fallback_col]
                        )

                model_ref: pd.Series | None = None
                for model_col in ["Model Code", "Model Code-1", "Model"]:
                    if model_col in claims_df.columns:
                        claims_df[model_col] = self._clean_text_series(claims_df[model_col])
                        model_ref = (
                            claims_df[model_col]
                            if model_ref is None
                            else model_ref.fillna(claims_df[model_col])
                        )
                claims_df["Device Plan Category"] = self._canonicalize_device_plan_category(
                    claims_df["Device Plan Category"],
                    model_ref,
                )
                # Keep alias columns synchronized so downstream dimension matching
                # does not accidentally pick stale blank alias fields.
                claims_df["Plan_Category"] = claims_df["Plan Category"]
                claims_df["Device_Plan_Category"] = claims_df["Device Plan Category"]

                # Legacy fallback: if no device segment column exists in the upload,
                # keep dashboard populated using plan categories.
                if (
                    not has_explicit_device_category
                    and "Plan Category" in claims_df.columns
                    and claims_df["Device Plan Category"].isna().all()
                ):
                    claims_df["Device Plan Category"] = claims_df["Plan Category"]

                if "Partner Name" in claims_df.columns:
                    claims_df["Partner Name"] = (
                        claims_df["Partner Name"]
                        .astype(str)
                        .str.replace(" Bulk", "", regex=False)
                        .str.strip()
                    )

                # Build canonical claims month with Fiscal Month as the primary source.
                # Claims uploads are monthly and fiscal buckets are typically the stable
                # timeline anchor; claim/call dates remain fallback.
                month_from_claim = pd.Series(pd.NaT, index=claims_df.index, dtype="datetime64[ns]")
                for claim_col in ["Claim Date", "Day of Call_Date", "Call_Date", "Call Date", "Date"]:
                    if claim_col not in claims_df.columns:
                        continue
                    try:
                        parsed_claim = pd.to_datetime(claims_df[claim_col], format="mixed", errors="coerce")
                    except TypeError:
                        parsed_claim = pd.to_datetime(claims_df[claim_col], errors="coerce")
                    if parsed_claim.notna().any():
                        month_from_claim = month_from_claim.where(month_from_claim.notna(), parsed_claim)
                month_from_claim = month_from_claim.dt.to_period("M").dt.to_timestamp()

                fiscal_month = pd.Series(pd.NaT, index=claims_df.index, dtype="datetime64[ns]")
                if "Fiscal Month" in claims_df.columns:
                    parsed_fiscal = self._parse_month_series(claims_df["Fiscal Month"])
                    if parsed_fiscal is not None and parsed_fiscal.notna().any():
                        fiscal_month = parsed_fiscal.dt.to_period("M").dt.to_timestamp()

                existing_month = pd.Series(pd.NaT, index=claims_df.index, dtype="datetime64[ns]")
                if "Month" in claims_df.columns:
                    parsed_existing_month = self._parse_month_series(claims_df["Month"])
                    if parsed_existing_month is not None and parsed_existing_month.notna().any():
                        existing_month = parsed_existing_month.dt.to_period("M").dt.to_timestamp()

                canonical_month = fiscal_month.where(fiscal_month.notna(), existing_month)
                # Guard against ambiguous source date strings being interpreted in
                # current year (e.g. "25-Aug" -> 2026-08-25). If Fiscal Month
                # exists and month matches but year differs, trust Fiscal Month.
                if canonical_month.notna().any() and fiscal_month.notna().any():
                    same_month = (
                        canonical_month.dt.month.eq(fiscal_month.dt.month)
                        & canonical_month.notna()
                        & fiscal_month.notna()
                    )
                    year_mismatch = canonical_month.dt.year.ne(fiscal_month.dt.year)
                    mismatch_mask = same_month & year_mismatch
                    if mismatch_mask.any():
                        canonical_month = canonical_month.where(~mismatch_mask, fiscal_month)
                canonical_month = canonical_month.where(canonical_month.notna(), month_from_claim)
                if canonical_month.notna().any():
                    claims_df["Month"] = canonical_month
                    claims_df = self._trim_sparse_croma_claims_tail_month(claims_df)

                # Apply date filter to claims using a coalesced date series.
                # Some files provide partial values across multiple date columns
                # (e.g. Call_Date for some rows, Month/Fiscal Month for others).
                date_series = (
                    canonical_month.copy()
                    if canonical_month.notna().any()
                    else pd.Series(pd.NaT, index=claims_df.index, dtype="datetime64[ns]")
                )

                def _parse_claim_date_column(column: str) -> pd.Series:
                    raw = claims_df[column]
                    if column in {"Month", "Month-Year", "Fiscal Month"}:
                        parsed_month = self._parse_month_series(raw)
                        if parsed_month is not None:
                            return parsed_month
                        return pd.Series(pd.NaT, index=claims_df.index, dtype="datetime64[ns]")
                    try:
                        return pd.to_datetime(raw, format="mixed", errors="coerce")
                    except TypeError:
                        return pd.to_datetime(raw, errors="coerce")

                for col in [
                    "Claim Date",
                    "Day of Call_Date",
                    "Call_Date",
                    "Call Date",
                    "Date",
                    "Month",
                    "Month-Year",
                    "Fiscal Month",
                    "Payment_date",
                    "Payment Date",
                ]:
                    if col not in claims_df.columns:
                        continue
                    parsed = _parse_claim_date_column(col)
                    if parsed.isna().all():
                        continue
                    date_series = date_series.where(date_series.notna(), parsed)

                if self.apply_date_filter and date_series.notna().any():
                    filter_start = self.report_start
                    filter_end = self.report_end
                    non_na = date_series.dropna()
                    if not non_na.empty and float(non_na.dt.is_month_start.mean()) >= 0.9:
                        if filter_start is not None:
                            filter_start = pd.Timestamp(filter_start).to_period("M").to_timestamp()
                        if filter_end is not None:
                            filter_end = pd.Timestamp(filter_end).to_period("M").to_timestamp(how="end")
                    mask = pd.Series(True, index=claims_df.index)
                    if filter_start is not None:
                        mask &= date_series >= filter_start
                    if filter_end is not None:
                        mask &= date_series <= filter_end
                    claims_df = claims_df[mask]
            logger.info(
                "TIMING samsung.load_data.claims_normalization source=%s dataset=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                (time.perf_counter() - claims_normalization_started) * 1000,
            )
            logger.info(
                "TIMING samsung.load_data.total source=%s dataset=%s duration_ms=%.2f",
                self.source,
                self.dataset_type,
                (time.perf_counter() - total_started) * 1000,
            )
            result = {"sales": sales_df, "claims": claims_df}
            self._loaded_data_cache[cache_key] = result
            with _samsung_load_cache_lock:
                _samsung_load_cache[shared_key] = (time.time() + SAMSUNG_LOAD_CACHE_TTL_SECONDS, self._clone_loaded_data(result))
            return result
        finally:
            if is_loader:
                with _samsung_load_cache_lock:
                    event = _samsung_load_inflight.pop(shared_key, None)
                if event is not None:
                    event.set()

    # --------------------------------------------------
    # EARNED (ROW LEVEL)
    # --------------------------------------------------
    def _earned(self, df: pd.DataFrame, col: str) -> pd.Series:
        return self._earned_with_dates(df, col, df["Start_Date"], df["End_Date"])

    def _earned_with_dates(
        self,
        df: pd.DataFrame,
        col: str,
        start_dates: pd.Series,
        end_dates: pd.Series,
    ) -> pd.Series:
        base_amount = pd.to_numeric(df[col], errors="coerce").fillna(0)
        eff_start = start_dates.clip(lower=self.report_start)
        eff_end = end_dates.clip(upper=self.report_end)

        exposure = (eff_end - eff_start).dt.days + 1
        coverage = (end_dates - start_dates).dt.days + 1

        ratio = (exposure / coverage).clip(lower=0, upper=1)
        earned = (base_amount * ratio).fillna(0)

        invalid = (coverage <= 0) | coverage.isna()
        earned = earned.where(~invalid, 0)

        # Safety cap to written premium
        earned = earned.clip(lower=0, upper=base_amount)
        return earned.fillna(0)

    # --------------------------------------------------
    # FIND POLICY COLUMN
    # --------------------------------------------------
    def _find_policy_column(self, df: pd.DataFrame) -> str | None:
        def _norm(s: str) -> str:
            return s.lower().replace(" ", "").replace("_", "")

        candidates = [
            "policy number",
            "policy no",
            "policy_id",
            "policyid",
            "plan id",
            "plan_id",
            "order id",
            "order_id",
        ]

        norm_cols = {_norm(c): c for c in df.columns}
        for key in candidates:
            k = _norm(key)
            if k in norm_cols:
                return norm_cols[k]
        return None

    def _is_ew_plan(self, df: pd.DataFrame) -> pd.Series:
        candidates = ["Plan_Category", "Plan Category", "Device Plan Category"]
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

    def _sales_date_series(
        self,
        df: pd.DataFrame,
        use_adjusted: bool,
    ) -> pd.Series | None:
        def _parse(series: pd.Series) -> pd.Series:
            raw = series.astype(str).str.strip()
            try:
                parsed = pd.to_datetime(raw, format="mixed", errors="coerce")
            except TypeError:
                parsed = pd.to_datetime(raw, errors="coerce")

            # pandas can parse values like "Jan-26" as year 0001; fix those explicitly.
            bad_year = parsed.dt.year < 2000
            if bad_year.any():
                mon_yy = pd.to_datetime(raw, format="%b-%y", errors="coerce")
                if mon_yy.notna().any():
                    parsed = parsed.where(~bad_year, mon_yy)
                    bad_year = parsed.dt.year < 2000
                if bad_year.any():
                    day_mon = pd.to_datetime(raw, format="%d-%b", errors="coerce")
                    if day_mon.notna().any():
                        inferred_year = self.report_end.year if self.report_end is not None else pd.Timestamp.now().year
                        day_mon = day_mon.map(lambda dt: dt.replace(year=inferred_year) if not pd.isna(dt) else dt)
                        parsed = parsed.where(~bad_year, day_mon)

            return parsed

        # For earned/loss calculations prefer policy period dates over transaction "Date".
        if use_adjusted:
            preferred_cols = ["_adj_start_date", "Start_Date", "_adj_end_date", "End_Date", "Month", "Date"]
        else:
            preferred_cols = ["Date", "Start_Date", "Month", "End_Date"]

        coalesced = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        for col in preferred_cols:
            if col not in df.columns:
                continue
            series = _parse(df[col])
            if series.isna().all():
                continue
            # Coalesce date candidates row-wise so sparse Date columns do not
            # drop valid rows (notably EW rows with only Start_Date populated).
            coalesced = coalesced.where(coalesced.notna(), series)
            if coalesced.notna().all():
                break

        if coalesced.isna().all():
            return None
        return coalesced

    def _apply_sales_date_filter(
        self,
        df: pd.DataFrame,
        use_adjusted: bool,
    ) -> pd.DataFrame:
        if not self.apply_date_filter or df.empty:
            return df
        date_series = self._sales_date_series(df, use_adjusted=use_adjusted)
        if date_series is None:
            return df
        mask = pd.Series(True, index=df.index)
        if self.report_start is not None:
            mask &= date_series >= self.report_start
        if self.report_end is not None:
            mask &= date_series <= self.report_end
        return df[mask]

    def _apply_sales_overlap_filter(
        self,
        df: pd.DataFrame,
        use_adjusted: bool,
    ) -> pd.DataFrame:
        if not self.apply_date_filter or df.empty:
            return df

        start_col = None
        end_col = None
        if use_adjusted and "_adj_start_date" in df.columns:
            start_col = "_adj_start_date"
        elif "Start_Date" in df.columns:
            start_col = "Start_Date"

        if use_adjusted and "_adj_end_date" in df.columns:
            end_col = "_adj_end_date"
        elif "End_Date" in df.columns:
            end_col = "End_Date"

        if start_col is None or end_col is None:
            return self._apply_sales_date_filter(df, use_adjusted=use_adjusted)

        start_series = pd.to_datetime(df[start_col], errors="coerce")
        end_series = pd.to_datetime(df[end_col], errors="coerce")
        if start_series.isna().all() or end_series.isna().all():
            return self._apply_sales_date_filter(df, use_adjusted=use_adjusted)

        mask = pd.Series(True, index=df.index)
        if self.report_start is not None:
            mask &= end_series >= self.report_start
        if self.report_end is not None:
            mask &= start_series <= self.report_end
        return df[mask]

    def _numeric_series(self, series: pd.Series) -> pd.Series:
        cleaned = (
            series.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("INR", "", regex=False)
            .str.replace("Rs.", "", regex=False)
            .str.replace("Rs", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)

    def _find_column_by_alias(self, df: pd.DataFrame, *candidates: str) -> str | None:
        def _norm_key(name: str) -> str:
            return (
                str(name)
                .strip()
                .lower()
                .replace("_", "")
                .replace(" ", "")
                .replace("(", "")
                .replace(")", "")
                .replace("-", "")
            )

        normalized = {_norm_key(c): c for c in df.columns}
        for candidate in candidates:
            key = _norm_key(candidate)
            if key in normalized:
                return normalized[key]
        return None

    def _earned_ratio_from_days(self, df: pd.DataFrame) -> pd.Series | None:
        earned_days_col = self._find_column_by_alias(df, "earned_days", "Earned Days", "Earned_Days")
        policy_days_col = self._find_column_by_alias(df, "policy_days", "Policy Days", "Policy_Days")
        if earned_days_col is None or policy_days_col is None:
            return None

        earned_days = self._numeric_series(df[earned_days_col])
        policy_days = self._numeric_series(df[policy_days_col]).replace(0, pd.NA)
        ratio = (earned_days / policy_days).replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)
        return ratio.clip(lower=0.0, upper=1.0)

    # --------------------------------------------------
    # MAIN AGGREGATION
    # --------------------------------------------------
    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        total_started = time.perf_counter()
        load_started = time.perf_counter()
        needs_sales = self.dataset_type == "sales" or metric == "loss_ratio"
        needs_claims = self.dataset_type == "claims"
        data = self.load_data(include_sales=needs_sales, include_claims=needs_claims)
        logger.info(
            "TIMING samsung.compute_by_dimension.load_data source=%s dataset=%s dimension=%s metric=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            dimension,
            metric,
            (time.perf_counter() - load_started) * 1000,
        )
        if self.dataset_type == "claims":
            df = data["claims"]
        else:
            df = data["sales"]

        if df.empty:
            logger.info(
                "TIMING samsung.compute_by_dimension.total source=%s dataset=%s dimension=%s metric=%s rows=0 duration_ms=%.2f",
                self.source,
                self.dataset_type,
                dimension,
                metric,
                (time.perf_counter() - total_started) * 1000,
            )
            return []

        policy_col = self._find_policy_column(df)

        # ---------------- METRIC ----------------
        metric_started = time.perf_counter()
        loss_ratio_mode = False
        if self.dataset_type == "claims":
            # Partner split based on source for samsung overview
            df = self._filter_claims_partner_rows(df)

            if metric == "claims":
                df["_value"] = self._claims_amount_series(df)
            elif metric == "net_claims":
                net_amt = self._claims_amount_series(df)
                if "OTD Amount" in df.columns:
                    otd = pd.to_numeric(df["OTD Amount"], errors="coerce").fillna(0)
                else:
                    otd = 0
                df["_value"] = net_amt - otd
            elif metric == "loss_ratio":
                loss_ratio_mode = True
            elif metric == "quantity":
                df["_value"] = 1
            else:
                return []
        else:
            # Gross premium should follow transaction/start-date scope.
            # Adjusted dates are only for earned-style metrics.
            premium_metric = metric in {"earned_premium", "zopper_earned_premium"}
            df = self._apply_sales_date_filter(df, use_adjusted=premium_metric)
            amount_col = self._find_column_by_alias(
                df,
                "Amount",
                "Gross Premium",
                "gross_premium",
                "Plan Selling Price",
                "Customer Premium",
            )
            earned_col = self._find_column_by_alias(df, "Earned Premium", "Earned_Premium", "earned_premium")
            zopper_earned_col = self._find_column_by_alias(
                df,
                "Zopper Earned Premium",
                "zopper_earned_premium",
                "earned_zopper",
            )
            zopper_share_col = self._find_column_by_alias(
                df,
                "Zopper Share",
                "Zopper Shared ( Transfer Price )",
                "transfer_price",
            )
            earned_ratio = self._earned_ratio_from_days(df)
            if metric == "gross_premium":
                if amount_col is None:
                    return []
                df["_value"] = self._numeric_series(df[amount_col])

            elif metric == "earned_premium":
                if earned_col is not None:
                    df["_value"] = self._numeric_series(df[earned_col])
                elif amount_col is not None and earned_ratio is not None:
                    df["_value"] = self._numeric_series(df[amount_col]) * earned_ratio
                elif amount_col is not None and "_adj_start_date" in df.columns and "_adj_end_date" in df.columns:
                    df["_value"] = self._earned_with_dates(
                        df, amount_col, df["_adj_start_date"], df["_adj_end_date"]
                    )
                elif amount_col is not None:
                    df["_value"] = self._numeric_series(df[amount_col])
                else:
                    return []

            elif metric == "zopper_earned_premium":
                if zopper_earned_col is not None:
                    df["_value"] = self._numeric_series(df[zopper_earned_col])
                elif zopper_share_col is not None and earned_ratio is not None:
                    df["_value"] = self._numeric_series(df[zopper_share_col]) * earned_ratio * ZOPPER_GST_MULTIPLIER
                elif zopper_share_col is not None and "_adj_start_date" in df.columns and "_adj_end_date" in df.columns:
                    df["_value"] = self._earned_with_dates(
                        df, zopper_share_col, df["_adj_start_date"], df["_adj_end_date"]
                    ) * ZOPPER_GST_MULTIPLIER
                elif zopper_share_col is not None:
                    df["_value"] = self._numeric_series(df[zopper_share_col]) * ZOPPER_GST_MULTIPLIER
                else:
                    return []

            elif metric == "quantity":
                df["_value"] = 1

            else:
                return []
        logger.info(
            "TIMING samsung.compute_by_dimension.metric_prep source=%s dataset=%s dimension=%s metric=%s rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            dimension,
            metric,
            len(df),
            (time.perf_counter() - metric_started) * 1000,
        )

        # ---------------- DIMENSION ----------------
        dimension_started = time.perf_counter()
        DIMENSION_MAP = {
            "month": ["Month", "Month-Year", "Date", "month", "Fiscal Month", "Day of Call_Date", "Call_Date"],
            "state": [
                "State",
                "State Name",
                "State/UT",
                "State_UT",
                "State_UT_Name",
                "State / City",
                "State/City",
            ],
            "plan_category": ["Plan_Category", "Plan Category"],
            "device_plan_category": ["Device_Plan_Category", "Device Plan Category"],
        }

        def _norm(s: str) -> str:
            return s.lower().replace(" ", "").replace("_", "")

        dim_key = dimension.lower()
        candidates = DIMENSION_MAP.get(dim_key, [dimension])

        def _non_empty_count(series: pd.Series) -> int:
            return int(self._clean_text_series(series).notna().sum())

        def _find_dim_column(frame: pd.DataFrame, cand: list[str]) -> str | None:
            available: list[str] = []
            for c in cand:
                if c in frame.columns:
                    available.append(c)
            if not available:
                # try normalized matches for each candidate
                for c in cand:
                    target = _norm(c)
                    matched = next((col for col in frame.columns if _norm(col) == target), None)
                    if matched is not None and matched not in available:
                        available.append(matched)
            if not available:
                return None
            # Prefer the most populated candidate (fixes blank alias column picks).
            best = max(available, key=lambda col: _non_empty_count(frame[col]))
            return best

        dim = _find_dim_column(df, candidates)
        if dim is None:
            # try normalized match
            # Claims files often carry only Device Plan Category; for plan_category
            # views use it as the nearest categorical fallback.
            if self.dataset_type == "claims" and dim_key == "plan_category":
                device_candidates = DIMENSION_MAP.get("device_plan_category", ["Device Plan Category"])
                matched_device = _find_dim_column(df, device_candidates)
                if matched_device is not None:
                    dim = matched_device
                else:
                    return []
            # special: derive Month from Start_Date if missing
            elif dim_key == "month" and "Start_Date" in df.columns:
                df = df.copy()
                df["Month"] = pd.to_datetime(df["Start_Date"], errors="coerce")
                dim = "Month"
            elif dim is None:
                return []

        # For plan_category loss ratio, prefer true plan category labels.
        # Fall back to device category only when plan category isn't available.
        if (
            loss_ratio_mode
            and dim_key == "plan_category"
            and dim not in {"Plan_Category", "Plan Category"}
            and "Device Plan Category" in df.columns
        ):
            dim = "Device Plan Category"

        # 🔥 FIX: DEDUPE FOR CATEGORY DIMENSIONS
        if policy_col and dim in ("Plan_Category", "Plan Category", "Device_Plan_Category", "Device Plan Category"):
            df = df.dropna(subset=[dim])
            df = df.drop_duplicates(subset=[policy_col, dim])

        if dim_key == "device_plan_category" and dim in df.columns:
            model_ref: pd.Series | None = None
            for model_col in ["Model Code", "Model Code-1", "Model"]:
                if model_col in df.columns:
                    model_ref = df[model_col] if model_ref is None else model_ref.fillna(df[model_col])
            df = df.copy()
            df[dim] = self._canonicalize_device_plan_category(df[dim], model_ref)

        if dim_key == "month":
            if self.dataset_type == "claims":
                start_series = None
                month_source = df[dim]
                if "Fiscal Month" in df.columns:
                    fiscal_month_source = self._parse_month_series(df["Fiscal Month"])
                    if fiscal_month_source is not None and fiscal_month_source.notna().any():
                        # Keep Fiscal Month as primary, but do not drop rows where it is blank.
                        month_source = fiscal_month_source.where(fiscal_month_source.notna(), month_source)
            else:
                start_series = None
                if "Date" in df.columns:
                    date_series = pd.to_datetime(df["Date"], errors="coerce")
                    if not date_series.isna().all():
                        start_series = date_series
                if start_series is None and "Start_Date" in df.columns:
                    start_series = df["Start_Date"]
                if metric == "quantity" and start_series is not None and not start_series.isna().all():
                    # Quantity should follow policy start month for consistent partner comparison.
                    month_source = start_series
                elif metric in {"gross_premium", "earned_premium", "zopper_earned_premium"} and start_series is not None:
                    month_source = start_series
                elif metric in {"gross_premium", "earned_premium", "zopper_earned_premium"} and "_adj_start_date" in df.columns:
                    month_source = df["_adj_start_date"]
                else:
                    month_source = df[dim]
            month_dt = self._parse_month_series(month_source, start_series)

            # Force monthly grouping key
            if month_dt is not None:
                df["_month_key"] = month_dt.dt.to_period("M").dt.to_timestamp()
                dim = "_month_key"

        if loss_ratio_mode:
            sales_df = data.get("sales", pd.DataFrame())
            if sales_df.empty:
                return []

            if "Net Amount" not in df.columns:
                return []

            def _norm_dim(series: pd.Series) -> pd.Series:
                return (
                    series
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r"^\d+\s*-\s*", "", regex=True)
                    .str.replace("_", " ", regex=False)
                    .str.replace(r"[^a-z0-9]+", " ", regex=True)
                    .str.replace(r"\s+", " ", regex=True)
                    .str.replace(r"\bsp\b", "screen protection", regex=True)
                    .str.replace(r"\bplan\b", "", regex=True)
                    .str.replace(r"\s+", " ", regex=True)
                    .str.strip()
                )

            claims_df = df.copy()
            net_amt = pd.to_numeric(claims_df["Net Amount"], errors="coerce").fillna(0)
            if "OTD Amount" in claims_df.columns:
                otd = pd.to_numeric(claims_df["OTD Amount"], errors="coerce").fillna(0)
            else:
                otd = 0
            claims_df["_net_claims"] = net_amt - otd

            sales_dim_candidates: list[str] = []
            primary_sales_dim = _find_dim_column(sales_df, candidates)
            if primary_sales_dim is not None:
                sales_dim_candidates.append(primary_sales_dim)
            if dim_key in {"plan_category", "device_plan_category"}:
                for alt_candidates in (
                    DIMENSION_MAP.get("plan_category", ["Plan Category"]),
                    DIMENSION_MAP.get("device_plan_category", ["Device Plan Category"]),
                ):
                    alt_dim = _find_dim_column(sales_df, alt_candidates)
                    if alt_dim is not None and alt_dim not in sales_dim_candidates:
                        sales_dim_candidates.append(alt_dim)
            if not sales_dim_candidates:
                return []

            if dim_key in {"plan_category", "device_plan_category"} and len(sales_dim_candidates) > 1:
                claim_values = _norm_dim(claims_df[dim]).dropna().astype(str)
                claim_set = set(claim_values.tolist())
                best_dim = sales_dim_candidates[0]
                best_score = -1
                for candidate_dim in sales_dim_candidates:
                    candidate_values = _norm_dim(sales_df[candidate_dim]).dropna().astype(str)
                    score = len(set(candidate_values.tolist()) & claim_set)
                    if score > best_score:
                        best_score = score
                        best_dim = candidate_dim
                sales_dim = best_dim
            else:
                sales_dim = sales_dim_candidates[0]

            sales_df = sales_df.copy()
            if dim_key == "device_plan_category" and sales_dim in sales_df.columns:
                sales_df[sales_dim] = self._canonicalize_device_plan_category(sales_df[sales_dim])
            if self.apply_date_filter and self.report_start is not None and self.report_end is not None:
                # Loss ratio denominator should include policies overlapping the
                # report window, not only those whose start date falls inside it.
                if "_adj_start_date" in sales_df.columns and "_adj_end_date" in sales_df.columns:
                    overlap_mask = (
                        (sales_df["_adj_end_date"] >= self.report_start)
                        & (sales_df["_adj_start_date"] <= self.report_end)
                    )
                    sales_df = sales_df[overlap_mask]
                elif "Start_Date" in sales_df.columns and "End_Date" in sales_df.columns:
                    overlap_mask = (
                        (sales_df["End_Date"] >= self.report_start)
                        & (sales_df["Start_Date"] <= self.report_end)
                    )
                    sales_df = sales_df[overlap_mask]
                else:
                    sales_df = self._apply_sales_date_filter(sales_df, use_adjusted=True)
            else:
                sales_df = self._apply_sales_date_filter(sales_df, use_adjusted=True)
            if dim_key == "month":
                start_series = None
                if "Date" in sales_df.columns:
                    date_series = pd.to_datetime(sales_df["Date"], errors="coerce")
                    if not date_series.isna().all():
                        start_series = date_series
                if start_series is None and "Start_Date" in sales_df.columns:
                    start_series = sales_df["Start_Date"]
                if start_series is None and "_adj_start_date" in sales_df.columns:
                    adj_series = pd.to_datetime(sales_df["_adj_start_date"], errors="coerce")
                    if not adj_series.isna().all():
                        start_series = adj_series
                if start_series is not None and not start_series.isna().all():
                    month_source = start_series
                else:
                    month_source = sales_df[sales_dim]
                month_dt = self._parse_month_series(month_source, start_series)
                if month_dt is not None:
                    sales_df["_month_key"] = month_dt.dt.to_period("M").dt.to_timestamp()
                    sales_dim = "_month_key"

            zopper_earned_col = self._find_column_by_alias(
                sales_df,
                "Zopper Earned Premium",
                "zopper_earned_premium",
                "earned_zopper",
            )
            zopper_share_col = self._find_column_by_alias(
                sales_df,
                "Zopper Share",
                "Zopper Shared ( Transfer Price )",
                "transfer_price",
            )
            earned_ratio = self._earned_ratio_from_days(sales_df)
            if zopper_earned_col is not None:
                sales_df["_zp"] = self._numeric_series(sales_df[zopper_earned_col])
            elif zopper_share_col is not None and earned_ratio is not None:
                sales_df["_zp"] = self._numeric_series(sales_df[zopper_share_col]) * earned_ratio * ZOPPER_GST_MULTIPLIER
            elif (
                zopper_share_col is not None
                and "_adj_start_date" in sales_df.columns
                and "_adj_end_date" in sales_df.columns
            ):
                sales_df["_zp"] = self._earned_with_dates(
                    sales_df,
                    zopper_share_col,
                    sales_df["_adj_start_date"],
                    sales_df["_adj_end_date"],
                ) * ZOPPER_GST_MULTIPLIER
            elif zopper_share_col is not None and "Start_Date" in sales_df.columns and "End_Date" in sales_df.columns:
                sales_df["_zp"] = self._earned_with_dates(
                    sales_df,
                    zopper_share_col,
                    sales_df["Start_Date"],
                    sales_df["End_Date"],
                ) * ZOPPER_GST_MULTIPLIER
            elif zopper_share_col is not None:
                sales_df["_zp"] = self._numeric_series(sales_df[zopper_share_col]) * ZOPPER_GST_MULTIPLIER
            else:
                return []

            claims_out = (
                claims_df
                .groupby(dim, dropna=False)["_net_claims"]
                .sum()
                .reset_index()
            )
            if dim_key == "month":
                claim_month_key = pd.to_datetime(claims_out[dim], errors="coerce").dt.to_period("M")
                claims_out = claims_out[claim_month_key.notna()].copy()
                claims_out["_k"] = claim_month_key[claim_month_key.notna()].astype(str)
                month_index = pd.DatetimeIndex(sorted(claim_month_key.dropna().dt.to_timestamp().unique()))
                if month_index.empty:
                    return []

                # For monthly loss ratio, allocate zopper premium across active
                # coverage months instead of lumping by policy start month.
                if zopper_share_col is not None:
                    zopper_total = self._numeric_series(sales_df[zopper_share_col]) * ZOPPER_GST_MULTIPLIER
                elif zopper_earned_col is not None:
                    zopper_total = self._numeric_series(sales_df[zopper_earned_col])
                else:
                    zopper_total = self._numeric_series(sales_df["_zp"])

                if "_adj_start_date" in sales_df.columns:
                    start_dt = pd.to_datetime(sales_df["_adj_start_date"], errors="coerce")
                elif "Start_Date" in sales_df.columns:
                    start_dt = pd.to_datetime(sales_df["Start_Date"], errors="coerce")
                elif "Date" in sales_df.columns:
                    start_dt = pd.to_datetime(sales_df["Date"], errors="coerce")
                else:
                    start_dt = pd.Series(pd.NaT, index=sales_df.index)

                if "_adj_end_date" in sales_df.columns:
                    end_dt = pd.to_datetime(sales_df["_adj_end_date"], errors="coerce")
                elif "End_Date" in sales_df.columns:
                    end_dt = pd.to_datetime(sales_df["End_Date"], errors="coerce")
                else:
                    end_dt = pd.Series(pd.NaT, index=sales_df.index)

                policy_days = (end_dt - start_dt).dt.days + 1
                valid = start_dt.notna() & end_dt.notna() & policy_days.gt(0) & zopper_total.ne(0)

                monthly_denominator = pd.Series(0.0, index=month_index, dtype="float64")
                for month_start in month_index:
                    month_start = pd.Timestamp(month_start)
                    month_end = (month_start + MonthEnd(1)).normalize()
                    overlap_start = start_dt.clip(lower=month_start)
                    overlap_end = end_dt.clip(upper=month_end)
                    overlap_days = (overlap_end - overlap_start).dt.days + 1
                    overlap_days = overlap_days.clip(lower=0)
                    accrued = (
                        zopper_total
                        * (overlap_days / policy_days.where(policy_days.gt(0), pd.NA))
                    ).fillna(0.0)
                    monthly_denominator.loc[month_start] = float(accrued[valid].sum())

                invalid = (~valid) & zopper_total.ne(0)
                if invalid.any():
                    fallback_parsed = self._parse_month_series(sales_df.loc[invalid, sales_dim])
                    if fallback_parsed is not None:
                        fallback_month = self._month_key(fallback_parsed)
                        fallback_df = pd.DataFrame(
                            {
                                "_k": fallback_month.dt.to_period("M").astype(str),
                                "_zp": zopper_total.loc[invalid],
                            }
                        )
                        fallback_df = fallback_df[fallback_df["_k"].notna()]
                        if not fallback_df.empty:
                            fallback_g = fallback_df.groupby("_k", dropna=False)["_zp"].sum()
                            for key, value in fallback_g.items():
                                month_key = pd.to_datetime(key, errors="coerce")
                                if pd.notna(month_key):
                                    ts_key = month_key.to_period("M").to_timestamp()
                                    if ts_key in monthly_denominator.index:
                                        monthly_denominator.loc[ts_key] += float(value or 0.0)

                sales_out = pd.DataFrame(
                    {
                        "_k": [m.to_period("M").strftime("%Y-%m") for m in monthly_denominator.index],
                        "_zp": monthly_denominator.values,
                    }
                )
                merged = claims_out.merge(sales_out, on="_k", how="left")
                merged["_month_sort"] = pd.to_datetime(merged["_k"], errors="coerce")
                merged = merged[merged["_month_sort"].notna()].sort_values("_month_sort").copy()
                if merged.empty:
                    return []
                merged["_cum_claims"] = pd.to_numeric(merged["_net_claims"], errors="coerce").fillna(0.0).cumsum()
                merged["_cum_zp"] = pd.to_numeric(merged["_zp"], errors="coerce").fillna(0.0).cumsum()
                merged["loss_ratio"] = (
                    merged["_cum_claims"] / merged["_cum_zp"].replace(0, pd.NA) * 100
                ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)
                merged["period_start"] = merged["_month_sort"].iloc[0]
                merged["period_end"] = merged["_month_sort"]
                out = merged[[dim, "loss_ratio", "period_start", "period_end"]].copy()
                out["period_start"] = pd.to_datetime(out["period_start"], errors="coerce").dt.strftime("%b-%y")
                out["period_end"] = pd.to_datetime(out["period_end"], errors="coerce").dt.strftime("%b-%y")
            else:
                sales_out = (
                    sales_df
                    .groupby(sales_dim, dropna=False)["_zp"]
                    .sum()
                    .reset_index()
                )
                claims_out["_k"] = _norm_dim(claims_out[dim])
                sales_out["_k"] = _norm_dim(sales_out[sales_dim])

                # Avoid column name collision when claims and sales use the same dim.
                sales_dim_col = sales_dim
                if sales_dim == dim:
                    sales_dim_col = f"{sales_dim}_sales"
                    sales_out = sales_out.rename(columns={sales_dim: sales_dim_col})

                merged = claims_out.merge(sales_out, on="_k", how="left")
                merged["loss_ratio"] = (
                    merged["_net_claims"] / merged["_zp"].replace(0, pd.NA) * 100
                ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)

                dim_col = dim if dim in merged.columns else (sales_dim_col if sales_dim_col in merged.columns else None)
                if dim_col is None:
                    return []
                out = merged[[dim_col, "loss_ratio"]].rename(columns={dim_col: dim})
        else:
            out = (
                df.groupby(dim, dropna=False)["_value"]
                .sum()
                .reset_index()
                .rename(columns={"_value": metric})
            )

        # Align claims Plan Category ordering with sales ordering
        if self.dataset_type == "claims" and dim_key == "plan_category":
            sales_df = data.get("sales", pd.DataFrame())
            sales_dim = _find_dim_column(sales_df, DIMENSION_MAP.get("plan_category", ["Plan Category"]))
            if sales_dim and not sales_df.empty and sales_dim in sales_df.columns:
                raw = sales_df[sales_dim].dropna().astype(str).str.strip()
                order = []
                seen = set()
                for v in raw.tolist():
                    if v and v not in seen:
                        seen.add(v)
                        order.append(v)
                if order:
                    out["_o"] = out[dim].astype(str).map({v: i for i, v in enumerate(order)})
                    out = out.sort_values(by=["_o", dim], na_position="last").drop(columns="_o")

        if dim_key == "month" and "_month_key" in out.columns:
            out["Month"] = pd.to_datetime(out["_month_key"], errors="coerce").dt.strftime("%b-%y")
            out = out.drop(columns=["_month_key"])
            dim = "Month"

        if dim_key == "device_plan_category":
            order = [
                "Mass",
                "Mid",
                "High",
                "Premium",
                "Super Premium",
                "Luxury Flip",
                "Luxury Fold",
            ]
            out[dim] = self._canonicalize_device_plan_category(out[dim])
            value_col = "loss_ratio" if loss_ratio_mode else metric
            out = out[out[dim].notna()].copy()
            if value_col in out.columns:
                existing = set(out[dim].dropna().astype(str).str.strip().tolist())
                missing = [label for label in order if label not in existing]
                if missing:
                    out = pd.concat(
                        [
                            out,
                            pd.DataFrame(
                                {
                                    dim: missing,
                                    value_col: [0.0] * len(missing),
                                }
                            ),
                        ],
                        ignore_index=True,
                    )
            order_index = {v: i for i, v in enumerate(order)}
            out["_o"] = out[dim].astype(str).str.strip().map(order_index).fillna(len(order))
            out = out.sort_values(by=["_o", dim], na_position="last").drop(columns="_o")

        if dim_key == "month":
            out["_s"] = pd.to_datetime(out[dim], format="%b-%y", errors="coerce")
            if not out["_s"].isna().all():
                out = out.dropna(subset=["_s"]).sort_values("_s").drop(columns="_s")
            else:
                out = out.drop(columns="_s")

        if dim_key in {"plan_category", "device_plan_category"} and dim in out.columns:
            dim_as_text = out[dim].astype(str).str.strip().str.lower()
            out = out[
                out[dim].notna()
                & ~dim_as_text.isin({"", "nan", "none", "null"})
            ].copy()

        value_cols = [c for c in out.columns if c != dim]
        if value_cols:
            out[value_cols] = out[value_cols].replace([float("inf"), float("-inf")], 0).fillna(0)
        logger.info(
            "TIMING samsung.compute_by_dimension.dimension_and_aggregation source=%s dataset=%s dimension=%s metric=%s out_rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            dimension,
            metric,
            len(out),
            (time.perf_counter() - dimension_started) * 1000,
        )
        logger.info(
            "TIMING samsung.compute_by_dimension.total source=%s dataset=%s dimension=%s metric=%s out_rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            dimension,
            metric,
            len(out),
            (time.perf_counter() - total_started) * 1000,
        )
        return out.to_dict(orient="records")

    # --------------------------------------------------
    # ✅ SUMMARY (REQUIRED BY ROUTER)
    # --------------------------------------------------
    def compute_summary(self) -> dict:
        total_started = time.perf_counter()
        data = self.load_data(
            include_sales=self.dataset_type != "claims",
            include_claims=self.dataset_type == "claims",
        )
        if self.dataset_type == "claims":
            df = data["claims"]
            if df.empty:
                logger.info(
                    "TIMING samsung.compute_summary.total source=%s dataset=%s rows=0 duration_ms=%.2f",
                    self.source,
                    self.dataset_type,
                    (time.perf_counter() - total_started) * 1000,
                )
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }

            df = self._filter_claims_partner_rows(df)
            claims = float(self._claims_amount_series(df).sum())
            if "OTD Amount" in df.columns:
                otd = pd.to_numeric(df["OTD Amount"], errors="coerce").fillna(0).sum()
            else:
                otd = 0
            net_claims = claims - otd

            return {
                "gross_premium": float(claims),
                "earned_premium": float(net_claims),
                "zopper_earned_premium": float(net_claims),
                "units_sold": int(len(df)),
            }

        df = data["sales"]
        if df.empty:
            logger.info(
                "TIMING samsung.compute_summary.total source=%s dataset=%s rows=0 duration_ms=%.2f",
                self.source,
                self.dataset_type,
                (time.perf_counter() - total_started) * 1000,
            )
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        # Units sold should always reflect total rows (EW included), not date-filtered
        df_qty = df
        # Keep gross premium in transaction/start-date scope (not adjusted EW dates)
        # so category/month splits stay aligned with quantity and ASP denominators.
        df_prem = self._apply_sales_date_filter(df, use_adjusted=False)
        df_earned_scope = self._apply_sales_overlap_filter(df, use_adjusted=True)

        def _sum_col(frame: pd.DataFrame, *candidates: str) -> float:
            col = self._find_column_by_alias(frame, *candidates)
            if col is None:
                return 0.0
            series = frame[col]
            if series is None:
                return 0.0
            return float(self._numeric_series(series).sum())

        gross = _sum_col(
            df_prem,
            "Amount",
            "Gross Premium",
            "gross_premium",
            "Plan Selling Price",
            "Plan Selling Price ",
            "Customer Premium",
        )

        amount_col = self._find_column_by_alias(df_earned_scope, "Amount", "Gross Premium", "Plan Selling Price", "Customer Premium")
        earned_col = self._find_column_by_alias(df_earned_scope, "Earned Premium", "Earned_Premium", "earned_premium")
        zopper_earned_col = self._find_column_by_alias(
            df_earned_scope,
            "Zopper Earned Premium",
            "zopper_earned_premium",
            "earned_zopper",
        )
        zopper_share_col = self._find_column_by_alias(
            df_earned_scope,
            "Zopper Share",
            "Zopper Shared ( Transfer Price )",
            "transfer_price",
        )
        earned_ratio = self._earned_ratio_from_days(df_earned_scope)

        start_col = "_adj_start_date" if "_adj_start_date" in df_earned_scope.columns else ("Start_Date" if "Start_Date" in df_earned_scope.columns else None)
        end_col = "_adj_end_date" if "_adj_end_date" in df_earned_scope.columns else ("End_Date" if "End_Date" in df_earned_scope.columns else None)
        can_accrue_by_overlap = (
            self.apply_date_filter
            and amount_col is not None
            and start_col is not None
            and end_col is not None
        )

        if can_accrue_by_overlap:
            earned = float(
                self._earned_with_dates(
                    df_earned_scope,
                    amount_col,
                    pd.to_datetime(df_earned_scope[start_col], errors="coerce"),
                    pd.to_datetime(df_earned_scope[end_col], errors="coerce"),
                ).sum()
            )
        elif earned_col is not None:
            earned = float(self._numeric_series(df_earned_scope[earned_col]).sum())
        elif amount_col is not None and earned_ratio is not None:
            earned = float((self._numeric_series(df_earned_scope[amount_col]) * earned_ratio).sum())
        elif amount_col is not None and start_col is not None and end_col is not None:
            earned = float(
                self._earned_with_dates(
                    df_earned_scope,
                    amount_col,
                    pd.to_datetime(df_earned_scope[start_col], errors="coerce"),
                    pd.to_datetime(df_earned_scope[end_col], errors="coerce"),
                ).sum()
            )
        elif amount_col is not None:
            earned = float(self._numeric_series(df_earned_scope[amount_col]).sum())
        else:
            earned = 0.0

        can_accrue_zopper_by_overlap = (
            self.apply_date_filter
            and zopper_share_col is not None
            and start_col is not None
            and end_col is not None
        )
        if can_accrue_zopper_by_overlap:
            zopper_earned = float(
                (
                    self._earned_with_dates(
                        df_earned_scope,
                        zopper_share_col,
                        pd.to_datetime(df_earned_scope[start_col], errors="coerce"),
                        pd.to_datetime(df_earned_scope[end_col], errors="coerce"),
                    )
                    * ZOPPER_GST_MULTIPLIER
                ).sum()
            )
        elif zopper_earned_col is not None:
            zopper_earned = float(self._numeric_series(df_earned_scope[zopper_earned_col]).sum())
        elif zopper_share_col is not None and earned_ratio is not None:
            zopper_earned = float((self._numeric_series(df_earned_scope[zopper_share_col]) * earned_ratio * ZOPPER_GST_MULTIPLIER).sum())
        elif zopper_share_col is not None and start_col is not None and end_col is not None:
            zopper_earned = float(
                (
                    self._earned_with_dates(
                        df_earned_scope,
                        zopper_share_col,
                        pd.to_datetime(df_earned_scope[start_col], errors="coerce"),
                        pd.to_datetime(df_earned_scope[end_col], errors="coerce"),
                    )
                    * ZOPPER_GST_MULTIPLIER
                ).sum()
            )
        elif zopper_share_col is not None:
            zopper_earned = float(self._numeric_series(df_earned_scope[zopper_share_col]).sum() * ZOPPER_GST_MULTIPLIER)
        else:
            zopper_earned = 0.0

        result = {
            "gross_premium": float(gross),
            "earned_premium": float(earned),
            "zopper_earned_premium": float(zopper_earned),
            "units_sold": int(len(df_qty)),
        }
        logger.info(
            "TIMING samsung.compute_summary.total source=%s dataset=%s rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            len(df_qty),
            (time.perf_counter() - total_started) * 1000,
        )
        return result

    def compute(self) -> dict:
        return {}
