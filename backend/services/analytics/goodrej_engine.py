import pandas as pd
import numpy as np
import time
import logging
from sqlalchemy.orm import Session
from models.data_rows import DataRow
from services.analytics.base_engine import BaseAnalyticsEngine

LOSS_RATIO_CAP_PERCENT = 300.0
logger = logging.getLogger(__name__)

REVENUE_SPLIT = {
    "D2D": {"Channel": 0.25, "Godrej": 0.35, "Zopper": 0.40},
    "POS": {"Channel": 0.25, "Godrej": 0.35, "Zopper": 0.40},
    "Calling Process": {"Channel": 0.30, "Godrej": 0.35, "Zopper": 0.35},
    "POD": {"Channel": 0.20, "Godrej": 0.35, "Zopper": 0.45},
    "Amazon": {"Channel": 0.40, "Godrej": 0.35, "Zopper": 0.25},
}

class GodrejAnalyticsEngine(BaseAnalyticsEngine):
    CLAIM_CHANNEL_MAP = {
        "d2d": "D2D",
        "pod": "POD",
        "pos": "POS",
        "calling process": "Calling Process",
        "callingprocess": "Calling Process",
        "calling_process": "Calling Process",
        "amazon": "Amazon",
    }
    STATE_ALIAS_MAP = {
        "delhi": "Delhi",
        "new delhi": "Delhi",
        "ghaziabad": "Uttar Pradesh",
        "lucknow": "Uttar Pradesh",
        "faridabad": "Haryana",
        "mumbai": "Maharashtra",
        "pune": "Maharashtra",
        "pune goa": "Maharashtra",
        "kolkata": "West Bengal",
        "bangalore": "Karnataka",
        "bengaluru": "Karnataka",
        "bhubaneshwar": "Odisha",
        "bhubaneswar": "Odisha",
        "chennai": "Tamil Nadu",
        "coimbatore": "Tamil Nadu",
        "hyderabad": "Telangana",
        "patna": "Bihar",
        "kochi": "Kerala",
        "guwahati": "Assam",
        "vijayawada": "Andhra Pradesh",
        "bhopal": "Madhya Pradesh",
        "ahmedabad": "Gujarat",
        "ranchi": "Jharkhand",
    }

    def __init__(
        self,
        db: Session,
        job_id: str | None,
        source: str | None = "godrej",
        dataset_type: str | None = "sales",
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        super().__init__(db=db, job_id=job_id, source=source)
        self.dataset_type = dataset_type or "sales"
        self._loaded_data_cache: dict[tuple[bool, bool], dict[str, pd.DataFrame]] = {}
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else None
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else None
        if self.report_start is not None and pd.isna(self.report_start):
            self.report_start = None
        if self.report_end is not None and pd.isna(self.report_end):
            self.report_end = None
        if self.report_start is not None and self.report_end is not None and self.report_end < self.report_start:
            self.report_end = self.report_start
        self.apply_date_filter = bool(self.report_start is not None or self.report_end is not None)
        self.valuation_date = self._resolve_valuation_date(self.report_end)

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------

    def load_data(self, include_sales: bool = True, include_claims: bool = True) -> dict[str, pd.DataFrame]:
        cache_key = (include_sales, include_claims)
        if cache_key in self._loaded_data_cache:
            return self._loaded_data_cache[cache_key]
        started = time.perf_counter()
        sales = self._load_rows("sales") if include_sales else pd.DataFrame()
        claims = self._load_rows("claims") if include_claims else pd.DataFrame()
        result = {"sales": sales, "claims": claims}
        self._loaded_data_cache[cache_key] = result
        logger.info(
            "TIMING godrej.load_data source=%s dataset=%s include_sales=%s include_claims=%s sales_rows=%s claims_rows=%s duration_ms=%.2f",
            self.source,
            self.dataset_type,
            include_sales,
            include_claims,
            len(sales),
            len(claims),
            (time.perf_counter() - started) * 1000,
        )
        return result

    def _load_rows(self, dataset_type):
        q = self.db.query(DataRow.data).filter(DataRow.dataset_type == dataset_type)
        if self.source:
            q = q.filter(
                (DataRow.source.ilike("godrej%")) |
                (DataRow.source.ilike("goodrej%")) |
                (DataRow.source.ilike("goddrej%"))
            )
        base_query = q
        tag = (self.job_id or "").strip()
        aggregate_claims_across_tags = dataset_type == "claims" and self.dataset_type == "claims"
        if tag and not aggregate_claims_across_tags:
            q = q.filter(DataRow.job_id == tag)
        rows = q.all()
        if (
            tag
            and not rows
            and dataset_type == "sales"
            and self.dataset_type == "claims"
        ):
            # Claims loss-ratio can be requested for tags that only have claims uploads.
            # Fall back to available sales rows so ratio graphs still render.
            rows = base_query.all()
        payloads = [r[0] if isinstance(r, tuple) else r.data for r in rows]
        df = pd.DataFrame(payloads)
        if df.empty:
            return df

        df.columns = df.columns.str.strip()
        if dataset_type == "sales" and not df.empty:
            # Normalize common column name variants to expected names
            col_map = {}
            for col in df.columns:
                key = str(col).strip().lower()
                if key in {"customer premium", "customer_premium", "premium"}:
                    col_map[col] = "Customer Premium"
                elif key in {"warranty activation code", "activation code", "activation_code"}:
                    col_map[col] = "Warranty Activation Code"
                elif key in {"warranty start date", "warranty start_date", "start date", "start_date"}:
                    col_map[col] = "Warranty Start Date"
                elif key in {"warranty end date", "warranty end_date", "end date", "end_date"}:
                    col_map[col] = "Warranty End Date"
                elif key in {"channel", "channel name", "channel_name"}:
                    col_map[col] = "Channel"
            if col_map:
                df = df.rename(columns=col_map)
        if dataset_type == "claims" and not df.empty:
            col_map = {}
            for col in df.columns:
                key = str(col).strip().lower()
                if key in {"claim amount", "claim_amount", "net claim amount", "net_claim_amount"}:
                    col_map[col] = "Claim_Amount"
                elif key in {"customer premium", "customer_premium", "premium"}:
                    col_map[col] = "Customer Premium"
                elif key in {"prodcut category", "product category", "product_category", "category"}:
                    col_map[col] = "Product_Category"
                elif key in {"channel", "channel name", "channel_name"}:
                    col_map[col] = "Channel"
                elif key in {"month", "month name", "month_name"}:
                    col_map[col] = "Month"
            if col_map:
                df = df.rename(columns=col_map)
        df = self._dedupe_columns(df)
        if dataset_type == "sales":
            df = self.compute_premiums(df)
        else:
            df = self._normalize_claims(df)
        return df

    # --------------------------------------------------
    # PREMIUM CALCULATION
    # --------------------------------------------------

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

    def compute_premiums(self, df: pd.DataFrame) -> pd.DataFrame:

        required = {
            "Warranty Activation Code",
            "Warranty Start Date",
            "Customer Premium",
            "Channel",
        }
        if not required.issubset(df.columns):
            return df

        df = df.copy()

        try:
            df["Warranty Start Date"] = pd.to_datetime(df["Warranty Start Date"], format="mixed", errors="coerce")
        except TypeError:
            df["Warranty Start Date"] = pd.to_datetime(df["Warranty Start Date"], errors="coerce")
        if "Warranty End Date" in df.columns:
            try:
                df["Warranty End Date"] = pd.to_datetime(df.get("Warranty End Date"), format="mixed", errors="coerce")
            except TypeError:
                df["Warranty End Date"] = pd.to_datetime(df.get("Warranty End Date"), errors="coerce")
        else:
            df["Warranty End Date"] = pd.NaT
        df["Customer Premium"]    = pd.to_numeric(df["Customer Premium"], errors="coerce").fillna(0)
        if "Zopper Plan Duration" in df.columns:
            df["Zopper Plan Duration"] = pd.to_numeric(df["Zopper Plan Duration"], errors="coerce")
        else:
            df["Zopper Plan Duration"] = np.nan

        # Coverage Days
        df["Coverage_Days"] = np.where(
            df["Warranty End Date"].notna(),
            (df["Warranty End Date"] - df["Warranty Start Date"]).dt.days,
            df["Zopper Plan Duration"] * 30
        )

        df["Coverage_Days"] = df["Coverage_Days"].clip(lower=1)

        # Used Days
        df["Used_Days"] = (self.valuation_date - df["Warranty Start Date"]).dt.days
        df["Used_Days"] = df[["Used_Days", "Coverage_Days"]].min(axis=1)
        df["Used_Days"] = df["Used_Days"].clip(lower=0)

        # Earned / Unearned
        df["Earned_Premium"] = df["Customer Premium"] * (df["Used_Days"] / df["Coverage_Days"])
        df["Unearned_Premium"] = df["Customer Premium"] - df["Earned_Premium"]

        missing_start = df["Warranty Start Date"].isna()
        if missing_start.any():
            df.loc[missing_start, "Earned_Premium"] = 0
            df.loc[missing_start, "Unearned_Premium"] = df.loc[missing_start, "Customer Premium"]

        df["Channel"] = self._canonical_sales_channel(df["Channel"])
        split_df = pd.DataFrame(df["Channel"].map(REVENUE_SPLIT).tolist(), index=df.index).fillna(0)
        zopper_share = split_df.get("Zopper", split_df.get("zopper", 0))
        godrej_share = split_df.get("Godrej", split_df.get("godrej", 0))
        channel_share = split_df.get("Channel", split_df.get("channel", 0))

        df["Zopper_Share_EP"] = df["Earned_Premium"] * zopper_share
        df["Zopper_Unearned"] = df["Unearned_Premium"] * zopper_share
        df["Godrej_Share_EP"] = df["Earned_Premium"] * godrej_share
        df["Channel_Share_EP"] = df["Earned_Premium"] * channel_share

        return df

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------

    @staticmethod
    def _as_series(value, index: pd.Index | None = None) -> pd.Series:
        if isinstance(value, pd.DataFrame):
            if value.shape[1] == 0:
                return pd.Series(dtype=float, index=index)
            out = value.iloc[:, 0]
            if value.shape[1] > 1:
                for i in range(1, value.shape[1]):
                    out = out.where(out.notna(), value.iloc[:, i])
            return out
        if isinstance(value, pd.Series):
            return value
        if index is None:
            return pd.Series(value)
        return pd.Series(value, index=index)

    @staticmethod
    def _normalize_col_key(value: str) -> str:
        return (
            str(value)
            .lower()
            .replace("_", "")
            .replace(" ", "")
            .replace("/", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "")
            .strip()
        )

    @classmethod
    def _canonical_claim_channel(cls, series: pd.Series) -> pd.Series:
        normalized = (
            series
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        return normalized.map(cls.CLAIM_CHANNEL_MAP).fillna("Unknown")

    @classmethod
    def _canonical_sales_channel(cls, series: pd.Series) -> pd.Series:
        normalized = (
            series
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        mapped = normalized.map(cls.CLAIM_CHANNEL_MAP)
        original = series.astype(str).str.strip()
        return mapped.where(mapped.notna(), original)

    @classmethod
    def _canonical_state(cls, series: pd.Series) -> pd.Series:
        cleaned = (
            series
            .astype(str)
            .str.strip()
            .str.replace("_", " ", regex=False)
            .str.replace("/", " ", regex=False)
            .str.replace("-", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        normalized = cleaned.str.lower()
        mapped = normalized.map(cls.STATE_ALIAS_MAP)
        base_title = cleaned.str.title()
        return mapped.where(mapped.notna(), base_title)

    @staticmethod
    def _is_identifier_like(value: str) -> bool:
        s = str(value or "").strip()
        if not s:
            return True
        low = s.lower()
        if low in {"unknown", "nan", "none", "null", "0"}:
            return True
        compact = s.replace(" ", "")
        if len(compact) < 8 or " " in s:
            return False
        alnum = "".join(ch for ch in compact if ch.isalnum())
        if len(alnum) < 8:
            return False
        has_alpha = any(ch.isalpha() for ch in alnum)
        has_digit = any(ch.isdigit() for ch in alnum)
        if not (has_alpha and has_digit):
            return False
        return (len(alnum) / max(len(compact), 1)) >= 0.85

    def _drop_noise_buckets(self, out: pd.DataFrame, dimension: str) -> pd.DataFrame:
        if out.empty or dimension not in out.columns:
            return out
        labels = out[dimension].astype(str).str.strip()
        normalized = (
            labels
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        bad = normalized.isin({"", "0", "unknown", "nan", "none", "null"})
        if dimension == "product_category":
            bad = bad | labels.map(self._is_identifier_like)
            bad = bad | normalized.str.match(
                r"^(q[1-4]\b|(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\b)",
                na=False,
            )
        if dimension in {"state", "region"}:
            channel_labels = {value.lower() for value in self.CLAIM_CHANNEL_MAP.values()}
            compact = labels.str.replace(" ", "", regex=False)
            bad = bad | labels.map(self._is_identifier_like)
            bad = bad | (compact.str.len().ge(6) & compact.str.isdigit())
            bad = bad | normalized.isin(channel_labels)
        return out.loc[~bad].copy()

    def _pick_first_series(self, df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> pd.Series:
        normalized = [self._normalize_col_key(c) for c in df.columns]
        for candidate in candidates:
            target = self._normalize_col_key(candidate)
            idxs = [i for i, key in enumerate(normalized) if key == target]
            if not idxs:
                continue
            selected = df.iloc[:, idxs]
            return self._as_series(selected, index=df.index)
        return pd.Series(default, index=df.index)

    def _as_numeric_series(self, df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> pd.Series:
        series = self._pick_first_series(df, candidates, default=default)
        return pd.to_numeric(series, errors="coerce").fillna(0)

    def _claim_amount_series(self, df: pd.DataFrame) -> pd.Series:
        if "Claim_Amount" in df.columns:
            return pd.to_numeric(df["Claim_Amount"], errors="coerce").fillna(0)
        return self._as_numeric_series(
            df,
            [
                "Claim_Amount",
                "Claim Amount",
                "Net Claim Amount",
                "Net_Claim_Amount",
                "Amount",
                "Invoice Amount",
                "Payment Amount",
            ],
        )

    def _coalesce_numeric_series(self, df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> pd.Series:
        normalized = [self._normalize_col_key(c) for c in df.columns]
        out = pd.Series(np.nan, index=df.index, dtype=float)

        for candidate in candidates:
            target = self._normalize_col_key(candidate)
            idxs = [i for i, key in enumerate(normalized) if key == target]
            if not idxs:
                continue
            selected = df.iloc[:, idxs]
            series = self._as_series(selected, index=df.index)
            parsed = pd.to_numeric(series, errors="coerce")
            out = out.where(out.notna(), parsed)

        return out.fillna(default)

    def _coalesce_text_series(self, df: pd.DataFrame, candidates: list[str], default: str = "Unknown") -> pd.Series:
        normalized = [self._normalize_col_key(c) for c in df.columns]
        out = pd.Series(pd.NA, index=df.index, dtype="object")

        for candidate in candidates:
            target = self._normalize_col_key(candidate)
            idxs = [i for i, key in enumerate(normalized) if key == target]
            if not idxs:
                continue
            selected = df.iloc[:, idxs]
            series = self._as_series(selected, index=df.index)
            text = series.astype(str).str.strip()
            text = text.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "None": pd.NA})
            out = out.where(out.notna(), text)

        return out.fillna(default)

    def _coalesce_datetime_series(self, df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        for candidate in candidates:
            if candidate not in df.columns:
                continue
            parsed = self._parse_month_series(df[candidate])
            if parsed is None:
                continue
            out = out.where(out.notna(), parsed)
        return out

    def _dedupe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not df.columns.duplicated().any():
            return df
        merged = pd.DataFrame(index=df.index)
        seen: set[str] = set()
        for col in df.columns:
            if col in seen:
                continue
            seen.add(col)
            same = df.loc[:, df.columns == col]
            merged[col] = self._as_series(same, index=df.index)
        return merged

    def _normalize_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self._dedupe_columns(df)

        # Claims uploads can contain multiple amount-like columns across files.
        # Coalesce row-by-row across known variants.
        claim_amount = self._coalesce_numeric_series(
            df,
            [
                "Claim_Amount",
                "Claim Amount",
                "Net Claim Amount",
                "Net_Claim_Amount",
                "Amount",
                "Invoice Amount",
                "Payment Amount",
            ],
        )
        df["Claim_Amount"] = claim_amount

        product_category = self._coalesce_text_series(
            df,
            [
                "Product_Category",
                "Product Category",
                "Prodcut Category",
                "Category",
                "Item Description",
                "Appliance Model Name",
                "Item Name",
                "item",
                "Type",
            ],
            default="Unknown",
        )
        product_category = product_category.astype(str).str.strip()
        product_category = product_category.replace({"": "Unknown", "nan": "Unknown", "none": "Unknown", "None": "Unknown"})
        code_like_mask = product_category.map(self._is_identifier_like)
        if code_like_mask.any():
            fallback_category = self._coalesce_text_series(
                df,
                [
                    "Item Description",
                    "Appliance Model Name",
                    "Type",
                    "Item Name",
                ],
                default="Unknown",
            ).astype(str).str.strip()
            fallback_category = fallback_category.replace({"": "Unknown", "nan": "Unknown", "none": "Unknown", "None": "Unknown"})
            product_category = product_category.where(~code_like_mask, fallback_category)
            code_like_mask = product_category.map(self._is_identifier_like)
            product_category = product_category.where(~code_like_mask, "Unknown")
        df["Product_Category"] = product_category

        raw_channel = self._coalesce_text_series(
            df,
            [
                "Channel",
                "Channel Name",
                "Channel_Name",
            ],
            default="",
        ).astype(str).str.strip()
        df["Channel"] = self._canonical_claim_channel(raw_channel)

        state = self._coalesce_text_series(
            df,
            [
                "State",
                "Customer_State",
                "Customer State",
                "Branch",
                "Branch Name",
                "Store Name",
                "Customer_City",
                "Customer City",
                "City",
                "Location",
            ],
            default="",
        )
        state = state.astype(str).str.strip()
        state = state.replace({"": "Unknown", "nan": "Unknown", "none": "Unknown", "None": "Unknown"})
        state_norm = (
            state
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        raw_channel_norm = (
            raw_channel
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        known_channels = set(self.CLAIM_CHANNEL_MAP.keys())
        fallback_state = state_norm.isin({"unknown", "nan", "none", "null", "0"})
        fallback_state &= ~raw_channel_norm.isin(known_channels | {"", "nan", "none", "null"})
        state = state.where(~fallback_state, raw_channel)
        state_norm = (
            state
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("_", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
        )
        state = state.where(~state_norm.isin(known_channels), "Unknown")
        state = state.astype(str).str.strip()
        state = state.replace({"": "Unknown", "nan": "Unknown", "none": "Unknown", "None": "Unknown"})
        state_no_digits = ~state.str.contains(r"\d", regex=True)
        state = state.where(~state_no_digits, state.str.title())
        df["State"] = state

        df["Month"] = self._coalesce_text_series(
            df,
            [
                "Month",
                "Payment_date",
                "Payment Date",
                "Claim Date",
                "Claim_Date",
                "Date",
                "Date of Claim",
                "Warranty Purchase Date",
                "Warranty Start Date",
            ],
            default="",
        )
        return df

    def _parse_month_series(self, series: pd.Series) -> pd.Series:
        raw = series.astype(str).str.strip()
        cleaned = raw.str.replace(r"\.0$", "", regex=True)

        yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
        yyyymm_normalized = cleaned.where(
            ~yyyymm_mask, cleaned.str.slice(0, 4) + "-" + cleaned.str.slice(4, 6) + "-01"
        )
        try:
            parsed = pd.to_datetime(yyyymm_normalized, format="mixed", errors="coerce")
        except TypeError:
            parsed = pd.to_datetime(yyyymm_normalized, errors="coerce")

        if parsed.isna().any():
            try:
                parsed_try = pd.to_datetime(cleaned, format="mixed", errors="coerce")
            except TypeError:
                parsed_try = pd.to_datetime(cleaned, errors="coerce")
            parsed = parsed.fillna(parsed_try)

        if parsed.isna().all():
            for fmt in ["%b-%y", "%b-%Y", "%m-%Y", "%Y-%m", "%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y"]:
                parsed_try = pd.to_datetime(cleaned, format=fmt, errors="coerce")
                if parsed_try.notna().any():
                    parsed = parsed_try
                    break

        # Do not infer year from ambiguous month-only labels (e.g. "Jun", "6").
        ambiguous_month_only = cleaned.str.fullmatch(r"[A-Za-z]{3,9}") | cleaned.str.fullmatch(r"\d{1,2}")
        parsed = parsed.where(~ambiguous_month_only, pd.NaT)

        if parsed.notna().any():
            bad_year = parsed.dt.year < 2000
            if bad_year.any():
                if self.report_start is not None and self.report_start is not pd.NaT:
                    base_year = int(self.report_start.year)
                    parsed = parsed.where(
                        ~bad_year,
                        pd.to_datetime(
                            {
                                "year": base_year,
                                "month": parsed.dt.month.clip(1, 12),
                                "day": 1,
                            },
                            errors="coerce",
                        ),
                    )
                else:
                    parsed = parsed.where(~bad_year, pd.NaT)

        return parsed

    def _resolve_dimension(
        self,
        df: pd.DataFrame,
        dimension: str,
        dataset_type: str,
    ) -> tuple[pd.DataFrame, str | None]:
        dim_key = dimension.lower().strip()

        month_candidates_sales = [
            "Warranty Purchase Date",
            "Product Purchased Date",
            "Warranty Start Date",
            "Warranty Start_Date",
            "Start Date",
            "Start_Date",
            "Plan Start Date",
            "Date",
            "Month",
            "Month Name",
            "Month_Name",
            "Invoice_Date_",
            "Invoice Date",
            "Bill Created Date",
            "Payment_date",
            "Payment Date",
        ]
        month_candidates_claims = [
            "Date",
            "Claim Date",
            "Claim_Date",
            "Payment_date",
            "Payment Date",
            "Day of Call_Date",
            "Call_Date",
            "Call Date",
            "Date of Claim",
            "Invoice_Date_",
            "Invoice Date",
            "Bill Created Date",
            "Warranty Purchase Date",
            "Warranty Start Date",
            "Warranty Start_Date",
            "Start Date",
            "Start_Date",
            "Month",
            "Month Name",
            "Month_Name",
        ]

        dim_map = {
            "channel": [
                "Channel",
                "Channel Name",
                "Channel_Name",
            ],
            "product_category": [
                "Product_Category",
                "Product Category",
                "Prodcut Category",
                "Product_Category_Name",
                "Product Category Name",
                "Category",
            ],
            "month": [
                *(month_candidates_claims if dataset_type == "claims" else month_candidates_sales),
            ],
            "state": [
                "State",
                "Customer_State",
                "Customer State",
                "Location",
                "Branch",
                "Branch Name",
                "Customer_City",
                "Customer City",
                "City",
                "State Name",
                "State/City",
                "State / City",
                "Region",
            ],
            "region": [
                "Region",
                "State",
                "Customer_State",
                "Customer State",
                "Location",
                "Branch",
                "Branch Name",
                "Customer_City",
                "Customer City",
                "City",
                "State Name",
                "State/City",
                "State / City",
            ],
            "plan_category": [
                "Plan Category",
                "Plan_Category",
                "Product_Category",
                "Product Category",
                "Category",
            ],
            "device_plan_category": [
                "Device Plan Category",
                "Device_Plan_Category",
                "Product_Category",
                "Product Category",
                "Category",
            ],
        }

        candidates = dim_map.get(dim_key, [dimension])

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

        normalized = {_normalize_key(c): c for c in df.columns}
        dim_col = None
        for candidate in candidates:
            key = _normalize_key(candidate)
            if key in normalized:
                dim_col = normalized[key]
                break

        if dim_key == "month":
            df = df.copy()
            tried_columns: set[str] = set()
            best_month: pd.Series | None = None
            best_valid = -1
            best_years = -1
            for candidate in candidates:
                key = _normalize_key(candidate)
                matched_col = normalized.get(key)
                if matched_col is None or matched_col in tried_columns:
                    continue
                tried_columns.add(matched_col)
                month_series = self._parse_month_series(df[matched_col])
                valid = int(month_series.notna().sum())
                if valid <= 0:
                    continue
                years = int(month_series.dropna().dt.year.nunique())
                if valid > best_valid or (valid == best_valid and years > best_years):
                    best_month = month_series
                    best_valid = valid
                    best_years = years
            if best_month is not None:
                df["_month_key"] = best_month.dt.to_period("M").dt.to_timestamp()
                return df, "_month_key"
            return df, None

        return df, dim_col

    def _apply_date_filter(
        self,
        df: pd.DataFrame,
        dataset_type: str,
    ) -> pd.DataFrame:
        if df.empty or not self.apply_date_filter:
            return df

        if dataset_type == "claims":
            date_candidates = [
                "Month",
                "Month Name",
                "Month_Name",
                "Payment_date",
                "Payment Date",
                "Claim Date",
                "Claim_Date",
                "Day of Call_Date",
                "Call_Date",
                "Call Date",
                "Date",
                "Date of Claim",
                "Invoice_Date_",
                "Invoice Date",
                "Bill Created Date",
                "Warranty Purchase Date",
                "Warranty Start Date",
                "Warranty Start_Date",
                "Start Date",
                "Start_Date",
            ]
        else:
            date_candidates = [
                "Warranty Purchase Date",
                "Product Purchased Date",
                "Warranty Start Date",
                "Warranty Start_Date",
                "Start Date",
                "Start_Date",
                "Plan Start Date",
                "Invoice_Date_",
                "Invoice Date",
                "Bill Created Date",
                "Month",
                "Month Name",
                "Month_Name",
                "Payment_date",
                "Payment Date",
                "Date",
            ]

        series = self._coalesce_datetime_series(df, date_candidates)
        if series.isna().all():
            # If a range is explicitly requested but no usable dates exist,
            # keep behavior strict and return no rows.
            return df.iloc[0:0]

        filter_start = self.report_start
        filter_end = self.report_end
        if dataset_type == "claims":
            non_na = series.dropna()
            if not non_na.empty and float(non_na.dt.is_month_start.mean()) >= 0.9:
                if filter_start is not None and filter_start is not pd.NaT:
                    filter_start = pd.Timestamp(filter_start).to_period("M").to_timestamp()
                if filter_end is not None and filter_end is not pd.NaT:
                    filter_end = pd.Timestamp(filter_end).to_period("M").to_timestamp(how="end")

        mask = series.notna()
        if filter_start is not None and filter_start is not pd.NaT:
            mask &= series >= filter_start
        if filter_end is not None and filter_end is not pd.NaT:
            mask &= series <= filter_end
        return df[mask]

    # --------------------------------------------------
    # LOSS RATIO
    # --------------------------------------------------

    def compute_loss_ratio(self, sales_df, claims_df):
        claims_df = self._normalize_claims(claims_df)

        claims = (
            claims_df
            .groupby(["Channel","Product_Category"], as_index=False)["Claim_Amount"]
            .sum()
        )

        premium = (
            sales_df
            .groupby(["Channel","Product_Category"], as_index=False)
            .agg(Zopper_Earned=("Zopper_Share_EP","sum"))
        )

        out = premium.merge(claims, how="left", on=["Channel","Product_Category"]).fillna(0)

        out["Loss_Ratio"] = out["Claim_Amount"] / out["Zopper_Earned"]
        out.loc[out["Zopper_Earned"] == 0, "Loss_Ratio"] = np.nan

        return out

    def _compute_loss_ratio_by_dimension(self, dimension: str, data: dict) -> list[dict]:
        sales_df = data.get("sales", pd.DataFrame())
        claims_df = data.get("claims", pd.DataFrame())
        if sales_df.empty or claims_df.empty:
            return []

        sales_df = self._apply_date_filter(sales_df, "sales")
        claims_df = self._apply_date_filter(claims_df, "claims")

        if dimension == "month":
            def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
                normalized = {self._normalize_col_key(c): c for c in df.columns}
                for candidate in candidates:
                    key = self._normalize_col_key(candidate)
                    if key in normalized:
                        return normalized[key]
                return None

            def _pick_month_key(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
                month_like = {
                    self._normalize_col_key("Month"),
                    self._normalize_col_key("Month Name"),
                    self._normalize_col_key("Month_Name"),
                }
                best_key: pd.Series | None = None
                best_valid = -1
                best_years = -1
                for candidate in candidates:
                    col = _pick_column(df, [candidate])
                    if col is None:
                        continue
                    norm_col = self._normalize_col_key(col)
                    if norm_col in month_like:
                        parsed = self._parse_month_series(df[col])
                    else:
                        parsed = pd.to_datetime(df[col], errors="coerce")
                    month_key = parsed.dt.to_period("M").dt.to_timestamp()
                    valid = int(month_key.notna().sum())
                    if valid <= 0:
                        continue
                    years = int(month_key.dropna().dt.year.nunique())
                    if valid > best_valid or (valid == best_valid and years > best_years):
                        best_key = month_key
                        best_valid = valid
                        best_years = years
                return best_key

            claims_df = claims_df.copy()
            claims_df["_claims"] = self._claim_amount_series(claims_df)
            claims_df = claims_df[claims_df["_claims"] > 0]
            if claims_df.empty:
                return []

            claim_month = _pick_month_key(
                claims_df,
                [
                    "Date",
                    "Claim Date",
                    "Claim_Date",
                    "Payment_date",
                    "Payment Date",
                    "Day of Call_Date",
                    "Call_Date",
                    "Call Date",
                    "Date of Claim",
                    "Month",
                    "Month Name",
                    "Month_Name",
                ],
            )
            if claim_month is None:
                return []
            claims_df["_month"] = claim_month
            claims_df = claims_df[claims_df["_month"].notna()].copy()
            if claims_df.empty:
                return []

            claims_out = (
                claims_df
                .groupby("_month", dropna=False)["_claims"]
                .sum()
                .reset_index()
                .rename(columns={"_month": "month"})
            )
            month_index = pd.DatetimeIndex(sorted(pd.to_datetime(claims_out["month"], errors="coerce").dropna().unique()))
            if month_index.empty:
                return []

            sales_df = sales_df.copy()
            start_col = _pick_column(
                sales_df,
                ["Warranty Start Date", "Warranty Purchase Date", "Product Purchased Date", "Start Date", "Start_Date"],
            )
            if start_col is None:
                return []

            start_dt = pd.to_datetime(sales_df[start_col], errors="coerce")
            end_col = _pick_column(sales_df, ["Warranty End Date", "Warranty End_Date", "End Date", "End_Date"])
            end_dt = pd.to_datetime(sales_df[end_col], errors="coerce") if end_col is not None else pd.Series(pd.NaT, index=sales_df.index)
            coverage_days = pd.to_numeric(sales_df.get("Coverage_Days", 0), errors="coerce").fillna(0)
            computed_end = start_dt + pd.to_timedelta((coverage_days - 1).clip(lower=0), unit="D")
            end_dt = end_dt.where(end_dt.notna(), computed_end)
            policy_days = (end_dt - start_dt).dt.days + 1

            channel_series = (
                self._canonical_sales_channel(sales_df.get("Channel", pd.Series("", index=sales_df.index)))
                if "Channel" in sales_df.columns
                else pd.Series("", index=sales_df.index)
            )
            zopper_share = channel_series.map(lambda ch: float((REVENUE_SPLIT.get(ch) or {}).get("Zopper", 0.0)))
            customer_premium = pd.to_numeric(sales_df.get("Customer Premium", 0), errors="coerce").fillna(0.0)
            zopper_total = customer_premium * zopper_share.fillna(0.0)

            valid = start_dt.notna() & end_dt.notna() & policy_days.gt(0) & zopper_total.ne(0)
            sales_monthly = pd.Series(0.0, index=month_index, dtype="float64")
            for month_start in month_index:
                month_start = pd.Timestamp(month_start)
                month_end = (month_start + pd.offsets.MonthEnd(1)).normalize()
                overlap_start = start_dt.clip(lower=month_start)
                overlap_end = end_dt.clip(upper=month_end)
                overlap_days = (overlap_end - overlap_start).dt.days + 1
                overlap_days = overlap_days.clip(lower=0)
                accrued = (
                    zopper_total
                    * (overlap_days / policy_days.where(policy_days.gt(0), pd.NA))
                ).fillna(0.0)
                sales_monthly.loc[month_start] = float(accrued[valid].sum())

            invalid = (~valid) & zopper_total.ne(0)
            if invalid.any():
                fallback_month = _pick_month_key(
                    sales_df.loc[invalid],
                    ["Warranty Purchase Date", "Payment_date", "Date", "Month", "Warranty Start Date"],
                )
                if fallback_month is not None:
                    fallback_df = pd.DataFrame(
                        {
                            "month": pd.to_datetime(fallback_month, errors="coerce"),
                            "_zp": zopper_total.loc[invalid],
                        }
                    )
                    fallback_df = fallback_df[fallback_df["month"].notna()]
                    if not fallback_df.empty:
                        fallback_g = fallback_df.groupby("month", dropna=False)["_zp"].sum()
                        for month_key, value in fallback_g.items():
                            if month_key in sales_monthly.index:
                                sales_monthly.loc[month_key] += float(value or 0.0)

            sales_out = (
                sales_monthly
                .rename("_zp")
                .reset_index()
                .rename(columns={"index": "month"})
            )

            merged = claims_out.merge(sales_out, on="month", how="left")
            merged["_claims"] = pd.to_numeric(merged["_claims"], errors="coerce").fillna(0.0)
            merged["_zp"] = pd.to_numeric(merged["_zp"], errors="coerce").fillna(0.0)
            merged["month"] = pd.to_datetime(merged["month"], errors="coerce")
            merged = merged[merged["month"].notna()].sort_values("month").copy()
            if merged.empty:
                return []
            merged["_cum_claims"] = merged["_claims"].cumsum()
            merged["_cum_zp"] = merged["_zp"].cumsum()
            merged["loss_ratio"] = (
                merged["_cum_claims"] / merged["_cum_zp"].replace(0, pd.NA) * 100
            ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)
            merged["period_start"] = merged["month"].iloc[0]
            merged["period_end"] = merged["month"]

            out = merged[["month", "loss_ratio", "period_start", "period_end"]].copy()
            out = out.sort_values("month")
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")
            out["period_start"] = pd.to_datetime(out["period_start"], errors="coerce").dt.strftime("%b-%y")
            out["period_end"] = pd.to_datetime(out["period_end"], errors="coerce").dt.strftime("%b-%y")
            out = out[out["month"].notna()]
            return out.to_dict(orient="records")

        sales_df, sales_dim = self._resolve_dimension(sales_df, dimension, "sales")
        claims_df, claims_dim = self._resolve_dimension(claims_df, dimension, "claims")
        if sales_dim is None or claims_dim is None:
            return []

        sales_df = sales_df.copy()
        claims_df = claims_df.copy()

        if dimension in {"channel", "product_category"}:
            def _clean_dim(series: pd.Series) -> pd.Series:
                s = series.astype(str).str.strip()
                s = s.replace({"": None, "0": None, "nan": None, "none": None, "None": None})
                return s.fillna("Unknown")
            sales_df[sales_dim] = _clean_dim(sales_df[sales_dim])
            claims_df[claims_dim] = _clean_dim(claims_df[claims_dim])
        elif dimension in {"state", "region"}:
            sales_df[sales_dim] = self._canonical_state(sales_df[sales_dim])
            claims_df[claims_dim] = self._canonical_state(claims_df[claims_dim])

        claims_df["_claims"] = self._claim_amount_series(claims_df)
        claims_df = claims_df[claims_df["_claims"] > 0]
        if claims_df.empty:
            return []

        sales_df["_zp"] = pd.to_numeric(
            sales_df.get("Zopper_Share_EP", 0), errors="coerce"
        ).fillna(0)

        claims_out = (
            claims_df
            .groupby(claims_dim, dropna=False)["_claims"]
            .sum()
            .reset_index()
            .rename(columns={claims_dim: "_dim_claims"})
        )
        sales_out = (
            sales_df
            .groupby(sales_dim, dropna=False)["_zp"]
            .sum()
            .reset_index()
            .rename(columns={sales_dim: "_dim_sales"})
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

        claims_out["_k"] = _norm_dim(claims_out["_dim_claims"])
        sales_out["_k"] = _norm_dim(sales_out["_dim_sales"])

        merged = claims_out.merge(sales_out, on="_k", how="left")
        merged["_claims"] = pd.to_numeric(merged["_claims"], errors="coerce").fillna(0.0)
        merged["_zp"] = pd.to_numeric(merged["_zp"], errors="coerce").fillna(0.0)
        merged["loss_ratio"] = (
            merged["_claims"] / merged["_zp"].replace(0, pd.NA) * 100
        ).replace([float("inf"), float("-inf")], 0).fillna(0).clip(lower=0, upper=LOSS_RATIO_CAP_PERCENT)

        out = merged[["_dim_claims", "loss_ratio"]].rename(columns={"_dim_claims": dimension})
        out = self._drop_noise_buckets(out, dimension)
        return out.to_dict(orient="records")

    # --------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------

    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        data = self.load_data(
            include_sales=(self.dataset_type != "claims") or metric == "loss_ratio",
            include_claims=(self.dataset_type == "claims") or metric == "loss_ratio",
        )
        df = data["claims"] if self.dataset_type == "claims" else data["sales"]

        if df.empty:
            return []

        df = self._apply_date_filter(df, self.dataset_type)

        if metric == "loss_ratio":
            return self._compute_loss_ratio_by_dimension(dimension, data)

        df = df.copy()

        if self.dataset_type == "claims":
            claim_amount = self._claim_amount_series(df)
            positive_claims = claim_amount > 0
            if not positive_claims.any():
                return []
            df = df[positive_claims].copy()
            claim_amount = claim_amount[positive_claims]

            if metric == "claims":
                df["_value"] = claim_amount
            elif metric == "net_claims":
                df["_value"] = claim_amount
            elif metric == "quantity":
                df["_value"] = 1
            else:
                return []
        else:
            if metric == "gross_premium":
                df["_value"] = pd.to_numeric(df.get("Customer Premium", 0), errors="coerce").fillna(0)
            elif metric == "earned_premium":
                df["_value"] = pd.to_numeric(df.get("Earned_Premium", 0), errors="coerce").fillna(0)
            elif metric == "zopper_earned_premium":
                df["_value"] = pd.to_numeric(df.get("Zopper_Share_EP", 0), errors="coerce").fillna(0)
            elif metric == "quantity":
                df["_value"] = 1
            else:
                return []

        df, dim_col = self._resolve_dimension(df, dimension, self.dataset_type)
        if dim_col is None:
            return []

        if dimension in {"channel", "product_category"}:
            def _clean_dim(series: pd.Series) -> pd.Series:
                s = series.astype(str).str.strip()
                s = s.replace({"": None, "0": None, "nan": None, "none": None, "None": None})
                return s.fillna("Unknown")
            df[dim_col] = _clean_dim(df[dim_col])
        elif dimension in {"state", "region"}:
            df[dim_col] = self._canonical_state(df[dim_col])

        out = (
            df.groupby(dim_col, dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={dim_col: dimension, "_value": metric})
        )

        if dimension == "month" and "month" in out.columns:
            month_series = pd.to_datetime(out["month"], errors="coerce")
            out = out[month_series.notna()].copy()
            out["_month_sort"] = month_series[month_series.notna()]
            out["month"] = out["_month_sort"].dt.strftime("%b-%y")
            out = out.sort_values("_month_sort").drop(columns=["_month_sort"])
        else:
            out = self._drop_noise_buckets(out, dimension)

        if metric in out.columns:
            out[metric] = pd.to_numeric(out[metric], errors="coerce").fillna(0)

        return out.to_dict(orient="records")

    # --------------------------------------------------
    # SUMMARY
    # --------------------------------------------------

    def compute_summary(self) -> dict:
        data = self.load_data(
            include_sales=self.dataset_type != "claims",
            include_claims=self.dataset_type == "claims",
        )

        if self.dataset_type == "claims":
            df = data["claims"]
            if df.empty:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }
            df = self._apply_date_filter(df, "claims")
            if df.empty:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }
            claim_amount = self._claim_amount_series(df)
            positive_claims = claim_amount > 0
            if not positive_claims.any():
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }
            claims = claim_amount[positive_claims].sum()
            return {
                "gross_premium": float(claims),
                "earned_premium": float(claims),
                "zopper_earned_premium": float(claims),
                "units_sold": int(positive_claims.sum()),
            }

        df = data["sales"]
        if df.empty:
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        # Gross premium and units sold should reflect total uploaded sales rows,
        # not the currently selected date slice.
        df_all = df
        df_period = self._apply_date_filter(df, "sales")

        gross = pd.to_numeric(df_all.get("Customer Premium", 0), errors="coerce").fillna(0).sum()
        earned = pd.to_numeric(df_period.get("Earned_Premium", 0), errors="coerce").fillna(0).sum()
        zopper_earned = pd.to_numeric(df_period.get("Zopper_Share_EP", 0), errors="coerce").fillna(0).sum()

        return {
            "gross_premium": float(gross),
            "earned_premium": float(earned),
            "zopper_earned_premium": float(zopper_earned),
            "units_sold": int(len(df_all)),
        }

    def compute(self) -> dict:
        return {}

