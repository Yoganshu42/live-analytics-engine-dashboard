# services/analytics/samsung_engine.py

import pandas as pd
from sqlalchemy.orm import Session

from models.data_rows import DataRow
from services.analytics.base_engine import BaseAnalyticsEngine

REPORT_START = pd.Timestamp("2000-01-01")
REPORT_END = pd.Timestamp("2100-12-31")
ZOPPER_GST_MULTIPLIER = 1.18


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
        self.apply_date_filter = bool(from_date or to_date)
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else REPORT_START
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else REPORT_END
        if pd.isna(self.report_start):
            self.report_start = REPORT_START
        if pd.isna(self.report_end):
            self.report_end = REPORT_END

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
                month_dt = pd.to_datetime(
                    {
                        "year": REPORT_START.year,
                        "month": month_num.clip(1, 12),
                        "day": 1,
                    },
                    errors="coerce",
                )

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

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------
    def load_data(self) -> dict[str, pd.DataFrame]:
        sales_query = (
            self.db.query(DataRow)
            .filter(DataRow.dataset_type == "sales")
        )
        claims_query = (
            self.db.query(DataRow)
            .filter(DataRow.dataset_type == "claims")
        )

        if self.source == "samsung":
            sales_query = sales_query.filter(DataRow.source.ilike("samsung%"))
            claims_query = claims_query.filter(DataRow.source.ilike("samsung%"))
        else:
            if self.dataset_type == "claims" and self.source and self.source.startswith("samsung"):
                # For claims analytics, loss_ratio needs sales; allow samsung-wide sales fallback
                sales_query = sales_query.filter(DataRow.source.ilike("samsung%"))
            else:
                sales_query = sales_query.filter(DataRow.source == self.source)
            if self.source and self.source.startswith("samsung"):
                # claims are stored with partner names inside, so pull all samsung claims
                claims_query = claims_query.filter(DataRow.source.ilike("samsung%"))
            else:
                claims_query = claims_query.filter(DataRow.source == self.source)

        def _fetch_with_optional_job(query):
            if not self.job_id:
                return query.all()
            with_job = query.filter(DataRow.job_id == self.job_id).all()
            if with_job:
                return with_job
            return query.all()

        sales_rows = _fetch_with_optional_job(sales_query)
        claims_rows = _fetch_with_optional_job(claims_query)

        sales_df = pd.DataFrame([r.data for r in sales_rows])
        claims_df = pd.DataFrame([r.data for r in claims_rows])

        if sales_df.empty and claims_df.empty:
            return {"sales": sales_df, "claims": claims_df}

        # normalize column names (trim)
        if not sales_df.empty:
            sales_df.columns = [str(c).strip() for c in sales_df.columns]

        # normalize common date column variants
        col_renames = {}
        for src, dest in [
            ("Start Date", "Start_Date"),
            ("Start_Date", "Start_Date"),
            ("Plan Start Date", "Start_Date"),
            ("End Date", "End_Date"),
            ("End_Date", "End_Date"),
            ("Plan End Date", "End_Date"),
            ("Month", "Month"),
            ("Month ", "Month"),
            ("Month Name", "Month"),
            ("Month_Name", "Month"),
            ("Fiscal Month", "Fiscal Month"),
            ("State / City", "State"),
            ("State/City", "State"),
        ]:
            if src in sales_df.columns and dest not in sales_df.columns:
                col_renames[src] = dest
        if col_renames:
            sales_df = sales_df.rename(columns=col_renames)

        if not sales_df.empty:
            sales_df["Start_Date"] = pd.to_datetime(sales_df["Start_Date"], errors="coerce")
            sales_df["End_Date"] = pd.to_datetime(sales_df["End_Date"], errors="coerce")
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

        # Normalize claims columns
        if not claims_df.empty:
            claims_df.columns = [str(c).strip() for c in claims_df.columns]
            rename_map = {
                "Partner Name": "Partner Name",
                "Partner_Name": "Partner Name",
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
                "Month Name": "Month",
                "Month_Name": "Month",
                "Fiscal Month": "Fiscal Month",
                "State / City": "State",
                "State/City": "State",
                "Pack type": "Device Plan Category",
            }
            col_renames = {}
            for src, dest in rename_map.items():
                if src in claims_df.columns and dest not in claims_df.columns:
                    col_renames[src] = dest
            if col_renames:
                claims_df = claims_df.rename(columns=col_renames)

            # Samsung claims: treat Plan Category as Device Plan Category when missing
            if "Device Plan Category" not in claims_df.columns and "Plan Category" in claims_df.columns:
                claims_df["Device Plan Category"] = claims_df["Plan Category"]

            if "Partner Name" in claims_df.columns:
                claims_df["Partner Name"] = (
                    claims_df["Partner Name"]
                    .astype(str)
                    .str.replace(" Bulk", "", regex=False)
                    .str.strip()
                )

            # Apply date filter to claims (prefer Day of Call_Date, Call_Date, then Month/Fiscal Month)
            date_series = None
            for col in ["Day of Call_Date", "Call_Date", "Month", "Fiscal Month"]:
                if col in claims_df.columns:
                    if col == "Fiscal Month":
                        fm = claims_df[col].astype(str).str.strip()
                        series = pd.to_datetime(
                            fm.where(~fm.str.fullmatch(r"\d{6}"), fm.str.slice(0, 4) + "-" + fm.str.slice(4, 6) + "-01"),
                            errors="coerce",
                        )
                    else:
                        series = pd.to_datetime(claims_df[col], errors="coerce")
                    if not series.isna().all():
                        date_series = series
                        break

            if self.apply_date_filter and date_series is not None:
                mask = pd.Series(True, index=claims_df.index)
                if self.report_start is not None:
                    mask &= date_series >= self.report_start
                if self.report_end is not None:
                    mask &= date_series <= self.report_end
                claims_df = claims_df[mask]

        return {"sales": sales_df, "claims": claims_df}

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
        eff_start = start_dates.clip(lower=self.report_start)
        eff_end = end_dates.clip(upper=self.report_end)

        exposure = (eff_end - eff_start).dt.days + 1
        coverage = (end_dates - start_dates).dt.days + 1

        ratio = (exposure / coverage).clip(lower=0, upper=1)
        earned = (df[col] * ratio).fillna(0)

        invalid = (coverage <= 0) | coverage.isna()
        earned = earned.where(~invalid, 0)

        # Safety cap to written premium
        earned = earned.clip(upper=df[col])
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
            try:
                return pd.to_datetime(series, format="mixed", errors="coerce")
            except TypeError:
                return pd.to_datetime(series, errors="coerce")

        if "Month" in df.columns:
            series = _parse(df["Month"])
            if not series.isna().all():
                return series
        if "Date" in df.columns:
            series = _parse(df["Date"])
            if not series.isna().all():
                return series
        if use_adjusted and "_adj_start_date" in df.columns:
            series = _parse(df["_adj_start_date"])
            if not series.isna().all():
                return series
        for col in ["Start_Date", "End_Date"]:
            if col in df.columns:
                series = _parse(df[col])
                if not series.isna().all():
                    return series
        return None

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

    # --------------------------------------------------
    # MAIN AGGREGATION
    # --------------------------------------------------
    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        data = self.load_data()
        if self.dataset_type == "claims":
            df = data["claims"]
        else:
            df = data["sales"]

        if df.empty:
            return []

        policy_col = self._find_policy_column(df)

        # ---------------- METRIC ----------------
        loss_ratio_mode = False
        if self.dataset_type == "claims":
            # Partner split based on source for samsung overview
            if self.source and "Partner Name" in df.columns:
                src = self.source.lower()
                if "vijay" in src:
                    df = df[df["Partner Name"].astype(str) == "Vijay Sales"]
                elif "croma" in src:
                    df = df[df["Partner Name"].astype(str) == "Croma"]

            if metric == "claims":
                if "Net Amount" not in df.columns:
                    return []
                df["_value"] = pd.to_numeric(df["Net Amount"], errors="coerce").fillna(0)
            elif metric == "net_claims":
                if "Net Amount" not in df.columns:
                    return []
                net_amt = pd.to_numeric(df["Net Amount"], errors="coerce").fillna(0)
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
            premium_metric = metric in {"gross_premium", "earned_premium", "zopper_earned_premium"}
            df = self._apply_sales_date_filter(df, use_adjusted=premium_metric)
            if metric == "gross_premium":
                df["_value"] = df["Amount"]

            elif metric == "earned_premium":
                if "_adj_start_date" in df.columns and "_adj_end_date" in df.columns:
                    df["_value"] = self._earned_with_dates(
                        df, "Amount", df["_adj_start_date"], df["_adj_end_date"]
                    )
                else:
                    df["_value"] = self._earned(df, "Amount")

            elif metric == "zopper_earned_premium":
                if "_adj_start_date" in df.columns and "_adj_end_date" in df.columns:
                    df["_value"] = self._earned_with_dates(
                        df, "Zopper Share", df["_adj_start_date"], df["_adj_end_date"]
                    ) * ZOPPER_GST_MULTIPLIER
                else:
                    df["_value"] = self._earned(df, "Zopper Share") * ZOPPER_GST_MULTIPLIER

            elif metric == "quantity":
                df["_value"] = 1

            else:
                return []

        # ---------------- DIMENSION ----------------
        DIMENSION_MAP = {
            "month": ["Month", "Date", "month", "Fiscal Month", "Day of Call_Date", "Call_Date"],
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

        def _find_dim_column(frame: pd.DataFrame, cand: list[str]) -> str | None:
            for c in cand:
                if c in frame.columns:
                    return c
            # try normalized match
            target = _norm(cand[0])
            return next((c for c in frame.columns if _norm(c) == target), None)

        dim = None
        for c in candidates:
            if c in df.columns:
                dim = c
                break

        if dim is None:
            # try normalized match
            matched = _find_dim_column(df, candidates)
            if matched is None:
                # special: derive Month from Start_Date if missing
                if dim_key == "month" and "Start_Date" in df.columns:
                    df = df.copy()
                    df["Month"] = pd.to_datetime(df["Start_Date"], errors="coerce")
                    dim = "Month"
                else:
                    return []
            else:
                dim = matched

        # For loss ratio, align plan_category to device plan category if present
        if loss_ratio_mode and dim_key == "plan_category" and "Device Plan Category" in df.columns:
            dim = "Device Plan Category"

        # 🔥 FIX: DEDUPE FOR CATEGORY DIMENSIONS
        if policy_col and dim in ("Plan_Category", "Device_Plan_Category"):
            df = df.dropna(subset=[dim])
            df = df.drop_duplicates(subset=[policy_col, dim])

        if dim_key == "month":
            if self.dataset_type == "claims":
                start_series = None
                month_source = df[dim]
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

            sales_dim = _find_dim_column(sales_df, candidates)
            if sales_dim is None:
                return []

            # For loss ratio, align plan_category to device plan category if present
            if dim_key == "plan_category" and "Device Plan Category" in sales_df.columns:
                sales_dim = "Device Plan Category"

            if "Net Amount" not in df.columns:
                return []

            claims_df = df.copy()
            net_amt = pd.to_numeric(claims_df["Net Amount"], errors="coerce").fillna(0)
            if "OTD Amount" in claims_df.columns:
                otd = pd.to_numeric(claims_df["OTD Amount"], errors="coerce").fillna(0)
            else:
                otd = 0
            claims_df["_net_claims"] = net_amt - otd

            if "Zopper Share" not in sales_df.columns or "Start_Date" not in sales_df.columns or "End_Date" not in sales_df.columns:
                return []

            sales_df = sales_df.copy()
            sales_df = self._apply_sales_date_filter(sales_df, use_adjusted=True)
            if dim_key == "month":
                start_series = None
                if "Date" in sales_df.columns:
                    date_series = pd.to_datetime(sales_df["Date"], errors="coerce")
                    if not date_series.isna().all():
                        start_series = date_series
                if start_series is None and "Start_Date" in sales_df.columns:
                    start_series = sales_df["Start_Date"]
                if start_series is not None and not start_series.isna().all():
                    month_source = start_series
                else:
                    month_source = sales_df[sales_dim]
                month_dt = self._parse_month_series(month_source, start_series)
                if month_dt is not None:
                    sales_df["_month_key"] = month_dt.dt.to_period("M").dt.to_timestamp()
                    sales_dim = "_month_key"

            if "_adj_start_date" in sales_df.columns and "_adj_end_date" in sales_df.columns:
                sales_df["_zp"] = self._earned_with_dates(
                    sales_df, "Zopper Share", sales_df["_adj_start_date"], sales_df["_adj_end_date"]
                ) * ZOPPER_GST_MULTIPLIER
            else:
                sales_df["_zp"] = self._earned(sales_df, "Zopper Share") * ZOPPER_GST_MULTIPLIER

            claims_out = (
                claims_df
                .groupby(dim, dropna=False)["_net_claims"]
                .sum()
                .reset_index()
            )
            sales_out = (
                sales_df
                .groupby(sales_dim, dropna=False)["_zp"]
                .sum()
                .reset_index()
            )

            def _norm_dim(series: pd.Series) -> pd.Series:
                return (
                    series
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r"^\d+\s*-\s*", "", regex=True)
                    .str.replace("_", " ", regex=False)
                    .str.replace(r"\s+", " ", regex=True)
                )

            claims_out["_k"] = _norm_dim(claims_out[dim])
            sales_out["_k"] = _norm_dim(sales_out[sales_dim])

            # Avoid column name collision when claims and sales use the same dim (e.g., _month_key)
            sales_dim_col = sales_dim
            if sales_dim == dim:
                sales_dim_col = f"{sales_dim}_sales"
                sales_out = sales_out.rename(columns={sales_dim: sales_dim_col})

            merged = claims_out.merge(sales_out, on="_k", how="left")
            merged["loss_ratio"] = (
                merged["_net_claims"] / merged["_zp"] * 100
            ).replace([float("inf"), float("-inf")], 0).fillna(0)

            # Prefer claims dimension label; fall back to sales label if needed
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

        if dim_key in ("device_plan_category", "plan_category"):
            order = [
                "mass",
                "mid",
                "high",
                "premium",
                "super premium",
                "luxury flip",
                "luxury fold",
            ]
            order_index = {v: i for i, v in enumerate(order)}
            normalized = (
                out[dim]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace("flip luxury", "luxury flip", regex=False)
                .str.replace("fold luxury", "luxury fold", regex=False)
            )
            has_device_plan_values = normalized.isin(order).any()
            if dim_key == "device_plan_category" or has_device_plan_values:
                out["_o"] = normalized.map(order_index)
                out = out.sort_values(
                    by=["_o", dim],
                    na_position="last",
                ).drop(columns="_o")

        if dim_key == "month":
            out["_s"] = pd.to_datetime(out[dim], format="%b-%y", errors="coerce")
            if not out["_s"].isna().all():
                out = out.dropna(subset=["_s"]).sort_values("_s").drop(columns="_s")
            else:
                out = out.drop(columns="_s")

        out = out.fillna(0)
        out = out.replace([float("inf"), float("-inf")], 0)
        return out.to_dict(orient="records")

    # --------------------------------------------------
    # ✅ SUMMARY (REQUIRED BY ROUTER)
    # --------------------------------------------------
    def compute_summary(self) -> dict:
        data = self.load_data()
        if self.dataset_type == "claims":
            df = data["claims"]
            if df.empty:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": 0,
                }

            # Partner split for samsung overview
            if self.source and "Partner Name" in df.columns:
                src = self.source.lower()
                if "vijay" in src:
                    df = df[df["Partner Name"].astype(str) == "Vijay Sales"]
                elif "croma" in src:
                    df = df[df["Partner Name"].astype(str) == "Croma"]

            if "Net Amount" in df.columns:
                claims = pd.to_numeric(df["Net Amount"], errors="coerce").fillna(0).sum()
            else:
                claims = 0
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
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        # Units sold should always reflect total rows (EW included), not date-filtered
        df_qty = df
        df_prem = self._apply_sales_date_filter(df, use_adjusted=True)

        return {
            "gross_premium": float(df_prem["Amount"].sum()),
            "earned_premium": float(
                (
                    self._earned_with_dates(
                        df_prem,
                        "Amount",
                        df_prem["_adj_start_date"] if "_adj_start_date" in df_prem.columns else df_prem["Start_Date"],
                        df_prem["_adj_end_date"] if "_adj_end_date" in df_prem.columns else df_prem["End_Date"],
                    )
                ).sum()
            ),
            "zopper_earned_premium": float(
                (
                    self._earned_with_dates(
                        df_prem,
                        "Zopper Share",
                        df_prem["_adj_start_date"] if "_adj_start_date" in df_prem.columns else df_prem["Start_Date"],
                        df_prem["_adj_end_date"] if "_adj_end_date" in df_prem.columns else df_prem["End_Date"],
                    )
                    * ZOPPER_GST_MULTIPLIER
                ).sum()
            ),
            "units_sold": int(len(df_qty)),
        }

    def compute(self) -> dict:
        return {}
