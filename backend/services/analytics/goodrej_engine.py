import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from models.data_rows import DataRow
from services.analytics.base_engine import BaseAnalyticsEngine

VALUATION_DATE = pd.Timestamp("2025-12-31")

REVENUE_SPLIT = {
    'D2D':     {'channel':0.25,'godrej':0.35,'zopper':0.40},
    'POS':     {'channel':0.25,'godrej':0.35,'zopper':0.40},
    'Calling Process': {'channel':0.30,'godrej':0.35,'zopper':0.35},
    'POD':     {'channel':0.20,'godrej':0.35,'zopper':0.45},
    'Amazon':  {'channel':0.40,'godrej':0.35,'zopper':0.25},
}

class GodrejAnalyticsEngine(BaseAnalyticsEngine):

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
        self.apply_date_filter = bool(from_date or to_date)
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else None
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else None

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------

    def load_data(self) -> dict[str, pd.DataFrame]:
        sales = self._load_rows("sales")
        claims = self._load_rows("claims")
        return {"sales": sales, "claims": claims}

    def _load_rows(self, dataset_type):
        q = self.db.query(DataRow).filter(DataRow.dataset_type == dataset_type)
        if self.source:
            q = q.filter(
                (DataRow.source.ilike("godrej%")) |
                (DataRow.source.ilike("goodrej%")) |
                (DataRow.source.ilike("goddrej%"))
            )
        # Goodrej dashboards should aggregate across all uploads, even when a job_id
        # is passed from the UI. This ensures totals reflect the full database.
        rows = q.all()

        df = pd.DataFrame([r.data for r in rows])
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
        if dataset_type == "sales":
            df = self.compute_premiums(df)
        else:
            df = self._normalize_claims(df)
        return df

    # --------------------------------------------------
    # PREMIUM CALCULATION
    # --------------------------------------------------

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

        df["Warranty Start Date"] = pd.to_datetime(df["Warranty Start Date"], errors="coerce")
        if "Warranty End Date" in df.columns:
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
        df["Used_Days"] = (VALUATION_DATE - df["Warranty Start Date"]).dt.days
        df["Used_Days"] = df[["Used_Days", "Coverage_Days"]].min(axis=1)
        df["Used_Days"] = df["Used_Days"].clip(lower=0)

        # Earned / Unearned
        df["Earned_Premium"] = df["Customer Premium"] * (df["Used_Days"] / df["Coverage_Days"])
        df["Unearned_Premium"] = df["Customer Premium"] - df["Earned_Premium"]

        missing_start = df["Warranty Start Date"].isna()
        if missing_start.any():
            df.loc[missing_start, "Earned_Premium"] = 0
            df.loc[missing_start, "Unearned_Premium"] = df.loc[missing_start, "Customer Premium"]

        # Revenue Split
        def split(row):
            split = REVENUE_SPLIT.get(row["Channel"])
            if not split:
                return 0,0,0,0

            ep = row["Earned_Premium"]
            up = row["Unearned_Premium"]

            return (
                ep * split["zopper"],
                up * split["zopper"],
                ep * split["godrej"],
                ep * split["channel"]
            )

        df[[ 
            "Zopper_Share_EP",
            "Zopper_Unearned",
            "Godrej_Share_EP",
            "Channel_Share_EP"
        ]] = df.apply(split, axis=1, result_type="expand")

        return df

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------

    def _normalize_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "Claim_Amount" in df.columns:
            df["Claim_Amount"] = pd.to_numeric(
                df["Claim_Amount"], errors="coerce"
            ).fillna(0)
        return df

    def _parse_month_series(self, series: pd.Series) -> pd.Series:
        raw = series.astype(str).str.strip()
        cleaned = raw.str.replace(r"\.0$", "", regex=True)

        yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
        parsed = pd.to_datetime(
            cleaned.where(~yyyymm_mask, cleaned.str.slice(0, 4) + "-" + cleaned.str.slice(4, 6) + "-01"),
            errors="coerce",
        )

        if parsed.isna().any():
            parsed_try = pd.to_datetime(cleaned, errors="coerce")
            parsed = parsed.fillna(parsed_try)

        if parsed.isna().all():
            for fmt in ["%b-%y", "%b-%Y", "%m-%Y", "%Y-%m", "%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y"]:
                parsed_try = pd.to_datetime(cleaned, format=fmt, errors="coerce")
                if parsed_try.notna().any():
                    parsed = parsed_try
                    break

        return parsed

    def _resolve_dimension(
        self,
        df: pd.DataFrame,
        dimension: str,
        dataset_type: str,
    ) -> tuple[pd.DataFrame, str | None]:
        dim_key = dimension.lower().strip()

        dim_map = {
            "channel": [
                "Channel",
                "Channel Name",
                "Channel_Name",
                "State",
                "State Name",
                "State/City",
                "State / City",
                "Region",
            ],
            "product_category": [
                "Product_Category",
                "Product Category",
                "Product_Category_Name",
                "Product Category Name",
                "Category",
            ],
            "month": [
                "Month",
                "Warranty Start Date",
                "Warranty Start_Date",
                "Warranty Start",
                "Start Date",
                "Start_Date",
                "Claim Date",
                "Claim_Date",
                "Day of Call_Date",
                "Call_Date",
                "Date",
                "Date of Claim",
            ],
            "state": [
                "State",
                "State Name",
                "State/City",
                "State / City",
                "Region",
                "Channel",
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
            if dim_col is None:
                return df, None
            df = df.copy()
            month_series = self._parse_month_series(df[dim_col])
            if month_series.isna().all():
                return df, None
            df["_month_key"] = month_series.dt.to_period("M").dt.to_timestamp()
            return df, "_month_key"

        return df, dim_col

    def _apply_date_filter(
        self,
        df: pd.DataFrame,
        dataset_type: str,
    ) -> pd.DataFrame:
        if df.empty or not self.apply_date_filter:
            return df

        date_candidates = [
            "Warranty Start Date",
            "Warranty Start_Date",
            "Start Date",
            "Start_Date",
            "Month",
            "Claim Date",
            "Claim_Date",
            "Day of Call_Date",
            "Call_Date",
            "Date",
            "Date of Claim",
        ]

        date_col = next((c for c in date_candidates if c in df.columns), None)
        if date_col is None:
            return df

        series = self._parse_month_series(df[date_col])
        if series.isna().all():
            return df

        mask = pd.Series(True, index=df.index)
        if self.report_start is not None and self.report_start is not pd.NaT:
            mask &= series >= self.report_start
        if self.report_end is not None and self.report_end is not pd.NaT:
            mask &= series <= self.report_end
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

        claims_df["_claims"] = pd.to_numeric(
            claims_df.get("Claim_Amount", 0), errors="coerce"
        ).fillna(0)

        sales_df["_zp"] = pd.to_numeric(
            sales_df.get("Zopper_Share_EP", 0), errors="coerce"
        ).fillna(0)

        claims_out = (
            claims_df
            .groupby(claims_dim, dropna=False)["_claims"]
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
                .str.replace("_", " ", regex=False)
                .str.replace(r"\s+", " ", regex=True)
            )

        claims_out["_k"] = _norm_dim(claims_out[claims_dim])
        sales_out["_k"] = _norm_dim(sales_out[sales_dim])

        merged = claims_out.merge(sales_out, on="_k", how="left").fillna(0)
        merged["loss_ratio"] = (
            merged["_claims"] / merged["_zp"] * 100
        ).replace([float("inf"), float("-inf")], 0).fillna(0)

        dim_col = claims_dim if claims_dim in merged.columns else sales_dim
        out = merged[[dim_col, "loss_ratio"]].rename(columns={dim_col: dimension})

        if dimension == "month" and "month" in out.columns:
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")

        return out.to_dict(orient="records")

    # --------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------

    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        data = self.load_data()
        df = data["claims"] if self.dataset_type == "claims" else data["sales"]

        if df.empty:
            return []

        df = self._apply_date_filter(df, self.dataset_type)

        if metric == "loss_ratio":
            return self._compute_loss_ratio_by_dimension(dimension, data)

        df = df.copy()

        if self.dataset_type == "claims":
            if metric == "claims":
                df["_value"] = pd.to_numeric(df.get("Claim_Amount", 0), errors="coerce").fillna(0)
            elif metric == "net_claims":
                df["_value"] = pd.to_numeric(df.get("Claim_Amount", 0), errors="coerce").fillna(0)
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

        out = (
            df.groupby(dim_col, dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={dim_col: dimension, "_value": metric})
        )

        if dimension == "month" and "month" in out.columns:
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")

        return out.fillna(0).to_dict(orient="records")

    # --------------------------------------------------
    # SUMMARY
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
            df = self._apply_date_filter(df, "claims")
            claims = pd.to_numeric(df.get("Claim_Amount", 0), errors="coerce").fillna(0).sum()
            return {
                "gross_premium": float(claims),
                "earned_premium": float(claims),
                "zopper_earned_premium": float(claims),
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

        df = self._apply_date_filter(df, "sales")

        gross = pd.to_numeric(df.get("Customer Premium", 0), errors="coerce").fillna(0).sum()
        earned = pd.to_numeric(df.get("Earned_Premium", 0), errors="coerce").fillna(0).sum()
        zopper_earned = pd.to_numeric(df.get("Zopper_Share_EP", 0), errors="coerce").fillna(0).sum()

        return {
            "gross_premium": float(gross),
            "earned_premium": float(earned),
            "zopper_earned_premium": float(zopper_earned),
            "units_sold": int(len(df)),
        }

    def compute(self) -> dict:
        return {}

