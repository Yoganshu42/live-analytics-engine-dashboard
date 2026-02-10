# services/analytics/reliance_engine.py

import logging
import pandas as pd
from sqlalchemy.orm import Session

from models.data_rows import DataRow
from services.analytics.base_engine import BaseAnalyticsEngine

logger = logging.getLogger(__name__)

GST_MULTIPLIER = 1.18
VALUATION_DATE = pd.Timestamp("2025-12-31")
RELIANCE_START = pd.Timestamp("2025-07-01")
RELIANCE_END = pd.Timestamp("2025-12-31")


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
        self.report_start = pd.to_datetime(from_date, errors="coerce") if from_date else RELIANCE_START
        self.report_end = pd.to_datetime(to_date, errors="coerce") if to_date else RELIANCE_END
        if pd.isna(self.report_start):
            self.report_start = RELIANCE_START
        if pd.isna(self.report_end):
            self.report_end = RELIANCE_END
        if self.report_start < RELIANCE_START:
            self.report_start = RELIANCE_START
        if self.report_end > RELIANCE_END:
            self.report_end = RELIANCE_END
        if self.report_end < self.report_start:
            self.report_end = self.report_start

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

        # yyyymm
        yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
        parsed = pd.to_datetime(
            cleaned.where(~yyyymm_mask, cleaned.str.slice(0, 4) + "-" + cleaned.str.slice(4, 6) + "-01"),
            errors="coerce",
        )

        # try full datetime / date formats
        if parsed.isna().any():
            parsed_try = pd.to_datetime(cleaned, errors="coerce")
            parsed = parsed.fillna(parsed_try)

        if parsed.isna().all():
            for fmt in ["%b-%y", "%b-%Y", "%m-%Y", "%Y-%m", "%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y"]:
                parsed_try = pd.to_datetime(cleaned, format=fmt, errors="coerce")
                if parsed_try.notna().any():
                    parsed = parsed_try
                    break

        # month name only (e.g., "Jul", "Jan (till 10)")
        if parsed.isna().any():
            tokens = (
                cleaned.str.lower()
                .str.replace(r"[^a-z]", " ", regex=True)
                .str.strip()
            )
            first = tokens.str.split().str[0].str.slice(0, 3)
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
            month_num = first.map(month_map)
            year = self.report_start.year
            parsed_month = pd.to_datetime(
                {"year": year, "month": month_num, "day": 1},
                errors="coerce",
            )
            parsed = parsed.fillna(parsed_month)

        # Fix bogus years (e.g., 0001) by mapping to report year
        if parsed.notna().any():
            bad_year = parsed.dt.year < 2000
            if bad_year.any():
                parsed = parsed.where(
                    ~bad_year,
                    pd.to_datetime(
                        {
                            "year": self.report_start.year,
                            "month": parsed.dt.month.clip(1, 12),
                            "day": 1,
                        },
                        errors="coerce",
                    ),
                )

        return parsed

    def _month_key(self, series: pd.Series) -> pd.Series:
        dt = pd.to_datetime(series, errors="coerce")
        return dt.dt.to_period("M").dt.to_timestamp()

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------

    def load_data(self) -> dict[str, pd.DataFrame]:
        sales_q = self.db.query(DataRow).filter(DataRow.dataset_type == "sales")
        claims_q = self.db.query(DataRow).filter(DataRow.dataset_type == "claims")

        if self.source:
            sales_q = sales_q.filter(DataRow.source.ilike("reliance%"))
            claims_q = claims_q.filter(DataRow.source.ilike("reliance%"))

        if self.job_id:
            sales_q = sales_q.filter(DataRow.job_id == self.job_id)
            claims_q = claims_q.filter(DataRow.job_id == self.job_id)

        sales_df = pd.DataFrame([r.data for r in sales_q.all()])
        claims_df = pd.DataFrame([r.data for r in claims_q.all()])

        if not sales_df.empty:
            sales_df.columns = [str(c).strip() for c in sales_df.columns]

        if not claims_df.empty:
            claims_df.columns = [str(c).strip() for c in claims_df.columns]

        sales_ew_df = pd.DataFrame()

        # -----------------------------
        # SALES CLEANING (NOTEBOOK)
        # -----------------------------
        if not sales_df.empty:
            sales_df["Brand"] = sales_df.get("Brand").replace(
                {
                    "Idea": "Lenovo",
                    "Pad": "Redmi",
                    "GooglePixel": "Google",
                }
            )

            if "Plan Selling Price " in sales_df.columns and "Plan Selling Price" not in sales_df.columns:
                sales_df = sales_df.rename(
                    columns={"Plan Selling Price ": "Plan Selling Price"}
                )
            # Do not use Zopper Shared Transfer Price / Zopper Share for Gross Premium.
            # Gross Premium must strictly use Plan Selling Price.

            sales_df["Plan Start Date"] = pd.to_datetime(
                sales_df["Plan Start Date"], errors="coerce"
            )
            sales_df["Plan End Date"] = pd.to_datetime(
                sales_df["Plan End Date"], errors="coerce"
            )

            # NOTEBOOK: only year 2025 + July-Dec window
            if "Month" in sales_df.columns:
                sales_df["Month"] = self._parse_month_series(sales_df["Month"])
                sales_df = sales_df[sales_df["Month"].dt.year == 2025]
                sales_df = sales_df[
                    (sales_df["Month"] >= self.report_start)
                    & (sales_df["Month"] <= self.report_end)
                ]
            else:
                sales_df = sales_df[sales_df["Plan Start Date"].dt.year == 2025]
                sales_df = sales_df[
                    (sales_df["Plan Start Date"] >= self.report_start)
                    & (sales_df["Plan Start Date"] <= self.report_end)
                ]

            # Exclude EW entirely (as requested)
            sales_df["_ew"] = self._is_ew_plan(sales_df)
            sales_ew_df = sales_df[sales_df["_ew"] == True].copy()
            sales_df = sales_df[sales_df["_ew"] != True]

        # -----------------------------
        # CLAIMS CLEANING (NOTEBOOK)
        # -----------------------------
        if not claims_df.empty:
            claims_df["Day of Call_Date"] = pd.to_datetime(
                claims_df["Day of Call_Date"], errors="coerce"
            )

            if "Month" in claims_df.columns:
                claims_df["Month"] = self._parse_month_series(claims_df["Month"])
                claims_df = claims_df[claims_df["Month"].dt.year == 2025]
                claims_df = claims_df[
                    (claims_df["Month"] >= self.report_start)
                    & (claims_df["Month"] <= self.report_end)
                ]
            else:
                claims_df = claims_df[
                    claims_df["Day of Call_Date"].dt.year == 2025
                ]
                claims_df = claims_df[
                    (claims_df["Day of Call_Date"] >= self.report_start)
                    & (claims_df["Day of Call_Date"] <= self.report_end)
                ]

            claims_df["Warranty Type"] = claims_df["Warranty Type"].replace(
                {"Screen Protection": "Cracked Screen"}
            )

            claims_df["Product Brand(Group)"] = claims_df[
                "Product Brand(Group)"
            ].replace({"OPPO": "Oppo"})

            claims_df["One time deductible"] = (
                self._clean_number(claims_df.get("One time deductible"))
                .fillna(999)
            )

            claims_df["Zopper's Cost"] = self._clean_number(
                claims_df.get("Zopper's Cost")
            )

            if "Customer Paid" in claims_df.columns:
                claims_df["Customer Paid"] = self._clean_number(
                    claims_df["Customer Paid"]
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

            coverage_days = (
                sales_df["Plan End Date"]
                - sales_df["Plan Start Date"]
            ).dt.days.clip(lower=1)

            exposure_days = (
                VALUATION_DATE - sales_df["Plan Start Date"]
            ).dt.days

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

        return {"sales": sales_df, "claims": claims_df, "sales_ew": sales_ew_df}

    # --------------------------------------------------
    # AGGREGATION
    # --------------------------------------------------


    def compute_by_dimension(self, dimension: str, metric: str) -> list[dict]:
        data = self.load_data()
        df = data["claims"] if self.dataset_type == "claims" else data["sales"]
        ew_df = data.get("sales_ew") if self.dataset_type == "sales" else None

        if df.empty:
            return []

        df = df.copy()

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
            return self._compute_loss_ratio(dimension)
        else:
            return []

        dim_map = {
            "month": "Month",
            "state": "State",
            "brand": "Brand"
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

            dim_col = dim_map.get(dimension)
            if dim_col not in local_df.columns:
                if dimension == "state":
                    dim_col = _pick_column([
                        "State",
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
                elif dimension == "device_plan_category":
                    dim_col = _pick_column([
                        "Device Plan Category",
                        "Device Category",
                        "Product Brand(Group)",
                        "Product Brand (Group)",
                        "Product Brand",
                        "Brand",
                        "Plan_Category",
                        "Plan Category",
                    ])

                if dim_col not in local_df.columns:
                    if dimension == "month":
                        date_col = (
                            "Plan Start Date"
                            if self.dataset_type == "sales"
                            else "Day of Call_Date"
                        )
                        if date_col in local_df.columns:
                            local_df["Month"] = self._month_key(local_df[date_col])
                            dim_col = "Month"
                        else:
                            return None, None
                    else:
                        return None, None

            if dimension == "month":
                if dim_col != "Month":
                    local_df["Month"] = self._month_key(local_df[dim_col])
                if "Month" in local_df.columns:
                    local_df["Month"] = self._month_key(local_df["Month"])
                    local_df = local_df[local_df["Month"].notna()]
                    dim_col = "Month"

            return local_df, dim_col

        df, dim_col = resolve_dimension(df)
        if dim_col is None:
            return []

        out = (
            df.groupby(dim_col, dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={dim_col: dimension, "_value": metric})
        )

        if metric == "quantity" and ew_df is not None and not ew_df.empty:
            ew_df = ew_df.copy()
            ew_df["_value"] = 1
            ew_df, ew_dim_col = resolve_dimension(ew_df)
            if ew_dim_col is not None:
                ew_out = (
                    ew_df.groupby(ew_dim_col, dropna=False)["_value"]
                    .sum()
                    .reset_index()
                    .rename(columns={ew_dim_col: dimension, "_value": "ew_count"})
                )
                if dimension == "month" and "month" in ew_out.columns:
                    ew_out["month"] = pd.to_datetime(ew_out["month"], errors="coerce").dt.strftime("%b-%y")
                out = out.merge(ew_out, on=dimension, how="outer").fillna(0)

        if dimension == "month" and "month" in out.columns:
            out["month"] = pd.to_datetime(out["month"], errors="coerce").dt.strftime("%b-%y")

        return out.fillna(0).to_dict(orient="records")

    # --------------------------------------------------
    # LOSS RATIO
    # --------------------------------------------------

    def _compute_loss_ratio(self, dimension: str) -> list[dict]:
        data = self.load_data()
        sales = data["sales"]
        claims = data["claims"]

        if sales.empty or claims.empty:
            return []

        if dimension == "month":
            dim_sales = "Month"
            dim_claims = "Month"
            if "Month" in sales.columns:
                sales = sales.copy()
                sales["Month"] = self._month_key(sales["Month"])
            elif "Plan Start Date" in sales.columns:
                sales = sales.copy()
                sales["Month"] = self._month_key(sales["Plan Start Date"])
            if "Month" in claims.columns:
                claims = claims.copy()
                claims["Month"] = self._month_key(claims["Month"])
            elif "Day of Call_Date" in claims.columns:
                claims = claims.copy()
                claims["Month"] = self._month_key(claims["Day of Call_Date"])
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

        merged = sales_g.merge(
            claims_g,
            left_on=dim_sales,
            right_on=dim_claims,
            how="left",
        ).fillna(0)

        merged["loss_ratio"] = (
            merged["Net Claims"] / merged["Zopper Earned Premium"] * 100
        ).replace([float("inf"), float("-inf")], 0)

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
            if "Zopper's Cost" not in df.columns or "Net Claims" not in df.columns:
                return {
                    "gross_premium": 0,
                    "earned_premium": 0,
                    "zopper_earned_premium": 0,
                    "units_sold": int(len(df)),
                }
            return {
                "gross_premium": float(df["Zopper's Cost"].sum()),
                "earned_premium": float(df["Net Claims"].sum()),
                "zopper_earned_premium": float(df["Net Claims"].sum()),
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
        if (
            "Gross Premium" not in df.columns
            or "Earned Premium" not in df.columns
            or "Zopper Earned Premium" not in df.columns
        ):
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": int(len(df)),
            }
        return {
            "gross_premium": float(df["Gross Premium"].sum()),
            "earned_premium": float(df["Earned Premium"].sum()),
            "zopper_earned_premium": float(df["Zopper Earned Premium"].sum()),
            "units_sold": int(len(df)),
        }

    def compute(self) -> dict:
        return {}
