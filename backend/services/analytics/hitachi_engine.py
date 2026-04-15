from __future__ import annotations

import numpy as np
import pandas as pd

from services.analytics.goodrej_engine import (
    GodrejAnalyticsEngine,
    invalidate_godrej_load_cache,
)
from services.hitachi_plan_mapping import (
    canonicalize_hitachi_claim_plan_category,
    canonicalize_hitachi_sales_plan_category,
)


def invalidate_hitachi_load_cache(
    source: str | None = None,
    dataset_type: str | None = None,
    job_id: str | None = None,
) -> None:
    invalidate_godrej_load_cache(
        source=source or "hitachi",
        dataset_type=dataset_type,
        job_id=job_id,
    )


class HitachiAnalyticsEngine(GodrejAnalyticsEngine):
    SOURCE_PATTERNS = ("hitachi%",)
    HITACHI_PREMIUM_RATE = 0.35
    # Hitachi sales filters should follow the sale/purchase date fields.
    # Warranty-start dates can extend into future coverage periods and skew UI presets.
    SALES_PRIMARY_DATE_CANDIDATES = (
        "Warranty Purchase Date",
        "Plan Start Date",
        "Date",
        "Start Date",
        "Start_Date",
        "Month",
        "Warranty Start Date",
        "Warranty Start_Date",
        "Product Purchased Date",
    )
    SALES_FALLBACK_DATE_CANDIDATES = (
        "Warranty Start Date",
        "Warranty Start_Date",
    )

    def __init__(
        self,
        db,
        job_id: str | None,
        source: str | None = "hitachi",
        dataset_type: str | None = "sales",
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        super().__init__(
            db=db,
            job_id=job_id,
            source=source or "hitachi",
            dataset_type=dataset_type,
            from_date=from_date,
            to_date=to_date,
        )

    def _retail_premium_series(self, sales_df: pd.DataFrame) -> pd.Series:
        for column in ("Retail Premium", "Retail_Premium"):
            if column in sales_df.columns:
                return self._numeric_series_from_frame(sales_df, column)
        return pd.Series(0.0, index=sales_df.index, dtype="float64")

    def _hitachi_premium_total_series(self, sales_df: pd.DataFrame) -> pd.Series:
        customer_premium = self._numeric_series_from_frame(sales_df, "Customer Premium")
        return customer_premium * self.HITACHI_PREMIUM_RATE

    def _store_premium_total_series(self, sales_df: pd.DataFrame) -> pd.Series:
        customer_premium = self._numeric_series_from_frame(sales_df, "Customer Premium")
        retail_premium = self._retail_premium_series(sales_df)
        return (customer_premium - retail_premium).fillna(0.0)

    def _zopper_total_share_amount_series(self, sales_df: pd.DataFrame) -> pd.Series:
        customer_premium = self._numeric_series_from_frame(sales_df, "Customer Premium")
        store_premium = self._store_premium_total_series(sales_df)
        hitachi_premium = self._hitachi_premium_total_series(sales_df)
        zopper_share = customer_premium - (store_premium + hitachi_premium)
        return zopper_share.fillna(0.0).clip(lower=0.0)

    def _first_available_datetime_series(
        self,
        df: pd.DataFrame,
        candidates: tuple[str, ...],
    ) -> pd.Series:
        if df is None or df.empty:
            return pd.Series(dtype="datetime64[ns]")

        for candidate in candidates:
            if candidate not in df.columns:
                continue
            parsed = self._parse_month_series(df[candidate])
            if parsed is not None and parsed.notna().any():
                return parsed

        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    def _sales_anchor_date_series(self, df: pd.DataFrame) -> pd.Series:
        primary = self._first_available_datetime_series(df, self.SALES_PRIMARY_DATE_CANDIDATES)
        if primary is not None and primary.notna().any():
            return primary
        return self._first_available_datetime_series(df, self.SALES_FALLBACK_DATE_CANDIDATES)

    def _claim_amount_series(self, df: pd.DataFrame) -> pd.Series:
        claim_amount = super()._claim_amount_series(df)
        if df is None or df.empty:
            return claim_amount

        dealer_price = self._numeric_series_from_frame(df, "Dealer Price")
        fallback_mask = claim_amount.le(0) & dealer_price.gt(0)
        if fallback_mask.any():
            claim_amount = claim_amount.where(~fallback_mask, dealer_price)
        return claim_amount

    def _normalize_sales_plan_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        work = df.copy()
        plan_raw = self._coalesce_text_series(
            work,
            [
                "Plan Category",
                "Plan_Category",
                "display_plan_name",
                "Warranty Type",
            ],
            default="",
        )
        canonical_plan = plan_raw.map(canonicalize_hitachi_sales_plan_category)
        mask = canonical_plan.ne("")
        if mask.any():
            work.loc[mask, "Plan Category"] = canonical_plan[mask]
            work.loc[mask, "Plan_Category"] = canonical_plan[mask]

            device_raw = self._coalesce_text_series(
                work,
                [
                    "Device Plan Category",
                    "Device_Plan_Category",
                ],
                default="",
            )
            device_replace_mask = device_raw.eq("") | device_raw.astype(str).str.strip().eq(plan_raw.astype(str).str.strip())
            device_replace_mask &= mask
            if device_replace_mask.any():
                work.loc[device_replace_mask, "Device Plan Category"] = canonical_plan[device_replace_mask]
                work.loc[device_replace_mask, "Device_Plan_Category"] = canonical_plan[device_replace_mask]
        return work

    def _normalize_claim_plan_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        work = df.copy()
        plan_name = self._coalesce_text_series(
            work,
            [
                "Care+ Plan Name",
                "Care + Plan Name",
                "Care Plus Plan Name",
                "Plan Category",
                "Plan_Category",
            ],
            default="",
        )
        plan_description = self._coalesce_text_series(
            work,
            [
                "Care+ Plan Description",
                "Care + Plan Description",
                "Care Plus Plan Description",
                "Plan Description",
            ],
            default="",
        )
        product_category = self._coalesce_text_series(
            work,
            [
                "Product Category",
                "Product_Category",
                "Prodcut Category",
                "Category",
            ],
            default="",
        )
        model_description = self._coalesce_text_series(
            work,
            [
                "Model Description",
                "Item Description",
                "Model No",
            ],
            default="",
        )
        canonical_plan = pd.Series(
            [
                canonicalize_hitachi_claim_plan_category(
                    plan_name=plan_name.loc[idx],
                    plan_description=plan_description.loc[idx],
                    product_category=product_category.loc[idx],
                    model_description=model_description.loc[idx],
                )
                for idx in work.index
            ],
            index=work.index,
            dtype="object",
        )
        mask = canonical_plan.ne("")
        if mask.any():
            work.loc[mask, "Plan Category"] = canonical_plan[mask]
            work.loc[mask, "Plan_Category"] = canonical_plan[mask]
        return work

    def compute_premiums(self, df: pd.DataFrame) -> pd.DataFrame:
        required = {
            "Warranty Start Date",
            "Customer Premium",
        }
        if not required.issubset(df.columns):
            return self._normalize_sales_plan_fields(df)

        df = self._normalize_sales_plan_fields(df)

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

        df["Customer Premium"] = pd.to_numeric(df["Customer Premium"], errors="coerce").fillna(0.0)
        df["Retail Premium"] = self._retail_premium_series(df)
        if "Zopper Plan Duration" in df.columns:
            df["Zopper Plan Duration"] = pd.to_numeric(df["Zopper Plan Duration"], errors="coerce")
        else:
            df["Zopper Plan Duration"] = np.nan

        df["Coverage_Days"] = np.where(
            df["Warranty End Date"].notna(),
            (df["Warranty End Date"] - df["Warranty Start Date"]).dt.days,
            df["Zopper Plan Duration"] * 30,
        )
        df["Coverage_Days"] = df["Coverage_Days"].clip(lower=1)

        df["Used_Days"] = (self.valuation_date - df["Warranty Start Date"]).dt.days
        df["Used_Days"] = df[["Used_Days", "Coverage_Days"]].min(axis=1)
        df["Used_Days"] = df["Used_Days"].clip(lower=0)

        earned_ratio = (df["Used_Days"] / df["Coverage_Days"]).fillna(0.0)
        df["Earned_Premium"] = df["Customer Premium"] * earned_ratio
        df["Unearned_Premium"] = df["Customer Premium"] - df["Earned_Premium"]

        missing_start = df["Warranty Start Date"].isna()
        if missing_start.any():
            df.loc[missing_start, "Earned_Premium"] = 0
            df.loc[missing_start, "Unearned_Premium"] = df.loc[missing_start, "Customer Premium"]

        df["Store Premium"] = self._store_premium_total_series(df)
        df["Hitachi Premium"] = self._hitachi_premium_total_series(df)
        df["Zopper Share"] = self._zopper_total_share_amount_series(df)
        df["Zopper_Share_EP"] = df["Zopper Share"] * earned_ratio
        df["Zopper_Unearned"] = (df["Zopper Share"] - df["Zopper_Share_EP"]).clip(lower=0.0)

        # Keep shared appliance outputs populated so the rest of the analytics pipeline stays compatible.
        df["Hitachi_Share_EP"] = df["Hitachi Premium"] * earned_ratio
        df["Godrej_Share_EP"] = df["Hitachi_Share_EP"]
        df["Channel_Share_EP"] = df["Store Premium"] * earned_ratio

        return df

    def _normalize_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = super()._normalize_claims(df)
        return self._normalize_claim_plan_fields(normalized)

    def _apply_date_filter(
        self,
        df: pd.DataFrame,
        dataset_type: str,
    ) -> pd.DataFrame:
        if dataset_type != "sales":
            return super()._apply_date_filter(df, dataset_type)
        if df.empty or not self.apply_date_filter:
            return df

        series = self._sales_anchor_date_series(df)
        if series is None or series.empty or series.isna().all():
            return df.iloc[0:0]

        mask = series.notna()
        if self.report_start is not None and self.report_start is not pd.NaT:
            mask &= series >= self.report_start
        if self.report_end is not None and self.report_end is not pd.NaT:
            mask &= series <= self.report_end
        return df[mask]

    def _resolve_dimension(
        self,
        df: pd.DataFrame,
        dimension: str,
        dataset_type: str,
    ) -> tuple[pd.DataFrame, str | None]:
        dim_key = str(dimension or "").strip().lower()
        if dataset_type == "claims" and dim_key == "plan_category":
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

            normalized = {_normalize_key(str(column)): str(column) for column in df.columns}
            for candidate in ("Plan Category", "Plan_Category", "Care+ Plan Name", "Care+ Plan"):
                matched = normalized.get(_normalize_key(candidate))
                if matched:
                    return df, matched

        if dataset_type == "sales" and dim_key == "month":
            month_series = self._sales_anchor_date_series(df)
            if month_series is None or month_series.empty or month_series.isna().all():
                return df, None
            df = df.copy()
            df["_month_key"] = month_series.dt.to_period("M").dt.to_timestamp()
            return df, "_month_key"
        return super()._resolve_dimension(df, dimension, dataset_type)

    def compute_summary(self) -> dict:
        if self.dataset_type != "sales":
            return super().compute_summary()

        data = self.load_data(include_sales=True, include_claims=False)
        df = data.get("sales", pd.DataFrame())
        if df.empty:
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        df_period = self._apply_date_filter(df, "sales")
        if self.apply_date_filter and df_period.empty:
            return {
                "gross_premium": 0,
                "earned_premium": 0,
                "zopper_earned_premium": 0,
                "units_sold": 0,
            }

        gross = self._numeric_series_from_frame(df_period, "Customer Premium").sum()
        earned = self._numeric_series_from_frame(df_period, "Earned_Premium").sum()
        zopper_earned = self._numeric_series_from_frame(df_period, "Zopper_Share_EP").sum()

        return {
            "gross_premium": float(gross),
            "earned_premium": float(earned),
            "zopper_earned_premium": float(zopper_earned),
            "units_sold": int(len(df_period)),
        }
