import pandas as pd
import numpy as np
from datetime import datetime

VALUATION_DATE = pd.to_datetime("2025-12-31")

# ---------- NORMALIZATION ----------

def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "Brand" in df.columns:
        df["Brand"] = df["Brand"].replace({
            "Idea": "Lenovo",
            "Pad": "Redmi",
            "GooglePixel": "Google",
            "OPPO": "Oppo",
        })

    for col in ["Plan Start Date", "Plan End Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def normalize_claims(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "Day of Call_Date" in df.columns:
        df = df[df["Day of Call_Date"].dt.year == 2025]

    if "Warranty Type" in df.columns:
        df["Warranty Type"] = df["Warranty Type"].replace({
            "Screen Protection": "Cracked Screen"
        })

    if "One time deductible" in df.columns:
        df["One time deductible"] = df["One time deductible"].fillna(999)

    return df


# ---------- PREMIUM CALCULATIONS ----------

def compute_premiums(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required = {
        "Plan Start Date",
        "Plan End Date",
        "Plan Selling Price",
        "Zopper Shared ( Transfer Price )",
    }

    if not required.issubset(df.columns):
        return df

    df["Coverage Days"] = (
        df["Plan End Date"] - df["Plan Start Date"]
    ).dt.days.clip(lower=1)

    df["Exposure Days"] = (
        VALUATION_DATE - df["Plan Start Date"]
    ).dt.days.clip(lower=0)

    df["Written Premium"] = df["Zopper Shared ( Transfer Price )"] * 1.18
    df["Zopper Earned Premium"] = (
        df["Written Premium"] *
        (df["Exposure Days"] / df["Coverage Days"])
    )

    df["Gross Premium"] = df["Plan Selling Price"]
    df["Earned Premium"] = (
        df["Gross Premium"] *
        (df["Exposure Days"] / df["Coverage Days"])
    )

    return df


# ---------- CLAIMS ----------

def compute_claims(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if {"Zopper's Cost", "One time deductible"}.issubset(df.columns):
        df["Net Claims"] = df["Zopper's Cost"] - df["One time deductible"]
    else:
        df["Net Claims"] = 0

    return df


# ---------- AGGREGATION (GENERIC, FIXED) ----------

def aggregate_dimension(
    sales: pd.DataFrame,
    claims: pd.DataFrame,
    dimension: str
) -> pd.DataFrame:

    if dimension not in sales.columns:
        return pd.DataFrame()

    # ❌ Drop pandas junk columns FIRST
    sales = sales.loc[:, ~sales.columns.str.lower().str.startswith("unnamed")]
    claims = claims.loc[:, ~claims.columns.str.lower().str.startswith("unnamed")]

    sales = sales.copy()
    # Explicit quantity column
    sales["Quantity"] = 1


    # --- Normalize numeric columns safely ---
    for col in sales.columns:
        if col == dimension:
            continue

        # Only attempt numeric cleanup on object columns
        if not pd.api.types.is_object_dtype(sales[col]):
            continue

        sales[col] = (
            sales[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("₹", "", regex=False)
            .str.strip()
        )

        sales[col] = pd.to_numeric(sales[col], errors="coerce")

    # --- Detect valid measures (NO unnamed allowed) ---
    measures = [
        c for c in sales.columns
        if c != dimension
        and pd.api.types.is_numeric_dtype(sales[c])
        and not c.lower().startswith("unnamed")
    ]

    if not measures:
        return pd.DataFrame()

    result = (
        sales
        .groupby(dimension)[measures]
        .sum()
        .reset_index()
    )

    return result


# ---------- PLANS VS CLAIMS ----------

def plans_vs_claims(
    sales: pd.DataFrame,
    claims: pd.DataFrame
) -> pd.DataFrame:

    if "Plan Type" not in sales.columns:
        return pd.DataFrame()

    plans_sold = sales.groupby("Plan Type").size().rename("Plans Sold")

    zopper_earned = (
        sales.groupby("Plan Type")["Zopper Earned Premium"].sum()
        if "Zopper Earned Premium" in sales.columns
        else pd.Series(0, index=plans_sold.index)
    )

    if "Plan Type" in claims.columns and "Net Claims" in claims.columns:
        net_claims = claims.groupby("Plan Type")["Net Claims"].sum()
    else:
        net_claims = pd.Series(0, index=plans_sold.index)

    result = pd.concat(
        [plans_sold, zopper_earned, net_claims],
        axis=1
    ).fillna(0)

    result["Loss Ratio (%)"] = (
        result["Net Claims"] / result["Zopper Earned Premium"] * 100
    ).replace([np.inf, -np.inf], 0).fillna(0)

    return result.reset_index()
def aggregate_by_dimension(df: pd.DataFrame, dimension: str, metric: str):
    if df.empty or dimension not in df.columns:
        return df.iloc[0:0]

    # drop junk columns
    df = df.loc[:, ~df.columns.str.lower().str.startswith("unnamed")]

    metric_key = metric.lower().strip()

    # metric normalization
    METRIC_MAP = {
        "gross_premium": "Amount",
        "earned_premium": "earned_premium",
        "zopper_earned_premium": "earned_zopper",
        "quantity": "Quantity",
    }

    if metric_key in {"claims", "net_claims"}:
        df = df.copy()

        def _first_col(candidates: list[str]) -> str | None:
            return next((c for c in candidates if c in df.columns), None)

        net_amt_col = _first_col(["Net Amount", "Net_Amount", "Net Claims", "Net_Claims"])
        otd_col = _first_col([
            "OTD Amount",
            "OTD_Amount",
            "One time deductible",
            "One Time Deductible",
        ])

        if not net_amt_col:
            return df.iloc[0:0]

        net_amt = pd.to_numeric(df[net_amt_col], errors="coerce").fillna(0)
        if metric_key == "claims":
            df["_value"] = net_amt
        else:
            if otd_col:
                otd = pd.to_numeric(df[otd_col], errors="coerce").fillna(0)
            else:
                otd = 0
            df["_value"] = net_amt - otd

        result = (
            df
            .groupby(dimension, dropna=False)["_value"]
            .sum()
            .reset_index()
            .rename(columns={"_value": metric})
        )

        return result

    if metric_key not in METRIC_MAP:
        return df.iloc[0:0]

    col = METRIC_MAP[metric_key]

    if col not in df.columns:
        return df.iloc[0:0]

    # ensure quantity exists
    if col == "Quantity":
        df = df.copy()
        df["Quantity"] = 1

    result = (
        df
        .groupby(dimension, dropna=False)[col]
        .sum()
        .reset_index()
        .rename(columns={col: metric})
    )

    return result


# ---------- LAST UPDATED ----------

DATE_COLUMNS = [
    "Month",
    "Month Name",
    "Month_Name",
]


def get_date_bounds(
    df: pd.DataFrame,
    dataset_type: str,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if df is None or df.empty:
        return None, None

    sales_candidates = [
        "Start_Date",
        "Start Date",
        "Plan Start Date",
        "Warranty Start Date",
        "Warranty Start_Date",
        "Warranty Purchase Date",
        "Invoice_Date_",
        "Invoice Date",
        "Bill Created Date",
        "Payment_date",
        "Payment Date",
        "Month",
        "Month Name",
        "Month_Name",
        "Plan End Date",
        "End Date",
        "End_Date",
    ]
    claims_candidates = [
        "Day of Call_Date",
        "Call_Date",
        "Call Date",
        "Month",
        "Month Name",
        "Month_Name",
    ]

    candidates = sales_candidates if dataset_type == "sales" else claims_candidates
    parsed = _coalesce_datetime_candidates(df, candidates)
    if parsed.isna().all():
        return None, None

    return parsed.min(), parsed.max()


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip()
    cleaned = raw.str.replace(r"\.0$", "", regex=True)

    yyyymm_mask = cleaned.str.fullmatch(r"\d{6}")
    yyyymm_normalized = cleaned.where(
        ~yyyymm_mask,
        cleaned.str.slice(0, 4) + "-" + cleaned.str.slice(4, 6) + "-01",
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

    return parsed


def _coalesce_datetime_candidates(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    for col in candidates:
        if col not in df.columns:
            continue
        parsed = _parse_datetime_series(df[col])
        out = out.where(out.notna(), parsed)
    return out


def _is_month_granular_series(series: pd.Series) -> bool:
    non_na = series.dropna()
    if non_na.empty:
        return False
    try:
        return float(non_na.dt.is_month_start.mean()) >= 0.9
    except Exception:
        return False


def _align_claims_range_to_month_bounds(
    series: pd.Series,
    from_dt: pd.Timestamp | None,
    to_dt: pd.Timestamp | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if not _is_month_granular_series(series):
        return from_dt, to_dt

    aligned_from = from_dt
    aligned_to = to_dt
    if aligned_from is not None and not pd.isna(aligned_from):
        aligned_from = pd.Timestamp(aligned_from).to_period("M").to_timestamp()
    if aligned_to is not None and not pd.isna(aligned_to):
        aligned_to = pd.Timestamp(aligned_to).to_period("M").to_timestamp(how="end")
    return aligned_from, aligned_to


def filter_by_date_range(
    df: pd.DataFrame,
    dataset_type: str,
    from_date: str | None,
    to_date: str | None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if not from_date and not to_date:
        return df

    from_dt = pd.to_datetime(from_date, errors="coerce") if from_date else None
    to_dt = pd.to_datetime(to_date, errors="coerce") if to_date else None

    if from_dt is pd.NaT and to_dt is pd.NaT:
        return df

    # Candidate date columns by dataset
    sales_candidates = [
        "Start_Date",
        "Start Date",
        "Plan Start Date",
        "Warranty Start Date",
        "Warranty Start_Date",
        "Warranty Purchase Date",
        "Invoice_Date_",
        "Invoice Date",
        "Bill Created Date",
        "Payment_date",
        "Payment Date",
        "Month",
        "Month Name",
        "Month_Name",
        "Plan End Date",
        "End Date",
        "End_Date",
    ]
    claims_candidates = [
        "Day of Call_Date",
        "Call_Date",
        "Call Date",
        "Call_Registered_Date",
        "Call Registered Date",
        "Call_Initiated_Date",
        "Call Initiated Date",
        "Month",
        "Month Name",
        "Month_Name",
        "Month-Year",
        "Month Year",
        "Month_Year",
        "Fiscal Month",
        "Invoice_Date_",
        "Invoice Date",
        "Payment_date",
        "Payment Date",
        "Posting Date",
        "Complete Date",
        "Bill Created Date",
        "Warranty_start_date_",
        "Warranty Start Date",
        "Date",
    ]

    candidates = sales_candidates if dataset_type == "sales" else claims_candidates
    series = _coalesce_datetime_candidates(df, candidates)
    if series.isna().all():
        # Date filters are explicit. If no parseable date columns exist, return no rows
        # to avoid silently showing unfiltered data.
        return df.iloc[0:0]

    if dataset_type == "claims":
        from_dt, to_dt = _align_claims_range_to_month_bounds(series, from_dt, to_dt)

    mask = series.notna()
    if from_dt is not None and from_dt is not pd.NaT:
        mask &= series >= from_dt
    if to_dt is not None and to_dt is not pd.NaT:
        mask &= series <= to_dt

    return df[mask]


def get_latest_date(df: pd.DataFrame):
    if df is None or df.empty:
        return None

    current_year = datetime.now().year
    current_month = datetime.now().month

    latest = None
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue
        series = pd.to_datetime(df[col], errors="coerce")
        if series.notna().any():
            year = series.dt.year
            month = series.dt.month
            fixed = series

            # Fix bogus years (e.g., 0001) by mapping to current year
            bad_year = year < 2000
            if bad_year.any():
                fixed = fixed.where(
                    ~bad_year,
                    pd.to_datetime(
                        {
                            "year": current_year,
                            "month": month.clip(1, 12),
                            "day": 1,
                        },
                        errors="coerce",
                    ),
                )

            # If month/year are in the future, map them to last year
            future_mask = (year > current_year) | (
                (year == current_year) & (month > current_month)
            )
            if future_mask.any():
                fixed = fixed.where(
                    ~future_mask,
                    pd.to_datetime(
                        {
                            "year": current_year - 1,
                            "month": month.clip(1, 12),
                            "day": 1,
                        },
                        errors="coerce",
                    ),
                )

            max_date = fixed.max()
            if latest is None or max_date > latest:
                latest = max_date
    return latest
