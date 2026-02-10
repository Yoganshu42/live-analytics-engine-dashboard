import re
import pandas as pd
from typing import Dict, List


KEYWORDS = {
    "gross_premium": [
        "gross",
        "selling",
        "plan_price",
        "plan_amount"
    ],

    "earned_premium": [
        "earned",
        "exposure",
        "prorated",
        "earned_amount"
    ],

    "zopper_earned_premium": [
        "zopper",
        "transfer",
        "shared",
        "revenue",
        "company_share"
    ],

    "net_claims": [
        "net",
        "claim_cost",
        "settlement",
        "payout"
    ],

    "loss_ratio": [
        "loss",
        "ratio",
        "lr",
        "profitability"
    ]
}

def normalize(col: str) -> str:
    return re.sub(r"[^a-z0-9]", "", col.lower())


def suggest_gross_premium(df: pd.DataFrame) -> Dict:
    scores = []

    for col in df.columns:
        norm = normalize(col)

        score = 0
        reasons = []

        for kw in KEYWORDS["gross_premium"]:
            if kw in norm:
                score += 2
                reasons.append(f"Keyword match: {kw}")

        if pd.api.types.is_numeric_dtype(df[col]):
            score += 2
            reasons.append("Numeric column")

        null_ratio = df[col].isna().mean()
        if null_ratio < 0.2:
            score += 1
            reasons.append("Low null ratio")

        scores.append({
            "column": col,
            "score": score,
            "null_ratio": null_ratio,
            "reasons": reasons
        })

    scores.sort(key=lambda x: x["score"], reverse=True)
    best = scores[0] if scores else None

    return {
        "operation": "gross_premium",
        "confidence": min(0.95, best["score"] / 7) if best else 0,
        "suggested_column": best["column"] if best else None,
        "null_strategy": "fill_zero",
        "reasoning": best["reasons"] if best else [],
        "candidates": scores[:5]
    }
