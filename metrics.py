"""
metrics.py
Scoring, ranking, and analysis engine for Indian mutual funds.
"""

import pandas as pd
import numpy as np
from typing import Dict, List


# ─── MACRO CONTEXT (update this weekly based on news) ─────────────────────────
MACRO_CONTEXT = {
    "india_gdp_growth": "Strong (7%+)",
    "india_inflation": "Moderate (4.5-5%)",
    "rbi_rate_stance": "Neutral (repo rate 6.5%)",
    "us_fed_stance": "Holding rates high",
    "global_oil": "Volatile ($75-90/barrel)",
    "usd_inr": "Stable (83-84 range)",
    "india_market_sentiment": "Cautiously Bullish",
    "favored_sectors": ["IT", "Banking", "Pharma", "Infrastructure"],
    "caution_sectors": ["FMCG", "Auto (EV transition risk)"],
}


def get_top_funds(df: pd.DataFrame, category: str, n: int = 5) -> pd.DataFrame:
    """Return top N funds in a category by composite score."""
    cat_df = df[df["category"] == category].copy()
    return cat_df.nlargest(n, "composite_score")[
        ["scheme_name", "amc", "cagr_1y", "cagr_3y", "cagr_5y",
         "sharpe_ratio", "expense_ratio", "aum_cr", "composite_score"]
    ].reset_index(drop=True)


def get_risk_adjusted_picks(df: pd.DataFrame) -> pd.DataFrame:
    """Best funds sorted by Sharpe ratio (risk-adjusted return)."""
    return df.nlargest(10, "sharpe_ratio")[
        ["scheme_name", "category", "cagr_3y", "sharpe_ratio",
         "volatility", "expense_ratio", "composite_score"]
    ].reset_index(drop=True)


def get_sector_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate performance by sector for sectoral funds."""
    sec_df = df[df["category"] == "Sectoral/Thematic"].copy()
    sec_df = sec_df[sec_df["sector"] != "-"]

    if sec_df.empty:
        return pd.DataFrame()

    grouped = sec_df.groupby("sector").agg(
        fund_count=("scheme_name", "count"),
        avg_cagr_3y=("cagr_3y", "mean"),
        avg_cagr_5y=("cagr_5y", "mean"),
        avg_sharpe=("sharpe_ratio", "mean"),
        avg_expense=("expense_ratio", "mean"),
        avg_composite=("composite_score", "mean"),
    ).reset_index()

    grouped = grouped.round(2)
    grouped = grouped.sort_values("avg_composite", ascending=False)
    return grouped


def get_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """High-level summary stats per category."""
    summary = df.groupby("category").agg(
        fund_count=("scheme_name", "count"),
        avg_cagr_1y=("cagr_1y", "mean"),
        avg_cagr_3y=("cagr_3y", "mean"),
        avg_cagr_5y=("cagr_5y", "mean"),
        avg_sharpe=("sharpe_ratio", "mean"),
        avg_expense=("expense_ratio", "mean"),
        best_composite=("composite_score", "max"),
    ).reset_index().round(2)

    return summary.sort_values("avg_cagr_3y", ascending=False)


def build_sip_allocation(profile: str = "moderate") -> Dict:
    """
    Suggested SIP allocation by investor profile.
    Returns dict with category → allocation %.
    """
    profiles = {
        "conservative": {
            "Large Cap": 50,
            "Flexi Cap": 30,
            "Mid Cap": 15,
            "Sectoral/Thematic": 5,
        },
        "moderate": {
            "Large Cap": 40,
            "Mid Cap": 30,
            "Flexi Cap": 15,
            "Sectoral/Thematic": 10,
            "Small Cap": 5,
        },
        "aggressive": {
            "Mid Cap": 35,
            "Small Cap": 25,
            "Sectoral/Thematic": 20,
            "Large Cap": 15,
            "Flexi Cap": 5,
        },
    }
    return profiles.get(profile, profiles["moderate"])


def build_claude_prompt(df: pd.DataFrame) -> str:
    """
    Construct a structured prompt with key metrics summary for Claude.
    Keeps token usage lean.
    """
    cat_summary = get_category_summary(df).to_dict(orient="records")
    sector_summary = get_sector_analysis(df).head(6).to_dict(orient="records")

    # Top 3 per category
    top_per_cat = {}
    for cat in df["category"].unique():
        tops = get_top_funds(df, cat, n=3)[["scheme_name", "amc", "cagr_3y", "sharpe_ratio", "composite_score"]]
        top_per_cat[cat] = tops.to_dict(orient="records")

    prompt = f"""
You are a senior Indian mutual fund analyst. Analyze the following data and provide actionable investment insights.

## CATEGORY SUMMARY
{pd.DataFrame(cat_summary).to_string(index=False)}

## TOP FUNDS PER CATEGORY
{top_per_cat}

## SECTOR ANALYSIS (Sectoral/Thematic Funds)
{pd.DataFrame(sector_summary).to_string(index=False) if sector_summary else "No sectoral data"}

## CURRENT MACRO CONDITIONS
- India GDP Growth: {MACRO_CONTEXT['india_gdp_growth']}
- India Inflation: {MACRO_CONTEXT['india_inflation']}
- RBI Rate Stance: {MACRO_CONTEXT['rbi_rate_stance']}
- US Fed Stance: {MACRO_CONTEXT['us_fed_stance']}
- Oil Prices: {MACRO_CONTEXT['global_oil']}
- Market Sentiment: {MACRO_CONTEXT['india_market_sentiment']}
- Macro-Favored Sectors: {', '.join(MACRO_CONTEXT['favored_sectors'])}
- Sectors to Watch Cautiously: {', '.join(MACRO_CONTEXT['caution_sectors'])}

## YOUR TASKS
1. **Top 3 SIP Picks Overall** — Best funds for a 5-year SIP horizon with reasoning
2. **Category Outlook** — 2-line view on each category (Large Cap, Mid Cap, Sectoral)
3. **Best Sector Right Now** — Which sector fund to consider and why
4. **Risk Warning** — 2 key risks an Indian retail investor must watch
5. **Allocation Strategy** — Suggest a simple 3-category SIP split (%) for a moderate-risk investor

Be concise, analytical, and specific. Avoid vague statements. Reference the data.
Format your response with clear headers using ##.
"""
    return prompt.strip()


def simulate_sip_growth(monthly_amount: int, years: int, expected_cagr: float) -> Dict:
    """
    Simulate SIP returns using XIRR approximation.
    Returns invested amount, final value, and gain.
    """
    months = years * 12
    monthly_rate = expected_cagr / 100 / 12
    if monthly_rate == 0:
        final_value = monthly_amount * months
    else:
        final_value = monthly_amount * (((1 + monthly_rate) ** months - 1) / monthly_rate) * (1 + monthly_rate)

    invested = monthly_amount * months
    gain = final_value - invested
    return {
        "invested": round(invested),
        "final_value": round(final_value),
        "gain": round(gain),
        "return_pct": round((gain / invested) * 100, 1),
    }
