"""
market_data.py
Provides macro market data for the home dashboard.
Uses simulated-but-realistic multi-year series for Nifty50, Sensex, Gold, Silver, Oil.
In production: replace fetch_* functions with actual API calls (Yahoo Finance / NSE / MCX).
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random


def _generate_price_series(
    start_price: float,
    days: int,
    annual_return: float,
    annual_vol: float,
    seed: int,
) -> pd.DataFrame:
    """Generate a realistic GBM price series (daily)."""
    rng = np.random.default_rng(seed)
    dt = 1 / 252
    mu = annual_return / 100
    sigma = annual_vol / 100

    dates = pd.date_range(end=datetime.today(), periods=days, freq="B")
    prices = [start_price]
    for _ in range(days - 1):
        shock = rng.normal(0, 1)
        ret = (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * shock
        prices.append(prices[-1] * np.exp(ret))

    return pd.DataFrame({"date": dates, "price": [round(p, 2) for p in prices]})


# ─── INDIVIDUAL MARKET DATA FETCHERS ──────────────────────────────────────────

def fetch_nifty50(days: int = 756) -> pd.DataFrame:
    """Nifty 50 — ~3 years of daily data."""
    df = _generate_price_series(17500, days, annual_return=14, annual_vol=16, seed=1)
    df["index"] = "Nifty 50"
    return df


def fetch_sensex(days: int = 756) -> pd.DataFrame:
    """BSE Sensex 100."""
    df = _generate_price_series(58000, days, annual_return=13.5, annual_vol=15, seed=2)
    df["index"] = "Sensex"
    return df


def fetch_gold(days: int = 756) -> pd.DataFrame:
    """Gold price in INR per 10g."""
    df = _generate_price_series(55000, days, annual_return=10, annual_vol=12, seed=3)
    df["index"] = "Gold (₹/10g)"
    return df


def fetch_silver(days: int = 756) -> pd.DataFrame:
    """Silver price in INR per kg."""
    df = _generate_price_series(68000, days, annual_return=8, annual_vol=18, seed=4)
    df["index"] = "Silver (₹/kg)"
    return df


def fetch_crude_oil(days: int = 756) -> pd.DataFrame:
    """Brent crude in USD/barrel."""
    df = _generate_price_series(82, days, annual_return=2, annual_vol=28, seed=5)
    df["index"] = "Brent Crude (USD)"
    return df


def fetch_usd_inr(days: int = 756) -> pd.DataFrame:
    """USD/INR exchange rate."""
    df = _generate_price_series(82.5, days, annual_return=2.5, annual_vol=4, seed=6)
    df["index"] = "USD/INR"
    return df


# ─── AGGREGATED SNAPSHOT FOR KPI CARDS ────────────────────────────────────────

def get_market_snapshot() -> dict:
    """Current price + change for all instruments."""
    instruments = {
        "Nifty 50": fetch_nifty50(5),
        "Sensex": fetch_sensex(5),
        "Gold": fetch_gold(5),
        "Silver": fetch_silver(5),
        "Crude Oil": fetch_crude_oil(5),
        "USD/INR": fetch_usd_inr(5),
    }
    snapshot = {}
    for name, df in instruments.items():
        curr = df["price"].iloc[-1]
        prev = df["price"].iloc[-2]
        chg = curr - prev
        chg_pct = (chg / prev) * 100
        snapshot[name] = {
            "price": curr,
            "change": round(chg, 2),
            "change_pct": round(chg_pct, 2),
        }
    return snapshot


def get_1y_returns() -> dict:
    """1-year return for each instrument."""
    fns = {
        "Nifty 50": (fetch_nifty50, 252),
        "Sensex": (fetch_sensex, 252),
        "Gold": (fetch_gold, 252),
        "Silver": (fetch_silver, 252),
        "Crude Oil": (fetch_crude_oil, 252),
    }
    returns = {}
    for name, (fn, days) in fns.items():
        df = fn(days)
        ret = (df["price"].iloc[-1] / df["price"].iloc[0] - 1) * 100
        returns[name] = round(ret, 1)
    return returns


def get_sector_rotation_signal() -> list:
    """
    High-level sector rotation signals based on macro context.
    In production: derive from FII/DII sector-level flow data.
    """
    return [
        {"sector": "Banking & Finance", "signal": "Overweight", "reason": "Credit growth strong, RBI neutral", "color": "#00d4aa"},
        {"sector": "IT / Technology", "signal": "Overweight", "reason": "USD strength, US tech revival", "color": "#00d4aa"},
        {"sector": "Healthcare / Pharma", "signal": "Neutral", "reason": "US FDA approvals mixed", "color": "#f59e0b"},
        {"sector": "Infrastructure", "signal": "Overweight", "reason": "Govt capex ₹11L Cr budget", "color": "#00d4aa"},
        {"sector": "FMCG / Consumer", "signal": "Underweight", "reason": "Rural slowdown, margins squeezed", "color": "#ef4444"},
        {"sector": "Auto", "signal": "Neutral", "reason": "EV transition risk, export pressure", "color": "#f59e0b"},
        {"sector": "Metals", "signal": "Underweight", "reason": "China demand weak, global slowdown", "color": "#ef4444"},
        {"sector": "Real Estate", "signal": "Neutral", "reason": "Premium segment strong, affordable weak", "color": "#f59e0b"},
    ]
