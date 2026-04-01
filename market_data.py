"""
market_data.py
REAL market data via yfinance — NSE/BSE live prices, no API key needed.
Falls back to last cached data if network fails.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os, json

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

# ─── TICKER MAP ───────────────────────────────────────────────────────────────
# yfinance symbols for Indian markets
TICKERS = {
    "Nifty 50":  "^NSEI",
    "Sensex":    "^BSESN",
    "Gold":      "GC=F",       # Gold futures USD/oz → converted to INR/10g
    "Silver":    "SI=F",       # Silver futures USD/oz → converted to INR/kg
    "Crude Oil": "BZ=F",       # Brent crude USD/bbl
    "USD/INR":   "INR=X",      # USD to INR
}

CACHE_FILE = "market_cache.json"
CACHE_TTL_MINUTES = 30  # refresh every 30 min


def _load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(data: dict):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _cache_fresh(cache: dict, key: str) -> bool:
    ts = cache.get(f"{key}__ts")
    if not ts:
        return False
    age = (datetime.now() - datetime.fromisoformat(ts)).total_seconds() / 60
    return age < CACHE_TTL_MINUTES


def _get_usd_inr() -> float:
    """Fetch live USD/INR rate."""
    try:
        df = yf.download("INR=X", period="5d", interval="1d", progress=False, auto_adjust=True)
        if not df.empty:
            return float(df["Close"].dropna().iloc[-1])
    except Exception:
        pass
    return 84.0  # fallback


def _fetch_series(ticker_key: str, days: int) -> pd.DataFrame:
    """Fetch historical OHLCV from yfinance."""
    symbol = TICKERS[ticker_key]
    period_days = days + 30  # buffer for weekends/holidays
    start = (datetime.today() - timedelta(days=period_days)).strftime("%Y-%m-%d")

    df = yf.download(symbol, start=start, progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"No data for {symbol}")

    df = df[["Close"]].copy()
    df.columns = ["price"]
    df.index.name = "date"
    df = df.dropna().reset_index()
    df["date"] = pd.to_datetime(df["date"])

    # Convert commodity prices to INR if needed
    usd_inr = _get_usd_inr()

    if ticker_key == "Gold":
        # USD/troy oz → INR/10g  (1 troy oz = 31.1035g)
        df["price"] = df["price"] * usd_inr / 31.1035 * 10
    elif ticker_key == "Silver":
        # USD/troy oz → INR/kg  (1 troy oz = 31.1035g, 1 kg = 1000g)
        df["price"] = df["price"] * usd_inr / 31.1035 * 1000
    # Crude Oil stays in USD/bbl, USD/INR stays as is, Nifty/Sensex already in INR

    df["price"] = df["price"].round(2)
    # Return only the last `days` business days
    return df.tail(days).reset_index(drop=True)


def _fetch_with_cache(key: str, days: int) -> pd.DataFrame:
    """Fetch with cache fallback."""
    cache = _load_cache()
    cache_key = f"{key}_{days}"

    if _cache_fresh(cache, cache_key) and cache_key in cache:
        raw = cache[cache_key]
        return pd.DataFrame(raw)

    if not YF_AVAILABLE:
        return _fallback_series(key, days)

    try:
        df = _fetch_series(key, days)
        cache[cache_key] = df.to_dict(orient="list")
        cache[f"{cache_key}__ts"] = datetime.now().isoformat()
        _save_cache(cache)
        return df
    except Exception as e:
        print(f"⚠️ yfinance fetch failed for {key}: {e}")
        if cache_key in cache:
            return pd.DataFrame(cache[cache_key])
        return _fallback_series(key, days)


def _fallback_series(key: str, days: int) -> pd.DataFrame:
    """Last-resort static fallback so the app doesn't crash."""
    defaults = {
        "Nifty 50":  (22500, 14, 16, 1),
        "Sensex":    (74000, 13.5, 15, 2),
        "Gold":      (72000, 10, 12, 3),
        "Silver":    (85000, 8, 18, 4),
        "Crude Oil": (85, 2, 28, 5),
        "USD/INR":   (84, 2.5, 4, 6),
    }
    sp, ar, av, seed = defaults.get(key, (100, 10, 15, 0))
    rng = np.random.default_rng(seed)
    dt = 1 / 252
    mu, sigma = ar / 100, av / 100
    dates = pd.date_range(end=datetime.today(), periods=days, freq="B")
    prices = [sp]
    for _ in range(days - 1):
        ret = (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * rng.normal()
        prices.append(prices[-1] * np.exp(ret))
    return pd.DataFrame({"date": dates, "price": [round(p, 2) for p in prices]})


# ─── PUBLIC FETCH FUNCTIONS ───────────────────────────────────────────────────

def fetch_nifty50(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("Nifty 50", days)
    df["index"] = "Nifty 50"
    return df

def fetch_sensex(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("Sensex", days)
    df["index"] = "Sensex"
    return df

def fetch_gold(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("Gold", days)
    df["index"] = "Gold (₹/10g)"
    return df

def fetch_silver(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("Silver", days)
    df["index"] = "Silver (₹/kg)"
    return df

def fetch_crude_oil(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("Crude Oil", days)
    df["index"] = "Brent Crude (USD)"
    return df

def fetch_usd_inr(days: int = 756) -> pd.DataFrame:
    df = _fetch_with_cache("USD/INR", days)
    df["index"] = "USD/INR"
    return df


# ─── SNAPSHOT & RETURNS ───────────────────────────────────────────────────────

def get_market_snapshot() -> dict:
    """Live current price + today's change for all instruments."""
    keys = ["Nifty 50", "Sensex", "Gold", "Silver", "Crude Oil", "USD/INR"]
    snapshot = {}
    for key in keys:
        try:
            df = _fetch_with_cache(key, 5)
            curr = float(df["price"].iloc[-1])
            prev = float(df["price"].iloc[-2])
            chg = curr - prev
            chg_pct = (chg / prev) * 100
            snapshot[key] = {
                "price": curr,
                "change": round(chg, 2),
                "change_pct": round(chg_pct, 2),
            }
        except Exception:
            snapshot[key] = {"price": 0, "change": 0, "change_pct": 0}
    return snapshot


def get_1y_returns() -> dict:
    """Real 1-year return for each instrument."""
    keys = ["Nifty 50", "Sensex", "Gold", "Silver", "Crude Oil"]
    returns = {}
    for key in keys:
        try:
            df = _fetch_with_cache(key, 252)
            ret = (float(df["price"].iloc[-1]) / float(df["price"].iloc[0]) - 1) * 100
            returns[key] = round(ret, 1)
        except Exception:
            returns[key] = 0.0
    return returns


def get_sector_rotation_signal() -> list:
    """Macro sector signals — update weekly based on news."""
    return [
        {"sector": "Banking & Finance", "signal": "Overweight", "reason": "Credit growth strong, RBI neutral", "color": "#00d4aa"},
        {"sector": "IT / Technology",   "signal": "Overweight", "reason": "USD strength, US tech spending revival", "color": "#00d4aa"},
        {"sector": "Healthcare / Pharma","signal": "Neutral",   "reason": "US FDA approvals mixed", "color": "#f59e0b"},
        {"sector": "Infrastructure",    "signal": "Overweight", "reason": "Govt capex ₹11L Cr budget", "color": "#00d4aa"},
        {"sector": "FMCG / Consumer",   "signal": "Underweight","reason": "Rural slowdown, margins squeezed", "color": "#ef4444"},
        {"sector": "Auto",              "signal": "Neutral",    "reason": "EV transition risk, export pressure", "color": "#f59e0b"},
        {"sector": "Metals",            "signal": "Underweight","reason": "China demand weak, global slowdown", "color": "#ef4444"},
        {"sector": "Real Estate",       "signal": "Neutral",    "reason": "Premium segment strong, affordable weak", "color": "#f59e0b"},
    ]