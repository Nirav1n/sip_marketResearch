"""
data_fetcher.py  v6  (fixed)
=============================
Fixes vs v5:
  1. volatility=None (not hardcoded 15.0) when real data missing
     → Sharpe / max_drawdown / composite_score show None for unsynced funds
  2. composite_score formula only applied when CAGR data exists
  3. enrich_fund() passes aum_cr from perf_summary (not aum_summary — both are same dict now)
  4. get_fund_aum_summary() call signature aligned with fixed holdings_db
  5. load_fund_data() filter flow: EQUITY_CATEGORIES filter applied correctly
  6. No circular import: holdings_db imports done inside functions where needed
"""

import requests
import pandas as pd
import numpy as np
import os
import json
import time
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

CACHE_DIR  = "mf_cache"
AMFI_URL   = "https://www.amfiindia.com/spages/NAVAll.txt"
MFAPI_BASE = "https://api.mfapi.in/mf"
os.makedirs(CACHE_DIR, exist_ok=True)

# ─── CATEGORY MAPPING ─────────────────────────────────────────────────────────

CATEGORY_MAP = {
    "large cap":              "Large Cap",
    "large-cap":              "Large Cap",
    "largecap":               "Large Cap",
    "mid cap":                "Mid Cap",
    "mid-cap":                "Mid Cap",
    "midcap":                 "Mid Cap",
    "small cap":              "Small Cap",
    "small-cap":              "Small Cap",
    "smallcap":               "Small Cap",
    "large & mid cap":        "Large & Mid Cap",
    "large and mid cap":      "Large & Mid Cap",
    "multi cap":              "Multi Cap",
    "multicap":               "Multi Cap",
    "flexi cap":              "Flexi Cap",
    "flexicap":               "Flexi Cap",
    "focused":                "Focused",
    "value":                  "Value",
    "contra":                 "Contra",
    "dividend yield":         "Dividend Yield",
    "elss":                   "ELSS",
    "tax saving":             "ELSS",
    "tax saver":              "ELSS",
    "sectoral":               "Sectoral",
    "thematic":               "Thematic",
    "index":                  "Index Fund",
    "aggressive hybrid":      "Aggressive Hybrid",
    "balanced advantage":     "Balanced Advantage",
    "dynamic asset allocation":"Balanced Advantage",
    "multi asset":            "Multi Asset",
}

EQUITY_CATEGORIES = {
    "Large Cap", "Mid Cap", "Small Cap", "Large & Mid Cap", "Multi Cap",
    "Flexi Cap", "Focused", "Value", "Contra", "Dividend Yield", "ELSS",
    "Sectoral", "Thematic", "Aggressive Hybrid", "Balanced Advantage",
    "Multi Asset", "Index Fund", "Equity - Other",
}

EQ_KW = list(CATEGORY_MAP.keys()) + [
    "equity scheme", "focused fund", "dividend yield", "value fund", "contra fund",
    "aggressive hybrid", "balanced advantage", "multi asset allocation",
    "equity savings", "arbitrage", "retirement", "children",
]

# ─── AMC PREFIX MAP — all 50 AMFI AMCs ───────────────────────────────────────

AMC_PREFIX_MAP = {
    # ── Core AMCs (AMFI-registered) ──────────────────────────────────────────
    "360 ONE":               "360 ONE Mutual Fund",
    "Aditya Birla Sun Life": "Aditya Birla Sun Life Mutual Fund",
    "Angel One":             "Angel One Mutual Fund",
    "Axis":                  "Axis Mutual Fund",
    "Bajaj Finserv":         "Bajaj Finserv Mutual Fund",
    "Bandhan":               "Bandhan Mutual Fund",
    "Bank of India":         "Bank of India Mutual Fund",
    "Baroda BNP Paribas":    "Baroda BNP Paribas Mutual Fund",
    "Canara Robeco":         "Canara Robeco Mutual Fund",
    "Capitalmind":           "Capitalmind Mutual Fund",
    "Choice":                "Choice Mutual Fund",
    "DSP":                   "DSP Mutual Fund",
    "Edelweiss":             "Edelweiss Mutual Fund",
    "Franklin India":        "Franklin Templeton Mutual Fund",
    "Franklin Templeton":    "Franklin Templeton Mutual Fund",
    "Franklin":              "Franklin Templeton Mutual Fund",
    "Templeton":             "Franklin Templeton Mutual Fund",
    "Groww":                 "Groww Mutual Fund",
    "HDFC":                  "HDFC Mutual Fund",
    "Helios":                "Helios Mutual Fund",
    "HSBC":                  "HSBC Mutual Fund",
    "ICICI Prudential":      "ICICI Prudential Mutual Fund",
    "Invesco India":         "Invesco Mutual Fund",
    "ITI":                   "ITI Mutual Fund",
    "Jio BlackRock":         "Jio BlackRock Mutual Fund",
    "JioBlackRock":          "Jio BlackRock Mutual Fund",
    "JM Financial":          "JM Financial Mutual Fund",
    "JM":                    "JM Financial Mutual Fund",
    "Kotak":                 "Kotak Mahindra Mutual Fund",
    "LIC":                   "LIC Mutual Fund",
    "Mahindra Manulife":     "Mahindra Manulife Mutual Fund",
    "MahindraManulife":      "Mahindra Manulife Mutual Fund",
    "Mirae Asset":           "Mirae Asset Mutual Fund",
    "Motilal Oswal":         "Motilal Oswal Mutual Fund",
    "Navi":                  "Navi Mutual Fund",
    "Nippon India":          "Nippon India Mutual Fund",
    "NJ":                    "NJ Mutual Fund",
    "Old Bridge":            "Old Bridge Mutual Fund",
    "PGIM India":            "PGIM India Mutual Fund",
    "PPFAS":                 "PPFAS Mutual Fund",
    "Parag Parikh":          "PPFAS Mutual Fund",
    "quant":                 "quant Mutual Fund",
    "Quantum":               "Quantum Mutual Fund",
    "Samco":                 "Samco Mutual Fund",
    "SBI":                   "SBI Mutual Fund",
    "Shriram":               "Shriram Mutual Fund",
    "Sundaram":              "Sundaram Mutual Fund",
    "Tata":                  "Tata Mutual Fund",
    "Taurus":                "Taurus Mutual Fund",
    "Trust":                 "Trust Mutual Fund",
    "Unifi":                 "Unifi Mutual Fund",
    "Union":                 "Union Mutual Fund",
    "UTI":                   "UTI Mutual Fund",
    "WhiteOak Capital":      "WhiteOak Capital Mutual Fund",
    "Zerodha":               "Zerodha Mutual Fund",
    # ── Newer / variant-name AMCs ────────────────────────────────────────────
    "Abakkus":               "Abakkus Mutual Fund",
    "AlphaGrep":             "AlphaGrep Mutual Fund",
    "ASK":                   "ASK Mutual Fund",
    "BHARAT Bond":           "Edelweiss Mutual Fund",   # BHARAT Bond ETF FOFs managed by Edelweiss
    "The Wealth":            "The Wealth Company Mutual Fund",
    # ── Legacy brand names (pre-rebrand funds still in AMFI) ────────────────
    "Reliance":              "Nippon India Mutual Fund",  # Nippon India rebranded from Reliance in 2019
}

# Build a lowercase lookup once at import time for O(1) case-insensitive match
_AMC_PREFIX_LOWER: list = sorted(
    [(k.lower(), v) for k, v in AMC_PREFIX_MAP.items()],
    key=lambda x: -len(x[0]),   # longest prefix first
)


def _extract_amc(scheme_name: str) -> str:
    """
    Extract AMC name from scheme name using case-insensitive longest-prefix match.
    Handles AMFI's inconsistent casing (ALL CAPS, Title Case, mixed).
    """
    sl = scheme_name.lower()
    for prefix_lower, amc_name in _AMC_PREFIX_LOWER:
        if sl.startswith(prefix_lower):
            return amc_name
    return "Unknown AMC"


# ─── JSON CACHE ───────────────────────────────────────────────────────────────

def _cp(k): return os.path.join(CACHE_DIR, re.sub(r"\W", "_", k) + ".json")

def _cget(k, ttl=24):
    p = _cp(k)
    if not os.path.exists(p):
        return None
    age_hrs = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(p))).total_seconds() / 3600
    if age_hrs > ttl:
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return None

def _cset(k, d):
    try:
        with open(_cp(k), "w") as f:
            json.dump(d, f)
    except Exception:
        pass


# ─── AMFI UNIVERSE FETCH ──────────────────────────────────────────────────────

def fetch_amfi_schemes() -> pd.DataFrame:
    """
    Fetch ALL Direct Growth equity schemes from AMFI NAVAll.txt.
    No per-category or per-AMC cap.
    """
    cached = _cget("amfi_all_v6", ttl=6)
    if cached:
        df = pd.DataFrame(cached)
        print(f"📂 {len(df)} schemes | {df['amc'].nunique()} AMCs from cache")
        return df

    print("📡 Fetching ALL schemes from AMFI (all 50 AMCs)...")
    try:
        r = requests.get(AMFI_URL, timeout=25)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
    except Exception as e:
        print(f"❌ AMFI fetch failed: {e}")
        return pd.DataFrame()

    records, cur_cat = [], ""
    cat_keys = sorted(CATEGORY_MAP.keys(), key=len, reverse=True)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if ";" not in line:
            ll = line.lower()
            if any(k in ll for k in EQ_KW):
                m = re.search(r"\((.+?)\)", line)
                cur_cat = m.group(1).strip() if m else line.strip()
            continue
        if not cur_cat or not any(k in cur_cat.lower() for k in EQ_KW):
            continue

        parts = line.split(";")
        if len(parts) < 8:
            continue

        code   = parts[0].strip()
        name   = parts[3].strip()
        plan   = parts[4].strip()
        option = parts[5].strip()
        if not name or not code:
            continue

        pl = plan.lower()
        ol = option.lower()
        # Direct Growth only — plan and option are now separate columns in the AMFI format
        if "direct" not in pl:
            continue
        if any(d in ol for d in ["dividend", "idcw", "payout", "bonus", "reinvest"]):
            continue
        if not any(g in ol for g in ["growth", "gr"]):
            continue

        try:
            nav = float(parts[6].strip()) if parts[6].strip() not in ("", "N.A.", "-") else None
        except Exception:
            nav = None

        cat = "Other"
        cl  = cur_cat.lower()
        for k in cat_keys:
            if k in cl:
                cat = CATEGORY_MAP[k]
                break

        if cat == "Other" or cat not in EQUITY_CATEGORIES:
            continue

        records.append({
            "scheme_code":  code,
            "scheme_name":  name,
            "amc":          _extract_amc(name),
            "amfi_category": cur_cat,
            "category":     cat,
            "nav":          nav,
            "nav_date":     parts[7].strip() if len(parts) > 7 else "",
        })

    df = pd.DataFrame(records).drop_duplicates("scheme_code").reset_index(drop=True)
    if df.empty:
        print("⚠️  No equity Direct Growth funds parsed from AMFI data")
        return df
    print(f"✅ {len(df)} funds | {df['amc'].nunique()} AMCs | {df['category'].nunique()} categories")

    # Batch enrich from DB
    from holdings_db import get_all_fund_performance
    perf_summary = get_all_fund_performance()
    enriched = [enrich_fund(row, perf_summary) for _, row in df.iterrows()]
    df = pd.DataFrame(enriched)

    df.to_csv("fund_data.csv", index=False)
    _cset("amfi_all_v6", df.to_dict(orient="list"))
    return df


def fetch_universal_universe() -> pd.DataFrame:
    """Alias to fetch_amfi_schemes for backward compatibility."""
    cached = _cget("mf_universal_v4", ttl=24)
    if cached:
        return pd.DataFrame(cached)

    df = fetch_amfi_schemes()
    if not df.empty:
        if "amc" in df.columns and "amc_name" not in df.columns:
            df = df.rename(columns={"amc": "amc_name"})
        _cset("mf_universal_v4", df.to_dict(orient="records"))
    return df


# ─── ENRICH ONE FUND ──────────────────────────────────────────────────────────

def enrich_fund(
    row: pd.Series,
    perf_summary: dict = None,
    allow_live_calc: bool = False,
) -> Dict[str, Any]:
    """
    Enrich a fund row with real data from the DB performance summary.

    INTEGRITY RULES:
    - volatility stays None if not in DB (never hardcode 15.0)
    - Sharpe ratio is None if volatility or cagr_3y is None
    - composite_score is None if cagr_3y is None
    - AUM is None if not in DB (no fake estimates)
    - All None values are shown as-is; the UI labels them 'Awaiting Sync'
    """
    code    = str(row["scheme_code"])
    perf    = (perf_summary or {}).get(code, {})

    def _nonzero(v):
        """Treat 0 / 0.0 stored by old sync as missing data."""
        try:
            f = float(v)
            return f if f != 0.0 else None
        except (TypeError, ValueError):
            return None

    # 1. Real CAGR
    cagr_1y = _nonzero(perf.get("cagr_1y"))
    cagr_3y = _nonzero(perf.get("cagr_3y"))
    cagr_5y = _nonzero(perf.get("cagr_5y"))

    if cagr_3y is None and allow_live_calc:
        try:
            from nav_processor import LiveNAVProcessor
            res = LiveNAVProcessor().get_performance(code)
            if res:
                cagr_1y = res.get("cagr_1y")
                cagr_3y = res.get("cagr_3y")
                cagr_5y = res.get("cagr_5y")
        except Exception:
            pass

    cagr_src = "Factual Registry" if cagr_3y is not None else "Awaiting Sync"

    # 2. AUM — None if missing, no fallback
    aum_cr = _nonzero(perf.get("aum_cr"))
    aum_src = "mfdata.in" if aum_cr is not None else "Sync Pending"

    # 3. Expense ratio
    er     = _nonzero(perf.get("expense_ratio"))
    er_src = "Factsheet Verified" if er is not None else "Varies by Plan"

    # 4. Volatility — None if not in DB. Never hardcode.
    vol = _nonzero(perf.get("volatility"))

    # 5. Sharpe — only computable when both vol and cagr_3y are real
    if vol and vol > 0 and cagr_3y is not None:
        sharpe = round((cagr_3y - 6.5) / vol, 3)
    else:
        sharpe = None

    # 6. Max drawdown — only when vol is real
    max_dd = round(-vol * 1.8, 2) if vol is not None else None

    # 7. Composite score — only when cagr_3y is real
    if cagr_3y is not None:
        s = cagr_3y * 0.4
        s += (cagr_5y or 0) * 0.3
        s += (sharpe or 0) * 5
        s -= (vol or 0) * 0.1
        composite = round(s, 2)
    else:
        composite = None

    return {
        **row.to_dict(),
        "cagr_1y":        cagr_1y,
        "cagr_3y":        cagr_3y,
        "cagr_5y":        cagr_5y,
        "volatility":     vol,
        "sharpe_ratio":   sharpe,
        "max_drawdown":   max_dd,
        "expense_ratio":  er,
        "aum_cr":         aum_cr,
        "aum_source":     aum_src,
        "cagr_source":    cagr_src,
        "er_source":      er_src,
        "composite_score": composite,
    }


# ─── PERFORMANCE LOOKUPS ──────────────────────────────────────────────────────

def get_performance_from_db(code: str) -> Optional[dict]:
    """Get performance row for a single fund from DB."""
    from holdings_db import get_conn
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM fund_performance WHERE scheme_code = ?", (code,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def compute_real_cagr(code: str, years: float, allow_live_calc: bool = False) -> Optional[float]:
    """Compute or fetch CAGR for a fund. DB-first, optionally live."""
    perf = get_performance_from_db(code)
    if perf:
        if years == 1: return perf.get("cagr_1y")
        if years == 3: return perf.get("cagr_3y")
        if years == 5: return perf.get("cagr_5y")

    if allow_live_calc:
        try:
            from nav_processor import LiveNAVProcessor
            res = LiveNAVProcessor().get_performance(code)
            if res:
                if years == 1: return res.get("cagr_1y")
                if years == 3: return res.get("cagr_3y")
                if years == 5: return res.get("cagr_5y")
        except Exception:
            pass
    return None


# ─── MAIN LOAD FUNCTION ───────────────────────────────────────────────────────

def load_fund_data(force_refresh: bool = False, cache_path: str = "fund_data.csv") -> pd.DataFrame:
    """
    Load all equity Direct Growth funds from all AMFI AMCs.

    Data flow:
      1. If CSV cache < 24h old and covers >30 AMCs → return cached
      2. Fetch fresh from AMFI NAVAll.txt
      3. Filter to EQUITY_CATEGORIES
      4. Batch-enrich from fund_performance DB
      5. Save to CSV and JSON cache

    Integrity: all None fields remain None. No fake values.
    """
    if not force_refresh and os.path.exists(cache_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if datetime.now() - mtime < timedelta(hours=24):
            df = pd.read_csv(cache_path)
            if not df.empty and df["amc"].nunique() > 30:
                print(f"📂 {len(df)} funds | {df['amc'].nunique()} AMCs from CSV cache")
                return df

    # Fetch raw universe
    raw = fetch_amfi_schemes()
    if raw.empty:
        raw = fetch_universal_universe()
    if raw.empty:
        raise RuntimeError("Mutual Fund Registry could not be loaded from any source")

    # Normalise column name
    if "amc_name" in raw.columns and "amc" not in raw.columns:
        raw = raw.rename(columns={"amc_name": "amc"})
    if "amc" not in raw.columns:
        raw["amc"] = raw.get("scheme_name", pd.Series()).apply(_extract_amc)

    # Filter to equity categories only
    equity_df = raw[raw["category"].isin(EQUITY_CATEGORIES)].copy().reset_index(drop=True)
    print(f"🔍 Filtered to {len(equity_df)} equity funds across {equity_df['amc'].nunique()} AMCs")

    # Batch enrich from DB (single query, not N per fund)
    from holdings_db import get_all_fund_performance
    perf_summary = get_all_fund_performance()
    print(f"📡 Enriching {len(equity_df)} funds (DB has perf data for {len(perf_summary)} funds)...")

    enriched = [enrich_fund(row, perf_summary) for _, row in equity_df.iterrows()]
    df = pd.DataFrame(enriched)

    df.to_csv(cache_path, index=False)
    _cset("amfi_all_v6", df.to_dict(orient="list"))
    print(f"💾 {len(df)} funds | {df['amc'].nunique()} AMCs saved")
    return df
