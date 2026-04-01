"""
data_fetcher.py
Fetches mutual fund data from AMFI (free, official, no API key needed)
and enriches it with mock risk metrics (replace with Value Research API if you have access)
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import random
import time

# ─── AMFI NAV DATA (Official, Free) ───────────────────────────────────────────

AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# Categories we care about (mapped from AMFI scheme category strings)
TARGET_CATEGORIES = {
    "large cap": "Large Cap",
    "mid cap": "Mid Cap",
    "small cap": "Small Cap",
    "sectoral": "Sectoral/Thematic",
    "thematic": "Sectoral/Thematic",
    "flexi cap": "Flexi Cap",
    "multi cap": "Multi Cap",
}

# ─── SECTOR KEYWORDS for classification ───────────────────────────────────────
SECTOR_KEYWORDS = {
    "Technology": ["tech", "it", "digital", "infotech", "software"],
    "Banking & Finance": ["bank", "finance", "financial", "nifty bank", "bfsi"],
    "Healthcare": ["pharma", "health", "hospital", "medical"],
    "Infrastructure": ["infra", "infrastructure", "realty", "real estate"],
    "Energy": ["energy", "power", "oil", "gas", "petroleum"],
    "FMCG": ["fmcg", "consumption", "consumer"],
    "Auto": ["auto", "automobile", "vehicle", "ev"],
    "Defence": ["defence", "psu", "government"],
}


def fetch_amfi_data() -> pd.DataFrame:
    """
    Pulls NAV data from AMFI. Returns raw dataframe.
    AMFI format: Scheme Code;ISIN Div Payout;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
    """
    print("📡 Fetching data from AMFI India...")
    try:
        resp = requests.get(AMFI_URL, timeout=15)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")

        records = []
        current_amc = ""

        for line in lines:
            line = line.strip()
            if not line:
                continue
            # AMC header lines don't have semicolons
            if ";" not in line:
                current_amc = line
                continue
            parts = line.split(";")
            if len(parts) < 6:
                continue
            try:
                records.append({
                    "scheme_code": parts[0].strip(),
                    "scheme_name": parts[3].strip(),
                    "nav": float(parts[4].strip()) if parts[4].strip() not in ("", "N.A.") else None,
                    "nav_date": parts[5].strip(),
                    "amc": current_amc.strip(),
                })
            except (ValueError, IndexError):
                continue

        df = pd.DataFrame(records)
        df = df.dropna(subset=["nav"])
        print(f"✅ Fetched {len(df)} schemes from AMFI")
        return df

    except Exception as e:
        print(f"❌ AMFI fetch failed: {e}")
        return pd.DataFrame()


def classify_category(scheme_name: str) -> str:
    """Classify fund category from scheme name."""
    name_lower = scheme_name.lower()
    for keyword, category in TARGET_CATEGORIES.items():
        if keyword in name_lower:
            return category
    return "Other"


def classify_sector(scheme_name: str) -> str:
    """For sectoral funds, identify which sector."""
    name_lower = scheme_name.lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        for kw in keywords:
            if kw in name_lower:
                return sector
    return "Diversified"


def enrich_with_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds simulated but realistic metrics.
    In production: replace with Value Research / Morningstar API calls.
    Metrics are seeded per scheme_code so they stay consistent across runs.
    """
    print("⚙️  Enriching with performance & risk metrics...")

    enriched = []
    for _, row in df.iterrows():
        seed = int(row["scheme_code"]) if row["scheme_code"].isdigit() else hash(row["scheme_code"]) % 10000
        rng = random.Random(seed)

        category = row["category"]

        # Return ranges by category (realistic Indian MF ranges)
        return_ranges = {
            "Large Cap":         {"1y": (8, 22),  "3y": (10, 18), "5y": (11, 16)},
            "Mid Cap":           {"1y": (10, 35),  "3y": (14, 28), "5y": (13, 24)},
            "Small Cap":         {"1y": (5, 45),   "3y": (12, 32), "5y": (12, 28)},
            "Sectoral/Thematic": {"1y": (5, 55),   "3y": (10, 40), "5y": (8, 35)},
            "Flexi Cap":         {"1y": (9, 24),   "3y": (12, 20), "5y": (12, 18)},
            "Multi Cap":         {"1y": (9, 26),   "3y": (12, 22), "5y": (11, 19)},
        }.get(category, {"1y": (5, 20), "3y": (8, 15), "5y": (8, 14)})

        cagr_1y = round(rng.uniform(*return_ranges["1y"]), 2)
        cagr_3y = round(rng.uniform(*return_ranges["3y"]), 2)
        cagr_5y = round(rng.uniform(*return_ranges["5y"]), 2)

        volatility = round(rng.uniform(8, 28), 2)
        sharpe = round(rng.uniform(0.4, 2.2), 2)
        max_drawdown = round(rng.uniform(-35, -5), 2)
        expense_ratio = round(rng.uniform(0.1, 2.5), 2)
        aum_cr = round(rng.uniform(100, 80000), 0)

        # Composite score: reward returns, penalize risk & expense
        composite = round(
            (cagr_3y * 0.4) + (cagr_5y * 0.3) + (sharpe * 5) - (abs(max_drawdown) * 0.3) - (expense_ratio * 2),
            2
        )

        enriched.append({
            **row,
            "cagr_1y": cagr_1y,
            "cagr_3y": cagr_3y,
            "cagr_5y": cagr_5y,
            "volatility": volatility,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_drawdown,
            "expense_ratio": expense_ratio,
            "aum_cr": aum_cr,
            "sector": classify_sector(row["scheme_name"]) if category == "Sectoral/Thematic" else "-",
            "composite_score": composite,
        })

    return pd.DataFrame(enriched)


def load_fund_data(force_refresh: bool = False, cache_path: str = "fund_data.csv") -> pd.DataFrame:
    """
    Main entry point. Returns enriched fund dataframe.
    Caches to CSV for 24 hours to avoid hammering AMFI.
    """
    # Check cache
    if not force_refresh and os.path.exists(cache_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if datetime.now() - mtime < timedelta(hours=24):
            print(f"📂 Loading from cache ({cache_path})")
            df = pd.read_csv(cache_path)
            print(f"✅ Loaded {len(df)} funds from cache")
            return df

    # Fresh fetch
    raw = fetch_amfi_data()
    if raw.empty:
        raise RuntimeError("Could not fetch data from AMFI. Check internet connection.")

    # Classify categories
    raw["category"] = raw["scheme_name"].apply(classify_category)

    # Filter to our target categories only
    target_cats = list(TARGET_CATEGORIES.values())
    filtered = raw[raw["category"].isin(target_cats)].copy()
    print(f"🔍 Filtered to {len(filtered)} funds in target categories")

    # Take top N per category (to keep dataset manageable)
    filtered = filtered.groupby("category").head(60).reset_index(drop=True)

    # Enrich
    enriched = enrich_with_metrics(filtered)

    # Save cache
    enriched.to_csv(cache_path, index=False)
    print(f"💾 Saved to {cache_path}")

    return enriched


if __name__ == "__main__":
    df = load_fund_data(force_refresh=True)
    print(df[["scheme_name", "category", "cagr_1y", "cagr_3y", "composite_score"]].head(10))
