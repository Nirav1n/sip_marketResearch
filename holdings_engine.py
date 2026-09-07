"""
holdings_engine.py v4
Serves holdings data to the dashboard.
Priority: real DB data → fallback to representative data.
Shows data source clearly on every call.
"""

import pandas as pd
import random
import re
import os
import json
import time
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from mf_api_client import MfDataClient


CACHE_DIR = "mf_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Initialize API Client
api_client = MfDataClient()


def get_dynamic_quarters(n: int = 8) -> List[str]:
    now = datetime.now()
    year, month = now.year, now.month
    q = (month - 1) // 3 + 1
    fy_year = year if month >= 4 else year - 1
    quarters = []
    for _ in range(n):
        quarters.append(f"Q{q} FY{str(fy_year)[2:]}")
        q -= 1
        if q == 0:
            q = 4
            fy_year -= 1
    return list(reversed(quarters))

QUARTERS = get_dynamic_quarters(8)

NSE_SECTOR = {
    "RELIANCE":"Energy","HDFCBANK":"Banking","INFY":"IT","ICICIBANK":"Banking",
    "TCS":"IT","LT":"Infrastructure","AXISBANK":"Banking","KOTAKBANK":"Banking",
    "BAJFINANCE":"NBFC","ASIANPAINT":"Consumer","HINDUNILVR":"FMCG",
    "MARUTI":"Auto","SUNPHARMA":"Pharma","TITAN":"Consumer","WIPRO":"IT",
    "HCLTECH":"IT","TATAMOTORS":"Auto","ADANIPORTS":"Infrastructure",
    "POWERGRID":"Energy","NTPC":"Energy","BHARTIARTL":"Telecom","ITC":"FMCG",
    "SBIN":"Banking","NESTLEIND":"FMCG","BAJAJ-AUTO":"Auto","TECHM":"IT",
    "JSWSTEEL":"Metals","TATASTEEL":"Metals","DRREDDY":"Pharma","CIPLA":"Pharma",
    "DIVISLAB":"Pharma","ULTRACEMCO":"Materials","HINDALCO":"Metals",
    "INDUSINDBK":"Banking","EICHERMOT":"Auto","BRITANNIA":"FMCG",
    "PERSISTENT":"IT","COFORGE":"IT","MPHASIS":"IT","MAXHEALTH":"Healthcare",
    "FORTIS":"Healthcare","AUBANK":"Banking","FEDERALBNK":"Banking",
    "CHOLAFIN":"NBFC","MUTHOOTFIN":"NBFC","POLYCAB":"Electricals",
    "ABB":"Capital Goods","SIEMENS":"Capital Goods","CUMMINSIND":"Capital Goods",
    "TRENT":"Retail","DMART":"Retail","INDHOTEL":"Hospitality","ZOMATO":"Consumer Tech",
    "PIIND":"Agrochemicals","DEEPAKNTR":"Chemicals","BANKBARODA":"Banking",
    "AUROPHARMA":"Pharma","LUPIN":"Pharma","BHEL":"Infrastructure",
    "DABUR":"FMCG","MARICO":"FMCG","ADANIENT":"Conglomerate","TATAPOWER":"Energy",
    "HAVELLS":"Electricals","M&M":"Auto","ASHOKLEY":"Auto","HDFCLIFE":"Insurance",
    "SBILIFE":"Insurance","ICICIGI":"Insurance","INFOEDGE":"Consumer Tech",
    "DELHIVERY":"Logistics","IRFC":"NBFC","RECLTD":"NBFC","PFC":"NBFC",
}

UNIVERSE = {
    "Large Cap": [
        ("Reliance Industries","RELIANCE"),("HDFC Bank","HDFCBANK"),
        ("Infosys","INFY"),("ICICI Bank","ICICIBANK"),("TCS","TCS"),
        ("Larsen & Toubro","LT"),("Axis Bank","AXISBANK"),("Kotak Mahindra Bank","KOTAKBANK"),
        ("Bajaj Finance","BAJFINANCE"),("Asian Paints","ASIANPAINT"),
        ("HUL","HINDUNILVR"),("Maruti Suzuki","MARUTI"),("Sun Pharma","SUNPHARMA"),
        ("Titan Company","TITAN"),("Wipro","WIPRO"),("HCL Technologies","HCLTECH"),
        ("Tata Motors","TATAMOTORS"),("Adani Ports","ADANIPORTS"),
        ("Power Grid","POWERGRID"),("NTPC","NTPC"),("Bharti Airtel","BHARTIARTL"),
        ("ITC","ITC"),("SBI","SBIN"),("Nestle India","NESTLEIND"),
        ("Bajaj Auto","BAJAJ-AUTO"),("Tech Mahindra","TECHM"),("JSW Steel","JSWSTEEL"),
        ("Dr Reddy's","DRREDDY"),("Cipla","CIPLA"),("Divis Labs","DIVISLAB"),
        ("UltraTech Cement","ULTRACEMCO"),("Hindalco","HINDALCO"),
        ("IndusInd Bank","INDUSINDBK"),("Eicher Motors","EICHERMOT"),
        ("Britannia","BRITANNIA"),("Coal India","COALINDIA"),
        ("Bank of Baroda","BANKBARODA"),("Adani Enterprises","ADANIENT"),
        ("Tata Power","TATAPOWER"),("Havells India","HAVELLS"),
    ],
    "Mid Cap": [
        ("Persistent Systems","PERSISTENT"),("Coforge","COFORGE"),("Mphasis","MPHASIS"),
        ("Max Healthcare","MAXHEALTH"),("Fortis Healthcare","FORTIS"),
        ("AU Small Finance Bank","AUBANK"),("Federal Bank","FEDERALBNK"),
        ("Cholamandalam Finance","CHOLAFIN"),("Muthoot Finance","MUTHOOTFIN"),
        ("Polycab India","POLYCAB"),("ABB India","ABB"),("Siemens","SIEMENS"),
        ("Cummins India","CUMMINSIND"),("Trent","TRENT"),("Avenue Supermarts","DMART"),
        ("Indian Hotels","INDHOTEL"),("Zomato","ZOMATO"),("PI Industries","PIIND"),
        ("Deepak Nitrite","DEEPAKNTR"),("Lupin","LUPIN"),("Aurobindo Pharma","AUROPHARMA"),
        ("Marico","MARICO"),("Dabur India","DABUR"),("Info Edge","INFOEDGE"),
        ("Ashok Leyland","ASHOKLEY"),
    ],
    "Small Cap": [
        ("KPIT Technologies","KPITTECH"),("Intellect Design","INTELLECT"),
        ("Newgen Software","NEWGEN"),("IIFL Finance","IIFL"),
        ("Manappuram Finance","MANAPPURAM"),("Ujjivan SFB","UJJIVANSFB"),
        ("JK Cement","JKCEMENT"),("TCI Express","TCIEXP"),
        ("KPR Mill","KPRMILL"),("Sapphire Foods","SAPPHIRE"),
        ("Clean Science","CLEANSCIENCE"),("Balrampur Chini","BALRAMCHIN"),
        ("Delhivery","DELHIVERY"),("Birlasoft","BSOFT"),
    ],
}
from data_fetcher import EQUITY_CATEGORIES, CATEGORY_MAP

for cat in EQUITY_CATEGORIES:
    if cat not in UNIVERSE:
        UNIVERSE[cat] = UNIVERSE["Large Cap"][:20]


def _has_real_data(selected_categories: List[str]) -> tuple[bool, str]:
    """Check if the DB has real holdings data for the latest month.
    
    NOTE: We intentionally do NOT filter by category here — the holdings table
    has no category column and fund_metadata may be empty. If any real data
    exists we use it and let the user see what's actually scraped.
    """
    try:
        from holdings_db import get_available_months
        months = get_available_months()
        if not months:
            return False, ""
        latest = months[0]
        # Just check if ANY real data exists in holdings table
        return True, latest
    except Exception:
        pass
    return False, ""


def get_data_status(categories: List[str]) -> Dict[str, Any]:
    """Return data health metrics for selected categories."""
    from data_fetcher import load_fund_data
    from holdings_db import get_available_months, get_conn
    
    try:
        # 1. Use the SAME registry as the 'All Funds' page for parity
        # BATCH MATCHING: Case-insensitive & Trim-safe
        master_df = load_fund_data()
        selected_upper = [c.upper().strip() for c in categories]
        target_codes = master_df[master_df["category"].str.upper().str.strip().isin(selected_upper)]["scheme_code"].unique().tolist()
        
        target_len = len(target_codes)
        latest = get_available_months()[0] if get_available_months() else None
        
        conn = get_conn()
        # COUNT REAL DATA (BANNER)
        synced_h = 0
        if target_codes and latest:
            placeholders = ', '.join(['?'] * len(target_codes))
            h_query = f"SELECT COUNT(DISTINCT scheme_code) FROM holdings WHERE disclosure_month = ? AND scheme_code IN ({placeholders})"
            synced_h = conn.execute(h_query, [latest] + target_codes).fetchone()[0]
        
        # 3. Overall stats
        total_rows = conn.execute("SELECT count(*) FROM holdings").fetchone()[0]
        conn.close()
        
        return {
            "has_real_data": synced_h > 0,
            "latest_month": latest,
            "total_funds_in_cat": target_len,
            "synced_funds_in_cat": synced_h,
            "total_holdings": total_rows,
        }
    except Exception as e:
        print(f"⚠️ Status check failed: {e}")
        return {
            "has_real_data": False, 
            "latest_month": "N/A",
            "total_funds_in_cat": 0,
            "synced_funds_in_cat": 0,
            "total_holdings": 0
        }

def build_holdings_data(selected_categories: List[str]) -> pd.DataFrame:
    """
    Returns holdings DataFrame.
    Uses real DB data if available, falls back to representative with clear labelling.
    
    Category filtering is skipped intentionally — the holdings table has no category
    column. Instead we return all scraped data and add a note about data coverage.
    """
    has_real, latest_month = _has_real_data(selected_categories)
    
    # FORCED RE-CHECK: If categories changed, re-query latest for those categories
    if has_real:
        try:
            from data_fetcher import load_fund_data
            from holdings_db import get_holdings

            # Filter holdings to match the 'All Funds' registry (Case-Insensitive)
            master_df = load_fund_data()
            selected_upper = [c.upper().strip() for c in selected_categories]
            target_codes = master_df[master_df["category"].str.upper().str.strip().isin(selected_upper)]["scheme_code"].unique()
            target_codes = [str(c) for c in target_codes if c is not None]

            # Direct code filtering is more robust than category name join
            df = get_holdings(latest_month, categories=selected_categories, scheme_codes=target_codes)
            if not df.empty:
                df["data_source_type"] = "real"
                return df
        except Exception as e:
            print(f"⚠️ DB load failed: {e}")

    # Fallback: representative holdings (no real DB data yet)
    return _build_representative_holdings(selected_categories)


def sync_popular_funds():
    """Sync target funds to bootstrap the DB."""
    # Top ~20 Large Cap, ~20 Mid Cap, etc.
    popular_keywords = ["Bluechip", "Large Cap", "Mid Cap", "Small Cap", "Flexi Cap", "ELSS", "Index"]
    from data_fetcher import fetch_universal_universe
    univ = fetch_universal_universe()
    if univ.empty: return
    
    # Select top 15 from each popular category
    for kw in popular_keywords:
        subset = univ[univ["scheme_name"].str.contains(kw, case=False)].head(15)
        for _, f in subset.iterrows():
            try:
                sync_fund(f["scheme_name"], f.get("scheme_code"))
                time.sleep(1.0)
            except:
                continue

def sync_categories(categories: List[str], limit: int = 10):
    """Sync all missing funds for the selected categories."""
    for cat in categories:
        sync_missing_for_month(cat)

def sync_missing_for_month(category: str, month: Optional[str] = None):
    """Sync holdings for all funds in a category that are missing data for the month."""
    if not month:
        month = datetime.now().strftime("%Y-%m")
    
    from data_fetcher import get_conn
    conn = get_conn()
    
    # Identify funds in this category with NO holdings for this month
    query = """
    SELECT fm.scheme_name, fm.scheme_code
    FROM fund_metadata fm
    LEFT JOIN holdings h ON fm.scheme_code = h.scheme_code AND h.disclosure_month = ?
    WHERE fm.category = ? AND h.id IS NULL
    """
    missing = pd.read_sql_query(query, conn, params=[month, category])
    conn.close()
    
    if missing.empty:
        print(f"✅ Category '{category}' is already fully synced for {month}")
        return
        
    print(f"🔄 Delta Syncing {len(missing)} missing funds for '{category}' in {month}...")
    for _, fund in missing.iterrows():
        try:
            sync_fund(fund["scheme_name"], fund["scheme_code"])
            time.sleep(2.0) # Conservative rate limit for background sync
        except Exception as e:
            print(f"  ⚠️ Failed for {fund['scheme_name']}: {e}")
            continue

def sync_fund_by_name(name: str):
    """Legacy wrapper for backward compatibility."""
    sync_fund(name)

def sync_fund(name: str, code: Optional[str] = None):
    """Search for a fund and sync its metadata and holdings."""
    print(f"🔍 Syncing '{name}'...")
    api_client = MfDataClient()
    
    amfi_code = code
    family_id = None
    
    # 1. Resolve family_id (needed for holdings)
    if amfi_code:
        details = api_client.get_scheme_details(amfi_code)
        if details and "family_id" in details:
            family_id = details["family_id"]
        else:
            results = api_client.search_schemes(name)
            if results: family_id = results[0].get("family_id")
    else:
        results = api_client.search_schemes(name)
        if not results:
            print(f"⚠️ No results found for '{name}'")
            return
        top = results[0]
        amfi_code = top.get("amfi_code")
        family_id = top.get("family_id")

    if not family_id:
        print(f"⚠️ Could not resolve family_id for {name}")
        return

    # 2. Sync Metadata & Performance
    details = api_client.get_scheme_details(amfi_code)
    if details:
        from holdings_db import upsert_fund_metadata, upsert_performance
        
        # Prepare metadata record
        meta_record = {
            "scheme_code": amfi_code,
            "scheme_name": details.get("name"),
            "family_id": family_id,
            "amc_id": details.get("amc_slug"),
            "amc_name": details.get("amc_name"),
            "amfi_category": details.get("category"),
            "category": details.get("category"),
            "nav": details.get("nav"),
            "nav_date": details.get("nav_date"),
            "isin_growth": details.get("isin"),
            "isin_div": None,
        }
        upsert_fund_metadata([meta_record])
        print(f"✅ Metadata synced for {amfi_code}")

        # Performance record — use None for any missing/zero value so the UI
        # shows "Awaiting Sync" rather than a misleading 0.0
        perf = details.get("returns", {})
        aum_raw = details.get("aum")
        aum_val = (float(aum_raw) / 1e7) if aum_raw else None

        def _real(v):
            """Return v if it is a non-zero number, else None."""
            try:
                f = float(v)
                return f if f != 0.0 else None
            except (TypeError, ValueError):
                return None

        perf_record = {
            "scheme_code": amfi_code,
            "cagr_1y":       _real(perf.get("return_1y")),
            "cagr_3y":       _real(perf.get("return_3y")),
            "cagr_5y":       _real(perf.get("return_5y")),
            "cagr_10y":      _real(perf.get("return_inception")),
            "volatility":    _real(details.get("std_dev")),
            "sharpe_ratio":  _real(details.get("sharpe")),
            "max_drawdown":  None,
            "expense_ratio": _real(details.get("expense_ratio")),
            "aum_cr":        aum_val,
            "aum_source": "mfdata.in API",
            "cagr_source": "mfdata.in API",
            "er_source": "mfdata.in API",
            "composite_score": None,
            "nav_data_points": None
        }
        upsert_performance([perf_record])
        print(f"✅ Performance synced for {amfi_code}")

    # 3. Sync Holdings
    holdings_data = api_client.get_family_holdings(family_id)
    if holdings_data:
        from holdings_db import insert_holdings
        equity = holdings_data.get("equity_holdings", [])
        rows = []
        month = holdings_data.get("month", datetime.now().strftime("%Y-%m"))
        amc_slug = details.get("amc_slug") if details else "unknown"
        amc_name = details.get("amc_name") if details else "Unknown AMC"
        
        for h in equity:
            rows.append({
                "disclosure_month": month,
                "amc_id": amc_slug,
                "amc_name": amc_name,
                "scheme_name": details.get("name") if details else name,
                "scheme_code": amfi_code,
                "stock_name": h.get("stock_name"),
                "isin": h.get("isin"),
                "ticker": h.get("ticker"),
                "sector": h.get("sector"),
                "sector_normalised": h.get("sector"),
                "asset_type": "Equity",
                "quantity": h.get("quantity"),
                "market_value_cr": (h.get("market_value") or 0) / 1e7,
                "weight_pct": h.get("weight") or 0.0,
                "rating": None,
                "listing": None,
                "data_source": "mfdata.in API",
            })
        
        if rows:
            insert_holdings(rows)
            print(f"✅ Synced {len(rows)} holdings for {name}")


def _build_representative_holdings(selected_categories: List[str]) -> pd.DataFrame:
    """Representative holdings based on SEBI category mandate."""
    rows = []
    for cat in selected_categories:
        universe = UNIVERSE.get(cat, UNIVERSE["Large Cap"])
        fund_names = [f"Representative {cat} Fund {i+1}" for i in range(8)]
        n = min(30, len(universe))

        for fund in fund_names:
            seed = abs(hash(fund + cat)) % 100000
            rng = random.Random(seed)
            pool = universe.copy(); rng.shuffle(pool)
            sel = pool[:n]
            raw_w = [10/(1+0.28*i)+rng.uniform(-0.5,0.5) for i in range(n)]
            raw_w = [max(0.3,w) for w in raw_w]
            tot = sum(raw_w)
            for (sname,ticker),weight in zip(sel,[round(w/tot*100,2) for w in raw_w]):
                rows.append({
                    "fund_name": fund, "scheme_name": fund,
                    "scheme_code": None, "category": cat,
                    "amc_id": "representative", "amc_name": "Representative",
                    "stock_name": sname, "ticker": ticker,
                    "sector_normalised": NSE_SECTOR.get(ticker, "Other"),
                    "sector": NSE_SECTOR.get(ticker, "Other"),
                    "weight_pct": weight,
                    "data_source": f"Representative data (SEBI {cat} mandate)",
                    "data_source_type": "representative",
                    "disclosure_month": datetime.now().strftime("%Y-%m"),
                })
    return pd.DataFrame(rows)


def build_stock_conviction_table(holdings_df: pd.DataFrame, selected_categories: List[str]) -> pd.DataFrame:
    # 1. Join holdings with AUM from fund_performance
    from holdings_db import get_conn
    conn = get_conn()
    perf = pd.read_sql_query("SELECT scheme_code, aum_cr FROM fund_performance", conn)
    conn.close()
    
    # Robustness: ensure we have columns or fill with Unknown
    df = holdings_df.copy()
    df["stock_name"] = df["stock_name"].fillna("Unknown Stock")
    df["ticker"] = df["ticker"].fillna("")
    df["sector_normalised"] = df["sector_normalised"].fillna(df.get("sector", "Other"))
    df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0)
    
    # Merge AUM into holdings
    df = df.merge(perf, on="scheme_code", how="left")
    df["aum_cr"] = df["aum_cr"].fillna(500.0) # Conservative fallback for missing AUM
    
    fn_col = "scheme_name" if "scheme_name" in df.columns else "fund_name"
    total_funds = df[fn_col].nunique()
    total_aum = df.drop_duplicates(fn_col)["aum_cr"].sum()

    # Aggregation with AUM weighting
    grp = df.groupby(["stock_name","ticker","sector_normalised"]).agg(
        fund_count=(fn_col,"nunique"),
        aum_sum=("aum_cr", "sum"),
        categories=("category", lambda x: " | ".join(sorted(x.dropna().unique()))),
        avg_weight=("weight_pct","mean"),
        max_weight=("weight_pct","max"),
        amc_count=("amc_id","nunique") if "amc_id" in df.columns else ("stock_name","count"),
    ).reset_index()

    grp["funds_pct"] = (grp["fund_count"]/total_funds*100).round(1) if total_funds > 0 else 0
    grp["aum_pct"] = (grp["aum_sum"]/total_aum*100).round(1) if total_aum > 0 else 0
    
    # Overwrite conviction with AUM-Weighted score
    mp = total_funds * grp["avg_weight"].max()
    grp["conviction_score"] = ((grp["fund_count"]*grp["avg_weight"])/mp*100).round(1) if mp>0 else grp["funds_pct"]
    
    # Boost factor based on AUM influence
    grp["conviction_score"] = (grp["conviction_score"] * 0.7 + grp["aum_pct"] * 0.3).round(1)
    
    grp["conviction_label"] = grp["conviction_score"].apply(
        lambda p: "🔴 Universal" if p>=80 else ("🟠 High" if p>=50 else ("🟡 Moderate" if p>=25 else "🟢 Selective"))
    )
    grp = grp.sort_values("fund_count",ascending=False).reset_index(drop=True)
    grp.index = grp.index+1
    grp = grp.reset_index().rename(columns={"index":"rank"})
    grp["sector"] = grp["sector_normalised"]
    return grp


def build_rotation_data(selected_categories: List[str]) -> pd.DataFrame:
    """Build rotation — uses real DB data if available."""
    try:
        from holdings_db import get_rotation_data, get_available_months
        months = get_available_months()
        if len(months) >= 2:
            return get_rotation_data(selected_categories)
    except Exception:
        pass

    # Fallback: modelled rotation
    holdings_df = build_holdings_data(selected_categories)
    if holdings_df.empty: return pd.DataFrame()
    conviction = build_stock_conviction_table(holdings_df, selected_categories)
    if conviction.empty: return pd.DataFrame()

    rows = []
    quarters = get_dynamic_quarters(8)
    fn_col = "scheme_name" if "scheme_name" in holdings_df.columns else "fund_name"
    total_funds = holdings_df[fn_col].nunique()
    top_stocks = conviction.head(30)

    for _, stock in top_stocks.iterrows():
        base = int(stock["fund_count"])
        seed = abs(hash(str(stock["ticker"]))) % 10000
        rng = random.Random(seed)
        start = max(1, base-rng.randint(2,4)); prev = start
        for q in quarters:
            drift = (base-prev)/(len(quarters)+1)
            count = max(1,min(total_funds,round(prev+drift+rng.uniform(-0.8,0.8))))
            rows.append({
                "quarter":q,"stock_name":stock["stock_name"],
                "ticker":stock["ticker"],"sector":stock["sector"],
                "category":selected_categories[0] if selected_categories else "Mixed",
                "fund_count":count,"fund_pct":round(count/total_funds*100,1),
            })
            prev=count

    df=pd.DataFrame(rows).drop_duplicates(["quarter","ticker"])
    fq,lq=quarters[0],quarters[-1]
    first=df[df["quarter"]==fq][["ticker","fund_count"]].rename(columns={"fund_count":"start"})
    last=df[df["quarter"]==lq][["ticker","fund_count"]].rename(columns={"fund_count":"end"})
    trend=first.merge(last,on="ticker")
    trend["trend"]=trend["end"]-trend["start"]
    trend["trend_label"]=trend["trend"].apply(
        lambda x:"📈 Accumulating" if x>=3 else("📉 Distributing" if x<=-3 else "➡️ Stable"))
    return df.merge(trend[["ticker","trend","trend_label"]],on="ticker",how="left")