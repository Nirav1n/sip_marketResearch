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
import requests
from datetime import datetime, timedelta
from typing import List, Dict

CACHE_DIR = "mf_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


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
for cat in ["Large & Mid Cap","Multi Cap","Flexi Cap","Focused","Value","Contra",
            "ELSS","Sectoral","Thematic","Aggressive Hybrid","Balanced Advantage",
            "Multi Asset","Index Fund","Dividend Yield"]:
    UNIVERSE[cat] = UNIVERSE["Large Cap"][:20]


def _has_real_data(selected_categories: List[str]) -> tuple[bool, str]:
    """Check if the DB has real holdings data for the latest month.
    
    NOTE: We intentionally do NOT filter by category here — the holdings table
    has no category column and fund_metadata may be empty. If any real data
    exists we use it and let the user see what's actually scraped.
    """
    try:
        from holdings_db import get_available_months, get_holdings
        months = get_available_months()
        if not months:
            return False, ""
        latest = months[0]
        # Check without category filter — categories aren't stored in holdings
        sample = get_holdings(latest, categories=None)
        if not sample.empty and len(sample) > 10:
            return True, latest
    except Exception:
        pass
    return False, ""


def build_holdings_data(selected_categories: List[str]) -> pd.DataFrame:
    """
    Returns holdings DataFrame.
    Uses real DB data if available, falls back to representative with clear labelling.
    
    Category filtering is skipped intentionally — the holdings table has no category
    column. Instead we return all scraped data and add a note about data coverage.
    """
    has_real, latest_month = _has_real_data(selected_categories)

    if has_real:
        try:
            from holdings_db import get_holdings
            # Do NOT filter by category — fund_metadata may be empty so
            # the join would return 0 rows. Show all real scraped data.
            df = get_holdings(latest_month, categories=None)
            df["data_source_type"] = "real"
            # Tag with a category so conviction table grouping still works
            if "category" not in df.columns:
                df["category"] = df.get("amc_name", df.get("amc_id", "Unknown AMC"))
            print(f"✅ Loaded {len(df)} real holdings from DB ({latest_month})")
            return df
        except Exception as e:
            print(f"⚠️ DB load failed: {e} — using representative data")

    # Fallback: representative data
    print("ℹ️ Using representative holdings (no real DB data yet)")
    return _build_representative_holdings(selected_categories)


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
    if holdings_df.empty: return pd.DataFrame()
    fn_col = "scheme_name" if "scheme_name" in holdings_df.columns else "fund_name"
    total_funds = holdings_df[fn_col].nunique()

    grp = holdings_df.groupby(["stock_name","ticker","sector_normalised"]).agg(
        fund_count=(fn_col,"nunique"),
        categories=("category", lambda x: " | ".join(sorted(x.dropna().unique()))),
        avg_weight=("weight_pct","mean"),
        max_weight=("weight_pct","max"),
        amc_count=("amc_id","nunique") if "amc_id" in holdings_df.columns else ("stock_name","count"),
    ).reset_index()

    grp["funds_pct"] = (grp["fund_count"]/total_funds*100).round(1)
    mp = total_funds * grp["avg_weight"].max()
    grp["conviction_score"] = ((grp["fund_count"]*grp["avg_weight"])/mp*100).round(1) if mp>0 else grp["funds_pct"]
    grp["conviction_label"] = grp["funds_pct"].apply(
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


def get_data_status(selected_categories: List[str]) -> dict:
    """Return current data status for display in dashboard."""
    has_real, latest_month = _has_real_data(selected_categories)
    try:
        from holdings_db import get_scrape_status, get_db_stats
        stats = get_db_stats()
        return {
            "has_real_data": has_real,
            "latest_month": latest_month,
            "total_holdings": stats.get("holdings", 0),
            "amcs_with_data": stats.get("holdings_amcs", 0),
            "months_stored": stats.get("holdings_months", []),
            "source_type": "real" if has_real else "representative",
        }
    except:
        return {"has_real_data": False, "latest_month": None,
                "source_type": "representative", "total_holdings": 0}