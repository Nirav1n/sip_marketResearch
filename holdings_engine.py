"""
holdings_engine.py
Core engine for stock-level analysis across SIP fund categories.
Tracks which stocks appear across funds, how many funds hold them,
and how holdings rotate over time (quarterly snapshots).
"""

import pandas as pd
import numpy as np
import random
from typing import List, Dict

# ─── MASTER STOCK UNIVERSE (NSE-listed, categorized) ──────────────────────────
# In production: replace with actual fund disclosure PDFs / AMFI holdings data
STOCK_UNIVERSE = {
    "Large Cap": [
        ("Reliance Industries", "RELIANCE", "Energy/Conglomerate"),
        ("HDFC Bank", "HDFCBANK", "Banking"),
        ("Infosys", "INFY", "IT"),
        ("ICICI Bank", "ICICIBANK", "Banking"),
        ("TCS", "TCS", "IT"),
        ("Larsen & Toubro", "LT", "Infrastructure"),
        ("Axis Bank", "AXISBANK", "Banking"),
        ("Kotak Mahindra Bank", "KOTAKBANK", "Banking"),
        ("Bajaj Finance", "BAJFINANCE", "NBFC"),
        ("Asian Paints", "ASIANPAINT", "Consumer"),
        ("HUL", "HINDUNILVR", "FMCG"),
        ("Maruti Suzuki", "MARUTI", "Auto"),
        ("Sun Pharma", "SUNPHARMA", "Pharma"),
        ("Titan Company", "TITAN", "Consumer"),
        ("Wipro", "WIPRO", "IT"),
        ("HCL Technologies", "HCLTECH", "IT"),
        ("Tata Motors", "TATAMOTORS", "Auto"),
        ("Adani Ports", "ADANIPORTS", "Infrastructure"),
        ("Power Grid", "POWERGRID", "Energy"),
        ("NTPC", "NTPC", "Energy"),
        ("Coal India", "COALINDIA", "Energy"),
        ("Bharti Airtel", "BHARTIARTL", "Telecom"),
        ("ITC", "ITC", "FMCG"),
        ("SBI", "SBIN", "Banking"),
        ("Nestle India", "NESTLEIND", "FMCG"),
        ("Bajaj Auto", "BAJAJ-AUTO", "Auto"),
        ("Tech Mahindra", "TECHM", "IT"),
        ("JSW Steel", "JSWSTEEL", "Metals"),
        ("Tata Steel", "TATASTEEL", "Metals"),
        ("Dr Reddy's", "DRREDDY", "Pharma"),
        ("Cipla", "CIPLA", "Pharma"),
        ("Divis Labs", "DIVISLAB", "Pharma"),
        ("UltraTech Cement", "ULTRACEMCO", "Materials"),
        ("Grasim Industries", "GRASIM", "Materials"),
        ("Hindalco", "HINDALCO", "Metals"),
        ("Vedanta", "VEDL", "Metals"),
        ("IndusInd Bank", "INDUSINDBK", "Banking"),
        ("Eicher Motors", "EICHERMOT", "Auto"),
        ("Hero MotoCorp", "HEROMOTOCO", "Auto"),
        ("Britannia", "BRITANNIA", "FMCG"),
    ],
    "Mid Cap": [
        ("Persistent Systems", "PERSISTENT", "IT"),
        ("Coforge", "COFORGE", "IT"),
        ("Mphasis", "MPHASIS", "IT"),
        ("Max Healthcare", "MAXHEALTH", "Healthcare"),
        ("Fortis Healthcare", "FORTIS", "Healthcare"),
        ("Narayana Hrudayalaya", "NH", "Healthcare"),
        ("AU Small Finance Bank", "AUBANK", "Banking"),
        ("Federal Bank", "FEDERALBNK", "Banking"),
        ("Cholamandalam Finance", "CHOLAFIN", "NBFC"),
        ("Muthoot Finance", "MUTHOOTFIN", "NBFC"),
        ("Voltas", "VOLTAS", "Consumer Durables"),
        ("Blue Star", "BLUESTAR", "Consumer Durables"),
        ("Crompton Greaves", "CROMPTON", "Consumer Durables"),
        ("Dixon Technologies", "DIXON", "Electronics"),
        ("Amber Enterprises", "AMBER", "Electronics"),
        ("Godrej Properties", "GODREJPROP", "Real Estate"),
        ("Prestige Estates", "PRESTIGE", "Real Estate"),
        ("Phoenix Mills", "PHOENIXLTD", "Real Estate"),
        ("Oberoi Realty", "OBEROIRLTY", "Real Estate"),
        ("Polycab India", "POLYCAB", "Electricals"),
        ("KEI Industries", "KEI", "Electricals"),
        ("ABB India", "ABB", "Capital Goods"),
        ("Siemens", "SIEMENS", "Capital Goods"),
        ("Bharat Forge", "BHARATFORG", "Capital Goods"),
        ("Cummins India", "CUMMINSIND", "Capital Goods"),
        ("Trent", "TRENT", "Retail"),
        ("V-Mart Retail", "VMART", "Retail"),
        ("Avenue Supermarts", "DMART", "Retail"),
        ("Indian Hotels", "INDHOTEL", "Hospitality"),
        ("Lemon Tree Hotels", "LEMONTREE", "Hospitality"),
        ("PVR INOX", "PVRINOX", "Entertainment"),
        ("Zomato", "ZOMATO", "Tech/Food"),
        ("Nykaa", "FSN", "Tech/Beauty"),
        ("Delhivery", "DELHIVERY", "Logistics"),
        ("One97 (Paytm)", "PAYTM", "Fintech"),
        ("Astral Poly", "ASTRAL", "Building Materials"),
        ("PI Industries", "PIIND", "Agrochemicals"),
        ("Deepak Nitrite", "DEEPAKNTR", "Chemicals"),
        ("Aarti Industries", "AARTIIND", "Chemicals"),
        ("Vinati Organics", "VINATIORGA", "Chemicals"),
    ],
    "Small Cap": [
        ("KPIT Technologies", "KPITTECH", "IT"),
        ("Intellect Design", "INTELLECT", "IT"),
        ("Newgen Software", "NEWGEN", "IT"),
        ("Birlasoft", "BSOFT", "IT"),
        ("Hester Biosciences", "HESTERBIO", "Pharma"),
        ("Solara Active Pharma", "SOLARA", "Pharma"),
        ("Shilpa Medicare", "SHILPAMED", "Pharma"),
        ("Venus Remedies", "VENUSREM", "Pharma"),
        ("IIFL Finance", "IIFL", "NBFC"),
        ("Manappuram Finance", "MANAPPURAM", "NBFC"),
        ("Repco Home Finance", "REPCOHOME", "NBFC"),
        ("Suryoday SFB", "SURYODAY", "Banking"),
        ("Ujjivan SFB", "UJJIVANSFB", "Banking"),
        ("Equitas SFB", "EQUITASBNK", "Banking"),
        ("Nuvoco Vistas", "NUVOCO", "Materials"),
        ("JK Cement", "JKCEMENT", "Materials"),
        ("HeidelbergCement", "HEIDELBERG", "Materials"),
        ("Mahindra Logistics", "MAHLOG", "Logistics"),
        ("TCI Express", "TCIEXP", "Logistics"),
        ("VRL Logistics", "VRLLOG", "Logistics"),
        ("Craftsman Auto", "CRAFTSMAN", "Auto Ancillary"),
        ("Suprajit Engineering", "SUPRAJIT", "Auto Ancillary"),
        ("Minda Industries", "MINDAIND", "Auto Ancillary"),
        ("Garware Tech Fibres", "GARFIBRES", "Textiles"),
        ("KPR Mill", "KPRMILL", "Textiles"),
        ("Welspun India", "WELSPUNIND", "Textiles"),
        ("Sheela Foam", "SFL", "Consumer"),
        ("Safari Industries", "SAFARI", "Consumer"),
        ("Campus Activewear", "CAMPUS", "Consumer"),
        ("Devyani International", "DEVYANI", "QSR"),
        ("Sapphire Foods", "SAPPHIRE", "QSR"),
        ("Westlife Foodworld", "WESTLIFE", "QSR"),
        ("Clean Science", "CLEANSCIENCE", "Chemicals"),
        ("Tatva Chintan", "TATVA", "Chemicals"),
        ("Anupam Rasayan", "ANURAS", "Chemicals"),
        ("Balrampur Chini", "BALRAMCHIN", "Sugar"),
        ("Triveni Engineering", "TRIVENI", "Sugar"),
        ("Dhampur Bio Organics", "DHAMPURE", "Sugar"),
    ],
    "Sectoral/Thematic": [
        # IT
        ("Infosys", "INFY", "IT"),
        ("TCS", "TCS", "IT"),
        ("Wipro", "WIPRO", "IT"),
        ("HCL Technologies", "HCLTECH", "IT"),
        ("Tech Mahindra", "TECHM", "IT"),
        ("Persistent Systems", "PERSISTENT", "IT"),
        ("Coforge", "COFORGE", "IT"),
        ("Mphasis", "MPHASIS", "IT"),
        # Banking
        ("HDFC Bank", "HDFCBANK", "Banking"),
        ("ICICI Bank", "ICICIBANK", "Banking"),
        ("SBI", "SBIN", "Banking"),
        ("Axis Bank", "AXISBANK", "Banking"),
        ("Kotak Mahindra Bank", "KOTAKBANK", "Banking"),
        ("IndusInd Bank", "INDUSINDBK", "Banking"),
        ("Bank of Baroda", "BANKBARODA", "Banking"),
        ("Federal Bank", "FEDERALBNK", "Banking"),
        # Pharma
        ("Sun Pharma", "SUNPHARMA", "Pharma"),
        ("Dr Reddy's", "DRREDDY", "Pharma"),
        ("Cipla", "CIPLA", "Pharma"),
        ("Divis Labs", "DIVISLAB", "Pharma"),
        ("Aurobindo Pharma", "AUROPHARMA", "Pharma"),
        ("Lupin", "LUPIN", "Pharma"),
        # Infra
        ("Larsen & Toubro", "LT", "Infrastructure"),
        ("Adani Ports", "ADANIPORTS", "Infrastructure"),
        ("Power Grid", "POWERGRID", "Infrastructure"),
        ("NTPC", "NTPC", "Infrastructure"),
        ("BHEL", "BHEL", "Infrastructure"),
        ("IRB Infrastructure", "IRB", "Infrastructure"),
        # FMCG
        ("HUL", "HINDUNILVR", "FMCG"),
        ("ITC", "ITC", "FMCG"),
        ("Nestle India", "NESTLEIND", "FMCG"),
        ("Britannia", "BRITANNIA", "FMCG"),
        ("Dabur", "DABUR", "FMCG"),
        ("Marico", "MARICO", "FMCG"),
    ],
}

# ─── QUARTERLY SNAPSHOTS for rotation analysis ─────────────────────────────────
QUARTERS = ["Q1 FY24", "Q2 FY24", "Q3 FY24", "Q4 FY24", "Q1 FY25", "Q2 FY25", "Q3 FY25", "Q4 FY25"]


def get_fund_names_for_category(category: str, n: int = 20) -> List[str]:
    """Generate realistic fund names for a category."""
    amcs = ["HDFC", "ICICI Pru", "SBI", "Axis", "Mirae Asset", "Kotak", "Nippon India",
            "DSP", "Franklin", "Canara Robeco", "Edelweiss", "UTI", "PGIM India", "Motilal Oswal"]
    suffix_map = {
        "Large Cap": ["Large Cap Fund", "Bluechip Fund", "Top 100 Fund"],
        "Mid Cap": ["Mid Cap Fund", "Emerging Bluechip", "Mid Cap Opportunities"],
        "Small Cap": ["Small Cap Fund", "Small & Midcap Fund", "Emerging Equities"],
        "Sectoral/Thematic": ["Sectoral Fund", "Thematic Fund", "Opportunities Fund"],
    }
    suffixes = suffix_map.get(category, ["Equity Fund"])
    rng = random.Random(hash(category))
    names = []
    for amc in amcs[:n]:
        suffix = rng.choice(suffixes)
        names.append(f"{amc} {suffix}")
    return names[:n]


def build_holdings_data(selected_categories: List[str]) -> pd.DataFrame:
    """
    Build stock-level holdings data across all funds in selected categories.
    Returns a dataframe with one row per (fund, stock) pair.
    Also simulates quarterly rotation by slightly varying holdings each quarter.
    """
    rng = random.Random(42)
    rows = []

    for category in selected_categories:
        universe = STOCK_UNIVERSE.get(category, [])
        if not universe:
            continue

        fund_names = get_fund_names_for_category(category, n=15)
        num_stocks_per_fund = {"Large Cap": 35, "Mid Cap": 40, "Small Cap": 45, "Sectoral/Thematic": 25}.get(category, 35)

        for fund in fund_names:
            fund_seed = hash(fund) % 10000
            frng = random.Random(fund_seed)

            # Each fund picks a subset of the universe
            pool = universe.copy()
            frng.shuffle(pool)
            selected = pool[:min(num_stocks_per_fund, len(pool))]

            # Assign weightings (top holdings get more weight)
            weights = sorted([frng.uniform(1, 10) for _ in selected], reverse=True)
            total_w = sum(weights)
            weights = [w / total_w * 100 for w in weights]

            for (name, ticker, sector), weight in zip(selected, weights):
                rows.append({
                    "fund_name": fund,
                    "category": category,
                    "stock_name": name,
                    "ticker": ticker,
                    "sector": sector,
                    "weight_pct": round(weight, 2),
                })

    return pd.DataFrame(rows)


def build_stock_conviction_table(holdings_df: pd.DataFrame, selected_categories: List[str]) -> pd.DataFrame:
    """
    For each stock, count how many funds hold it across selected categories.
    Returns sorted by fund_count descending — the "conviction" table.
    """
    if holdings_df.empty:
        return pd.DataFrame()

    total_funds = holdings_df["fund_name"].nunique()

    grouped = holdings_df.groupby(["stock_name", "ticker", "sector"]).agg(
        fund_count=("fund_name", "nunique"),
        categories=("category", lambda x: ", ".join(sorted(x.unique()))),
        avg_weight=("weight_pct", "mean"),
        max_weight=("weight_pct", "max"),
        total_weight=("weight_pct", "sum"),
    ).reset_index()

    grouped["funds_pct"] = (grouped["fund_count"] / total_funds * 100).round(1)
    grouped["conviction_label"] = grouped["fund_count"].apply(
        lambda n: "🔴 Universal" if n / total_funds >= 0.8
        else ("🟠 High" if n / total_funds >= 0.5
              else ("🟡 Moderate" if n / total_funds >= 0.25
                    else "🟢 Selective"))
    )
    grouped = grouped.sort_values("fund_count", ascending=False).reset_index(drop=True)
    grouped.index = grouped.index + 1  # 1-based rank
    grouped = grouped.reset_index().rename(columns={"index": "rank"})

    return grouped


def build_rotation_data(selected_categories: List[str]) -> pd.DataFrame:
    """
    Simulate quarterly rotation — tracks how a stock's fund_count changes over time.
    Shows which stocks are being accumulated or distributed.
    """
    rows = []
    rng = random.Random(99)

    for category in selected_categories:
        universe = STOCK_UNIVERSE.get(category, [])
        if not universe:
            continue

        # Pick top 20 stocks for rotation tracking
        top_stocks = universe[:20]
        base_funds = 15  # funds per category

        for stock_name, ticker, sector in top_stocks:
            stock_seed = hash(ticker) % 1000
            srng = random.Random(stock_seed)
            base_count = srng.randint(3, base_funds)

            prev = base_count
            for q in QUARTERS:
                # Slight drift each quarter
                delta = srng.randint(-2, 2)
                count = max(1, min(base_funds, prev + delta))
                rows.append({
                    "quarter": q,
                    "stock_name": stock_name,
                    "ticker": ticker,
                    "sector": sector,
                    "category": category,
                    "fund_count": count,
                    "fund_pct": round(count / base_funds * 100, 1),
                })
                prev = count

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Calculate trend: compare latest vs earliest quarter
    first_q = QUARTERS[0]
    last_q = QUARTERS[-1]
    first = df[df["quarter"] == first_q][["ticker", "fund_count"]].rename(columns={"fund_count": "start"})
    last = df[df["quarter"] == last_q][["ticker", "fund_count"]].rename(columns={"fund_count": "end"})
    trend = first.merge(last, on="ticker")
    trend["trend"] = trend["end"] - trend["start"]
    trend["trend_label"] = trend["trend"].apply(
        lambda x: "📈 Accumulating" if x > 1 else ("📉 Distributing" if x < -1 else "➡️ Stable")
    )
    df = df.merge(trend[["ticker", "trend", "trend_label"]], on="ticker", how="left")
    return df
