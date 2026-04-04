"""
holdings_db.py
==============
Robust SQLite storage for all holdings, fund metadata, scrape logs.
Designed for long-term use — monthly snapshots accumulate over time.

Tables:
  amc_scrape_log   → every scrape attempt with status + errors
  fund_metadata    → master fund list (from AMFI NAVAll.txt)
  holdings         → monthly stock-level holdings per fund
  holdings_summary → derived conviction + rotation data (computed on demand)
  nav_daily        → daily NAV cache per fund (from mfapi.in)
  fund_performance → computed CAGR + metrics cache
"""

import sqlite3
import pandas as pd
import json
import os
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional

DB_PATH = "sip_analyzer.db"


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Get DB connection with WAL mode (concurrent read/write safe)."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH):
    """
    Create all tables if they don't exist.
    Safe to call on every startup — idempotent.
    """
    conn = get_conn(db_path)
    c = conn.cursor()

    # ── AMC scrape log ──────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS amc_scrape_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        amc_id          TEXT NOT NULL,
        amc_name        TEXT NOT NULL,
        disclosure_month TEXT NOT NULL,      -- 'YYYY-MM' format
        scrape_date     TEXT NOT NULL,       -- ISO datetime
        status          TEXT NOT NULL,       -- 'success' | 'failed' | 'skipped' | 'partial'
        funds_parsed    INTEGER DEFAULT 0,   -- number of fund schemes parsed
        holdings_count  INTEGER DEFAULT 0,   -- total (fund, stock) rows inserted
        file_url        TEXT,                -- actual URL downloaded
        file_format     TEXT,                -- 'xlsx' | 'xls' | 'csv' | 'pdf'
        file_size_kb    REAL,
        error_message   TEXT,
        raw_file_path   TEXT                 -- local path to downloaded file
    )
    """)

    # ── Fund metadata (master list from AMFI NAVAll.txt) ────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS fund_metadata (
        scheme_code     TEXT PRIMARY KEY,
        scheme_name     TEXT NOT NULL,
        amc_id          TEXT,
        amc_name        TEXT,
        amfi_category   TEXT,
        category        TEXT,               -- our standardised category
        nav             REAL,
        nav_date        TEXT,
        isin_growth     TEXT,
        isin_div        TEXT,
        last_updated    TEXT,
        is_active       INTEGER DEFAULT 1
    )
    """)

    # ── Holdings — core table ────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS holdings (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        disclosure_month TEXT NOT NULL,     -- 'YYYY-MM'
        amc_id          TEXT NOT NULL,
        amc_name        TEXT NOT NULL,
        scheme_name     TEXT NOT NULL,
        scheme_code     TEXT,               -- linked to fund_metadata if available
        stock_name      TEXT NOT NULL,
        isin            TEXT,               -- stock ISIN (e.g. INE002A01018)
        ticker          TEXT,               -- NSE ticker (derived from ISIN)
        sector          TEXT,               -- as disclosed by AMC
        sector_normalised TEXT,             -- our standardised sector name
        asset_type      TEXT DEFAULT 'Equity', -- Equity|Debt|REIT|InvIT|Cash|Other
        quantity        REAL,               -- shares held
        market_value_cr REAL,               -- market value in crores
        weight_pct      REAL NOT NULL,      -- % of NAV (the key number)
        rating          TEXT,               -- for debt instruments
        listing         TEXT,               -- Listed|Unlisted|Awaiting
        data_source     TEXT NOT NULL,      -- 'HDFC AMC factsheet Mar 2026'
        inserted_at     TEXT NOT NULL,

        UNIQUE(disclosure_month, amc_id, scheme_name, stock_name)
    )
    """)

    # ── Indices for fast lookups ─────────────────────────────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_month ON holdings(disclosure_month)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_scheme ON holdings(scheme_name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_stock ON holdings(stock_name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_isin ON holdings(isin)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_amc ON holdings(amc_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_ticker ON holdings(ticker)")

    # ── Conviction summary (computed and cached) ─────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS conviction_cache (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        disclosure_month TEXT NOT NULL,
        categories_key  TEXT NOT NULL,      -- sorted joined categories e.g. "Large Cap|Mid Cap"
        stock_name      TEXT NOT NULL,
        ticker          TEXT,
        sector          TEXT,
        fund_count      INTEGER NOT NULL,   -- how many funds hold this stock
        total_funds     INTEGER NOT NULL,   -- total funds in selected categories
        funds_pct       REAL NOT NULL,
        avg_weight      REAL,
        max_weight      REAL,
        conviction_score REAL,
        conviction_label TEXT,
        categories_present TEXT,            -- which categories hold this stock
        computed_at     TEXT NOT NULL,

        UNIQUE(disclosure_month, categories_key, stock_name)
    )
    """)

    # ── Daily NAV cache ──────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS nav_daily (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        scheme_code TEXT NOT NULL,
        nav_date    TEXT NOT NULL,
        nav         REAL NOT NULL,
        inserted_at TEXT NOT NULL,

        UNIQUE(scheme_code, nav_date)
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_nav_code ON nav_daily(scheme_code)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_daily(nav_date)")

    # ── Fund performance cache (CAGR + metrics) ──────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS fund_performance (
        scheme_code     TEXT PRIMARY KEY,
        cagr_1y         REAL,
        cagr_3y         REAL,
        cagr_5y         REAL,
        cagr_10y        REAL,
        volatility      REAL,
        sharpe_ratio    REAL,
        max_drawdown    REAL,
        expense_ratio   REAL,
        aum_cr          REAL,
        aum_source      TEXT,       -- 'AMFI monthly' | 'estimated' | 'mfapi'
        cagr_source     TEXT,       -- 'mfapi NAV history' | 'AMFI median'
        er_source       TEXT,
        composite_score REAL,
        computed_at     TEXT NOT NULL,
        nav_data_points INTEGER     -- how many NAV data points we used
    )
    """)

    # ── AMFI AUM monthly data ────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS amfi_aum (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        report_month    TEXT NOT NULL,      -- 'YYYY-MM'
        scheme_code     TEXT,
        scheme_name     TEXT,
        amc_name        TEXT,
        category        TEXT,
        avg_aum_cr      REAL NOT NULL,
        inserted_at     TEXT NOT NULL,

        UNIQUE(report_month, scheme_code)
    )
    """)

    conn.commit()
    conn.close()
    print(f"✅ DB initialised at {db_path}")


# ─── HOLDINGS WRITE ───────────────────────────────────────────────────────────

def insert_holdings(rows: list[dict], db_path: str = DB_PATH) -> int:
    """
    Insert holdings rows. Uses INSERT OR REPLACE to handle re-runs.
    Returns count of rows inserted.
    """
    if not rows:
        return 0

    now = datetime.now().isoformat()
    conn = get_conn(db_path)
    inserted = 0

    for row in rows:
        try:
            conn.execute("""
            INSERT OR REPLACE INTO holdings (
                disclosure_month, amc_id, amc_name, scheme_name, scheme_code,
                stock_name, isin, ticker, sector, sector_normalised,
                asset_type, quantity, market_value_cr, weight_pct,
                rating, listing, data_source, inserted_at
            ) VALUES (
                :disclosure_month, :amc_id, :amc_name, :scheme_name, :scheme_code,
                :stock_name, :isin, :ticker, :sector, :sector_normalised,
                :asset_type, :quantity, :market_value_cr, :weight_pct,
                :rating, :listing, :data_source, :inserted_at
            )
            """, {**row, "inserted_at": now})
            inserted += 1
        except Exception as e:
            print(f"  ⚠️  Insert error for {row.get('scheme_name','?')} / {row.get('stock_name','?')}: {e}")

    conn.commit()
    conn.close()
    return inserted


def log_scrape(log: dict, db_path: str = DB_PATH):
    """Log a scrape attempt."""
    conn = get_conn(db_path)
    conn.execute("""
    INSERT INTO amc_scrape_log (
        amc_id, amc_name, disclosure_month, scrape_date, status,
        funds_parsed, holdings_count, file_url, file_format,
        file_size_kb, error_message, raw_file_path
    ) VALUES (
        :amc_id, :amc_name, :disclosure_month, :scrape_date, :status,
        :funds_parsed, :holdings_count, :file_url, :file_format,
        :file_size_kb, :error_message, :raw_file_path
    )
    """, {
        "amc_id": log.get("amc_id",""),
        "amc_name": log.get("amc_name",""),
        "disclosure_month": log.get("disclosure_month",""),
        "scrape_date": datetime.now().isoformat(),
        "status": log.get("status","unknown"),
        "funds_parsed": log.get("funds_parsed", 0),
        "holdings_count": log.get("holdings_count", 0),
        "file_url": log.get("file_url"),
        "file_format": log.get("file_format"),
        "file_size_kb": log.get("file_size_kb"),
        "error_message": log.get("error_message"),
        "raw_file_path": log.get("raw_file_path"),
    })
    conn.commit()
    conn.close()


def upsert_fund_metadata(records: list[dict], db_path: str = DB_PATH):
    """Upsert fund metadata from AMFI NAVAll.txt."""
    conn = get_conn(db_path)
    now = datetime.now().isoformat()
    for r in records:
        conn.execute("""
        INSERT OR REPLACE INTO fund_metadata (
            scheme_code, scheme_name, amc_id, amc_name,
            amfi_category, category, nav, nav_date,
            isin_growth, isin_div, last_updated, is_active
        ) VALUES (
            :scheme_code, :scheme_name, :amc_id, :amc_name,
            :amfi_category, :category, :nav, :nav_date,
            :isin_growth, :isin_div, :last_updated, 1
        )
        """, {**r, "last_updated": now})
    conn.commit()
    conn.close()


def upsert_performance(records: list[dict], db_path: str = DB_PATH):
    """Upsert computed performance metrics."""
    conn = get_conn(db_path)
    now = datetime.now().isoformat()
    for r in records:
        conn.execute("""
        INSERT OR REPLACE INTO fund_performance (
            scheme_code, cagr_1y, cagr_3y, cagr_5y, cagr_10y,
            volatility, sharpe_ratio, max_drawdown, expense_ratio,
            aum_cr, aum_source, cagr_source, er_source,
            composite_score, computed_at, nav_data_points
        ) VALUES (
            :scheme_code, :cagr_1y, :cagr_3y, :cagr_5y, :cagr_10y,
            :volatility, :sharpe_ratio, :max_drawdown, :expense_ratio,
            :aum_cr, :aum_source, :cagr_source, :er_source,
            :composite_score, :computed_at, :nav_data_points
        )
        """, {**r, "computed_at": now})
    conn.commit()
    conn.close()


# ─── HOLDINGS READ ────────────────────────────────────────────────────────────

def get_holdings(
    disclosure_month: str,
    categories: list[str] | None = None,
    amc_ids: list[str] | None = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Fetch holdings for a given month, optionally filtered by category or AMC.
    Joins with fund_metadata to get SEBI category.
    """
    conn = get_conn(db_path)
    query = """
    SELECT
        h.disclosure_month,
        h.amc_id, h.amc_name,
        h.scheme_name, h.scheme_code,
        COALESCE(fm.category, h.amc_name) as category,
        h.stock_name, h.isin, h.ticker,
        h.sector, h.sector_normalised,
        h.asset_type, h.quantity,
        h.market_value_cr, h.weight_pct,
        h.data_source
    FROM holdings h
    LEFT JOIN fund_metadata fm ON h.scheme_code = fm.scheme_code
    WHERE h.disclosure_month = ?
    """
    params = [disclosure_month]

    if amc_ids:
        placeholders = ",".join("?" * len(amc_ids))
        query += f" AND h.amc_id IN ({placeholders})"
        params.extend(amc_ids)

    if categories:
        placeholders = ",".join("?" * len(categories))
        query += f" AND COALESCE(fm.category, 'Other') IN ({placeholders})"
        params.extend(categories)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_available_months(db_path: str = DB_PATH) -> list[str]:
    """Return list of months with holdings data, newest first."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT DISTINCT disclosure_month FROM holdings ORDER BY disclosure_month DESC"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_scrape_status(db_path: str = DB_PATH) -> pd.DataFrame:
    """Return latest scrape status per AMC."""
    conn = get_conn(db_path)
    df = pd.read_sql_query("""
    SELECT
        amc_id, amc_name, disclosure_month,
        status, funds_parsed, holdings_count,
        file_url, error_message, scrape_date
    FROM amc_scrape_log
    WHERE id IN (
        SELECT MAX(id) FROM amc_scrape_log GROUP BY amc_id, disclosure_month
    )
    ORDER BY tier_sort, amc_id
    """.replace("tier_sort,", ""), conn)
    conn.close()
    return df


def get_fund_list(
    categories: list[str] | None = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """Return all funds with their performance data."""
    conn = get_conn(db_path)
    query = """
    SELECT
        fm.scheme_code, fm.scheme_name, fm.amc_name,
        fm.amfi_category, fm.category, fm.nav, fm.nav_date,
        fp.cagr_1y, fp.cagr_3y, fp.cagr_5y,
        fp.volatility, fp.sharpe_ratio, fp.max_drawdown,
        fp.expense_ratio, fp.aum_cr,
        fp.aum_source, fp.cagr_source, fp.er_source,
        fp.composite_score
    FROM fund_metadata fm
    LEFT JOIN fund_performance fp ON fm.scheme_code = fp.scheme_code
    WHERE fm.is_active = 1
    """
    params = []
    if categories:
        placeholders = ",".join("?" * len(categories))
        query += f" AND fm.category IN ({placeholders})"
        params.extend(categories)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_conviction_data(
    disclosure_month: str,
    categories: list[str],
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Compute conviction table: for each stock, how many funds hold it.
    Uses DB holdings — real data.
    """
    holdings = get_holdings(disclosure_month, categories=categories, db_path=db_path)
    if holdings.empty:
        return pd.DataFrame()

    total_funds = holdings["scheme_name"].nunique()

    grouped = holdings.groupby(["stock_name", "ticker", "sector_normalised"]).agg(
        fund_count=("scheme_name", "nunique"),
        categories=("category", lambda x: " | ".join(sorted(x.dropna().unique()))),
        avg_weight=("weight_pct", "mean"),
        max_weight=("weight_pct", "max"),
        amc_count=("amc_id", "nunique"),
    ).reset_index()

    grouped["funds_pct"] = (grouped["fund_count"] / total_funds * 100).round(1)
    grouped["conviction_label"] = grouped["funds_pct"].apply(
        lambda p: "🔴 Universal" if p >= 80
        else ("🟠 High" if p >= 50
              else ("🟡 Moderate" if p >= 25
                    else "🟢 Selective"))
    )
    mp = total_funds * grouped["avg_weight"].max()
    grouped["conviction_score"] = (
        (grouped["fund_count"] * grouped["avg_weight"]) / mp * 100
    ).round(1) if mp > 0 else grouped["funds_pct"]

    return grouped.sort_values("fund_count", ascending=False).reset_index(drop=True)


def get_rotation_data(
    categories: list[str],
    top_n: int = 30,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Build quarterly rotation from real DB holdings.
    Returns fund_count per stock per month.
    """
    months = get_available_months(db_path)
    if not months:
        return pd.DataFrame()

    rows = []
    for month in months:
        h = get_holdings(month, categories=categories, db_path=db_path)
        if h.empty:
            continue
        total = h["scheme_name"].nunique()
        grp = h.groupby(["stock_name", "ticker", "sector_normalised"])["scheme_name"].nunique().reset_index()
        grp.columns = ["stock_name", "ticker", "sector", "fund_count"]
        grp["quarter"] = _month_to_quarter(month)
        grp["fund_pct"] = (grp["fund_count"] / total * 100).round(1)
        grp["total_funds"] = total
        rows.append(grp)

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)
    # Filter to top N stocks by latest month fund count
    latest = months[0]
    top_stocks = df[df["quarter"] == _month_to_quarter(latest)].nlargest(top_n, "fund_count")["stock_name"].tolist()
    df = df[df["stock_name"].isin(top_stocks)]

    # Compute trend
    if len(months) >= 2:
        first_q = _month_to_quarter(months[-1])
        last_q  = _month_to_quarter(months[0])
        fq = df[df["quarter"] == first_q][["stock_name","fund_count"]].rename(columns={"fund_count":"start"})
        lq = df[df["quarter"] == last_q][["stock_name","fund_count"]].rename(columns={"fund_count":"end"})
        trend = fq.merge(lq, on="stock_name")
        trend["trend"] = trend["end"] - trend["start"]
        trend["trend_label"] = trend["trend"].apply(
            lambda x: "📈 Accumulating" if x >= 2 else ("📉 Distributing" if x <= -2 else "➡️ Stable")
        )
        df = df.merge(trend[["stock_name","trend","trend_label"]], on="stock_name", how="left")

    return df


def _month_to_quarter(month_str: str) -> str:
    """Convert '2026-03' to 'Q4 FY26'."""
    try:
        y, m = int(month_str[:4]), int(month_str[5:7])
        fy = y if m >= 4 else y - 1
        q = ((m - 4) % 12) // 3 + 1
        return f"Q{q} FY{str(fy)[2:]}"
    except:
        return month_str


def get_db_stats(db_path: str = DB_PATH) -> dict:
    """Return summary statistics about the database."""
    conn = get_conn(db_path)
    stats = {}
    tables = ["fund_metadata", "holdings", "nav_daily", "fund_performance", "amc_scrape_log"]
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            stats[t] = count
        except:
            stats[t] = 0

    stats["holdings_months"] = get_available_months(db_path)
    stats["holdings_amcs"] = conn.execute(
        "SELECT COUNT(DISTINCT amc_id) FROM holdings"
    ).fetchone()[0]
    conn.close()
    return stats


def get_fund_aum_summary(disclosure_month: str, db_path: str = DB_PATH) -> dict:
    """
    Calculate total AUM (market_value_cr) per fund for a specific month.
    Returns: {scheme_name: total_aum_cr}
    """
    conn = get_conn(db_path)
    res = conn.execute("""
        SELECT scheme_name, SUM(market_value_cr) as total_aum
        FROM holdings
        WHERE disclosure_month = ?
        GROUP BY scheme_name
    """, (disclosure_month,)).fetchall()
    conn.close()
    return {r["scheme_name"]: round(r["total_aum"], 2) for r in res if r["total_aum"]}


def link_holdings_to_metadata(db_path: str = DB_PATH):
    """
    Try to populate scheme_code in holdings table by matching scheme_names 
    with fund_metadata table. Uses exact match first, then fuzzy.
    """
    from difflib import get_close_matches
    conn = get_conn(db_path)
    
    # 1. Exact match
    conn.execute("""
        UPDATE holdings 
        SET scheme_code = (
            SELECT scheme_code FROM fund_metadata 
            WHERE fund_metadata.scheme_name = holdings.scheme_name
        )
        WHERE scheme_code IS NULL
    """)
    conn.commit()
    
    # 2. Fuzzy match for remaining
    holdings_to_match = conn.execute(
        "SELECT DISTINCT scheme_name FROM holdings WHERE scheme_code IS NULL"
    ).fetchall()
    
    if not holdings_to_match:
        conn.close()
        return

    metadata = conn.execute("SELECT scheme_name, scheme_code FROM fund_metadata").fetchall()
    meta_names = [m["scheme_name"] for m in metadata]
    meta_map = {m["scheme_name"]: m["scheme_code"] for m in metadata}
    
    matched = 0
    for row in holdings_to_match:
        name = row["scheme_name"]
        matches = get_close_matches(name, meta_names, n=1, cutoff=0.8)
        if matches:
            code = meta_map[matches[0]]
            conn.execute(
                "UPDATE holdings SET scheme_code = ? WHERE scheme_name = ?",
                (code, name)
            )
            matched += 1
            
    conn.commit()
    conn.close()
    if matched:
        print(f"🔗 Linked {matched} funds via fuzzy matching")


if __name__ == "__main__":
    init_db()
    stats = get_db_stats()
    print("\nDB Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")