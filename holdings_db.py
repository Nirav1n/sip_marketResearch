"""
holdings_db.py  — v5  (fixed)
==============================
Fixes applied vs v4:
  1. Duplicate get_fund_aum_summary removed — single version with optional month param
  2. get_conn() used as context manager fixed — all callers use try/finally conn.close()
  3. insert_holdings() now uses executemany() — 10-50x faster
  4. get_all_fund_performance() and get_fund_aum_summary() at module level (not appended)
  5. Explicit conn.close() in every function — no leaks
  6. standardize_db_categories() updated with full mapping
"""

import sqlite3
import pandas as pd
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_PATH = "sip_analyzer.db"


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Get a DB connection with WAL mode. Caller MUST call conn.close()."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")   # 32 MB page cache
    conn.execute("PRAGMA optimize")            # refresh query planner stats
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH):
    """Create all tables if they don't exist. Safe to call on every startup."""
    conn = get_conn(db_path)
    try:
        c = conn.cursor()

        c.execute("""
        CREATE TABLE IF NOT EXISTS amc_scrape_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            amc_id          TEXT NOT NULL,
            amc_name        TEXT NOT NULL,
            disclosure_month TEXT NOT NULL,
            scrape_date     TEXT NOT NULL,
            status          TEXT NOT NULL,
            funds_parsed    INTEGER DEFAULT 0,
            holdings_count  INTEGER DEFAULT 0,
            file_url        TEXT,
            file_format     TEXT,
            file_size_kb    REAL,
            error_message   TEXT,
            raw_file_path   TEXT
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS fund_metadata (
            scheme_code     TEXT PRIMARY KEY,
            scheme_name     TEXT NOT NULL,
            family_id       INTEGER,
            amc_id          TEXT,
            amc_name        TEXT,
            amfi_category   TEXT,
            category        TEXT NOT NULL,
            nav             REAL,
            nav_date        TEXT,
            isin_growth     TEXT,
            isin_div        TEXT,
            last_updated    TEXT NOT NULL,
            is_active       INTEGER DEFAULT 1
        )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_fm_cat     ON fund_metadata(category)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_fm_amc     ON fund_metadata(amc_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_fm_code_cat ON fund_metadata(scheme_code, category)")

        c.execute("""
        CREATE TABLE IF NOT EXISTS holdings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            disclosure_month TEXT NOT NULL,
            amc_id          TEXT,
            amc_name        TEXT,
            scheme_name     TEXT NOT NULL,
            scheme_code     TEXT NOT NULL,
            stock_name      TEXT NOT NULL,
            isin            TEXT,
            ticker          TEXT,
            sector          TEXT,
            sector_normalised TEXT,
            asset_type      TEXT,
            quantity        REAL,
            market_value_cr REAL NOT NULL,
            weight_pct      REAL NOT NULL,
            rating          TEXT,
            listing         TEXT,
            data_source     TEXT,
            inserted_at     TEXT NOT NULL,

            UNIQUE(disclosure_month, scheme_code, isin, stock_name)
        )
        """)
        # Composite lookup — covers fund-level and month-level queries
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_lookup     ON holdings(disclosure_month, scheme_code)")
        # Conviction table — covers month+stock GROUP BY without touching fund_metadata
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_conviction ON holdings(disclosure_month, stock_name, weight_pct)")
        # Point lookups
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_stock      ON holdings(stock_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_isin       ON holdings(isin)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_ticker     ON holdings(ticker)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_h_amc        ON holdings(amc_id)")

        c.execute("""
        CREATE TABLE IF NOT EXISTS conviction_cache (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            disclosure_month TEXT NOT NULL,
            categories_key  TEXT NOT NULL,
            stock_name      TEXT NOT NULL,
            ticker          TEXT,
            sector          TEXT,
            fund_count      INTEGER NOT NULL,
            total_funds     INTEGER NOT NULL,
            funds_pct       REAL NOT NULL,
            avg_weight      REAL,
            max_weight      REAL,
            conviction_score REAL,
            conviction_label TEXT,
            categories_present TEXT,
            computed_at     TEXT NOT NULL,

            UNIQUE(disclosure_month, categories_key, stock_name)
        )
        """)

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
        # UNIQUE(scheme_code, nav_date) is already the primary lookup pattern;
        # the autoindex covers it — no additional indexes needed for nav_daily.

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
            aum_source      TEXT,
            cagr_source     TEXT,
            er_source       TEXT,
            composite_score REAL,
            computed_at     TEXT NOT NULL,
            nav_data_points INTEGER
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS amfi_aum (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            report_month    TEXT NOT NULL,
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
        print(f"✅ DB initialised at {db_path}")
    finally:
        conn.close()


# ─── HOLDINGS WRITE ────────────────────────────────────────────────────────────

def insert_holdings(rows: list[dict], db_path: str = DB_PATH) -> int:
    """
    Bulk-insert holdings rows using executemany — 10-50x faster than row-by-row.
    Uses INSERT OR REPLACE to handle re-runs safely.
    Returns count of rows attempted.
    """
    if not rows:
        return 0

    now = datetime.now().isoformat()
    # Stamp inserted_at on all rows
    for row in rows:
        row["inserted_at"] = now

    conn = get_conn(db_path)
    try:
        conn.executemany("""
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
        """, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"❌ Bulk insert failed: {e}. Falling back to row-by-row...")
        # Fallback: insert one by one, skip bad rows
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
                """, row)
                inserted += 1
            except Exception as row_err:
                print(f"  ⚠️  Skip {row.get('scheme_name','?')} / {row.get('stock_name','?')}: {row_err}")
        conn.commit()
        return inserted
    finally:
        conn.close()


def log_scrape(log: dict, db_path: str = DB_PATH):
    """Log a scrape attempt."""
    conn = get_conn(db_path)
    try:
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
            "amc_id": log.get("amc_id", ""),
            "amc_name": log.get("amc_name", ""),
            "disclosure_month": log.get("disclosure_month", ""),
            "scrape_date": datetime.now().isoformat(),
            "status": log.get("status", "unknown"),
            "funds_parsed": log.get("funds_parsed", 0),
            "holdings_count": log.get("holdings_count", 0),
            "file_url": log.get("file_url"),
            "file_format": log.get("file_format"),
            "file_size_kb": log.get("file_size_kb"),
            "error_message": log.get("error_message"),
            "raw_file_path": log.get("raw_file_path"),
        })
        conn.commit()
    finally:
        conn.close()


def update_aum_from_holdings(month: str, db_path: str = DB_PATH):
    """
    Calculate equity AUM (sum of market_value_cr) from holdings
    and upsert into fund_performance.
    """
    conn = get_conn(db_path)
    try:
        df = pd.read_sql_query("""
            SELECT scheme_code, SUM(market_value_cr) as total_aum
            FROM holdings
            WHERE disclosure_month = ? AND asset_type = 'Equity'
            GROUP BY scheme_code
        """, conn, params=[month])

        rows = [
            (float(row["total_aum"]), str(row["scheme_code"]))
            for _, row in df.iterrows()
            if row["total_aum"] is not None
        ]
        conn.executemany("""
            INSERT INTO fund_performance (scheme_code, aum_cr, computed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(scheme_code) DO UPDATE SET aum_cr = excluded.aum_cr
        """, [(aum, code, datetime.now().isoformat()) for aum, code in rows])
        conn.commit()
        print(f"✅ Updated AUM for {len(rows)} funds from {month} holdings.")
    finally:
        conn.close()


def upsert_fund_metadata(records: list[dict], db_path: str = DB_PATH):
    """Upsert fund metadata from AMFI NAVAll.txt or mfdata.in."""
    conn = get_conn(db_path)
    try:
        now = datetime.now().isoformat()
        conn.executemany("""
        INSERT OR REPLACE INTO fund_metadata (
            scheme_code, scheme_name, family_id, amc_id, amc_name,
            amfi_category, category, nav, nav_date,
            isin_growth, isin_div, last_updated, is_active
        ) VALUES (
            :scheme_code, :scheme_name, :family_id, :amc_id, :amc_name,
            :amfi_category, :category, :nav, :nav_date,
            :isin_growth, :isin_div, :last_updated, 1
        )
        """, [{**r, "last_updated": now} for r in records])
        conn.commit()
    finally:
        conn.close()


def upsert_performance(records: list[dict], db_path: str = DB_PATH):
    """Upsert computed performance metrics."""
    conn = get_conn(db_path)
    try:
        now = datetime.now().isoformat()
        conn.executemany("""
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
        """, [{**r, "computed_at": now} for r in records])
        conn.commit()
    finally:
        conn.close()


# ─── NAV DAILY STORAGE ───────────────────────────────────────────────────────────

def upsert_nav_daily(scheme_code: str, nav_df: "pd.DataFrame", db_path: str = DB_PATH):
    """
    Bulk-insert NAV history rows for a fund. Skips rows already present (INSERT OR IGNORE).
    nav_df must have columns: date (datetime), nav (float).
    """
    from datetime import datetime as _dt
    conn = get_conn(db_path)
    try:
        now = _dt.now().isoformat()
        records = [
            {
                "scheme_code": str(scheme_code),
                "nav_date":    row["date"].strftime("%Y-%m-%d"),
                "nav":         float(row["nav"]),
                "inserted_at": now,
            }
            for _, row in nav_df.iterrows()
            if row["nav"] > 0
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO nav_daily (scheme_code, nav_date, nav, inserted_at) "
            "VALUES (:scheme_code, :nav_date, :nav, :inserted_at)",
            records,
        )
        conn.commit()
    finally:
        conn.close()


# ─── BATCH PERFORMANCE READS (called from data_fetcher in hot path) ────────────

def get_all_fund_performance(db_path: str = DB_PATH) -> dict:
    """
    Pull ALL performance metrics in one query → dict keyed by scheme_code.
    Returns: {scheme_code: {cagr_1y, cagr_3y, cagr_5y, expense_ratio, volatility, aum_cr}}
    Used by load_fund_data() to avoid N+1 queries.
    """
    conn = get_conn(db_path)
    try:
        rows = conn.execute("""
            SELECT scheme_code, cagr_1y, cagr_3y, cagr_5y,
                   expense_ratio, volatility, aum_cr, sharpe_ratio
            FROM fund_performance
        """).fetchall()
        return {
            str(r["scheme_code"]): {
                "cagr_1y":       r["cagr_1y"],
                "cagr_3y":       r["cagr_3y"],
                "cagr_5y":       r["cagr_5y"],
                "expense_ratio": r["expense_ratio"],
                "volatility":    r["volatility"],
                "aum_cr":        r["aum_cr"],
                "sharpe_ratio":  r["sharpe_ratio"],
            }
            for r in rows
        }
    except Exception:
        return {}
    finally:
        conn.close()


def get_fund_aum_summary(disclosure_month: Optional[str] = None, db_path: str = DB_PATH) -> dict:
    """
    Returns {scheme_code: aum_cr}.
    If disclosure_month is given, aggregates from holdings table for that month.
    Otherwise falls back to fund_performance table (pre-synced AUM).
    """
    conn = get_conn(db_path)
    try:
        if disclosure_month:
            rows = conn.execute("""
                SELECT scheme_code, SUM(market_value_cr) as total_aum
                FROM holdings
                WHERE disclosure_month = ? AND asset_type = 'Equity'
                GROUP BY scheme_code
            """, (disclosure_month,)).fetchall()
            return {
                str(r["scheme_code"]): round(float(r["total_aum"]), 2)
                for r in rows if r["total_aum"]
            }
        else:
            rows = conn.execute("""
                SELECT scheme_code, aum_cr FROM fund_performance WHERE aum_cr IS NOT NULL
            """).fetchall()
            return {str(r["scheme_code"]): r["aum_cr"] for r in rows}
    except Exception:
        return {}
    finally:
        conn.close()


# ─── HOLDINGS READ ──────────────────────────────────────────────────────────────

def get_holdings(
    disclosure_month: str,
    categories: Optional[list[str]] = None,
    amc_ids: Optional[list[str]] = None,
    scheme_codes: Optional[list[str]] = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Fetch holdings for a given month.
    Joins with fund_metadata for SEBI category.
    Filters are applied post-join for category (case-insensitive).
    """
    conn = get_conn(db_path)
    try:
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
        LEFT JOIN fund_metadata fm
               ON CAST(h.scheme_code AS TEXT) = CAST(fm.scheme_code AS TEXT)
        WHERE h.disclosure_month = ?
        """
        params = [disclosure_month]

        if amc_ids:
            placeholders = ",".join("?" * len(amc_ids))
            query += f" AND h.amc_id IN ({placeholders})"
            params.extend(amc_ids)

        if scheme_codes:
            sc_str = [str(c) for c in scheme_codes]
            placeholders = ",".join("?" * len(sc_str))
            query += f" AND CAST(h.scheme_code AS TEXT) IN ({placeholders})"
            params.extend(sc_str)

        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    # Post-filter by category (case-insensitive, strip-safe)
    if not df.empty and categories:
        selected_clean = {c.upper().replace("-", " ").strip() for c in categories}
        df["_cat_clean"] = df["category"].str.upper().str.replace("-", " ").str.strip()
        df = df[df["_cat_clean"].isin(selected_clean)].drop(columns=["_cat_clean"])

    return df.reset_index(drop=True)


def get_available_months(db_path: str = DB_PATH) -> list[str]:
    """Return list of months with holdings data, newest first."""
    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT disclosure_month FROM holdings ORDER BY disclosure_month DESC"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_scrape_status(db_path: str = DB_PATH) -> pd.DataFrame:
    """Return latest scrape status per AMC."""
    conn = get_conn(db_path)
    try:
        df = pd.read_sql_query("""
        SELECT
            amc_id, amc_name, disclosure_month,
            status, funds_parsed, holdings_count,
            file_url, error_message, scrape_date
        FROM amc_scrape_log
        WHERE id IN (
            SELECT MAX(id) FROM amc_scrape_log GROUP BY amc_id, disclosure_month
        )
        ORDER BY amc_id
        """, conn)
        return df
    finally:
        conn.close()


def get_fund_list(
    categories: Optional[list[str]] = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """Return all funds with their performance data."""
    conn = get_conn(db_path)
    try:
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

        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def get_conviction_data(
    disclosure_month: str,
    categories: list[str],
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """Compute conviction table from real DB holdings."""
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
    """Build quarterly rotation from real DB holdings."""
    months = get_available_months(db_path)
    if not months:
        return pd.DataFrame()

    rows = []
    for month in months:
        h = get_holdings(month, categories=categories, db_path=db_path)
        if h.empty:
            continue
        total = h["scheme_name"].nunique()
        grp = (
            h.groupby(["stock_name", "ticker", "sector_normalised"])["scheme_name"]
            .nunique()
            .reset_index()
        )
        grp.columns = ["stock_name", "ticker", "sector", "fund_count"]
        grp["quarter"] = _month_to_quarter(month)
        grp["fund_pct"] = (grp["fund_count"] / total * 100).round(1)
        grp["total_funds"] = total
        rows.append(grp)

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)
    latest = months[0]
    top_stocks = (
        df[df["quarter"] == _month_to_quarter(latest)]
        .nlargest(top_n, "fund_count")["stock_name"]
        .tolist()
    )
    df = df[df["stock_name"].isin(top_stocks)]

    if len(months) >= 2:
        first_q = _month_to_quarter(months[-1])
        last_q  = _month_to_quarter(months[0])
        fq = df[df["quarter"] == first_q][["stock_name", "fund_count"]].rename(columns={"fund_count": "start"})
        lq = df[df["quarter"] == last_q][["stock_name", "fund_count"]].rename(columns={"fund_count": "end"})
        trend = fq.merge(lq, on="stock_name")
        trend["trend"] = trend["end"] - trend["start"]
        trend["trend_label"] = trend["trend"].apply(
            lambda x: "📈 Accumulating" if x >= 2 else ("📉 Distributing" if x <= -2 else "➡️ Stable")
        )
        df = df.merge(trend[["stock_name", "trend", "trend_label"]], on="stock_name", how="left")

    return df


def _month_to_quarter(month_str: str) -> str:
    """Convert '2026-03' → 'Q4 FY26'."""
    try:
        y, m = int(month_str[:4]), int(month_str[5:7])
        fy = y if m >= 4 else y - 1
        q = ((m - 4) % 12) // 3 + 1
        return f"Q{q} FY{str(fy)[2:]}"
    except Exception:
        return month_str


def get_db_stats(db_path: str = DB_PATH) -> dict:
    """Return summary statistics about the database."""
    conn = get_conn(db_path)
    try:
        stats = {}
        for t in ["fund_metadata", "holdings", "nav_daily", "fund_performance", "amc_scrape_log"]:
            try:
                stats[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                stats[t] = 0

        stats["holdings_months"] = get_available_months(db_path)
        stats["holdings_amcs"] = conn.execute(
            "SELECT COUNT(DISTINCT amc_id) FROM holdings"
        ).fetchone()[0]
        return stats
    finally:
        conn.close()


def standardize_db_categories(db_path: str = DB_PATH):
    """Standardize category names in fund_metadata to canonical forms."""
    mapping = {
        "Mid-Cap":            "Mid Cap",
        "Large-Cap":          "Large Cap",
        "Small-Cap":          "Small Cap",
        "ELSS (Tax Savings)": "ELSS",
        "Flexi-Cap":          "Flexi Cap",
        "Multi-Cap":          "Multi Cap",
        "Sectoral/Thematic":  "Sectoral",
    }
    conn = get_conn(db_path)
    try:
        for old, new in mapping.items():
            conn.execute("UPDATE fund_metadata SET category = ? WHERE category = ?", (new, old))
        conn.commit()
    finally:
        conn.close()


def link_holdings_to_metadata(db_path: str = DB_PATH):
    """
    Populate scheme_code in holdings by matching scheme_names with fund_metadata.
    Uses exact match first, then fuzzy match (cutoff 0.80).
    """
    from difflib import get_close_matches

    conn = get_conn(db_path)
    try:
        # 1. Exact match
        conn.execute("""
            UPDATE holdings
            SET scheme_code = (
                SELECT scheme_code FROM fund_metadata
                WHERE fund_metadata.scheme_name = holdings.scheme_name
                LIMIT 1
            )
            WHERE scheme_code IS NULL OR scheme_code = ''
        """)
        conn.commit()

        # 2. Fuzzy match for remaining nulls
        unmatched = conn.execute(
            "SELECT DISTINCT scheme_name FROM holdings WHERE scheme_code IS NULL OR scheme_code = ''"
        ).fetchall()

        if not unmatched:
            return

        meta = conn.execute("SELECT scheme_name, scheme_code FROM fund_metadata").fetchall()
        meta_names = [m["scheme_name"] for m in meta]
        meta_map   = {m["scheme_name"]: m["scheme_code"] for m in meta}

        matched = 0
        for row in unmatched:
            name = row["scheme_name"]
            hits = get_close_matches(name, meta_names, n=1, cutoff=0.80)
            if hits:
                conn.execute(
                    "UPDATE holdings SET scheme_code = ? WHERE scheme_name = ?",
                    (meta_map[hits[0]], name),
                )
                matched += 1

        conn.commit()
        if matched:
            print(f"🔗 Linked {matched} funds via fuzzy matching")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    stats = get_db_stats()
    print("\nDB Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
