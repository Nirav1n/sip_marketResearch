"""
amfi_scraper.py
===============
Downloads and parses monthly portfolio Excel files from all 47 AMFI-listed AMCs.
Handles 3 Excel formats + graceful fallback + full audit logging.

Usage:
  python amfi_scraper.py                    # scrape all tier-1 AMCs for current month
  python amfi_scraper.py --tier 1           # tier 1 only
  python amfi_scraper.py --amc hdfc sbi     # specific AMCs only
  python amfi_scraper.py --month 2026-02    # specific month
  python amfi_scraper.py --dry-run          # test URLs without downloading
"""

import requests
import pandas as pd
import numpy as np
import re
import os
import io
import time
import json
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, date
from typing import Optional
from bs4 import BeautifulSoup

from amc_registry import AMC_REGISTRY, get_active_amcs, get_amc_url
from holdings_db import (
    init_db, insert_holdings, log_scrape, get_db_stats, DB_PATH
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
RAW_DIR = Path("raw_downloads")
RAW_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/vnd.ms-excel,"
              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.amfiindia.com/",
    "Origin": "https://www.amfiindia.com",
}
TIMEOUT = 30
RATE_LIMIT_SECS = 2.0  # polite delay between AMC requests

# ─── COLUMN NAME NORMALISATION ────────────────────────────────────────────────
# AMCs use different column names for the same data — we normalise them all
COLUMN_ALIASES = {
    "stock_name": [
        "name of the instrument", "name of instrument", "instrument name",
        "security name", "security", "company name", "company", "stock",
        "name", "scrip name", "scrip", "issuer", "issuer name",
    ],
    "isin": [
        "isin", "isin code", "isin no", "isin number", "isin no.",
    ],
    "sector": [
        "industry", "sector", "industry/sector", "sector/industry",
        "market segment", "asset class", "sub-category",
    ],
    "quantity": [
        "quantity", "qty", "units", "no. of shares", "no of shares",
        "number of shares", "shares", "no. of units", "face value",
    ],
    "market_value_cr": [
        "market value", "market value (rs. lacs)", "market value (rs. in lacs)",
        "market value (lakhs)", "market value (rs lacs)", "market/fair value (rs lacs)",
        "value (rs. lacs)", "value (rs in lacs)", "value rs lacs",
        "market value (cr)", "market value (crores)", "value (cr.)",
        "corpus %", "value in lakhs", "value (₹ lacs)", "amount (rs in lacs)",
        "amount", "value",
    ],
    "weight_pct": [
        "% to nav", "% of nav", "% to net assets", "% net assets",
        "percentage to nav", "percentage of nav", "% to aum",
        "% assets", "weight", "% holding", "holdings (%)",
        "% to total assets", "% to corpus", "weight (%)", "nav %",
    ],
    "rating": [
        "rating", "credit rating", "instrument rating",
    ],
    "listing": [
        "listing status", "listed / unlisted", "listed/unlisted",
        "listing", "listed",
    ],
    "asset_type": [
        "type", "asset type", "instrument type", "category",
        "security type", "asset class",
    ],
}


def _normalise_col(raw: str) -> str:
    """Map raw column name to our standard name."""
    s = raw.strip().lower().replace("\n", " ").replace("  ", " ")
    for standard, aliases in COLUMN_ALIASES.items():
        if s in aliases or any(a in s for a in aliases):
            return standard
    return s


def _find_header_row(df_raw: pd.DataFrame) -> int:
    """
    Find the actual header row in a messy Excel file.
    Looks for the row containing ISIN or % to NAV keywords.
    """
    key_terms = ["isin", "% to nav", "% to net", "weight", "nav %", "market value"]
    for i, row in df_raw.iterrows():
        row_str = " ".join(str(v).lower() for v in row.values if pd.notna(v))
        if sum(1 for k in key_terms if k in row_str) >= 2:
            return i
    return 0


def _detect_scheme_name(sheet_name: str, df_chunk: pd.DataFrame) -> str:
    """
    Extract fund scheme name from sheet name or from first rows of data.
    """
    # Sheet name is often the fund name
    if sheet_name and sheet_name.lower() not in ["portfolio", "holdings", "sheet1", "data"]:
        return sheet_name.strip()

    # Look in first 5 rows for a scheme name pattern
    for i in range(min(5, len(df_chunk))):
        row_vals = [str(v).strip() for v in df_chunk.iloc[i].values if pd.notna(v) and str(v).strip()]
        for val in row_vals:
            val_l = val.lower()
            if any(k in val_l for k in ["fund", "scheme", "direct", "growth", "plan"]):
                if len(val) > 10 and len(val) < 200:
                    return val

    return sheet_name or "Unknown Scheme"


def _normalise_weight(val) -> Optional[float]:
    """Parse weight % value — handles strings like '8.24%', '8.24', '8,24'."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace("%", "").replace(",", ".").strip()
    try:
        f = float(s)
        # If someone accidentally stored as 0-1 fraction, convert
        if 0 < f < 1.0 and "." in s:
            return round(f * 100, 4)
        return round(f, 4)
    except:
        return None


def _normalise_market_value(val, col_name: str = "") -> Optional[float]:
    """
    Normalise market value to crores.
    AMCs use lakhs, crores, millions — detect from column name.
    """
    if pd.isna(val):
        return None
    try:
        raw = float(str(val).replace(",", "").replace("₹", "").strip())
        col_l = col_name.lower()
        if "lacs" in col_l or "lakhs" in col_l or "lakh" in col_l:
            return round(raw / 100, 4)  # lakhs → crores
        elif "million" in col_l:
            return round(raw / 10, 4)  # million INR → crores (approx)
        return round(raw, 4)  # assume crores
    except:
        return None


NSE_SECTOR_MAP = {
    "RELIANCE": "Energy", "HDFCBANK": "Banking", "INFY": "IT",
    "ICICIBANK": "Banking", "TCS": "IT", "LT": "Infrastructure",
    "AXISBANK": "Banking", "KOTAKBANK": "Banking", "BAJFINANCE": "NBFC",
    "HINDUNILVR": "FMCG", "SUNPHARMA": "Pharma", "WIPRO": "IT",
    "HCLTECH": "IT", "TATAMOTORS": "Auto", "BHARTIARTL": "Telecom",
    "ITC": "FMCG", "SBIN": "Banking", "DRREDDY": "Pharma",
    "CIPLA": "Pharma", "DIVISLAB": "Pharma", "ULTRACEMCO": "Materials",
    "NTPC": "Energy", "COALINDIA": "Energy", "POWERGRID": "Energy",
}

SECTOR_NORMALISE = {
    "banks": "Banking", "bank": "Banking", "banking": "Banking",
    "finance": "NBFC", "financial services": "NBFC", "nbfc": "NBFC",
    "it": "IT", "information technology": "IT", "software": "IT",
    "pharma": "Pharma", "pharmaceuticals": "Pharma", "healthcare": "Healthcare",
    "fmcg": "FMCG", "consumer staples": "FMCG",
    "auto": "Auto", "automobile": "Auto", "automotive": "Auto",
    "infra": "Infrastructure", "infrastructure": "Infrastructure",
    "energy": "Energy", "oil": "Energy", "power": "Energy",
    "metals": "Metals", "steel": "Metals", "mining": "Metals",
    "realty": "Real Estate", "real estate": "Real Estate",
    "capital goods": "Capital Goods", "industrials": "Capital Goods",
    "telecom": "Telecom", "telecommunication": "Telecom",
    "chemicals": "Chemicals", "materials": "Materials",
    "consumer durables": "Consumer Durables",
    "media": "Media", "entertainment": "Media",
    "insurance": "Insurance",
}


def _normalise_sector(raw: str) -> str:
    if not raw or pd.isna(raw): return "Other"
    r = str(raw).strip().lower()
    for k, v in SECTOR_NORMALISE.items():
        if k in r: return v
    return str(raw).strip().title()


def _isin_to_ticker(isin: str) -> str:
    """Best-effort ISIN → NSE ticker."""
    KNOWN = {
        "INE002A01018": "RELIANCE", "INE040A01034": "HDFCBANK",
        "INE009A01021": "INFY", "INE090A01021": "ICICIBANK",
        "INE467B01029": "TCS", "INE018A01030": "LT",
        "INE238A01034": "AXISBANK", "INE237A01028": "KOTAKBANK",
        "INE296A01024": "BAJFINANCE", "INE021A01026": "ASIANPAINT",
        "INE030A01027": "HINDUNILVR", "INE585B01010": "MARUTI",
        "INE044A01036": "SUNPHARMA", "INE280A01028": "TITAN",
        "INE075A01022": "WIPRO", "INE860A01027": "HCLTECH",
        "INE155A01022": "TATAMOTORS", "INE742F01042": "ADANIPORTS",
        "INE752E01010": "POWERGRID", "INE733E01010": "NTPC",
        "INE397D01024": "BHARTIARTL", "INE154A01025": "ITC",
        "INE062A01020": "SBIN", "INE239A01024": "NESTLEIND",
        "INE669C01036": "TECHM", "INE019A01038": "JSWSTEEL",
        "INE089A01023": "DRREDDY", "INE059A01026": "CIPLA",
        "INE361B01024": "DIVISLAB", "INE481G01011": "ULTRACEMCO",
    }
    return KNOWN.get(str(isin).strip(), "")


# ─── CORE PARSER ──────────────────────────────────────────────────────────────

def parse_excel_to_holdings(
    file_bytes: bytes,
    amc_id: str,
    amc_name: str,
    disclosure_month: str,
    file_format: str = "xlsx",
) -> tuple[list[dict], list[str]]:
    """
    Parse an AMC Excel file into standardised holding records.
    Returns: (list of holding dicts, list of warning messages)
    """
    rows = []
    warnings = []

    try:
        engine = "openpyxl" if file_format in ("xlsx", "xlsm") else "xlrd"
        xl = pd.ExcelFile(io.BytesIO(file_bytes), engine=engine)
        sheet_names = xl.sheet_names
    except Exception as e:
        warnings.append(f"Could not open Excel: {e}")
        return rows, warnings

    data_source = f"{amc_name} factsheet {disclosure_month}"

    for sheet in sheet_names:
        sheet_l = sheet.strip().lower()
        # Skip cover/index/summary sheets
        if any(skip in sheet_l for skip in ["cover", "index", "summary", "disclaimer",
                                             "toc", "contents", "key", "performance"]):
            continue

        try:
            raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet,
                                header=None, engine=engine, dtype=str)
        except Exception as e:
            warnings.append(f"Sheet '{sheet}' read error: {e}")
            continue

        if raw.empty or len(raw) < 3:
            continue

        # Find header row
        header_row = _find_header_row(raw)
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet,
                           header=header_row, engine=engine, dtype=str)
        df.columns = [_normalise_col(str(c)) for c in df.columns]

        # Skip if no weight column found
        if "weight_pct" not in df.columns:
            continue

        # Drop rows that are blank or section headers
        df = df.dropna(subset=["weight_pct"])
        df = df[df["weight_pct"].astype(str).str.strip() != ""]

        # Detect scheme name
        scheme_name = _detect_scheme_name(sheet, raw)

        sheet_rows = 0
        for _, row in df.iterrows():
            weight = _normalise_weight(row.get("weight_pct"))
            if weight is None or weight <= 0 or weight > 100:
                continue

            stock_name = str(row.get("stock_name", "")).strip()
            if not stock_name or stock_name.lower() in ["nan", "total", "grand total",
                                                          "sub total", "subtotal", ""]:
                continue

            isin = str(row.get("isin", "")).strip()
            if isin.lower() in ["nan", "", "-", "na"]: isin = ""

            mv_raw = row.get("market_value_cr")
            mv_col = next((c for c in df.columns if c == "market_value_cr"), "")
            market_value = _normalise_market_value(mv_raw, mv_col)

            sector_raw = str(row.get("sector", "")).strip()
            sector_raw = "" if sector_raw.lower() in ["nan", "", "-"] else sector_raw

            asset_type = str(row.get("asset_type", "Equity")).strip()
            asset_type = "Equity" if asset_type.lower() in ["nan", "", "-"] else asset_type

            ticker = _isin_to_ticker(isin) if isin else ""

            rows.append({
                "disclosure_month": disclosure_month,
                "amc_id": amc_id,
                "amc_name": amc_name,
                "scheme_name": scheme_name,
                "scheme_code": None,  # matched later via fund_metadata
                "stock_name": stock_name,
                "isin": isin or None,
                "ticker": ticker or None,
                "sector": sector_raw or None,
                "sector_normalised": _normalise_sector(sector_raw),
                "asset_type": asset_type,
                "quantity": _normalise_weight(row.get("quantity")),
                "market_value_cr": market_value,
                "weight_pct": weight,
                "rating": str(row.get("rating", "")).strip() or None,
                "listing": str(row.get("listing", "")).strip() or None,
                "data_source": data_source,
            })
            sheet_rows += 1

        if sheet_rows > 0:
            print(f"    📄 Sheet '{sheet}' → {sheet_rows} holdings for '{scheme_name[:50]}'")

    return rows, warnings


# ─── URL DISCOVERY ────────────────────────────────────────────────────────────

def discover_excel_url(amc: dict, disclosure_month: str) -> Optional[str]:
    """
    For AMCs without a known file_pattern, scrape their portfolio page
    to find the Excel download link.
    """
    page_url = amc["portfolio_url"]
    try:
        resp = requests.get(page_url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find all links
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            href_l = href.lower()

            # Look for Excel/CSV files
            if not any(ext in href_l for ext in [".xlsx", ".xls", ".csv"]):
                continue

            # Look for portfolio/monthly keywords
            link_text = (a_tag.get_text() + href_l).lower()
            if any(k in link_text for k in ["portfolio", "monthly", "holding", "disclosure"]):
                # Make absolute URL
                if href.startswith("http"):
                    return href
                elif href.startswith("/"):
                    from urllib.parse import urlparse
                    base = urlparse(page_url)
                    return f"{base.scheme}://{base.netloc}{href}"
                else:
                    return f"{page_url.rstrip('/')}/{href}"

    except Exception as e:
        print(f"  ⚠️  URL discovery failed for {amc['amc_id']}: {e}")

    return None


# ─── MAIN SCRAPER ─────────────────────────────────────────────────────────────

def scrape_amc(
    amc: dict,
    disclosure_month: str,
    dry_run: bool = False,
) -> dict:
    """
    Scrape one AMC. Downloads their Excel, parses it, stores to DB.
    Returns a log record.
    """
    log = {
        "amc_id": amc["amc_id"],
        "amc_name": amc["name"],
        "disclosure_month": disclosure_month,
        "status": "failed",
        "funds_parsed": 0,
        "holdings_count": 0,
        "file_url": None,
        "file_format": None,
        "file_size_kb": None,
        "error_message": None,
        "raw_file_path": None,
    }

    print(f"\n{'─'*60}")
    print(f"🏢 {amc['name']} [{amc['amc_id']}] (Tier {amc['tier']})")

    # Step 1: Get the file URL
    file_url = get_amc_url(amc, disclosure_month)

    if not file_url:
        print(f"   🔍 Discovering Excel URL from {amc['portfolio_url']}")
        if not dry_run:
            file_url = discover_excel_url(amc, disclosure_month)

    if not file_url:
        log["status"] = "failed"
        log["error_message"] = "Could not find Excel URL"
        print(f"   ❌ No Excel URL found")
        return log

    log["file_url"] = file_url
    print(f"   🔗 Candidate URL: {file_url[:80]}...")

    if dry_run:
        log["status"] = "dry_run"
        print(f"   ✅ Dry run — skipping download")
        return log

    # Step 2: Download file (with fallback discovery on failure)
    try:
        resp = requests.get(file_url, headers=HEADERS, timeout=TIMEOUT)
        
        # If direct link fails (404 or 403), try discovery as fallback
        if resp.status_code in [403, 404]:
            print(f"   ⚠️  Direct link failed ({resp.status_code}). Trying discovery fallback...")
            discovered_url = discover_excel_url(amc, disclosure_month)
            if discovered_url and discovered_url != file_url:
                print(f"   ✨ Found new URL via discovery: {discovered_url[:80]}...")
                file_url = discovered_url
                log["file_url"] = file_url
                resp = requests.get(file_url, headers=HEADERS, timeout=TIMEOUT)
        
        resp.raise_for_status()

        file_bytes = resp.content
        file_size_kb = len(file_bytes) / 1024
        log["file_size_kb"] = round(file_size_kb, 1)

        if file_size_kb < 5:
            log["status"] = "failed"
            log["error_message"] = f"File too small ({file_size_kb:.1f} KB) — likely error page"
            print(f"   ❌ File too small: {file_size_kb:.1f} KB")
            return log

        print(f"   📥 Downloaded: {file_size_kb:.1f} KB")

        # Detect format
        url_l = file_url.lower()
        if ".xlsx" in url_l: fmt = "xlsx"
        elif ".xls" in url_l: fmt = "xls"
        elif ".csv" in url_l: fmt = "csv"
        else: fmt = "xlsx"  # assume xlsx
        log["file_format"] = fmt

        # Save raw file
        safe_name = re.sub(r"[^\w]", "_", amc["amc_id"])
        raw_path = RAW_DIR / f"{safe_name}_{disclosure_month}.{fmt}"
        raw_path.write_bytes(file_bytes)
        log["raw_file_path"] = str(raw_path)
        print(f"   💾 Saved to {raw_path}")

    except requests.exceptions.RequestException as e:
        log["status"] = "failed"
        log["error_message"] = f"Download error: {e}"
        print(f"   ❌ Download failed: {e}")
        return log

    # Step 3: Parse
    try:
        if fmt == "csv":
            # Simple CSV parsing
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
            # TODO: add CSV parser path
            log["status"] = "partial"
            log["error_message"] = "CSV format — basic parse only"
            return log

        holdings, warnings = parse_excel_to_holdings(
            file_bytes, amc["amc_id"], amc["name"], disclosure_month, fmt
        )

        if warnings:
            for w in warnings:
                print(f"   ⚠️  {w}")

        if not holdings:
            log["status"] = "failed"
            log["error_message"] = "No holdings rows parsed from file"
            print(f"   ❌ No holdings found in file")
            return log

        # Count unique schemes
        schemes = set(h["scheme_name"] for h in holdings)
        log["funds_parsed"] = len(schemes)
        print(f"   📊 Parsed {len(holdings)} holdings across {len(schemes)} schemes")

        # Step 4: Store to DB
        inserted = insert_holdings(holdings)
        log["holdings_count"] = inserted
        log["status"] = "success"
        print(f"   ✅ Inserted {inserted} rows to DB")

    except Exception as e:
        log["status"] = "failed"
        log["error_message"] = f"Parse error: {e}"
        print(f"   ❌ Parse error: {e}")
        import traceback
        traceback.print_exc()

    return log


def run_scrape(
    tiers: list[int] | None = None,
    amc_ids: list[str] | None = None,
    disclosure_month: str | None = None,
    dry_run: bool = False,
):
    """
    Main scrape runner.
    """
    month = disclosure_month or datetime.now().strftime("%Y-%m")
    print(f"\n{'='*60}")
    print(f"🚀 AMFI Holdings Scraper — {month}")
    print(f"{'='*60}")

    # Init DB
    init_db()

    # Select AMCs
    if amc_ids:
        amcs = [a for a in AMC_REGISTRY if a["amc_id"] in amc_ids and a["active"]]
    elif tiers:
        amcs = [a for t in tiers for a in get_active_amcs(tier=t)]
    else:
        amcs = get_active_amcs(tier=1)  # default: tier 1 only

    print(f"📋 Scraping {len(amcs)} AMCs for month {month}")
    print(f"   AMCs: {', '.join(a['amc_id'] for a in amcs)}")
    if dry_run: print("   ⚠️  DRY RUN — no files will be downloaded")

    results = {"success": 0, "failed": 0, "partial": 0, "dry_run": 0}

    for amc in amcs:
        try:
            log = scrape_amc(amc, month, dry_run=dry_run)
            log_scrape(log)
            results[log["status"]] = results.get(log["status"], 0) + 1
        except Exception as e:
            print(f"   💥 Unexpected error for {amc['amc_id']}: {e}")
            results["failed"] += 1
        time.sleep(RATE_LIMIT_SECS)

    # Summary
    print(f"\n{'='*60}")
    print(f"✅ Scrape complete — {month}")
    print(f"   Success: {results['success']}  |  Failed: {results['failed']}  "
          f"|  Partial: {results['partial']}")

    if not dry_run:
        stats = get_db_stats()
        print(f"\nDB Summary:")
        print(f"   Total holdings rows : {stats.get('holdings', 0):,}")
        print(f"   AMCs with data      : {stats.get('holdings_amcs', 0)}")
        print(f"   Months stored       : {', '.join(stats.get('holdings_months', []))}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMFI Holdings Scraper")
    parser.add_argument("--tier", type=int, nargs="+", help="AMC tiers to scrape (1, 2, 3)")
    parser.add_argument("--amc", type=str, nargs="+", help="Specific AMC IDs")
    parser.add_argument("--month", type=str, help="Disclosure month YYYY-MM")
    parser.add_argument("--dry-run", action="store_true", help="Test URLs without downloading")
    parser.add_argument("--all", action="store_true", help="Scrape all active AMCs")
    args = parser.parse_args()

    tiers = args.tier if not args.all else [1, 2, 3]
    run_scrape(
        tiers=tiers,
        amc_ids=args.amc,
        disclosure_month=args.month,
        dry_run=args.dry_run,
    )