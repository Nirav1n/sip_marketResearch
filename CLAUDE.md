# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

India SIP Analyzer is a Streamlit-based web app for researching Indian mutual funds. It computes real CAGR from NAV history, shows stock-level holdings intelligence across fund portfolios, and provides Claude AI-powered fund comparisons. The core principle is integrity-first: all metrics are sourced from real data (AMFI, mfapi.in, mfdata.in), never estimated or hardcoded.

## Running the App

```bash
# Install dependencies
pip install -r requirements.txt

# Start the dashboard
streamlit run app.py
# Opens at http://localhost:8501
```

**First run** auto-creates `sip_analyzer.db` (SQLite), seeds fund metadata from AMFI NAVAll.txt, and shows "Awaiting Sync" for unsynced funds — this is expected behavior.

**AI insights** require an Anthropic API key entered in the sidebar at runtime (Claude Sonnet 4).

## Data Sync Commands

```bash
# Sync holdings for top N funds per category (recommended for dev: 10-20)
python sync_universe.py 20

# Sync all missing funds (1600+ funds, takes hours)
python sync_universe.py

# Monthly scheduler (runs on 11th at 08:00)
python scheduler.py

# Force scrape now (optional: specify AMC names)
python scheduler.py --run-now
python scheduler.py --run-now hdfc icici

# Only sync missing CAGR (no factsheet scraping)
python scheduler.py --sync-cagr
```

## Architecture

### Module Responsibilities

| Module | Role |
|--------|------|
| `app.py` | Streamlit entry point; calls `startup_check()` which inits DB and seeds metadata |
| `data_fetcher.py` | Loads equity fund universe from AMFI; enriches with performance metrics from DB |
| `holdings_db.py` | All SQLite schema + query logic (no business logic here) |
| `holdings_engine.py` | Analytics layer: conviction tables, rotation data, fund sync orchestration |
| `claude_analyst.py` | Builds Claude prompts from exact on-screen data; calls Claude API |
| `nav_processor.py` | Fetches NAV history from mfapi.in; computes CAGR, volatility, Sharpe, drawdown |
| `mf_api_client.py` | REST client for mfdata.in (scheme details, holdings, AUM) |
| `amfi_factsheet_scraper.py` | Downloads SEBI-mandated XLSX factsheets from AMC websites |
| `scheduler.py` | Cron runner; triggers scraper + CAGR sync on 11th of each month |
| `migration_metadata.py` | Seeds `fund_metadata` table from AMFI NAVAll.txt on first startup |
| `sync_universe.py` | Bulk sync driver for iterating missing holdings across all funds |

### Data Flow

1. **Startup**: `app.py` → `init_db()` → `migration_metadata.migrate()` (AMFI → `fund_metadata` table)
2. **Fund browser**: `data_fetcher.load_fund_data()` fetches AMFI universe, batch-enriches from `fund_performance` (single SQL query, no N+1)
3. **Holdings sync**: Dashboard "Sync" button → `holdings_engine.sync_fund()` → `mf_api_client` → `holdings_db.insert_holdings()`
4. **AI analysis**: Page builds exact displayed data → `claude_analyst.build_*_prompt()` → Claude API → response shown in sidebar

### External APIs

| Source | Data | Endpoint |
|--------|------|----------|
| AMFI India | Fund universe, NAV | `www.amfiindia.com/spages/NAVAll.txt` |
| mfapi.in | NAV history (daily) | `api.mfapi.in/mf/{scheme_code}` |
| mfdata.in | Holdings, scheme details, AUM | REST API `mfdata.in/api/v1/*` |
| Anthropic | LLM insights | Claude Sonnet 4 via `anthropic` SDK |

### Database (SQLite: `sip_analyzer.db`)

Key tables and their purpose:
- `fund_metadata` — master list of all equity funds (scheme_code PK)
- `fund_performance` — CAGR (1/3/5/10y), volatility, Sharpe, AUM, expense ratio
- `holdings` — monthly stock holdings per fund (disclosure_month, scheme_code, isin unique)
- `nav_daily` — raw daily NAV values for CAGR computation
- `conviction_cache` — pre-computed conviction scores (performance optimization)
- `amc_scrape_log` — audit trail for factsheet scrape attempts

WAL mode and `synchronous=NORMAL` are set on every connection for performance. Always use `holdings_db.get_connection()` rather than opening SQLite directly.

### Caching

- `mf_cache/` — JSON cache for API responses (6–24 hour TTL, checked before any network call)
- `raw_downloads/` — downloaded XLSX factsheets
- Streamlit `@st.cache_data` with TTL on expensive data loads

## Key Design Constraints

**Integrity-first**: Never invent or estimate metrics. If data is missing, display "Awaiting Sync" or `None`. Every value is tagged with its source (`mfapi.in`, `AMFI`, `Factsheet Verified`, `Awaiting Sync`).

**Rate limiting**: 0.15s delay per fund in `nav_processor` (mfapi.in), 2–4s delay in sync loops (mfdata.in). Do not remove these.

**Batch queries**: `holdings_db.get_all_fund_performance()` returns all funds in one SQL call. Avoid adding per-fund queries inside loops.

**AI prompts are data-grounded**: `claude_analyst.py` builds prompts from exact fund names, CAGR values, expense ratios, and stock holdings visible on screen — not generic descriptions. Maintain this principle when modifying prompts.

## Equity Category Filtering

AMFI uses 48+ sub-categories. `data_fetcher.py` maps these to 13 SEBI categories (`EQUITY_CATEGORIES` dict) and filters to Direct Growth plans only. When adding or changing category logic, update this mapping — it drives all filtering in the fund browser.

## Sharpe Ratio

Computed in `nav_processor.py` using **6.5% risk-free rate** (approximate RBI repo rate). If this needs updating, change the constant there.
