# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Who I Am

**Nickson** — fullstack developer, 25 years old, active investor. I build things and I invest; this project sits at the intersection of both.

## What We Are Building

A research platform that aggregates **real stock-level holdings data from all Indian AMCs** (mutual fund companies) and surfaces patterns that are invisible to a regular investor browsing Groww or Zerodha.

The end goal is a **publicly published website** where anyone can:
- See what every major AMC actually holds (not just the top 10 — the full portfolio)
- Track how funds are **shifting between sectors** over time (e.g. HDFC moving from IT to Pharma month-on-month)
- Spot **conviction builds and exits** — when multiple big AMCs start buying the same stock, or quietly selling it
- Compare AMC behaviour — how do the aggressive AMCs (Quant, Motilal) differ from conservative ones (PPFAS, HDFC) in their actual allocation decisions
- Use all of this to make **better personal investment decisions** — not just picking a fund by its CAGR but understanding what's inside it and whether the fund manager's behaviour aligns with your thesis

## The Core Analytical Vision

The holdings data is just the raw input. The real value is in the analysis layer:

- **Sector rotation tracking** — which sectors are AMCs collectively moving into or out of each month
- **Conviction scoring** — stocks held by many AMCs with increasing weight = strong institutional conviction
- **Smart money flow** — where are the top-performing funds allocating fresh capital
- **AMC style fingerprinting** — characterise each AMC's actual behaviour (momentum vs value vs quality vs defensive) from their holdings, not their marketing material
- **Overlap analysis** — how similar are two funds really, at the stock level
- **Hidden concentration risk** — if you hold 3 "different" funds, how many unique stocks do you actually own

## My Investment Context

I use this research to make my own investment decisions. The platform is built from the perspective of someone who wants to invest like the smart money — understanding what large institutional players are doing with their portfolios before deciding where to put my own money.

---

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run app.py

# Run the monthly AMC portfolio scraper
python scheduler.py --run-now          # immediate run
python scheduler.py --tier 1 2         # specific tiers only
python amfi_scraper.py --dry-run       # test URLs without downloading
python amfi_scraper.py --month 2026-02 # scrape a specific month
```

There is no test framework. To verify components manually:
```bash
python -c "from data_fetcher import load_fund_data; df = load_fund_data(force_refresh=True); print(len(df))"
python -c "from market_data import get_market_snapshot; print(get_market_snapshot())"
python -c "from holdings_db import init_db, get_db_stats; init_db(); print(get_db_stats())"
```

## Architecture

**Streamlit multi-page app** for Indian mutual fund (SIP) research. `app.py` is the landing page; sub-pages live in `pages/` and are auto-loaded by Streamlit.

### Page → Data Module Mapping

| Page | Primary Module | Purpose |
|------|---------------|---------|
| `pages/1_Home.py` | `market_data.py` | Live Nifty 50, Sensex, commodities via yfinance |
| `pages/2_Stock_Holdings.py` | `holdings_engine.py` | Stock-level conviction & rotation from AMC factsheets |
| `pages/3_All_Funds.py` | `data_fetcher.py` | Full fund browser with CAGR/AUM/Sharpe filters |
| `pages/4_Compare_Funds.py` | `data_fetcher.py` | Side-by-side fund comparison with Claude verdict |

### Core Data Pipeline

1. **Fund metadata**: `data_fetcher.py` fetches from AMFI NAVAll.txt (all ~50 AMCs, Direct Growth schemes only), computes real CAGR from mfapi.in NAV history, enriches with AUM, TER, Sharpe, and composite score. Results cached in `fund_data.csv` (24h TTL).

2. **Holdings intelligence**: `amfi_scraper.py` downloads AMC portfolio Excel files monthly (scheduled via `scheduler.py` on the 11th — AMFI disclosure deadline), parses 3 different Excel formats, and stores stock-level holdings into `sip_analyzer.db`. `holdings_engine.py` serves this data to the UI with a `data_source: real` flag, falling back to representative SEBI-category data if the DB is empty.

3. **AI analysis**: `claude_analyst.py` wraps the Claude API (model: `claude-sonnet-4-20250514`, max 2000 tokens). API key is entered per-session via sidebar or read from `ANTHROPIC_API_KEY` env var; absent a key, it returns demo placeholder text. Prompts are built from the exact funds/stocks currently on screen.

### Caching Strategy

- **`@st.cache_data(ttl=3600)`** on `load_fund_data()` — in-process Streamlit cache
- **`fund_data.csv`** — file cache for fund metrics (24h TTL via mtime check)
- **`market_cache.json`** — live market prices (30min TTL)
- **`mf_cache/*.json`** — per-fund NAV/CAGR data (24–48h TTL via `_cget()`/`_cset()` in `data_fetcher.py`)

### Database (`sip_analyzer.db`)

SQLite with WAL mode. Key tables:
- `holdings` — monthly stock-level holdings snapshots (accumulates over time)
- `fund_metadata` — master fund list from AMFI
- `amc_scrape_log` — audit trail of scraper runs
- `fund_performance` — optional computed metrics cache

Schema managed in `holdings_db.py`; `amc_registry.py` holds the master list of 47 AMC names + portfolio page URLs.

### State Management

Pages are stateless — filters reset on navigation. The Claude API key is transient (sidebar input only). All persistent state lives in `sip_analyzer.db` or the file caches above.

---

## Holdings Data Pipeline — Current Status

### Data Origin

All Indian AMCs are legally required (SEBI Regulation 59A + Master Circular SEBI/HO/IMD/IMD-PoD-1/P/CIR/2024/90) to publish monthly portfolio holdings by the **10th of each month** as downloadable Excel files — on their own website AND on `amfiindia.com/online-center/portfolio-disclosure`. The scraper runs on the **11th** via `scheduler.py`.

### AMC Scraper Architecture (`amfi_scraper.py`)

Two URL resolution strategies per AMC:
1. **`file_pattern`** — a direct URL template with date tokens, expanded by `get_amc_url()` in `amc_registry.py`
2. **`discover_excel_urls()`** — scrapes the AMC's `portfolio_url` page and extracts all `.xlsx`/`.xls` links

For AMCs with `format: "excel_per_fund"` (HDFC), the scraper downloads one file per fund scheme (~100 files) instead of one multi-sheet file. The scraper loops over all URLs and accumulates holdings before a single DB insert.

### Confirmed Working AMCs

| AMC | Method | Notes |
|-----|--------|-------|
| **HDFC** | Page scrape | `hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio` → ~109 xlsx links (one per fund). Format: `excel_per_fund`. |
| **SBI** | Direct URL template | `file_pattern` uses `{LAST_DAY_ORD}` + `{MMMM_LOWER}` + `{YYYY}` tokens. e.g. `all-schemes-monthly-portfolio---as-on-30th-april-2026.xlsx` |
| **Nippon India** | Page scrape | `mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures` → `.xls` links at `/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-*.xls` |
| **Groww** | Page scrape | `growwmf.in/statutory-disclosure/portfolio` → CDN xlsx. 58 sheets per file (one per scheme). |
| **360 ONE** | Page scrape | `360.one/asset/mutual-funds/downloads/` → S3 bucket URLs with content hashes (must scrape each month). |

### AMCs Blocked — Need Playwright or Manual Download

These AMCs load portfolio links via JavaScript after page load. Simple HTTP requests get an empty HTML shell.

| AMC | Blocker | Notes |
|-----|---------|-------|
| ICICI Prudential | JS-SPA + browser session | Archive CDN found: `archive.icicipruamc.com/download/Monthly_Portfolio_{Mon}_{YY}.xlsx` — but needs browser session cookie to reach it via redirect from `icicipruamc.com/download/`. |
| Kotak | Bot protection | perfdrive.com bot mitigation blocks all automated requests. |
| Aditya Birla | JS-SPA (jQuery AJAX) | Portfolio links loaded via AJAX after page load. |
| Axis | JS-SPA (Next.js) | Both `axismf.com` and `transact.axismf.com` are fully client-rendered. |
| Mirae Asset | JS-SPA (Sitefinity CMS) | REST search API exists at `/restapi/search/suggestions` — returns file titles but not URLs. |
| DSP | JS-SPA (Storyblok CMS) | Only CFI file found in static HTML; portfolio files loaded dynamically. |
| Franklin Templeton | JS-SPA (Vue.js + prerender.io) | CDN at `franklintempletonindia.com/content/dam/ftindia/pdf/portfolio/` returns HTML (soft 404). |

**Workaround**: Download manually from `amfiindia.com/online-center/portfolio-disclosure` (AMFI centralises all AMC uploads) and place in `raw_downloads/manual/{amc_id}_{YYYY-MM}.xlsx`. The parser handles these the same way.

### Custom Date Tokens in `get_amc_url()` (`amc_registry.py`)

| Token | Example | Used by |
|-------|---------|---------|
| `{YYYY-MM}` | `2026-04` | generic |
| `{YYYY}` | `2026` | SBI |
| `{MMMM_LOWER}` | `april` | SBI |
| `{LAST_DAY_ORD}` | `30th` | SBI |
| `{MON_YY}` | `APR26` | generic |
| `{MMMYYYY}` | `APR2026` | generic |
| `{MON_YY_TITLE}` | `Apr_26` | ICICI Pru (future) |

### Excel Format Variants

Three formats seen across AMCs — parser auto-detects via `_find_header_row()`:

- **`excel_multi`** — one file, multiple sheets (one sheet per fund). Used by SBI, Groww, Nippon India.
- **`excel_per_fund`** — one file per fund scheme, single sheet each. Used by HDFC (~109 files/month).
- **`excel_detect`** — unknown at config time; parser probes for the header row.

### To Resume This Work (AMC URL Discovery)

1. Go to `amfiindia.com/online-center/portfolio-disclosure` → select Monthly
2. Copy the direct Excel download URL for each blocked AMC
3. Paste it here — pattern will be tested against older months automatically and added to `amc_registry.py`
4. Angel One factsheet PDF pattern also shelved: `cms.angelonemf.com/amc-cms/wp-content/uploads/formidable/15/Factsheet-Angel-One-Mutual-Fund-Schemes-{Mon}-{YYYY}.pdf` (confirmed working Jan–Apr 2026)

---

## Lessons Learned — Token Efficiency

Mistakes made during AMC URL discovery. Avoid repeating these.

**Before touching any AMC:**
- Read `amc_registry.py` first — notes and status are already there
- One `curl -sI --max-time 5` HEAD check to confirm the site is alive before any deeper work

**URL discovery order (cheapest → most expensive):**
```
1. curl sitemap.xml | grep -i portfolio     # finds real page URLs instantly
2. curl the page, grep for .xlsx/.xls hrefs  # server-rendered pages expose links
3. Check Content-Type on every 200 response  # 200 + text/html = fake, not Excel
4. Max 3 CDN pattern guesses, then stop
5. If blocked → mark "needs Playwright", move on immediately
```

**Never do these:**
- Declare a URL working based on HTTP 200 alone — always verify `Content-Type: application/vnd.ms-excel` or `openxmlformats`
- Run the `deep-research` workflow for factual research questions — a single Agent prompt is enough and costs 10x less
- Investigate a site further before confirming it's alive (e.g. mfdata.in was dead, cost many rounds)
- Guess CDN URL patterns before checking sitemap and page HTML first
- Spawn a new Agent on a question already researched — use `SendMessage` to continue the existing one
- Try `pip`/`python`/`python3` blindly on Windows — find the real Python path first: `Get-ChildItem C:\,"C:\Program Files\" -Recurse -Filter python.exe -ErrorAction SilentlyContinue | Where-Object { $_.FullName -notlike "*WindowsApps*" }`
