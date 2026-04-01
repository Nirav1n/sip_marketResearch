# 🇮🇳 India SIP Analyzer — Setup Guide

## Project Structure
```
india_sip_analyzer/
├── app.py              ← Streamlit dashboard (run this)
├── data_fetcher.py     ← Pulls data from AMFI India
├── metrics.py          ← Scoring & analysis engine
├── claude_analyst.py   ← Claude AI integration
├── requirements.txt    ← Python dependencies
└── README.md
```

---

## Quick Start (3 Steps)

### Step 1 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 2 — Run the dashboard
```bash
streamlit run app.py
```

### Step 3 — Open browser
Streamlit will auto-open: `http://localhost:8501`

---

## Using Claude AI Analysis

1. Get your free API key from: https://console.anthropic.com
2. Enter it in the **sidebar** of the dashboard
3. Click **"Generate Analysis"** in the AI Insights tab

Without an API key, a realistic demo analysis is shown automatically.

---

## Data Sources

| Source | Data | Cost |
|--------|------|------|
| AMFI India | NAV, Scheme Names, AMC | Free (official) |
| Simulated metrics | CAGR, Sharpe, Expense Ratio | - |
| Claude AI | Investment insights | ~$0.01/analysis |

### Upgrading to Real Metrics (Phase 2)
Replace the `enrich_with_metrics()` function in `data_fetcher.py` with:
- **Value Research API** — Returns, Sharpe, Expense Ratio (paid)
- **Morningstar API** — Deep analytics (enterprise)
- **NSE/BSE APIs** — Live NAV and holdings

---

## Customizing Macro Conditions

Edit `MACRO_CONTEXT` in `metrics.py` to update weekly:
```python
MACRO_CONTEXT = {
    "india_gdp_growth": "Strong (7%+)",
    "rbi_rate_stance": "Neutral",
    "us_fed_stance": "Cutting rates",   # ← update this
    ...
}
```

---

## Automation (Optional)

To auto-refresh data daily, add a cron job:
```bash
# Run every day at 7am
0 7 * * * cd /path/to/india_sip_analyzer && python data_fetcher.py
```

---

## Important Disclaimer

> This tool is for **research and education only**.
> It does **not** constitute financial advice.
> Past performance does not guarantee future returns.
> Consult a SEBI-registered investment advisor before investing.
# sip_marketResearch
