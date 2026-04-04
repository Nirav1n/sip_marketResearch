"""
claude_analyst.py v4
Advisor AI — all prompts built from the EXACT data shown on screen.
No generic advice. Every insight references actual fund names, real numbers,
actual stocks from the conviction table.
"""

import anthropic, os
from datetime import datetime

SYSTEM = """You are Arjun, a SEBI-registered investment advisor with 12 years experience.

Rules you never break:
- ONLY reference funds, stocks, and numbers from the data given to you
- NEVER invent fund names, stock names, or return figures
- If a field shows None or N/A, say "AUM data not available" — do not guess
- Speak like an advisor sitting across the table, not like a report
- Plain English. Explain WHY behind every number
- Use ₹ for amounts. Use Indian format (lakhs, crores)
- Specific allocation percentages with reasoning
- 2-3 sentences max per point — concise and actionable"""


def get_claude_analysis(prompt:str, api_key:str=None) -> str:
    key = api_key or os.environ.get("ANTHROPIC_API_KEY","")
    if not key:
        return _demo()
    try:
        client = anthropic.Anthropic(api_key=key)
        r = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=2000,
            system=SYSTEM, messages=[{"role":"user","content":prompt}]
        )
        return r.content[0].text
    except Exception as e:
        return f"⚠️ API error: {e}\n\n{_demo()}"


def build_fund_filter_prompt(
    selected_cats:list, filters:dict, top_funds:list,
    worst_funds:list, cat_summary:dict, total_matching:int,
    group:str, sort_by:str, sort_asc:bool,
) -> str:
    """
    Build advisor prompt from EXACT data currently shown on the All Funds page.
    top_funds = the funds actually visible on page 1 of the table.
    """
    return f"""
You are advising a user who has applied these EXACT filters on our mutual fund platform:

## WHAT THE USER FILTERED
Category group: {group}
Specific categories: {selected_cats}
Minimum 1Y CAGR: {filters.get('min_cagr_1y',0)}%
Minimum 3Y CAGR: {filters.get('min_cagr_3y',0)}%
Minimum 5Y CAGR: {filters.get('min_cagr_5y',0)}%
Maximum expense ratio: {filters.get('max_er',2.5)}%
Minimum Sharpe ratio: {filters.get('min_sharpe',0)}
Minimum AUM: ₹{filters.get('min_aum',0):,} Cr
Sorted by: {sort_by} ({'ascending' if sort_asc else 'descending'})
Total funds matching these exact filters: {total_matching}

## TOP FUNDS FROM THIS FILTERED LIST (what the user sees on screen right now)
{top_funds}

## WEAKEST FUNDS IN THIS LIST (bottom of filtered results)
{worst_funds}

## CATEGORY AVERAGES WITHIN THIS FILTER
{cat_summary}

## YOUR ANALYSIS — based ONLY on the above data

### 1. What your filter tells me
In plain English: what does this combination of categories + thresholds reveal about what you're looking for? What's your implied risk appetite?

### 2. Top 3 picks from YOUR list
Name exactly 3 funds from the "TOP FUNDS" data above. For each:
"[Exact fund name] — [1-sentence plain English description]. At [X]% 3Y CAGR with [Y] Sharpe, this fund [comparison to category avg]. The [Z]% expense ratio means on ₹10,000/month SIP over 10 years you pay approximately ₹[calculated amount] in fees. Risk: [specific risk in one sentence]."
If AUM shows as None or N/A — say "AUM not disclosed via free data sources."

### 3. The expense ratio reality
Compare the cheapest vs most expensive fund in your list. Calculate the fee difference on ₹10,000/month over 20 years.

### 4. 2 specific risks for THIS filter
Not generic market risk. Specific to the categories and return thresholds the user set.

### 5. Suggested allocation
₹10,000/month split across 2-3 funds from YOUR list above. Exact fund names + exact percentages + one-line reasoning.

Be specific. Reference actual data. If something is missing (like AUM), say so clearly.
"""


def build_stock_holdings_prompt(
    selected_cats:list,
    total_funds:int,
    total_stocks:int,
    top10_stocks:list,
    acc_stocks:list,
    dis_stocks:list,
    stable_stocks:list,
    quarters:list,
    data_source_type:str,  # 'real' or 'representative'
    disclosure_month:str|None,
) -> str:
    """
    Build advisor prompt from EXACT stock conviction data shown on holdings page.
    """
    source_note = (
        f"Data source: REAL holdings from AMFI factsheet disclosures ({disclosure_month})"
        if data_source_type == "real"
        else "Data source: Representative holdings based on SEBI category mandates (not real fund disclosures yet)"
    )

    return f"""
You are advising on stock-level mutual fund holdings intelligence.

## CONTEXT
{source_note}
Categories analysed: {selected_cats}
Total funds analysed: {total_funds}
Unique stocks found across all funds: {total_stocks}
Quarters shown: {quarters[0] if quarters else 'N/A'} to {quarters[-1] if quarters else 'N/A'}

## TOP 10 MOST HELD STOCKS (from conviction table — exact data shown on screen)
{top10_stocks}

## STOCKS BEING ACCUMULATED (fund count rising over quarters)
{acc_stocks if acc_stocks else 'None identified in this period'}

## STOCKS BEING DISTRIBUTED (fund count falling over quarters)
{dis_stocks if dis_stocks else 'None identified in this period'}

## STABLE HOLDINGS (no significant change)
{stable_stocks[:5] if stable_stocks else 'None'}

## YOUR ANALYSIS

### What fund managers are collectively betting on
Look at the top 10 list. What sectors dominate? What does the conviction score tell us about fund manager consensus? Name the top 3 stocks specifically.

### The accumulation signal
For each stock in the accumulating list: why might fund managers be adding now? Connect to macro (RBI rates, USD/INR, govt spending, sector tailwinds). Be specific to the actual stocks named.

### The distribution warning
For distributing stocks: what might the exit mean? "When multiple funds reduce [stock name] in the same period, it typically signals..."

### Diversification reality check
With [X] unique stocks across [Y] funds in [categories]: are SIP investors actually diversified? What is the effective concentration?

### One non-obvious insight
Something in this holdings data that a retail investor would miss.

{"⚠️ NOTE: This analysis is based on representative data modelled from SEBI category mandates, not actual monthly disclosures. Run the AMFI scraper to get real holdings." if data_source_type == 'representative' else ""}
"""


def build_comparison_prompt(funds:list) -> str:
    """
    Build advisor prompt for the fund comparator page.
    funds = exact list of funds the user selected with their real metrics.
    """
    names = [f["scheme_name"] for f in funds]
    return f"""
You are comparing {len(funds)} mutual funds for an investor who selected them side by side.

## THE EXACT FUNDS BEING COMPARED
{funds}

## YOUR ANALYSIS

### The one-line verdict
Which single fund would you pick for a 10-year SIP and why? One sentence.

### Fund by fund — plain English
For each fund:
"**[exact scheme_name]**: [What kind of fund it is in plain words]. Returns: {'{'}1Y/3Y/5Y CAGR{'}'}. Expense ratio [X]% means [cost impact in rupees over 10 years on ₹10k/month]. [Main risk in one sentence]. Best for: [investor type]."
If AUM is None — say "AUM not available from free sources."

### Head to head: the numbers that matter
- Which has the most CONSISTENT returns across 1Y/3Y/5Y? (consistency = good long-term SIP)
- Which gives best return per rupee of fees? (CAGR ÷ expense_ratio)
- Which has the safest risk profile? (Sharpe ratio + volatility)

### The cost of the wrong choice
If an investor picks the fund with the lowest composite score instead of the highest, what does that cost on ₹10,000/month over 20 years? Calculate approximately.

### Final allocation table
| Investor type | Best fund | % allocation |
|---|---|---|
| Conservative (10yr) | [fund name] | [%] |
| Moderate (12yr) | [fund name] | [%] |
| Aggressive (15yr+) | [fund name] | [%] |

Only reference the {len(funds)} funds given. No other suggestions.
"""


def _demo() -> str:
    return """
## What your filter tells me

You've set a minimum 3Y CAGR threshold with an expense ratio cap — that immediately tells me you're not chasing recent momentum, you want consistent compounders. That's the right instinct for a 7-10 year SIP horizon.

## Top 3 picks from your list

Add your Anthropic API key in the sidebar to get a live AI analysis based on the exact funds shown in your filter results. The demo can't reference specific funds because it doesn't know which ones you've filtered.

## How to get real analysis

1. Enter your Anthropic API key in the sidebar (Settings)
2. Apply your filters
3. Click "Get Advisor Analysis" — Claude will read the exact fund names and numbers from your screen

---
*Demo mode — API key required for personalised fund analysis*
"""