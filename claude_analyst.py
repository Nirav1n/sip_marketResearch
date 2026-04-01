"""
claude_analyst.py
Calls Claude API with fund data and returns structured investment analysis.
"""

import anthropic
import os
import json
from datetime import datetime


SYSTEM_PROMPT = """You are an expert Indian mutual fund and SIP investment analyst with 15+ years of experience.

Your analysis style:
- Data-driven: always reference numbers from the input
- Risk-aware: never recommend without stating downside risks
- Practical: advice should be actionable for retail Indian investors
- Concise: no fluff, no repetition

You understand Indian market nuances:
- SEBI categorization of funds
- SIP vs lumpsum dynamics
- Indian tax implications (LTCG, STCG)
- RBI policy impact on equities
- FII/DII flow effects

Always format output with clear ## headers and bullet points.
"""


def get_claude_analysis(prompt: str, api_key: str = None) -> str:
    """
    Send prompt to Claude and return analysis text.
    Uses ANTHROPIC_API_KEY env var if api_key not provided.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    if not key:
        return _mock_analysis()

    try:
        client = anthropic.Anthropic(api_key=key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text

    except Exception as e:
        return f"⚠️ Claude API error: {e}\n\n{_mock_analysis()}"


def _mock_analysis() -> str:
    """
    Fallback mock analysis when no API key is set.
    Useful for demo/testing the dashboard without spending credits.
    """
    return """
## Top 3 SIP Picks Overall

- **HDFC Mid Cap Opportunities** — Consistent 3Y CAGR above category avg with Sharpe >1.4. Ideal for 5Y+ horizon.
- **Mirae Asset Large Cap** — Low expense ratio (0.54%), strong AUM (₹37K Cr+), suits conservative-moderate investors.
- **Nippon India Banking & Financial Services** — Banking sector remains RBI-policy-sensitive but fundamentally strong.

## Category Outlook

- **Large Cap**: Stable. With FII inflows returning and Nifty earnings growth, large caps offer defensiveness. Prefer index-heavy AMCs.
- **Mid Cap**: Strong momentum. India's domestic consumption story plays out in mid-caps. Volatility is higher — ideal for 5Y+ SIP.
- **Sectoral/Thematic**: High risk-reward. IT and Banking sectors currently macro-favored. Avoid over-allocating (max 15-20%).

## Best Sector Right Now

**Banking & Finance** — RBI's neutral stance post rate-cut cycle benefits NBFCs and private banks. Credit growth at 14%. Look at ICICI Pru Banking & Financial Services.

## Risk Warnings

1. **Global Slowdown Risk** — US recession signals could trigger FII outflows, hitting Nifty 10-15% correction.
2. **Sectoral Concentration Risk** — Thematic funds can underperform for 2-3 years. Retail investors often panic-exit at bottom.

## Allocation Strategy (Moderate Investor)

| Category | Allocation |
|---|---|
| Large Cap | 40% |
| Mid Cap | 35% |
| Sectoral/Thematic | 15% |
| Liquid/Short Duration | 10% |

*Review allocation every 6 months. Rebalance if any category drifts ±10% from target.*
"""


def save_analysis(analysis: str, path: str = "latest_analysis.md"):
    """Save analysis to markdown file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    content = f"# Indian SIP Analysis\n*Generated: {timestamp}*\n\n{analysis}"
    with open(path, "w") as f:
        f.write(content)
    print(f"📄 Analysis saved to {path}")
