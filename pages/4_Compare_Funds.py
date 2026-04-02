"""
pages/4_Compare_Funds.py
AI-powered fund comparator — select up to 3 funds, full side-by-side view + Claude analysis.
Improvement #7.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from data_fetcher import load_fund_data
from claude_analyst import get_claude_analysis

st.set_page_config(page_title="Compare Funds", page_icon="⚖️", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.hdr{font-size:1.8rem;font-weight:700;color:#f1f5f9;margin:0 0 4px;}
.sub{color:#4b5563;font-size:.88rem;margin:0 0 24px;}
.section-hdr{font-size:.9rem;font-weight:700;color:#94a3b8;text-transform:uppercase;
  letter-spacing:2px;border-bottom:1px solid #1e2d3d;padding-bottom:8px;margin:24px 0 14px;}
.cmp-card{border-radius:12px;padding:20px;text-align:center;border:1px solid;}
.metric-row{display:flex;justify-content:space-between;padding:8px 0;
  border-bottom:1px solid #1a2535;font-size:.88rem;}
.metric-label{color:#4b5563;}
.metric-val{font-family:JetBrains Mono,monospace;font-weight:600;color:#e2e8f0;}
.winner{color:#00d4aa !important;}
.loser{color:#ef4444 !important;}
.analysis-box{background:#0d1520;border:1px solid #1a2535;border-left:3px solid #00d4aa;
  border-radius:8px;padding:20px 24px;font-size:.9rem;line-height:1.75;color:#cbd5e1;}
.select-hint{background:#0d1520;border:2px dashed #1a2535;border-radius:12px;
  padding:40px;text-align:center;color:#374151;font-size:.9rem;}
</style>
""", unsafe_allow_html=True)

COLORS = ["#00d4aa", "#3b82f6", "#f59e0b"]
BG_COLORS = ["rgba(0,212,170,.08)", "rgba(59,130,246,.08)", "rgba(245,158,11,.08)"]
BORDER_COLORS = ["rgba(0,212,170,.3)", "rgba(59,130,246,.3)", "rgba(245,158,11,.3)"]

def apply_theme(fig, height=None, **kw):
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Sora,sans-serif", color="#6b7280"),
        margin=dict(l=10,r=10,t=36,b=10),
        **({"height":height} if height else {}), **kw
    )
    return fig

# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown('<div class="hdr">⚖️ Fund Comparator</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Select up to 3 mutual funds · Side-by-side metrics · AI-powered verdict</div>', unsafe_allow_html=True)

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_df(): return load_fund_data()

with st.spinner("Loading fund data..."):
    df = get_df()

if df.empty:
    st.error("Could not load fund data.")
    st.stop()

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────
api_key = st.sidebar.text_input("Anthropic API Key", type="password", placeholder="sk-ant-...")
st.sidebar.markdown("---")
st.sidebar.markdown("### How to use")
st.sidebar.markdown("""
1. Select a category
2. Pick a fund from the list
3. Add up to 3 funds total
4. Click **Compare** for AI analysis
""")

# ─── FUND SELECTION UI ────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Select Funds to Compare (max 3)</div>', unsafe_allow_html=True)

all_cats = sorted(df["category"].dropna().unique())
selected_funds = []

sel_cols = st.columns(3)
for i, col in enumerate(sel_cols):
    with col:
        st.markdown(f"**Fund {i+1}** {'(optional)' if i > 0 else ''}")
        cat = st.selectbox(f"Category", ["— Select —"] + all_cats, key=f"cat_{i}")
        if cat != "— Select —":
            cat_funds = df[df["category"] == cat]["scheme_name"].sort_values().tolist()
            fund_name = st.selectbox(f"Fund", ["— Select —"] + cat_funds, key=f"fund_{i}")
            if fund_name != "— Select —":
                fund_row = df[df["scheme_name"] == fund_name].iloc[0]
                selected_funds.append(fund_row)
                # Mini preview card
                st.markdown(f"""
                <div class="cmp-card" style="background:{BG_COLORS[i]};border-color:{BORDER_COLORS[i]};">
                    <div style="font-size:.8rem;font-weight:600;color:{COLORS[i]};">{fund_row['category']}</div>
                    <div style="font-size:.85rem;color:#e2e8f0;margin-top:6px;">{fund_name[:45]}{'...' if len(fund_name)>45 else ''}</div>
                    <div style="font-size:.75rem;color:#4b5563;margin-top:4px;">{fund_row['amc']}</div>
                    <div style="font-family:JetBrains Mono,monospace;font-size:1.1rem;color:{COLORS[i]};margin-top:10px;">
                        {fund_row['cagr_3y']:.1f}% <span style="font-size:.7rem;color:#4b5563;">3Y CAGR</span>
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown('<div class="select-hint">Pick a fund above</div>', unsafe_allow_html=True)

if len(selected_funds) < 2:
    st.info("👆 Select at least 2 funds to compare.")
    st.stop()

# ─── COMPARISON METRICS TABLE ─────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Side-by-Side Metrics</div>', unsafe_allow_html=True)

metrics = [
    ("1Y CAGR", "cagr_1y", "%", True),
    ("3Y CAGR", "cagr_3y", "%", True),
    ("5Y CAGR", "cagr_5y", "%", True),
    ("Sharpe Ratio", "sharpe_ratio", "", True),
    ("Expense Ratio", "expense_ratio", "%", False),
    ("Volatility", "volatility", "%", False),
    ("Max Drawdown", "max_drawdown", "%", True),
    ("AUM (Cr)", "aum_cr", "₹", True),
    ("Composite Score", "composite_score", "", True),
]

header_cols = st.columns([2] + [1] * len(selected_funds))
header_cols[0].markdown("**Metric**")
for i, fund in enumerate(selected_funds):
    short = fund["scheme_name"][:30] + ("..." if len(fund["scheme_name"]) > 30 else "")
    header_cols[i+1].markdown(f"**<span style='color:{COLORS[i]};'>{short}</span>**", unsafe_allow_html=True)

for label, col, suffix, higher_is_better in metrics:
    vals = [float(f[col]) for f in selected_funds]
    best_idx = vals.index(max(vals) if higher_is_better else min(vals))

    row_cols = st.columns([2] + [1] * len(selected_funds))
    row_cols[0].markdown(f"<span style='color:#6b7280;font-size:.86rem;'>{label}</span>", unsafe_allow_html=True)
    for i, (fund, val) in enumerate(zip(selected_funds, vals)):
        is_best = (i == best_idx)
        css = f"color:{'#00d4aa' if is_best else '#94a3b8'};font-family:JetBrains Mono,monospace;font-size:.9rem;font-weight:{'700' if is_best else '400'};"
        if suffix == "%":
            display_val = f"{val:.2f}%"
        elif suffix == "₹":
            display_val = f"₹{val:,.0f}"
        else:
            display_val = f"{val:.3f}"
        prefix = "✦ " if is_best else ""
        row_cols[i+1].markdown(f"<span style='{css}'>{prefix}{display_val}</span>", unsafe_allow_html=True)

# ─── CHARTS ───────────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Visual Comparison</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)

with c1:
    # CAGR bar comparison
    fig = go.Figure()
    periods = ["1Y","3Y","5Y"]
    cagr_keys = ["cagr_1y","cagr_3y","cagr_5y"]
    for i, fund in enumerate(selected_funds):
        short = fund["scheme_name"][:25] + "..."
        fig.add_trace(go.Bar(
            name=short, x=periods,
            y=[fund[k] for k in cagr_keys],
            marker_color=COLORS[i],
            text=[f"{fund[k]:.1f}%" for k in cagr_keys],
            textposition="outside",
        ))
    apply_theme(fig, height=340, title="CAGR Comparison (1Y / 3Y / 5Y)",
                barmode="group", yaxis_title="Return (%)")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    # Radar chart
    categories_radar = ["3Y CAGR","5Y CAGR","Sharpe×10","Low Expense","Low Volatility","AUM Score"]
    fig_r = go.Figure()
    max_aum = max(float(f["aum_cr"]) for f in selected_funds)
    for i, fund in enumerate(selected_funds):
        vals = [
            min(float(fund["cagr_3y"]), 35) / 35 * 10,
            min(float(fund["cagr_5y"]), 30) / 30 * 10,
            min(max(float(fund["sharpe_ratio"]), 0), 3) / 3 * 10,
            max(0, (2.5 - float(fund["expense_ratio"])) / 2.5 * 10),
            max(0, (30 - float(fund["volatility"])) / 30 * 10),
            float(fund["aum_cr"]) / max_aum * 10 if max_aum > 0 else 5,
        ]
        short = fund["scheme_name"][:20] + "..."
        fig_r.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=categories_radar + [categories_radar[0]],
            fill="toself", name=short,
            line_color=COLORS[i],
            fillcolor=BG_COLORS[i],
        ))
    apply_theme(fig_r, height=340, title="Fund Profile Radar",
                polar=dict(radialaxis=dict(visible=True, range=[0,10])))
    st.plotly_chart(fig_r, use_container_width=True)

# ─── QUICK VERDICT ────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Quick Verdict</div>', unsafe_allow_html=True)
best_composite = max(selected_funds, key=lambda x: float(x["composite_score"]))
best_return = max(selected_funds, key=lambda x: float(x["cagr_3y"]))
best_sharpe = max(selected_funds, key=lambda x: float(x["sharpe_ratio"]))
cheapest = min(selected_funds, key=lambda x: float(x["expense_ratio"]))

v1,v2,v3,v4 = st.columns(4)
def verdict_card(col, emoji, label, fund_name, color):
    col.markdown(f"""<div style='background:#0d1520;border:1px solid {color}30;border-radius:10px;
    padding:14px;text-align:center;'>
    <div style='font-size:1.3rem;'>{emoji}</div>
    <div style='font-size:.7rem;color:#4b5563;margin:4px 0;text-transform:uppercase;letter-spacing:1px;'>{label}</div>
    <div style='font-size:.82rem;color:{color};font-weight:600;'>{fund_name[:30]}{'...' if len(fund_name)>30 else ''}</div>
    </div>""", unsafe_allow_html=True)

verdict_card(v1,"🏆","Best Overall",best_composite["scheme_name"],"#00d4aa")
verdict_card(v2,"📈","Best Return",best_return["scheme_name"],"#3b82f6")
verdict_card(v3,"⚖️","Best Risk-Adj",best_sharpe["scheme_name"],"#f59e0b")
verdict_card(v4,"💰","Cheapest ER",cheapest["scheme_name"],"#8b5cf6")

# ─── AI COMPARISON ANALYSIS ───────────────────────────────────────────────────
st.markdown('<div class="section-hdr">🤖 AI Comparison & Recommendation</div>', unsafe_allow_html=True)

if st.button("▶ Generate AI Fund Comparison", type="primary"):
    fund_data = []
    for f in selected_funds:
        fund_data.append({
            "name": f["scheme_name"], "category": f["category"], "amc": f["amc"],
            "cagr_1y": float(f["cagr_1y"]), "cagr_3y": float(f["cagr_3y"]), "cagr_5y": float(f["cagr_5y"]),
            "expense_ratio": float(f["expense_ratio"]), "sharpe_ratio": float(f["sharpe_ratio"]),
            "volatility": float(f["volatility"]), "aum_cr": float(f["aum_cr"]),
            "composite_score": float(f["composite_score"]),
        })

    prompt = f"""
You are comparing {len(selected_funds)} Indian mutual funds for a long-term SIP investor.

## FUND DATA
{fund_data}

Provide a structured comparison covering:

## 1. Head-to-Head Performance
Compare 1Y, 3Y, 5Y CAGR across funds. Which shows the most consistent compounding?

## 2. Risk Profile
Compare volatility and Sharpe ratios. Which fund offers best risk-adjusted return?

## 3. Cost Analysis
Compare expense ratios. Over a 10-year SIP, what is the approximate cost drag?

## 4. Category Suitability
Given their categories ({[f['category'] for f in selected_funds]}), which fits a moderate-risk investor better?

## 5. AI Verdict
Clear winner recommendation for:
- Conservative investor (5Y+ horizon)
- Moderate investor (7Y+ horizon)
- Aggressive investor (10Y+ horizon)

## 6. Allocation Suggestion
If investing ₹10,000/month across these funds, suggest % split with reasoning.

Be direct. Use the actual numbers. No vague generalisations.
"""
    with st.spinner("🤖 Claude is comparing your funds..."):
        result = get_claude_analysis(prompt, api_key=api_key or None)
    st.session_state["compare_analysis"] = result

if "compare_analysis" in st.session_state:
    st.markdown(f'<div class="analysis-box">{st.session_state["compare_analysis"]}</div>', unsafe_allow_html=True)
    names_str = "_vs_".join([f["scheme_name"][:15].replace(" ","_") for f in selected_funds])
    st.download_button(
        "⬇ Download Comparison", st.session_state["compare_analysis"],
        f"compare_{names_str}.md", "text/markdown"
    )