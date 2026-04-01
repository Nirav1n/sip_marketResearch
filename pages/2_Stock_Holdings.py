"""
pages/2_Stock_Holdings.py
Stock-level holdings intelligence across selected SIP fund categories.
Shows conviction (how many funds hold a stock), rotation over time, sector breakdown.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import math
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from holdings_engine import (
    build_holdings_data,
    build_stock_conviction_table,
    build_rotation_data,
    QUARTERS,
)
from claude_analyst import get_claude_analysis

st.set_page_config(page_title="Stock Holdings", page_icon="🔍", layout="wide")

# ─── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

.page-header { padding: 4px 0 20px; }
.page-title { font-size: 1.9rem; font-weight: 700; color: #f1f5f9; margin: 0; }
.page-sub { color: #4b5563; font-size: 0.88rem; margin: 4px 0 0; }

.step-box {
    background: #0d1520; border: 1px solid #1a2535;
    border-radius: 12px; padding: 20px 24px; margin-bottom: 20px;
}
.step-label {
    font-size: 0.7rem; font-weight: 700; color: #00d4aa;
    text-transform: uppercase; letter-spacing: 2px; margin-bottom: 10px;
}

.conviction-bar-wrap { background: #1a2535; border-radius: 4px; height: 8px; width: 100%; }
.conviction-bar-fill { height: 8px; border-radius: 4px; background: #00d4aa; }

.stat-chip {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600; margin: 2px;
}
.chip-green { background: rgba(0,212,170,0.12); color: #00d4aa; border: 1px solid rgba(0,212,170,0.25); }
.chip-blue  { background: rgba(59,130,246,0.12); color: #3b82f6; border: 1px solid rgba(59,130,246,0.25); }
.chip-amber { background: rgba(245,158,11,0.12); color: #f59e0b; border: 1px solid rgba(245,158,11,0.25); }
.chip-red   { background: rgba(239,68,68,0.12);  color: #ef4444;  border: 1px solid rgba(239,68,68,0.25); }

.section-hdr {
    font-size: 1rem; font-weight: 700; color: #94a3b8;
    text-transform: uppercase; letter-spacing: 2px;
    border-bottom: 1px solid #1e2d3d; padding-bottom: 8px; margin: 28px 0 16px;
}
.analysis-box {
    background: #0d1520; border: 1px solid #1a2535;
    border-left: 3px solid #00d4aa; border-radius: 8px;
    padding: 20px 24px; font-size: 0.9rem; line-height: 1.75; color: #cbd5e1;
}
</style>
""", unsafe_allow_html=True)

def apply_theme(fig, height=None, **kw):
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Sora, sans-serif", color="#6b7280"),
        margin=dict(l=10, r=10, t=36, b=10),
        **({"height": height} if height else {}), **kw,
    )
    return fig


# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
  <div class="page-title">🔍 Stock Holdings Intelligence</div>
  <div class="page-sub">Which stocks do funds actually own — and how many are buying the same one?</div>
</div>
""", unsafe_allow_html=True)

# ─── STEP 1: CATEGORY SELECTION ───────────────────────────────────────────────
with st.container():
    st.markdown('<div class="step-box">', unsafe_allow_html=True)
    st.markdown('<div class="step-label">Step 1 — Select SIP Categories to Analyse</div>', unsafe_allow_html=True)

    ALL_CATEGORIES = ["Large Cap", "Mid Cap", "Small Cap", "Sectoral/Thematic"]
    selected_cats = st.multiselect(
        label="Choose one or more fund categories",
        options=ALL_CATEGORIES,
        default=["Large Cap", "Mid Cap"],
        help="Selects all funds in each category and analyses their underlying stock holdings",
    )

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        items_per_page = st.select_slider(
            "Stocks per page",
            options=[20, 25, 50, 75, 100],
            value=25,
        )
    with c2:
        min_funds = st.slider(
            "Min funds holding stock (filter)",
            min_value=1, max_value=15, value=2,
            help="Only show stocks held by at least N funds",
        )
    with c3:
        sector_filter = st.selectbox("Filter by sector", ["All Sectors"] + [
            "IT", "Banking", "Pharma", "Infrastructure", "FMCG",
            "Auto", "Metals", "NBFC", "Consumer", "Energy",
            "Real Estate", "Capital Goods", "Chemicals", "Logistics"
        ])
    st.markdown('</div>', unsafe_allow_html=True)

if not selected_cats:
    st.info("👆 Select at least one category above to begin analysis.")
    st.stop()

# ─── LOAD & COMPUTE DATA ──────────────────────────────────────────────────────
with st.spinner("🔄 Analysing fund holdings..."):
    holdings_df = build_holdings_data(selected_cats)
    conviction_df = build_stock_conviction_table(holdings_df, selected_cats)
    rotation_df = build_rotation_data(selected_cats)

total_funds = holdings_df["fund_name"].nunique()
total_stocks = conviction_df["stock_name"].nunique()
total_cats = len(selected_cats)

# ─── SUMMARY KPIs ─────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
def kpi(col, label, value, sub=""):
    col.markdown(f"""
    <div style='background:#0d1520;border:1px solid #1a2535;border-radius:10px;padding:16px 18px;'>
        <div style='font-size:0.7rem;color:#4b5563;text-transform:uppercase;letter-spacing:1px;'>{label}</div>
        <div style='font-family:JetBrains Mono,monospace;font-size:1.7rem;font-weight:700;color:#00d4aa;margin:4px 0 2px;'>{value}</div>
        <div style='font-size:0.75rem;color:#374151;'>{sub}</div>
    </div>""", unsafe_allow_html=True)

kpi(k1, "Categories Selected", total_cats, " + ".join(selected_cats[:2]) + ("..." if len(selected_cats) > 2 else ""))
kpi(k2, "Total Funds Analysed", total_funds, "across selected categories")
kpi(k3, "Unique Stocks Found", total_stocks, "in fund portfolios")
kpi(k4, "Universal Holdings", int((conviction_df["funds_pct"] >= 80).sum()), "stocks in 80%+ of funds")

st.markdown("<br>", unsafe_allow_html=True)

# ─── CONVICTION TABLE WITH PAGINATION ─────────────────────────────────────────
st.markdown('<div class="section-hdr">Stock Conviction Table — How Many Funds Own Each Stock</div>', unsafe_allow_html=True)

# Apply filters
filtered = conviction_df[conviction_df["fund_count"] >= min_funds].copy()
if sector_filter != "All Sectors":
    filtered = filtered[filtered["sector"].str.contains(sector_filter, case=False, na=False)]

total_rows = len(filtered)
total_pages = max(1, math.ceil(total_rows / items_per_page))

# Pagination controls
pg_col1, pg_col2, pg_col3 = st.columns([1, 3, 1])
with pg_col2:
    page = st.number_input(
        f"Page (1–{total_pages}) — {total_rows} stocks matching filters",
        min_value=1, max_value=total_pages, value=1, step=1,
    )

start_idx = (page - 1) * items_per_page
end_idx = start_idx + items_per_page
page_df = filtered.iloc[start_idx:end_idx].copy()

# Build display dataframe
display_cols = {
    "rank": "Rank",
    "stock_name": "Stock",
    "ticker": "Ticker",
    "sector": "Sector",
    "fund_count": "# Funds",
    "funds_pct": "% Funds",
    "avg_weight": "Avg Weight%",
    "max_weight": "Max Weight%",
    "conviction_label": "Conviction",
    "categories": "In Categories",
}
display_df = page_df[list(display_cols.keys())].rename(columns=display_cols)
display_df["Avg Weight%"] = display_df["Avg Weight%"].map(lambda x: f"{x:.2f}%")
display_df["Max Weight%"] = display_df["Max Weight%"].map(lambda x: f"{x:.2f}%")
display_df["% Funds"] = display_df["% Funds"].map(lambda x: f"{x:.1f}%")

st.dataframe(
    display_df,
    use_container_width=True,
    height=min(600, 40 + len(display_df) * 38),
    hide_index=True,
)

pg_info_col = st.columns([1, 2, 1])[1]
pg_info_col.caption(f"Showing {start_idx+1}–{min(end_idx, total_rows)} of {total_rows} stocks · Page {page}/{total_pages}")

# ─── CONVICTION DISTRIBUTION CHART ────────────────────────────────────────────
st.markdown('<div class="section-hdr">Conviction Distribution</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)

with c1:
    top20 = conviction_df.head(20)
    fig_conv = go.Figure()
    colors = ["#00d4aa" if p >= 80 else "#3b82f6" if p >= 50 else "#f59e0b" if p >= 25 else "#374151"
              for p in top20["funds_pct"]]
    fig_conv.add_trace(go.Bar(
        x=top20["ticker"], y=top20["fund_count"],
        marker_color=colors,
        text=top20["fund_count"].map(lambda x: f"{x} funds"),
        textposition="outside",
        hovertext=top20["stock_name"],
    ))
    apply_theme(fig_conv, height=340,
                title="Top 20 Stocks by Fund Count",
                yaxis_title="Number of Funds Holding",
                xaxis_tickangle=-35)
    st.plotly_chart(fig_conv, use_container_width=True)

with c2:
    # Sector breakdown of top holdings
    top50 = conviction_df.head(50)
    sec_grp = top50.groupby("sector")["fund_count"].sum().reset_index().sort_values("fund_count", ascending=False)
    fig_sec = px.pie(
        sec_grp, names="sector", values="fund_count",
        title="Sector Share in Top 50 Holdings",
        color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899","#14b8a6","#f97316"],
    )
    fig_sec.update_traces(textposition="inside", textinfo="percent+label")
    apply_theme(fig_sec, height=340)
    st.plotly_chart(fig_sec, use_container_width=True)

# ─── STOCK ROTATION TRACKER ───────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Stock Rotation Tracker — Quarterly Fund Ownership Shifts</div>', unsafe_allow_html=True)

st.caption("Track how many funds hold a stock across each quarter. Rising = accumulation. Falling = distribution.")

if not rotation_df.empty:
    # Latest quarter trend summary
    latest_q = QUARTERS[-1]
    latest = rotation_df[rotation_df["quarter"] == latest_q].copy()

    trend_summary = latest.drop_duplicates("ticker")[["stock_name", "ticker", "sector", "category", "trend_label", "trend"]].copy()
    trend_summary = trend_summary.sort_values("trend", ascending=False)

    acc_col, dis_col, sta_col = st.columns(3)
    acc = trend_summary[trend_summary["trend_label"].str.contains("Accum")]
    dis = trend_summary[trend_summary["trend_label"].str.contains("Distrib")]
    sta = trend_summary[trend_summary["trend_label"].str.contains("Stable")]

    with acc_col:
        st.markdown("**📈 Accumulating** — More funds buying")
        if not acc.empty:
            st.dataframe(acc[["stock_name", "ticker", "sector"]].head(10), hide_index=True, use_container_width=True, height=310)

    with dis_col:
        st.markdown("**📉 Distributing** — Funds reducing")
        if not dis.empty:
            st.dataframe(dis[["stock_name", "ticker", "sector"]].head(10), hide_index=True, use_container_width=True, height=310)

    with sta_col:
        st.markdown("**➡️ Stable** — No significant change")
        if not sta.empty:
            st.dataframe(sta[["stock_name", "ticker", "sector"]].head(10), hide_index=True, use_container_width=True, height=310)

    # Line chart — pick top 6 most held stocks and show rotation
    st.markdown("<br>", unsafe_allow_html=True)
    top6_tickers = conviction_df["ticker"].head(6).tolist()
    rot_top = rotation_df[rotation_df["ticker"].isin(top6_tickers)]

    if not rot_top.empty:
        fig_rot = px.line(
            rot_top, x="quarter", y="fund_count", color="ticker",
            markers=True,
            title="Quarterly Fund Ownership — Top 6 Held Stocks",
            labels={"fund_count": "# Funds Holding", "quarter": "Quarter"},
            color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899"],
        )
        apply_theme(fig_rot, height=360)
        st.plotly_chart(fig_rot, use_container_width=True)

# ─── AI ANALYSIS ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">🤖 AI Analysis of Holdings</div>', unsafe_allow_html=True)

api_key = st.sidebar.text_input("Anthropic API Key", type="password", placeholder="sk-ant-...")

if st.button("▶ Generate Holdings Analysis", type="primary"):
    top10 = conviction_df.head(10)[["stock_name", "sector", "fund_count", "funds_pct", "avg_weight"]].to_dict("records")
    acc_stocks = trend_summary[trend_summary["trend_label"].str.contains("Accum")]["stock_name"].head(5).tolist()
    dis_stocks = trend_summary[trend_summary["trend_label"].str.contains("Distrib")]["stock_name"].head(5).tolist()

    prompt = f"""
You are analysing Indian mutual fund stock holdings data. Selected categories: {', '.join(selected_cats)}.
Total funds analysed: {total_funds}. Unique stocks: {total_stocks}.

## Top 10 Most Held Stocks (by # of funds):
{top10}

## Accumulation signals (more funds adding):
{acc_stocks}

## Distribution signals (funds reducing):
{dis_stocks}

Your tasks:
1. **Interpretation** — What does the top-10 holdings concentration tell us about fund manager consensus?
2. **Accumulation Insight** — Why might funds be accumulating these stocks now?
3. **Distribution Warning** — What risk do distributing stocks signal?
4. **Sector Concentration Risk** — Is the portfolio over-exposed to any sector?
5. **Investment Angle** — 2-3 actionable takeaways for a long-term SIP investor based on this data.

Be specific. Reference the actual stocks and numbers. Format with ## headers.
"""
    with st.spinner("🤖 Analysing holdings with Claude..."):
        analysis = get_claude_analysis(prompt, api_key=api_key or None)
    st.session_state["holdings_analysis"] = analysis

if "holdings_analysis" in st.session_state:
    st.markdown(f'<div class="analysis-box">{st.session_state["holdings_analysis"]}</div>', unsafe_allow_html=True)
    st.download_button(
        "⬇ Download Analysis",
        data=st.session_state["holdings_analysis"],
        file_name="holdings_analysis.md",
        mime="text/markdown",
    )
