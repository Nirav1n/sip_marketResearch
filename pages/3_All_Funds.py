"""
pages/3_All_Funds.py
Full mutual fund browser — real 1Y/3Y/5Y CAGR, specific filters, AI insights.
Improvement #5: detailed filters; Improvement #6: dedicated all-funds page.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from data_fetcher import load_fund_data, EQUITY_CATEGORIES
from claude_analyst import get_claude_analysis

st.set_page_config(page_title="All Mutual Funds", page_icon="📋", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.hdr{font-size:1.8rem;font-weight:700;color:#f1f5f9;margin:0 0 4px;}
.sub{color:#4b5563;font-size:0.88rem;margin:0 0 24px;}
.section-hdr{font-size:.9rem;font-weight:700;color:#94a3b8;text-transform:uppercase;
  letter-spacing:2px;border-bottom:1px solid #1e2d3d;padding-bottom:8px;margin:24px 0 14px;}
.fund-card{background:#0d1520;border:1px solid #1a2535;border-radius:10px;padding:18px 20px;margin-bottom:12px;}
.fund-name{font-size:1rem;font-weight:600;color:#f1f5f9;margin-bottom:6px;}
.fund-amc{font-size:0.76rem;color:#4b5563;margin-bottom:10px;}
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.72rem;font-weight:600;margin:2px;}
.bg{background:rgba(0,212,170,.12);color:#00d4aa;border:1px solid rgba(0,212,170,.25);}
.bb{background:rgba(59,130,246,.12);color:#3b82f6;border:1px solid rgba(59,130,246,.25);}
.ba{background:rgba(245,158,11,.12);color:#f59e0b;border:1px solid rgba(245,158,11,.25);}
.br{background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.25);}
.analysis-box{background:#0d1520;border:1px solid #1a2535;border-left:3px solid #00d4aa;
  border-radius:8px;padding:20px 24px;font-size:.9rem;line-height:1.75;color:#cbd5e1;}
</style>
""", unsafe_allow_html=True)

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
st.markdown('<div class="hdr">📋 All Mutual Funds</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Real CAGR from NAV history · AMFI official data · Filter by any dimension</div>', unsafe_allow_html=True)

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_df(): return load_fund_data()

with st.spinner("Loading fund data..."):
    df = get_df()

if df.empty:
    st.error("Could not load fund data. Check internet connection.")
    st.stop()

# ─── SIDEBAR FILTERS (improvement #5 — specific) ──────────────────────────────
st.sidebar.markdown("## 🔽 Filters")
api_key = st.sidebar.text_input("Anthropic API Key", type="password", placeholder="sk-ant-...")
st.sidebar.markdown("---")

# Category group filter
cat_groups = {
    "All Equity": list(EQUITY_CATEGORIES),
    "Pure Equity (Large/Mid/Small)": ["Large Cap","Mid Cap","Small Cap","Large & Mid Cap","Multi Cap","Flexi Cap"],
    "Tax Saving (ELSS)": ["ELSS"],
    "Thematic & Sectoral": ["Sectoral","Thematic"],
    "Hybrid": ["Aggressive Hybrid","Balanced Advantage","Multi Asset"],
    "Special (Focused/Value/Contra)": ["Focused","Value","Contra","Dividend Yield"],
    "Index Funds": ["Index Fund"],
}
group = st.sidebar.selectbox("Category Group", list(cat_groups.keys()))
available_cats = [c for c in cat_groups[group] if c in df["category"].unique()]
sel_cats = st.sidebar.multiselect("Specific Categories", available_cats, default=available_cats[:3])
if not sel_cats: sel_cats = available_cats

# AMC filter
all_amcs = sorted(df["amc"].dropna().unique())
sel_amcs = st.sidebar.multiselect("Fund House (AMC)", ["All AMCs"] + all_amcs, default=["All AMCs"])
if "All AMCs" in sel_amcs or not sel_amcs: sel_amcs = all_amcs

# Performance filters
st.sidebar.markdown("---")
st.sidebar.markdown("**Performance Filters**")
min_cagr_1y = st.sidebar.slider("Min 1Y CAGR (%)", -20, 60, 0)
min_cagr_3y = st.sidebar.slider("Min 3Y CAGR (%)", -10, 40, 5)
min_cagr_5y = st.sidebar.slider("Min 5Y CAGR (%)", -5, 35, 5)

st.sidebar.markdown("**Risk Filters**")
max_er = st.sidebar.slider("Max Expense Ratio (%)", 0.1, 2.5, 1.5, step=0.05)
min_sharpe = st.sidebar.slider("Min Sharpe Ratio", -1.0, 3.0, 0.0, step=0.1)

st.sidebar.markdown("**AUM Filter**")
min_aum = st.sidebar.number_input("Min AUM (₹ Cr)", 0, 100000, 100, step=100)

sort_by = st.sidebar.selectbox("Sort By", ["composite_score","cagr_3y","cagr_5y","cagr_1y","sharpe_ratio","expense_ratio","aum_cr"])
sort_asc = st.sidebar.checkbox("Sort Ascending", False)

# ─── APPLY FILTERS ────────────────────────────────────────────────────────────
fdf = df[
    (df["category"].isin(sel_cats)) &
    (df["amc"].isin(sel_amcs)) &
    (df["cagr_1y"] >= min_cagr_1y) &
    (df["cagr_3y"] >= min_cagr_3y) &
    (df["cagr_5y"] >= min_cagr_5y) &
    (df["expense_ratio"] <= max_er) &
    (df["sharpe_ratio"] >= min_sharpe) &
    (df["aum_cr"] >= min_aum)
].copy().sort_values(sort_by, ascending=sort_asc).reset_index(drop=True)

# ─── KPI ROW ──────────────────────────────────────────────────────────────────
c1,c2,c3,c4,c5 = st.columns(5)
def kpi(col, label, val, color="#00d4aa"):
    col.markdown(f"""<div style='background:#0d1520;border:1px solid #1a2535;border-radius:10px;padding:14px 16px;'>
    <div style='font-size:.68rem;color:#4b5563;text-transform:uppercase;letter-spacing:1px;'>{label}</div>
    <div style='font-family:JetBrains Mono,monospace;font-size:1.4rem;font-weight:700;color:{color};margin-top:4px;'>{val}</div>
    </div>""", unsafe_allow_html=True)

kpi(c1,"Funds Shown", len(fdf))
kpi(c2,"Avg 1Y CAGR", f"{fdf['cagr_1y'].mean():.1f}%" if not fdf.empty else "—")
kpi(c3,"Avg 3Y CAGR", f"{fdf['cagr_3y'].mean():.1f}%" if not fdf.empty else "—", "#3b82f6")
kpi(c4,"Avg 5Y CAGR", f"{fdf['cagr_5y'].mean():.1f}%" if not fdf.empty else "—", "#f59e0b")
kpi(c5,"Best Sharpe", f"{fdf['sharpe_ratio'].max():.2f}" if not fdf.empty else "—", "#8b5cf6")

st.markdown("<br>", unsafe_allow_html=True)

if fdf.empty:
    st.warning("No funds match filters. Try relaxing the criteria.")
    st.stop()

# ─── CHARTS ROW ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Performance Landscape</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)

with c1:
    fig = px.scatter(
        fdf.head(100), x="cagr_3y", y="cagr_5y", color="category", size="aum_cr",
        hover_data=["scheme_name","expense_ratio","sharpe_ratio"],
        title="3Y vs 5Y CAGR (bubble = AUM)",
        color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6",
                                  "#ec4899","#14b8a6","#f97316","#a78bfa","#34d399"],
        labels={"cagr_3y":"3Y CAGR (%)","cagr_5y":"5Y CAGR (%)"},
    )
    apply_theme(fig, height=340)
    st.plotly_chart(fig, use_container_width=True)

with c2:
    cat_avg = fdf.groupby("category")[["cagr_1y","cagr_3y","cagr_5y"]].mean().round(1).reset_index()
    cat_avg_m = cat_avg.melt("category", var_name="Period", value_name="CAGR%")
    period_labels = {"cagr_1y":"1 Year","cagr_3y":"3 Year","cagr_5y":"5 Year"}
    cat_avg_m["Period"] = cat_avg_m["Period"].map(period_labels)
    fig2 = px.bar(
        cat_avg_m, x="category", y="CAGR%", color="Period", barmode="group",
        title="Avg CAGR by Category (1Y / 3Y / 5Y)",
        color_discrete_sequence=["#3b82f6","#00d4aa","#f59e0b"],
    )
    apply_theme(fig2, height=340, xaxis_tickangle=-30)
    st.plotly_chart(fig2, use_container_width=True)

# ─── EXPENSE RATIO vs RETURN ──────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Expense Ratio vs 3Y Return</div>', unsafe_allow_html=True)
fig3 = px.scatter(
    fdf.head(150), x="expense_ratio", y="cagr_3y", color="category",
    size="aum_cr", hover_data=["scheme_name","sharpe_ratio"],
    title="Does lower expense ratio → better returns?",
    labels={"expense_ratio":"Expense Ratio (%)","cagr_3y":"3Y CAGR (%)"},
    color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899"],
)
apply_theme(fig3, height=320)
st.plotly_chart(fig3, use_container_width=True)

# ─── FUND TABLE ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Fund Directory</div>', unsafe_allow_html=True)

# Items per page
ipp_col, pg_col, _ = st.columns([1, 2, 2])
with ipp_col:
    items_per_page = st.selectbox("Per page", [20, 25, 50, 100], index=0)
import math
total_pages = max(1, math.ceil(len(fdf) / items_per_page))
with pg_col:
    page = st.number_input(f"Page (1–{total_pages}) · {len(fdf)} funds", 1, total_pages, 1)

start, end = (page-1)*items_per_page, page*items_per_page
page_df = fdf.iloc[start:end].copy()
page_df.index = range(start+1, start+len(page_df)+1)

display = page_df[[
    "scheme_name","category","amc","cagr_1y","cagr_3y","cagr_5y",
    "expense_ratio","sharpe_ratio","aum_cr","composite_score"
]].copy()
display.columns = ["Fund Name","Category","AMC","1Y CAGR%","3Y CAGR%","5Y CAGR%",
                   "Exp Ratio%","Sharpe","AUM (Cr)","Score"]
for c in ["1Y CAGR%","3Y CAGR%","5Y CAGR%","Exp Ratio%"]:
    display[c] = display[c].map(lambda x: f"{x:.2f}%")
display["Sharpe"] = display["Sharpe"].map(lambda x: f"{x:.3f}")
display["AUM (Cr)"] = display["AUM (Cr)"].map(lambda x: f"₹{x:,.0f}")

st.dataframe(display, use_container_width=True, height=min(700, 40+len(display)*38), hide_index=False)
st.caption(f"Showing {start+1}–{min(end,len(fdf))} of {len(fdf)} · Page {page}/{total_pages}")

# ─── AI INSIGHTS ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">🤖 AI Analysis of Filtered Funds</div>', unsafe_allow_html=True)

if st.button("▶ Generate AI Insights for Current Filter", type="primary"):
    top10 = fdf.head(10)[["scheme_name","category","cagr_1y","cagr_3y","cagr_5y","expense_ratio","sharpe_ratio"]].to_dict("records")
    cat_summary = fdf.groupby("category")[["cagr_3y","cagr_5y","expense_ratio"]].mean().round(2).to_dict("index")
    prompt = f"""
Analyse this filtered set of Indian mutual funds.
Filter applied: Categories = {sel_cats}, Min 3Y CAGR = {min_cagr_3y}%, Max ER = {max_er}%
Total funds matching: {len(fdf)}

Top 10 funds by composite score:
{top10}

Category averages:
{cat_summary}

Provide:
1. **Quality Assessment** — Overall quality of this filtered fund set
2. **Top 3 Standout Funds** — With specific reasoning from the data
3. **Category Comparison** — Which categories look best in this filter
4. **Expense Ratio Analysis** — Is the market offering value or overcharging?
5. **Risk-Return Verdict** — Is the Sharpe ratio justifying the volatility?
6. **Action Plan** — What should a moderate-risk investor do with this data?
Use ## headers. Be direct and data-specific.
"""
    with st.spinner("🤖 Analysing..."):
        result = get_claude_analysis(prompt, api_key=api_key or None)
    st.session_state["af_analysis"] = result

if "af_analysis" in st.session_state:
    st.markdown(f'<div class="analysis-box">{st.session_state["af_analysis"]}</div>', unsafe_allow_html=True)
    st.download_button("⬇ Download", st.session_state["af_analysis"], "fund_analysis.md", "text/markdown")