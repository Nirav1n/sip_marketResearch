import streamlit as st, plotly.express as px, plotly.graph_objects as go
import pandas as pd, math, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from holdings_engine import (build_holdings_data, build_stock_conviction_table,
    build_rotation_data, get_dynamic_quarters, get_data_status)
from data_fetcher import load_fund_data, EQUITY_CATEGORIES
from claude_analyst import get_claude_analysis, build_stock_holdings_prompt

st.set_page_config(page_title="Stock Holdings", page_icon="🔍", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.shdr{font-size:.9rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:2px;
  border-bottom:1px solid #1e2d3d;padding-bottom:8px;margin:24px 0 14px;}
.analysis-box{background:#0d1520;border:1px solid #1a2535;border-left:3px solid #00d4aa;
  border-radius:8px;padding:20px 24px;font-size:.9rem;line-height:1.75;color:#cbd5e1;}
.source-real{background:rgba(0,212,170,.08);border:1px solid rgba(0,212,170,.25);border-radius:8px;
  padding:10px 14px;font-size:.8rem;color:#00d4aa;margin-bottom:16px;}
.source-rep{background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.25);border-radius:8px;
  padding:10px 14px;font-size:.8rem;color:#f59e0b;margin-bottom:16px;}
</style>""", unsafe_allow_html=True)

def theme(fig,h=None,**kw):
    fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Sora,sans-serif",color="#6b7280"),
        margin=dict(l=10,r=10,t=36,b=10),**({"height":h} if h else {}),**kw)
    return fig

def hex_rgba(h,a=0.08):
    h=h.lstrip("#"); r,g,b=int(h[0:2],16),int(h[2:4],16),int(h[4:6],16)
    return f"rgba({r},{g},{b},{a})"

api_key = st.sidebar.text_input("Anthropic API Key",type="password",placeholder="sk-ant-...")

st.markdown("<h1 style='font-size:1.8rem;font-weight:700;color:#f1f5f9;'>🔍 Stock Holdings Intelligence</h1>",unsafe_allow_html=True)
st.markdown("<p style='color:#4b5563;font-size:.85rem;margin:0 0 20px;'>Which stocks do funds actually own — and how many are buying the same one?</p>",unsafe_allow_html=True)

# ── Load fund data to show real categories/AMCs ────────────────────────────
@st.cache_data(ttl=3600)
def get_fund_df():
    return load_fund_data()

with st.spinner("Loading fund list..."):
    fund_df = get_fund_df()

# ── Get available categories from our actual fund list ─────────────────────
available_cats = sorted(fund_df["category"].dropna().unique()) if not fund_df.empty else list(EQUITY_CATEGORIES)

# ── Category selector ──────────────────────────────────────────────────────
selected = st.multiselect(
    "Select SIP Fund Categories (from our fund list)",
    available_cats,
    default=["Large Cap","Mid Cap"] if "Large Cap" in available_cats else available_cats[:2],
    help=f"We track {len(fund_df)} funds across {fund_df['amc'].nunique()} AMCs from AMFI"
)

c1,c2,c3 = st.columns([2,2,1])
ipp = c1.select_slider("Stocks per page",[20,25,50,75,100],25)
min_f = c2.slider("Min funds holding (filter)",1,20,2)
all_sectors = sorted(fund_df["category"].dropna().unique()) if not fund_df.empty else []
sec_f = c3.selectbox("Filter sector",["All"]+["IT","Banking","Pharma","Infrastructure","FMCG",
    "Auto","Metals","NBFC","Consumer","Energy","Real Estate","Capital Goods","Chemicals"])

if not selected:
    st.info("Select at least one category to begin.")
    st.stop()

# ── Data status banner ─────────────────────────────────────────────────────
status = get_data_status(selected)
if status["has_real_data"]:
    st.markdown(f"""<div class="source-real">
    ✅ <strong>Real holdings data</strong> — AMFI factsheet disclosure ({status['latest_month']}) ·
    {status['total_holdings']:,} holdings rows · {status['amcs_with_data']} AMCs
    </div>""", unsafe_allow_html=True)
else:
    st.markdown("""<div class="source-rep">
    ⚠️ <strong>Representative holdings</strong> — Run <code>python amfi_scraper.py --tier 1</code>
    to load real AMFI factsheet data. Current data is modelled from SEBI category mandates.
    </div>""", unsafe_allow_html=True)

# ── Build holdings ─────────────────────────────────────────────────────────
with st.spinner("Analysing fund holdings..."):
    hdf = build_holdings_data(selected)
    conv = build_stock_conviction_table(hdf, selected)
    rot  = build_rotation_data(selected)

quarters = get_dynamic_quarters(8)
fn_col = "scheme_name" if "scheme_name" in hdf.columns else "fund_name"
tf = hdf[fn_col].nunique(); ts = conv["stock_name"].nunique() if not conv.empty else 0

# ── KPIs ───────────────────────────────────────────────────────────────────
k1,k2,k3,k4 = st.columns(4)
def kpi(col,lbl,val,sub=""):
    col.markdown(f"""<div style='background:#0d1520;border:1px solid #1a2535;border-radius:10px;padding:14px 16px;'>
    <div style='font-size:.68rem;color:#4b5563;text-transform:uppercase;letter-spacing:1px;'>{lbl}</div>
    <div style='font-family:JetBrains Mono,monospace;font-size:1.5rem;font-weight:700;color:#00d4aa;margin-top:4px;'>{val}</div>
    <div style='font-size:.73rem;color:#374151;margin-top:2px;'>{sub}</div></div>""",unsafe_allow_html=True)
kpi(k1,"Categories",len(selected)," + ".join(selected[:2])+("…" if len(selected)>2 else ""))
kpi(k2,"Funds Analysed",tf,"across selected categories")
kpi(k3,"Unique Stocks",ts,"in fund portfolios")
kpi(k4,"Universal Holdings",int((conv["funds_pct"]>=80).sum()) if not conv.empty else 0,"in 80%+ of funds")
st.markdown("<br>",unsafe_allow_html=True)

# ── Conviction table ───────────────────────────────────────────────────────
st.markdown('<div class="shdr">Stock Conviction Table — How Many Funds Own Each Stock</div>',unsafe_allow_html=True)
if conv.empty: st.warning("No holdings data."); st.stop()

filtered = conv[conv["fund_count"]>=min_f].copy()
if sec_f != "All": filtered = filtered[filtered["sector_normalised"].str.contains(sec_f,case=False,na=False)]
tp = max(1, math.ceil(len(filtered)/ipp))
pg_c = st.columns([1,3,1])
page = pg_c[1].number_input(f"Page (1–{tp}) · {len(filtered)} stocks",1,tp,1)
s,e = (page-1)*ipp, page*ipp
pf = filtered.iloc[s:e].copy()
disp = pf[["rank","stock_name","ticker","sector_normalised","fund_count","funds_pct",
            "avg_weight","max_weight","conviction_label"]].copy()
disp.columns=["Rank","Stock","Ticker","Sector","# Funds","% Funds","Avg Wt%","Max Wt%","Conviction"]
disp["Avg Wt%"]=disp["Avg Wt%"].map(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
disp["Max Wt%"]=disp["Max Wt%"].map(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
disp["% Funds"]=disp["% Funds"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
st.dataframe(disp,use_container_width=True,height=min(600,40+len(disp)*38),hide_index=True)
st.caption(f"Showing {s+1}–{min(e,len(filtered))} of {len(filtered)} · Page {page}/{tp}")

# ── Charts ─────────────────────────────────────────────────────────────────
st.markdown('<div class="shdr">Conviction Distribution</div>',unsafe_allow_html=True)
c1,c2=st.columns(2)
with c1:
    t20=conv.head(20)
    colors=["#00d4aa" if p>=80 else "#3b82f6" if p>=50 else "#f59e0b" if p>=25 else "#374151"
            for p in t20["funds_pct"]]
    fig=go.Figure(); fig.add_trace(go.Bar(x=t20["ticker"],y=t20["fund_count"],
        marker_color=colors,text=t20["fund_count"].map(lambda x:f"{x}"),
        textposition="outside",hovertext=t20["stock_name"]))
    theme(fig,h=340,title="Top 20 by Fund Count",xaxis_tickangle=-35)
    st.plotly_chart(fig,use_container_width=True)
with c2:
    t50=conv.head(50); sg=t50.groupby("sector_normalised")["fund_count"].sum().reset_index().sort_values("fund_count",ascending=False)
    fig2=px.pie(sg,names="sector_normalised",values="fund_count",title="Sector Share in Top 50",
        color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899","#14b8a6","#f97316"])
    fig2.update_traces(textposition="inside",textinfo="percent+label")
    theme(fig2,h=340); st.plotly_chart(fig2,use_container_width=True)

# ── Rotation tracker ───────────────────────────────────────────────────────
st.markdown(f'<div class="shdr">Rotation Tracker — {quarters[0]} to {quarters[-1]}</div>',unsafe_allow_html=True)
if not rot.empty:
    rot_dedup = rot.drop_duplicates("ticker")
    has_trend = "trend_label" in rot.columns
    if has_trend:
        trend_sum = rot_dedup[["stock_name","ticker","sector_normalised" if "sector_normalised" in rot_dedup.columns else "sector","trend_label","trend"]].copy()
        trend_sum = trend_sum.sort_values("trend",ascending=False)
        ac = trend_sum[trend_sum["trend_label"].str.contains("Accum",na=False)]
        di = trend_sum[trend_sum["trend_label"].str.contains("Distrib",na=False)]
        st_ = trend_sum[trend_sum["trend_label"].str.contains("Stable",na=False)]
        ca,cd,cs = st.columns(3)
        sector_col = "sector_normalised" if "sector_normalised" in trend_sum.columns else "sector"
        for col,sub,lbl in [(ca,ac,"📈 Accumulating"),(cd,di,"📉 Distributing"),(cs,st_,"➡️ Stable")]:
            col.markdown(f"**{lbl}**")
            if not sub.empty:
                col.dataframe(sub[["stock_name","ticker",sector_col]].head(10),hide_index=True,use_container_width=True,height=300)
        t6=conv["ticker"].head(6).tolist(); rt6=rot[rot["ticker"].isin(t6)]
        if not rt6.empty:
            fig3=px.line(rt6,x="quarter",y="fund_count",color="ticker",markers=True,
                title=f"Quarterly Ownership — Top 6 Stocks",
                labels={"fund_count":"# Funds","quarter":"Quarter"},
                color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899"])
            theme(fig3,h=360); st.plotly_chart(fig3,use_container_width=True)

# ── AI Analysis ────────────────────────────────────────────────────────────
st.markdown('<div class="shdr">🤖 AI Advisor — Based on Stocks Shown Above</div>',unsafe_allow_html=True)
if st.button("▶ Get AI Analysis of These Holdings",type="primary"):
    t10 = conv.head(10)[["stock_name","sector_normalised","fund_count","funds_pct","avg_weight"]].rename(
        columns={"sector_normalised":"sector"}).round(2).to_dict("records")
    acc_list = []
    dis_list = []
    sta_list = []
    if not rot.empty and has_trend:
        rot_dd = rot.drop_duplicates("ticker")
        acc_list = rot_dd[rot_dd["trend_label"].str.contains("Accum",na=False)]["stock_name"].head(5).tolist()
        dis_list = rot_dd[rot_dd["trend_label"].str.contains("Distrib",na=False)]["stock_name"].head(5).tolist()
        sta_list = rot_dd[rot_dd["trend_label"].str.contains("Stable",na=False)]["stock_name"].head(5).tolist()

    prompt = build_stock_holdings_prompt(
        selected_cats=selected,
        total_funds=tf, total_stocks=ts,
        top10_stocks=t10,
        acc_stocks=acc_list, dis_stocks=dis_list, stable_stocks=sta_list,
        quarters=quarters,
        data_source_type=status.get("source_type","representative"),
        disclosure_month=status.get("latest_month"),
    )
    with st.spinner("🤖 Analysing holdings..."):
        res = get_claude_analysis(prompt, api_key=api_key or None)
    st.session_state["h_analysis"] = res
    st.session_state["h_context"] = f"{', '.join(selected)} · {ts} stocks · {tf} funds · {status['source_type']} data"

if "h_analysis" in st.session_state:
    ctx = st.session_state.get("h_context","")
    if ctx: st.caption(f"Analysis context: {ctx}")
    st.markdown(f'<div class="analysis-box">{st.session_state["h_analysis"]}</div>',unsafe_allow_html=True)
    st.download_button("⬇ Download",st.session_state["h_analysis"],"holdings_analysis.md","text/markdown")