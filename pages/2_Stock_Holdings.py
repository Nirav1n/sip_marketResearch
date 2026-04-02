import streamlit as st, plotly.express as px, plotly.graph_objects as go
import pandas as pd, math, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from holdings_engine import (build_holdings_data,build_stock_conviction_table,
    build_rotation_data,get_dynamic_quarters)
from claude_analyst import get_claude_analysis

st.set_page_config(page_title="Stock Holdings",page_icon="🔍",layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.shdr{font-size:.9rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:2px;
  border-bottom:1px solid #1e2d3d;padding-bottom:8px;margin:24px 0 14px;}
.analysis-box{background:#0d1520;border:1px solid #1a2535;border-left:3px solid #00d4aa;
  border-radius:8px;padding:20px 24px;font-size:.9rem;line-height:1.75;color:#cbd5e1;}
</style>""",unsafe_allow_html=True)

def theme(fig,h=None,**kw):
    fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Sora,sans-serif",color="#6b7280"),
        margin=dict(l=10,r=10,t=36,b=10),**({"height":h} if h else {}),**kw)
    return fig

st.markdown("<h1 style='font-size:1.8rem;font-weight:700;color:#f1f5f9;'>🔍 Stock Holdings Intelligence</h1>",unsafe_allow_html=True)
st.markdown("<p style='color:#4b5563;font-size:.85rem;margin:0 0 20px;'>Which stocks do funds actually own — and how many funds are buying the same one?</p>",unsafe_allow_html=True)

api_key = st.sidebar.text_input("Anthropic API Key",type="password",placeholder="sk-ant-...")

ALL_CATS = ["Large Cap","Mid Cap","Small Cap","Large & Mid Cap","Multi Cap","Flexi Cap",
            "Focused","Value","Contra","ELSS","Sectoral","Thematic","Aggressive Hybrid","Index Fund"]
selected = st.multiselect("Select SIP Fund Categories",ALL_CATS,default=["Large Cap","Mid Cap"])

c1,c2,c3 = st.columns([2,2,1])
ipp = c1.select_slider("Stocks per page",[20,25,50,75,100],25)
min_f = c2.slider("Min funds holding",1,15,2)
sec_f = c3.selectbox("Sector",["All"]+sorted(set(["IT","Banking","Pharma","Infrastructure","FMCG","Auto","Metals","NBFC","Consumer","Energy","Real Estate","Capital Goods","Chemicals","Logistics"])))

if not selected: st.info("Select at least one category."); st.stop()

with st.spinner("Analysing fund holdings..."):
    hdf = build_holdings_data(selected)
    conv = build_stock_conviction_table(hdf, selected)
    rot  = build_rotation_data(selected)

quarters = get_dynamic_quarters(8)

tf = hdf["fund_name"].nunique(); ts = conv["stock_name"].nunique() if not conv.empty else 0
k1,k2,k3,k4 = st.columns(4)
def kpi(col,lbl,val,sub=""):
    col.markdown(f"""<div style='background:#0d1520;border:1px solid #1a2535;border-radius:10px;padding:14px 16px;'>
    <div style='font-size:.68rem;color:#4b5563;text-transform:uppercase;letter-spacing:1px;'>{lbl}</div>
    <div style='font-family:JetBrains Mono,monospace;font-size:1.5rem;font-weight:700;color:#00d4aa;margin-top:4px;'>{val}</div>
    <div style='font-size:.73rem;color:#374151;margin-top:2px;'>{sub}</div></div>""",unsafe_allow_html=True)
kpi(k1,"Categories",len(selected)," + ".join(selected[:2])+("..." if len(selected)>2 else ""))
kpi(k2,"Funds Analysed",tf,"across selected categories")
kpi(k3,"Unique Stocks",ts,"in fund portfolios")
kpi(k4,"Universal Holdings",int((conv["funds_pct"]>=80).sum()) if not conv.empty else 0,"in 80%+ of funds")

st.markdown('<div class="shdr">Stock Conviction Table</div>',unsafe_allow_html=True)
if conv.empty: st.warning("No holdings data."); st.stop()

filtered = conv[conv["fund_count"]>=min_f].copy()
if sec_f != "All": filtered = filtered[filtered["sector"].str.contains(sec_f,case=False,na=False)]

tp = max(1, math.ceil(len(filtered)/ipp))
pg_c = st.columns([1,3,1])
page = pg_c[1].number_input(f"Page (1–{tp}) · {len(filtered)} stocks",1,tp,1)
s,e = (page-1)*ipp, page*ipp
pf = filtered.iloc[s:e].copy()
disp = pf[["rank","stock_name","ticker","sector","fund_count","funds_pct","avg_weight","max_weight","conviction_label","categories"]].copy()
disp.columns=["Rank","Stock","Ticker","Sector","# Funds","% Funds","Avg Wt%","Max Wt%","Conviction","Categories"]
disp["Avg Wt%"]=disp["Avg Wt%"].map(lambda x:f"{x:.2f}%")
disp["Max Wt%"]=disp["Max Wt%"].map(lambda x:f"{x:.2f}%")
disp["% Funds"]=disp["% Funds"].map(lambda x:f"{x:.1f}%")
st.dataframe(disp,use_container_width=True,height=min(600,40+len(disp)*38),hide_index=True)
st.caption(f"Page {page}/{tp} · Showing {s+1}–{min(e,len(filtered))} of {len(filtered)}")

st.markdown('<div class="shdr">Conviction Chart</div>',unsafe_allow_html=True)
c1,c2=st.columns(2)
with c1:
    t20=conv.head(20)
    colors=["#00d4aa" if p>=80 else "#3b82f6" if p>=50 else "#f59e0b" if p>=25 else "#374151" for p in t20["funds_pct"]]
    fig=go.Figure(); fig.add_trace(go.Bar(x=t20["ticker"],y=t20["fund_count"],marker_color=colors,
        text=t20["fund_count"].map(lambda x:f"{x} funds"),textposition="outside",hovertext=t20["stock_name"]))
    theme(fig,h=340,title="Top 20 by Fund Count",yaxis_title="Funds Holding",xaxis_tickangle=-35)
    st.plotly_chart(fig,use_container_width=True)
with c2:
    t50=conv.head(50); sg=t50.groupby("sector")["fund_count"].sum().reset_index().sort_values("fund_count",ascending=False)
    fig2=px.pie(sg,names="sector",values="fund_count",title="Sector Share in Top 50",
        color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899","#14b8a6","#f97316"])
    fig2.update_traces(textposition="inside",textinfo="percent+label")
    theme(fig2,h=340); st.plotly_chart(fig2,use_container_width=True)

st.markdown(f'<div class="shdr">Rotation Tracker — {quarters[0]} to {quarters[-1]}</div>',unsafe_allow_html=True)
if not rot.empty:
    lq=quarters[-1]; latest=rot[rot["quarter"]==lq].drop_duplicates("ticker")
    trend_sum=latest.merge(conv[["ticker","trend_label"]].drop_duplicates(),"left",on="ticker") if "trend_label" in rot.columns else latest
    trend_sum=rot.drop_duplicates("ticker")[["stock_name","ticker","sector","trend_label","trend"]].sort_values("trend",ascending=False)
    ac=trend_sum[trend_sum["trend_label"].str.contains("Accum",na=False)]
    di=trend_sum[trend_sum["trend_label"].str.contains("Distrib",na=False)]
    st_=trend_sum[trend_sum["trend_label"].str.contains("Stable",na=False)]
    ca,cd,cs=st.columns(3)
    for col,sub,lbl in [(ca,ac,"📈 Accumulating"),(cd,di,"📉 Distributing"),(cs,st_,"➡️ Stable")]:
        col.markdown(f"**{lbl}**")
        if not sub.empty: col.dataframe(sub[["stock_name","ticker","sector"]].head(10),hide_index=True,use_container_width=True,height=300)

    t6=conv["ticker"].head(6).tolist(); rt6=rot[rot["ticker"].isin(t6)]
    if not rt6.empty:
        fig3=px.line(rt6,x="quarter",y="fund_count",color="ticker",markers=True,
            title=f"Quarterly Ownership — Top 6 Stocks ({quarters[0]}→{quarters[-1]})",
            labels={"fund_count":"# Funds","quarter":"Quarter"},
            color_discrete_sequence=["#00d4aa","#3b82f6","#f59e0b","#ef4444","#8b5cf6","#ec4899"])
        theme(fig3,h=360); st.plotly_chart(fig3,use_container_width=True)

st.markdown('<div class="shdr">🤖 AI Holdings Analysis</div>',unsafe_allow_html=True)
if st.button("▶ Generate Analysis",type="primary"):
    t10=conv.head(10)[["stock_name","sector","fund_count","funds_pct","avg_weight"]].to_dict("records")
    acc=rot.drop_duplicates("ticker")[rot.drop_duplicates("ticker")["trend_label"].str.contains("Accum",na=False)]["stock_name"].head(5).tolist() if not rot.empty else []
    dis=rot.drop_duplicates("ticker")[rot.drop_duplicates("ticker")["trend_label"].str.contains("Distrib",na=False)]["stock_name"].head(5).tolist() if not rot.empty else []
    prompt=f"""Analysing Indian mutual fund stock holdings. Categories: {selected}. Funds: {tf}. Stocks: {ts}.
Top 10 held stocks: {t10}
Accumulating: {acc}
Distributing: {dis}
Quarters analysed: {quarters[0]} to {quarters[-1]}
Tasks: 1) Fund manager consensus interpretation 2) Accumulation insight 3) Distribution warning 4) Sector concentration risk 5) 3 actionable takeaways for long-term SIP investor. Use ## headers."""
    with st.spinner("Analysing..."):
        res=get_claude_analysis(prompt,api_key=api_key or None)
    st.session_state["h_analysis"]=res
if "h_analysis" in st.session_state:
    st.markdown(f'<div class="analysis-box">{st.session_state["h_analysis"]}</div>',unsafe_allow_html=True)
    st.download_button("⬇ Download",st.session_state["h_analysis"],"holdings_analysis.md","text/markdown")