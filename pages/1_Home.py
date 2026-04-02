import streamlit as st, plotly.graph_objects as go, plotly.express as px, pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from market_data import (fetch_nifty50,fetch_sensex,fetch_gold,fetch_silver,
    fetch_crude_oil,fetch_usd_inr,get_market_snapshot,get_1y_returns,get_sector_rotation_signal)

st.set_page_config(page_title="Market Overview", page_icon="📊", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.kpi{background:linear-gradient(135deg,#0f1923,#1a2535);border:1px solid #1e2d3d;border-radius:12px;padding:18px 20px;}
.kpi-l{font-size:.7rem;color:#4b5563;text-transform:uppercase;letter-spacing:1.5px;}
.kpi-p{font-family:'JetBrains Mono',monospace;font-size:1.45rem;font-weight:700;color:#e2e8f0;margin:4px 0 2px;}
.shdr{font-size:.95rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:2px;
  border-bottom:1px solid #1e2d3d;padding-bottom:8px;margin:24px 0 14px;}
.sig{display:flex;align-items:center;gap:14px;background:#0d1520;border:1px solid #1a2535;
  border-radius:10px;padding:12px 16px;margin-bottom:8px;}
</style>""", unsafe_allow_html=True)

def theme(fig, h=None, **kw):
    fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Sora,sans-serif",color="#6b7280"),
        margin=dict(l=10,r=10,t=36,b=10),**({"height":h} if h else {}),**kw)
    return fig

st.markdown("<h1 style='font-size:1.8rem;font-weight:700;color:#f1f5f9;margin:0 0 4px;'>📊 Market Overview</h1>",unsafe_allow_html=True)
st.markdown("<p style='color:#4b5563;font-size:.85rem;margin:0 0 20px;'>Long-term macro view · 3-year charts · Not for day trading</p>",unsafe_allow_html=True)

snap = get_market_snapshot()
ret1y = get_1y_returns()

items = [("Nifty 50","₹",""),("Sensex","₹",""),("Gold","₹","/10g"),("Silver","₹","/kg"),("Crude Oil","$","/bbl"),("USD/INR","₹","")]
cols = st.columns(6)
for col,(key,pre,suf) in zip(cols,items):
    d = snap.get(key,{"price":0,"change_pct":0})
    p,chg = d["price"],d["change_pct"]
    pf = f"{pre}{p:,.0f}{suf}" if p>500 else f"{pre}{p:.2f}{suf}"
    clr = "#00d4aa" if chg>=0 else "#ef4444"
    sym = "▲" if chg>=0 else "▼"
    col.markdown(f"""<div class="kpi"><div class="kpi-l">{key}</div><div class="kpi-p">{pf}</div>
    <div style="font-size:.8rem;color:{clr};">{sym} {abs(chg):.2f}% today</div></div>""",unsafe_allow_html=True)

st.markdown('<div class="shdr">1-Year Returns</div>',unsafe_allow_html=True)
rdf = pd.DataFrame({"Asset":list(ret1y.keys()),"1Y Return (%)":list(ret1y.values())})
fig_r = px.bar(rdf,x="Asset",y="1Y Return (%)",color="1Y Return (%)",
    color_continuous_scale="RdYlGn",text=rdf["1Y Return (%)"].map(lambda x:f"{x:.1f}%"),
    title="1-Year Return Comparison")
fig_r.update_traces(textposition="outside")
theme(fig_r,h=300); st.plotly_chart(fig_r,use_container_width=True)

st.markdown('<div class="shdr">3-Year Index Charts</div>',unsafe_allow_html=True)
c1,c2 = st.columns(2)
for col,(fn,name,color) in zip([c1,c2],[(fetch_nifty50,"Nifty 50","#00d4aa"),(fetch_sensex,"Sensex","#3b82f6")]):
    df = fn(756)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"],y=df["price"],fill="tozeroy",name=name,
        line=dict(color=color,width=1.5),fillcolor=f"{color}14"))
    theme(fig,h=280,title=f"{name} — 3Y",yaxis=dict(tickprefix="₹",tickformat=","))
    col.plotly_chart(fig,use_container_width=True)

st.markdown('<div class="shdr">Commodities — 3 Year</div>',unsafe_allow_html=True)
c3,c4,c5 = st.columns(3)
for col,(fn,name,color,pre) in zip([c3,c4,c5],[
    (fetch_gold,"Gold ₹/10g","#f59e0b","₹"),(fetch_silver,"Silver ₹/kg","#94a3b8","₹"),
    (fetch_crude_oil,"Brent Crude USD","#ef4444","$")]):
    df = fn(756)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"],y=df["price"],fill="tozeroy",name=name,
        line=dict(color=color,width=1.5),fillcolor=f"{color}14"))
    theme(fig,h=260,title=f"{name} — 3Y",yaxis=dict(tickprefix=pre,tickformat=","))
    col.plotly_chart(fig,use_container_width=True)

st.markdown('<div class="shdr">Sector Rotation Signals — 12M+ Horizon</div>',unsafe_allow_html=True)
sigs = get_sector_rotation_signal()
cl,cr = st.columns(2)
for i,s in enumerate(sigs):
    col = cl if i%2==0 else cr
    clr = s["color"]
    lbl = {"#00d4aa":"Overweight","#f59e0b":"Neutral","#ef4444":"Underweight"}.get(clr,"Neutral")
    col.markdown(f"""<div class="sig">
    <div style="width:10px;height:10px;border-radius:50%;background:{clr};flex-shrink:0;"></div>
    <div><div style="font-weight:600;font-size:.88rem;color:#e2e8f0;">{s['sector']}
    <span style="font-size:.7rem;color:{clr};margin-left:8px;">{lbl}</span></div>
    <div style="font-size:.75rem;color:#4b5563;">{s['reason']}</div></div></div>""",unsafe_allow_html=True)

st.markdown("<p style='color:#374151;font-size:.73rem;margin-top:16px;'>⚠️ Research only. Not financial advice.</p>",unsafe_allow_html=True)