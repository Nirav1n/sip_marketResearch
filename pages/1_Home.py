"""
pages/1_Home.py
Macro Market Dashboard — Nifty50, Sensex, Gold, Silver, Oil, Sector Signals
Long-term investing oriented. No day-trade noise.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from market_data import (
    fetch_nifty50, fetch_sensex, fetch_gold, fetch_silver,
    fetch_crude_oil, fetch_usd_inr, get_market_snapshot,
    get_1y_returns, get_sector_rotation_signal,
)

st.set_page_config(page_title="Market Overview", page_icon="🏠", layout="wide")

# ─── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

.kpi-card {
    background: linear-gradient(135deg, #0f1923 0%, #1a2535 100%);
    border: 1px solid #1e2d3d;
    border-radius: 12px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
}
.kpi-label { font-size: 0.72rem; color: #5a6a7a; text-transform: uppercase; letter-spacing: 1.5px; }
.kpi-price { font-family: 'JetBrains Mono', monospace; font-size: 1.55rem; font-weight: 700; color: #e2e8f0; margin: 4px 0 2px; }
.kpi-change-pos { font-size: 0.82rem; color: #00d4aa; font-weight: 600; }
.kpi-change-neg { font-size: 0.82rem; color: #ef4444; font-weight: 600; }

.signal-card {
    display: flex; align-items: center; gap: 14px;
    background: #0d1520; border: 1px solid #1a2535;
    border-radius: 10px; padding: 12px 16px; margin-bottom: 8px;
}
.signal-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
.signal-sector { font-weight: 600; font-size: 0.9rem; color: #e2e8f0; }
.signal-reason { font-size: 0.76rem; color: #5a6a7a; margin-top: 2px; }

.section-hdr {
    font-size: 1.1rem; font-weight: 700; color: #94a3b8;
    text-transform: uppercase; letter-spacing: 2px;
    border-bottom: 1px solid #1e2d3d; padding-bottom: 8px; margin: 24px 0 16px;
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
<h1 style='font-size:1.9rem;font-weight:700;color:#f1f5f9;margin:0 0 4px;'>
  📊 Market Overview
</h1>
<p style='color:#4b5563;font-size:0.88rem;margin:0 0 24px;'>
  Long-term macro indicators · 3-year view · Updated daily · Not for day trading
</p>
""", unsafe_allow_html=True)

# ─── KPI CARDS ────────────────────────────────────────────────────────────────
snapshot = get_market_snapshot()
returns_1y = get_1y_returns()

kpi_items = [
    ("Nifty 50", "Nifty 50", "₹", ""),
    ("Sensex", "Sensex", "₹", ""),
    ("Gold", "Gold", "₹", "/10g"),
    ("Silver", "Silver", "₹", "/kg"),
    ("Crude Oil", "Crude Oil", "$", "/bbl"),
    ("USD/INR", "USD/INR", "₹", ""),
]

cols = st.columns(6)
for col, (key, label, prefix, suffix) in zip(cols, kpi_items):
    data = snapshot[key]
    price = data["price"]
    chg = data["change_pct"]
    chg_str = f"{'▲' if chg >= 0 else '▼'} {abs(chg):.2f}% today"
    chg_cls = "kpi-change-pos" if chg >= 0 else "kpi-change-neg"
    price_fmt = f"{prefix}{price:,.0f}{suffix}" if price > 500 else f"{prefix}{price:.2f}{suffix}"
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-price">{price_fmt}</div>
        <div class="{chg_cls}">{chg_str}</div>
    </div>""", unsafe_allow_html=True)

# ─── 1Y RETURNS BAR ───────────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">1-Year Returns Comparison</div>', unsafe_allow_html=True)
ret_df = pd.DataFrame({"Asset": list(returns_1y.keys()), "1Y Return (%)": list(returns_1y.values())})
fig_ret = px.bar(
    ret_df, x="Asset", y="1Y Return (%)",
    color="1Y Return (%)", color_continuous_scale="RdYlGn",
    text=ret_df["1Y Return (%)"].map(lambda x: f"{x:.1f}%"),
    title="1-Year Return — Nifty vs Sensex vs Commodities",
)
fig_ret.update_traces(textposition="outside")
apply_theme(fig_ret, height=300)
st.plotly_chart(fig_ret, use_container_width=True)

# ─── CHARTS ROW 1 — NIFTY + SENSEX ───────────────────────────────────────────
st.markdown('<div class="section-hdr">Index Performance — 3 Year</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)

nifty_df = fetch_nifty50(756)
sensex_df = fetch_sensex(756)

with c1:
    fig_n = go.Figure()
    fig_n.add_trace(go.Scatter(
        x=nifty_df["date"], y=nifty_df["price"],
        fill="tozeroy", name="Nifty 50",
        line=dict(color="#00d4aa", width=1.5),
        fillcolor="rgba(0,212,170,0.08)",
    ))
    apply_theme(fig_n, height=280, title="Nifty 50 — 3Y",
                yaxis=dict(tickprefix="₹", tickformat=","))
    st.plotly_chart(fig_n, use_container_width=True)

with c2:
    fig_s = go.Figure()
    fig_s.add_trace(go.Scatter(
        x=sensex_df["date"], y=sensex_df["price"],
        fill="tozeroy", name="Sensex",
        line=dict(color="#3b82f6", width=1.5),
        fillcolor="rgba(59,130,246,0.08)",
    ))
    apply_theme(fig_s, height=280, title="Sensex — 3Y",
                yaxis=dict(tickprefix="₹", tickformat=","))
    st.plotly_chart(fig_s, use_container_width=True)

# ─── CHARTS ROW 2 — GOLD + SILVER ─────────────────────────────────────────────
st.markdown('<div class="section-hdr">Commodities — 3 Year</div>', unsafe_allow_html=True)
c3, c4, c5 = st.columns(3)

gold_df = fetch_gold(756)
silver_df = fetch_silver(756)
oil_df = fetch_crude_oil(756)

with c3:
    fig_g = go.Figure()
    fig_g.add_trace(go.Scatter(
        x=gold_df["date"], y=gold_df["price"],
        fill="tozeroy", name="Gold",
        line=dict(color="#f59e0b", width=1.5),
        fillcolor="rgba(245,158,11,0.08)",
    ))
    apply_theme(fig_g, height=260, title="Gold ₹/10g — 3Y",
                yaxis=dict(tickprefix="₹", tickformat=","))
    st.plotly_chart(fig_g, use_container_width=True)

with c4:
    fig_sv = go.Figure()
    fig_sv.add_trace(go.Scatter(
        x=silver_df["date"], y=silver_df["price"],
        fill="tozeroy", name="Silver",
        line=dict(color="#94a3b8", width=1.5),
        fillcolor="rgba(148,163,184,0.08)",
    ))
    apply_theme(fig_sv, height=260, title="Silver ₹/kg — 3Y",
                yaxis=dict(tickprefix="₹", tickformat=","))
    st.plotly_chart(fig_sv, use_container_width=True)

with c5:
    fig_oil = go.Figure()
    fig_oil.add_trace(go.Scatter(
        x=oil_df["date"], y=oil_df["price"],
        fill="tozeroy", name="Crude",
        line=dict(color="#ef4444", width=1.5),
        fillcolor="rgba(239,68,68,0.08)",
    ))
    apply_theme(fig_oil, height=260, title="Brent Crude USD/bbl — 3Y",
                yaxis=dict(tickprefix="$"))
    st.plotly_chart(fig_oil, use_container_width=True)

# ─── SECTOR ROTATION SIGNALS ──────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Sector Rotation Signals — Investment Horizon 12M+</div>', unsafe_allow_html=True)

signals = get_sector_rotation_signal()
c_left, c_right = st.columns(2)
for i, sig in enumerate(signals):
    col = c_left if i % 2 == 0 else c_right
    badge_color = sig["color"]
    badge_map = {"#00d4aa": "Overweight", "#f59e0b": "Neutral", "#ef4444": "Underweight"}
    badge_label = [v for k, v in badge_map.items() if k == badge_color][0]
    col.markdown(f"""
    <div class="signal-card">
        <div class="signal-dot" style="background:{badge_color};box-shadow:0 0 6px {badge_color}60;"></div>
        <div style="flex:1;">
            <div class="signal-sector">{sig['sector']}
                <span style="font-size:0.72rem;color:{badge_color};font-weight:600;margin-left:8px;">
                    {badge_label}
                </span>
            </div>
            <div class="signal-reason">{sig['reason']}</div>
        </div>
    </div>""", unsafe_allow_html=True)

st.markdown("""
<p style='color:#374151;font-size:0.75rem;margin-top:20px;'>
⚠️ Signals are research-oriented and based on macro conditions. Not financial advice.
Consult a SEBI-registered investment advisor before making decisions.
</p>""", unsafe_allow_html=True)
