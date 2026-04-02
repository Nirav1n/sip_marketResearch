"""
app.py — India SIP Analyzer v3 — Engaging Landing Page
"""
import streamlit as st

st.set_page_config(
    page_title="India SIP Analyzer", page_icon="🇮🇳",
    layout="wide", initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;background:#080d14;}

/* Hero */
.hero{text-align:center;padding:64px 20px 48px;}
.hero-badge{display:inline-block;padding:5px 16px;border-radius:20px;font-size:.72rem;
  font-weight:700;letter-spacing:2px;text-transform:uppercase;
  background:rgba(0,212,170,.1);color:#00d4aa;border:1px solid rgba(0,212,170,.25);
  margin-bottom:20px;}
.hero-title{font-size:3.2rem;font-weight:800;color:#f1f5f9;line-height:1.15;
  margin:0 0 16px;letter-spacing:-1px;}
.hero-title span{background:linear-gradient(135deg,#00d4aa,#3b82f6);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.hero-sub{color:#6b7280;font-size:1.05rem;max-width:560px;margin:0 auto 36px;line-height:1.7;}

/* Stats bar */
.stats-bar{display:flex;justify-content:center;gap:48px;
  background:rgba(255,255,255,.03);border:1px solid #1a2535;
  border-radius:16px;padding:20px 40px;margin:0 auto 56px;max-width:700px;}
.stat-item{text-align:center;}
.stat-num{font-family:'JetBrains Mono',monospace;font-size:1.6rem;font-weight:700;color:#00d4aa;}
.stat-lbl{font-size:.72rem;color:#4b5563;text-transform:uppercase;letter-spacing:1px;margin-top:2px;}

/* Nav cards */
.nav-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:900px;margin:0 auto 48px;}
.nav-card{background:linear-gradient(135deg,#0d1a28 0%,#0a1520 100%);
  border:1px solid #1a2d42;border-radius:16px;padding:28px 28px 24px;
  cursor:pointer;transition:all .25s;text-decoration:none;display:block;position:relative;overflow:hidden;}
.nav-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--accent),transparent);opacity:0;transition:.25s;}
.nav-card:hover{border-color:var(--accent);transform:translateY(-3px);
  box-shadow:0 12px 40px rgba(0,0,0,.4);}
.nav-card:hover::before{opacity:1;}
.nav-icon{font-size:1.8rem;margin-bottom:12px;}
.nav-title{font-size:1.05rem;font-weight:700;color:#f1f5f9;margin-bottom:6px;}
.nav-desc{font-size:.82rem;color:#4b5563;line-height:1.6;}
.nav-badge{position:absolute;top:16px;right:16px;padding:3px 10px;border-radius:12px;
  font-size:.65rem;font-weight:700;letter-spacing:1px;text-transform:uppercase;}
.badge-live{background:rgba(0,212,170,.12);color:#00d4aa;border:1px solid rgba(0,212,170,.25);}
.badge-new{background:rgba(59,130,246,.12);color:#3b82f6;border:1px solid rgba(59,130,246,.25);}

/* Trust section */
.trust-section{background:rgba(255,255,255,.02);border:1px solid #1a2535;
  border-radius:16px;padding:28px 32px;max-width:900px;margin:0 auto 40px;}
.trust-title{font-size:.75rem;color:#4b5563;text-transform:uppercase;letter-spacing:2px;
  margin-bottom:16px;text-align:center;}
.trust-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;}
.trust-item{text-align:center;padding:12px;}
.trust-icon{font-size:1.3rem;margin-bottom:6px;}
.trust-hdr{font-size:.82rem;font-weight:600;color:#94a3b8;margin-bottom:4px;}
.trust-txt{font-size:.74rem;color:#4b5563;line-height:1.5;}

/* Disclaimer */
.disclaimer{text-align:center;color:#374151;font-size:.73rem;padding:16px 0 32px;line-height:1.6;}

div[data-testid="stButton"]>button{
  background:linear-gradient(135deg,#0d1a28,#0a1520) !important;
  border:1px solid #1a2d42 !important;border-radius:16px !important;
  padding:28px 28px 24px !important;color:#f1f5f9 !important;
  font-family:'Sora',sans-serif !important;font-size:.9rem !important;
  font-weight:600 !important;text-align:left !important;
  width:100% !important;height:auto !important;min-height:120px !important;
  white-space:pre-line !important;line-height:1.7 !important;
  transition:all .2s !important;
}
div[data-testid="stButton"]>button:hover{
  border-color:#00d4aa !important;transform:translateY(-2px) !important;
  box-shadow:0 8px 30px rgba(0,0,0,.4) !important;color:#00d4aa !important;
}
</style>
""", unsafe_allow_html=True)

# ─── HERO ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <div class="hero-badge">🇮🇳 India · Mutual Fund Research Platform</div>
  <h1 class="hero-title">Know exactly where your<br><span>SIP money goes</span></h1>
  <p class="hero-sub">
    Real CAGR from NAV history · Stock-level holdings intelligence · 
    AI-powered fund comparison · Built for long-term investors
  </p>
</div>
""", unsafe_allow_html=True)

# ─── STATS BAR ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="stats-bar">
  <div class="stat-item"><div class="stat-num">5,000+</div><div class="stat-lbl">Funds Tracked</div></div>
  <div class="stat-item"><div class="stat-num">13</div><div class="stat-lbl">SEBI Categories</div></div>
  <div class="stat-item"><div class="stat-num">200+</div><div class="stat-lbl">NSE Stocks Mapped</div></div>
  <div class="stat-item"><div class="stat-num">Free</div><div class="stat-lbl">No Subscription</div></div>
</div>
""", unsafe_allow_html=True)

# ─── NAV CARDS via Streamlit buttons ──────────────────────────────────────────
c1, c2 = st.columns(2, gap="large")

with c1:
    if st.button(
        "📊  Market Overview\n\n"
        "Nifty 50 · Sensex · Gold · Silver · Crude Oil\n"
        "3-year price charts · Sector rotation signals",
        key="nav_home", use_container_width=True,
    ): st.switch_page("pages/1_Home.py")

    if st.button(
        "📋  All Mutual Funds\n\n"
        "Real 1Y / 3Y / 5Y CAGR from NAV history\n"
        "Detailed filters · AI insights per selection",
        key="nav_funds", use_container_width=True,
    ): st.switch_page("pages/3_All_Funds.py")

with c2:
    if st.button(
        "🔍  Stock Holdings Intelligence\n\n"
        "Which stocks do funds actually own?\n"
        "Conviction scores · Quarterly rotation tracker",
        key="nav_hold", use_container_width=True,
    ): st.switch_page("pages/2_Stock_Holdings.py")

    if st.button(
        "⚖️  Compare Funds  ·  AI Verdict\n\n"
        "Select up to 3 funds · Side-by-side metrics\n"
        "Radar chart · Claude AI recommendation",
        key="nav_cmp", use_container_width=True,
    ): st.switch_page("pages/4_Compare_Funds.py")

st.markdown("<br>", unsafe_allow_html=True)

# ─── DATA TRUST SECTION ───────────────────────────────────────────────────────
st.markdown("""
<div class="trust-section">
  <div class="trust-title">📡 How we source & verify data</div>
  <div class="trust-grid">
    <div class="trust-item">
      <div class="trust-icon">📈</div>
      <div class="trust-hdr">CAGR — Real NAV History</div>
      <div class="trust-txt">
        Fetched from <strong>mfapi.in</strong> — India's most used free MF API.
        CAGR computed directly from actual NAV on start and end date.
        Same method used by Groww, ET Money.
      </div>
    </div>
    <div class="trust-item">
      <div class="trust-icon">🏦</div>
      <div class="trust-hdr">AUM — AMFI Official Data</div>
      <div class="trust-txt">
        Fund list from <strong>AMFI NAVAll.txt</strong> (official, updated daily).
        Per-fund AUM attempted via mfapi metadata. 
        Where unavailable: AMFI category-level AUM is shown with a clear label.
      </div>
    </div>
    <div class="trust-item">
      <div class="trust-icon">💸</div>
      <div class="trust-hdr">Expense Ratio — SEBI TER Data</div>
      <div class="trust-txt">
        Fetched from mfapi metadata where available.
        Fallback: SEBI-mandated TER limit ranges for Direct plans (0.1%–1.2%).
        Always shown with data source label.
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ─── DISCLAIMER ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="disclaimer">
  ⚠️ This is a research and education tool only. Data sourced from AMFI India and mfapi.in (free, public APIs).<br>
  AUM values shown are approximations where per-fund data is unavailable from free sources.<br>
  Not financial advice. Consult a SEBI-registered investment advisor before making investment decisions.
</div>
""", unsafe_allow_html=True)