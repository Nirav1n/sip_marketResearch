"""
app.py — India SIP Analyzer v3 entry point
"""
import streamlit as st

st.set_page_config(page_title="India SIP Analyzer", page_icon="🇮🇳", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.nav-card{background:#0d1520;border:1px solid #1a2535;border-radius:14px;padding:28px 24px;
  text-align:center;transition:border-color .2s;}
.nav-card:hover{border-color:#00d4aa;}
.nav-icon{font-size:2rem;margin-bottom:10px;}
.nav-title{font-size:1rem;font-weight:700;color:#f1f5f9;margin-bottom:6px;}
.nav-desc{color:#4b5563;font-size:.83rem;line-height:1.6;}
.tag{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.68rem;
  font-weight:600;margin-top:8px;background:rgba(0,212,170,.12);color:#00d4aa;border:1px solid rgba(0,212,170,.25);}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style='text-align:center;padding:48px 20px 36px;'>
  <div style='font-size:3rem;'>🇮🇳</div>
  <h1 style='font-size:2.2rem;font-weight:700;color:#f1f5f9;margin:14px 0 6px;'>India SIP Analyzer</h1>
  <p style='color:#4b5563;font-size:.95rem;max-width:500px;margin:0 auto 8px;'>
    Real mutual fund intelligence · AMFI + mfapi.in data · Claude AI insights
  </p>
  <p style='color:#374151;font-size:.78rem;'>Built for long-term investors · Not for day trading</p>
</div>
""", unsafe_allow_html=True)

pages = [
    ("📊","Market Overview","pages/1_Home.py",
     "Nifty 50 · Sensex · Gold · Silver · Crude Oil\n3-year charts · Sector rotation signals","Live"),
    ("🔍","Stock Holdings","pages/2_Stock_Holdings.py",
     "Select SIP categories · Stock conviction scores\nQuarterly rotation tracker · AI analysis","Real"),
    ("📋","All Mutual Funds","pages/3_All_Funds.py",
     "Full fund browser · Real 1Y/3Y/5Y CAGR\nDetailed filters · AI insights per filter","New"),
    ("⚖️","Compare Funds","pages/4_Compare_Funds.py",
     "Select up to 3 funds · Side-by-side metrics\nRadar chart · AI verdict & allocation","New"),
]

c1, c2 = st.columns(2, gap="large")
cols = [c1, c2, c1, c2]

for col, (icon, title, path, desc, badge) in zip(cols, pages):
    with col:
        if st.button(f"{icon}  {title}\n\n{desc}", key=f"nav_{title}", use_container_width=True):
            st.switch_page(path)
        st.markdown(f'<div class="tag">{badge}</div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

st.markdown("""
<p style='text-align:center;color:#374151;font-size:.75rem;margin-top:16px;'>
  👈 Use sidebar to navigate · Data: AMFI India + mfapi.in (free, official) + yfinance<br>
  ⚠️ Research tool only. Not financial advice. Consult a SEBI-registered advisor.
</p>""", unsafe_allow_html=True)