"""
app.py — Entry point for India SIP Analyzer multi-page app.
Run: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="India SIP Analyzer",
    page_icon="🇮🇳",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

div[data-testid="stButton"] > button {
    width: 100%;
    background: #0d1520;
    border: 1px solid #1a2535;
    border-radius: 14px;
    padding: 36px 20px;
    color: #f1f5f9;
    font-size: 0.95rem;
    font-weight: 600;
    cursor: pointer;
    white-space: pre-line;
    line-height: 1.9;
    height: auto;
    min-height: 140px;
}
div[data-testid="stButton"] > button:hover {
    border-color: #00d4aa !important;
    background: #0f1e2e !important;
    color: #00d4aa !important;
}
div[data-testid="stButton"] > button:focus {
    box-shadow: none;
    border-color: #00d4aa !important;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style='text-align:center; padding: 60px 20px 40px;'>
    <div style='font-size:3.5rem;'>🇮🇳</div>
    <h1 style='font-size:2.4rem;font-weight:700;color:#f1f5f9;margin:16px 0 8px;'>India SIP Analyzer</h1>
    <p style='color:#4b5563;font-size:1rem;max-width:520px;margin:0 auto 32px;'>
        Stock-level mutual fund intelligence. Powered by AMFI data and Claude AI.<br>
        Built for long-term investors — not traders.
    </p>
</div>
""", unsafe_allow_html=True)

c1, c2 = st.columns(2, gap="large")

with c1:
    if st.button(
        "📊  Market Overview\n\nNifty 50 · Sensex · Gold · Silver · Crude Oil\n3-year charts · Sector rotation signals",
        key="nav_home",
        use_container_width=True,
    ):
        st.switch_page("pages/1_Home.py")

with c2:
    if st.button(
        "🔍  Stock Holdings Intelligence\n\nSelect SIP categories → See which stocks funds own\nConviction scores · Rotation tracker · AI analysis",
        key="nav_holdings",
        use_container_width=True,
    ):
        st.switch_page("pages/2_Stock_Holdings.py")

st.markdown("""
<p style='text-align:center;color:#374151;font-size:0.78rem;margin-top:40px;'>
👈 Use the sidebar to navigate between pages<br>
⚠️ Not financial advice. Consult a SEBI-registered advisor before investing.
</p>""", unsafe_allow_html=True)