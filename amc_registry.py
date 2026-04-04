"""
amc_registry.py
===============
Single source of truth for all 47 AMCs listed on AMFI.
Each entry contains:
  - amc_id        : short unique key used in DB tables and file names
  - name          : exact name as shown on AMFI download-factsheets page
  - portfolio_url : direct URL to monthly portfolio Excel/CSV
                    (the page that has the actual holdings file link)
  - file_pattern  : known direct download URL pattern (if discoverable)
  - format        : 'excel_single'  = all funds in one sheet
                    'excel_multi'   = one sheet per fund scheme
                    'excel_detect'  = auto-detect at parse time
                    'csv'           = CSV file
                    'pdf_only'      = PDF only, skip for now
                    'js_dynamic'    = page is JS-rendered, needs selenium
  - tier          : 1 = top 10 by AUM (parse first, highest priority)
                    2 = mid-size AMCs
                    3 = smaller AMCs
  - active        : True = include in scrape run
  - notes         : any special parsing notes

MAINTENANCE:
  - Run `python amc_registry.py --verify` to test all URLs
  - Update file_pattern when an AMC changes their URL structure
  - Set active=False to temporarily skip a broken AMC
"""

from datetime import datetime

# Format symbols used in file_pattern templates:
# {YYYY-MM}, {MON_YY}, {MMMYYYY}, {MM_YYYY}

AMC_REGISTRY = [

    # ── TIER 1 — Top 10 by AUM (cover ~80% of industry AUM) ──────────────────

    {
        "amc_id": "hdfc",
        "name": "HDFC Mutual Fund",
        "portfolio_url": "https://www.hdfcfund.com/statutory-disclosure/portfolio/fortnightly-portfolio",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "File hosted on files.hdfcfund.com/s3fs-public/YYYY-MM/. Sheet per fund.",
    },
    {
        "amc_id": "sbi",
        "name": "SBI Mutual Fund",
        "portfolio_url": "https://www.sbimf.com/en-us/portfolios",
        "file_pattern": "https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-28th-february-2026.xlsx",
        "format": "excel_single",
        "tier": 1,
        "active": True,
        "notes": "SBI uses MMMYYYY format e.g. MAR2026. Sheet per fund.",
    },
    {
        "amc_id": "icici_pru",
        "name": "ICICI Prudential Mutual Fund",
        "portfolio_url": "https://www.icicipruamc.com/media-center/downloads?currentTabFilter=OtherSchemeDisclosures",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "ICICI uses MON_YY format e.g. MAR_26.",
    },
    {
        "amc_id": "nippon",
        "name": "Nippon India Mutual Fund",
        "portfolio_url": "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-reports",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Nippon page has direct Excel link. Scrape page to find link.",
    },
    {
        "amc_id": "kotak",
        "name": "Kotak Mahindra Mutual Fund",
        "portfolio_url": "https://www.kotakmf.com/downloads/monthly-portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Kotak page has Excel download. Scrape page for link.",
    },
    {
        "amc_id": "aditya_birla",
        "name": "Aditya Birla Sun Life Mutual Fund",
        "portfolio_url": "https://mutualfund.adityabirlacapital.com/Investor/ResourceCenter/PortfolioDisclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "ABSL page. Scrape for Excel link.",
    },
    {
        "amc_id": "axis",
        "name": "Axis Mutual Fund",
        "portfolio_url": "https://www.axismf.com/statutory-disclosures",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "Axis uses MON_YY e.g. MAR_26.",
    },
    {
        "amc_id": "mirae",
        "name": "Mirae Asset Mutual Fund",
        "portfolio_url": "https://www.miraeassetmf.co.in/downloads/monthly-portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Mirae page. Scrape for Excel link.",
    },
    {
        "amc_id": "dsp",
        "name": "DSP Mutual Fund",
        "portfolio_url": "https://www.dspim.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "DSP page. Scrape for Excel link.",
    },
    {
        "amc_id": "franklin",
        "name": "Franklin Templeton Mutual Fund",
        "portfolio_url": "https://www.franklintempletonindia.com/investor/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Franklin page. Scrape for Excel link.",
    },

    # ── TIER 2 — Mid-size AMCs ────────────────────────────────────────────────

    {
        "amc_id": "uti",
        "name": "UTI Mutual Fund",
        "portfolio_url": "https://www.utimf.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": "UTI page. Scrape for Excel.",
    },
    {
        "amc_id": "tata",
        "name": "Tata Mutual Fund",
        "portfolio_url": "https://www.tatamutualfund.com/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "edelweiss",
        "name": "Edelweiss Mutual Fund",
        "portfolio_url": "https://www.edelweissmf.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "invesco",
        "name": "Invesco Mutual Fund",
        "portfolio_url": "https://www.invescomutualfund.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "sundaram",
        "name": "Sundaram Mutual Fund",
        "portfolio_url": "https://www.sundarammutual.com/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "canara_robeco",
        "name": "Canara Robeco Mutual Fund",
        "portfolio_url": "https://www.canararobeco.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "bandhan",
        "name": "Bandhan Mutual Fund",
        "portfolio_url": "https://bandhanmutual.com/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "hsbc",
        "name": "HSBC Mutual Fund",
        "portfolio_url": "https://www.assetmanagement.hsbc.co.in/en/retail-investors/downloads",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "motilal_oswal",
        "name": "Motilal Oswal Mutual Fund",
        "portfolio_url": "https://www.motilaloswalmf.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "pgim",
        "name": "PGIM India Mutual Fund",
        "portfolio_url": "https://www.pgimindiamf.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "360one",
        "name": "360 ONE Mutual Fund",
        "portfolio_url": "https://www.360onemf.com/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "ppfas",
        "name": "PPFAS Mutual Fund",
        "portfolio_url": "https://www.ppfas.com/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": "PPFAS (Parag Parikh). Small but high AUM.",
    },
    {
        "amc_id": "quant",
        "name": "quant Mutual Fund",
        "portfolio_url": "https://www.quantmutual.com/statutory-disclosures/portfolio-disclosures",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "baroda_bnp",
        "name": "Baroda BNP Paribas Mutual Fund",
        "portfolio_url": "https://www.barodabnpparibasmf.in/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },
    {
        "amc_id": "jm_financial",
        "name": "JM Financial Mutual Fund",
        "portfolio_url": "https://www.jmfinancialmf.com/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": None,
    },

    # ── TIER 3 — Smaller AMCs (scrape opportunistically) ─────────────────────

    {
        "amc_id": "bajaj_finserv",
        "name": "Bajaj Finserv Mutual Fund",
        "portfolio_url": "https://www.bajajfinservmf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "groww",
        "name": "Groww Mutual Fund",
        "portfolio_url": "https://www.growwmutualfund.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "mahindra_manulife",
        "name": "Mahindra Manulife Mutual Fund",
        "portfolio_url": "https://www.mahindramanulifemf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "lic",
        "name": "LIC Mutual Fund",
        "portfolio_url": "https://www.licmf.com/downloads/portfolio-disclosure",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "nj",
        "name": "NJ Mutual Fund",
        "portfolio_url": "https://www.njmutualfund.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "zerodha",
        "name": "Zerodha Mutual Fund",
        "portfolio_url": "https://zerodhamf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "helios",
        "name": "Helios Mutual Fund",
        "portfolio_url": "https://www.heliosmf.in/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "whiteoak",
        "name": "WhiteOak Capital Mutual Fund",
        "portfolio_url": "https://www.whiteoakcapital.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "angel_one",
        "name": "Angel One Mutual Fund",
        "portfolio_url": "https://www.angelonemf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "samco",
        "name": "Samco Mutual Fund",
        "portfolio_url": "https://www.samcomf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "navi",
        "name": "Navi Mutual Fund",
        "portfolio_url": "https://www.navimutualfund.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "old_bridge",
        "name": "Old Bridge Mutual Fund",
        "portfolio_url": "https://www.oldbridgemf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "shriram",
        "name": "Shriram Mutual Fund",
        "portfolio_url": "https://www.shrirammf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "taurus",
        "name": "Taurus Mutual Fund",
        "portfolio_url": "https://www.taurusmutualfund.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "trust",
        "name": "Trust Mutual Fund",
        "portfolio_url": "https://www.trustmf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "quantum",
        "name": "Quantum Mutual Fund",
        "portfolio_url": "https://www.quantumamc.com/portfolio-disclosure",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "union",
        "name": "Union Mutual Fund",
        "portfolio_url": "https://www.unionmf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "capitalmind",
        "name": "Capitalmind Mutual Fund",
        "portfolio_url": "https://www.capitalmind.in/mf/downloads",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "iti",
        "name": "ITI Mutual Fund",
        "portfolio_url": "https://www.itimf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "jio_blackrock",
        "name": "Jio BlackRock Mutual Fund",
        "portfolio_url": "https://www.jioblackrockmf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "unifi",
        "name": "Unifi Mutual Fund",
        "portfolio_url": "https://www.unifimf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "abakkus",
        "name": "Abakkus Mutual Fund",
        "portfolio_url": "https://www.abakkusmf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "choice",
        "name": "Choice Mutual Fund",
        "portfolio_url": "https://www.choicemf.com/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
    {
        "amc_id": "wealth_company",
        "name": "The Wealth Company Mutual Fund",
        "portfolio_url": "https://www.thewealthcompany.in/downloads/portfolio",
        "file_pattern": None, "format": "excel_detect", "tier": 3, "active": True, "notes": None,
    },
]

# ─── LOOKUP HELPERS ───────────────────────────────────────────────────────────
def get_amc(amc_id: str) -> dict | None:
    return next((a for a in AMC_REGISTRY if a["amc_id"] == amc_id), None)

def get_active_amcs(tier: int | None = None) -> list:
    amcs = [a for a in AMC_REGISTRY if a["active"]]
    if tier: amcs = [a for a in amcs if a["tier"] == tier]
    return sorted(amcs, key=lambda x: (x["tier"], x["amc_id"]))

def get_amc_names() -> list:
    return [a["name"] for a in AMC_REGISTRY if a["active"]]

def get_tier1_amcs() -> list:
    return get_active_amcs(tier=1)

def get_amc_url(amc: dict, disclosure_month: str) -> str | None:
    pattern = amc.get("file_pattern")
    if not pattern: return None
    
    dt = datetime.strptime(disclosure_month, "%Y-%m")
    fmt = {
        "{YYYY-MM}": disclosure_month,
        "{MON_YY}": dt.strftime("%b%y").upper(),
        "{MMMYYYY}": dt.strftime("%b%Y").upper(),
        "{MM_YYYY}": dt.strftime("%m%Y"),
    }
    url = pattern
    for k, v in fmt.items():
        url = url.replace(k, v)
    return url


if __name__ == "__main__":
    print(f"Total AMCs registered: {len(AMC_REGISTRY)}")
    for tier in [1, 2, 3]:
        amcs = get_active_amcs(tier)
        print(f"\nTier {tier} ({len(amcs)} AMCs):")
        for a in amcs:
            url_status = "✅ known URL" if a["file_pattern"] else "🔍 needs page scrape"
            print(f"  {a['amc_id']:20} {a['name']:40} {url_status}")