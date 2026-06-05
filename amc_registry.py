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
import calendar

# Format symbols used in file_pattern templates:
# {YYYY-MM}       → "2026-04"
# {YYYY}          → "2026"
# {MON_YY}        → "APR26"
# {MMMYYYY}       → "APR2026"
# {MM_YYYY}       → "042026"
# {MMMM_LOWER}    → "april"
# {LAST_DAY_ORD}  → "30th"  (last day of month with ordinal suffix)

AMC_REGISTRY = [

    # ── TIER 1 — Top 10 by AUM (cover ~80% of industry AUM) ──────────────────
    # STATUS KEY: ✅ confirmed working  🔍 page-scrape fallback  ❌ JS-rendered / blocked

    {
        "amc_id": "hdfc",
        "name": "HDFC Mutual Fund",
        # ✅ Server-rendered page — scraper extracts ~109 xlsx links (one per fund)
        "portfolio_url": "https://www.hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio",
        "file_pattern": None,
        "format": "excel_per_fund",   # one xlsx per fund scheme (not one multi-sheet file)
        "tier": 1,
        "active": True,
        "notes": "CDN: files.hdfcfund.com/s3fs-public/{YYYY-MM}/Monthly {FundName} - {DD} {Month} {YYYY}.xlsx. Page scrape returns all 109 fund links.",
    },
    {
        "amc_id": "sbi",
        "name": "SBI Mutual Fund",
        # ✅ Direct URL confirmed working for Feb/Mar/Apr 2026
        "portfolio_url": "https://www.sbimf.com/en-us/portfolios",
        "file_pattern": "https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-{LAST_DAY_ORD}-{MMMM_LOWER}-{YYYY}.xlsx",
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "Single file all schemes. URL uses last day of month + ordinal e.g. '30th-april-2026'.",
    },
    {
        "amc_id": "icici_pru",
        "name": "ICICI Prudential Mutual Fund",
        # 🔍 JS-rendered SPA. Archive subdomain discovered: archive.icicipruamc.com/download/
        # Redirect: icicipruamc.com/download/Monthly_Portfolio_{MON}_{YY}.xlsx → archive subdomain
        "portfolio_url": "https://www.icicipruamc.com/media-center/downloads?currentTabFilter=OtherSchemeDisclosures",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "JS-SPA. Archive CDN at archive.icicipruamc.com/download/Monthly_Portfolio_{MON_YY_TITLE}.xlsx redirected from icicipruamc.com/download/ — needs browser session cookie. Needs Playwright.",
    },
    {
        "amc_id": "nippon",
        "name": "Nippon India Mutual Fund",
        # ✅ Page scrape confirmed — HTML contains relative .xls links in <a> tags
        # Direct URL pattern: mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-{DD}-{Month}-{YY}.xls
        # Month naming is inconsistent (April=full name, Mar/Feb=abbreviated) — use page scrape
        "portfolio_url": "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures",
        "file_pattern": None,
        "format": "excel_detect",   # .xls (old format), single multi-sheet file
        "tier": 1,
        "active": True,
        "notes": "SharePoint site but HTML is server-rendered. Links at /InvestorServices/FactsheetsDocuments/. Page scrape works. File is .xls not .xlsx.",
    },
    {
        "amc_id": "kotak",
        "name": "Kotak Mahindra Mutual Fund",
        # ❌ JS-rendered with bot-protection (perfdrive.com). Page scrape fails.
        "portfolio_url": "https://www.kotakmf.com/knowledge-center/forms-downloads/downloads",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Bot-protected JS SPA (perfdrive.com). Needs Playwright. Try AMFI portal fallback.",
    },
    {
        "amc_id": "aditya_birla",
        "name": "Aditya Birla Sun Life Mutual Fund",
        # ❌ JS-rendered SPA. No xlsx links in static HTML.
        "portfolio_url": "https://mutualfund.adityabirlacapital.com/Investor/ResourceCenter/PortfolioDisclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "JS SPA. No static Excel links. Try AMFI portal fallback.",
    },
    {
        "amc_id": "axis",
        "name": "Axis Mutual Fund",
        # ❌ JS-rendered SPA (Angular). Portfolio files use numeric IDs on axismf.com CDN.
        "portfolio_url": "https://www.axismf.com/statutory-disclosures",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 1,
        "active": True,
        "notes": "Angular SPA. Files served from axismf.com/1/5/XXXX/XXXX/ numeric CDN paths. Needs Playwright.",
    },
    {
        "amc_id": "mirae",
        "name": "Mirae Asset Mutual Fund",
        # ❌ JS-rendered. Redirect on guessed URLs leads to /downloads/forms (not portfolio).
        "portfolio_url": "https://www.miraeassetmf.co.in/downloads",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "JS-rendered. Direct URL guesses fail. Try AMFI portal fallback.",
    },
    {
        "amc_id": "dsp",
        "name": "DSP Mutual Fund",
        # 🔍 Try their mandatory disclosures page
        "portfolio_url": "https://www.dspim.com/mandatory-disclosures/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Try page scrape on mandatory-disclosures/portfolio.",
    },
    {
        "amc_id": "franklin",
        "name": "Franklin Templeton Mutual Fund",
        # 🔍 Page is server-rendered but no xlsx links found on initial attempt.
        "portfolio_url": "https://www.franklintempletonindia.com/investor/downloads/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 1,
        "active": True,
        "notes": "Server-rendered but Excel links not visible in initial scrape. Try with Referer header.",
    },

    # ── TIER 2 — Mid-size AMCs ────────────────────────────────────────────────

    {
        "amc_id": "uti",
        "name": "UTI Mutual Fund",
        "portfolio_url": "https://www.utimf.com/statutory-disclosures/portfolio-disclosure",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": "Try page scrape.",
    },
    {
        "amc_id": "tata",
        "name": "Tata Mutual Fund",
        # 🔍 Server-rendered (Drupal). CDN: betacmsadmin.tatamutualfund.com/system/files/{YYYY-MM}/
        "portfolio_url": "https://www.tatamutualfund.com/statutory-disclosure/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": "Drupal CMS. CDN path: betacmsadmin.tatamutualfund.com/system/files/. Try page scrape.",
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
        # ✅ Page scrape confirmed — links at S3 bucket with content hashes (unpredictable, must scrape)
        "portfolio_url": "https://www.360.one/asset/mutual-funds/downloads/",
        "file_pattern": None,
        "format": "excel_multi",
        "tier": 2,
        "active": True,
        "notes": "S3 bucket: s3.ap-south-1.amazonaws.com/x-web-s3.360.one/IN_MF_MONTHLY_PORTFOLIO_*.xlsx. Hashes unpredictable — page scrape only.",
    },
    {
        "amc_id": "ppfas",
        "name": "PPFAS Mutual Fund",
        "portfolio_url": "https://www.ppfas.com/mutual-fund/downloads/portfolio",
        "file_pattern": None,
        "format": "excel_detect",
        "tier": 2,
        "active": True,
        "notes": "Parag Parikh — high AUM, popular. Try page scrape.",
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
        # ✅ Page confirmed working — returns direct CDN xlsx link
        "portfolio_url": "https://www.growwmf.in/statutory-disclosure/portfolio",
        "file_pattern": None, "format": "excel_multi", "tier": 3, "active": True,
        "notes": "CDN: assets-netstorage.growwmf.in. Page scrape finds direct xlsx link. 58 sheets per file.",
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

def _ordinal(n: int) -> str:
    """Return number with ordinal suffix: 1→'1st', 2→'2nd', 30→'30th'."""
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"


def get_amc_url(amc: dict, disclosure_month: str) -> str | None:
    pattern = amc.get("file_pattern")
    if not pattern: return None

    dt = datetime.strptime(disclosure_month, "%Y-%m")
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    # MON_YY_TITLE = "Apr_26" (used by ICICI Pru archive CDN)
    mon_yy_title = dt.strftime("%b") + "_" + dt.strftime("%y")

    fmt = {
        "{YYYY-MM}":       disclosure_month,
        "{YYYY}":          dt.strftime("%Y"),
        "{MON_YY}":        dt.strftime("%b%y").upper(),
        "{MMMYYYY}":       dt.strftime("%b%Y").upper(),
        "{MM_YYYY}":       dt.strftime("%m%Y"),
        "{MMMM_LOWER}":    dt.strftime("%B").lower(),
        "{LAST_DAY_ORD}":  _ordinal(last_day),
        "{MON_YY_TITLE}":  mon_yy_title,
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