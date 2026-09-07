"""
nav_processor.py  v3  (fixed)
================================
Fixes vs v2:
  1. batch_process() — processes a full DataFrame of scheme_codes in one pass
     writes results directly to fund_performance DB via upsert_performance()
  2. compute_volatility() — real annualised std dev from NAV daily returns
  3. compute_sharpe() — uses RFR=6.5% (RBI repo rate), real vol
  4. compute_max_drawdown() — real max drawdown from NAV series
  5. Rate-limited: 0.15s delay per fund to avoid mfapi.in 429s
  6. Progress logging every 50 funds
  7. Inception-aware CAGR preserved from v2
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import time
import logging

log = logging.getLogger(__name__)

MFAPI_BASE = "https://api.mfapi.in/mf"
RISK_FREE_RATE = 6.5   # RBI repo rate %
DELAY_BETWEEN_CALLS = 0.15  # seconds — polite rate limiting


class LiveNAVProcessor:
    """
    Calculates real 1Y/3Y/5Y CAGR, volatility, Sharpe, and max drawdown
    from NAV history via api.mfapi.in.
    All metrics are inception-aware — no fake data for short-history funds.
    """

    @staticmethod
    def calculate_cagr(start_nav: float, end_nav: float, years: float) -> Optional[float]:
        if not start_nav or not end_nav or start_nav <= 0 or years <= 0:
            return None
        return round((((end_nav / start_nav) ** (1 / years)) - 1) * 100, 2)

    @staticmethod
    def compute_volatility(df: pd.DataFrame) -> Optional[float]:
        """
        Annualised volatility from daily NAV returns.
        Uses log returns for accuracy. Returns None for < 252 data points.
        Zero/negative NAV rows are dropped before computing to avoid log(0).
        """
        clean = df[df["nav"] > 0].copy()
        if len(clean) < 252:
            return None
        returns = np.log(clean["nav"] / clean["nav"].shift(1)).dropna()
        daily_vol = returns.std()
        if not np.isfinite(daily_vol) or daily_vol == 0:
            return None
        return round(float(daily_vol * np.sqrt(252) * 100), 2)  # annualised %

    @staticmethod
    def compute_sharpe(cagr_3y: Optional[float], volatility: Optional[float]) -> Optional[float]:
        """Sharpe ratio = (3Y CAGR - RFR) / annualised vol."""
        if cagr_3y is None or volatility is None or volatility <= 0:
            return None
        return round((cagr_3y - RISK_FREE_RATE) / volatility, 3)

    @staticmethod
    def compute_max_drawdown(df: pd.DataFrame) -> Optional[float]:
        """
        Maximum peak-to-trough drawdown over the NAV history.
        Returns negative %, e.g. -32.5 means 32.5% drawdown.
        Zero/negative NAV rows are dropped to avoid -100% artifacts.
        """
        clean = df[df["nav"] > 0].copy()
        if len(clean) < 30:
            return None
        nav = clean["nav"].values
        peak = np.maximum.accumulate(nav)
        drawdown = (nav - peak) / peak * 100
        result = float(drawdown.min())
        if not np.isfinite(result):
            return None
        return round(result, 2)

    def get_performance(self, scheme_code: str, save_nav: bool = True) -> Dict[str, Any]:
        """
        Fetch NAV history from mfapi.in and compute all metrics.
        Optionally persists raw NAV rows to nav_daily table.
        Returns empty dict on failure.
        """
        try:
            r = requests.get(
                f"{MFAPI_BASE}/{scheme_code}",
                timeout=15,
                headers={"User-Agent": "SIPAnalyzer/1.0"}
            )
            r.raise_for_status()
            raw     = r.json()
            history = raw.get("data", [])
            meta    = raw.get("meta", {})

            if not history:
                return {}

            df = pd.DataFrame(history)
            df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
            df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
            df = df.dropna(subset=["nav", "date"]).sort_values("date").reset_index(drop=True)

            if df.empty or len(df) < 30:
                return {}

            latest_nav  = df.iloc[-1]["nav"]
            latest_date = df.iloc[-1]["date"]
            launch_date = df.iloc[0]["date"]
            total_days  = (latest_date - launch_date).days

            perf = {
                "scheme_code":    str(scheme_code),
                "last_nav_date":  latest_date.strftime("%Y-%m-%d"),
                "launch_date":    launch_date.strftime("%Y-%m-%d"),
                "nav_data_points": len(df),
                "cagr_1y":   None,
                "cagr_3y":   None,
                "cagr_5y":   None,
                "cagr_10y":  None,
                "volatility":     None,
                "sharpe_ratio":   None,
                "max_drawdown":   None,
                "cagr_source":   "mfapi.in",
            }

            # CAGR calculations — inception-aware
            if total_days >= 360:
                h1y = df[df["date"] <= (latest_date - timedelta(days=365))]
                if not h1y.empty:
                    perf["cagr_1y"] = self.calculate_cagr(h1y.iloc[-1]["nav"], latest_nav, 1.0)

            if total_days >= 1080:
                h3y = df[df["date"] <= (latest_date - timedelta(days=1095))]
                if not h3y.empty:
                    perf["cagr_3y"] = self.calculate_cagr(h3y.iloc[-1]["nav"], latest_nav, 3.0)

            if total_days >= 1800:
                h5y = df[df["date"] <= (latest_date - timedelta(days=1825))]
                if not h5y.empty:
                    perf["cagr_5y"] = self.calculate_cagr(h5y.iloc[-1]["nav"], latest_nav, 5.0)

            if total_days >= 3600:
                h10y = df[df["date"] <= (latest_date - timedelta(days=3650))]
                if not h10y.empty:
                    perf["cagr_10y"] = self.calculate_cagr(h10y.iloc[-1]["nav"], latest_nav, 10.0)

            # Risk metrics — require sufficient history
            perf["volatility"]    = self.compute_volatility(df)
            perf["sharpe_ratio"]  = self.compute_sharpe(perf["cagr_3y"], perf["volatility"])
            perf["max_drawdown"]  = self.compute_max_drawdown(df)

            # Persist raw NAV history so future recomputations don't need a re-fetch
            if save_nav:
                try:
                    from holdings_db import upsert_nav_daily
                    upsert_nav_daily(scheme_code, df[["date", "nav"]])
                except Exception as e:
                    log.warning(f"⚠️  nav_daily write failed for {scheme_code}: {e}")

            return perf

        except requests.exceptions.RequestException as e:
            log.warning(f"⚠️  mfapi.in request failed for {scheme_code}: {e}")
            return {}
        except Exception as e:
            log.error(f"❌ Unexpected error for {scheme_code}: {e}")
            return {}


def batch_process(
    df: pd.DataFrame,
    limit: Optional[int] = None,
    delay: float = DELAY_BETWEEN_CALLS,
    write_to_db: bool = True,
    er_registry: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    """
    Process a DataFrame of funds through LiveNAVProcessor.
    Writes enriched results to fund_performance DB.

    Args:
        df: DataFrame with at minimum columns [scheme_code, scheme_name]
        limit: max number of funds to process (None = all)
        delay: seconds between API calls (default 0.15s)
        write_to_db: whether to upsert results into fund_performance table
        er_registry: optional dict {scheme_code: expense_ratio} for enriching ER

    Returns:
        DataFrame with added performance columns
    """
    processor = LiveNAVProcessor()
    codes = df["scheme_code"].astype(str).tolist()
    if limit:
        codes = codes[:limit]

    results = []
    total   = len(codes)
    skipped = 0

    log.info(f"🚀 Processing {total} funds via mfapi.in (delay={delay}s)...")

    for i, code in enumerate(codes):
        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{total} | skipped={skipped}")

        perf = processor.get_performance(code)

        if not perf:
            skipped += 1
            results.append({"scheme_code": code})
            time.sleep(delay)
            continue

        # Add expense ratio from registry if available
        er = (er_registry or {}).get(code)
        if er is not None:
            perf["expense_ratio"] = er
            perf["er_source"]     = "Factsheet Registry"

        # Compute composite score
        c1  = perf.get("cagr_1y")  or 0
        c3  = perf.get("cagr_3y")  or 0
        c5  = perf.get("cagr_5y")  or 0
        sh  = perf.get("sharpe_ratio") or 0
        vol = perf.get("volatility")   or 0

        if c3 > 0:
            perf["composite_score"] = round(
                c3 * 0.4 + c5 * 0.3 + sh * 5 - vol * 0.1, 2
            )
        else:
            perf["composite_score"] = None

        results.append(perf)
        time.sleep(delay)

    result_df = pd.DataFrame(results)

    if write_to_db and not result_df.empty:
        from holdings_db import upsert_performance
        # Write any fund that has at least cagr_1y — newer funds may not have 3y/5y yet
        has_any_cagr = (
            result_df["cagr_1y"].notna() if "cagr_1y" in result_df.columns
            else pd.Series(False, index=result_df.index)
        )
        valid = result_df[has_any_cagr]
        if not valid.empty:
            now = datetime.now().isoformat()
            records = valid.to_dict(orient="records")
            for r in records:
                r.setdefault("computed_at", now)
                r.setdefault("aum_cr", None)
                r.setdefault("aum_source", None)
                r.setdefault("expense_ratio", None)
                r.setdefault("er_source", r.get("er_source", "mfapi.in"))
                r.setdefault("cagr_source", "mfapi.in")
                r.setdefault("nav_data_points", r.get("nav_data_points"))
            upsert_performance(records)
            log.info(f"💾 Upserted {len(records)} performance records to DB")

    log.info(f"✅ Done. Processed={len(codes)}, skipped={skipped}, success={len(codes)-skipped}")
    return result_df


def sync_missing_cagr(limit: Optional[int] = None) -> int:
    """
    Sync CAGR for equity Direct Growth funds that have no cagr_1y yet.

    Sources the fund universe from load_fund_data() (AMFI equity direct-growth
    universe) rather than raw fund_metadata, so only the correct 1,570 funds
    are processed. Prioritises Large/Mid/Small Cap first.

    Args:
        limit: max funds to process in this run (None = all missing)

    Returns:
        count of funds submitted for processing
    """
    from data_fetcher import load_fund_data

    # Load the canonical equity universe
    all_funds = load_fund_data()

    # Keep only funds without a real cagr_1y
    missing = all_funds[all_funds["cagr_1y"].isna()].copy()

    if missing.empty:
        log.info("✅ All equity funds have CAGR data — nothing to sync")
        return 0

    # Prioritise core categories first
    priority = {"Large Cap": 1, "Mid Cap": 2, "Small Cap": 3,
                "Flexi Cap": 4, "ELSS": 5, "Multi Cap": 6}
    missing["_pri"] = missing["category"].map(priority).fillna(99)
    missing = missing.sort_values("_pri").drop(columns=["_pri"]).reset_index(drop=True)

    if limit:
        missing = missing.head(limit)

    log.info(f"📡 Syncing CAGR for {len(missing)} funds via mfapi.in "
             f"(prioritised by category)...")
    batch_process(missing, write_to_db=True)
    return len(missing)


if __name__ == "__main__":
    # Quick test with Motilal Oswal Midcap
    logging.basicConfig(level=logging.INFO)
    proc = LiveNAVProcessor()
    res = proc.get_performance("127042")
    print("\n📊 Real Performance:")
    for k, v in res.items():
        print(f"  {k}: {v}")
