"""
scheduler.py
============
Runs the AMFI scraper automatically on the 11th of each month.
AMFI deadline for portfolio disclosure is the 10th — we scrape on the 11th.

Run this as a background process:
  python scheduler.py &               # Linux/macOS background
  nohup python scheduler.py &         # survives terminal close
  python scheduler.py --run-now       # immediate run for testing

For production: use cron instead:
  # crontab -e
  0 8 11 * * cd /path/to/project && python scheduler.py --run-now >> logs/scrape.log 2>&1
"""

import schedule
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/scheduler_{datetime.now().strftime('%Y%m')}.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


def monthly_scrape():
    """Run full tier-1 scrape + tier-2 on the 11th."""
    log.info("=" * 60)
    log.info("Monthly AMFI scrape starting")

    try:
        from amfi_scraper import run_scrape
        # Tier 1: top 10 AMCs (highest priority)
        results = run_scrape(tiers=[1])
        log.info(f"Tier 1 done: {results}")
        time.sleep(30)

        # Tier 2: mid-size AMCs
        results2 = run_scrape(tiers=[2])
        log.info(f"Tier 2 done: {results2}")

    except Exception as e:
        log.error(f"Scrape failed: {e}")
        import traceback
        log.error(traceback.format_exc())

    log.info("Monthly scrape complete")


def check_and_run():
    """Run on 11th of current month."""
    if datetime.now().day == 11:
        log.info("It's the 11th — running monthly scrape")
        monthly_scrape()
    else:
        log.info(f"Day {datetime.now().day} — waiting for the 11th")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true", help="Run immediately")
    parser.add_argument("--tier", type=int, nargs="+", default=[1], help="Tiers to scrape")
    args = parser.parse_args()

    if args.run_now:
        log.info("Manual run triggered")
        from amfi_scraper import run_scrape
        run_scrape(tiers=args.tier)
    else:
        log.info("Scheduler started — will scrape on 11th of each month at 8am")
        schedule.every().day.at("08:00").do(check_and_run)
        while True:
            schedule.run_pending()
            time.sleep(3600)