import os
import asyncio
import pandas as pd
from multy_scrap import scrap_webstore_multy
import logging
import datetime as dt
import pytz

# ──────────────────────────────────────────────────────────────
MASTER_LINKS_PATH = "master_links.csv"
DAILY_CSV_PATH = "daily_prices.csv"
LOG_FILE = "daily_scraper.log"
NY_TZ = pytz.timezone("America/New_York")

# ──────────────────────────────────────────────────────────────
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def daily_scrape():
    try:
        if not os.path.exists(MASTER_LINKS_PATH):
            logging.warning("No master_links.csv found. Skipping daily scrape.")
            return

        df_links = pd.read_csv(MASTER_LINKS_PATH)
        df_links.columns = df_links.columns.str.strip().str.lower()

        if not {"sku", "links"}.issubset(df_links.columns):
            logging.warning("master_links.csv must contain 'sku' and 'links' columns.")
            return

        logging.info("Starting daily scrape...")

        links_df = df_links.rename(columns={"links": "url"})[["url"]]
        results = await asyncio.to_thread(scrap_webstore_multy, links_df)

        results.insert(0, "sku", df_links["sku"])
        results.to_csv(DAILY_CSV_PATH, index=False)

        logging.info("✅ Daily prices file successfully saved.")

    except Exception as e:
        logging.error(f"❌ Daily scrape failed: {e}")

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.info("🚀 Daily scraper started.")
    asyncio.run(daily_scrape())
    logging.info("✅ Daily scraper finished.")
