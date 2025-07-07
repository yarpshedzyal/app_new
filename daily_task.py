import os
import asyncio
import pandas as pd
from multy_scrap import scrap_webstore_multy

MASTER_LINKS_PATH = "master_links.csv"
DAILY_CSV_PATH = "daily_prices.csv"

async def daily_scrape():
    if not os.path.exists(MASTER_LINKS_PATH):
        print("⚠️ No master_links.csv found. Skipping run.")
        return

    df_links = pd.read_csv(MASTER_LINKS_PATH)
    df_links.columns = df_links.columns.str.strip().str.lower()

    if not {"sku", "links"}.issubset(df_links.columns):
        print("⚠️ master_links.csv must contain 'sku' and 'links' columns.")
        return

    print("🔍 Starting daily scrape...")

    links_df = df_links.rename(columns={"links": "url"})[["url"]]
    results = await asyncio.to_thread(scrap_webstore_multy, links_df)
    results.insert(0, "sku", df_links["sku"])

    results.to_csv(DAILY_CSV_PATH, index=False)
    print("✅ Daily prices file saved to daily_prices.csv")

if __name__ == "__main__":
    asyncio.run(daily_scrape())
