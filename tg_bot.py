import os
import asyncio
import datetime as dt
import pandas as pd
import pytz
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from multy_scrap import scrap_webstore_multy          
from single_scrap import scrap_webstore_single        
# ──────────────────────────────────────────────────────────────
GET_PRICES, UPDATE = "GET_PRICES", "UPDATE"
DAILY_CSV_PATH     = "daily_prices.csv"
MASTER_LINKS_PATH  = "master_links.csv"
NY_TZ            = pytz.timezone("America/New_York")
RUN_AT             = dt.time(hour=8, minute=30, tzinfo=NY_TZ)   
TOKEN_FILE         = "config.txt"
# ──────────────────────────────────────────────────────────────
def read_token(fname: str) -> str:
    with open(fname) as f:
        return f.readline().strip()
# ──────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = [[
        InlineKeyboardButton("📈 Get actual prices", callback_data=GET_PRICES),
        InlineKeyboardButton("🔄 Update",            callback_data=UPDATE),
    ]]
    await update.message.reply_text(
        "Hello! Choose an action:",
        reply_markup=InlineKeyboardMarkup(kb),
    )
# ──────────────────────────────────────────────────────────────
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    if q.data == GET_PRICES:
        if not os.path.exists(DAILY_CSV_PATH):
            await q.message.reply_text("Price file isn’t ready yet — please try later.")
            return
        mtime = dt.datetime.fromtimestamp(os.path.getmtime(DAILY_CSV_PATH), NY_TZ)
        await q.message.reply_document(open(DAILY_CSV_PATH, "rb"))
        await q.message.reply_text(
            f"Last generated at: {mtime:%Y-%m-%d %H:%M} NY time"
        )
    elif q.data == UPDATE:
        await q.message.reply_text("Great! Please send me a CSV file containing links.")
# ──────────────────────────────────────────────────────────────
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    telegram_file = await update.message.document.get_file()
    await telegram_file.download_to_drive("links.csv")
    await update.message.reply_text("File received! Processing…")

    # ── read & clean ────────────────────────────────────────────────
    df = pd.read_csv("links.csv")
    df.columns = df.columns.str.strip().str.lower()  # e.g. "SKU" → "sku"

    required_cols = {"sku", "links"}
    if not required_cols.issubset(df.columns):
        await update.message.reply_text(
            "CSV must contain *both* 'SKU' and 'links' columns (case-insensitive)."
        )
        return

    # ── save master list for the daily job ──────────────────────────
    df[["sku", "links"]].to_csv(MASTER_LINKS_PATH, index=False)

    # ── scrape immediately for the user ────────────────────────────
    links_df = df.rename(columns={"links": "url"})[["url"]]
    results  = await asyncio.to_thread(scrap_webstore_multy, links_df)
    results  = results.rename(columns={"url": "links"})

    # prepend SKU so rows stay aligned with the original upload
    results.insert(0, "sku", df["sku"])

    results.to_csv("results.csv", index=False)
    await update.message.reply_document(open("results.csv", "rb"))
    await update.message.reply_text("Done! Here is your result.")

# ──────────────────────────────────────────────────────────────
async def daily_prices_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not os.path.exists(MASTER_LINKS_PATH):
        print("⚠️ No master_links.csv yet — skipping daily run.")
        return

    df_links = pd.read_csv(MASTER_LINKS_PATH)
    df_links.columns = df_links.columns.str.strip().str.lower()

    if not {"sku", "links"}.issubset(df_links.columns):
        print("⚠️ master_links.csv must contain 'sku' and 'links' columns.")
        return

    # Rename and extract the 'url' column for scraping
    links_df = df_links.rename(columns={"links": "url"})[["url"]]

    # Perform scraping
    results = await asyncio.to_thread(scrap_webstore_multy, links_df)

    # Add SKU back to the front of the results
    results.insert(0, "sku", df_links["sku"])

    # Save to CSV
    results.to_csv(DAILY_CSV_PATH, index=False)
    print("✅ Daily prices file refreshed.")

# ──────────────────────────────────────────────────────────────
def main() -> None:
    async def post_init(app):
        app.job_queue.run_daily(daily_prices_job, RUN_AT)
        print("🕗 Daily job scheduled.")

    application = (
        ApplicationBuilder()
        .token(read_token(TOKEN_FILE))
        .post_init(post_init)  # ✅ Register post-init properly here
        .build()
    )

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(buttons))
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), handle_document))

    application.run_polling()

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
