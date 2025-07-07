import os
import datetime as dt
import pandas as pd
import pytz
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from multy_scrap import scrap_webstore_multy

# ──────────────────────────────────────────────────────────────
GET_PRICES, UPDATE = "GET_PRICES", "UPDATE"
DAILY_CSV_PATH = "daily_prices.csv"
MASTER_LINKS_PATH = "master_links.csv"
NY_TZ = pytz.timezone("America/New_York")
TOKEN_FILE = "config.txt"

# ──────────────────────────────────────────────────────────────
def read_token(fname: str) -> str:
    with open(fname) as f:
        return f.readline().strip()

# ──────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = [[
        InlineKeyboardButton("📈 Get actual prices", callback_data=GET_PRICES),
        InlineKeyboardButton("🔄 Update", callback_data=UPDATE),
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
        await q.message.reply_text(f"Last generated at: {mtime:%Y-%m-%d %H:%M} NY time")

    elif q.data == UPDATE:
        await q.message.reply_text("Great! Please send me a CSV file containing links.")

# ──────────────────────────────────────────────────────────────
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    telegram_file = await update.message.document.get_file()
    await telegram_file.download_to_drive("links.csv")
    await update.message.reply_text("File received! Processing…")

    df = pd.read_csv("links.csv")
    df.columns = df.columns.str.strip().str.lower()

    required_cols = {"sku", "links"}
    if not required_cols.issubset(df.columns):
        await update.message.reply_text("CSV must contain *both* 'SKU' and 'links' columns (case-insensitive).")
        return

    df[["sku", "links"]].to_csv(MASTER_LINKS_PATH, index=False)

    links_df = df.rename(columns={"links": "url"})[["url"]]
    results = await asyncio.to_thread(scrap_webstore_multy, links_df)
    results = results.rename(columns={"url": "links"})

    results.insert(0, "sku", df["sku"])

    results.to_csv("results.csv", index=False)
    await update.message.reply_document(open("results.csv", "rb"))
    await update.message.reply_text("Done! Here is your result.")

# ──────────────────────────────────────────────────────────────
def main() -> None:
    application = (
        ApplicationBuilder()
        .token(read_token(TOKEN_FILE))
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(buttons))
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), handle_document))

    application.run_polling()

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
