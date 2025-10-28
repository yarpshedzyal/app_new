# file: bot.py
import os
import sys
import io
import datetime as dt
import pandas as pd
import pytz
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from multy_scrap import scrap_webstore_multy

# ──────────────────────────────────────────────────────────────
GET_PRICES, UPDATE, START = "GET_PRICES", "UPDATE", "START"
DAILY_CSV_PATH = "daily_prices.csv"
MASTER_LINKS_PATH = "master_links.csv"
NY_TZ = pytz.timezone("America/New_York")
TOKEN_FILE = "config.txt"
DAILY_TASK_PATH = "daily_task.py"

# ──────────────────────────────────────────────────────────────
def read_token(fname: str) -> str:
    with open(fname) as f:
        return f.readline().strip()

# ──────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = [[
        InlineKeyboardButton("📈 Get actual prices", callback_data=GET_PRICES),
        InlineKeyboardButton("🔄 Update", callback_data=UPDATE),
        InlineKeyboardButton("▶️ Start daily task", callback_data=START),
    ]]
    # Why: show main menu quickly after /start.
    await update.message.reply_text(
        "Hello! Choose an action:",
        reply_markup=InlineKeyboardMarkup(kb),
    )

# ──────────────────────────────────────────────────────────────
async def launch_daily_task(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run daily_task.py as a child process and notify the user when it ends."""
    try:
        if not os.path.exists(DAILY_TASK_PATH):
            await context.application.bot.send_message(
                chat_id=chat_id,
                text=f"❌ daily_task.py not found at: {DAILY_TASK_PATH}"
            )
            return

        # Why: use the same Python interpreter/environment as the bot.
        proc = await asyncio.create_subprocess_exec(
            sys.executable, DAILY_TASK_PATH,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            await context.application.bot.send_message(chat_id=chat_id, text="✅ Daily task finished successfully.")
            # Optional: attach logs if any output was produced.
            if stdout:
                await context.application.bot.send_document(
                    chat_id=chat_id,
                    document=io.BytesIO(stdout),
                    filename="daily_task_stdout.txt"
                )
        else:
            await context.application.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Daily task failed (exit code {proc.returncode})."
            )
            if stderr:
                await context.application.bot.send_document(
                    chat_id=chat_id,
                    document=io.BytesIO(stderr),
                    filename="daily_task_stderr.txt"
                )
    except Exception as exc:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error while running daily task: {exc}"
        )
    finally:
        # Why: ensure flag is cleared even if errors happen.
        context.chat_data["daily_running"] = False

# ──────────────────────────────────────────────────────────────
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()

    if q.data == GET_PRICES:
        if not os.path.exists(DAILY_CSV_PATH):
            await q.message.reply_text("Price file isn't ready yet — please try later.")
            return

        mtime = dt.datetime.fromtimestamp(os.path.getmtime(DAILY_CSV_PATH), NY_TZ)
        await q.message.reply_document(open(DAILY_CSV_PATH, "rb"))
        await q.message.reply_text(f"Last generated at: {mtime:%Y-%m-%d %H:%M} NY time")

    elif q.data == UPDATE:
        await q.message.reply_text("Great! Please send me a CSV file containing links.")

    elif q.data == START:
        # Prevent concurrent runs per chat.
        if context.chat_data.get("daily_running"):
            await q.message.reply_text("⏳ Daily task is already running. I'll notify you when it's done.")
            return

        context.chat_data["daily_running"] = True
        await q.message.reply_text("🚀 Starting daily task… I'll message you when it completes.")
        # Run without blocking the bot.
        context.application.create_task(launch_daily_task(q.message.chat_id, context))

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
