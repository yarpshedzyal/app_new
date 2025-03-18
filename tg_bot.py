import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from multy_scrap import scrap_webstore_multy
import asyncio

# Function to read the bot token from a file
def read_token_from_file(filename):
    with open(filename, 'r') as file:
        return file.readline().strip()

# Start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Hello! Please send me a CSV file containing links.")

# Handle document uploads
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file = await update.message.document.get_file()
    await file.download_to_drive('links.csv')
    await update.message.reply_text("File received! Processing...")

    # Read CSV into a Pandas DataFrame
    df = pd.read_csv('links.csv')

    # Check if 'links' column exists
    if 'links' not in df.columns:
        await update.message.reply_text("The CSV file must contain a column named 'links'.")
        return

    df.columns = df.columns.str.strip()  # Remove spaces before/after column names
    df.columns = df.columns.str.lower()  # Convert to lowercase for consistency
    print("Columns after cleaning:", df.columns.tolist())

    links = df['links'].tolist()

    # Scrape data using scrap_webstore_multy
    df = df.rename(columns={'links': 'url'})
    results = scrap_webstore_multy(df[['url']])   

    results = results.rename(columns={'url': 'links'})

    if 'sku' in df.columns:
        results.insert(0, 'sku', df['sku']) 

    # Convert results into DataFrame and save to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv('results.csv', index=False)

    # Send the results file back to the user
    await update.message.reply_document(document=open('results.csv', 'rb'))
    await update.message.reply_text("Processing complete! Here is your result.")

# Main function to run the bot
def main():
    token = read_token_from_file('config.txt')
    application = ApplicationBuilder().token(token).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), handle_document))

    # Run bot
    application.run_polling()

# Run the bot
if __name__ == "__main__":
    main()
