# Quarantine Bot — Setup Guide

## A. Create the Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` and follow the prompts to name your bot
3. Copy the **bot token** you receive — you will need it for the `.env` file
4. (Optional) Set the bot's command menu via `/setcommands`:
   ```
   add - Log a quarantined product
   check - View quarantined items for a shop
   cancel - Cancel current operation
   ```

## B. Get a Google AI Studio API Key (for Gemma 4)

1. Go to [https://aistudio.google.com/apikey](https://aistudio.google.com/apikey)
2. Click **Create API Key**
3. Copy the key — this works for Gemma 4 31B IT (`gemma-4-31b-it`) via the Gemini API infrastructure
4. No GCP billing account required for the free tier

## C. Set Up Google Sheets API Access

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (or use an existing one)
3. Enable the **Google Sheets API** (APIs & Services → Library → search "Google Sheets API" → Enable)
4. Go to **IAM & Admin → Service Accounts** → Click **Create Service Account**
5. Give it a name (e.g., `quarantine-bot`), click through to finish
6. On the service account page, go to the **Keys** tab → **Add Key** → **Create new key** → **JSON**
7. Download the JSON key file and save it (e.g., as `service_account.json` in the project directory)
8. Note the service account email (e.g., `quarantine-bot@your-project.iam.gserviceaccount.com`) — you will need it in the next step

## D. Prepare the Google Sheet

1. Create a new Google Sheet at [sheets.google.com](https://sheets.google.com)
2. Rename the first tab (bottom of the page) to exactly: `Quarantine_Log`
3. In **Row 1**, add these exact headers (one per cell, A through H):

   | A | B | C | D | E | F | G | H |
   |---|---|---|---|---|---|---|---|
   | `Timestamp` | `Shop_Identifier` | `Product_Name` | `Batch_Number` | `Expiry_Date` | `Quantity` | `Inspector_Username` | `Status` |

4. Click **Share** → paste the service account email from step C.8 → grant **Editor** access
5. **You (the admin) must remain as Owner.** The service account is an additional editor — it does not replace your access.
6. Copy the **Sheet ID** from the URL — it is the long string between `/d/` and `/edit`:
   ```
   https://docs.google.com/spreadsheets/d/{THIS_PART}/edit
   ```

## E. Configure & Run

```bash
# 1. Copy the example env file
cp .env.example .env

# 2. Open .env and fill in all four values:
#    TELEGRAM_BOT_TOKEN=...
#    GEMINI_API_KEY=...
#    GOOGLE_CREDS_PATH=./service_account.json
#    GOOGLE_SHEET_ID=...

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the bot
python bot.py
```

The bot will validate all environment variables and the Google Sheet connection on startup. If anything is misconfigured, it will print a clear error message and exit.

## Notes

- The bot **only appends new rows** and **reads existing rows**. It never modifies, deletes, or reformats existing data.
- Column H (`Status`) is set to `"Quarantined"` on insert. After that, only a human admin should change it (e.g., to `"Released"`, `"Destroyed"`, `"Resolved"`).
- Admins can freely add columns, apply formatting, filters, or conditional formatting — the bot references columns by header name, not position.
