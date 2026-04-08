"""Telegram Quarantine Bot for Ministry of Health pharmacy inspectors in Kuwait.

Inspectors photograph health supplement labels, extract batch/expiry data via
Gemma 4 31B Vision, record quarantine actions to a shared Google Sheet, and
retrieve those records during follow-up inspections.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import json
import logging
import io
import os
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import fitz  # pymupdf — for PDF page rendering
import gspread
from google.auth.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google import genai
from google.genai import types
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment variable loading & validation
# ---------------------------------------------------------------------------
load_dotenv()

REQUIRED_ENV_VARS: list[str] = [
    "TELEGRAM_BOT_TOKEN",
    "GEMINI_API_KEY",
    "GOOGLE_CREDS_PATH",
    "GOOGLE_SHEET_ID",
]


def validate_env_vars() -> dict[str, str]:
    """Load and validate all required environment variables.

    GOOGLE_CREDS_PATH is optional if GOOGLE_CREDS_JSON is set (for cloud deployment).
    Returns a dict mapping variable names to their values.
    Calls sys.exit(1) if any are missing.
    """
    has_creds_json = bool(os.getenv("GOOGLE_CREDS_JSON"))
    missing = []
    for v in REQUIRED_ENV_VARS:
        if v == "GOOGLE_CREDS_PATH" and has_creds_json:
            continue
        if not os.getenv(v):
            missing.append(v)
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    return {v: os.getenv(v, "") for v in REQUIRED_ENV_VARS}


# ---------------------------------------------------------------------------
# Conversation states
# ---------------------------------------------------------------------------
WAITING_PRODUCT_NAME: int = 0
WAITING_PHOTO: int = 1
CONFIRMING: int = 2
MANUAL_INPUT: int = 3
WAITING_QUANTITY: int = 4
WAITING_STATUS: int = 5
WAITING_NEXT_OR_DONE: int = 6

# Bulk flow states
BULK_WAITING_PHOTO: int = 10
BULK_REVIEW: int = 11
BULK_EDITING: int = 12

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KUWAIT_TZ = ZoneInfo("Asia/Kuwait")
REQUIRED_HEADERS: list[str] = [
    "Timestamp",
    "Shop_Identifier",
    "Product_Name",
    "Batch_Number",
    "Expiry_Date",
    "Quantity",
    "Inspector_Username",
    "Status",
]
MAX_QTY_RETRIES: int = 3
MAX_CHECK_RESULTS: int = 15
VISION_MODEL: str = "gemma-4-31b-it"

VISION_PROMPT: str = (
    "You are a strict data-extraction tool for pharmaceutical product labels. "
    "From the provided image, extract ONLY the following fields. Return your "
    "answer as valid JSON with exactly these keys: batch_number, expiry_date. "
    "If you can also identify a product name, include a product_name key. If "
    "any field is unreadable, set its value to null. Do not guess, hallucinate, "
    "or infer — only return what is clearly legible. If the image is too blurry "
    'or does not contain a product label, return: {"error": "unreadable"}. '
    "Return ONLY the JSON object, no markdown fences, no explanation."
)

BULK_VISION_PROMPT: str = (
    "You are a strict data-extraction tool for pharmaceutical product inspection tables. "
    "The provided image contains a table or list of multiple products with their details. "
    "Extract ALL products from the table. For each product, extract: product_name, "
    "batch_number, expiry_date, quantity (as an integer), and notes (any additional remarks). "
    "Return your answer as a valid JSON array of objects, each with exactly these keys: "
    "product_name, batch_number, expiry_date, quantity, notes. "
    "If a field is unreadable or missing, set its value to null. For quantity, use an integer "
    "or null. Do not guess, hallucinate, or infer — only return what is clearly legible. "
    'If the image is too blurry or has no table, return: {"error": "unreadable"}. '
    "Return ONLY the JSON, no markdown fences, no explanation."
)

# ---------------------------------------------------------------------------
# Google Sheets helper functions
# ---------------------------------------------------------------------------


def get_sheet(creds_path: str, sheet_id: str) -> gspread.Worksheet:
    """Open the Google Sheet and return the Quarantine_Log worksheet.

    Supports either a file path (GOOGLE_CREDS_PATH) or inline JSON string
    (GOOGLE_CREDS_JSON env var) for cloud deployment.
    Exits the process if the sheet or tab cannot be found.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = os.getenv("GOOGLE_CREDS_JSON", "")
    # Also check if GOOGLE_CREDS_PATH contains inline JSON (starts with '{')
    if not creds_json and creds_path and creds_path.strip().startswith("{"):
        creds_json = creds_path
    if creds_json:
        info = json.loads(creds_json)
        credentials = ServiceAccountCredentials.from_service_account_info(
            info, scopes=scopes
        )
    else:
        credentials = ServiceAccountCredentials.from_service_account_file(
            creds_path, scopes=scopes
        )
    gc = gspread.authorize(credentials)
    try:
        spreadsheet = gc.open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error("FATAL: Spreadsheet with ID '%s' not found. Exiting.", sheet_id)
        sys.exit(1)
    try:
        worksheet = spreadsheet.worksheet("Quarantine_Log")
    except gspread.exceptions.WorksheetNotFound:
        logger.error(
            "FATAL: Sheet tab 'Quarantine_Log' not found or required headers missing. Exiting."
        )
        sys.exit(1)
    return worksheet


def validate_sheet_headers(worksheet: gspread.Worksheet) -> bool:
    """Check that all required headers exist in the first row.

    Returns True if valid. Calls sys.exit(1) if headers are missing.
    """
    first_row = worksheet.row_values(1)
    missing = [h for h in REQUIRED_HEADERS if h not in first_row]
    if missing:
        logger.error(
            "FATAL: Sheet tab 'Quarantine_Log' not found or required headers missing. "
            "Missing headers: %s. Exiting.",
            ", ".join(missing),
        )
        sys.exit(1)
    return True


def append_quarantine_entry(
    worksheet: gspread.Worksheet,
    shop_id: str,
    product_name: str,
    batch_number: str,
    expiry_date: str,
    quantity: int,
    inspector_username: str,
    status: str = "Quarantined",
) -> None:
    """Append a single quarantine entry row to the sheet."""
    timestamp = datetime.now(KUWAIT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    row = [
        timestamp,
        shop_id,
        product_name,
        batch_number,
        expiry_date,
        quantity,
        inspector_username,
        status,
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")
    logger.info(
        "Sheet write: shop=%s batch=%s expiry=%s qty=%d inspector=%s",
        shop_id,
        batch_number,
        expiry_date,
        quantity,
        inspector_username,
    )


def query_active_items(worksheet: gspread.Worksheet, shop_id: str) -> list[dict]:
    """Return all rows where Shop_Identifier matches and Status is Quarantined or Confiscated.

    Matching is case-insensitive and stripped of surrounding whitespace.
    """
    all_records = worksheet.get_all_records()
    target = shop_id.strip().lower()
    active_statuses = {"Quarantined", "Confiscated"}
    return [
        r
        for r in all_records
        if str(r.get("Shop_Identifier", "")).strip().lower() == target
        and str(r.get("Status", "")).strip() in active_statuses
    ]


# ---------------------------------------------------------------------------
# PDF helper
# ---------------------------------------------------------------------------


def pdf_to_images(pdf_bytes: bytes) -> list[bytes]:
    """Convert each page of a PDF to a JPEG image in memory.

    Returns a list of JPEG byte strings, one per page.
    """
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("jpeg")
        images.append(img_bytes)
    doc.close()
    return images


async def download_file_bytes(message) -> tuple[bytes, str]:
    """Download file bytes from a Telegram message (photo or document).

    Returns (file_bytes, mime_type). Supports photos, PDFs, and image documents.
    """
    if message.photo:
        photo = message.photo[-1]
        file = await photo.get_file()
        data = await file.download_as_bytearray()
        return bytes(data), "image/jpeg"

    if message.document:
        doc = message.document
        mime = doc.mime_type or ""
        file = await doc.get_file()
        data = await file.download_as_bytearray()
        return bytes(data), mime

    return b"", ""


# ---------------------------------------------------------------------------
# Gemma 4 Vision helper functions
# ---------------------------------------------------------------------------


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences from a string if present."""
    stripped = text.strip()
    stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
    stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def extract_label_data(api_key: str, image_bytes: bytes) -> dict:
    """Send image bytes to Gemma 4 31B Vision and return extracted label data.

    Returns a dict with keys: batch_number, expiry_date, product_name (optional).
    On error returns {"error": "..."} with a description.
    """
    client = genai.Client(api_key=api_key)
    image_part = types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")

    start_ms = time.time() * 1000
    try:
        response = client.models.generate_content(
            model=VISION_MODEL,
            contents=[image_part, VISION_PROMPT],
        )
    except Exception as exc:
        elapsed = time.time() * 1000 - start_ms
        logger.error(
            "Gemma 4 Vision API error after %.0fms: %s", elapsed, exc, exc_info=True
        )
        return {"error": "api_failure"}

    elapsed = time.time() * 1000 - start_ms
    logger.info("Gemma 4 Vision API call completed in %.0fms", elapsed)

    raw_text = response.text if response.text else ""
    cleaned = _strip_markdown_fences(raw_text)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Failed to parse Gemma 4 response as JSON: %s", raw_text[:500])
        return {"error": "unreadable"}

    return data


def extract_bulk_data(api_key: str, image_bytes: bytes) -> list[dict] | dict:
    """Send image bytes to Gemma 4 31B Vision and return a list of extracted products.

    Returns a list of dicts on success, or a dict with {"error": "..."} on failure.
    """
    client = genai.Client(api_key=api_key)
    image_part = types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")

    start_ms = time.time() * 1000
    try:
        response = client.models.generate_content(
            model=VISION_MODEL,
            contents=[image_part, BULK_VISION_PROMPT],
        )
    except Exception as exc:
        elapsed = time.time() * 1000 - start_ms
        logger.error(
            "Gemma 4 Vision API error (bulk) after %.0fms: %s", elapsed, exc, exc_info=True
        )
        return {"error": "api_failure"}

    elapsed = time.time() * 1000 - start_ms
    logger.info("Gemma 4 Vision API call (bulk) completed in %.0fms", elapsed)

    raw_text = response.text if response.text else ""
    cleaned = _strip_markdown_fences(raw_text)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Failed to parse bulk Gemma 4 response as JSON: %s", raw_text[:500])
        return {"error": "unreadable"}

    # If the model returned a dict with error, pass through
    if isinstance(data, dict):
        return data

    # Ensure it's a list
    if not isinstance(data, list):
        return {"error": "unreadable"}

    return data


def _format_bulk_list(items: list[dict]) -> str:
    """Format a list of bulk items into a numbered display string for review."""
    lines = []
    for i, item in enumerate(items, 1):
        name = item.get("product_name") or "N/A"
        batch = item.get("batch_number") or "N/A"
        expiry = item.get("expiry_date") or "N/A"
        qty = item.get("quantity") or "N/A"
        notes = item.get("notes") or ""
        line = f"{i}. *{name}* — Batch: `{batch}` — Expiry: `{expiry}` — Qty: {qty}"
        if notes:
            line += f" — Notes: _{notes}_"
        lines.append(line)
    return "\n".join(lines)


def _format_bulk_editable(items: list[dict]) -> str:
    """Format bulk items as plain text the user can copy, edit, and send back."""
    lines = []
    for item in items:
        name = item.get("product_name") or ""
        batch = item.get("batch_number") or ""
        expiry = item.get("expiry_date") or ""
        qty = item.get("quantity") or ""
        notes = item.get("notes") or ""
        line = f"{name} | {batch} | {expiry} | {qty}"
        if notes:
            line += f" | {notes}"
        lines.append(line)
    return "\n".join(lines)


def _parse_bulk_editable(text: str) -> list[dict] | None:
    """Parse user-edited bulk text back into a list of product dicts.

    Expected format per line: Product | Batch | Expiry | Qty | Notes (optional)
    Lines that are empty or can't be parsed are skipped.
    Returns None if no valid lines found.
    """
    items = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # Remove leading number+dot if present (e.g. "1. ")
        line = re.sub(r"^\d+\.\s*", "", line)
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        qty = None
        try:
            qty = int(parts[3])
        except (ValueError, IndexError):
            pass
        items.append({
            "product_name": parts[0] or None,
            "batch_number": parts[1] or None,
            "expiry_date": parts[2] or None,
            "quantity": qty,
            "notes": parts[4] if len(parts) > 4 else "",
        })
    return items if items else None


def parse_manual_input(text: str) -> dict | None:
    """Parse manual text input in the format 'Batch: XXXX, Expiry: XXXX'.

    Returns a dict with batch_number and expiry_date, or None if format doesn't match.
    """
    match = re.match(r"Batch:\s*(.+?),\s*Expiry:\s*(.+)", text, re.IGNORECASE)
    if not match:
        return None
    return {
        "batch_number": match.group(1).strip(),
        "expiry_date": match.group(2).strip(),
    }


# ---------------------------------------------------------------------------
# Telegram handler helper
# ---------------------------------------------------------------------------


def get_inspector_username(update: Update) -> str:
    """Return the inspector's @username or numeric user_id as a string."""
    user = update.effective_user
    if user and user.username:
        return f"@{user.username}"
    if user:
        return str(user.id)
    return "unknown"


# ---------------------------------------------------------------------------
# Telegram handler functions
# ---------------------------------------------------------------------------


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command — display welcome message and available commands."""
    logger.info(
        "/start invoked by user_id=%s",
        update.effective_user.id if update.effective_user else "unknown",
    )
    await update.message.reply_text(
        "👋 Welcome, Inspector.\n\n"
        "Available commands:\n"
        "• `/add <Shop>` — Log quarantined products one by one\n"
        "• `/bulk <Shop>` — Upload a table photo to log multiple products at once\n"
        "• `/check <Shop>` — View active quarantined items for a shop\n"
        "• `/done` — Finish adding products for the current shop\n"
        "• `/cancel` — Cancel the current operation",
        parse_mode="Markdown",
    )


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the /add command — begin the quarantine entry conversation flow."""
    user_id = update.effective_user.id if update.effective_user else "unknown"

    args_text = update.message.text.strip()
    # Remove the /add prefix (handles both "/add" and "/add@botname")
    prefix_match = re.match(r"^/add(?:@\S+)?\s*(.*)", args_text, re.DOTALL)
    shop_id = prefix_match.group(1).strip() if prefix_match else ""

    if not shop_id:
        logger.info("/add invoked with no shop identifier by user_id=%s", user_id)
        await update.message.reply_text(
            "Usage: `/add <Shop_Identifier>` — please include the shop's license number or name.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    logger.info("/add invoked by user_id=%s shop=%s", user_id, shop_id)
    context.user_data["shop_id"] = shop_id
    context.user_data["product_name"] = ""
    context.user_data["batch_number"] = ""
    context.user_data["expiry_date"] = ""
    context.user_data["qty_retries"] = 0

    await update.message.reply_text(
        "📝 What is the product name?"
    )
    return WAITING_PRODUCT_NAME


async def receive_product_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle text input for the product name."""
    text = update.message.text.strip() if update.message.text else ""
    if not text:
        await update.message.reply_text("📝 Please type the product name:")
        return WAITING_PRODUCT_NAME

    context.user_data["product_name"] = text
    logger.info("Product name received: %s", text)

    await update.message.reply_text(
        "📸 Please send a clear photo of the product label showing the Batch Number and Expiry Date."
    )
    return WAITING_PHOTO


async def receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle a photo or document message during the WAITING_PHOTO state."""
    if not update.message.photo and not update.message.document:
        await update.message.reply_text(
            "📸 Please send a photo or PDF, not text. Or type `/cancel` to abort.",
            parse_mode="Markdown",
        )
        return WAITING_PHOTO

    file_bytes, mime_type = await download_file_bytes(update.message)

    # If PDF, use the first page as the image
    if mime_type == "application/pdf":
        pages = pdf_to_images(file_bytes)
        if not pages:
            await update.message.reply_text("⚠️ Could not read the PDF. Please try a photo instead.")
            return WAITING_PHOTO
        file_bytes = pages[0]

    logger.info(
        "File received from user_id=%s, size=%d bytes, mime=%s",
        update.effective_user.id if update.effective_user else "unknown",
        len(file_bytes),
        mime_type,
    )

    # Call Gemma 4 Vision
    api_key = os.getenv("GEMINI_API_KEY", "")
    data = extract_label_data(api_key, file_bytes)

    # Check for errors
    if data.get("error"):
        await update.message.reply_text(
            "⚠️ I couldn't read that clearly. Please send a sharper photo, "
            "or type the details manually in this format:\n"
            "`Batch: XXXX, Expiry: XXXX`",
            parse_mode="Markdown",
        )
        return MANUAL_INPUT

    batch_number = data.get("batch_number")
    expiry_date = data.get("expiry_date")

    # Handle partially missing fields
    if batch_number is None and expiry_date is None:
        await update.message.reply_text(
            "⚠️ I couldn't read the Batch Number or Expiry Date. "
            "Please type them manually:\n"
            "`Batch: XXXX, Expiry: XXXX`",
            parse_mode="Markdown",
        )
        return MANUAL_INPUT

    if batch_number is None:
        context.user_data["expiry_date"] = str(expiry_date)
        await update.message.reply_text(
            "⚠️ I couldn't read the *Batch Number*. "
            f"I detected Expiry: `{expiry_date}`.\n"
            "Please type the Batch Number:",
            parse_mode="Markdown",
        )
        context.user_data["_missing_field"] = "batch"
        return MANUAL_INPUT

    if expiry_date is None:
        context.user_data["batch_number"] = str(batch_number)
        await update.message.reply_text(
            "⚠️ I couldn't read the *Expiry Date*. "
            f"I detected Batch: `{batch_number}`.\n"
            "Please type the Expiry Date:",
            parse_mode="Markdown",
        )
        context.user_data["_missing_field"] = "expiry"
        return MANUAL_INPUT

    # Both fields present — show confirmation
    context.user_data["batch_number"] = str(batch_number)
    context.user_data["expiry_date"] = str(expiry_date)

    product_name = context.user_data.get("product_name", "") or "N/A"
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Yes, continue", callback_data="confirm_yes"),
                InlineKeyboardButton("✏️ No, let me type it", callback_data="confirm_no"),
            ]
        ]
    )
    await update.message.reply_text(
        f"I detected:\n"
        f"• Product: `{product_name}`\n"
        f"• Batch: `{batch_number}`\n"
        f"• Expiry: `{expiry_date}`\n\n"
        f"Is this correct?",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )
    return CONFIRMING


async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the inline keyboard callback for confirming extracted data."""
    query = update.callback_query
    await query.answer()

    if query.data == "confirm_yes":
        context.user_data["qty_retries"] = 0
        await query.edit_message_text(
            "How many units are being quarantined? (Enter a whole number ≥ 1)"
        )
        return WAITING_QUANTITY

    # confirm_no — ask for manual input
    await query.edit_message_text(
        "Please type the correct details in this format:\n"
        "`Batch: XXXX, Expiry: XXXX`",
        parse_mode="Markdown",
    )
    context.user_data["_missing_field"] = ""
    return MANUAL_INPUT


async def handle_manual_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle manual text input or a re-sent photo/PDF during MANUAL_INPUT state."""
    # If the user sends a photo or document, try vision again
    if update.message.photo or update.message.document:
        return await receive_photo(update, context)

    text = update.message.text.strip() if update.message.text else ""
    missing_field = context.user_data.get("_missing_field", "")

    # Single missing field mode
    if missing_field == "batch":
        if not text:
            await update.message.reply_text("Please type the Batch Number:")
            return MANUAL_INPUT
        context.user_data["batch_number"] = text
        context.user_data["_missing_field"] = ""
        # Now show confirmation
        return await _show_confirmation(update, context)

    if missing_field == "expiry":
        if not text:
            await update.message.reply_text("Please type the Expiry Date:")
            return MANUAL_INPUT
        context.user_data["expiry_date"] = text
        context.user_data["_missing_field"] = ""
        return await _show_confirmation(update, context)

    # Full manual input mode — expect "Batch: XXXX, Expiry: XXXX"
    parsed = parse_manual_input(text)
    if parsed is None:
        await update.message.reply_text(
            "⚠️ I couldn't parse that. Please use this format:\n"
            "`Batch: XXXX, Expiry: XXXX`\n\n"
            "Or send a new photo.",
            parse_mode="Markdown",
        )
        return MANUAL_INPUT

    context.user_data["batch_number"] = parsed["batch_number"]
    context.user_data["expiry_date"] = parsed["expiry_date"]
    return await _show_confirmation(update, context)


async def _show_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Display the confirmation keyboard with the current extracted/entered data."""
    batch = context.user_data.get("batch_number", "")
    expiry = context.user_data.get("expiry_date", "")
    product = context.user_data.get("product_name", "") or "N/A"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Yes, continue", callback_data="confirm_yes"),
                InlineKeyboardButton("✏️ No, let me type it", callback_data="confirm_no"),
            ]
        ]
    )
    await update.message.reply_text(
        f"I detected:\n"
        f"• Product: `{product}`\n"
        f"• Batch: `{batch}`\n"
        f"• Expiry: `{expiry}`\n\n"
        f"Is this correct?",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )
    return CONFIRMING


async def receive_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the quantity input during the WAITING_QUANTITY state."""
    text = update.message.text.strip() if update.message.text else ""

    try:
        quantity = int(text)
        if quantity < 1:
            raise ValueError("Quantity must be >= 1")
    except (ValueError, TypeError):
        retries = context.user_data.get("qty_retries", 0) + 1
        context.user_data["qty_retries"] = retries

        if retries >= MAX_QTY_RETRIES:
            await update.message.reply_text(
                "❌ Too many invalid attempts. Operation cancelled. Start again with `/add`.",
                parse_mode="Markdown",
            )
            return ConversationHandler.END

        await update.message.reply_text(
            "❌ Please enter a valid whole number (e.g., 12)."
        )
        return WAITING_QUANTITY

    context.user_data["quantity"] = quantity

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔒 Quarantined", callback_data="status_Quarantined"),
                InlineKeyboardButton("🚫 Confiscated", callback_data="status_Confiscated"),
            ]
        ]
    )
    await update.message.reply_text(
        "What is the action taken?",
        reply_markup=keyboard,
    )
    return WAITING_STATUS


async def receive_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the status selection callback."""
    query = update.callback_query
    await query.answer()

    status = query.data.replace("status_", "")
    context.user_data["status"] = status

    # Write to Google Sheet
    shop_id = context.user_data.get("shop_id", "")
    product_name = context.user_data.get("product_name", "")
    batch_number = context.user_data.get("batch_number", "")
    expiry_date = context.user_data.get("expiry_date", "")
    quantity = context.user_data.get("quantity", 1)
    user = query.from_user
    if user and user.username:
        inspector = f"@{user.username}"
    elif user:
        inspector = str(user.id)
    else:
        inspector = "unknown"
    timestamp = datetime.now(KUWAIT_TZ).strftime("%Y-%m-%d %H:%M:%S")

    worksheet = get_fresh_worksheet()
    try:
        append_quarantine_entry(
            worksheet=worksheet,
            shop_id=shop_id,
            product_name=product_name,
            batch_number=batch_number,
            expiry_date=expiry_date,
            quantity=quantity,
            inspector_username=inspector,
            status=status,
        )
    except gspread.exceptions.APIError as exc:
        logger.error("Google Sheets API error: %s", exc, exc_info=True)
        await query.edit_message_text(
            "⚠️ Could not reach the database. Please try again in a moment."
        )
        return ConversationHandler.END
    except Exception as exc:
        logger.error("Unexpected error writing to sheet: %s", exc, exc_info=True)
        await query.edit_message_text(
            "⚠️ Could not reach the database. Please try again in a moment."
        )
        return ConversationHandler.END

    await query.edit_message_text(
        f"✅ Entry logged:\n"
        f"• Shop: `{shop_id}`\n"
        f"• Product: `{product_name}`\n"
        f"• Batch: `{batch_number}` — Expiry: `{expiry_date}`\n"
        f"• Qty: `{quantity}`\n"
        f"• Status: `{status}`\n"
        f"• Inspector: `{inspector}`\n"
        f"• Time: `{timestamp}`\n\n"
        f"📝 Send the next product name to add another, or type /done to finish.",
        parse_mode="Markdown",
    )
    # Reset per-product fields, keep shop_id
    context.user_data["product_name"] = ""
    context.user_data["batch_number"] = ""
    context.user_data["expiry_date"] = ""
    context.user_data["qty_retries"] = 0
    return WAITING_NEXT_OR_DONE


async def handle_next_or_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle input after a product is logged — either a new product name or /done."""
    text = update.message.text.strip() if update.message.text else ""

    if not text:
        await update.message.reply_text(
            "📝 Send the next product name, or type /done to finish."
        )
        return WAITING_NEXT_OR_DONE

    # Treat as the next product name
    context.user_data["product_name"] = text
    logger.info("Next product name received: %s", text)

    await update.message.reply_text(
        "📸 Please send a clear photo of the product label showing the Batch Number and Expiry Date."
    )
    return WAITING_PHOTO


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the /done command — finish adding products for the current shop."""
    shop_id = context.user_data.get("shop_id", "")
    logger.info(
        "/done invoked by user_id=%s for shop=%s",
        update.effective_user.id if update.effective_user else "unknown",
        shop_id,
    )
    await update.message.reply_text(
        f"✅ Done adding products for `{shop_id}`.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Bulk flow handlers
# ---------------------------------------------------------------------------


async def bulk_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the /bulk command — begin bulk entry from a table photo."""
    user_id = update.effective_user.id if update.effective_user else "unknown"

    args_text = update.message.text.strip()
    prefix_match = re.match(r"^/bulk(?:@\S+)?\s*(.*)", args_text, re.DOTALL)
    shop_id = prefix_match.group(1).strip() if prefix_match else ""

    if not shop_id:
        logger.info("/bulk invoked with no shop identifier by user_id=%s", user_id)
        await update.message.reply_text(
            "Usage: `/bulk <Shop_Identifier>` — please include the shop's license number or name.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    logger.info("/bulk invoked by user_id=%s shop=%s", user_id, shop_id)
    context.user_data["shop_id"] = shop_id
    context.user_data["bulk_items"] = []

    await update.message.reply_text(
        "📸 Send a photo of the table/list containing the products, "
        "batch numbers, expiry dates, quantities, and any notes."
    )
    return BULK_WAITING_PHOTO


async def bulk_receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle a photo or PDF document in the BULK_WAITING_PHOTO state."""
    if not update.message.photo and not update.message.document:
        await update.message.reply_text(
            "📸 Please send a photo or PDF, not text. Or type `/cancel` to abort.",
            parse_mode="Markdown",
        )
        return BULK_WAITING_PHOTO

    file_bytes, mime_type = await download_file_bytes(update.message)

    logger.info(
        "Bulk file received from user_id=%s, size=%d bytes, mime=%s",
        update.effective_user.id if update.effective_user else "unknown",
        len(file_bytes),
        mime_type,
    )

    api_key = os.getenv("GEMINI_API_KEY", "")

    # For PDFs, extract from each page and combine results
    if mime_type == "application/pdf":
        pages = pdf_to_images(file_bytes)
        if not pages:
            await update.message.reply_text("⚠️ Could not read the PDF. Please try a photo instead.")
            return BULK_WAITING_PHOTO
        all_items = []
        for page_img in pages:
            page_data = extract_bulk_data(api_key, page_img)
            if isinstance(page_data, list):
                all_items.extend(page_data)
        data = all_items
    else:
        data = extract_bulk_data(api_key, file_bytes)

    if isinstance(data, dict) and data.get("error"):
        await update.message.reply_text(
            "⚠️ I couldn't read the table. Please send a clearer photo or PDF, or type `/cancel` to abort.",
            parse_mode="Markdown",
        )
        return BULK_WAITING_PHOTO

    if not data:
        await update.message.reply_text(
            "⚠️ No products detected. Please send a clearer photo or PDF, or type `/cancel` to abort.",
            parse_mode="Markdown",
        )
        return BULK_WAITING_PHOTO

    context.user_data["bulk_items"] = data

    formatted = _format_bulk_list(data)
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Confirm & Save All", callback_data="bulk_confirm"),
                InlineKeyboardButton("✏️ Edit", callback_data="bulk_edit"),
            ],
            [
                InlineKeyboardButton("🔄 Retake Photo", callback_data="bulk_retake"),
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_cancel"),
            ],
        ]
    )
    await update.message.reply_text(
        f"📋 I found *{len(data)} product(s)*:\n\n{formatted}\n\n"
        f"What would you like to do?",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )
    return BULK_REVIEW


async def bulk_review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle inline keyboard callbacks during bulk review."""
    query = update.callback_query
    await query.answer()

    if query.data == "bulk_confirm":
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🔒 Quarantined", callback_data="bulk_status_Quarantined"),
                    InlineKeyboardButton("🚫 Confiscated", callback_data="bulk_status_Confiscated"),
                ]
            ]
        )
        await query.edit_message_text(
            "What is the action taken for all these items?",
            reply_markup=keyboard,
        )
        return BULK_REVIEW

    if query.data.startswith("bulk_status_"):
        status = query.data.replace("bulk_status_", "")
        context.user_data["bulk_status"] = status
        return await _bulk_save_all(query, context)

    if query.data == "bulk_edit":
        items = context.user_data.get("bulk_items", [])
        editable = _format_bulk_editable(items)
        await query.edit_message_text(
            "✏️ Here's the list in editable format. "
            "Copy the text below, make your changes, and send it back.\n"
            "Delete any lines you want to remove.\n\n"
            "Format: `Product | Batch | Expiry | Qty | Notes`\n\n"
            f"{editable}",
            parse_mode="Markdown",
        )
        return BULK_EDITING

    if query.data == "bulk_retake":
        await query.edit_message_text(
            "📸 Send a new photo of the table."
        )
        return BULK_WAITING_PHOTO

    if query.data == "bulk_cancel":
        await query.edit_message_text("❌ Bulk operation cancelled.")
        return ConversationHandler.END

    return BULK_REVIEW


async def bulk_edit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the user's edited text during BULK_EDITING state.

    The user copies the editable list, modifies it, and sends it back.
    Each line: Product | Batch | Expiry | Qty | Notes (optional)
    """
    text = update.message.text.strip() if update.message.text else ""

    if not text:
        await update.message.reply_text(
            "Please send the edited list, or type /cancel to abort."
        )
        return BULK_EDITING

    parsed = _parse_bulk_editable(text)
    if parsed is None:
        await update.message.reply_text(
            "⚠️ I couldn't parse that. Please use this format (one product per line):\n"
            "`Product | Batch | Expiry | Qty | Notes`\n\n"
            "Or type /cancel to abort.",
            parse_mode="Markdown",
        )
        return BULK_EDITING

    context.user_data["bulk_items"] = parsed
    formatted = _format_bulk_list(parsed)
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Confirm & Save All", callback_data="bulk_confirm"),
                InlineKeyboardButton("✏️ Edit Again", callback_data="bulk_edit"),
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_cancel"),
            ],
        ]
    )
    await update.message.reply_text(
        f"📋 Updated list (*{len(parsed)} items*):\n\n{formatted}\n\nSave all?",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )
    return BULK_REVIEW


async def _bulk_save_all(query, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Save all bulk items to the Google Sheet."""
    items = context.user_data.get("bulk_items", [])
    shop_id = context.user_data.get("shop_id", "")
    user = query.from_user
    if user and user.username:
        inspector = f"@{user.username}"
    elif user:
        inspector = str(user.id)
    else:
        inspector = "unknown"
    worksheet = get_fresh_worksheet()
    status = context.user_data.get("bulk_status", "Quarantined")

    saved = 0
    try:
        for item in items:
            product_name = str(item.get("product_name") or "")
            batch_number = str(item.get("batch_number") or "")
            expiry_date = str(item.get("expiry_date") or "")
            quantity = item.get("quantity")
            if not isinstance(quantity, int) or quantity < 1:
                quantity = 1

            append_quarantine_entry(
                worksheet=worksheet,
                shop_id=shop_id,
                product_name=product_name,
                batch_number=batch_number,
                expiry_date=expiry_date,
                quantity=quantity,
                inspector_username=inspector,
                status=status,
            )
            saved += 1
    except gspread.exceptions.APIError as exc:
        logger.error("Google Sheets API error during bulk save: %s", exc, exc_info=True)
        await query.edit_message_text(
            f"⚠️ Database error after saving {saved}/{len(items)} items. "
            "Please try again in a moment."
        )
        return ConversationHandler.END
    except Exception as exc:
        logger.error("Unexpected error during bulk save: %s", exc, exc_info=True)
        await query.edit_message_text(
            f"⚠️ Error after saving {saved}/{len(items)} items. "
            "Please try again in a moment."
        )
        return ConversationHandler.END

    await query.edit_message_text(
        f"✅ All *{saved} product(s)* saved for shop `{shop_id}`.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /check command — look up active quarantined items for a shop."""
    user_id = update.effective_user.id if update.effective_user else "unknown"

    args_text = update.message.text.strip()
    prefix_match = re.match(r"^/check(?:@\S+)?\s*(.*)", args_text, re.DOTALL)
    shop_id = prefix_match.group(1).strip() if prefix_match else ""

    if not shop_id:
        logger.info("/check invoked with no shop identifier by user_id=%s", user_id)
        await update.message.reply_text(
            "Usage: `/check <Shop_Identifier>` — please include the shop's license number or name.",
            parse_mode="Markdown",
        )
        return

    logger.info("/check invoked by user_id=%s shop=%s", user_id, shop_id)

    worksheet = get_fresh_worksheet()
    try:
        records = query_active_items(worksheet, shop_id)
    except gspread.exceptions.APIError as exc:
        logger.error("Google Sheets API error during /check: %s", exc, exc_info=True)
        await update.message.reply_text(
            "⚠️ Could not reach the database. Please try again in a moment."
        )
        return
    except Exception as exc:
        logger.error("Unexpected error reading sheet: %s", exc, exc_info=True)
        await update.message.reply_text(
            "⚠️ Could not reach the database. Please try again in a moment."
        )
        return

    if not records:
        await update.message.reply_text(
            f"✅ No active items found for `{shop_id}`.",
            parse_mode="Markdown",
        )
        return

    total = len(records)
    display_records = records[:MAX_CHECK_RESULTS]
    lines = [f"📋 Active items for `{shop_id}`:\n"]

    for i, r in enumerate(display_records, 1):
        product = r.get("Product_Name", "N/A")
        batch = r.get("Batch_Number", "N/A")
        expiry = r.get("Expiry_Date", "N/A")
        qty = r.get("Quantity", "N/A")
        status = r.get("Status", "N/A")
        date = r.get("Timestamp", "N/A")
        inspector = r.get("Inspector_Username", "N/A")
        lines.append(
            f"{i}. *{product}* — Batch `{batch}` — Expiry `{expiry}` — Qty: {qty} "
            f"— _{status}_ (logged {date} by {inspector})"
        )

    if total > MAX_CHECK_RESULTS:
        lines.append(f"\nShowing {MAX_CHECK_RESULTS} of {total} records. Contact admin for full export.")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the /cancel command — abort the current conversation flow."""
    logger.info(
        "/cancel invoked by user_id=%s",
        update.effective_user.id if update.effective_user else "unknown",
    )
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END


async def timeout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle conversation timeout after 5 minutes of inactivity."""
    if update and update.effective_user:
        logger.info(
            "Conversation timed out for user_id=%s", update.effective_user.id
        )
    if update and update.message:
        await update.message.reply_text(
            "⏳ Session timed out. Please start again with `/add`.",
            parse_mode="Markdown",
        )
    return ConversationHandler.END


async def fallback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle unexpected messages during the conversation flow."""
    await update.message.reply_text(
        "I didn't understand that. Please follow the current step or type /cancel to start over."
    )
    # Return the current state to stay in the conversation
    return None  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def get_fresh_worksheet() -> gspread.Worksheet:
    """Get a fresh worksheet connection (avoids stale token issues)."""
    creds_path = os.getenv("GOOGLE_CREDS_PATH", "")
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    return get_sheet(creds_path, sheet_id)


async def post_init(application: Application) -> None:
    """Validate Google Sheets connection after the bot starts."""
    worksheet = get_fresh_worksheet()
    validate_sheet_headers(worksheet)
    logger.info("Google Sheet connected and headers validated.")


def main() -> None:
    """Validate configuration, connect to services, and start the bot."""
    # Validate environment variables
    env = validate_env_vars()
    logger.info("All environment variables loaded successfully.")

    # Build the Telegram Application (sheet connection deferred to post_init)
    application = (
        Application.builder()
        .token(env["TELEGRAM_BOT_TOKEN"])
        .post_init(post_init)
        .build()
    )

    # Define the ConversationHandler for /add
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            WAITING_PRODUCT_NAME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, receive_product_name
                ),
            ],
            WAITING_PHOTO: [
                MessageHandler(filters.PHOTO | filters.Document.PDF | filters.Document.IMAGE, receive_photo),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, fallback_handler
                ),
            ],
            CONFIRMING: [
                CallbackQueryHandler(handle_confirmation, pattern="^confirm_"),
            ],
            MANUAL_INPUT: [
                MessageHandler(filters.PHOTO | filters.Document.PDF | filters.Document.IMAGE, handle_manual_input),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, handle_manual_input
                ),
            ],
            WAITING_QUANTITY: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, receive_quantity
                ),
            ],
            WAITING_STATUS: [
                CallbackQueryHandler(receive_status, pattern="^status_"),
            ],
            WAITING_NEXT_OR_DONE: [
                CommandHandler("done", done_command),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, handle_next_or_done
                ),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, timeout_handler),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_command),
            CommandHandler("done", done_command),
            MessageHandler(filters.ALL, fallback_handler),
        ],
        per_user=True,
        per_chat=True,
        conversation_timeout=300,
    )

    # Define the ConversationHandler for /bulk
    bulk_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("bulk", bulk_command)],
        states={
            BULK_WAITING_PHOTO: [
                MessageHandler(filters.PHOTO | filters.Document.PDF | filters.Document.IMAGE, bulk_receive_photo),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, fallback_handler
                ),
            ],
            BULK_REVIEW: [
                CallbackQueryHandler(bulk_review_callback, pattern="^bulk_"),
            ],
            BULK_EDITING: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, bulk_edit_handler
                ),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, timeout_handler),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_command),
            MessageHandler(filters.ALL, fallback_handler),
        ],
        per_user=True,
        per_chat=True,
        conversation_timeout=300,
    )

    # Register handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(conv_handler)
    application.add_handler(bulk_conv_handler)
    application.add_handler(CommandHandler("check", check_command))

    # Determine run mode: webhook (Cloud Run) or polling (local)
    port = os.getenv("PORT")
    webhook_url = os.getenv("WEBHOOK_URL", "")

    if port:
        logger.info("Bot is starting in webhook mode on port %s...", port)
        import asyncio
        asyncio.run(_run_webhook(application, int(port), webhook_url))
    else:
        logger.info("Bot is starting polling...")
        application.run_polling(drop_pending_updates=True)


async def _run_webhook(application: Application, port: int, webhook_url: str) -> None:
    """Start a lightweight HTTP server immediately, then initialize the bot."""
    from aiohttp import web
    import asyncio

    # Queue to hold the initialized application
    app_ready = asyncio.Event()

    async def health_handler(request: web.Request) -> web.Response:
        """Health check endpoint — responds immediately."""
        return web.Response(text="OK")

    async def webhook_handler(request: web.Request) -> web.Response:
        """Process incoming Telegram updates."""
        if not app_ready.is_set():
            return web.Response(status=503, text="Starting")
        try:
            data = await request.json()
            update = Update.de_json(data, application.bot)
            await application.process_update(update)
        except Exception as exc:
            logger.error("Error processing update: %s", exc, exc_info=True)
        return web.Response(text="OK")

    # Create and start HTTP server immediately (satisfies Cloud Run health check)
    aio_app = web.Application()
    aio_app.router.add_get("/", health_handler)
    aio_app.router.add_post("/webhook", webhook_handler)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("HTTP server listening on port %d", port)

    # Now initialize the bot application (sheet connection, etc.)
    await application.initialize()
    await application.start()

    # Set webhook with Telegram
    if webhook_url:
        await application.bot.set_webhook(
            url=f"{webhook_url}/webhook",
            drop_pending_updates=True,
        )
        logger.info("Webhook set to %s/webhook", webhook_url)

    app_ready.set()
    logger.info("Bot is ready to process updates.")

    # Keep running forever
    try:
        await asyncio.Event().wait()
    finally:
        await application.stop()
        await application.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    main()
