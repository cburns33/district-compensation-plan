"""
migrate_columns.py — One-time migration to populate new column layout.

Reads existing data written by the old script format and splits it into
the new column structure:

  K  Best_Score             "R1:7pts +fixed"  →  "7"           (number only)
  L  Best_URL_Classification                  →  "R1"          (prefix)
  M  Search_Method                            →  "+fixed"      (modifiers)
  N  QA_Status              already correct (shifted here when cols inserted)
  O  Redirect_URL           extracts URL from "⚠ REDIRECT → https://..."

Only writes to K, L, M, O. Never touches any other column.

Usage:
    python migrate_columns.py             # live run
    python migrate_columns.py --dry-run   # preview without writing
"""

import argparse
import io
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ── Column indices (1-based) ──────────────────────────────────────────────────

COL_BEST_SCORE           = 11   # K  — currently "R1:7pts +fixed", will become "7"
COL_BEST_URL_CLASS       = 12   # L  — empty, will become "R1"
COL_SEARCH_METHOD        = 13   # M  — empty, will become "+fixed"
COL_QA_STATUS            = 14   # N  — already correct, read-only in this script
COL_REDIRECT_URL         = 15   # O  — empty, will become extracted redirect URL

# Migration only writes K, L, M, O
WRITE_COLS = {COL_BEST_SCORE, COL_BEST_URL_CLASS, COL_SEARCH_METHOD, COL_REDIRECT_URL}
WRITE_COL_MIN = COL_BEST_SCORE    # 11 (K)
WRITE_COL_MAX = COL_REDIRECT_URL  # 15 (O)

BATCH_SIZE     = 25
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]
WORKSHEET_NAME = "Unique Districts"

# ── Safety guard ──────────────────────────────────────────────────────────────

def _assert_col_in_bounds(col: int) -> None:
    if col not in WRITE_COLS:
        raise RuntimeError(
            f"SAFETY HALT: migrate_columns.py attempted write to column {col}. "
            f"Allowed: {sorted(WRITE_COLS)} (K, L, M, O only). Halting."
        )


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_best_label(label: str):
    """
    Parse old Best_Score label into (score_str, classification, search_method).

    Examples:
      "R1:7pts"              → ("7",  "R1",  "")
      "R2:6pts +fixed"       → ("6",  "R2",  "+fixed")
      "T3:5pts +domain"      → ("5",  "T3",  "+domain")
      "R?:4pts +noyear +fixed" → ("4", "R?", "+noyear +fixed")
      "R1:0pts (no clear winner)" → ("0", "R1", "")
      "7"                    → ("7",  "",    "")   # already migrated / manual entry
    """
    label = label.strip()

    # Already a plain number — partially migrated or manually entered
    if re.match(r'^\d+$', label):
        return label, "", ""

    cls_match  = re.match(r'^(R\d+|R\?|T\d+)', label)
    classification = cls_match.group(1) if cls_match else ""

    score_match = re.search(r':(\d+)pts', label)
    score_str   = score_match.group(1) if score_match else "0"

    methods = re.findall(r'\+\w+', label)
    search_method = " ".join(methods)

    return score_str, classification, search_method


def extract_redirect_url(qa_status: str) -> str:
    """Return the URL portion of '⚠ REDIRECT → https://...' or ''."""
    if "→" in qa_status:
        return qa_status.split("→", 1)[1].strip()
    return ""


# ── Google Sheets ─────────────────────────────────────────────────────────────

def open_sheet(creds_path: str, spreadsheet_id: str):
    creds  = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(WORKSHEET_NAME)


def flush_batch(sheet, buffer: list, dry_run: bool) -> None:
    if not buffer:
        return
    if dry_run:
        for item in buffer:
            print(f"  [DRY-RUN] Row {item['row']}: K={item['score']}  L={item['cls']}  "
                  f"M={item['method']}  O={item['redirect']}")
        buffer.clear()
        return

    data = []
    for item in buffer:
        row = item["row"]
        # K only
        data.append({"range": f"K{row}", "values": [[item["score"]]]})
        # L only
        data.append({"range": f"L{row}", "values": [[item["cls"]]]})
        # M only
        data.append({"range": f"M{row}", "values": [[item["method"]]]})
        # O only
        data.append({"range": f"O{row}", "values": [[item["redirect"]]]})

    sheet.batch_update(data, value_input_option="USER_ENTERED")
    buffer.clear()


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logger(dry_run: bool) -> logging.Logger:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    log_path = logs_dir / f"migrate_{ts}{suffix}.log"

    logger = logging.getLogger("migrate")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    logger.addHandler(fh)

    logger.info("Migration log: %s", log_path)
    return logger


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Migrate column layout in Google Sheet.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing to the sheet")
    parser.add_argument("--start-row", type=int, default=2)
    parser.add_argument("--end-row",   type=int, default=None)
    return parser.parse_args()


def main():
    args   = parse_args()
    logger = setup_logger(args.dry_run)

    load_dotenv()
    creds_path_raw = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    creds_path_obj = Path(creds_path_raw).expanduser()
    if not creds_path_obj.is_absolute():
        creds_path_obj = Path(__file__).parent / creds_path_obj
    creds_path     = str(creds_path_obj)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    if not creds_path or not spreadsheet_id:
        print("ERROR: GOOGLE_SHEETS_CREDS_JSON and SPREADSHEET_ID must be set in .env")
        sys.exit(1)
    if not GSPREAD_AVAILABLE:
        print("ERROR: gspread not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    print("Opening sheet...")
    sheet      = open_sheet(creds_path, spreadsheet_id)
    all_values = sheet.get_all_values()
    total_rows = len(all_values)

    start_row = args.start_row
    end_row   = min(args.end_row or total_rows, total_rows)

    def get_col(row_data, col_1based):
        idx = col_1based - 1
        return row_data[idx].strip() if idx < len(row_data) else ""

    buffer   = []
    migrated = 0
    skipped  = 0

    for sheet_row in range(start_row, end_row + 1):
        row_data = all_values[sheet_row - 1]

        raw_score  = get_col(row_data, COL_BEST_SCORE)
        existing_l = get_col(row_data, COL_BEST_URL_CLASS)
        qa_status  = get_col(row_data, COL_QA_STATUS)

        # Skip rows with no data
        if not raw_score:
            skipped += 1
            continue

        # Skip rows already migrated (L is already populated)
        if existing_l:
            skipped += 1
            continue

        score_str, classification, search_method = parse_best_label(raw_score)
        redirect_url = extract_redirect_url(qa_status)

        logger.info(
            "Row %d | raw_score=%r → score=%s cls=%s method=%s redirect=%s",
            sheet_row, raw_score, score_str, classification, search_method, redirect_url,
        )

        buffer.append({
            "row":      sheet_row,
            "score":    score_str,
            "cls":      classification,
            "method":   search_method,
            "redirect": redirect_url,
        })
        migrated += 1

        if len(buffer) >= BATCH_SIZE:
            flush_batch(sheet, buffer, args.dry_run)
            print(f"  Flushed batch at row {sheet_row}...")

    flush_batch(sheet, buffer, args.dry_run)

    print(f"\nMigration complete. Rows migrated: {migrated}  |  Skipped: {skipped}")
    logger.info("Done. migrated=%d skipped=%d", migrated, skipped)


if __name__ == "__main__":
    main()
