"""
prepare_rerun.py — Pre-flight script for a full search rerun.

Does three things:
  1. Duplicates the active sheet tab as a timestamped backup.
  2. Clears search + QA columns (F–O) on the original tab.
  3. Reads the backup data and suggests 10 "healthy" and 10 "messy" rows
     to use as a test sample before committing to the full rerun.

Usage:
    python prepare_rerun.py [--dry-run]

Options:
    --dry-run   Print suggestions and exit without touching the sheet.
"""

import argparse
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# ── Column indices (1-based) ──────────────────────────────────────────────────
COL_DIST_NAME  = 4   # D
COL_R1_URL     = 6   # F  — first search column to clear
COL_REDIRECT   = 15  # O  — last search column to clear
COL_DOC_CLASS  = 16  # P  — not cleared; keep classification labels as reference
COL_QA_STATUS  = 14  # N
COL_BEST_URL   = 10  # J
COL_BEST_SCORE = 11  # K

CLEAR_START_COL = COL_R1_URL    # F
CLEAR_END_COL   = COL_REDIRECT  # O

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Filename patterns that indicate a correct compensation document
_GOOD_FILENAME_RE = re.compile(
    r'compensation|salary|pay.?scale|pay.?plan|pay.?plans|pay.?schedule|pay.?grade|wage|matrix',
    re.IGNORECASE,
)
# Filename patterns that indicate the wrong document type
_BAD_FILENAME_RE = re.compile(
    r'handbook|budget|cafr|agenda|minutes|student|parent.?guide|code.?of.?conduct|benefits',
    re.IGNORECASE,
)


def _col_letter(n: int) -> str:
    letter = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        letter = chr(65 + r) + letter
    return letter


def open_sheet(creds_path: str, spreadsheet_id: str):
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id)


def filename_from_url(url: str) -> str:
    path = url.lower().split("?")[0]
    return path.rsplit("/", 1)[-1]


def classify_row(row_data: list) -> str:
    """
    Return 'healthy', 'messy', or 'skip' for a data row.
    row_data is 0-indexed from the sheet (index 0 = col A).
    """
    def cell(col_1based):
        idx = col_1based - 1
        return row_data[idx].strip() if idx < len(row_data) else ""

    qa      = cell(COL_QA_STATUS)
    best    = cell(COL_BEST_URL)
    doc_cls = cell(COL_DOC_CLASS)

    if not best or best in ("NO_RESULT", "API_ERROR", ""):
        return "skip"

    filename = filename_from_url(best)

    # Healthy: confirmed PDF, no wrong-doc signal, good filename keyword
    if qa == "✓ PDF" and doc_cls not in ("Wrong Doc", "Error") and _GOOD_FILENAME_RE.search(filename):
        return "healthy"

    # Messy: review flag, wrong-doc classification, or bad filename
    if qa in ("⚠ REVIEW",) or doc_cls in ("Wrong Doc", "Error") or _BAD_FILENAME_RE.search(filename):
        return "messy"

    # Healthy enough: confirmed PDF, no red flags (even without keyword in filename)
    if qa == "✓ PDF" and doc_cls not in ("Wrong Doc", "Error"):
        return "healthy"

    return "skip"


def main():
    parser = argparse.ArgumentParser(description="Prepare sheet for full rerun.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print suggestions only; do not modify the sheet.")
    args = parser.parse_args()

    load_dotenv()
    creds_path_raw = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    creds_path_obj = Path(creds_path_raw).expanduser()
    if not creds_path_obj.is_absolute():
        creds_path_obj = Path(__file__).parent / creds_path_obj
    creds_path     = str(creds_path_obj)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    if not creds_path or not Path(creds_path).exists():
        print(f"ERROR: service account JSON not found: {creds_path}")
        sys.exit(1)
    if not spreadsheet_id:
        print("ERROR: SPREADSHEET_ID not set in .env")
        sys.exit(1)

    print("Connecting to Google Sheets...")
    spreadsheet = open_sheet(creds_path, spreadsheet_id)
    sheet = spreadsheet.sheet1

    # ── Read current data ─────────────────────────────────────────────────────
    print("Reading sheet data...")
    all_values = sheet.get_all_values()
    total_rows = len(all_values)
    print(f"  {total_rows - 1} data rows found (excluding header)")

    # ── Step 1: Duplicate tab ─────────────────────────────────────────────────
    timestamp   = datetime.now().strftime("%Y-%m-%d %H:%M")
    backup_name = f"Backup {timestamp}"

    if args.dry_run:
        print(f"\n[DRY RUN] Would duplicate tab as: '{backup_name}'")
    else:
        print(f"\nDuplicating tab as '{backup_name}'...")
        spreadsheet.duplicate_sheet(
            source_sheet_id=sheet.id,
            new_sheet_name=backup_name,
        )
        print("  Done.")

    # ── Step 2: Clear search columns F–O on original tab ─────────────────────
    start_letter = _col_letter(CLEAR_START_COL)
    end_letter   = _col_letter(CLEAR_END_COL)
    clear_range  = f"{start_letter}2:{end_letter}{total_rows}"

    if args.dry_run:
        print(f"[DRY RUN] Would clear {clear_range} on original tab")
    else:
        print(f"Clearing {clear_range} on original tab...")
        sheet.batch_clear([clear_range])
        print("  Done.")

    # ── Step 3: Suggest test rows ─────────────────────────────────────────────
    healthy_rows = []
    messy_rows   = []

    for sheet_row in range(2, total_rows + 1):
        row_data   = all_values[sheet_row - 1]
        district   = row_data[COL_DIST_NAME - 1].strip() if len(row_data) >= COL_DIST_NAME else ""
        if not district:
            continue
        cls = classify_row(row_data)
        best_url = row_data[COL_BEST_URL - 1].strip() if len(row_data) >= COL_BEST_URL else ""
        if cls == "healthy":
            healthy_rows.append((sheet_row, district, best_url))
        elif cls == "messy":
            messy_rows.append((sheet_row, district, best_url))

    random.seed(42)
    sample_healthy = random.sample(healthy_rows, min(10, len(healthy_rows)))
    sample_messy   = random.sample(messy_rows,   min(10, len(messy_rows)))

    sample_healthy.sort(key=lambda x: x[0])
    sample_messy.sort(key=lambda x: x[0])

    all_sample_rows = sorted(r[0] for r in sample_healthy + sample_messy)
    rows_arg = ",".join(str(r) for r in all_sample_rows)

    print(f"\n{'='*60}")
    print(f"HEALTHY SAMPLE ({len(sample_healthy)} rows)")
    print(f"{'='*60}")
    for row, name, url in sample_healthy:
        fname = filename_from_url(url)
        print(f"  Row {row:4d}  {name[:40]:<40}  {fname}")

    print(f"\n{'='*60}")
    print(f"MESSY SAMPLE ({len(sample_messy)} rows)")
    print(f"{'='*60}")
    for row, name, url in sample_messy:
        fname = filename_from_url(url)
        print(f"  Row {row:4d}  {name[:40]:<40}  {fname}")

    print(f"\n{'='*60}")
    print("RUN THE TEST WITH:")
    print(f"  python search_urls.py --rows {rows_arg}")
    print(f"{'='*60}")
    print(f"\nTotal candidates:  {len(healthy_rows)} healthy,  {len(messy_rows)} messy")
    print("Once satisfied with test results, run the full rerun:")
    print("  python search_urls.py")


if __name__ == "__main__":
    main()
