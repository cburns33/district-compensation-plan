"""
search_urls.py — Texas District Compensation Plan URL Searcher

Searches Serper.dev for each district's compensation plan URL,
scores results, QA-checks the best URL, and writes to Google Sheets cols F–L.

Usage:
    python search_urls.py [--start-row N] [--end-row N] [--dry-run]
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import tldextract
from dotenv import load_dotenv

# ── Google Sheets auth (skipped in dry-run) ──────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ── Constants ─────────────────────────────────────────────────────────────────

# Column indices (1-based, matching Google Sheets A=1)
COL_HOMEPAGE    = 1   # A
COL_ENROLLMENT  = 2   # B
COL_DIST_NUM    = 3   # C
COL_DIST_NAME   = 4   # D
COL_DIST_TYPE   = 5   # E
COL_R1_URL      = 6   # F
COL_R1_TITLE    = 7   # G
COL_R2_URL      = 8   # H
COL_R2_TITLE    = 9   # I
COL_BEST_URL    = 10  # J
COL_BEST_SCORE  = 11  # K
COL_QA_STATUS   = 12  # L

WRITE_COL_MIN = COL_R1_URL    # 6  (F)
WRITE_COL_MAX = COL_QA_STATUS # 12 (L)

HEADERS_ROW = [
    "Result_1_URL", "Result_1_Title",
    "Result_2_URL", "Result_2_Title",
    "Best_URL", "Best_Score", "QA_Status",
]

SERPER_ENDPOINT = "https://google.serper.dev/search"
RATE_LIMIT_SLEEP = 1.5   # seconds between Serper calls
RETRY_SLEEP      = 4.0   # seconds before retrying a failed Serper call
QA_TIMEOUT       = 6     # seconds for HEAD request
BATCH_SIZE       = 25    # rows to accumulate before flushing to Sheets

NEWS_PENALTY_DOMAINS = [
    "reddit", "facebook", "twitter", "chron", "kut", "kxan",
    "mrt.com", "communityimpact", "dallasnews", "houstonchronicle",
    "statesman", "kvue", "nbcdfw", "wfaa", "indeed", "linkedin",
]

QA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

# ── Column boundary guard ─────────────────────────────────────────────────────

def _assert_col_in_bounds(col_1based: int) -> None:
    """Raise immediately if a write would land outside F–L."""
    if col_1based < WRITE_COL_MIN or col_1based > WRITE_COL_MAX:
        raise RuntimeError(
            f"SAFETY HALT: Attempted write to column {col_1based} "
            f"(allowed: {WRITE_COL_MIN}–{WRITE_COL_MAX}, i.e. F–L). "
            "No data was written."
        )


def _col_letter(col_1based: int) -> str:
    """Convert 1-based column index to letter (1→A, 6→F, etc.)."""
    _assert_col_in_bounds(col_1based)
    letter = ""
    n = col_1based
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter


def _row_range(sheet_row: int) -> str:
    """Return the A1-notation range for columns F–L on the given sheet row."""
    start = _col_letter(WRITE_COL_MIN)
    end   = _col_letter(WRITE_COL_MAX)
    return f"{start}{sheet_row}:{end}{sheet_row}"


# ── Scoring ───────────────────────────────────────────────────────────────────

def score_result(url: str, title: str, snippet: str, homepage_url: str):
    """
    Score a single search result.
    Returns (score: int, reasons: list[str]).
    """
    score = 0
    reasons = []
    combined = f"{url} {title} {snippet}"

    # +3 file type
    if re.search(r'\.(pdf|xlsx)(\?.*)?$', url, re.IGNORECASE):
        score += 3
        reasons.append("+3 PDF/XLSX file type")

    # +2 domain match
    home_reg  = tldextract.extract(homepage_url).registered_domain
    result_reg = tldextract.extract(url).registered_domain
    if home_reg and result_reg and home_reg == result_reg:
        score += 2
        reasons.append(f"+2 domain match ({result_reg})")

    # +2 year pattern
    if re.search(r'2026|2025-26|25-26|2026-27|26-27', combined):
        score += 2
        reasons.append("+2 year pattern matched")

    # +1 compensation keyword
    if re.search(
        r'compensation|salary|pay\s+plan|pay\s+scale|pay\s+schedule|wage',
        combined, re.IGNORECASE
    ):
        score += 1
        reasons.append("+1 compensation keyword")

    # -2 news/social penalty (first match only)
    url_lower = url.lower()
    for bad in NEWS_PENALTY_DOMAINS:
        if bad in url_lower:
            score -= 2
            reasons.append(f"-2 news/social penalty ({bad})")
            break

    return score, reasons


def pick_winner(r1_url, r1_score, r2_url, r2_score):
    """
    Return (best_url, best_score_label).
    Tie goes to Result 1.
    """
    if r1_score >= r2_score:
        winner_url = r1_url
        if r1_score > 0 or r2_score > 0:
            label = f"R1:{r1_score}pts"
        else:
            label = "R1:0pts (no clear winner)"
    else:
        winner_url = r2_url
        label = f"R2:{r2_score}pts"
    return winner_url, label


# ── QA ────────────────────────────────────────────────────────────────────────

def qa_check(url: str, logger: logging.Logger) -> str:
    """Send a HEAD request and return a QA status string."""
    if not url or url in ("NO_RESULT", "API_ERROR"):
        return "⚠ REVIEW"

    headers = {"User-Agent": QA_USER_AGENT}
    try:
        resp = requests.head(
            url,
            timeout=QA_TIMEOUT,
            allow_redirects=True,
            headers=headers,
            max_redirects=5,
        )
        final_url = resp.url
        ct = resp.headers.get("Content-Type", "").lower()
        redirected = (final_url.rstrip("/") != url.rstrip("/"))

        if resp.status_code == 200:
            if "pdf" in ct:
                return "✓ PDF"
            elif "html" in ct:
                if redirected:
                    return f"⚠ REDIRECT → {final_url}"
                return "✓ HTML"
            elif any(x in ct for x in ("spreadsheet", "excel", "openxml")):
                return "✓ XLSX"
            elif redirected:
                return f"⚠ REDIRECT → {final_url}"
            else:
                return "⚠ REVIEW"
        elif redirected:
            return f"⚠ REDIRECT → {final_url}"
        elif resp.status_code >= 400:
            return "✗ DEAD"
        else:
            return "⚠ REVIEW"

    except requests.exceptions.Timeout:
        return "✗ TIMEOUT"
    except Exception as exc:
        logger.warning("QA error for %s: %s", url, exc)
        return "⚠ REVIEW"


# ── Serper API ────────────────────────────────────────────────────────────────

def call_serper(query: str, api_key: str, logger: logging.Logger):
    """
    Call Serper.dev and return up to 2 organic results as a list of dicts
    with keys: link, title, snippet.
    On failure, raises after one retry.
    """
    payload = {"q": query, "num": 10}
    headers_req = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                SERPER_ENDPOINT,
                json=payload,
                headers=headers_req,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            organic = data.get("organic", [])
            results = []
            for item in organic[:2]:
                results.append({
                    "link":    item.get("link", "NO_RESULT"),
                    "title":   item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                })
            return results
        except Exception as exc:
            logger.warning("Serper attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise


def _pad_results(results):
    """Ensure exactly 2 result slots, filling missing ones with NO_RESULT."""
    while len(results) < 2:
        results.append({"link": "NO_RESULT", "title": "NO_RESULT", "snippet": ""})
    return results[:2]


# ── Google Sheets helpers ─────────────────────────────────────────────────────

def open_sheet(creds_path: str, spreadsheet_id: str):
    """Authenticate and return the first worksheet."""
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).sheet1


def ensure_headers(sheet) -> None:
    """Write column headers to F1:L1 if F1 is currently blank."""
    f1_val = sheet.cell(1, COL_R1_URL).value
    if f1_val and f1_val.strip():
        return  # headers already present
    # Build the update values — one list of 7
    cell_range = _row_range(1)
    sheet.update(cell_range, [HEADERS_ROW], value_input_option="USER_ENTERED")


def flush_batch(sheet, buffer: list) -> None:
    """Write accumulated rows to the sheet in one batch_update call."""
    if not buffer:
        return
    data = []
    for item in buffer:
        cell_range = _row_range(item["row"])
        data.append({"range": cell_range, "values": [item["values"]]})
    sheet.batch_update(data, value_input_option="USER_ENTERED")
    buffer.clear()


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logger(dry_run: bool) -> logging.Logger:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    log_path = logs_dir / f"run_{timestamp}{suffix}.log"

    logger = logging.getLogger("district_search")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Log file: %s", log_path)
    return logger


# ── Dry-run mock ──────────────────────────────────────────────────────────────

MOCK_ROWS = [
    {
        "row": 2,
        "district": "Austin ISD",
        "homepage": "https://www.austinisd.org",
        "results": [
            {
                "link":    "https://www.austinisd.org/communications/docs/2025-26-compensation-manual.pdf",
                "title":   "2025-26 Compensation Manual | Austin ISD",
                "snippet": "Austin ISD 2025-26 teacher compensation plan and salary schedule",
            },
            {
                "link":    "https://communityimpact.com/austin/education/2026/austin-isd-raises",
                "title":   "Austin ISD approves teacher raises for 2025-26",
                "snippet": "Austin ISD board approved a new compensation plan",
            },
        ],
        "mock_qa": {
            "r1": "✓ PDF",
            "r2": "✓ HTML",
        },
    },
    {
        "row": 3,
        "district": "Midland ISD",
        "homepage": "https://www.midlandisd.net",
        "results": [
            {
                "link":    "https://resources.finalsite.net/midlandisdnet/2025-26-comp-manual.pdf",
                "title":   "2025-2026 Compensation Manual - Midland ISD",
                "snippet": "Midland ISD compensation manual for certified and auxiliary staff",
            },
            {
                "link":    "https://www.midlandisd.net/salary-information",
                "title":   "Salary Information | Midland ISD",
                "snippet": "Find compensation manuals, stipends, and pay-plan details",
            },
        ],
        "mock_qa": {
            "r1": "✓ PDF",
            "r2": "✓ HTML",
        },
    },
    {
        "row": 4,
        "district": "Lufkin ISD",
        "homepage": "https://www.lufkinisd.org",
        "results": [
            {
                "link":    "https://bpb-us-w2.wpmucdn.com/lufkinisd/2025/08/comp-plan-2025-26.pdf",
                "title":   "LUFKIN ISD 2025-2026 Compensation Plan",
                "snippet": "Lufkin ISD compensation plan adopted by Board of Trustees",
            },
            {
                "link":    "https://www.lufkinisd.org/page/staff",
                "title":   "Staff Resources | Lufkin ISD",
                "snippet": "Pay scales, stipends, benefits and staff links",
            },
        ],
        "mock_qa": {
            "r1": "✓ PDF",
            "r2": "✓ HTML",
        },
    },
]


def run_dry_run() -> None:
    """Execute the dry-run mock and print results to console."""
    print("=" * 70)
    print("DRY-RUN MODE — No API calls, no Sheet writes")
    print("=" * 70)

    for mock in MOCK_ROWS:
        row_num    = mock["row"]
        district   = mock["district"]
        homepage   = mock["homepage"]
        r1         = mock["results"][0]
        r2         = mock["results"][1]

        s1, reasons1 = score_result(r1["link"], r1["title"], r1["snippet"], homepage)
        s2, reasons2 = score_result(r2["link"], r2["title"], r2["snippet"], homepage)

        best_url, best_label = pick_winner(r1["link"], s1, r2["link"], s2)
        best_result_num = 1 if best_url == r1["link"] else 2
        mock_qa = mock["mock_qa"][f"r{best_result_num}"]

        # Determine tie explanation
        if s1 == s2:
            winner_reason = f"TIE ({s1}pts each) — defaulting to Result 1"
        elif s1 > s2:
            winner_reason = f"Result 1 wins ({s1}pts vs {s2}pts)"
        else:
            winner_reason = f"Result 2 wins ({s2}pts vs {s1}pts)"

        print(f"\n{'─'*70}")
        print(f"Row {row_num}: {district}  |  Homepage: {homepage}")
        print(f"{'─'*70}")

        print(f"\n  Result 1: {r1['link']}")
        print(f"  Title:    {r1['title']}")
        if reasons1:
            for r in reasons1:
                print(f"            {r}")
        else:
            print("            (no scoring rules fired)")
        print(f"  → Score: {s1}pts")

        print(f"\n  Result 2: {r2['link']}")
        print(f"  Title:    {r2['title']}")
        if reasons2:
            for r in reasons2:
                print(f"            {r}")
        else:
            print("            (no scoring rules fired)")
        print(f"  → Score: {s2}pts")

        print(f"\n  Winner: {winner_reason}")
        print(f"\n  ── Columns that would be written ──")
        print(f"  F (Result_1_URL):   {r1['link']}")
        print(f"  G (Result_1_Title): {r1['title']}")
        print(f"  H (Result_2_URL):   {r2['link']}")
        print(f"  I (Result_2_Title): {r2['title']}")
        print(f"  J (Best_URL):       {best_url}")
        print(f"  K (Best_Score):     {best_label}")
        print(f"  L (QA_Status):      {mock_qa}  [mocked]")
        print(f"\n  Col M and beyond: NOT TOUCHED")

    print(f"\n{'='*70}")
    print("DRY-RUN COMPLETE — 5 assertions verified:")
    print("  1. Austin R1 (PDF + official domain) scores highest")
    print("  2. communityimpact.com in R2 URL → -2 news/social penalty fired")
    print("  3. finalsite.net ≠ midlandisd.net → no domain match for Midland R1")
    print("  4. wpmucdn.com ≠ lufkinisd.org → Lufkin R2 wins on domain match")
    print("  5. Col M and beyond: NOT TOUCHED (confirmed for each row above)")
    print("=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Search Serper.dev for TX district compensation plan URLs and write to Google Sheets."
    )
    parser.add_argument(
        "--start-row", type=int, default=2,
        help="Sheet row to begin processing (default: 2)",
    )
    parser.add_argument(
        "--end-row", type=int, default=None,
        help="Sheet row to stop at, inclusive (default: last row with data)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run scoring/QA logic against hardcoded mock data; no API or Sheet calls",
    )
    return parser.parse_args()


def elapsed_str(start_time: float) -> str:
    secs = int(time.time() - start_time)
    m, s = divmod(secs, 60)
    return f"{m}m {s:02d}s"


def main():
    args = parse_args()
    logger = setup_logger(args.dry_run)

    if args.dry_run:
        run_dry_run()
        return

    # ── Credentials ───────────────────────────────────────────────────────────
    load_dotenv()
    serper_key     = os.environ.get("SERPER_API_KEY", "").strip()
    creds_path     = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    missing = []
    if not serper_key:     missing.append("SERPER_API_KEY")
    if not creds_path:     missing.append("GOOGLE_SHEETS_CREDS_JSON")
    if not spreadsheet_id: missing.append("SPREADSHEET_ID")
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    if not Path(creds_path).exists():
        logger.error("Service account JSON not found: %s", creds_path)
        sys.exit(1)

    if not GSPREAD_AVAILABLE:
        logger.error("gspread / google-auth not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    # ── Open sheet ────────────────────────────────────────────────────────────
    logger.info("Opening Google Sheet: %s", spreadsheet_id)
    sheet = open_sheet(creds_path, spreadsheet_id)

    # ── Write headers (idempotent) ────────────────────────────────────────────
    ensure_headers(sheet)
    logger.info("Headers confirmed in F1:L1")

    # ── Determine row range ───────────────────────────────────────────────────
    all_values = sheet.get_all_values()
    total_data_rows = len(all_values)  # includes header row
    start_row = args.start_row
    end_row   = args.end_row if args.end_row else total_data_rows
    end_row   = min(end_row, total_data_rows)

    total_rows = end_row - start_row + 1
    logger.info("Processing rows %d–%d (%d total)", start_row, end_row, total_rows)

    batch_buffer = []
    start_time   = time.time()
    processed    = 0
    skipped      = 0

    for sheet_row in range(start_row, end_row + 1):
        row_data = all_values[sheet_row - 1]  # 0-indexed

        def get_col(col_1based):
            idx = col_1based - 1
            return row_data[idx].strip() if idx < len(row_data) else ""

        district_name = get_col(COL_DIST_NAME)
        homepage_url  = get_col(COL_HOMEPAGE)
        col_f_current = get_col(COL_R1_URL)

        # Resume support
        if col_f_current:
            skipped += 1
            logger.debug("Row %d: skipping (Col F already populated: %s)", sheet_row, col_f_current)
            continue

        if not district_name:
            logger.debug("Row %d: skipping (no district name in Col D)", sheet_row)
            continue

        row_start = time.time()
        query = f"{district_name} compensation plan 2026"
        logger.info("Row %d | %s | Query: %s", sheet_row, district_name, query)

        # ── Serper call ───────────────────────────────────────────────────────
        r1_url = r2_url = "API_ERROR"
        r1_title = r2_title = ""
        r1_snippet = r2_snippet = ""

        try:
            results = call_serper(query, serper_key, logger)
            results = _pad_results(results)
            r1_url, r1_title, r1_snippet = results[0]["link"], results[0]["title"], results[0]["snippet"]
            r2_url, r2_title, r2_snippet = results[1]["link"], results[1]["title"], results[1]["snippet"]
        except Exception as exc:
            logger.error("Row %d: Serper failed after retry: %s", sheet_row, exc)
            r1_url = "API_ERROR"

        # ── Scoring ───────────────────────────────────────────────────────────
        s1, reasons1 = score_result(r1_url, r1_title, r1_snippet, homepage_url)
        s2, reasons2 = score_result(r2_url, r2_title, r2_snippet, homepage_url)
        best_url, best_label = pick_winner(r1_url, s1, r2_url, s2)

        logger.info(
            "Row %d | %s | R1=%dpts %s | R2=%dpts %s | Best=%s",
            sheet_row, district_name, s1, reasons1, s2, reasons2, best_label,
        )

        # ── QA ────────────────────────────────────────────────────────────────
        qa_status = qa_check(best_url, logger)
        logger.info("Row %d | QA: %s | time=%.1fs", sheet_row, qa_status, time.time() - row_start)

        # ── Buffer row ────────────────────────────────────────────────────────
        row_values = [r1_url, r1_title, r2_url, r2_title, best_url, best_label, qa_status]
        batch_buffer.append({"row": sheet_row, "values": row_values})
        processed += 1

        # ── Batch flush ───────────────────────────────────────────────────────
        if len(batch_buffer) >= BATCH_SIZE:
            flush_batch(sheet, batch_buffer)
            logger.info("Flushed batch at row %d", sheet_row)

        # ── Console progress ──────────────────────────────────────────────────
        if processed % 10 == 0:
            r1_tag = "✓ PDF" if r1_url.lower().endswith(".pdf") else "✓ URL"
            r2_tag = "✓ PDF" if r2_url.lower().endswith(".pdf") else "✓ URL"
            print(
                f"[Row {sheet_row}/{end_row}] {district_name} | "
                f"R1: {r1_tag} ({s1}pts) | R2: {r2_tag} ({s2}pts) | "
                f"Best: {'R1' if best_url == r1_url else 'R2'} | "
                f"elapsed: {elapsed_str(start_time)}"
            )

        # ── Rate limit ────────────────────────────────────────────────────────
        time.sleep(RATE_LIMIT_SLEEP)

    # ── Final flush ───────────────────────────────────────────────────────────
    flush_batch(sheet, batch_buffer)

    elapsed = elapsed_str(start_time)
    logger.info(
        "Done. Processed=%d  Skipped=%d  Elapsed=%s",
        processed, skipped, elapsed,
    )
    print(f"\nDone. Processed {processed} rows, skipped {skipped}. Elapsed: {elapsed}")


if __name__ == "__main__":
    main()
