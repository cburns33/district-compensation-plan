"""
remediate.py — Targeted fix pass for failed rows in the Google Sheet.

Processes only rows where QA_Status (Col L) is one of:
    ✗ DEAD | ✗ TIMEOUT | ⚠ REVIEW

For each failed row, escalates through three stages before spending a Serper credit:

  Stage 1 — GET fallback on existing Best_URL (Col J)
              Fixes Round Rock type: server blocks HEAD but serves GET fine.

  Stage 2 — Re-score R1 (Col F) and R2 (Col H) from the sheet
              Fixes Lewisville/Willis type: Tier-2/3 promoted a bad URL when
              a valid result was already in the sheet.

  Stage 3 — Fresh year-free Serper query (spends 1 credit)
              Fixes Sulphur Springs type: correct URL was never in the top 10.

Only columns J, K, L are ever written. F–I are never touched.

Usage:
    python remediate.py                        # process all failed rows
    python remediate.py --dry-run              # print what would happen, no writes
    python remediate.py --start-row 2 --end-row 100
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Force UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
import tldextract
from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ── Constants ─────────────────────────────────────────────────────────────────

COL_HOMEPAGE   = 1   # A
COL_DIST_NAME  = 4   # D
COL_R1_URL     = 6   # F
COL_R1_TITLE   = 7   # G
COL_R2_URL     = 8   # H
COL_R2_TITLE   = 9   # I
COL_BEST_URL       = 10  # J
COL_BEST_SCORE     = 11  # K
COL_BEST_URL_CLASS = 12  # L
COL_SEARCH_METHOD  = 13  # M
COL_QA_STATUS      = 14  # N
COL_REDIRECT_URL   = 15  # O

# Remediation only ever writes J–O
WRITE_COL_MIN = COL_BEST_URL      # 10
WRITE_COL_MAX = COL_REDIRECT_URL  # 15

FAILED_STATUSES = {"✗ DEAD", "✗ TIMEOUT", "⚠ REVIEW"}

# A row is already remediated if its Best_Score ends with +fixed
REMEDIATED_MARKER = "+fixed"

SERPER_ENDPOINT  = "https://google.serper.dev/search"
RATE_LIMIT_SLEEP = 1.5
RETRY_SLEEP      = 4.0
QA_TIMEOUT       = 6
BATCH_SIZE       = 25

NEWS_PENALTY_DOMAINS = [
    "reddit", "facebook", "twitter", "chron", "kut", "kxan",
    "mrt.com", "communityimpact", "dallasnews", "houstonchronicle",
    "statesman", "kvue", "nbcdfw", "wfaa", "indeed", "linkedin",
    "addtoany", "sharethis",
]

QA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
WORKSHEET_NAME = "Unique Districts"

# ── Column boundary guard ─────────────────────────────────────────────────────

def _assert_col_in_bounds(col_1based: int) -> None:
    if col_1based < WRITE_COL_MIN or col_1based > WRITE_COL_MAX:
        raise RuntimeError(
            f"SAFETY HALT: remediate.py attempted write to column {col_1based} "
            f"(allowed: {WRITE_COL_MIN}–{WRITE_COL_MAX}, i.e. J–L only). Halting."
        )


def _row_range(sheet_row: int) -> str:
    """Return A1-notation for J–O on the given row."""
    _assert_col_in_bounds(WRITE_COL_MIN)
    _assert_col_in_bounds(WRITE_COL_MAX)
    return f"J{sheet_row}:O{sheet_row}"


# ── Scoring (mirrors search_urls.py, with social filter applied) ──────────────

def score_result(url: str, title: str, snippet: str, homepage_url: str):
    score = 0
    reasons = []
    combined = f"{url} {title} {snippet}"

    if re.search(r'\.(pdf|xlsx)(\?.*)?$', url, re.IGNORECASE):
        score += 3
        reasons.append("+3 PDF/XLSX")

    home_reg   = tldextract.extract(homepage_url).registered_domain
    result_reg = tldextract.extract(url).registered_domain
    if home_reg and result_reg and home_reg == result_reg:
        score += 2
        reasons.append(f"+2 domain match ({result_reg})")

    if re.search(r'2026|2025-26|25-26|2026-27|26-27', combined):
        score += 2
        reasons.append("+2 year pattern")

    if re.search(
        r'compensation|salary|pay\s+plan|pay\s+scale|pay\s+schedule|wage',
        combined, re.IGNORECASE
    ):
        score += 1
        reasons.append("+1 keyword")

    url_lower = url.lower()
    for bad in NEWS_PENALTY_DOMAINS:
        if bad in url_lower:
            score -= 2
            reasons.append(f"-2 social/news ({bad})")
            break

    return score, reasons


def parse_best_label(label: str):
    """Split 'R2:6pts +noyear +fixed' into (score_str, classification, search_method)."""
    cls_match      = re.match(r'^(R\d+|R\?|T\d+)', label)
    classification = cls_match.group(1) if cls_match else ""
    score_match    = re.search(r':(\d+)pts', label)
    score_str      = score_match.group(1) if score_match else "0"
    methods        = re.findall(r'\+\w+', label)
    return score_str, classification, " ".join(methods)


def extract_redirect_url(qa_status: str) -> str:
    """Return the URL from '⚠ REDIRECT → https://...' or ''."""
    if "→" in qa_status:
        return qa_status.split("→", 1)[1].strip()
    return ""


def clean_qa_status(qa_status: str) -> str:
    """Strip appended URL from redirect statuses."""
    if qa_status.startswith("⚠ REDIRECT"):
        return "⚠ REDIRECT"
    return qa_status


def is_social_or_news(url: str) -> bool:
    url_lower = url.lower()
    return any(bad in url_lower for bad in NEWS_PENALTY_DOMAINS)


# ── QA ────────────────────────────────────────────────────────────────────────

def _classify_response(resp, original_url: str) -> str:
    """Turn a requests.Response into a QA status string."""
    final_url = resp.url
    ct = resp.headers.get("Content-Type", "").lower()
    redirected = (final_url.rstrip("/") != original_url.rstrip("/"))

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


def qa_check(url: str, logger: logging.Logger) -> str:
    """
    HEAD first; if that returns 4xx, retry with GET+stream=True.
    This fixes the Round Rock pattern where servers block HEAD but serve GET.
    """
    if not url or url in ("NO_RESULT", "API_ERROR"):
        return "⚠ REVIEW"
    if is_social_or_news(url):
        return "✗ DEAD"

    headers = {"User-Agent": QA_USER_AGENT}
    try:
        resp = requests.head(
            url, timeout=QA_TIMEOUT, allow_redirects=True, headers=headers
        )
        result = _classify_response(resp, url)

        # If HEAD said dead, verify with GET before giving up
        if result == "✗ DEAD":
            try:
                get_resp = requests.get(
                    url, timeout=QA_TIMEOUT, allow_redirects=True,
                    headers=headers, stream=True
                )
                get_resp.close()
                get_result = _classify_response(get_resp, url)
                if get_result != "✗ DEAD":
                    logger.info("GET fallback rescued %s: HEAD=DEAD, GET=%s", url, get_result)
                    return get_result
            except Exception:
                pass  # GET also failed — keep original DEAD result

        return result

    except requests.exceptions.Timeout:
        return "✗ TIMEOUT"
    except Exception as exc:
        logger.warning("QA error for %s: %s", url, exc)
        return "⚠ REVIEW"


# ── Serper API ────────────────────────────────────────────────────────────────

def call_serper_noyear(district_name: str, api_key: str, logger: logging.Logger):
    """Year-free fallback query — Stage 3 of remediation."""
    query = f'{district_name} "pay scale" OR "salary schedule" OR "compensation plan"'
    logger.info("Stage-3 Serper (year-free): %s", query)
    payload = {"q": query, "num": 10}
    req_headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}

    for attempt in range(2):
        try:
            resp = requests.post(
                SERPER_ENDPOINT, json=payload, headers=req_headers, timeout=10
            )
            resp.raise_for_status()
            organic = resp.json().get("organic", [])
            return [
                {"link": r.get("link", ""), "title": r.get("title", ""), "snippet": r.get("snippet", "")}
                for r in organic[:10] if r.get("link")
            ]
        except Exception as exc:
            logger.warning("Serper attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise
    return []


# ── Google Sheets ─────────────────────────────────────────────────────────────

def open_sheet(creds_path: str, spreadsheet_id: str):
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(WORKSHEET_NAME)


def flush_batch(sheet, buffer: list, dry_run: bool) -> None:
    if not buffer:
        return
    if dry_run:
        for item in buffer:
            print(f"  [DRY-RUN] Would write row {item['row']}: {item['values']}")
        buffer.clear()
        return
    data = [{"range": _row_range(item["row"]), "values": [item["values"]]} for item in buffer]
    sheet.batch_update(data, value_input_option="USER_ENTERED")
    buffer.clear()


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logger(dry_run: bool) -> logging.Logger:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    log_path = logs_dir / f"remediate_{timestamp}{suffix}.log"

    logger = logging.getLogger("remediate")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Remediation log: %s", log_path)
    return logger


# ── Per-row remediation logic ─────────────────────────────────────────────────

def remediate_row(
    sheet_row: int,
    district_name: str,
    homepage_url: str,
    r1_url: str,
    r1_title: str,
    r2_url: str,
    r2_title: str,
    current_best_url: str,
    current_best_score: str,
    current_qa: str,
    serper_key: str,
    logger: logging.Logger,
    dry_run: bool,
):
    """
    Returns (new_best_url, new_best_score, new_qa, stage_used, changed: bool).
    """
    logger.info(
        "Row %d | %s | current Best=%s | QA=%s",
        sheet_row, district_name, current_best_url, current_qa,
    )

    # ── Stage 1: GET fallback on the existing Best_URL ────────────────────────
    stage1_qa = qa_check(current_best_url, logger)
    if stage1_qa not in FAILED_STATUSES:
        if stage1_qa != current_qa:
            label = current_best_score.rstrip() + " +fixed"
            logger.info("Row %d | Stage-1 rescued: %s → %s", sheet_row, current_qa, stage1_qa)
            return current_best_url, label, stage1_qa, 1, True
        else:
            # QA confirmed still bad — move on
            logger.info("Row %d | Stage-1 confirmed still %s", sheet_row, stage1_qa)

    # ── Stage 2: re-score R1 and R2 from the sheet ────────────────────────────
    candidates = []
    for url, title in [(r1_url, r1_title), (r2_url, r2_title)]:
        if not url or url in ("NO_RESULT", "API_ERROR") or is_social_or_news(url):
            continue
        sc, reasons = score_result(url, title, "", homepage_url)
        candidates.append((url, title, sc, reasons))

    candidates.sort(key=lambda x: x[2], reverse=True)

    for url, title, sc, reasons in candidates:
        if sc <= 0:
            continue
        qa = qa_check(url, logger)
        if qa not in FAILED_STATUSES:
            label = f"R?:{sc}pts +fixed"
            logger.info(
                "Row %d | Stage-2 found live URL: %s (%dpts, %s) %s",
                sheet_row, url, sc, qa, reasons,
            )
            return url, label, qa, 2, True
        else:
            logger.info("Row %d | Stage-2 candidate dead: %s → %s", sheet_row, url, qa)

    # ── Stage 3: fresh year-free Serper query ─────────────────────────────────
    if not serper_key:
        logger.info("Row %d | Stage-3 skipped (no API key in dry-run)", sheet_row)
        return current_best_url, current_best_score, current_qa, 3, False

    try:
        results = call_serper_noyear(district_name, serper_key, logger)
        time.sleep(RATE_LIMIT_SLEEP)
    except Exception as exc:
        logger.error("Row %d | Stage-3 Serper failed: %s", sheet_row, exc)
        return current_best_url, current_best_score, current_qa, 3, False

    stage3_candidates = []
    for r in results:
        url = r["link"]
        if not url or is_social_or_news(url):
            continue
        sc, reasons = score_result(url, r["title"], r["snippet"], homepage_url)
        stage3_candidates.append((url, r["title"], sc, reasons))

    stage3_candidates.sort(key=lambda x: x[2], reverse=True)

    for url, title, sc, reasons in stage3_candidates:
        if sc <= 0:
            continue
        qa = qa_check(url, logger)
        if qa not in FAILED_STATUSES:
            label = f"R?:{sc}pts +noyear +fixed"
            logger.info(
                "Row %d | Stage-3 found live URL: %s (%dpts, %s) %s",
                sheet_row, url, sc, qa, reasons,
            )
            return url, label, qa, 3, True
        else:
            logger.info("Row %d | Stage-3 candidate dead: %s → %s", sheet_row, url, qa)

    logger.info("Row %d | All stages exhausted — no improvement found", sheet_row)
    return current_best_url, current_best_score, current_qa, 3, False


# ── CLI args ──────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Remediate failed rows (✗ DEAD / ✗ TIMEOUT / ⚠ REVIEW) in the Google Sheet."
    )
    parser.add_argument("--start-row", type=int, default=2)
    parser.add_argument("--end-row",   type=int, default=None)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without writing to the sheet or calling Serper",
    )
    return parser.parse_args()


def elapsed_str(start_time: float) -> str:
    secs = int(time.time() - start_time)
    m, s = divmod(secs, 60)
    return f"{m}m {s:02d}s"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    logger = setup_logger(args.dry_run)

    load_dotenv()
    serper_key     = os.environ.get("SERPER_API_KEY", "").strip()
    creds_path_raw = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    creds_path_obj = Path(creds_path_raw).expanduser()
    if not creds_path_obj.is_absolute():
        creds_path_obj = Path(__file__).parent / creds_path_obj
    creds_path     = str(creds_path_obj)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    # Sheet access is always required (even dry-run needs to read which rows failed)
    # Only Serper calls and writes are skipped in dry-run mode
    missing = []
    if not creds_path:     missing.append("GOOGLE_SHEETS_CREDS_JSON")
    if not spreadsheet_id: missing.append("SPREADSHEET_ID")
    if not args.dry_run and not serper_key:
        missing.append("SERPER_API_KEY")
    if missing:
        logger.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)
    if not GSPREAD_AVAILABLE:
        logger.error("gspread not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    logger.info("Opening sheet: %s", spreadsheet_id)
    sheet = open_sheet(creds_path, spreadsheet_id)

    all_values = sheet.get_all_values()
    total_rows = len(all_values)
    start_row  = args.start_row
    end_row    = min(args.end_row or total_rows, total_rows)

    def get_col(row_data, col_1based):
        idx = col_1based - 1
        return row_data[idx].strip() if idx < len(row_data) else ""

    # Scan for failed rows
    failed_rows = []
    for sheet_row in range(start_row, end_row + 1):
        row_data  = all_values[sheet_row - 1]
        qa_status  = get_col(row_data, COL_QA_STATUS)
        best_score = get_col(row_data, COL_BEST_SCORE)
        # Best_Score is now just a number — check Search_Method col for +fixed marker
        search_method = get_col(row_data, COL_SEARCH_METHOD)

        # Skip rows already remediated
        if REMEDIATED_MARKER in search_method or REMEDIATED_MARKER in best_score:
            continue

        if any(qa_status.startswith(s) for s in FAILED_STATUSES):
            failed_rows.append(sheet_row)

    total_failed = len(failed_rows)
    print(f"Found {total_failed} failed rows to remediate (rows {start_row}–{end_row})")
    logger.info("Failed rows found: %d", total_failed)

    if total_failed == 0:
        print("Nothing to do.")
        return

    batch_buffer = []
    start_time   = time.time()
    fixed        = 0
    unchanged    = 0
    stage_counts = {1: 0, 2: 0, 3: 0}

    for i, sheet_row in enumerate(failed_rows, 1):
        row_data = all_values[sheet_row - 1]

        district_name    = get_col(row_data, COL_DIST_NAME)
        homepage_url     = get_col(row_data, COL_HOMEPAGE)
        r1_url           = get_col(row_data, COL_R1_URL)
        r1_title         = get_col(row_data, COL_R1_TITLE)
        r2_url           = get_col(row_data, COL_R2_URL)
        r2_title         = get_col(row_data, COL_R2_TITLE)
        current_best_url = get_col(row_data, COL_BEST_URL)
        current_score    = get_col(row_data, COL_BEST_SCORE)
        current_qa       = get_col(row_data, COL_QA_STATUS)

        print(f"[{i}/{total_failed}] Row {sheet_row} | {district_name} | {current_qa}")

        new_best, new_score, new_qa, stage, changed = remediate_row(
            sheet_row=sheet_row,
            district_name=district_name,
            homepage_url=homepage_url,
            r1_url=r1_url,
            r1_title=r1_title,
            r2_url=r2_url,
            r2_title=r2_title,
            current_best_url=current_best_url,
            current_best_score=current_score,
            current_qa=current_qa,
            serper_key=serper_key if not args.dry_run else "",
            logger=logger,
            dry_run=args.dry_run,
        )

        if changed:
            fixed += 1
            stage_counts[stage] += 1
            print(f"  → Stage {stage} fixed: {new_qa}  ({new_score})")
            score_str, classification, search_method = parse_best_label(new_score)
            redirect_url = extract_redirect_url(new_qa)
            qa_clean     = clean_qa_status(new_qa)
            batch_buffer.append({
                "row": sheet_row,
                "values": [new_best, score_str, classification, search_method, qa_clean, redirect_url],
            })
        else:
            unchanged += 1
            print(f"  → No improvement found")

        if len(batch_buffer) >= BATCH_SIZE:
            flush_batch(sheet, batch_buffer, args.dry_run)
            logger.info("Flushed batch at row %d", sheet_row)

    flush_batch(sheet, batch_buffer, args.dry_run)

    elapsed = elapsed_str(start_time)
    summary = (
        f"\nRemediation complete. "
        f"Fixed: {fixed}  |  Unchanged: {unchanged}  |  Elapsed: {elapsed}\n"
        f"  Stage 1 (GET fallback):     {stage_counts[1]}\n"
        f"  Stage 2 (R1/R2 from sheet): {stage_counts[2]}\n"
        f"  Stage 3 (new Serper query): {stage_counts[3]}"
    )
    print(summary)
    logger.info(summary)


if __name__ == "__main__":
    main()
