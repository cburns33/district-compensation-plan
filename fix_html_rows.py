"""
fix_html_rows.py — Crawl HTML landing pages to find direct PDF/XLSX links.

For each Unique Districts row where QA_Status == "✓ HTML", fetches the page,
looks for PDF/XLSX links near salary-related text, and if found:
  - Writes the document URL to Redirect_URL (col O)
  - Updates QA_Status (col N) to "✓ PDF" or "✓ XLSX"
  - Updates Search_Method (col M) to note the crawl
  - Clears Doc_Class (col P) so classify_documents.py will re-process the row

Rows where no document link is found are logged for manual review or re-search.

Usage:
    python fix_html_rows.py [--dry-run] [--limit N]
"""

import argparse
import io
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
CREDS_JSON     = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
SHEET_NAME     = "Unique Districts"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]

COL_DIST_NAME    = 4   # D
COL_BEST_URL     = 10  # J
COL_BEST_SCORE   = 11  # K
COL_SEARCH_METHOD = 13 # M
COL_QA_STATUS    = 14  # N
COL_REDIRECT_URL = 15  # O
COL_DOC_CLASS    = 16  # P

FETCH_TIMEOUT = 15
SLEEP_BETWEEN = 1.0
BATCH_SIZE    = 20

QA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SALARY_KEYWORDS = re.compile(
    r"salary|compensation|pay\s+scale|pay\s+schedule|pay\s+plan|stipend|step|lane|base\s+pay",
    re.IGNORECASE,
)


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    Path("logs").mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"logs/fix_html_{ts}.log"

    logger = logging.getLogger("fix_html")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log: {log_path}")
    return logger


# ── Sheets ────────────────────────────────────────────────────────────────────

def open_sheet(logger):
    if not GSPREAD_AVAILABLE:
        logger.error("gspread not installed.")
        sys.exit(1)
    creds = Credentials.from_service_account_file(CREDS_JSON, scopes=SCOPES)
    gc = gspread.authorize(creds)
    wb = gc.open_by_key(SPREADSHEET_ID)
    return wb.worksheet(SHEET_NAME)


def flush_batch(ws, buffer: list, dry_run: bool, logger):
    if not buffer:
        return
    if dry_run:
        for item in buffer:
            logger.info(
                f"  [dry-run] Row {item['row']} {item['name'][:35]:<35} "
                f"-> {item['result']}"
            )
        buffer.clear()
        return

    data = []
    for item in buffer:
        r = item["row"]
        if item["found_url"]:
            data.append({
                "range": f"M{r}:P{r}",
                "values": [[
                    item["new_method"],   # M: Search_Method
                    item["new_qa"],       # N: QA_Status
                    item["found_url"],    # O: Redirect_URL
                    "",                   # P: Doc_Class — cleared for re-classification
                ]],
            })
        # Rows with no find are not written — they stay as "✓ HTML" for manual review
    if data:
        ws.batch_update(data, value_input_option="USER_ENTERED")
    buffer.clear()


# ── Crawl ─────────────────────────────────────────────────────────────────────

def score_link(href: str, link_text: str, context: str) -> int:
    """Score a candidate PDF/XLSX link. Higher = more likely to be a salary doc."""
    score = 0
    combined = f"{href} {link_text} {context}"

    if SALARY_KEYWORDS.search(combined):
        score += 2
    if re.search(r"2025|2026|25-26|26-27", combined):
        score += 1
    if re.search(r"teacher", combined, re.IGNORECASE):
        score += 1
    # Penalize clearly wrong documents
    if re.search(
        r"handbook|policy|calendar|budget|improvement\s+plan|strategic\s+plan|audit"
        r"|bond\s+election|election|minutes|agenda|notice|board\s+order|signed"
        r"|contract|agreement|report|annual\s+report|financial|waiver|application",
        combined, re.IGNORECASE,
    ):
        score -= 2

    return score


def find_doc_link(page_url: str) -> tuple[str | None, str | None]:
    """
    Fetch page_url and return (doc_url, doc_type) for the best PDF/XLSX found,
    or (None, None) if nothing useful is on the page.
    """
    headers = {"User-Agent": QA_USER_AGENT}
    try:
        resp = requests.get(page_url, headers=headers, timeout=FETCH_TIMEOUT, allow_redirects=True)
        if resp.status_code >= 400:
            return None, f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, str(e)[:80]

    soup = BeautifulSoup(resp.content, "html.parser")
    base_url = resp.url  # use final URL after any redirects

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue

        # Resolve relative URLs
        full_url = urljoin(base_url, href)
        ext = urlparse(full_url).path.lower().rsplit(".", 1)[-1]

        if ext not in ("pdf", "xlsx", "xls"):
            continue

        link_text = a.get_text(strip=True)
        # Grab surrounding text (parent element) for context
        parent_text = ""
        if a.parent:
            parent_text = a.parent.get_text(separator=" ", strip=True)[:200]

        sc = score_link(full_url, link_text, parent_text)
        candidates.append((sc, full_url, ext))

    if not candidates:
        return None, "no PDF/XLSX links found"

    # Sort by score descending; break ties by preferring PDF over XLSX
    candidates.sort(key=lambda x: (x[0], x[2] == "pdf"), reverse=True)
    best_score, best_url, best_ext = candidates[0]

    # Only accept if score >= 0 (not actively penalized)
    if best_score < 0:
        return None, f"best link scored {best_score} (likely wrong doc)"

    doc_type = "PDF" if best_ext == "pdf" else "XLSX"
    return best_url, doc_type


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit",   type=int, default=0)
    args = parser.parse_args()

    logger = setup_logging()
    if args.dry_run:
        logger.info("=== DRY RUN — no sheet writes ===")

    ws = None if args.dry_run else open_sheet(logger)

    if args.dry_run:
        # 5 mock rows representative of real cases
        rows = [
            (10, "Orangefield ISD",           "https://www.orangefieldisd.net/documents/salary-schedule/436", "✓ HTML", ""),
            (20, "Rankin ISD",                "https://www.rankinisd.net/page/salary-schedules",              "✓ HTML", ""),
            (30, "Early ISD",                 "https://www.earlyisd.net/apps/pages/compensation",             "✓ HTML", ""),
            (40, "Some JS-only District",     "https://example.com/salary",                                  "✓ HTML", ""),
            (50, "Already Fixed District",    "https://example.com/salary",                                  "✓ PDF",  "https://example.com/real.pdf"),
        ]
    else:
        raw = ws.get_all_values()
        rows = []
        for i, row in enumerate(raw[1:], start=2):
            def cell(col, r=row): return r[col - 1] if len(r) >= col else ""
            qa = cell(COL_QA_STATUS)
            if qa.strip() != "✓ HTML":
                continue
            # Skip if Redirect_URL already populated (already processed by this script)
            if cell(COL_REDIRECT_URL).strip():
                continue
            rows.append((
                i,
                cell(COL_DIST_NAME),
                cell(COL_BEST_URL),
                qa,
                cell(COL_SEARCH_METHOD),
            ))

    logger.info(f"Found {len(rows)} HTML rows to process")

    buffer = []
    found_count = 0
    not_found = []

    for idx, (row_num, dist_name, best_url, qa_status, search_method) in enumerate(rows):
        if args.limit and idx >= args.limit:
            logger.info(f"Reached --limit {args.limit}, stopping.")
            break

        logger.info(f"[{idx+1}/{len(rows)}] {dist_name[:45]}")

        if args.dry_run:
            # Don't actually fetch — simulate based on name
            if "js-only" in dist_name.lower() or "already" in dist_name.lower():
                doc_url, doc_type = None, "no PDF/XLSX links found"
            else:
                doc_url, doc_type = f"https://example.com/{dist_name.lower().replace(' ','_')}_salary.pdf", "PDF"
        else:
            if not best_url or not best_url.strip():
                not_found.append((row_num, dist_name, "blank URL"))
                continue
            doc_url, doc_type = find_doc_link(best_url)
            time.sleep(SLEEP_BETWEEN)

        if doc_url:
            new_qa     = f"✓ {doc_type}"
            new_method = (search_method + "+html_crawl").lstrip("+") if search_method else "html_crawl"
            buffer.append({
                "row":        row_num,
                "name":       dist_name,
                "found_url":  doc_url,
                "new_qa":     new_qa,
                "new_method": new_method,
                "result":     f"found {doc_type} -> {doc_url[:70]}",
            })
            found_count += 1
            logger.info(f"  Found {doc_type}: {doc_url[:80]}")
        else:
            not_found.append((row_num, dist_name, doc_type))
            logger.info(f"  Not found: {doc_type}")
            buffer.append({
                "row": row_num, "name": dist_name,
                "found_url": None, "result": f"not found: {doc_type}",
            })

        if len(buffer) >= BATCH_SIZE:
            flush_batch(ws, buffer, args.dry_run, logger)

    flush_batch(ws, buffer, args.dry_run, logger)

    logger.info(f"\nDone. Found: {found_count} | Not found: {len(not_found)}")
    if not_found:
        logger.info("\nRows with no doc found (manual review or re-search):")
        for row_num, name, reason in not_found:
            logger.info(f"  Row {row_num:>4}  {name:<45}  {reason}")


if __name__ == "__main__":
    main()
