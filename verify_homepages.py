"""
verify_homepages.py

For each district row, searches Brave for the district's official website
and writes the result to column AL:
  "✓ match"   — found domain matches column A
  <url>        — found a different domain (mismatch to review)
  "no result"  — search returned nothing usable
  "error"      — API/network error on this row

Supports resume: skips rows where column AL is already populated.
Stops immediately on Brave quota/auth errors.
"""

import os, sys, time, logging, argparse
from pathlib import Path
from datetime import datetime

import requests
import tldextract
import gspread
from dotenv import load_dotenv

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────
SPREADSHEET_ID      = os.environ["SPREADSHEET_ID"]
BRAVE_API_KEY       = os.environ.get("BRAVE_API_KEY", "").strip()
GOOGLE_SHEETS_CREDS = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "")

COL_HOMEPAGE  = 1   # A
COL_DIST_NAME = 4   # D
COL_RESULT    = 38  # AL

RATE_LIMIT_SLEEP = 1.2
BATCH_SIZE       = 25
BRAVE_ENDPOINT   = "https://api.search.brave.com/res/v1/web/search"

# Domains to skip when picking the "official site" from search results
SKIP_DOMAINS = {
    "tea.texas.gov", "texas.gov", "txed.net",
    "nces.ed.gov", "ed.gov", "data.texas.gov",
    "texastribune.org", "texasmonthly.com",
    "indeed.com", "linkedin.com", "facebook.com", "twitter.com", "x.com",
    "yelp.com", "greatschools.org", "niche.com", "publicschoolreview.com",
    "usnews.com", "reddit.com", "wikipedia.org", "wikimedia.org",
    "communityimpact.com", "chron.com", "dallasnews.com",
    "glassdoor.com", "ziprecruiter.com", "simplyhired.com",
    "schooldigger.com", "rating.com", "collegetuitioncompare.com",
    "schools.texastribune.org",
}

# Regex patterns that suggest an official district site
import re as _re
DISTRICT_SITE_RE = _re.compile(
    r'(isd|cisd|gisd|kisd|lisd|misd|nisd|pisd|risd|visd|wisd'
    r'|school|schools|k12\.tx\.us)',
    _re.IGNORECASE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def col_letter(n: int) -> str:
    letter = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter


def normalize_domain(url: str) -> str:
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    return tldextract.extract(url).registered_domain.lower()


def search_homepage(district_name: str) -> str | None:
    """
    Search Brave for the district's official website.
    Returns the best candidate URL, or None.
    Raises RuntimeError with code string on quota/auth failure.
    """
    query = f"{district_name} Texas ISD official website"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    resp = requests.get(
        BRAVE_ENDPOINT,
        headers=headers,
        params={"q": query, "count": 5, "country": "us"},
        timeout=10,
    )

    if resp.status_code in (401, 403):
        raise RuntimeError("BRAVE_AUTH_ERROR")
    if resp.status_code in (402, 429):
        raise RuntimeError("BRAVE_QUOTA_EXCEEDED")
    if resp.status_code != 200:
        raise RuntimeError(f"BRAVE_HTTP_{resp.status_code}")

    results = resp.json().get("web", {}).get("results", [])
    candidates = [r.get("url", "") for r in results
                  if r.get("url") and tldextract.extract(r["url"]).registered_domain not in SKIP_DOMAINS
                  and tldextract.extract(r["url"]).registered_domain]

    # Prefer a result whose domain looks like an official district site
    for url in candidates:
        if DISTRICT_SITE_RE.search(tldextract.extract(url).registered_domain):
            return url

    # Fall back to first non-skip result
    return candidates[0] if candidates else None


def flush(ws, buffer: list, col: str) -> None:
    if not buffer:
        return
    data = [{"range": f"{col}{item['row']}", "values": [[item["value"]]]} for item in buffer]
    ws.batch_update(data)
    buffer.clear()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Verify district homepages via Brave search")
    parser.add_argument("--start-row", type=int, default=2)
    parser.add_argument("--end-row",   type=int, default=None)
    args = parser.parse_args()

    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    log_path = logs_dir / f"verify_homepages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info("Log: %s", log_path)

    if not BRAVE_API_KEY:
        logger.error("BRAVE_API_KEY not set in .env — exiting")
        sys.exit(1)

    gc = gspread.service_account(filename=GOOGLE_SHEETS_CREDS)
    ws = gc.open_by_key(SPREADSHEET_ID).sheet1

    # Write header to AL1 if missing
    col_al = col_letter(COL_RESULT)
    if not ws.cell(1, COL_RESULT).value:
        ws.update_cell(1, COL_RESULT, "Suggested_Homepage")
        logger.info("Wrote header to %s1", col_al)

    all_values = ws.get_all_values()
    end_row    = args.end_row or len(all_values)

    batch: list = []
    processed = skipped = errors = 0

    for sheet_row in range(args.start_row, end_row + 1):
        row_data = all_values[sheet_row - 1]

        def get(col):
            idx = col - 1
            return row_data[idx].strip() if idx < len(row_data) else ""

        district_name = get(COL_DIST_NAME)
        homepage_url  = get(COL_HOMEPAGE)
        existing      = get(COL_RESULT)

        if not district_name:
            continue

        if existing:
            skipped += 1
            continue

        time.sleep(RATE_LIMIT_SLEEP)

        try:
            found_url = search_homepage(district_name)
        except RuntimeError as exc:
            msg = str(exc)
            if "AUTH" in msg or "RATE_LIMIT" in msg or "QUOTA" in msg:
                logger.error("Row %d | Brave budget/auth error (%s) — stopping", sheet_row, msg)
                break
            logger.warning("Row %d | %s | search error: %s", sheet_row, district_name, exc)
            batch.append({"row": sheet_row, "value": "error"})
            errors += 1
            if len(batch) >= BATCH_SIZE:
                flush(ws, batch, col_al)
            continue

        if not found_url:
            result = "no result"
        else:
            found_domain    = normalize_domain(found_url)
            existing_domain = normalize_domain(homepage_url)
            if found_domain and found_domain == existing_domain:
                result = "✓ match"
            else:
                result = found_url

        status = "MATCH" if result == "✓ match" else ("NO RESULT" if result == "no result" else f"MISMATCH -> {result}")
        logger.info("Row %d | %-30s | col_A=%-25s | %s",
                    sheet_row, district_name, normalize_domain(homepage_url) or "(none)", status)

        batch.append({"row": sheet_row, "value": result})
        processed += 1

        if len(batch) >= BATCH_SIZE:
            flush(ws, batch, col_al)

    flush(ws, batch, col_al)
    logger.info("Done. Processed=%d  Skipped=%d  Errors=%d", processed, skipped, errors)


if __name__ == "__main__":
    main()
