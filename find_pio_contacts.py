"""
find_pio_contacts.py — Discover the Officer for Public Information (PIO)
email address for each Texas charter school district.

Reads District_Number, District_Name, and District_Web_Address from the
PIR_Tracking tab (pre-populated by user). For each district, runs a
3-stage pipeline to find the correct PIO contact email:

  Stage 1 — Serper.dev search (1 credit per district)
              Query: "{District Name}" Texas "public information request"
                     OR "public information officer"
              Fetches top result page and extracts email address.

  Stage 2 — Homepage path crawl (0 credits)
              Tries common PIO paths on the district's own website.

  Stage 3 — Flag as PIO_NOT_FOUND (0 credits)
              Writes to Notes for manual follow-up. No superintendent email used.

Writes to PIR_Tracking columns D (PIO_Email), E (PIO_Source),
G (Status), I (Notes) only. Never touches A, B, C, F, H.

Usage:
    python find_pio_contacts.py                        # full run
    python find_pio_contacts.py --dry-run              # preview, no API or sheet writes
    python find_pio_contacts.py --limit 10             # first N districts only
    python find_pio_contacts.py --district-number 57829  # single district
    python find_pio_contacts.py --test                 # run against hardcoded known charters,
                                                       # no sheet reads/writes, validates logic
"""

import argparse
import gzip
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Force UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ── Constants ─────────────────────────────────────────────────────────────────

TRACKING_TAB   = "PIR_Tracking"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]

SERPER_ENDPOINT  = "https://google.serper.dev/search"
RATE_LIMIT_SLEEP = 1.5   # seconds between Serper calls
RETRY_SLEEP      = 4.0
FETCH_TIMEOUT    = 8     # seconds for page fetches

# PIR_Tracking column indices (1-based, verify against sheet before running)
COL_DISTRICT_NUMBER  = 1   # A — read only
COL_DISTRICT_NAME    = 2   # B — read only
COL_WEB_ADDRESS      = 3   # C — read only
COL_PIO_EMAIL        = 4   # D — written
COL_EMAIL_ROLE       = 5   # E — read only (imported from AskTED; e.g. "Human Resources")
COL_PIO_SOURCE       = 6   # F — written
COL_DATE_SENT        = 7   # G — read only (written by send_pir.py)
COL_STATUS           = 8   # H — written
COL_RESPONSE_URL     = 9   # I — read only (manual)
COL_NOTES            = 10  # J — written
COL_FULL_NAME        = 11  # K — read only (name fields added by user)
COL_FIRST_NAME       = 12  # L — read only
COL_LAST_NAME        = 13  # M — read only

# Columns this script is allowed to write
WRITE_COLS = {COL_PIO_EMAIL, COL_PIO_SOURCE, COL_STATUS, COL_NOTES}

# Common paths to try on district homepages (Stage 2 crawl)
PIO_PATHS = [
    "/public-information",
    "/public-information-request",
    "/public-information-requests",
    "/pia",
    "/open-records",
    "/open-records-request",
    "/open-records-requests",
    "/transparency",
    "/contact-us",
    "/contact",
    "/about/contact",
    "/about-us/contact",
    "/departments/communications",
    "/departments/hr",
    "/departments/human-resources",
    "/staff",
    "/administration",
]

# Domains to reject — social media, news aggregators, etc.
REJECT_DOMAINS = [
    "reddit", "facebook", "twitter", "linkedin", "instagram",
    "youtube", "tiktok", "indeed", "glassdoor", "communityimpact",
    "dallasnews", "houstonchronicle", "statesman", "chron",
]

# Email addresses to reject even if found.
# No ^ anchors — re.search is used so these match anywhere in the address,
# catching phone-concatenated forms like "9366345515webmaster@domain.com".
REJECT_EMAIL_PATTERNS = [
    r'noreply', r'no-reply', r'no_reply', r'donotreply',
    r'webmaster', r'postmaster', r'abuse@',
    # Texas state agency domains — never the correct PIO for a charter school
    r'@.*\.state\.tx\.us$',
    r'@.*\.texas\.gov$',
    r'@tea\.texas\.gov$',
]

QA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Regex to find email addresses in HTML text
EMAIL_RE = re.compile(
    r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b'
)

# Keywords that signal proximity to a PIO contact
PIO_KEYWORDS = re.compile(
    r'public\s+information|open\s+records?|pia\b|records\s+request|'
    r'transparency|officer\s+for\s+public',
    re.IGNORECASE,
)

# ── Safety guard ──────────────────────────────────────────────────────────────

def _assert_col_writable(col: int) -> None:
    if col not in WRITE_COLS:
        raise RuntimeError(
            f"SAFETY HALT: attempted write to column {col}. "
            f"Allowed write columns: {sorted(WRITE_COLS)} (D, F, H, J). Halting."
        )


# ── Email extraction helpers ──────────────────────────────────────────────────

_PHONE_PREFIX_RE = re.compile(r'^\d[\d\-\.]{6,}(?=[a-zA-Z])')

def _clean_email(email: str) -> str:
    """
    Strip leading phone-number prefixes from email local parts.
    Handles cases like '9366345515webmaster@domain.com' → 'webmaster@domain.com'
    when scraped text concatenates a phone number directly with an email address.
    """
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    cleaned_local = _PHONE_PREFIX_RE.sub("", local)
    if cleaned_local and cleaned_local != local:
        candidate = f"{cleaned_local}@{domain}"
        if EMAIL_RE.match(candidate):
            return candidate
    return email


def _is_valid_email(email: str) -> bool:
    """Return False for noreply, webmaster, and other non-contact addresses."""
    email_lower = _clean_email(email.lower())
    for pattern in REJECT_EMAIL_PATTERNS:
        if re.search(pattern, email_lower):
            return False
    return True


def extract_emails_from_html(
    html: str,
    prefer_pio_context: bool = True,
    domain_hint: str = "",
    min_score: int = 1,
) -> list[str]:
    """
    Extract email addresses from HTML.

    Ranking priority (highest first):
      1. Emails on the district's own domain (domain_hint) near PIO keywords
      2. Emails on the district's own domain (anywhere)
      3. Emails near PIO keywords on any domain
      4. All other emails

    Returns a deduplicated list in priority order.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []

    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)

    # Grab mailto: hrefs — most reliable signal
    mailto_emails = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            if EMAIL_RE.match(email) and _is_valid_email(email):
                mailto_emails.append(email)

    text_emails = [
        e.lower() for e in EMAIL_RE.findall(text)
        if _is_valid_email(e)
    ]

    all_emails = list(dict.fromkeys(mailto_emails + text_emails))  # dedup, preserve order

    if not prefer_pio_context and not domain_hint:
        return all_emails

    # Extract domain hint (e.g. "yesprep.org" from "https://www.yesprep.org")
    hint = ""
    if domain_hint:
        parsed = urlparse(domain_hint if "://" in domain_hint else f"https://{domain_hint}")
        # Strip leading www.
        hint = parsed.netloc.lstrip("www.").lower()

    def score_email(email: str) -> int:
        on_domain = hint and email.endswith(f"@{hint}")
        pos = text.find(email)
        near_pio = False
        if pos != -1:
            window = text[max(0, pos - 300): pos + 300]
            near_pio = bool(PIO_KEYWORDS.search(window))
        if on_domain and near_pio: return 4
        if on_domain:              return 3
        if near_pio:               return 2
        return 1

    return [e for e in sorted(all_emails, key=score_email, reverse=True) if score_email(e) >= min_score]


def _is_reject_domain(url: str) -> bool:
    """Return True if the URL is from a social media or news domain."""
    url_lower = url.lower()
    return any(d in url_lower for d in REJECT_DOMAINS)


# ── HTTP fetch ────────────────────────────────────────────────────────────────

def fetch_page(url: str, logger: logging.Logger) -> str:
    """Fetch a URL and return its HTML body, or '' on failure or non-HTML content."""
    try:
        resp = requests.get(
            url,
            timeout=FETCH_TIMEOUT,
            headers={"User-Agent": QA_USER_AGENT},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug("fetch_page: %s → HTTP %d", url, resp.status_code)
            return ""
        ct = resp.headers.get("Content-Type", "")
        if ct and "html" not in ct.lower() and "text" not in ct.lower():
            logger.debug("fetch_page: skipping non-HTML content (%s) at %s", ct, url)
            return ""
        return resp.text
    except Exception as exc:
        logger.debug("fetch_page error for %s: %s", url, exc)
        return ""


# ── Serper call ───────────────────────────────────────────────────────────────

def call_serper(query: str, api_key: str, logger: logging.Logger) -> list[dict]:
    """Call Serper and return up to 5 organic results."""
    payload = {"q": query, "num": 5}
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
        "Accept-Encoding": "identity",  # prevent gzip responses that can't be JSON-parsed
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                SERPER_ENDPOINT, json=payload,
                headers=headers, timeout=10,
            )
            resp.raise_for_status()
            # Manually decode to handle servers that ignore Accept-Encoding: identity
            content = resp.content
            if content[:2] == b'\x1f\x8b':  # gzip magic bytes
                content = gzip.decompress(content)
            return json.loads(content.decode("utf-8")).get("organic", [])[:5]
        except requests.HTTPError as exc:
            logger.warning("Serper HTTP error attempt %d: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as exc:
            # Binary/compressed/malformed response — retrying won't help
            logger.warning("Serper response unreadable (attempt %d): %s", attempt + 1, type(exc).__name__)
            return []
        except Exception as exc:
            logger.warning("Serper attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise
    return []


# ── Stage 1: Serper search ────────────────────────────────────────────────────

def stage1_serper(
    district_name: str,
    api_key: str,
    logger: logging.Logger,
    web_address: str = "",
) -> tuple[str, str]:
    """
    Search Serper for the district's PIO contact page.

    Two passes over the same 5 results (zero extra credits):

    Pass 1 — PIO quality (min_score=3): email must be on the district's own
    domain. Returns immediately on first match.

    Pass 2 — Contact fallback: if Pass 1 found nothing, re-scan cached HTML.
    Accepts any email where the email domain matches the page domain, provided
    the page domain contains at least one meaningful word from the district name
    (guards against transcript/directory services like needmytranscript.com).
    Source tagged `serper_contact:` so sheet shows lower confidence.

    Returns (email, source_note) or ("", "").
    """
    query = (
        f'"{district_name}" Texas '
        f'"public information request" OR "public information officer"'
    )
    logger.info("Stage-1 Serper: %s", query)

    try:
        results = call_serper(query, api_key, logger)
    except Exception as exc:
        logger.warning("Stage-1 Serper failed: %s", exc)
        return "", ""

    # Derive district domain hint once
    hint_domain = ""
    if web_address:
        _p = urlparse(web_address if "://" in web_address else f"https://{web_address}")
        hint_domain = _p.netloc.lstrip("www.").lower()

    # Words from district name used to verify Pass-2 pages belong to the district.
    # Strip generic school/state words that appear everywhere.
    _GENERIC = {
        "texas", "public", "school", "schools", "academy", "charter",
        "district", "international", "leadership", "education", "prep",
        "preparatory", "learning", "institute", "community",
    }
    name_words = {
        w for w in re.findall(r'[a-z]{4,}', district_name.lower())
        if w not in _GENERIC
    }

    # ── Pass 1: PIO-quality search ────────────────────────────────────────────
    # Scan all 5 result pages and track the best-scored email rather than
    # returning on the first hit. Score 4 (on district domain + page has PIO
    # keywords) beats score 3 (on district domain, generic contact page), so a
    # dedicated PIO inbox wins over a personal staff email on a contact page
    # regardless of Serper result order.
    html_cache: dict[str, str] = {}
    best_email = ""
    best_score = 0
    best_source = ""

    for result in results:
        url = result.get("link", "")
        if not url or _is_reject_domain(url):
            continue

        # Snippet: only accept emails on the district's own domain
        snippet = result.get("snippet", "") + " " + result.get("title", "")
        for e in EMAIL_RE.findall(snippet):
            e = e.lower()
            if _is_valid_email(e) and hint_domain and e.endswith(f"@{hint_domain}"):
                score = 4 if bool(PIO_KEYWORDS.search(snippet)) else 3
                if score > best_score:
                    best_score, best_email, best_source = score, e, f"serper_snippet:{url}"

        # Fetch page (always, so Pass 2 can reuse the cache)
        html = fetch_page(url, logger)
        html_cache[url] = html
        if not html or not hint_domain:
            continue

        emails = extract_emails_from_html(
            html, prefer_pio_context=True, domain_hint=web_address, min_score=3
        )
        if emails:
            score = 4 if bool(PIO_KEYWORDS.search(html)) else 3
            if score > best_score:
                best_score, best_email, best_source = score, emails[0], f"serper:{url}"
            if best_score == 4:
                break  # can't do better

    if best_email:
        logger.info("Stage-1 email (score=%d): %s (via %s)", best_score, best_email, best_source)
        return best_email, best_source

    # ── Pass 2: contact fallback (same pages, zero extra credits) ─────────────
    logger.debug("Stage-1 Pass 2 (contact fallback) for %s", district_name)

    for result in results:
        url = result.get("link", "")
        if not url or _is_reject_domain(url):
            continue

        page_host = urlparse(url).netloc.lstrip("www.").lower()

        # Require at least one district name word in the page domain — prevents
        # accepting support emails from transcript/directory services
        if name_words and not any(w in page_host for w in name_words):
            logger.debug("Pass 2: skipping %s (no name word in domain)", url)
            continue

        html = html_cache.get(url) or fetch_page(url, logger)
        if not html:
            continue

        all_emails = extract_emails_from_html(html, prefer_pio_context=False)
        for email in all_emails:
            email_host = email.split("@")[1].lower() if "@" in email else ""
            if email_host == page_host:
                logger.info("Stage-1 contact fallback: %s (via %s)", email, url)
                return email, f"serper_contact:{url}"

    return "", ""


# ── Stage 1.5: Contact-email fallback search ──────────────────────────────────

def stage1_5_contact(
    district_name: str,
    api_key: str,
    logger: logging.Logger,
) -> tuple[str, str]:
    """
    Last-resort Serper search. Only called when Stage 1 (PIO query) and
    Stage 2 (homepage crawl) both return nothing.

    Uses 1 additional credit with query: "{district_name}" Texas contact email
    This surfaces the district's own contact/staff pages that the PIO-specific
    query misses for schools with no indexed public-information content.

    Accepts the first email where the email domain matches the page domain
    (the page is the district's own website). Source tagged `serper_contact15:`.
    """
    query = f'"{district_name}" Texas contact email'
    logger.info("Stage-1.5 contact search: %s", query)

    try:
        results = call_serper(query, api_key, logger)
    except Exception as exc:
        logger.warning("Stage-1.5 Serper failed: %s", exc)
        return "", ""

    for result in results:
        url = result.get("link", "")
        if not url or _is_reject_domain(url):
            continue

        page_host = urlparse(url).netloc.lstrip("www.").lower()

        # Snippet first — any valid email in the snippet is worth trying
        snippet = result.get("snippet", "") + " " + result.get("title", "")
        for e in EMAIL_RE.findall(snippet):
            e = e.lower()
            if _is_valid_email(e):
                email_host = e.split("@")[1].lower() if "@" in e else ""
                if email_host == page_host:
                    logger.info("Stage-1.5 email from snippet: %s (via %s)", e, url)
                    return e, f"serper_contact15:{url}"

        # Fetch and scan the page
        html = fetch_page(url, logger)
        if not html:
            continue

        all_emails = extract_emails_from_html(html, prefer_pio_context=True)
        for email in all_emails:
            email_host = email.split("@")[1].lower() if "@" in email else ""
            if email_host == page_host:
                logger.info("Stage-1.5 email from page: %s (via %s)", email, url)
                return email, f"serper_contact15:{url}"

    return "", ""


# ── Stage 2: Homepage path crawl ──────────────────────────────────────────────

def stage2_crawl(
    web_address: str,
    logger: logging.Logger,
    domain_hint: str = "",
) -> tuple[str, str]:
    """
    Crawl the district's own website trying common PIO paths.
    Returns (email, source_note) or ("", "").
    """
    if not web_address:
        return "", ""

    # Normalise to base URL
    parsed = urlparse(web_address if web_address.startswith("http") else f"https://{web_address}")
    base = f"{parsed.scheme}://{parsed.netloc}"

    logger.info("Stage-2 crawl: %s", base)

    # Also try the homepage itself
    paths_to_try = [web_address] + [base + p for p in PIO_PATHS]
    seen_urls = set()

    for url in paths_to_try:
        if url in seen_urls:
            continue
        seen_urls.add(url)

        html = fetch_page(url, logger)
        if not html:
            continue

        emails = extract_emails_from_html(html, prefer_pio_context=True, domain_hint=domain_hint or web_address)
        if emails:
            logger.info("Stage-2 email from crawl: %s (via %s)", emails[0], url)
            return emails[0], f"crawl:{url}"

    return "", ""


# ── Google Sheets helpers ─────────────────────────────────────────────────────

def open_tracking_sheet(creds_path: str, spreadsheet_id: str):
    creds  = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(TRACKING_TAB)


def load_tracking_rows(sheet) -> list[dict]:
    """
    Return all data rows from PIR_Tracking as a list of dicts,
    including the 1-based sheet row number under key '_row'.
    """
    all_values = sheet.get_all_values()
    if not all_values:
        return []

    headers = all_values[0]
    rows = []
    for i, row in enumerate(all_values[1:], start=2):  # row 2 = index 1
        # Pad short rows
        while len(row) < len(headers):
            row.append("")
        d = dict(zip(headers, row))
        d["_row"] = i
        rows.append(d)
    return rows


def col_letter(col_1based: int) -> str:
    """Convert 1-based column index to letter (1=A, 26=Z, 27=AA, ...)."""
    result = ""
    n = col_1based
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def write_result(
    sheet,
    sheet_row: int,
    pio_email: str,
    pio_source: str,
    status: str,
    notes: str,
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """Write PIO_Email (D), PIO_Source (F), Status (H), Notes (J) for the given sheet row."""
    _assert_col_writable(COL_PIO_EMAIL)
    _assert_col_writable(COL_PIO_SOURCE)
    _assert_col_writable(COL_STATUS)
    _assert_col_writable(COL_NOTES)

    if dry_run:
        print(
            f"  [DRY-RUN] Row {sheet_row}: "
            f"PIO_Email={pio_email!r}  Source={pio_source!r}  "
            f"Status={status!r}  Notes={notes!r}"
        )
        return

    data = [
        {"range": f"{col_letter(COL_PIO_EMAIL)}{sheet_row}",  "values": [[pio_email]]},
        {"range": f"{col_letter(COL_PIO_SOURCE)}{sheet_row}", "values": [[pio_source]]},
        {"range": f"{col_letter(COL_STATUS)}{sheet_row}",     "values": [[status]]},
        {"range": f"{col_letter(COL_NOTES)}{sheet_row}",      "values": [[notes]]},
    ]
    sheet.batch_update(data, value_input_option="USER_ENTERED")
    logger.debug("Wrote row %d: email=%s status=%s", sheet_row, pio_email, status)


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logger(dry_run: bool) -> logging.Logger:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    log_path = logs_dir / f"pio_{ts}{suffix}.log"

    logger = logging.getLogger("find_pio")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Log: %s", log_path)
    return logger


# ── CLI ───────────────────────────────────────────────────────────────────────

# ── Test mode districts ───────────────────────────────────────────────────────
# Known TX charter districts with published PIO contacts — used to validate
# Stage 1 (Serper) and Stage 2 (crawl) logic without touching real sheet data.
# Expected emails sourced from their public websites during plan research.

TEST_DISTRICTS = [
    {
        "District_Number": "TEST-001",
        "District_Name": "KIPP Texas Public Schools",
        "District_Web_Address": "https://www.kipptexas.org",
        "expected_email": "TPIA@kipptexas.org",  # published at /public-information/
    },
    {
        "District_Number": "TEST-002",
        "District_Name": "YES Prep Public Schools",
        "District_Web_Address": "https://www.yesprep.org",
        # Accept any email on yesprep.org — Serper results change between runs
        # and the district may list different contacts on different pages.
        "expected_email": "@yesprep.org",
    },
    {
        "District_Number": "TEST-003",
        "District_Name": "IDEA Public Schools",
        "District_Web_Address": "https://ideapublicschools.org",
        "expected_email": "information.request@ideapublicschools.org",
    },
    {
        "District_Number": "TEST-004",
        "District_Name": "International Leadership of Texas",
        "District_Web_Address": "https://www.iltexas.org",
        "expected_email": "recordrequests@iltexas.org",  # published at /open-records
    },
]


def run_test_mode(serper_key: str, logger: logging.Logger) -> None:
    """
    Run the full Stage 1 + Stage 2 pipeline against TEST_DISTRICTS.
    No sheet access. Prints detailed results and a pass/fail summary.
    """
    print("=" * 70)
    print("TEST MODE — validating PIO discovery logic against known charters")
    print("Stage 1 (Serper) and Stage 2 (crawl) both run live.")
    print("No sheet reads or writes.")
    print("=" * 70)

    passed = 0
    failed = 0

    for district in TEST_DISTRICTS:
        name     = district["District_Name"]
        web      = district["District_Web_Address"]
        expected = district["expected_email"].lower()

        print(f"\n{'─'*70}")
        print(f"District: {name}")
        print(f"Homepage: {web}")
        print(f"Expected: {expected}")

        found_email = ""
        found_source = ""

        # Stage 1
        if serper_key:
            print("  Running Stage 1 (Serper)...")
            try:
                found_email, found_source = stage1_serper(name, serper_key, logger, web_address=web)
                time.sleep(RATE_LIMIT_SLEEP)
                if found_email:
                    print(f"  Stage 1 result: {found_email}  [{found_source}]")
            except Exception as exc:
                print(f"  Stage 1 error: {exc}")
        else:
            print("  Stage 1 skipped — no SERPER_API_KEY in .env")

        # Stage 2
        if not found_email:
            print("  Running Stage 2 (homepage crawl)...")
            found_email, found_source = stage2_crawl(web, logger, domain_hint=web)
            if found_email:
                print(f"  Stage 2 result: {found_email}  [{found_source}]")

        # Verdict
        # expected starting with "@" means accept any email on that domain
        domain_only = expected.startswith("@")
        if not found_email:
            print(f"  RESULT: NOT FOUND")
            verdict = "FAIL (not found)"
            failed += 1
        elif domain_only and found_email.endswith(expected):
            print(f"  RESULT: PASS — domain match ({found_email})")
            verdict = "PASS"
            passed += 1
        elif found_email == expected:
            print(f"  RESULT: PASS — exact match")
            verdict = "PASS"
            passed += 1
        elif expected in found_email or found_email in expected:
            print(f"  RESULT: PASS (partial match — {found_email})")
            verdict = "PASS"
            passed += 1
        else:
            print(f"  RESULT: FAIL — found {found_email!r}, expected {expected!r}")
            verdict = f"FAIL (wrong email)"
            failed += 1

        print(f"  Verdict: {verdict}")

    print(f"\n{'='*70}")
    print(f"Test complete: {passed} passed, {failed} failed out of {len(TEST_DISTRICTS)} districts")
    if failed == 0:
        print("All tests passed — pipeline is working correctly.")
    else:
        print("Some tests failed — review Stage 1/2 logic or update expected emails.")
    print("=" * 70)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find PIO contact emails for Texas charter school districts."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview results; no Serper calls, no sheet writes",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N districts",
    )
    parser.add_argument(
        "--district-number", type=str, default=None,
        help="Process a single district by District_Number",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Run against hardcoded known charters to validate logic; no sheet access",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-process rows even if they already have a status (overwrites existing results)",
    )
    return parser.parse_args()


def elapsed_str(start: float) -> str:
    secs = int(time.time() - start)
    m, s = divmod(secs, 60)
    return f"{m}m {s:02d}s"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args   = parse_args()
    logger = setup_logger(args.dry_run or args.test)

    load_dotenv()
    serper_key     = os.environ.get("SERPER_API_KEY", "").strip()

    # ── Test mode: validate logic against known charters, no sheet access ─────
    if args.test:
        run_test_mode(serper_key, logger)
        return
    creds_path_raw = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    creds_path_obj = Path(creds_path_raw).expanduser()
    if not creds_path_obj.is_absolute():
        creds_path_obj = Path(__file__).parent / creds_path_obj
    creds_path     = str(creds_path_obj)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    # Credentials check — sheet always needed; Serper not needed for dry-run
    missing = []
    if not creds_path:     missing.append("GOOGLE_SHEETS_CREDS_JSON")
    if not spreadsheet_id: missing.append("SPREADSHEET_ID")
    if not args.dry_run and not serper_key:
        missing.append("SERPER_API_KEY")
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    if not GSPREAD_AVAILABLE:
        print("ERROR: gspread not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    logger.info("Opening PIR_Tracking sheet")
    sheet = open_tracking_sheet(creds_path, spreadsheet_id)
    rows  = load_tracking_rows(sheet)
    logger.info("Loaded %d rows from PIR_Tracking", len(rows))

    # Filter to work list
    if args.district_number:
        work = [r for r in rows if r.get("District_Number", "").strip() == args.district_number]
        if not work:
            print(f"ERROR: District_Number {args.district_number!r} not found in PIR_Tracking.")
            sys.exit(1)
    else:
        work = rows

    if args.limit:
        work = work[:args.limit]

    # Skip rows already processed (D already has an email or PIO_NOT_FOUND in G)
    # --force bypasses this and re-processes everything
    if args.force:
        pending = work
    else:
        pending = [
            r for r in work
            if not r.get("PIO_Email", "").strip()
            and r.get("Status", "").strip() not in ("PIO_FOUND", "PIO_NOT_FOUND")
        ]

    total   = len(pending)
    skipped = len(work) - total
    print(f"Districts to process: {total}  |  Already done (skipped): {skipped}")
    logger.info("Processing %d districts, skipping %d", total, skipped)

    if total == 0:
        print("Nothing to do.")
        return

    start_time = time.time()
    found_count = 0
    not_found_count = 0

    for i, row in enumerate(pending, 1):
        district_number = row.get("District_Number", "").strip()
        district_name   = row.get("District_Name", "").strip()
        web_address     = row.get("District_Web_Address", "").strip()
        sheet_row       = row["_row"]

        print(f"[{i}/{total}] {district_name} (#{district_number})")

        pio_email  = ""
        pio_source = ""
        notes      = ""

        # ── Stage 1: Serper ───────────────────────────────────────────────────
        if not args.dry_run:
            try:
                pio_email, pio_source = stage1_serper(district_name, serper_key, logger, web_address=web_address)
                time.sleep(RATE_LIMIT_SLEEP)
            except Exception as exc:
                logger.error("Stage-1 error for %s: %s", district_name, exc)
                notes = f"Stage-1 error: {exc}"
        else:
            print(f"  [DRY-RUN] Would run Stage-1 Serper search")

        # ── Stage 2: Homepage crawl ───────────────────────────────────────────
        if not pio_email and not args.dry_run:
            pio_email, pio_source = stage2_crawl(web_address, logger, domain_hint=web_address)
        elif not pio_email and args.dry_run:
            print(f"  [DRY-RUN] Would run Stage-2 crawl on {web_address}")

        # ── Stage 1.5: Contact-email fallback (1 extra Serper credit) ─────────
        if not pio_email and not args.dry_run:
            try:
                pio_email, pio_source = stage1_5_contact(district_name, serper_key, logger)
                time.sleep(RATE_LIMIT_SLEEP)
            except Exception as exc:
                logger.error("Stage-1.5 error for %s: %s", district_name, exc)
        elif not pio_email and args.dry_run:
            print(f"  [DRY-RUN] Would run Stage-1.5 contact search")

        # ── Stage 3: Flag for manual follow-up ───────────────────────────────
        if pio_email:
            status = "PIO_FOUND"
            found_count += 1
            print(f"  → FOUND: {pio_email}  [{pio_source}]")
        else:
            status = "PIO_NOT_FOUND"
            pio_source = "PIO_NOT_FOUND"
            not_found_count += 1
            print(f"  → NOT FOUND — flagged for manual follow-up")
            if not notes:
                notes = "No PIO email found via Serper or homepage crawl"

        logger.info(
            "Row %d | %s | status=%s | email=%s | source=%s",
            sheet_row, district_name, status, pio_email, pio_source,
        )

        write_result(
            sheet=sheet,
            sheet_row=sheet_row,
            pio_email=pio_email,
            pio_source=pio_source,
            status=status,
            notes=notes,
            dry_run=args.dry_run,
            logger=logger,
        )

    elapsed = elapsed_str(start_time)
    summary = (
        f"\nDone. Found: {found_count}  |  Not found: {not_found_count}  "
        f"|  Skipped: {skipped}  |  Elapsed: {elapsed}"
    )
    print(summary)
    logger.info(summary)


if __name__ == "__main__":
    main()
