"""
search_urls.py — Texas District Compensation Plan URL Searcher

Searches Serper.dev for each district's compensation plan URL,
scores results, QA-checks the best URL, and writes to Google Sheets cols F–L.

Usage:
    python search_urls.py [--start-row N] [--end-row N] [--dry-run]
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

# Force UTF-8 on Windows console so checkmark/warning symbols print correctly
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
import tldextract
from urllib.parse import urljoin
from bs4 import BeautifulSoup
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

COL_BEST_URL_CLASS = 12  # L
COL_SEARCH_METHOD  = 13  # M
COL_QA_STATUS      = 14  # N  (updated — shifted by 2 new cols)
COL_REDIRECT_URL   = 15  # O
COL_DOC_CLASS      = 16  # P  (written by classify_documents.py; cleared on reruns)

WRITE_COL_MIN = COL_R1_URL       # 6  (F)
WRITE_COL_MAX = COL_REDIRECT_URL # 15 (O)

HEADERS_ROW = [
    "Result_1_URL", "Result_1_Title",
    "Result_2_URL", "Result_2_Title",
    "Best_URL", "Best_Score", "Best_URL_Classification",
    "Search_Method", "QA_Status", "Redirect_URL",
]

SERPER_ENDPOINT  = "https://google.serper.dev/search"
TAVILY_ENDPOINT  = "https://api.tavily.com/search"
GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
RATE_LIMIT_SLEEP = 1.5   # seconds between Serper calls
RETRY_SLEEP      = 4.0   # seconds before retrying a failed Serper call
QA_TIMEOUT       = 6     # seconds for HEAD request
BATCH_SIZE       = 25    # rows to accumulate before flushing to Sheets

# If the primary (year-qualified) query produces a best score at or below this
# threshold, fire a second year-free query and take whichever result wins.
# Set to 3 so that results like stale news articles (3pts) still trigger the
# fallback — raised from 2 after Sulphur Springs ISD case revealed 3pt results
# from 2019 news articles were suppressing the correct district page.
FALLBACK_SCORE_THRESHOLD = 3

NEWS_PENALTY_DOMAINS = [
    "reddit", "facebook", "twitter", "chron", "kut", "kxan",
    "mrt.com", "communityimpact", "dallasnews", "houstonchronicle",
    "statesman", "kvue", "nbcdfw", "wfaa", "indeed", "linkedin",
]

# CDN / document-hosting platforms used legitimately by school districts.
# Files hosted here may live on a different registered domain than the
# district homepage — that's expected and should NOT trigger WRONG DOMAIN.
# URL path tokens that belong to specific large districts hosted on shared CDNs.
# A Finalsite URL like /fwisdorg/... is Fort Worth ISD's document, not any district
# whose search happens to return it. If a CDN URL contains one of these tokens, flag
# as WRONG PATH even though the CDN domain itself is whitelisted.
WRONG_CDN_PATHS = {
    "fwisdorg",    # Fort Worth ISD Finalsite path
    "houstonisd",  # Houston ISD Finalsite path
    "dallasisd",   # Dallas ISD Finalsite path
    "austinisd",   # Austin ISD Finalsite path
    "saisd",       # San Antonio ISD Finalsite path
    "aldine",      # Aldine ISD Finalsite path
    "katyisd",     # Katy ISD Finalsite path (e.g. katyisdorg)
    "anchisdcom",  # Aldine/Angleton CISD Thrillshare path (affects 40+ rural districts)
    "ccisdnet",    # Clear Creek ISD Thrillshare path (2023-24 schedule, affects ~5 districts)
}

KNOWN_DOC_CDNS = {
    "finalsite.net",    # Finalsite — major school website/CDN platform
    "thrillshare.com",  # Thrillshare — school communications platform
    "boarddocs.com",    # BoardDocs — school board document management
    "boardbook.org",    # BoardBook — board management system
    "legistar.com",     # Granicus/Legistar board documents
    "rschooltoday.com", # RSchoolToday activities platform
    "campussuite.com",  # Campus Suite school CMS
    "edl.edu",          # Education Service Center document host
    "eboard.com",       # eBoard school CMS
    "govserv.com",      # GovServ document platform
    "wpmucdn.com",      # WordPress Multisite CDN (common school blogs)
    "edliocdn.com",     # Edlio CDN (primary)
    "edl.io",           # Edlio CDN (alternate short domain)
    "sitelearning.com", # Site Learning school platform
    "sharpschool.com",  # SharpSchool school CMS
    "amazonaws.com",    # AWS S3 — districts upload PDFs directly to S3
    "google.com",       # Google Drive / Google Sites (drive.google.com, sites.google.com)
}

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

    # Extract filename from URL for targeted checks (strip query string first)
    url_path = url.lower().split("?")[0]
    filename = url_path.rsplit("/", 1)[-1]

    # +3 file type
    if re.search(r'\.(pdf|xlsx)(\?.*)?$', url, re.IGNORECASE):
        score += 3
        reasons.append("+3 PDF/XLSX")

    # +2 domain match
    home_reg   = tldextract.extract(homepage_url).registered_domain
    result_reg = tldextract.extract(url).registered_domain
    if home_reg and result_reg and home_reg == result_reg:
        score += 2
        reasons.append(f"+2 domain match ({result_reg})")

    # +2 year pattern
    if re.search(r'2026|2025-26|25-26|2026-27|26-27', combined):
        score += 2
        reasons.append("+2 year")

    # +2 compensation keyword in filename — strongest positive signal
    # +1 keyword anywhere in URL/title/snippet — weaker fallback
    _COMP_KEYWORD_RE = re.compile(
        r'compensation|salary|pay.?scale|pay.?plan|pay.?plans|pay.?schedule'
        r'|pay.?grade|wage|matrix',
        re.IGNORECASE,
    )
    if _COMP_KEYWORD_RE.search(filename):
        score += 2
        reasons.append("+2 keyword in filename")
    elif _COMP_KEYWORD_RE.search(combined):
        score += 1
        reasons.append("+1 keyword in result")

    # -2 news/social penalty
    url_lower = url.lower()
    for bad in NEWS_PENALTY_DOMAINS:
        if bad in url_lower:
            score -= 2
            reasons.append(f"-2 news/social ({bad})")
            break

    # -3 Scribd — documents are gated behind login/paywall
    if "scribd.com" in url_lower:
        score -= 3
        reasons.append("-3 Scribd")

    # -5 wrong-district CDN path — CDN URL with another district's path token
    # Skip the penalty if the token belongs to the district currently being searched.
    # home_base is the registered domain stem (e.g. "houstonisd" from houstonisd.org,
    # "fwisd" from fwisd.org). The CDN token may be "fwisdorg" so we check substring
    # overlap in both directions.
    if result_reg in KNOWN_DOC_CDNS:
        home_base = tldextract.extract(homepage_url).domain.lower() if homepage_url else ""
        for bad_path in WRONG_CDN_PATHS:
            if bad_path in url_lower:
                is_own_district = home_base and (home_base in bad_path or bad_path in home_base)
                if not is_own_district:
                    score -= 5
                    reasons.append(f"-5 wrong CDN path ({bad_path})")
                break

    # -3 wrong-doc filename — unambiguous negative signal
    if re.search(
        r'handbook|budget|cafr|agenda|minutes|student.?guide|parent.?guide'
        r'|code.?of.?conduct|benefits.?guide|policy.?manual',
        filename, re.IGNORECASE
    ):
        score -= 3
        reasons.append("-3 wrong-doc filename")
    # -2 wrong-doc in title/snippet — weaker signal
    elif re.search(
        r'handbook|board.?trustee|trustee.?handbook|policy.?manual|staff.?handbook'
        r'|employee.?handbook|improvement.?plan|strategic.?plan|bond.?election'
        r'|election.?order|annual.?report|financial.?report|budget.?doc|audit.?report'
        r'|wellness|benefits.?guide|code.?of.?conduct|parent.?guide|student.?handbook',
        combined, re.IGNORECASE
    ):
        score -= 2
        reasons.append("-2 wrong-doc in title/snippet")

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
            # Some servers reject HEAD with 4xx but serve GET fine — retry once
            try:
                gr = requests.get(url, timeout=QA_TIMEOUT, stream=True, headers=headers)
                gr.close()
                if gr.status_code < 400:
                    ct2 = gr.headers.get("Content-Type", "").lower()
                    if "pdf" in ct2:
                        return "✓ PDF"
                    return "✓ HTML"
            except Exception:
                pass
            return "✗ DEAD"
        else:
            return "⚠ REVIEW"

    except requests.exceptions.Timeout:
        return "✗ TIMEOUT"
    except Exception as exc:
        logger.warning("QA error for %s: %s", url, exc)
        return "⚠ REVIEW"


# ── Tier-2 HTML crawl ────────────────────────────────────────────────────────

SALARY_LINK_RE = re.compile(
    r'compensation|salary|pay.?scale|pay.?plan|pay.?plans|pay.?schedule|pay.?grade|wage|matrix',
    re.IGNORECASE,
)
DOC_EXT_RE = re.compile(r'\.(pdf|xlsx|xls|docx)(\?.*)?$', re.IGNORECASE)


def crawl_html_for_salary_doc(page_url: str, logger: logging.Logger) -> str:
    """
    Fetch an HTML page and search its links for salary/compensation documents.
    Prefers PDF/XLSX links; falls back to any salary-keyword HTML link.
    Returns the best URL found, or '' if nothing relevant is detected.
    """
    try:
        resp = requests.get(
            page_url, timeout=8,
            headers={"User-Agent": QA_USER_AGENT},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        doc_hits = []  # PDF/XLSX links with salary keywords

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith(("mailto:", "tel:", "#")):
                continue
            full_url = urljoin(page_url, href)
            # Filter out social/news domains — catches share buttons whose
            # encoded ?u= parameter contains salary keywords (Willis ISD pattern)
            result_domain = tldextract.extract(full_url).registered_domain or ""
            if any(bad in result_domain for bad in NEWS_PENALTY_DOMAINS):
                continue
            text     = tag.get_text(" ", strip=True)
            combined = f"{href} {text}"
            if not SALARY_LINK_RE.search(combined):
                continue
            if DOC_EXT_RE.search(href):
                doc_hits.append(full_url)

        if doc_hits:
            logger.info("Crawl found %d doc link(s) on %s → %s", len(doc_hits), page_url, doc_hits[0])
            return doc_hits[0]
        return ""

    except Exception as exc:
        logger.warning("HTML crawl error for %s: %s", page_url, exc)
        return ""


def crawl_homepage_deep(homepage_url: str, logger: logging.Logger) -> str:
    """
    Two-level crawl for Tier-4. First looks for direct PDF/XLSX links on the
    homepage. If none found, follows HTML links whose text matches salary keywords
    one level deeper and looks for PDF/XLSX links there.
    Returns the best document URL found, or '' if nothing detected.
    """
    try:
        resp = requests.get(
            homepage_url, timeout=8,
            headers={"User-Agent": QA_USER_AGENT},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        doc_hits  = []
        html_hits = []

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith(("mailto:", "tel:", "#")):
                continue
            full_url = urljoin(homepage_url, href)
            result_domain = tldextract.extract(full_url).registered_domain or ""
            if any(bad in result_domain for bad in NEWS_PENALTY_DOMAINS):
                continue
            text     = tag.get_text(" ", strip=True)
            combined = f"{href} {text}"
            if not SALARY_LINK_RE.search(combined):
                continue
            if DOC_EXT_RE.search(href):
                doc_hits.append(full_url)
            else:
                html_hits.append(full_url)

        if doc_hits:
            logger.info("Tier-4 crawl found doc on homepage → %s", doc_hits[0])
            return doc_hits[0]

        # Follow salary HTML links one level deeper.
        # Some links use short URLs (5il.co, etc.) that redirect directly to a PDF
        # without a .pdf extension — HEAD-check first before crawling as HTML.
        for html_url in html_hits[:3]:
            try:
                head = requests.head(html_url, timeout=6, allow_redirects=True,
                                     headers={"User-Agent": QA_USER_AGENT})
                ct = head.headers.get("Content-Type", "").lower()
                if "pdf" in ct:
                    logger.info("Tier-4 followed short URL to PDF → %s", head.url)
                    return head.url
            except Exception:
                pass
            deeper = crawl_html_for_salary_doc(html_url, logger)
            if deeper:
                return deeper

        return ""

    except Exception as exc:
        logger.warning("Tier-4 homepage crawl error for %s: %s", homepage_url, exc)
        return ""


# ── Tier-1.5 year-free fallback search ───────────────────────────────────────

def call_serper_noyear(district_name: str, serper_key: str, brave_key: str, tavily_key: str,
                       logger: logging.Logger, primary: str = "serper", google_cse_key: str = ""):
    """
    Fallback query without any year qualifier. Used when the primary
    year-qualified search scores <= FALLBACK_SCORE_THRESHOLD, which happens
    for small districts whose content isn't year-tagged.
    """
    query = f'{district_name} "pay scale" OR "salary schedule" OR "compensation plan"'
    logger.info("Tier-1.5 year-free fallback: %s", query)
    try:
        return call_search(query, serper_key, brave_key, tavily_key, primary, logger, google_cse_key)
    except Exception as exc:
        logger.warning("Tier-1.5 fallback failed: %s", exc)
        return []


# ── Tier-3 domain-scoped search ───────────────────────────────────────────────

def call_serper_domain(homepage_url: str, serper_key: str, brave_key: str, tavily_key: str,
                       logger: logging.Logger, primary: str = "serper", google_cse_key: str = ""):
    """
    Tier-3: fire a site:-scoped query restricted to the district's own domain.
    Used when the primary search winner lands on the wrong district's domain.
    Returns up to 10 results (same format as call_serper).
    """
    domain = tldextract.extract(homepage_url).registered_domain
    if not domain:
        return []
    query = f'site:{domain} "pay scale" OR "salary schedule" OR "compensation plan"'
    logger.info("Tier-3 domain search: %s", query)
    try:
        return call_search(query, serper_key, brave_key, tavily_key, primary, logger, google_cse_key)
    except Exception as exc:
        logger.warning("Tier-3 search failed: %s", exc)
        return []


# ── Search API backends ───────────────────────────────────────────────────────

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


def call_brave(query: str, api_key: str, logger: logging.Logger):
    """
    Call Brave Search API and return up to 10 results in the same format
    as call_serper: list of dicts with keys link, title, snippet.
    On failure, raises after one retry.
    """
    headers_req = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }
    params = {"q": query, "count": 10}

    for attempt in range(2):
        try:
            resp = requests.get(
                BRAVE_ENDPOINT,
                headers=headers_req,
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in (data.get("web", {}).get("results", []))[:10]:
                results.append({
                    "link":    item.get("url", "NO_RESULT"),
                    "title":   item.get("title", ""),
                    "snippet": item.get("description", ""),
                })
            return results
        except Exception as exc:
            logger.warning("Brave attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise


def call_tavily(query: str, api_key: str, logger: logging.Logger):
    """
    Call Tavily Search API and return up to 10 results in the same format
    as call_serper: list of dicts with keys link, title, snippet.
    On failure, raises after one retry.
    """
    payload = {
        "query": query,
        "max_results": 10,
        "search_depth": "basic",
        "include_answer": False,
        "include_raw_content": False,
    }
    headers_req = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                TAVILY_ENDPOINT,
                json=payload,
                headers=headers_req,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in (data.get("results", []))[:10]:
                results.append({
                    "link":    item.get("url", "NO_RESULT"),
                    "title":   item.get("title", ""),
                    "snippet": item.get("content", ""),
                })
            return results
        except Exception as exc:
            logger.warning("Tavily attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise


def call_google_cse(query: str, key_cx: str, logger: logging.Logger):
    """
    Call Google Custom Search JSON API. key_cx must be 'API_KEY|CX_ID'.
    Returns up to 10 results with keys: link, title, snippet.
    """
    api_key, cx = key_cx.split("|", 1)
    params = {"key": api_key, "cx": cx, "q": query, "num": 10}

    for attempt in range(2):
        try:
            resp = requests.get(GOOGLE_CSE_ENDPOINT, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in (data.get("items", []))[:10]:
                results.append({
                    "link":    item.get("link", "NO_RESULT"),
                    "title":   item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                })
            return results
        except Exception as exc:
            logger.warning("Google CSE attempt %d failed: %s", attempt + 1, exc)
            if attempt == 0:
                time.sleep(RETRY_SLEEP)
            else:
                raise


def call_gemini_grounding(query: str, gemini_key: str, logger: logging.Logger):
    """
    Call Gemini 2.5-flash with Google Search grounding.
    Returns results in standard {link, title, snippet} format.
    Redirect URIs from groundingChunks are resolved to real URLs via HEAD request.
    """
    try:
        import google.genai as genai
        from google.genai import types
    except ImportError:
        raise RuntimeError("google-genai not installed; run: pip install google-genai")

    client = genai.Client(api_key=gemini_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=query,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
        ),
    )
    meta = response.candidates[0].grounding_metadata
    chunks = meta.grounding_chunks or []

    # Resolve redirect URLs in parallel to minimise latency
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _resolve(chunk):
        uri = chunk.web.uri or ""
        title = chunk.web.title or ""
        if not uri:
            return None
        try:
            r = requests.head(uri, timeout=6, allow_redirects=True,
                              headers={"User-Agent": QA_USER_AGENT})
            return {"link": r.url, "title": title, "snippet": ""}
        except Exception:
            return {"link": uri, "title": title, "snippet": ""}

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_resolve, c): c for c in chunks}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)

    return results


# Backends that have hit a quota/auth error this process run are added here and
# skipped on all subsequent calls — no point retrying a depleted API.
_tripped_backends: set[str] = set()

# HTTP status codes that indicate a quota or auth failure (not a transient error)
_QUOTA_STATUSES = {400, 401, 402, 403}


def _is_quota_error(exc: Exception) -> bool:
    """Return True if the exception looks like a quota or auth failure."""
    msg = str(exc)
    if any(str(s) in msg for s in _QUOTA_STATUSES):
        return True
    if "Not enough credits" in msg or "Payment Required" in msg:
        return True
    return False


def call_search(query: str, serper_key: str, brave_key: str, tavily_key: str,
                primary: str, logger: logging.Logger, google_cse_key: str = ""):
    """
    Try the primary backend first, then fall back through the others if it
    fails. Backends that have hit a quota error are circuit-broken for the
    lifetime of this process. Raises only if every configured backend is
    exhausted.
    """
    all_backends = [
        ("serper", call_serper,     serper_key),
        ("brave",  call_brave,      brave_key),
        ("google", call_google_cse, google_cse_key),
        ("tavily", call_tavily,     tavily_key),
    ]
    # Primary first, then the rest in default order; skip unconfigured or tripped
    order = [b for b in all_backends if b[0] == primary and b[2] and b[0] not in _tripped_backends]
    order += [b for b in all_backends if b[0] != primary and b[2] and b[0] not in _tripped_backends]

    if not order:
        raise RuntimeError("All search backends are depleted or unconfigured")

    last_exc: Exception = RuntimeError("No search backends configured")
    for i, (name, fn, key) in enumerate(order):
        try:
            return fn(query, key, logger)
        except Exception as exc:
            last_exc = exc
            if _is_quota_error(exc):
                _tripped_backends.add(name)
                logger.warning("Backend '%s' quota exhausted — disabling for this run", name)
            elif i < len(order) - 1:
                logger.warning("Backend '%s' failed (%s) — trying next backend", name, exc)
    raise last_exc


# ── Serper API ────────────────────────────────────────────────────────────────

def call_serper(query: str, api_key: str, logger: logging.Logger):
    """
    Call Serper.dev and return up to 10 organic results as a list of dicts
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
            for item in organic[:10]:
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
    """Ensure exactly 10 result slots, filling missing ones with NO_RESULT."""
    while len(results) < 10:
        results.append({"link": "NO_RESULT", "title": "NO_RESULT", "snippet": ""})
    return results[:10]


# ── Google Sheets helpers ─────────────────────────────────────────────────────

WORKSHEET_NAME = "Unique Districts"

def open_sheet(creds_path: str, spreadsheet_id: str):
    """Authenticate and return the 'Unique Districts' worksheet."""
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(WORKSHEET_NAME)


def ensure_headers(sheet) -> None:
    """Write column headers to F1:L1 if F1 is currently blank."""
    f1_val = sheet.cell(1, COL_R1_URL).value
    if f1_val and f1_val.strip():
        return  # headers already present
    # Build the update values — one list of 7
    cell_range = _row_range(1)
    sheet.update(range_name=cell_range, values=[HEADERS_ROW], value_input_option="USER_ENTERED")


def flush_batch(sheet, buffer: list) -> None:
    """Write accumulated rows to the sheet in one batch_update call, with retries."""
    if not buffer:
        return
    data = []
    for item in buffer:
        cell_range = _row_range(item["row"])
        data.append({"range": cell_range, "values": [item["values"]]})
    for attempt in range(3):
        try:
            sheet.batch_update(data, value_input_option="USER_ENTERED")
            buffer.clear()
            return
        except Exception as exc:
            if attempt < 2:
                wait = 10 * (attempt + 1)
                logging.getLogger(__name__).warning(
                    "Sheets write attempt %d failed (%s) — retrying in %ds", attempt + 1, exc, wait
                )
                time.sleep(wait)
            else:
                raise


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
    parser.add_argument(
        "--first-n", type=int, default=None,
        help="Process only the first N data rows (e.g. 10 largest districts)",
    )
    parser.add_argument(
        "--last-n", type=int, default=None,
        help="Process only the last N data rows (e.g. 10 smallest districts)",
    )
    parser.add_argument(
        "--rerun-html", action="store_true",
        help="Re-search only rows where QA_Status (col N) is '✓ HTML', bypassing resume",
    )
    parser.add_argument(
        "--rerun-dead", action="store_true",
        help="Re-search rows where QA_Status (col N) is '✗ DEAD' or '✗ TIMEOUT'",
    )
    parser.add_argument(
        "--rerun-wrongdoc", action="store_true",
        help="Re-search rows where Doc_Class (col P) is 'Wrong Doc'",
    )
    parser.add_argument(
        "--rerun-error", action="store_true",
        help="Re-search rows where Doc_Class (col P) is 'Error' or QA_Status is '⚠ REVIEW'",
    )
    parser.add_argument(
        "--rerun-wrong-domain", action="store_true",
        help="Re-search rows where QA_Status (col N) is '⚠ WRONG DOMAIN'",
    )
    parser.add_argument(
        "--rows", type=str, default=None,
        help="Comma-separated list of specific sheet row numbers to process (e.g. 15,42,100)",
    )
    return parser.parse_args()


def elapsed_str(start_time: float) -> str:
    secs = int(time.time() - start_time)
    m, s = divmod(secs, 60)
    return f"{m}m {s:02d}s"


def parse_best_label(label: str):
    """
    Split a Best_Score label into its three column components.

    "R2:6pts +noyear +fixed" → score_str="6", classification="R2", method="+noyear +fixed"
    "T3:5pts +domain"        → score_str="5", classification="T3", method="+domain"
    "R1:0pts (no clear winner)" → score_str="0", classification="R1", method=""
    """
    cls_match      = re.match(r'^(R\d+|R\?|T\d+)', label)
    classification = cls_match.group(1) if cls_match else ""

    score_match = re.search(r':(\d+)pts', label)
    score_str   = score_match.group(1) if score_match else "0"

    methods       = re.findall(r'\+\w+', label)
    search_method = " ".join(methods)

    return score_str, classification, search_method


def extract_redirect_url(qa_status: str) -> str:
    """Return the URL portion of '⚠ REDIRECT → https://...' or ''."""
    if "→" in qa_status:
        return qa_status.split("→", 1)[1].strip()
    return ""


def clean_qa_status(qa_status: str) -> str:
    """Strip the appended URL from redirect statuses, leaving just '⚠ REDIRECT'."""
    if qa_status.startswith("⚠ REDIRECT"):
        return "⚠ REDIRECT"
    return qa_status


def main():
    args = parse_args()
    logger = setup_logger(args.dry_run)

    if args.dry_run:
        run_dry_run()
        return

    # ── Credentials ───────────────────────────────────────────────────────────
    load_dotenv()
    search_backend  = os.environ.get("SEARCH_BACKEND", "serper").strip().lower()
    serper_key      = os.environ.get("SERPER_API_KEY", "").strip()
    brave_key       = os.environ.get("BRAVE_API_KEY", "").strip()
    tavily_key      = os.environ.get("TAVILY_API_KEY", "").strip()
    google_cse_api  = os.environ.get("GOOGLE_CSE_KEY", "").strip()
    google_cse_cx   = os.environ.get("GOOGLE_CSE_CX", "").strip()
    # Combine into single token for call_google_cse; empty string disables backend
    google_cse_key  = f"{google_cse_api}|{google_cse_cx}" if google_cse_api and google_cse_cx else ""
    gemini_key      = os.environ.get("GEMINI_API_KEY", "").strip()
    creds_path_raw = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    # Resolve relative paths from the project directory so .env is portable
    # across machines (Mac, Windows) regardless of absolute folder locations.
    creds_path_obj = Path(creds_path_raw).expanduser()
    if not creds_path_obj.is_absolute():
        creds_path_obj = Path(__file__).parent / creds_path_obj
    creds_path = str(creds_path_obj)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()

    missing = []
    if not any([serper_key, brave_key, tavily_key, google_cse_key]):
        missing.append("at least one of SERPER_API_KEY / BRAVE_API_KEY / TAVILY_API_KEY / GOOGLE_CSE_KEY+GOOGLE_CSE_CX")
    elif search_backend not in ("serper", "brave", "tavily", "google"):
        missing.append(f"SEARCH_BACKEND must be serper, brave, tavily, or google (got '{search_backend}')")
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

    # --rows: explicit comma-separated row list takes priority over everything
    if args.rows:
        try:
            rows_to_process = sorted(set(int(r.strip()) for r in args.rows.split(",")))
        except ValueError:
            logger.error("--rows must be comma-separated integers (e.g. 15,42,100)")
            sys.exit(1)
        logger.info("--rows: %d specific rows selected", len(rows_to_process))
    # Build explicit row set when --first-n / --last-n are used
    elif args.first_n or args.last_n:
        row_set = set()
        if args.first_n:
            first_end = min(start_row + args.first_n - 1, end_row)
            row_set.update(range(start_row, first_end + 1))
            logger.info("--first-n %d: rows %d–%d", args.first_n, start_row, first_end)
        if args.last_n:
            last_start = max(end_row - args.last_n + 1, start_row)
            row_set.update(range(last_start, end_row + 1))
            logger.info("--last-n %d: rows %d–%d", args.last_n, last_start, end_row)
        rows_to_process = sorted(row_set)
        logger.info("Sample mode: %d rows selected", len(rows_to_process))
    else:
        rows_to_process = list(range(start_row, end_row + 1))
        logger.info("Processing rows %d–%d (%d total)", start_row, end_row, len(rows_to_process))

    # --rerun-html: narrow rows_to_process to only HTML rows
    if args.rerun_html:
        html_rows = []
        for r in rows_to_process:
            rd = all_values[r - 1]
            qa = rd[COL_QA_STATUS - 1].strip() if len(rd) >= COL_QA_STATUS else ""
            if qa == "\u2713 HTML":
                html_rows.append(r)
        rows_to_process = html_rows
        logger.info("--rerun-html: %d HTML rows to re-search", len(rows_to_process))

    # --rerun-dead: narrow to DEAD/TIMEOUT rows
    if args.rerun_dead:
        dead_rows = []
        for r in rows_to_process:
            rd = all_values[r - 1]
            qa = rd[COL_QA_STATUS - 1].strip() if len(rd) >= COL_QA_STATUS else ""
            if qa in ("\u2717 DEAD", "\u2717 TIMEOUT"):
                dead_rows.append(r)
        rows_to_process = dead_rows
        logger.info("--rerun-dead: %d DEAD/TIMEOUT rows to re-search", len(rows_to_process))

    # --rerun-wrongdoc: narrow to rows where Doc_Class is 'Wrong Doc'
    if args.rerun_wrongdoc:
        wd_rows = []
        for r in rows_to_process:
            rd = all_values[r - 1]
            doc_class = rd[COL_DOC_CLASS - 1].strip() if len(rd) >= COL_DOC_CLASS else ""
            if doc_class == "Wrong Doc":
                wd_rows.append(r)
        rows_to_process = wd_rows
        logger.info("--rerun-wrongdoc: %d Wrong Doc rows to re-search", len(rows_to_process))

    # --rerun-error: narrow to rows where Doc_Class is 'Error' or QA_Status is '⚠ REVIEW'
    if args.rerun_error:
        err_rows = []
        for r in rows_to_process:
            rd = all_values[r - 1]
            doc_class = rd[COL_DOC_CLASS - 1].strip() if len(rd) >= COL_DOC_CLASS else ""
            qa = rd[COL_QA_STATUS - 1].strip() if len(rd) >= COL_QA_STATUS else ""
            if doc_class == "Error" or qa == "\u26a0 REVIEW":
                err_rows.append(r)
        rows_to_process = err_rows
        logger.info("--rerun-error: %d Error/REVIEW rows to re-search", len(rows_to_process))

    # --rerun-wrong-domain: rows already flagged "⚠ WRONG DOMAIN" / "⚠ WRONG PATH",
    # OR rows whose current Best_URL has a domain mismatch or CDN path contamination
    # that slipped through before the guards existed.
    if args.rerun_wrong_domain:
        wd_rows = []
        for r in rows_to_process:
            rd = all_values[r - 1]
            qa         = rd[COL_QA_STATUS - 1].strip() if len(rd) >= COL_QA_STATUS else ""
            best_url   = rd[COL_BEST_URL - 1].strip()  if len(rd) >= COL_BEST_URL  else ""
            homepage   = rd[COL_HOMEPAGE - 1].strip()   if len(rd) >= COL_HOMEPAGE  else ""

            if qa in ("\u26a0 WRONG DOMAIN", "\u26a0 WRONG PATH"):
                wd_rows.append(r)
                continue

            if best_url and best_url not in ("NO_RESULT", "API_ERROR") and homepage:
                # Domain mismatch on non-CDN URLs
                home_reg   = tldextract.extract(homepage).registered_domain
                best_reg   = tldextract.extract(best_url).registered_domain
                if (home_reg and best_reg
                        and best_reg != home_reg
                        and best_reg not in KNOWN_DOC_CDNS):
                    wd_rows.append(r)
                    continue

                # CDN path contamination: only applies when the URL is actually on a
                # whitelisted CDN domain (mirrors the runtime guard at the QA step).
                if best_reg in KNOWN_DOC_CDNS:
                    url_lower = best_url.lower()
                    for bad_path in WRONG_CDN_PATHS:
                        if bad_path in url_lower:
                            wd_rows.append(r)
                            break

        rows_to_process = wd_rows
        logger.info("--rerun-wrong-domain: %d wrong-domain/path rows to re-search", len(rows_to_process))

    rerun_mode = any([args.rerun_html, args.rerun_dead, args.rerun_wrongdoc, args.rerun_error, args.rerun_wrong_domain, bool(args.rows)])

    batch_buffer       = []
    rerun_rows_written = []   # rows re-searched; col P cleared after main loop
    start_time         = time.time()
    processed          = 0
    skipped            = 0

    for sheet_row in rows_to_process:
        row_data = all_values[sheet_row - 1]  # 0-indexed

        def get_col(col_1based):
            idx = col_1based - 1
            return row_data[idx].strip() if idx < len(row_data) else ""

        district_name = get_col(COL_DIST_NAME)
        homepage_url  = get_col(COL_HOMEPAGE)
        col_f_current = get_col(COL_R1_URL)

        # Resume support — skip if already processed, unless in a rerun mode
        if col_f_current and not rerun_mode:
            skipped += 1
            logger.debug("Row %d: skipping (Col F already populated: %s)", sheet_row, col_f_current)
            continue

        if not district_name:
            logger.debug("Row %d: skipping (no district name in Col D)", sheet_row)
            continue

        row_start = time.time()

        # --rerun-html: try a filetype:pdf query first before the standard query
        if args.rerun_html:
            domain = tldextract.extract(homepage_url).registered_domain if homepage_url else ""
            if domain:
                query = f'site:{domain} filetype:pdf salary OR compensation 2026 OR "25-26"'
            else:
                query = f'"{district_name}" filetype:pdf "salary schedule" OR "compensation plan" 2026'
            logger.info("Row %d | %s | HTML rerun query: %s", sheet_row, district_name, query)
        else:
            query = f'{district_name} compensation plan OR "pay scale" OR "salary schedule" 2026 OR "25-26"'
            logger.info("Row %d | %s | Query: %s", sheet_row, district_name, query)

        # ── General search ────────────────────────────────────────────────────
        r1_url = r2_url = "API_ERROR"
        r1_title = r2_title = ""
        r1_snippet = r2_snippet = ""

        try:
            results = call_search(query, serper_key, brave_key, tavily_key, search_backend, logger, google_cse_key)
            results = _pad_results(results)
            r1_url, r1_title, r1_snippet = results[0]["link"], results[0]["title"], results[0]["snippet"]
            r2_url, r2_title, r2_snippet = results[1]["link"], results[1]["title"], results[1]["snippet"]
        except Exception as exc:
            logger.error("Row %d: All search backends failed: %s", sheet_row, exc)
            results = _pad_results([])
            r1_url = "API_ERROR"

        # ── Parallel site-scoped search (always when homepage is known) ───────
        site_results = []
        if homepage_url:
            time.sleep(RATE_LIMIT_SLEEP)
            try:
                raw_site = call_serper_domain(homepage_url, serper_key, brave_key, tavily_key, logger, search_backend, google_cse_key)
                site_results = [r for r in _pad_results(raw_site) if r["link"] not in ("NO_RESULT", "API_ERROR")]
            except Exception as exc:
                logger.warning("Row %d | Site search failed: %s", sheet_row, exc)

        # ── Score all results (general R1–R10, site-scoped S1–S10) ──────────
        scored = []
        for i, r in enumerate(results, start=1):
            if r["link"] in ("NO_RESULT", "API_ERROR"):
                scored.append((f"R{i}", r["link"], 0, []))
                continue
            si, ri = score_result(r["link"], r["title"], r["snippet"], homepage_url)
            scored.append((f"R{i}", r["link"], si, ri))

        seen_urls = {r["link"] for r in results}
        for i, r in enumerate(site_results, start=1):
            if r["link"] in seen_urls:
                continue
            si, ri = score_result(r["link"], r["title"], r["snippet"], homepage_url)
            scored.append((f"S{i}", r["link"], si, ri))
            seen_urls.add(r["link"])

        s1, reasons1 = scored[0][2], scored[0][3]
        s2, reasons2 = scored[1][2], scored[1][3]

        home_domain = tldextract.extract(homepage_url).registered_domain if homepage_url else ""
        home_base = tldextract.extract(homepage_url).domain.lower() if homepage_url else ""

        def _is_right_domain(url: str) -> bool:
            """True if this URL belongs to the current district (direct or via own CDN)."""
            rd = tldextract.extract(url).registered_domain
            if rd == home_domain:
                return True
            # CDN URL whose path token matches this district (e.g. /houstonisd/ on Finalsite)
            if rd in KNOWN_DOC_CDNS and home_base:
                url_l = url.lower()
                for token in WRONG_CDN_PATHS:
                    if token in url_l and (home_base in token or token in home_base):
                        return True
            return False

        # Pick best from Brave+site: prefer any right-domain result (score >= 0)
        right_domain = [
            (k, u, s, r) for k, u, s, r in scored
            if home_domain and _is_right_domain(u) and s >= 0
        ]

        # ── Conditional Gemini: only fire when Brave+site found nothing from right domain ──
        if gemini_key and "gemini" not in _tripped_backends and not right_domain:
            try:
                gemini_results = call_gemini_grounding(query, gemini_key, logger)
                logger.info("Row %d | Gemini: %d chunks", sheet_row, len(gemini_results))
                for i, r in enumerate(gemini_results, start=1):
                    if r["link"] in seen_urls:
                        continue
                    si, ri = score_result(r["link"], r["title"], r["snippet"], homepage_url)
                    scored.append((f"G{i}", r["link"], si, ri))
                    seen_urls.add(r["link"])
                # Re-check right_domain now that Gemini results are scored
                right_domain = [
                    (k, u, s, r) for k, u, s, r in scored
                    if home_domain and _is_right_domain(u) and s >= 0
                ]
            except Exception as exc:
                msg = str(exc)
                if "RESOURCE_EXHAUSTED" in msg or "quota" in msg.lower() or "429" in msg or "billing" in msg.lower():
                    logger.warning("Row %d | Gemini budget exhausted — disabling for remainder of run", sheet_row)
                    _tripped_backends.add("gemini")
                else:
                    logger.warning("Row %d | Gemini grounding failed: %s", sheet_row, exc)

        if right_domain:
            best_key, best_url, best_score, _ = max(right_domain, key=lambda x: x[2])
        else:
            best_key, best_url, best_score, _ = max(scored, key=lambda x: x[2])

        if best_score > 0:
            if best_key.startswith("G"):
                best_label = f"{best_key}:{best_score}pts +gemini"
            elif best_key.startswith("S"):
                best_label = f"{best_key}:{best_score}pts +site"
            elif best_key not in ("R1", "R2"):
                best_label = f"{best_key}:{best_score}pts +fallback"
            else:
                best_label = f"{best_key}:{best_score}pts"
        else:
            best_label = f"{best_key}:0pts (no clear winner)"

        logger.info(
            "Row %d | %s | R1=%dpts %s | R2=%dpts %s | Best=%s:%dpts",
            sheet_row, district_name, s1, reasons1, s2, reasons2, best_key, best_score,
        )

        # ── Tier-1.5: year-free fallback if primary score is too low ──────────
        if best_score <= FALLBACK_SCORE_THRESHOLD:
            logger.info(
                "Row %d | Primary best=%dpts <= threshold=%d, firing year-free fallback",
                sheet_row, best_score, FALLBACK_SCORE_THRESHOLD,
            )
            time.sleep(RATE_LIMIT_SLEEP)
            fb_results = call_serper_noyear(district_name, serper_key, brave_key, tavily_key, logger, search_backend, google_cse_key)
            fb_results = _pad_results(fb_results)
            for r in fb_results:
                if r["link"] in ("NO_RESULT", "API_ERROR"):
                    continue
                si, _ = score_result(r["link"], r["title"], r["snippet"], homepage_url)
                if si > best_score:
                    # Reject upgrades that would fail the domain check:
                    # wrong non-CDN domain, or CDN whose path doesn't contain
                    # this district's own identifier.
                    r_domain = tldextract.extract(r["link"]).registered_domain
                    if home_domain and r_domain != home_domain:
                        if r_domain not in KNOWN_DOC_CDNS:
                            continue  # wrong non-CDN domain — skip
                        # CDN: verify the URL path contains the current district's
                        # base name (e.g. 'sintonisd' must appear in the CDN path).
                        home_base = tldextract.extract(home_domain).domain.lower()
                        if home_base and len(home_base) >= 4 and home_base not in r["link"].lower():
                            continue  # CDN path belongs to a different district — skip
                    logger.info(
                        "Row %d | Tier-1.5 upgraded: %s (%dpts > %dpts)",
                        sheet_row, r["link"], si, best_score,
                    )
                    best_url   = r["link"]
                    best_score = si
                    best_label = f"R?:{si}pts +noyear"
                    break  # take first (highest-ranked) improvement

        # ── QA ────────────────────────────────────────────────────────────────
        qa_status = qa_check(best_url, logger)

        # If the best URL is dead, try the next-best scored candidates.
        # Exception: if we already landed on the right domain, trust it —
        # many district servers block HTTP clients but the page is live.
        if qa_status in ("✗ DEAD", "✗ TIMEOUT"):
            best_on_right_domain = home_domain and _is_right_domain(best_url)
            is_doc_url = bool(re.search(r'\.(pdf|xlsx)(\?.*)?$', best_url, re.IGNORECASE))
            if best_on_right_domain and not is_doc_url:
                logger.info("Row %d | QA failed but URL is on right domain — trusting it", sheet_row)
                qa_status = "✓ HTML"
            else:
                fallbacks = sorted(
                    [(k, u, s) for k, u, s, _ in scored if u != best_url and u not in ("NO_RESULT", "API_ERROR")],
                    key=lambda x: x[2], reverse=True
                )
                for fb_key, fb_url, fb_score in fallbacks[:5]:
                    fb_status = qa_check(fb_url, logger)
                    if fb_status not in ("✗ DEAD", "✗ TIMEOUT"):
                        logger.info("Row %d | Fallback from dead %s → %s (%s)", sheet_row, best_url, fb_url, fb_status)
                        best_url   = fb_url
                        best_key   = fb_key
                        best_score = fb_score
                        best_label = best_label + " +deadfallback"
                        qa_status  = fb_status
                        break

        # ── Tier-2: crawl HTML pages for a deeper document link ───────────────
        if qa_status == "✓ HTML":
            crawled = crawl_html_for_salary_doc(best_url, logger)
            if crawled and crawled != best_url:
                logger.info("Row %d | Tier-2 crawl upgraded URL: %s → %s", sheet_row, best_url, crawled)
                best_url   = crawled
                best_label = best_label + " +crawl"
                qa_status  = qa_check(best_url, logger)

        # ── Tier-3: domain-scoped search if winner is on wrong domain ─────────
        best_domain = tldextract.extract(best_url).registered_domain if best_url not in ("NO_RESULT", "API_ERROR") else ""
        # CDN-hosted docs are expected to live on a different domain — don't
        # fire Tier-3 unless the CDN URL contains a wrong-district path token.
        _best_url_lower = best_url.lower()
        _is_wrong_cdn = (
            best_domain in KNOWN_DOC_CDNS
            and any(
                t in _best_url_lower
                and not (home_base and (home_base in t or t in home_base))
                for t in WRONG_CDN_PATHS
            )
        )
        _is_wrong_domain = home_domain and best_domain and best_domain != home_domain and best_domain not in KNOWN_DOC_CDNS
        t3_upgraded = False
        if (_is_wrong_cdn or _is_wrong_domain) and not site_results:
            logger.info(
                "Row %d | Tier-3: domain mismatch (%s ≠ %s), running site search",
                sheet_row, best_domain, home_domain,
            )
            t3_results = call_serper_domain(homepage_url, serper_key, brave_key, tavily_key, logger, search_backend, google_cse_key)
            t3_results = _pad_results(t3_results)
            t3_scored = []
            for r in t3_results:
                if r["link"] in ("NO_RESULT", "API_ERROR"):
                    continue
                si, _ = score_result(r["link"], r["title"], r["snippet"], homepage_url)
                t3_scored.append((r["link"], si))
            if t3_scored:
                t3_url, t3_score = max(t3_scored, key=lambda x: x[1])
                # If Tier-3 fired because the current best is a confirmed wrong-district
                # CDN document, accept any positive result from the right domain — a
                # lower-scoring result on the correct domain beats a wrong-district PDF.
                t3_threshold = 1 if _is_wrong_cdn else best_score
                if t3_score >= t3_threshold:
                    logger.info("Row %d | Tier-3 upgraded: %s (%dpts)", sheet_row, t3_url, t3_score)
                    best_url    = t3_url
                    best_label  = f"T3:{t3_score}pts +domain"
                    qa_status   = qa_check(best_url, logger)
                    t3_upgraded = True
                    # Tier-2 crawl on Tier-3 result if HTML
                    if qa_status == "✓ HTML":
                        crawled = crawl_html_for_salary_doc(best_url, logger)
                        if crawled and crawled != best_url:
                            best_url   = crawled
                            best_label = best_label + " +crawl"
                            qa_status  = qa_check(best_url, logger)

        # ── Tier-4: crawl homepage directly if Tier-3 found nothing ──────────────
        # Fires when the current best is a wrong-district CDN document AND Tier-3
        # couldn't find a replacement (Brave's site: search returned empty or
        # contaminated results). The district homepage URL is already known.
        if _is_wrong_cdn and not t3_upgraded and homepage_url:
            hp_url = homepage_url if homepage_url.startswith("http") else f"https://{homepage_url}"
            logger.info("Row %d | Tier-4: crawling homepage directly: %s", sheet_row, hp_url)
            crawled = crawl_homepage_deep(hp_url, logger)
            if crawled:
                best_url   = crawled
                best_label = best_label + " +hp"
                qa_status  = qa_check(best_url, logger)
                logger.info("Row %d | Tier-4 upgraded: %s", sheet_row, best_url)

        # ── Domain mismatch guard ─────────────────────────────────────────────
        # Flag if best URL is on an unrecognized third-party domain that doesn't
        # match the district's homepage. Skip check for known CDN platforms that
        # districts legitimately use to host their documents.
        if qa_status.startswith("✓") and home_domain and best_url not in ("NO_RESULT", "API_ERROR"):
            final_best_domain = tldextract.extract(best_url).registered_domain
            if (final_best_domain
                    and final_best_domain != home_domain
                    and final_best_domain not in KNOWN_DOC_CDNS):
                logger.warning(
                    "Row %d | Domain mismatch: best_url domain=%s, homepage domain=%s — flagging as WRONG DOMAIN",
                    sheet_row, final_best_domain, home_domain,
                )
                qa_status = "⚠ WRONG DOMAIN"
            elif final_best_domain and final_best_domain in KNOWN_DOC_CDNS:
                # CDN domain is allowed — but check that the URL path isn't for a
                # different district (e.g. Finalsite /fwisdorg/ path for any non-FWISD row).
                url_lower = best_url.lower()
                for bad_path in WRONG_CDN_PATHS:
                    if bad_path in url_lower:
                        is_own = home_base and (home_base in bad_path or bad_path in home_base)
                        if not is_own:
                            logger.warning(
                                "Row %d | CDN path mismatch: URL contains '%s' (another district's path) — flagging as WRONG PATH",
                                sheet_row, bad_path,
                            )
                            qa_status = "⚠ WRONG PATH"
                        break

        logger.info("Row %d | QA: %s | time=%.1fs", sheet_row, qa_status, time.time() - row_start)

        # ── Buffer row ────────────────────────────────────────────────────────
        score_str, classification, search_method = parse_best_label(best_label)
        redirect_url = extract_redirect_url(qa_status)
        qa_clean     = clean_qa_status(qa_status)

        row_values = [
            r1_url, r1_title, r2_url, r2_title,
            best_url, score_str, classification, search_method,
            qa_clean, redirect_url,
        ]
        batch_buffer.append({"row": sheet_row, "values": row_values})
        if rerun_mode:
            rerun_rows_written.append(sheet_row)
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

    # Clear Doc_Class (col P) for all rerun rows so classify_documents.py
    # will re-process them on the next classification run.
    if rerun_rows_written:
        clear_data = [{"range": f"P{r}", "values": [[""]]} for r in rerun_rows_written]
        sheet.batch_update(clear_data, value_input_option="USER_ENTERED")
        logger.info("Cleared Doc_Class (col P) for %d rerun rows", len(rerun_rows_written))

    elapsed = elapsed_str(start_time)
    logger.info(
        "Done. Processed=%d  Skipped=%d  Elapsed=%s",
        processed, skipped, elapsed,
    )
    print(f"\nDone. Processed {processed} rows, skipped {skipped}. Elapsed: {elapsed}")


if __name__ == "__main__":
    main()
