"""
fix_redirects.py — One-time cleanup of ⚠ REDIRECT rows in Unique Districts tab.

Group A (22 rows): redirect destination is a valid live page — update Best_URL
  and QA_Status directly, no Serper needed.

Group B (12 rows): destination is wrong/dead/stale — run a fresh Serper search
  and update with the best result found.
"""

import io
import os
import re
import sys
import time

import requests
import tldextract
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

load_dotenv()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}
SERPER_KEY = os.environ["SERPER_API_KEY"]

# These districts have redirect destinations pointing to a different district's
# site or a stale file — force them into re-search regardless of URL pattern.
FORCE_RESEARCH = {"PETTUS ISD", "HUMBLE ISD"}

REDIRECT_RE = re.compile(r"^⚠ REDIRECT → (.+)$")
GOOD_HTML_RE = re.compile(
    r"apps/pages/index\.jsp|/compensation.plan|/pay.scale|/pay.grade"
    r"|/salary|/human.resources/|/employment.policies|/hr/",
    re.IGNORECASE,
)
BAD_DEST_RE = re.compile(
    r"teachnyc\.net|applitrack\.com|board.of.trustees|school_board_members"
    r"|board-of-trustees|check.register|en-US$",
    re.IGNORECASE,
)
GOOGLE_RE  = re.compile(r"docs\.google\.com|drive\.google\.com")
LOGIN_RE   = re.compile(
    r"login\.aspx|accounts\.google\.com/v3/signin|gateway/login", re.IGNORECASE
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def score_url(url, title, snippet, homepage):
    s = 0
    if re.search(r"\.(pdf|xlsx)$", url, re.IGNORECASE):
        s += 3
    hd = tldextract.extract(homepage).registered_domain
    rd = tldextract.extract(url).registered_domain
    if hd and hd == rd:
        s += 2
    combined = f"{url} {title} {snippet}"
    if re.search(r"2026|2025-26|25-26", combined):
        s += 2
    if re.search(r"compensation|salary|pay plan|pay scale|pay schedule", combined, re.IGNORECASE):
        s += 1
    for bad in ["reddit", "facebook", "twitter", "linkedin", "communityimpact",
                "dallasnews", "statesman", "chron", "indeed", "teachnyc"]:
        if bad in url.lower():
            s -= 2
            break
    return s


def serper_search(query):
    resp = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
        json={"q": query, "num": 10},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("organic", [])


def qa_check(url):
    try:
        r = requests.head(url, timeout=6, allow_redirects=True, headers=HEADERS)
        ct = r.headers.get("Content-Type", "").lower()
        final = r.url
        if r.status_code >= 400:
            return f"✗ DEAD", final
        if "pdf" in ct:
            return "✓ PDF", final
        if any(x in ct for x in ["spreadsheet", "excel", "openxml"]):
            return "✓ XLSX", final
        if "html" in ct:
            return "✓ HTML", final
        return "⚠ REVIEW", final
    except Exception:
        return "✗ TIMEOUT", url


# ── Load sheet ─────────────────────────────────────────────────────────────────

creds = Credentials.from_service_account_file(
    os.environ["GOOGLE_SHEETS_CREDS_JSON"],
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
sheet = gspread.authorize(creds).open_by_key(os.environ["SPREADSHEET_ID"]).worksheet("Unique Districts")
all_rows = sheet.get_all_values()

redirect_rows = []
for i, row in enumerate(all_rows[1:], start=2):
    while len(row) < 15:
        row.append("")
    qa = row[13].strip()
    m = REDIRECT_RE.match(qa)
    if not m:
        continue
    redirect_rows.append({
        "sheet_row": i,
        "district":  row[3].strip(),
        "web_addr":  row[0].strip(),
        "dest":      m.group(1).strip(),
    })

print(f"Found {len(redirect_rows)} redirect rows.")

# ── Categorize ─────────────────────────────────────────────────────────────────

direct_updates = []   # (sheet_row, new_best_url, new_qa, district)
to_search      = []   # row dicts

for r in redirect_rows:
    dest = r["dest"]

    if r["district"] in FORCE_RESEARCH:
        to_search.append(r)
        continue

    if LOGIN_RE.search(dest):
        to_search.append(r)
        continue

    if GOOGLE_RE.search(dest):
        try:
            resp = requests.get(dest, timeout=8, allow_redirects=True, headers=HEADERS)
            ct   = resp.headers.get("Content-Type", "").lower()
            if resp.status_code == 200 and "accounts.google.com" not in resp.url:
                direct_updates.append((r["sheet_row"], dest, "✓ HTML", r["district"]))
            else:
                to_search.append(r)
        except Exception:
            to_search.append(r)
        time.sleep(0.3)
        continue

    if BAD_DEST_RE.search(dest):
        to_search.append(r)
        continue

    if GOOD_HTML_RE.search(dest):
        direct_updates.append((r["sheet_row"], dest, "✓ HTML", r["district"]))
        continue

    # Generic district page — HEAD to confirm alive
    try:
        resp = requests.head(dest, timeout=6, allow_redirects=True, headers=HEADERS)
        ct   = resp.headers.get("Content-Type", "").lower()
        if resp.status_code == 200 and "pdf" in ct:
            direct_updates.append((r["sheet_row"], dest, "✓ PDF", r["district"]))
        elif resp.status_code == 200:
            direct_updates.append((r["sheet_row"], dest, "✓ HTML", r["district"]))
        else:
            to_search.append(r)
    except Exception:
        to_search.append(r)
    time.sleep(0.3)

print(f"\nGroup A (direct update): {len(direct_updates)}")
print(f"Group B (re-search):     {len(to_search)}")

# ── Write Group A ──────────────────────────────────────────────────────────────

print("\n── Writing Group A updates ──")
batch = []
for sheet_row, dest, qa_new, district in direct_updates:
    batch.append({"range": f"J{sheet_row}", "values": [[dest]]})
    batch.append({"range": f"N{sheet_row}", "values": [[qa_new]]})
    batch.append({"range": f"M{sheet_row}", "values": [["+redirect_fixed"]]})
    print(f"  {qa_new}  {district}")
sheet.batch_update(batch, value_input_option="USER_ENTERED")

# ── Group B: fresh Serper searches ─────────────────────────────────────────────

print(f"\n── Running {len(to_search)} Serper searches ──")
search_batch = []

for r in to_search:
    district = r["district"]
    home     = r["web_addr"]
    query    = f'{district} Texas compensation plan OR "salary schedule" 2026 OR "25-26"'

    try:
        results = serper_search(query)
        time.sleep(1.5)
    except Exception as e:
        print(f"  ERROR {district}: {e}")
        continue

    if not results:
        print(f"  NO RESULTS: {district}")
        search_batch.append({"range": f"N{r['sheet_row']}", "values": [["✗ DEAD"]]})
        search_batch.append({"range": f"M{r['sheet_row']}", "values": [["+researched"]]})
        continue

    best = max(results, key=lambda x: score_url(
        x.get("link", ""), x.get("title", ""), x.get("snippet", ""), home
    ))
    best_url   = best.get("link", "")
    best_score = score_url(best_url, best.get("title", ""), best.get("snippet", ""), home)
    qa, final  = qa_check(best_url)
    if final != best_url:
        qa = f"⚠ REDIRECT → {final}"

    print(f"  {district}: {qa} (score {best_score}) -> {best_url[:65]}")
    search_batch.append({"range": f"J{r['sheet_row']}", "values": [[best_url]]})
    search_batch.append({"range": f"K{r['sheet_row']}", "values": [[str(best_score)]]})
    search_batch.append({"range": f"N{r['sheet_row']}", "values": [[qa]]})
    search_batch.append({"range": f"M{r['sheet_row']}", "values": [["+researched"]]})
    search_batch.append({"range": f"O{r['sheet_row']}", "values": [[final if final != best_url else ""]]})
    time.sleep(0.5)

if search_batch:
    sheet.batch_update(search_batch, value_input_option="USER_ENTERED")

print("\nDone.")
