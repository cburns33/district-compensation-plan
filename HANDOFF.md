# Handoff — TX District Compensation Plan Research System

> **New session?** Read this file top to bottom before touching any script.
> It is the authoritative project state document.

---

## What This Project Does

Three parallel tracks:

1. **Phase 1 (COMPLETE)** — Search Google via Serper.dev for each of ~895 Texas
   school district compensation plan URLs. Score, QA-check, and write results to
   the **Unique Districts** tab of a Google Sheet.

2. **Phase 2 (DESIGNED, NOT BUILT)** — Parse the found documents to extract
   salary anchor points (min, mid, max, BA 0 yrs) into new columns.

3. **PIR Track (IN PROGRESS)** — Send Public Information Requests to ~150 Texas
   charter school districts whose salary schedules aren't publicly available
   online. Currently on the contact-discovery sub-step.

---

## Credentials and Setup

`.env` file in project root (never committed):
```
SERPER_API_KEY=...
GOOGLE_SHEETS_CREDS_JSON=district-compensation-search-e9f9f750566f.json
SPREADSHEET_ID=...
```

Service account JSON file also lives in the project root. The Google Sheet must
have the service account email added as **Editor**.

```bash
pip install -r requirements.txt
```

---

## File Structure

```
district-compensation-plan/
├── search_urls.py          # Phase 1: URL search (COMPLETE — do not re-run full)
├── remediate.py            # Phase 1: targeted fix for failed rows (run once, done)
├── find_pio_contacts.py    # PIR Track: PIO email discovery (IN PROGRESS)
├── requirements.txt
├── HANDOFF.md              # This file
├── .gitignore
├── .env                    # Not committed
├── district-compensation-search-*.json  # Not committed
└── logs/                   # Timestamped run logs (not committed)
```

`migrate_columns.py` also exists — a one-time migration script that was never
run because the user migrated the columns manually.

---

## Phase 1 — URL Search (COMPLETE)

### Google Sheet: "Unique Districts" tab

| Col | Header | Contents |
|-----|--------|----------|
| A | District_Web_Address | District homepage URL |
| B | Enrollment | |
| C | District_Number | |
| D | District_Name | |
| E | District_Type | |
| **F** | Result_1_URL | Top Serper result URL |
| **G** | Result_1_Title | Top Serper result title |
| **H** | Result_2_URL | Second Serper result URL |
| **I** | Result_2_Title | Second Serper result title |
| **J** | Best_URL | Highest-scoring URL |
| **K** | Best_Score | Numeric score (e.g. `7`) |
| **L** | Best_URL_Classification | e.g. `R1`, `R4`, `T3` |
| **M** | Search_Method | e.g. `+fallback`, `+domain`, `+fixed` |
| **N** | QA_Status | `✓ PDF`, `✓ HTML`, `✗ DEAD`, `✗ TIMEOUT`, `⚠ REVIEW` |
| **O** | Redirect_URL | Final URL after redirect (if any) |

Scripts write **only columns F–O (indices 6–15)**. Column guard enforced in code.

### Status

- All ~895 rows processed.
- `remediate.py` was run once to fix 38 failed rows (DEAD/TIMEOUT/REVIEW):
  - Stage 1: GET fallback on existing Best_URL — fixed 7
  - Stage 2: Re-score R1/R2 already in sheet — fixed 17
  - Stage 3: Fresh year-free Serper query — fixed 14
- All 38 rows resolved. Do not re-run remediate.py.

### How search_urls.py works (for reference)

**3-tier search logic:**

- **Tier 1** — Serper query with current year:
  `{district} compensation plan OR "pay scale" OR "salary schedule" 2026 OR "25-26"`
  Scores all 10 results. Best wins.

- **Tier 1.5** — If best score ≤ 3, re-queries without year qualifier. Prevents
  stale news articles from blocking correct untagged content.

- **Tier 2** — If winner is HTML, crawls it for PDF/XLSX links matching salary
  keywords. Upgrades Best_URL if found.

- **Tier 3** — If winner's domain doesn't match district homepage, fires a
  `site:{domain}` scoped Serper query.

**Scoring:**

| Rule | Points |
|------|--------|
| URL ends `.pdf` or `.xlsx` | +3 |
| Domain matches district homepage | +2 |
| Contains year pattern | +2 |
| Contains salary/compensation keyword | +1 |
| URL is from news/social domain | -2 |

---

## PIR Track — Public Information Requests

### Background

~150 Texas charter school districts don't publish salary schedules online.
Under Tex. Educ. Code § 12.1051 and Texas Government Code Chapter 552 (TPIA),
charter schools are subject to public information requests. Salary schedules are
mandatory disclosure under § 552.022(a)(2).

The plan is to email each district's Officer for Public Information (PIO) with a
formal PIR, then manually log responses into the sheet.

### Google Sheet: "PIR_Tracking" tab

Pre-populated columns (do not write to these):
- **A** — District_Number (primary key)
- **B** — District_Name
- **C** — District_Web_Address
- **F** — Date_Sent (written by send_pir.py, not yet built)
- **H** — Response_URL (manual entry)

Written by `find_pio_contacts.py`:
- **D** — PIO_Email
- **E** — PIO_Source (e.g. `serper:https://...` or `crawl:https://...`)
- **G** — Status (`PIO_FOUND` or `PIO_NOT_FOUND`)
- **I** — Notes

Script enforces a write-column safety guard — only cols 4, 5, 7, 9 (D, E, G, I).

### find_pio_contacts.py — Current State

**Purpose:** Discover the PIO email for each charter district.

**Pipeline per district:**

1. **Stage 1 (Serper, 1 credit):** Query
   `"{District Name}" Texas "public information request" OR "public information officer"`
   Fetch top result pages and extract email. Only accepts emails that are on the
   district's own domain (verified via `domain_hint` scoring). Off-domain emails
   are rejected regardless of PIO keyword context.

2. **Stage 2 (homepage crawl, 0 credits):** Try common PIO paths on the
   district's own website (`/public-information`, `/open-records`, `/contact`,
   etc.). Same domain-hint scoring applies.

3. **Stage 3:** Flag `PIO_NOT_FOUND` and write to Notes for manual follow-up.
   No superintendent email fallback.

**Key design decisions:**
- Emails on `*.state.tx.us` and `*.texas.gov` are rejected (state agencies are
  never a charter school's PIO contact).
- Snippet emails: only accepted if on the district's own domain (PIO keyword
  proximity alone is insufficient — TEA's own PIR page would otherwise match).
- Page-fetch emails: require `min_score=3` (must be on district domain). Off-domain
  emails near PIO keywords score 2 and are filtered out.
- Without a district web address in col C, page fetches are skipped entirely
  (no way to verify email belongs to the district).
- Binary/PDF responses from Serper are handled: `fetch_page` checks Content-Type
  and skips non-HTML. BeautifulSoup parse errors are caught and return `[]`.

**Commands:**
```bash
# Validate logic against 4 known charters (no sheet access, uses Serper)
python find_pio_contacts.py --test

# Full run (resumes — skips rows with existing Status)
python find_pio_contacts.py

# First N districts only
python find_pio_contacts.py --limit 50

# Single district by District_Number
python find_pio_contacts.py --district-number 057829

# Re-process already-written rows (overwrites)
python find_pio_contacts.py --limit 10 --force

# Preview without API or sheet writes
python find_pio_contacts.py --dry-run
```

**Current progress:**
- First 10 rows of PIR_Tracking have been processed.
- Results written to sheet. 4/10 found, 6/10 NOT_FOUND.
- `--test` mode passes 4/4 known charters.
- Full batch (~140 remaining) has not been run yet.

**Expected accuracy:** ~65–75% PIO_FOUND across all 150 districts based on test
results. NOT_FOUND rows will need manual follow-up.

### send_pir.py — Not Built Yet

The next script after PIO discovery is complete. Will:
1. Read PIO_FOUND rows from PIR_Tracking
2. Draft a TPIA-compliant PIR email per district
3. Send via Gmail API (OAuth 2.0 desktop flow)
4. Write Date_Sent to col F

Gmail API deps not yet installed. Will need:
```
google-api-python-client
google-auth-httplib2
google-auth-oauthlib
```

Sample PIR text (from prior research):
> Pursuant to the Texas Public Information Act, Texas Government Code Chapter 552,
> I am requesting a copy of [District Name]'s current teacher/staff salary schedule
> or compensation plan, including all pay grades, steps, and applicable stipends.
> Please provide the record in electronic format if available.

---

## Phase 2 — Salary Extraction (NOT BUILT)

Once Phase 1 URLs are validated, a separate script will:

1. Read Best_URL (col J) and QA_Status (col N) per row
2. Fetch/parse the document
3. Extract salary anchor points into new columns (P+):
   - **Min salary** — 0 yrs experience, lowest credential
   - **Mid salary** — ~10 yrs experience, BA
   - **Max salary** — top step, highest credential
   - **BA 0 yrs** — TEA standard benchmark

Extraction approach by format:
- `✓ PDF` → `pdfplumber` for table detection
- `✓ XLSX` → `openpyxl` or `pandas`
- `✓ HTML` → BeautifulSoup table scrape
- JS-rendered → Playwright headless browser fallback

---

## Important Behavioral Notes

- **Never re-run search_urls.py on already-processed rows.** Resume logic skips
  rows with col F populated, but be careful with `--start-row` overrides.
- **Never re-run remediate.py.** It was a one-time fix pass.
- **Serper credits cost money.** Always run `--test` or `--dry-run` before a
  live batch run. Ask before running any command that consumes credits.
- **Column safety guards** are enforced in every script. Any write outside the
  allowed range raises a `RuntimeError` and halts.
- **Windows UTF-8**: all scripts have a stdout/stderr wrapper to handle
  Unicode symbols (✓, ⚠, ✗) on Windows cp1252 consoles.

---

## Known Issues / Limitations

- **No web address in PIR_Tracking col C**: some charter districts have blank
  web addresses. These skip Stage 1 page fetches and rely on Stage 2, which also
  produces nothing. Likely NOT_FOUND — manual lookup needed.
- **JS-routed pages**: some district sites use React/Next.js routing and return
  blank HTML to the requests library. Stage 2 crawl misses emails on these.
- **Generic addresses (info@, contact@)**: these are valid PIR recipients and
  are accepted if on the district's own domain. Not filtered out.
