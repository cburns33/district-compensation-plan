# Handoff — TX District Compensation Plan Research System

> **New session?** Read this file top to bottom before touching any script.
> It is the authoritative project state document.

---

## What This Project Does

Three parallel tracks:

1. **Phase 1 (RERUN IN PROGRESS)** — Search for each of ~895 Texas school district
   compensation plan URLs. Score, QA-check, classify, and write results to the
   **Unique Districts** tab of a Google Sheet. A full rerun is underway using an
   improved scoring model (see Scoring Model section below).

2. **Phase 2 (IN DESIGN)** — Extract full step/lane salary matrices from found
   documents into a normalized database. Target output feeds two products:
   - **Product 1:** Teacher compensation dashboard (base + TIA allotment by
     experience/degree/designation at any TX district)
   - **Product 2:** Intervention benefit-cost analysis engine (shadow-prices
     staff costs using salary schedules; designed to interoperate with district
     data via Ed-Fi data standards)
   Database: SQLite during build, Postgres at deployment. Schema to follow
   Ed-Fi standards from the start. Pilot will use PIR response documents.

3. **PIR Track (IN PROGRESS)** — Send Public Information Requests to 196 Texas
   school districts (188 charters + 8 non-charter ISDs) whose salary schedules
   aren't publicly available online. Emails sent; responses being tracked.

---

## Credentials and Setup

`.env` file in project root (never committed):
```
SERPER_API_KEY=...          # depleted — do not set as primary
BRAVE_API_KEY=...           # active primary; $10/month spend cap
SEARCH_BACKEND=brave        # active backend: brave | serper | tavily | google
TAVILY_API_KEY=...          # fallback; 1,000 free credits/month
GOOGLE_CSE_KEY=...          # present but not viable (see Search Backends section)
GOOGLE_CSE_CX=...           # present but not viable
GOOGLE_SHEETS_CREDS_JSON=district-compensation-search-e9f9f750566f.json
SPREADSHEET_ID=...
GMAIL_CREDENTIALS_JSON=gmail-api.json
GMAIL_SENDER_EMAIL=chase.burns@talos-advisory.com
PIR_SENDER_NAME=Chase Burns
```

Credential files in project root (never committed):
- `district-compensation-search-*.json` — Google service account (Sheets access)
- `gmail-api.json` — Gmail OAuth Desktop client
- `token.json` — saved Gmail OAuth token for send_pir.py (gmail.send scope)
- `token_reader.json` — saved Gmail OAuth token for check_pir_responses.py (gmail.readonly + gmail.send)

The Google Sheet must have the service account email added as **Editor**.

```bash
pip install -r requirements.txt
```

---

## File Structure

```
district-compensation-plan/
├── search_urls.py              # Phase 1: URL search — writes cols F–O
├── classify_documents.py       # Phase 1: document classification — writes cols P–S
├── qa_pipeline.py              # Phase 1: orchestrated QA cleanup (run after any bulk search)
├── prepare_rerun.py            # Phase 1: pre-flight for full rerun — duplicates tab, clears cols, suggests test rows
├── remediate.py                # Phase 1: one-time fix script — COMPLETE, do not re-run
├── find_pio_contacts.py        # PIR Track: PIO email discovery — COMPLETE
├── send_pir.py                 # PIR Track: send PIR emails — all 196 sent
├── check_pir_responses.py      # PIR Track: scan Gmail for replies, update sheet, print report
├── requirements.txt
├── README.md                   # Setup and usage reference
├── HANDOFF.md                  # This file — authoritative project state
├── .gitignore
├── .env                        # Not committed
├── district-compensation-search-*.json  # Not committed
├── gmail-api.json              # Not committed
├── token.json                  # Not committed (send_pir.py OAuth token)
├── token_reader.json           # Not committed (check_pir_responses.py OAuth token)
└── logs/                       # Timestamped run logs (not committed)
```

`migrate_columns.py` also exists — a one-time migration script, never run (user migrated manually).

---

## Phase 1 — URL Search + Classification (RERUN IN PROGRESS)

### Google Sheet: "Unique Districts" tab

| Col | Header | Written by | Contents |
|-----|--------|------------|----------|
| A | District_Web_Address | pre-populated | District homepage URL |
| B | Enrollment | pre-populated | |
| C | District_Number | pre-populated | |
| D | District_Name | pre-populated | |
| E | District_Type | pre-populated | |
| F | Result_1_URL | search_urls.py | Top search result URL |
| G | Result_1_Title | search_urls.py | |
| H | Result_2_URL | search_urls.py | Second result URL |
| I | Result_2_Title | search_urls.py | |
| J | Best_URL | search_urls.py | Highest-scoring URL |
| K | Best_Score | search_urls.py | Numeric score (e.g. `7`) |
| L | Best_URL_Classification | search_urls.py | `R1`, `R2`, `T3` |
| M | Search_Method | search_urls.py | `+fallback`, `+domain`, `+crawl`, `+fixed` |
| N | QA_Status | search_urls.py | `✓ PDF`, `✓ HTML`, `⚠ WRONG DOMAIN`, `⚠ WRONG PATH`, etc. |
| O | Redirect_URL | search_urls.py | Final URL after redirect (if any) |
| P | Doc_Class | classify_documents.py | `Simple`, `Medium`, `Complex`, `Skipped`, `Error`, etc. |
| Q | Doc_Pages | classify_documents.py | Page count |
| R | Doc_Tables | classify_documents.py | Table count |
| S | Doc_Notes | classify_documents.py | Keyword sample or error detail |

Scripts write only their allowed column ranges. Column guard enforced in code — any write
outside the allowed range raises `RuntimeError` and halts.

### Doc_Class Breakdown (pre-rerun, as of 2026-04-16)

| Doc_Class | Count | Notes |
|-----------|-------|-------|
| Complex | 269 | >15 pages or dense multi-table structure |
| Simple | 161 | ≤3 pages, salary keywords found |
| Medium | 60 | 4–15 pages |
| Skipped | ~373 | No valid URL (WRONG DOMAIN, WRONG PATH, DEAD, etc.) |
| Unreadable | 27 | Scanned PDFs — no extractable text |
| HTML | 17 | HTML pages, no downloadable doc found |
| Error | 4 | Corrupt or oversized PDFs |
| Blank | 8 | ⚠ REVIEW with no URL — small rural districts, in PIR bucket |

Counts are from the pre-rerun pass. A full rerun with improved scoring is underway;
these numbers will change.

### Full Rerun Workflow

A full clean rerun was triggered by two issues discovered in analysis:
1. The old scoring model awarded 7pts to any PDF on the right domain — handbooks
   scored identically to salary schedules. 27% of "high-confidence" rows had wrong docs.
2. Multiple corruption passes from CDN path filter bugs left large-district rows with
   incorrect documents.

**Steps:**
```bash
# 1. Duplicate tab as backup, clear cols F–O, get 20 test row suggestions
python prepare_rerun.py

# 2. Test the new scoring model on those 20 rows
python search_urls.py --rows <rows from step 1>

# 3. Review results in the sheet. If good, run the full rerun:
python search_urls.py

# 4. After the full run completes, re-classify all rows:
python classify_documents.py

# 5. QA cleanup pass:
python qa_pipeline.py
```

### Search Backends

| Backend | Status | Cost |
|---------|--------|------|
| Brave | **Active primary** | ~$3–5/1,000 queries; $10/month cap set |
| Tavily | Auto-fallback | 1,000 free credits/month |
| Serper | Depleted | $50 minimum purchase; reserve for emergency reruns |
| Google CSE | Not viable | "Search entire web" feature deprecated Jan 2026 — site-restricted only |

The circuit breaker in `call_search()` automatically trips depleted backends for the
lifetime of the process — no wasted retries on quota errors.

### Scoring Model (as of 2026-04-17)

Rewritten to use filename as the primary signal rather than the full URL+title+snippet.

| Signal | Points | Notes |
|--------|--------|-------|
| PDF or XLSX file type | +3 | |
| Domain match (homepage vs result) | +2 | |
| Year pattern (2026, 25-26, etc.) | +2 | |
| Compensation keyword in **filename** | +2 | salary, compensation, pay_scale, matrix, etc. |
| Compensation keyword in title/snippet | +1 | Only if filename didn't fire |
| News/social domain | -2 | reddit, indeed, linkedin, local news, etc. |
| Scribd URL | -3 | Documents gated behind login |
| Wrong-doc keyword in **filename** | -3 | handbook, budget, cafr, agenda, minutes, etc. |
| Wrong-doc keyword in title/snippet | -2 | Weaker signal; only fires if filename didn't |

All queries now include `filetype:pdf` to filter at the search layer before scoring.

**Why this matters:** the old model gave 7pts (pdf+domain+year) to any PDF on the
right domain. Handbooks and salary schedules scored identically. The new model
separates them by 5+ points when the filename keyword fires.

### Search History

- All ~895 rows processed via search_urls.py (multiple passes over several sessions).
- `remediate.py` was run once to fix 38 failed rows (DEAD/TIMEOUT/REVIEW) — do not re-run.
- Multiple QA cleanup passes were run to fix wrong-domain and CDN-path contamination.
- CDN path filter bug caused ~179 large-district rows to be corrupted; Tavily recovery
  was partial. Full rerun with Brave is the fix.
- See "QA Cleanup" section below for the ongoing process.

---

## QA Cleanup — Running and Maintaining Data Quality

### When to Run

After any large re-search (new districts, updated scoring logic, a fresh `--start-row 2`
run). Not needed after small targeted patches.

### The Pipeline

`qa_pipeline.py` orchestrates all five rerun passes in the correct order, runs
`classify_documents.py` after each pass, and prints a before/after Doc_Class table.

```bash
# Full cleanup — all five passes
python qa_pipeline.py

# Specific passes only (e.g. after a scoring change only affects DEAD rows)
python qa_pipeline.py --passes dead error

# Preview what would run — no API calls, no sheet writes
python qa_pipeline.py --dry-run

# Search passes only, skip classify between passes (faster initial scan)
python qa_pipeline.py --no-classify
```

### Pass Order and What Each Targets

| Order | Pass name | Flag | Targets |
|-------|-----------|------|---------|
| 1 | dead | `--rerun-dead` | `✗ DEAD` or `✗ TIMEOUT` — broken links |
| 2 | error | `--rerun-error` | `Error` Doc_Class or `⚠ REVIEW` QA_Status |
| 3 | wrong-domain | `--rerun-wrong-domain` | `⚠ WRONG DOMAIN`, `⚠ WRONG PATH`, or any existing URL with undetected domain/CDN-path mismatch |
| 4 | wrongdoc | `--rerun-wrongdoc` | `Wrong Doc` Doc_Class — fetched OK, no salary keywords |
| 5 | html | `--rerun-html` | `✓ HTML` rows — tries to upgrade to direct PDF |

**Pass 3 is the most important.** Search engines frequently return FWISD, HISD, or College
Station ISD documents for unrelated small rural districts. Two guards in search_urls.py
catch this:

- **Domain mismatch guard** — flags `⚠ WRONG DOMAIN` if the URL's registered domain
  doesn't match the district homepage and isn't in `KNOWN_DOC_CDNS`.
- **CDN path guard** — flags `⚠ WRONG PATH` if the URL is on a whitelisted CDN
  (Finalsite, Thrillshare, etc.) but contains a known large-district path token
  (e.g. `/fwisdorg/`, `/houstonisd/`, `/katyisd/`).

Both are defined as constants at the top of `search_urls.py` (`KNOWN_DOC_CDNS`,
`WRONG_CDN_PATHS`) and should be expanded as new contaminants are identified.

### Rerun Flags (individual use)

Each pass can also be run directly on `search_urls.py` without the pipeline:

```bash
python search_urls.py --rerun-dead
python search_urls.py --rerun-error
python search_urls.py --rerun-wrong-domain
python search_urls.py --rerun-wrongdoc
python search_urls.py --rerun-html
```

Always run `python classify_documents.py` after any rerun to re-classify the cleared rows.
The rerun logic automatically clears col P (Doc_Class) for re-searched rows so classify
knows to re-process them.

### Adding a New Rerun Pass

To add a new pass type to the pipeline:

1. Add `--rerun-{name}` argparse flag to `search_urls.py`
2. Add the filter block in `main()` (after the existing rerun blocks)
3. Add the pass name to `rerun_mode = any([...])` in `search_urls.py`
4. Add entries to `PASS_ORDER`, `PASS_FLAGS`, `PASS_LABELS` in `qa_pipeline.py`

---

## PIR Track — Public Information Requests

### Background

188 Texas charter school districts don't publish salary schedules online.
Under Tex. Educ. Code § 12.1051 and Texas Government Code Chapter 552 (TPIA),
charter schools are subject to public information requests. Salary schedules are
mandatory disclosure under § 552.022(a)(2).

Contact emails were sourced from AskTED (TEA's role-based contact database),
prioritized in this order: HR > CFO > Secretary to Supt > Asst/Assoc/Deputy Supt
> PEIMS Coordinator > School Email (Directory) > Manual.

### Google Sheet: "PIR_Tracking" tab

| Col | Header | Source |
|-----|--------|--------|
| A | District_Number | Pre-populated — read only |
| B | District_Name | Pre-populated — read only |
| C | District_Web_Address | Pre-populated — read only |
| D | PIO_Email | Imported from AskTED; updated manually for corrections |
| E | Email_Role | Imported from AskTED (e.g. "Human Resources") — read only |
| F | PIO_Source | Written by find_pio_contacts.py |
| G | Date_Sent | Written by send_pir.py (`YYYY-MM-DD HH:MM:SS`) |
| H | Status | Written by find_pio_contacts.py (`PIO_FOUND` / `PIO_NOT_FOUND`) |
| I | Response_URL | Manual entry — never written by scripts |
| J | Notes | Written by find_pio_contacts.py |
| K | Full_Name | Contact full name — read only |
| L | First_Name | Contact first name — read only |
| M | Last_Name | Contact last name — read only |
| N | Send_Status | Written by send_pir.py (`Sent`, `Sent (grouped: N)`, `Failed: ...`) |
| O | Response_Date | Written by check_pir_responses.py (`YYYY-MM-DD`) |
| P | Response_Status | Written by check_pir_responses.py (`Doc Received`, `URL Received`, `Delay Notice`, `Denial`, `See Portal`, `Responded`) |
| Q | Followup_Sent | Written by check_pir_responses.py — date follow-up sent (`YYYY-MM-DD`) |

find_pio_contacts.py write guard: cols D, F, H, J only.
send_pir.py write guard: cols G, N only.
check_pir_responses.py write guard: cols O, P, Q only.

### Current PIR Status (as of 2026-04-16)

- **All 196 districts emailed** (188 original charters + 8 non-charter ISDs added manually)
- **Follow-ups sent** to all districts with Doc Received or URL Received status
- **8 non-charter ISDs in PIR bucket:** Claude ISD, Pettus ISD, Panther Creek CISD,
  Seminole ISD, McLean ISD, North Hopkins ISD, Coolidge ISD, Munday CISD
- **10-business-day deadline window** is active; run `check_pir_responses.py` periodically

**Corrections to date:**
- **ACCELERATED INTERMEDIATE ACADEMY** — resent to `aiapeims2020b@aol.com`
- **BASIS TEXAS** — resent to `ANDREW.FREEMAN@BTXSCHOOLS.ORG` (CFO)
- **WINFREE ACADEMY CHARTER SCHOOLS** — resent to `dstaples@wacsd.com`
- **UT TYLER UNIVERSITY ACADEMY** — submitted via web portal; Send_Status = "Submitted via portal"
- **EDUCATION CENTER INTERNATIONAL ACADEMY** — resent to `bdensmore@eciacharter.com`
- **RAPOPORT ACADEMY PUBLIC SCHOOL** — resent to `mnelson@rapswaco.org`; wrong Denial cleared
- **NOVA ACADEMY** — resent to `admissions.austin@novaacademy.school`
- **COMPASS ROSE PUBLIC SCHOOLS** — resent to `PublicInfoRequest@compassroseschools.org`
- **PRELUDE PREPARATORY CHARTER SCHOOL** — resent to `Office@preludeprep.org`
- **MIDLAND ACADEMY CHARTER SCHOOL** — resent to `kcoker@macharter.org`

### find_pio_contacts.py — COMPLETE

Email discovery is done. All 196 rows have a PIO_Email populated.
Do not re-run unless a new district is added to PIR_Tracking.

### send_pir.py — ALL SENT

All 196 districts have been emailed. Do not run a standard send.

Use `--force` only to correct and resend a specific district:
```bash
# Correct a contact and resend
python send_pir.py --district-number 57828 \
  --update email=dstaples@wacsd.com full_name="Deirdre Staples" first_name=Deirdre last_name=Staples \
  --force
```

**IMPORTANT:** Always use `--update` rather than editing the sheet directly. The guard
requires name fields alongside any email change to prevent stale salutations.

### check_pir_responses.py — Ongoing

Scans Gmail inbox for replies. Writes Response_Date (col O) and Response_Status (col P).
Uses `token_reader.json` (gmail.readonly + gmail.send scopes).

```bash
# Full run: scan Gmail + update sheet + print deadline report
python check_pir_responses.py

# Full run + send follow-up emails for Doc/URL Received responses
python check_pir_responses.py --send-followups

# Scan Gmail + print matches, no sheet writes, no sends
python check_pir_responses.py --dry-run

# Print deadline report from sheet data only (no Gmail scan)
python check_pir_responses.py --report
```

**Response status values:** `Doc Received`, `URL Received`, `Delay Notice`, `Denial`, `See Portal`, `Responded`

**Matching priority:** exact PIO_Email match > sender domain match > district name in subject

**Deadline report categories:**
- `OVERDUE` — past 10 business days, no response
- `APPROACHING` — 8–10 business days, no response
- `RESPONDED` — any response received, with status and date
- `PENDING` — sent, within 10 business days

**Follow-up email:** Short reply into the original thread. Sent once per PIO address.
Also searches `label:Done-Attachment` and `label:Done-URL` Gmail labels for archived threads.

---

## Phase 2 — Salary Extraction (IN DESIGN)

### Goal

Build a normalized salary database with the full step/lane matrix per district.
Each cell: `district × school_year × step × lane → dollar amount`.
This is the core data asset for both products.

### Extraction Target

Start with the 161 **Simple** rows (≤3 pages, salary keywords confirmed) — these are the
cleanest inputs for building and validating the extraction logic before scaling to
Medium/Complex rows.

### Products This Feeds

**Product 1 — Teacher Compensation Dashboard**
Teacher inputs TIA designation tier, years of experience, degree level, subject/role.
Gets projected total compensation (base + TIA allotment) at any TX district, current
year and career projection. No comparable tool exists today.

**Product 2 — Intervention Benefit-Cost Analysis Engine**
District administrators connect intervention records with shadow-priced staff costs
(derived from salary schedules) to produce cost-per-outcome efficiency ratios.
Interoperates with district data via Ed-Fi data standards.

### Database

SQLite during build phase, Postgres at deployment. Schema follows Ed-Fi standards
from the start so Product 2 can sync with district data in the Ed-Fi exchange.
ORM: SQLAlchemy (connection string swap = SQLite → Postgres).

### Planned Extraction Pipeline

**Stage 1 — Automated extraction by format**
- `✓ XLSX` → pandas / openpyxl (cleanest, build first)
- `✓ PDF` → pdfplumber table detection
- `✓ HTML` → BeautifulSoup table scrape
- Scanned PDF / JS-rendered → OCR / Playwright fallback

**Stage 2 — Normalization**
Map heterogeneous lane labels (BA, BS, Column I, Bachelors, Degree 1) to canonical set.
Map step labels (Year 0, Step 1, 0-1 yrs, Beginning) to integer years.
Flag unresolved cells for manual review.

**Pilot:** Start with PIR response documents (small known set, files in hand, easy to
verify by eye). Build and validate schema on those before scaling to Phase 1 URLs.

### Universal Comparable Schema

Even complex pay structures (HISD NES model, TIA-linked pay, subject-differentiated lanes)
can be reduced to four universal anchor points for cross-district comparison:

| Anchor | Meaning |
|--------|---------|
| Entry salary | Step 0/1, minimum lane (BA) |
| Mid-career salary | Step 10, minimum lane |
| MA premium | Salary difference between BA and MA lanes at Step 0 |
| Ceiling | Maximum salary in schedule |

These four values work even for the simplest (Milford ISD style) schedules and remain
meaningful for complex ones.

---

## Important Behavioral Notes

- **Full reruns are intentional** — use `prepare_rerun.py` first to back up the tab
  and get test row suggestions. Always test ~20 rows before committing to all ~895.
- **Never re-run remediate.py.** It was a one-time fix pass.
- **Never run find_pio_contacts.py on all rows.** Discovery is complete.
- **All 196 PIR emails are sent.** Never run `send_pir.py` without `--district-number`
  and `--force` unless adding genuinely new districts.
- **Search credits cost money.** Always use `--dry-run` before a live run. The
  `qa_pipeline.py --dry-run` flag previews all passes without any API calls.
  Brave is the active backend ($10/month cap). Serper is depleted — do not set as primary.
- **Column safety guards** are enforced in every script. Any write outside the allowed
  range raises `RuntimeError` and halts.
- **Windows UTF-8:** all scripts have a stdout/stderr wrapper to handle Unicode symbols
  (✓, ⚠, ✗) on Windows cp1252 consoles.
- **Correcting a sent email:** use `send_pir.py --update ... --force`, never edit the
  sheet directly. The guard requires name fields alongside any email change.
- **Email HTML rendering:** send_pir.py and check_pir_responses.py use `<div>` + `<p>`
  tags (not `<pre>`). Renders at full width with proportional fonts in all clients.

---

## Known Issues / Limitations

- **UT Tyler University Academy** uses a web portal for PIR submissions. Any future
  district that responds similarly should have Send_Status set to "Submitted via portal".
- **JS-routed pages:** some district sites use React/Next.js and return blank HTML to
  requests. Stage 2 crawl misses content on these.
- **8 blank Doc_Class rows:** Seminole ISD, North Hopkins ISD, Munday CISD, Pettus ISD,
  Claude ISD, Coolidge ISD, McLean ISD, Panther Creek CISD — all have `⚠ REVIEW` with
  no URL. All are in the PIR bucket. Not worth a rerun.
- **49 remaining WRONG DOMAIN rows:** small rural districts where search consistently
  returns another district's document. Tier-3 domain search finds nothing on their domain.
  These will be PIR bucket or manual lookup.
- **Generic addresses (info@, contact@):** valid PIR recipients if on the district's
  own domain. Not filtered out.
