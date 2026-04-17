# Texas District Compensation Plan Data Pipeline

Searches for, downloads, classifies, and (in Phase 2) extracts salary schedules
for all ~895 Texas school districts. Data writes into a Google Sheet ("Unique Districts" tab).

---

## Column Layout Reference

| Col | Header                  | Written by               | Contents |
|-----|-------------------------|--------------------------|----------|
| A   | District_Web_Address    | pre-populated            | District homepage URL — used for domain scoring |
| B   | Enrollment              | pre-populated            | |
| C   | District_Number         | pre-populated            | |
| D   | District_Name           | pre-populated            | Used as search term |
| E   | District_Type           | pre-populated            | |
| F   | Result_1_URL            | search_urls.py           | Top search result URL |
| G   | Result_1_Title          | search_urls.py           | Top search result title |
| H   | Result_2_URL            | search_urls.py           | Second search result URL |
| I   | Result_2_Title          | search_urls.py           | Second search result title |
| J   | Best_URL                | search_urls.py           | Higher-scoring URL (tie goes to Result 1) |
| K   | Best_Score              | search_urls.py           | Numeric score (e.g. `7`) |
| L   | Best_URL_Classification | search_urls.py           | Which result won: `R1`, `R2`, `T3` (Tier-3 domain search) |
| M   | Search_Method           | search_urls.py           | Search path taken: `+fallback`, `+domain`, `+crawl`, `+fixed` |
| N   | QA_Status               | search_urls.py           | HTTP check result — see table below |
| O   | Redirect_URL            | search_urls.py           | Final URL after redirect (if any) |
| P   | Doc_Class               | classify_documents.py    | Document classification — see table below |
| Q   | Doc_Pages               | classify_documents.py    | Page count (PDF/XLSX only) |
| R   | Doc_Tables              | classify_documents.py    | Table count detected in document |
| S   | Doc_Notes               | classify_documents.py    | Keyword sample, error detail, or notes |

Scripts enforce column write guards — any write outside their allowed range raises a `RuntimeError` and halts.

---

## QA_Status Values (col N)

| Value             | Meaning |
|-------------------|---------|
| `✓ PDF`           | HTTP 200, Content-Type is PDF |
| `✓ HTML`          | HTTP 200, Content-Type is HTML |
| `✓ XLSX`          | HTTP 200, Content-Type is spreadsheet/Excel |
| `⚠ REDIRECT`      | Final URL after redirects differs from Best_URL |
| `⚠ REVIEW`        | Unexpected content-type or connection error |
| `⚠ WRONG DOMAIN`  | URL domain doesn't match district homepage and isn't a known CDN |
| `⚠ WRONG PATH`    | URL is on a known CDN but path belongs to a different district (e.g. Finalsite `/fwisdorg/` path) |
| `✗ DEAD`          | HTTP 4xx or 5xx |
| `✗ TIMEOUT`       | No response within 6 seconds |

---

## Doc_Class Values (col P)

| Value       | Meaning |
|-------------|---------|
| `Simple`    | PDF/XLSX, ≤3 pages, salary keywords found, at least one table |
| `Medium`    | PDF/XLSX, 4–15 pages, salary keywords found |
| `Complex`   | PDF/XLSX, >15 pages or dense multi-table structure |
| `HTML`      | URL resolved to an HTML page (no downloadable doc found) |
| `Unreadable`| PDF present but no extractable text (likely scanned image) |
| `Wrong Doc` | Fetched OK but no salary-related keywords found |
| `Skipped`   | QA_Status not fetchable (DEAD, WRONG DOMAIN, WRONG PATH, etc.) |
| `Error`     | Fetch or parse exception |

---

## Setup

### a. API Keys and Credentials

This project supports two search backends (configure in `.env`):

- **Serper.dev** — free at https://serper.dev (2,500 searches/month free)
- **Brave Search API** — free tier available at https://api.search.brave.com

At least one must be configured.

### b. Google Cloud Setup (Sheets API + Service Account)

1. Create a Google Cloud project at https://console.cloud.google.com/projectcreate
2. Enable the Google Sheets API: https://console.cloud.google.com/apis/library/sheets.googleapis.com
3. Create a Service Account: https://console.cloud.google.com/iam-admin/serviceaccounts
4. Download the Service Account JSON key file
5. Share your Google Sheet with the service account email as **Editor**

### c. Install Dependencies

```bash
pip install -r requirements.txt
```

### d. Fill in .env

Copy `.env.example` to `.env`:

```
SERPER_API_KEY=your_key_here
BRAVE_API_KEY=your_key_here
GOOGLE_SHEETS_CREDS_JSON=C:\path\to\credentials.json
SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

The Spreadsheet ID is the long string in your Sheet URL:
`https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`

---

## Running the Pipeline

### Step 1 — Search (search_urls.py)

Searches for each district's compensation plan URL. Scores results, QA-checks the winner, and writes columns F–O.

```bash
# Test on 10 rows first
python search_urls.py --start-row 2 --end-row 11

# Full run
python search_urls.py --start-row 2
```

Progress is printed every 10 rows:
```
[Row 42/895] Midland ISD | R1: ✓ PDF (7pts) | R2: ✓ URL (3pts) | Best: R1 | elapsed: 2m 08s
```

**Resume support:** rows with col F already populated are skipped automatically. Re-run the same command after any interruption.

**Dry-run mode:** verifies scoring logic without API calls or sheet writes:
```bash
python search_urls.py --dry-run
```

### Step 2 — Classify (classify_documents.py)

Fetches each Best_URL and classifies it by type and complexity. Writes columns P–S.

```bash
python classify_documents.py

# Limit to first N rows (for testing)
python classify_documents.py --limit 10

# Re-classify only Error rows
python classify_documents.py --rerun-errors
```

**Resume support:** rows with col P already populated are skipped automatically.

---

## QA Cleanup (qa_pipeline.py)

After a full search+classify run, some rows will have bad QA statuses or wrong
documents. Run the QA pipeline to find and re-search them in the correct order:

```bash
# Full pipeline — all five passes, classify after each
python qa_pipeline.py

# Specific passes only
python qa_pipeline.py --passes dead error

# Preview what would run (no API calls, no sheet writes)
python qa_pipeline.py --dry-run

# Search passes only, skip classify between passes
python qa_pipeline.py --no-classify
```

### Pass Order and What Each Fixes

| Pass | Flag | Targets |
|------|------|---------|
| 1. dead | `--rerun-dead` | `✗ DEAD` or `✗ TIMEOUT` rows — broken links |
| 2. error | `--rerun-error` | `Error` Doc_Class or `⚠ REVIEW` QA status |
| 3. wrong-domain | `--rerun-wrong-domain` | `⚠ WRONG DOMAIN`, `⚠ WRONG PATH`, or any URL with undetected domain/CDN-path mismatch |
| 4. wrongdoc | `--rerun-wrongdoc` | `Wrong Doc` Doc_Class — fetched OK but no salary keywords |
| 5. html | `--rerun-html` | `✓ HTML` rows — tries to upgrade to a direct PDF link |

The pipeline prints a before/after Doc_Class breakdown table at the end.

### When to Run QA

- After any large search run (`--start-row 2` on a fresh or updated dataset)
- After adding new districts to the sheet
- After updating scoring logic (to reprocess affected rows)

You don't need to run all five passes every time. Use `--passes` to target only what changed.

---

## Search Credit Usage

Each district = 1 Serper/Brave search query (plus up to 2 retries for Tier-1.5 and Tier-3).
With ~895 Texas districts, a typical run uses **900–1,100 searches**.
Serper free tier: 2,500/month. Brave free tier: 2,000/month.

QA rerun passes use additional credits proportional to how many rows are targeted.
A full `qa_pipeline.py` run typically uses 50–300 additional credits depending on
how many bad rows exist.

---

## Scoring Logic

Each of the top 2 search results is scored independently:

| Rule | Points |
|------|--------|
| URL ends with `.pdf` or `.xlsx` | +3 |
| URL domain matches district homepage | +2 |
| Contains year pattern (2026, 2025-26, 25-26, etc.) | +2 |
| Contains salary/compensation keyword | +1 |
| URL is from a news/social domain | -2 |

The higher-scoring result becomes Best_URL. Ties go to Result 1.

**3-tier search logic:**

- **Tier 1** — Year-qualified query: `{district} compensation plan OR "salary schedule" 2026 OR "25-26"`
- **Tier 1.5** — If best score ≤ 3, re-queries without year qualifier (prevents stale news from blocking correct untagged content)
- **Tier 2** — If winner is HTML, crawls it for PDF/XLSX links matching salary keywords
- **Tier 3** — If winner's domain doesn't match district homepage, fires a `site:{domain}` scoped query

---

## Domain and CDN Safeguards

Search engines frequently return high-scoring documents from large districts (FWISD, HISD,
etc.) when queried for small rural districts. Two guards prevent this from contaminating
the dataset:

**Domain mismatch guard:** If Best_URL's registered domain doesn't match the district
homepage domain and isn't in the known CDN whitelist, QA_Status is set to `⚠ WRONG DOMAIN`.

**CDN path guard:** If Best_URL is on a whitelisted CDN (e.g. Finalsite) but the URL path
contains a known large-district token (e.g. `/fwisdorg/`, `/houstonisd/`), QA_Status is
set to `⚠ WRONG PATH`.

Both statuses cause `classify_documents.py` to write `Skipped` for that row. The
`--rerun-wrong-domain` pass in `qa_pipeline.py` targets both.

---

## Logs

Every run writes a timestamped log to `logs/`:
- `run_YYYYMMDD_HHMMSS.log` — search_urls.py runs
- `classify_YYYYMMDD_HHMMSS.log` — classify_documents.py runs

Logs contain per-row detail: query sent, URLs found and scores, QA result, timing, errors.
