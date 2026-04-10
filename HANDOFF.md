# Handoff — TX District Compensation Plan URL Searcher

## What This Project Does

Searches Google (via Serper.dev) for each Texas school district's compensation
plan URL, scores and QA-checks the best result, and writes everything into
columns F–L of the **Unique Districts** tab in a Google Sheet.

---

## Current Status

- Phase 1 (URL search) is **built and tested** but the full run (~895 rows) has
  not been executed yet.
- 20 sample rows have been written to the sheet (10 largest + 10 smallest
  districts by enrollment) to validate the pipeline.
- Phase 2 (extracting salary anchor points from the documents) has been
  **designed but not built yet** — see bottom of this file.

---

## How to Run

### Prerequisites

1. `.env` file in the project root (never committed — create from scratch on a
   new machine):
   ```
   SERPER_API_KEY=your_key_here
   GOOGLE_SHEETS_CREDS_JSON=district-compensation-search-e9f9f750566f.json
   SPREADSHEET_ID=your_sheet_id_here
   ```
2. Service account JSON file in the project root. The filename must match
   `GOOGLE_SHEETS_CREDS_JSON` above. Share the Google Sheet with the service
   account email as **Editor**.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Commands

```bash
# Dry run — no API calls, no sheet writes, verifies scoring logic
python search_urls.py --dry-run

# Test on first 10 + last 10 rows
python search_urls.py --first-n 10 --last-n 10

# Test on a specific slice
python search_urls.py --start-row 2 --end-row 11

# Full run (resumes automatically if interrupted — rows with col F already
# populated are skipped)
python search_urls.py --start-row 2
```

---

## Google Sheet Layout

| Col | Header | Contents |
|-----|--------|----------|
| A | (existing) | District homepage URL |
| B | (existing) | Enrollment |
| C | (existing) | District Number |
| D | (existing) | District Name |
| E | (existing) | District Type |
| **F** | Result_1_URL | Top Serper result URL |
| **G** | Result_1_Title | Top Serper result title |
| **H** | Result_2_URL | Second Serper result URL |
| **I** | Result_2_Title | Second Serper result title |
| **J** | Best_URL | Highest-scoring URL across all 10 results |
| **K** | Best_Score | e.g. `R1:7pts`, `R4:6pts +fallback`, `T3:5pts +domain` |
| **L** | QA_Status | `✓ PDF`, `✓ HTML`, `✗ DEAD`, `✗ TIMEOUT`, `⚠ REVIEW` |
| M+ | (existing) | **Never touched by this script** |

**Tab:** Script targets the **"Unique Districts"** tab only.

---

## Three-Tier Search Logic

Each district goes through up to three tiers before a winner is committed:

**Tier 1 — Primary Serper search**
Query: `{district} compensation plan OR "pay scale" OR "salary schedule" 2026 OR "25-26"`
Returns up to 10 results. All 10 are scored. The highest-scoring result across
all 10 wins (not just the top 2). If the winner came from results 3–10, the
Best_Score label gets `+fallback`.

**Tier 2 — HTML crawl**
If the winning URL is an HTML page (`✓ HTML`), the script fetches it and scans
all links for salary/pay keywords pointing to PDF or XLSX documents. If a better
link is found, it upgrades Best_URL and re-runs QA. Label gets `+crawl`.

**Tier 3 — Domain-scoped search**
If the winning URL's domain doesn't match the district's homepage domain (i.e.
the wrong district bled in), the script fires a second Serper query:
`site:{district_domain} "pay scale" OR "salary schedule" OR "compensation plan"`
If this finds a better or equal result, it takes over. Label becomes
`T3:{n}pts +domain`. Tier-2 crawl also runs on Tier-3 results if needed.

---

## Scoring Rules

| Rule | Points |
|------|--------|
| URL ends with `.pdf` or `.xlsx` | +3 |
| URL domain matches district homepage | +2 |
| Contains year pattern (2026, 2025-26, 25-26, etc.) | +2 |
| Contains compensation/salary keyword | +1 |
| URL is from a news/social site | -2 |

---

## QA Status Values

| Value | Meaning |
|-------|---------|
| `✓ PDF` | HTTP 200, Content-Type is PDF |
| `✓ HTML` | HTTP 200, Content-Type is HTML |
| `✓ XLSX` | HTTP 200, Content-Type is spreadsheet |
| `⚠ REDIRECT → url` | Redirected to a different final URL |
| `✗ DEAD` | HTTP 4xx or 5xx |
| `✗ TIMEOUT` | No response within 6 seconds |
| `⚠ REVIEW` | Unexpected content-type or connection error |

---

## Known Edge Cases / Failure Patterns

**Wrong-district bleed** — Small districts with minimal web presence sometimes
return results for a different district with a similar name. Tier-3 domain search
catches most of these. Residual cases show up as `T3` or `⚠ REVIEW` in col L.

**Very small districts (enrollment < ~100)** — Some have no indexed compensation
content at all. These typically come back `0pts (no clear winner)` and `⚠ REVIEW`.
Expect ~10–15% of the smallest districts to fall in this bucket. These will need
manual lookup or a future LLM-assisted navigation step.

**JavaScript-only pages** — A small number of districts serve their pay scale
data via JS-rendered modals or iframes that BeautifulSoup can't parse. These
return `✓ HTML` in QA but the Tier-2 crawl finds nothing. Flagged for Phase 2
handling with a headless browser (Playwright).

**Serper vs browser discrepancy** — Serper results can differ from what you see
in a real browser (no personalization, no geolocation). The expanded 10-result
scoring + Tier-3 largely compensates for this.

---

## Resume Behavior

If the script is interrupted at any point, re-run the same command. The script
checks column F before processing each row — if F already has a value, that row
is skipped. No duplicate API calls.

---

## Phase 2 — Not Built Yet

Once the URL search run is complete, Phase 2 will be a separate script that:

1. Reads Best_URL (col J) and QA_Status (col L) for each district
2. Fetches/parses the document (PDF, XLSX, or HTML)
3. Extracts salary anchor points and writes them to new columns (M+):
   - **Min salary** — 0 yrs experience, lowest credential
   - **Mid salary** — ~10 yrs experience, BA
   - **Max salary** — top step, highest credential
   - **BA 0 yrs** — TEA standard benchmark

This normalized data feeds the final dashboard so all districts sit on the same
scale regardless of how their source document was structured.

Extraction approach by format:
- `✓ PDF` → `pdfplumber` for table detection
- `✓ XLSX` → `openpyxl` or `pandas`
- `✓ HTML` → `BeautifulSoup` table scrape
- JS-rendered → `playwright` headless browser fallback

---

## File Structure

```
district-compensation-plan/
├── search_urls.py                          # Main script (Phase 1)
├── requirements.txt                        # Python dependencies
├── .gitignore                              # Excludes .env, *.json, logs/
├── HANDOFF.md                              # This file
├── district-compensation-search-*.json     # Service account key (not committed)
├── .env                                    # Credentials (not committed)
└── logs/                                  # Timestamped run logs (not committed)
```
