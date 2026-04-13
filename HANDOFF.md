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

3. **PIR Track (IN PROGRESS)** — Send Public Information Requests to 188 Texas
   charter school districts whose salary schedules aren't publicly available
   online. Emails are sending in daily batches of 40.

---

## Credentials and Setup

`.env` file in project root (never committed):
```
SERPER_API_KEY=...
GOOGLE_SHEETS_CREDS_JSON=district-compensation-search-e9f9f750566f.json
SPREADSHEET_ID=...
GMAIL_CREDENTIALS_JSON=gmail-api.json
GMAIL_SENDER_EMAIL=chase.burns@talos-advisory.com
PIR_SENDER_NAME=Chase Burns
```

Two JSON credential files in project root (never committed):
- `district-compensation-search-*.json` — Google service account (Sheets access)
- `gmail-api.json` — Gmail OAuth Desktop client
- `token.json` — saved Gmail OAuth token (auto-generated on first run)

The Google Sheet must have the service account email added as **Editor**.

```bash
pip install -r requirements.txt
```

---

## File Structure

```
district-compensation-plan/
├── search_urls.py          # Phase 1: URL search (COMPLETE — do not re-run full)
├── remediate.py            # Phase 1: targeted fix for failed rows (run once, done)
├── find_pio_contacts.py    # PIR Track: PIO email discovery (COMPLETE)
├── send_pir.py             # PIR Track: send PIR emails (IN PROGRESS — batches running)
├── requirements.txt
├── HANDOFF.md              # This file
├── .gitignore
├── .env                    # Not committed
├── district-compensation-search-*.json  # Not committed
├── gmail-api.json          # Not committed
├── token.json              # Not committed (auto-generated)
└── logs/                   # Timestamped run logs + empty_run_count.txt (not committed)
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

find_pio_contacts.py write guard: cols D, F, H, J only.
send_pir.py write guard: cols G, N only.

### Current PIR Send Progress (as of 2026-04-13)

- **Total districts:** 188 across 172 email groups (8 groups cover multiple districts)
- **Batch 1 sent:** 40 groups (42 districts) on 2026-04-13
- **Remaining:** 132 groups
- **Daily schedule:** 40 groups/day at 9:05 AM via Claude scheduled task `pir-daily-send`
- **Estimated completion:** ~4 more days

Manual corrections made to batch 1:
- **ACCELERATED INTERMEDIATE ACADEMY** — original aol.com address was dead; resent to `aiapeims2020b@aol.com` (district-provided)
- **BASIS TEXAS** — original `andrea.treesler@basised.com` was dead; resent to `ANDREW.FREEMAN@BTXSCHOOLS.ORG` (CFO, from AskTED backup)
- **WINFREE ACADEMY CHARTER SCHOOLS** — district redirected to `dstaples@wacsd.com`; resent with corrected contact name (Deirdre Staples)
- **UT TYLER UNIVERSITY ACADEMY** — district uses a web portal for PIR submissions; submitted manually; Send_Status set to "Submitted via portal"

### find_pio_contacts.py — COMPLETE

Email discovery is done. All 188 rows have a PIO_Email populated from AskTED.
Do not re-run this script unless a new district is added to PIR_Tracking.

**Commands (for reference):**
```bash
# Validate logic against 4 known charters (no sheet access, uses Serper)
python find_pio_contacts.py --test

# Single district by District_Number
python find_pio_contacts.py --district-number 057829

# Re-process a row (overwrites)
python find_pio_contacts.py --district-number 057829 --force

# Preview without API or sheet writes
python find_pio_contacts.py --dry-run
```

### send_pir.py — IN PROGRESS (batches sending daily)

Sends TPIA-compliant PIR emails via Gmail API. Districts that share the same
PIO email address receive a single grouped email listing all districts — legally
equivalent to separate PIRs, avoids near-identical duplicates in the same inbox.

**Commands:**
```bash
# Show grouping table (who shares an address) — no sends
python send_pir.py --groups

# Preview emails — no sends, no sheet writes
python send_pir.py --dry-run

# Send next batch (capped at 40 groups — runs automatically via scheduler)
python send_pir.py

# Send first N groups only
python send_pir.py --limit 5

# Correct a contact and resend in one atomic operation
python send_pir.py --district-number 57828 \
  --update email=dstaples@wacsd.com full_name="Deirdre Staples" first_name=Deirdre last_name=Staples \
  --force
```

**IMPORTANT — correcting a contact email:**
Always use `--update` rather than editing the sheet manually. It enforces that
email and name fields are updated together. The script blocks an email-only
update with no name fields — this prevents stale salutations going out to the
wrong person's name.

**Salutation logic (in order of priority):**
1. First + Last name in cols L/M → `Dear Jana Coulter,`
2. First name only → `Dear Jana,`
3. No name, known role → role display name (e.g. `Dear Human Resources Director,`)
4. Fallback → `Dear Public Information Officer,`

"School Email (Directory)" and "Manual" never appear verbatim in email text.

**Scheduled task:** `pir-daily-send` runs at 9:05 AM daily via Claude Code
scheduler. Auto-disables after 3 consecutive empty runs. Monitor from the
Scheduled section in the Claude Code sidebar.

**Rate limit:** 40 email groups per run, 2s delay between sends.
Sending from `chase.burns@talos-advisory.com` (Google Workspace, talos-advisory.com).

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
- **Correcting a sent email:** use `send_pir.py --update ... --force`, never
  edit the sheet directly. The guard requires name fields alongside any email
  change to prevent stale salutations.

---

## Known Issues / Limitations

- **UT Tyler University Academy** uses a web portal for PIR submissions rather
  than a direct email address. Any future charter school that responds similarly
  should have its Send_Status set to "Submitted via portal" manually.
- **JS-routed pages**: some district sites use React/Next.js routing and return
  blank HTML to the requests library. Stage 2 crawl misses emails on these.
- **Generic addresses (info@, contact@)**: valid PIR recipients if on the
  district's own domain. Not filtered out.
