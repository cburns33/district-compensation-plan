# Texas District Compensation Plan URL Searcher

Searches Serper.dev for each Texas school district's compensation plan URL,
scores the top 2 results, QA-checks the winner via HTTP HEAD, and writes
everything into columns F–L of your Google Sheet.

---

## Column Layout Reference

| Column | Header           | Contents                                    |
|--------|------------------|---------------------------------------------|
| A      | (existing)       | District homepage URL — used for domain scoring |
| B      | (existing)       | Enrollment                                  |
| C      | (existing)       | District Number                             |
| D      | (existing)       | District Name — used as search term         |
| E      | (existing)       | District Type                               |
| **F**  | Result_1_URL     | Top Serper organic result URL               |
| **G**  | Result_1_Title   | Top Serper organic result title             |
| **H**  | Result_2_URL     | Second Serper organic result URL            |
| **I**  | Result_2_Title   | Second Serper organic result title          |
| **J**  | Best_URL         | Higher-scoring URL (tie goes to Result 1)   |
| **K**  | Best_Score       | e.g. "R1:7pts" or "R1:0pts (no clear winner)" |
| **L**  | QA_Status        | e.g. "✓ PDF", "✓ HTML", "✗ DEAD", etc.     |
| M+     | (existing)       | **NEVER written by this script**            |

---

## Setup

### a. Get a Serper.dev API Key

1. Go to https://serper.dev and create a free account.
2. On the dashboard, copy your API key.
3. The free tier includes **2,500 searches/month** — enough for one full run of ~1,200 districts.

### b. Google Cloud Setup (Sheets API + Service Account)

**Step 1 — Create a project (skip if you already have one)**

1. Go to https://console.cloud.google.com/projectcreate
2. Name the project (e.g. "District Comp Search") and click **Create**.

**Step 2 — Enable the Google Sheets API**

1. Go to https://console.cloud.google.com/apis/library/sheets.googleapis.com
2. Make sure your new project is selected in the top dropdown.
3. Click **Enable**.

**Step 3 — Create a Service Account**

1. Go to https://console.cloud.google.com/iam-admin/serviceaccounts
2. Click **+ Create Service Account**.
3. Give it a name (e.g. "district-search") and click **Done** (no role required).

**Step 4 — Download the JSON credentials**

1. In the Service Accounts list, click the account you just created.
2. Go to the **Keys** tab → **Add Key** → **Create new key** → choose **JSON** → **Create**.
3. A `.json` file downloads automatically. Save it somewhere safe (e.g. your Documents folder).
4. Note the full path to this file — you'll need it for the `.env` file.

**Step 5 — Share your Google Sheet with the service account**

1. Open the service account you created and copy its email address
   (looks like `district-search@your-project.iam.gserviceaccount.com`).
2. Open your Google Sheet.
3. Click **Share** (top right) and paste that email address.
4. Set permission to **Editor** and click **Send**.

### c. Install Dependencies

```bash
pip install -r requirements.txt
```

### d. Fill in the .env File

Copy `.env.example` to `.env` and fill in the three values:

```
SERPER_API_KEY=your_key_here
GOOGLE_SHEETS_CREDS_JSON=C:\Users\yourname\Documents\your-credentials.json
SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

The Spreadsheet ID is the long string in your Sheet's URL:
`https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`

### e. Test Run (first 10 rows)

Verify everything works on a small slice before committing to the full run:

```bash
python search_urls.py --start-row 2 --end-row 11
```

Check that:
- Columns F–L are populated in rows 2–11
- Column M and beyond are untouched
- A log file appeared in the `logs/` folder

### f. Full Run

```bash
python search_urls.py --start-row 2
```

The script will process all rows from row 2 to the last row that has data.
Progress is printed to the console every 10 rows:

```
[Row 42/1200] Midland ISD | R1: ✓ PDF (7pts) | R2: ✓ URL (3pts) | Best: R1 | elapsed: 2m 08s
```

### g. Resuming an Interrupted Run

If the script is interrupted (Ctrl+C, network dropout, etc.), simply re-run the same command.
The script checks column F before processing each row: if F already has a value, that row is
skipped. No duplicate API calls, no overwritten data.

```bash
python search_urls.py --start-row 2
```

### h. Search Credit Usage

Each district = 1 Serper search query.
With ~1,200 Texas districts, a full run uses approximately **1,200 of your 2,500 free monthly credits**,
leaving 1,300 credits for re-runs or other projects.

---

## Scoring Logic

Each of the top 2 search results is scored independently:

| Rule                                  | Points |
|---------------------------------------|--------|
| URL ends with `.pdf` or `.xlsx`       | +3     |
| URL domain matches district homepage  | +2     |
| Contains "2026", "2025-26", "25-26", etc. | +2 |
| Contains compensation/salary keyword  | +1     |
| URL is from a news/social site        | -2     |

The higher-scoring result becomes the Best_URL. Ties go to Result 1.

---

## QA Status Values

| Value                  | Meaning                                       |
|------------------------|-----------------------------------------------|
| `✓ PDF`               | HTTP 200, Content-Type is PDF                 |
| `✓ HTML`              | HTTP 200, Content-Type is HTML                |
| `✓ XLSX`              | HTTP 200, Content-Type is spreadsheet/Excel   |
| `⚠ REDIRECT → url`   | Final URL after redirects differs from Best_URL |
| `✗ DEAD`              | HTTP 4xx or 5xx error                         |
| `✗ TIMEOUT`           | No response within 6 seconds                  |
| `⚠ REVIEW`            | Unexpected content-type or connection error   |

---

## Dry-Run Mode (verify logic before using real API calls)

```bash
python search_urls.py --dry-run
```

Runs the full scoring and QA logic against 3 hardcoded test districts (Austin ISD,
Midland ISD, Lufkin ISD) and prints a detailed breakdown to the console. No API
calls are made and nothing is written to the Sheet. Use this to confirm the script
is installed correctly before spending any Serper credits.

---

## Logs

Every run writes a timestamped log to `logs/run_YYYYMMDD_HHMMSS.log` containing:
- District name and query sent
- URLs found and scores
- QA result
- Time taken per row
- Any errors or retries
