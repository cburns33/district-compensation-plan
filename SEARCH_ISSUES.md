# Search Quality — Timeline and Current State

> Focused doc on the URL search problem. See HANDOFF.md for full project context.

---

## The Core Challenge

We are searching for each of ~894 Texas school districts' compensation plan pages using
the Brave Search API, then scoring and QA-checking the top result. The fundamental
difficulty: search engines treat query text as loosely matching, so a query for
"Bandera ISD salary schedule" may return a high-authority document from
Northside ISD (nisd.net) or Fort Worth ISD (fwisd.org) before returning anything
from banderaisd.net. Large district domains have massive link authority advantages.

---

## Timeline of Changes

### Phase 1 — Basic search (April 10)

- Single Brave query: `{District Name} compensation plan OR "pay scale" OR "salary schedule" 2026 OR "25-26"`
- Scoring: domain match +2, year +2, keyword +1, news/social -2
- Result: ~50% success. Northside ISD (nisd.net) dominated many unrelated districts.

### Phase 2 — Scoring model rewrite (April 16)

- Switched primary signal to **filename keyword** vs full URL+title+snippet.
- Added: +3 PDF/XLSX, +2 keyword in filename, -3 wrong-doc filename, -3 Scribd.
- Also added `filetype:pdf` suffix to queries (later removed — it filtered out correct HTML pages).
- Added Texas in query string.
- Result: better separation between salary docs and handbooks, but domain authority
  problem persisted.

### Phase 3 — Right-domain preference + parallel site search (April 17-18)

- Added `site:{district_homepage}` scoped search running in parallel with the main query.
- Added right-domain preference: if ANY result from the district's own domain scores >= 0,
  it wins regardless of how high a wrong-domain result scores. This was the primary fix
  for the Northside ISD domination problem.
- Added Tier-3: if the best result is on the wrong domain and the site-scoped search
  wasn't already run, fires `site:{domain}` as a fallback.
- Added Tier-4: crawls the district homepage directly when Tier-3 also fails.
- Result: jumped from ~50% to ~75% on 20-row test.

### Phase 4 — Gemini grounding (April 18)

- Added `gemini-2.5-flash` with Google Search grounding as a third search backend.
- Gemini returns proxy redirect URLs that must be resolved via HEAD requests (parallelized).
- Cost concern: $35/1,000 calls × 894 rows = ~$31. Made it conditional — only fires when
  Brave + site-scoped search found no right-domain result (~25-30% of rows).
- Budget exhaustion guard: if Gemini returns RESOURCE_EXHAUSTED or 429, it trips a
  circuit breaker and is skipped for the rest of the run.
- Result: ~85% on 20-row test, but introduced a wrong-district Tier-1.5 upgrade bug.

### Phase 5 — CDN penalty + dynamic district check (April 18)

- Added -5 penalty for CDN URLs whose path token belongs to another district
  (e.g., `/houstonisd/` on Finalsite when searching for Sheldon ISD).
- **Bug introduced:** the penalty was applied without checking which district is currently
  being searched. Houston ISD's own Finalsite paths got penalized when searching FOR
  Houston ISD. Fixed: penalty skips when the path token matches the current district's
  domain stem.
- Added `_is_right_domain()` helper: treats a CDN URL as "right domain" if its path token
  matches the current district (e.g., `/houstonisd/` = right domain when searching HISD).
- Fixed same district-awareness bug in `_is_wrong_cdn` (Tier-3/4 trigger) and the
  QA WRONG PATH guard.
- Result: ~74% clean on 20-row test.

### Phase 6 — Dead-URL fallback + QA fixes (April 18)

- QA `HEAD` requests fail on many district servers (bot-blocking). Added GET retry when
  HEAD returns 4xx. Fixes districts like Garland ISD whose servers reject HEAD.
- Added right-domain trust: if the selected URL is on the correct district domain and QA
  fails, trust it as `✓ HTML` rather than marking DEAD. (Many district servers block all
  HTTP clients but pages are live in browsers.) Only applies to HTML pages, not PDFs
  (a dead PDF is genuinely dead).
- Added dead-URL fallback: when the selected URL is dead AND it is NOT on the right domain,
  try the next 5 scored candidates until one passes QA.
- Result: 16/19 clean on 20-row test. Zero DEAD results.

### Phase 7 — Homepage verification (April 18-19)

- Column A (District_Web_Address) comes from AskTED and is several years old.
- A stale homepage URL means our right-domain preference rejects correct results as
  "wrong domain." Fort Bend ISD is the confirmed example: col A has `fortbend.k12.tx.us`
  but the district's actual site is `fortbendisd.com`.
- Built `verify_homepages.py`: searches Brave for each district's official website and
  writes match/mismatch to **column AL**.
- Full 894-row run complete. Results: 796 match, 74 mismatch, 3 no-result.

---

## Current State (as of April 23)

### What is working well

- 16/19 (84%) clean results on top 20 rows (by enrollment).
- No DEAD results on large districts after dead-URL fallback.
- Gemini running conditionally keeps API costs within budget (~$8-9 estimated for full run).
- Site-scoped parallel search reliably finds district-hosted documents Brave misses.

### Remaining QA statuses after 20-row test

| Status | Rows | Notes |
|--------|------|-------|
| ✓ PDF | 8 | Clean |
| ✓ HTML | 6 | Clean — Tier-2 crawl may upgrade some to PDF |
| ⚠ WRONG DOMAIN | 2 | Rows 7, 12 — see below |
| ⚠ REDIRECT | 2 | Rows 15, 21 — land on a news/about page |
| ✗ DEAD | 0 | |

### Known remaining issues

**Row 7 (Fort Bend ISD):** Col A has `fortbend.k12.tx.us` but district uses `fortbendisd.com`.
Column AL shows the mismatch. Fix: update col A row 7 with `https://www.fortbendisd.com/`,
then rerun row 7. This is one of the 74 mismatches waiting for review.

**Row 12 (North East ISD):** False positive WRONG DOMAIN. Tier-2 correctly crawled the
neisd.net pay schedules page and extracted a CloudFront CDN link
(`d16k74nzx9emoe.cloudfront.net/...25-26-CERTIFIED-PaySchedules.pdf`). The QA domain guard
doesn't know this CDN URL came from crawling the right source page, so it flags it.
Fix: the CDN URL path contains a GUID, not a district identifier, so the existing
`_is_right_domain()` logic can't verify it. Options: (a) accept the Tier-2 crawl result
as trusted by definition, or (b) check that the Tier-2 crawl source page was on the
right domain.

**Rows 15, 21 (Arlington ISD, Round Rock ISD):** Selected URL redirects to a news/about
page rather than salary schedule. The landing page exists but the direct salary link
wasn't found. Tier-2 crawl probably didn't find a link on the redirect destination.

**74 stale homepages in col A:** Until these are corrected, those districts will have
degraded right-domain preference. High-priority corrections are rows where col A has
a shared hosting platform (esc14.net, esc17.net, txed.net, thrillshare.com) rather
than the district's own domain.

---

## What Needs to Happen Next

1. **Review column AL mismatches** and update column A for confirmed stale homepages.
   Priority: any row where col A is `esc*.net`, `txed.net`, `thrillshare.com`, or
   another shared platform — those are almost certainly wrong.

2. **Run the full 894-row search:**
   ```bash
   python search_urls.py
   ```
   Expect ~25 min at current speed (~1.7s/row). Gemini budget ~$8-9.

3. **QA pipeline** after the full run:
   ```bash
   python qa_pipeline.py
   ```

4. **Classify:**
   ```bash
   python classify_documents.py
   ```

5. **Re-examine REDIRECT rows** — Arlington and Round Rock redirect to non-salary pages.
   May need a smarter crawl or manual lookup.

---

## Scoring Model (current)

| Signal | Points |
|--------|--------|
| PDF or XLSX file type | +3 |
| Domain matches district homepage (or own CDN path) | +2 |
| Year pattern (2026, 25-26, etc.) in URL/title/snippet | +2 |
| Salary/compensation keyword in filename | +2 |
| Salary/compensation keyword in title/snippet only | +1 |
| News/social domain | -2 |
| Scribd URL | -3 |
| Wrong-doc keyword in filename | -3 |
| Wrong-doc keyword in title/snippet only | -2 |
| Wrong-district CDN path (another district's token) | -5 |

**Best-result selection:** prefer any right-domain result with score >= 0 over
higher-scoring wrong-domain results. If no right-domain result, take global max.

**Backends (in parallel):**
- R1-R10: Brave general search
- S1-S10: Brave site-scoped search (always, when homepage known)
- G1-Gn: Gemini 2.5 Flash grounding (conditional — only when no right-domain result found)

---

## Files

| File | Purpose |
|------|---------|
| `search_urls.py` | Main search pipeline |
| `verify_homepages.py` | One-time homepage verification — writes to col AL |
| `qa_pipeline.py` | Orchestrated QA cleanup passes |
| `classify_documents.py` | Document classification after search |

---

## Key Constants in search_urls.py

- `KNOWN_DOC_CDNS` — CDN domains that legitimately host district documents
  (Finalsite, Thrillshare, CloudFront, etc.)
- `WRONG_CDN_PATHS` — Path tokens that identify a specific district's CDN folder
  (e.g. `houstonisd`, `fwisdorg`). Expand as new contaminants are found.
- `FALLBACK_SCORE_THRESHOLD` — Score at or below which Tier-1.5 fires (default: 3)
