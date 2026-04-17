"""
qa_pipeline.py — QA cleanup pipeline for district compensation plan data

Runs search_urls.py rerun passes in sequence, runs classify_documents.py after
each, and prints a before/after summary of Doc_Class counts.

Usage:
    python qa_pipeline.py                              # full pipeline, all passes
    python qa_pipeline.py --passes dead error          # specific passes only
    python qa_pipeline.py --dry-run                    # preview output, no changes
    python qa_pipeline.py --no-classify                # skip classify between passes

Pass order (default):
    1. dead         — re-search DEAD/TIMEOUT rows
    2. error        — re-search ERROR/REVIEW rows
    3. wrong-domain — re-search WRONG DOMAIN and WRONG PATH rows
    4. wrongdoc     — re-search Wrong Doc rows
    5. html         — re-search HTML rows (attempt to upgrade to PDF)
"""

import argparse
import io
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

# Force UTF-8 on Windows console so box-drawing and warning symbols print correctly
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

load_dotenv()

# ── Pass definitions ──────────────────────────────────────────────────────────

PASS_ORDER = ["dead", "error", "wrong-domain", "wrongdoc", "html"]

PASS_FLAGS = {
    "dead":         "--rerun-dead",
    "error":        "--rerun-error",
    "wrong-domain": "--rerun-wrong-domain",
    "wrongdoc":     "--rerun-wrongdoc",
    "html":         "--rerun-html",
}

PASS_LABELS = {
    "dead":         "Re-search DEAD/TIMEOUT rows",
    "error":        "Re-search ERROR/REVIEW rows",
    "wrong-domain": "Re-search WRONG DOMAIN/PATH rows",
    "wrongdoc":     "Re-search Wrong Doc rows",
    "html":         "Re-search HTML rows (upgrade to PDF)",
}

# Doc_Class display order for the summary table
CLASS_ORDER = ["Simple", "Medium", "Complex", "HTML", "Unreadable", "Wrong Doc",
               "Skipped", "Error", ""]

COL_DOC_CLASS = 16  # P (1-based)

# ── Sheets ────────────────────────────────────────────────────────────────────

def open_sheet():
    creds_path = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
    sheet_id   = os.getenv("SPREADSHEET_ID")
    if not (creds_path and sheet_id):
        return None
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id).worksheet("Unique Districts")


def get_doc_class_counts(ws) -> Counter:
    """Return a Counter of Doc_Class values across all data rows."""
    data = ws.get_all_values()
    return Counter(
        row[COL_DOC_CLASS - 1].strip() if len(row) >= COL_DOC_CLASS else ""
        for row in data[1:]   # skip header
    )


# ── Subprocess runner ─────────────────────────────────────────────────────────

def run_script(cmd: list[str]) -> tuple[int, list[str]]:
    """
    Run a command, stream its stdout to console, return (returncode, lines).
    stderr is merged into stdout so logger output is captured.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    lines = []
    for line in proc.stdout:
        stripped = line.rstrip("\n")
        print(f"    {stripped}")
        lines.append(stripped)
    proc.wait()
    return proc.returncode, lines


# ── Output parsers ────────────────────────────────────────────────────────────

def parse_search_stats(lines: list[str]) -> dict:
    """
    Parse search_urls.py output for pass statistics.

    Expected patterns:
      "N rows to re-search"   / "N ... rows to re-search"
      "Done. Processed N rows, skipped M."
      WARNING lines for WRONG DOMAIN / WRONG PATH
    """
    stats = {
        "targeted":     0,
        "processed":    0,
        "wrong_domain": 0,
        "wrong_path":   0,
    }
    for line in lines:
        m = re.search(r"(\d+) .{0,40}?rows? to re-search", line)
        if m:
            stats["targeted"] = int(m.group(1))
        m = re.search(r"Done\. Processed (\d+) rows", line)
        if m:
            stats["processed"] = int(m.group(1))
        if "WARNING" in line and "WRONG DOMAIN" in line:
            stats["wrong_domain"] += 1
        if "WARNING" in line and "WRONG PATH" in line:
            stats["wrong_path"] += 1
    return stats


def parse_classify_stats(lines: list[str]) -> dict:
    """
    Parse classify_documents.py output for pass statistics.

    Expected pattern:
      "Done. Classified: X | Skipped (resume): Y | Skipped (QA/no URL): Z"
    """
    stats = {"classified": 0, "skipped_resume": 0, "skipped_qa": 0}
    for line in lines:
        m = re.search(
            r"Done\. Classified:\s*(\d+).*?Skipped \(resume\):\s*(\d+).*?Skipped \(QA/no URL\):\s*(\d+)",
            line,
        )
        if m:
            stats["classified"]    = int(m.group(1))
            stats["skipped_resume"] = int(m.group(2))
            stats["skipped_qa"]     = int(m.group(3))
    return stats


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_pass_search_summary(stats: dict, elapsed: float) -> None:
    print(f"\n  Search summary:")
    print(f"    Targeted:   {stats['targeted']}")
    print(f"    Processed:  {stats['processed']}")
    if stats["wrong_domain"]:
        print(f"    Still WRONG DOMAIN: {stats['wrong_domain']}")
    if stats["wrong_path"]:
        print(f"    Still WRONG PATH:   {stats['wrong_path']}")
    m, s = divmod(int(elapsed), 60)
    print(f"    Elapsed:    {m}m {s:02d}s")


def print_pass_classify_summary(stats: dict) -> None:
    print(f"\n  Classify summary:")
    print(f"    Classified:      {stats['classified']}")
    print(f"    Skipped (QA):    {stats['skipped_qa']}")


def print_final_table(before: Counter, after: Counter) -> None:
    all_classes = set(list(before.keys()) + list(after.keys()))
    ordered = [c for c in CLASS_ORDER if c in all_classes]
    ordered += sorted(c for c in all_classes if c not in CLASS_ORDER)

    print(f"\n  {'Doc_Class':<14}  {'Before':>7}  {'After':>7}  {'Delta':>7}")
    print(f"  {'-'*14}  {'-'*7}  {'-'*7}  {'-'*7}")
    for cls in ordered:
        b = before.get(cls, 0)
        a = after.get(cls, 0)
        d = a - b
        delta_str = f"+{d}" if d > 0 else (str(d) if d < 0 else "")
        label = cls if cls else "(blank)"
        print(f"  {label:<14}  {b:>7}  {a:>7}  {delta_str:>7}")
    print()
    total_b = sum(before.values())
    total_a = sum(after.values())
    d = total_a - total_b
    delta_str = f"+{d}" if d > 0 else (str(d) if d < 0 else "")
    print(f"  {'Total':<14}  {total_b:>7}  {total_a:>7}  {delta_str:>7}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="QA cleanup pipeline for district compensation plan data"
    )
    parser.add_argument(
        "--passes", nargs="+",
        choices=PASS_ORDER,
        metavar="PASS",
        default=PASS_ORDER,
        help=f"Passes to run in order (choices: {', '.join(PASS_ORDER)}; default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to both scripts (hardcoded mock data, no API/sheet writes)",
    )
    parser.add_argument(
        "--no-classify", action="store_true",
        help="Skip classify_documents.py between passes",
    )
    args = parser.parse_args()

    python = sys.executable

    print("=" * 62)
    print("QA Pipeline — Texas District Compensation Plans")
    print(f"Passes : {', '.join(args.passes)}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 62)

    # Snapshot sheet state before any changes
    ws = None
    counts_before = Counter()
    if GSPREAD_AVAILABLE and not args.dry_run:
        print("\nConnecting to Google Sheets for baseline snapshot...")
        ws = open_sheet()
        if ws:
            counts_before = get_doc_class_counts(ws)
            total = sum(counts_before.values())
            print(f"  Baseline: {total} rows loaded")
        else:
            print("  WARNING: Could not open sheet — final summary will be skipped")

    any_processed = False

    for pass_name in args.passes:
        flag  = PASS_FLAGS[pass_name]
        label = PASS_LABELS[pass_name]

        print(f"\n{'─' * 62}")
        print(f"Pass: {label}")
        print(f"{'─' * 62}")

        # Build and run search_urls command
        search_cmd = [python, "search_urls.py", flag]
        if args.dry_run:
            search_cmd.append("--dry-run")

        t0 = time.time()
        rc, lines = run_script(search_cmd)
        elapsed = time.time() - t0
        search_stats = parse_search_stats(lines)
        print_pass_search_summary(search_stats, elapsed)

        if rc != 0:
            print(f"\n  WARNING: search_urls.py exited with code {rc}. Continuing.")

        if search_stats["targeted"] == 0:
            print("  No rows matched — skipping classify for this pass.")
            continue

        any_processed = True

        if not args.no_classify:
            classify_cmd = [python, "classify_documents.py"]
            if args.dry_run:
                classify_cmd.append("--dry-run")

            rc, lines = run_script(classify_cmd)
            classify_stats = parse_classify_stats(lines)
            print_pass_classify_summary(classify_stats)

            if rc != 0:
                print(f"\n  WARNING: classify_documents.py exited with code {rc}. Continuing.")

    # Final summary
    print(f"\n{'=' * 62}")
    print("Pipeline Complete")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if ws and any_processed and not args.dry_run:
        print("\nDoc_Class before / after:")
        counts_after = get_doc_class_counts(ws)
        print_final_table(counts_before, counts_after)
    elif args.dry_run:
        print("(dry-run — no sheet changes were made)")
    elif not any_processed:
        print("(no rows were processed in any pass)")

    print()


if __name__ == "__main__":
    main()
