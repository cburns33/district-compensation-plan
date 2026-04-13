"""
send_pir.py — Send Public Information Request emails to Texas charter school districts.

Reads PIO_FOUND rows from the PIR_Tracking Google Sheet, builds personalized
TPIA-compliant PIR emails, and sends via Gmail API. Districts that share the
same PIO email address receive a single grouped email listing all districts —
this avoids near-identical duplicates hitting the same inbox and is legally
equivalent to separate requests (each entity is named explicitly).

Writes Date_Sent (col G) and Send_Status (col N) back to the sheet.

Prerequisites:
    1. gmail-api.json in project root (OAuth Desktop client from Google Cloud Console)
    2. .env with GMAIL_SENDER_EMAIL, PIR_SENDER_NAME, GOOGLE_SHEETS_CREDS_JSON, SPREADSHEET_ID
    3. pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib

First run opens a browser for Gmail OAuth — token.json is saved for future runs.

Usage:
    python send_pir.py --dry-run               # preview emails, no sends, no sheet writes
    python send_pir.py --limit 5               # send first 5 unsent groups
    python send_pir.py --district-number 057829  # single district (always single email)
    python send_pir.py                         # full run (resumes from unsent rows)
"""

import argparse
import base64
import io
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

# Force UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

try:
    import gspread
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    GMAIL_AVAILABLE = True
except ImportError:
    GMAIL_AVAILABLE = False


# ── Constants ─────────────────────────────────────────────────────────────────

TRACKING_TAB      = "PIR_Tracking"
SHEETS_SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]
GMAIL_SCOPES      = ["https://www.googleapis.com/auth/gmail.send"]
TOKEN_FILE        = "token.json"

SEND_DELAY_SECS   = 2.0   # seconds between sends
MAX_PER_DAY       = 40    # hard cap per run (groups count as 1 each)

# PIR_Tracking column indices (1-based, verify against your sheet)
COL_DISTRICT_NUMBER  = 1   # A — read only
COL_DISTRICT_NAME    = 2   # B — read only
COL_WEB_ADDRESS      = 3   # C — read only
COL_PIO_EMAIL        = 4   # D — read only
COL_EMAIL_ROLE       = 5   # E — read only (e.g. "Human Resources", "CFO/Business Manager")
COL_PIO_SOURCE       = 6   # F — read only
COL_DATE_SENT        = 7   # G — WRITTEN by this script
COL_STATUS           = 8   # H — read only (PIO_FOUND / PIO_NOT_FOUND)
COL_RESPONSE_URL     = 9   # I — read only (manual)
COL_NOTES            = 10  # J — read only
COL_FULL_NAME        = 11  # K — read only
COL_FIRST_NAME       = 12  # L — read only
COL_LAST_NAME        = 13  # M — read only
COL_SEND_STATUS      = 14  # N — WRITTEN by this script

WRITE_COLS = {COL_DATE_SENT, COL_SEND_STATUS}


# ── Salutation logic ──────────────────────────────────────────────────────────

# Maps Email_Role values to a clean salutation title.
# "School Email (Directory)" and "Manual" must never appear verbatim in emails.
ROLE_SALUTATION = {
    "Human Resources":              "Human Resources Director",
    "CFO/Business Manager":         "Chief Financial Officer",
    "Secretary to Superintendent":  "Superintendent's Office",
    "Asst. Superintendent":         "Assistant Superintendent",
    "Assoc. Superintendent":        "Associate Superintendent",
    "Deputy Superintendent":        "Deputy Superintendent",
    "PEIMS Coordinator":            "PEIMS Coordinator",
    "School Email (Directory)":     "Public Information Officer",
    "Manual":                       "Public Information Officer",
}

DEFAULT_SALUTATION = "Public Information Officer"


def resolve_salutation(first_name: str, last_name: str, email_role: str) -> str:
    """
    Return the salutation string (no 'Dear' prefix, no comma).

    Priority:
    1. First + Last name  →  "Jana Coulter"
    2. First name only    →  "Jana"
    3. No name, known role  →  role display name from ROLE_SALUTATION
    4. Fallback  →  "Public Information Officer"
    """
    first = (first_name or "").strip()
    last  = (last_name  or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    return ROLE_SALUTATION.get((email_role or "").strip(), DEFAULT_SALUTATION)


# ── Email building ─────────────────────────────────────────────────────────────

def format_date() -> str:
    """Return today's date as 'April 13, 2026'."""
    dt = datetime.now()
    # %-d doesn't work on Windows; strip the leading zero manually
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}"


def build_single_email(
    district_name: str,
    district_number: str,
    salutation_name: str,
    sender_name: str,
    sender_email: str,
) -> tuple[str, str, str]:
    """Build subject, plain body, HTML body for a single-district PIR."""
    today   = format_date()
    subject = f"Public Information Request — {district_name} 2025-26 Salary Schedule"

    plain = f"""{today}

Officer for Public Information
{district_name}

Re: Public Information Request pursuant to Texas Government Code,
    Chapter 552 (Texas Public Information Act)

Dear {salutation_name},

This request is made under the Texas Public Information Act, Chapter 552,
Texas Government Code, which guarantees the public's access to information
in the custody of governmental agencies.

I hereby request the following records from {district_name}:

  1. The teacher salary schedule or pay scale in effect for the 2025-26
     school year, including all steps, lanes, or columns reflecting
     years of experience and/or educational attainment.

  2. The complete compensation plan or compensation manual for the
     2025-26 school year, if maintained separately from the above.

  3. Any board-adopted stipend schedule or supplemental pay table for
     classroom teachers for the 2025-26 school year.

Under Texas Government Code § 552.022(a)(2), salary information of
governmental body employees is expressly designated as public information
and may not be withheld under any general exemption.

Please provide responsive records in electronic format (PDF or Excel
preferred) via reply to this email address. If {district_name} is unable
to produce the records within 10 business days, please notify me in
writing of the specific date and time they will be available, as required
by § 552.221.

If you have any questions about this request, please contact me at the
address below.

Respectfully,

{sender_name}
{sender_email}"""

    html = _wrap_html(plain)
    return subject, plain, html


def build_grouped_email(
    districts: list[dict],
    salutation_name: str,
    sender_name: str,
    sender_email: str,
) -> tuple[str, str, str]:
    """
    Build subject, plain body, HTML body for a multi-district PIR.

    All districts share the same PIO email. Each district is listed explicitly
    so each request is unambiguous. Legally equivalent to separate PIRs.
    """
    today = format_date()
    n     = len(districts)

    # Subject: "Public Information Request — 2025-26 Salary Schedules (3 Districts)"
    subject = f"Public Information Request — 2025-26 Salary Schedules ({n} Districts)"

    # Build the district list block
    # Strip trailing .0 / .00 from district numbers stored as floats in the sheet
    def _clean_num(n: str) -> str:
        n = n.replace(",", "").strip()
        try:
            return str(int(float(n))) if n else n
        except ValueError:
            return n

    district_lines = "\n".join(
        f"  {i+1}. {d['District_Name']} (District #{_clean_num(d['District_Number'])})"
        for i, d in enumerate(districts)
    )

    plain = f"""{today}

Officer for Public Information
[See district list below]

Re: Public Information Request pursuant to Texas Government Code,
    Chapter 552 (Texas Public Information Act)

Dear {salutation_name},

This request is made under the Texas Public Information Act, Chapter 552,
Texas Government Code, which guarantees the public's access to information
in the custody of governmental agencies.

I hereby request the following records from each of the Texas charter school
districts listed below:

{district_lines}

For each district listed above, I am requesting:

  a. The teacher salary schedule or pay scale in effect for the 2025-26
     school year, including all steps, lanes, or columns reflecting
     years of experience and/or educational attainment.

  b. The complete compensation plan or compensation manual for the
     2025-26 school year, if maintained separately from the above.

  c. Any board-adopted stipend schedule or supplemental pay table for
     classroom teachers for the 2025-26 school year.

Under Texas Government Code § 552.022(a)(2), salary information of
governmental body employees is expressly designated as public information
and may not be withheld under any general exemption.

Please provide responsive records for each district in electronic format
(PDF or Excel preferred) via reply to this email address. If any of the
listed districts is unable to produce records within 10 business days,
please notify me in writing of the specific date and time they will be
available, as required by § 552.221.

If you have any questions about this request, please contact me at the
address below.

Respectfully,

{sender_name}
{sender_email}"""

    html = _wrap_html(plain)
    return subject, plain, html


def _wrap_html(plain_body: str) -> str:
    return (
        "<html><body>"
        '<pre style="white-space:pre-wrap; font-family:Arial, sans-serif; '
        'font-size:14px; line-height:1.5">'
        f"{plain_body}"
        "</pre></body></html>"
    )


def build_mime_message(
    sender_email: str,
    recipient_email: str,
    subject: str,
    plain_body: str,
    html_body: str,
) -> str:
    """Return base64url-encoded MIME message for the Gmail API."""
    msg = MIMEMultipart("alternative")
    msg["From"]     = sender_email
    msg["To"]       = recipient_email
    msg["Subject"]  = subject
    msg["Reply-To"] = sender_email
    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body,  "html",  "utf-8"))
    return base64.urlsafe_b64encode(msg.as_bytes()).decode()


# ── Gmail auth ────────────────────────────────────────────────────────────────

def get_gmail_service(credentials_json_path: str):
    """
    Authenticate with Gmail API using OAuth 2.0 Desktop flow.
    Loads token.json if it exists; refreshes if expired; opens browser if needed.
    """
    creds      = None
    token_path = Path(__file__).parent / TOKEN_FILE

    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)
        except Exception as e:
            print(f"WARNING: Could not load {TOKEN_FILE}: {e}")
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            print(f"Token refresh failed. Delete {TOKEN_FILE} and re-run to re-authenticate.")
            sys.exit(1)
    elif not creds or not creds.valid:
        if not Path(credentials_json_path).exists():
            print(f"ERROR: Gmail credentials file not found: {credentials_json_path}")
            print("Download it from Google Cloud Console > APIs & Services > Credentials")
            sys.exit(1)
        flow  = InstalledAppFlow.from_client_secrets_file(credentials_json_path, GMAIL_SCOPES)
        creds = flow.run_local_server(port=0)

    token_path.write_text(creds.to_json())
    return build("gmail", "v1", credentials=creds)


# ── Google Sheets helpers ─────────────────────────────────────────────────────

def col_letter(col_1based: int) -> str:
    result = ""
    n = col_1based
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _assert_col_writable(col: int) -> None:
    if col not in WRITE_COLS:
        raise RuntimeError(
            f"SAFETY HALT: attempted write to column {col}. "
            f"Allowed write columns: {sorted(WRITE_COLS)} "
            f"({col_letter(COL_DATE_SENT)}, {col_letter(COL_SEND_STATUS)}). Halting."
        )


def open_tracking_sheet(creds_path: str, spreadsheet_id: str):
    creds  = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(TRACKING_TAB)


def load_tracking_rows(sheet) -> list[dict]:
    all_values = sheet.get_all_values()
    if not all_values:
        return []
    headers = all_values[0]
    rows = []
    for i, row in enumerate(all_values[1:], start=2):
        while len(row) < len(headers):
            row.append("")
        d = dict(zip(headers, row))
        d["_row"] = i
        rows.append(d)
    return rows


def write_send_result(
    sheet,
    sheet_rows: list[int],
    date_sent: str,
    send_status: str,
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """Write Date_Sent (G) and Send_Status (N) for each sheet row in the list."""
    _assert_col_writable(COL_DATE_SENT)
    _assert_col_writable(COL_SEND_STATUS)

    if dry_run:
        for r in sheet_rows:
            print(f"  [DRY-RUN] Row {r}: Date_Sent={date_sent!r}  Send_Status={send_status!r}")
        return

    data = []
    for r in sheet_rows:
        data.append({"range": f"{col_letter(COL_DATE_SENT)}{r}",   "values": [[date_sent]]})
        data.append({"range": f"{col_letter(COL_SEND_STATUS)}{r}", "values": [[send_status]]})
    sheet.batch_update(data, value_input_option="USER_ENTERED")
    logger.debug("Wrote %d rows: Date_Sent=%s Send_Status=%s", len(sheet_rows), date_sent, send_status)


# ── Grouping ──────────────────────────────────────────────────────────────────

def normalize_district_number(n: str) -> str:
    """Strip commas and trailing .0/.00 so '101,849.00' == '101849'."""
    n = n.replace(",", "").strip()
    try:
        return str(int(float(n)))
    except ValueError:
        return n


def group_by_email(rows: list[dict]) -> list[list[dict]]:
    """
    Group rows by PIO_Email, preserving first-appearance order of each address.
    Returns a list of groups (each group is a list of one or more rows).
    """
    seen:   dict[str, list[dict]] = {}
    order:  list[str]             = []
    for row in rows:
        email = row.get("PIO_Email", "").strip().lower()
        if email not in seen:
            seen[email] = []
            order.append(email)
        seen[email].append(row)
    return [seen[e] for e in order]


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logger(dry_run: bool) -> logging.Logger:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = "_dryrun" if dry_run else ""
    log_path = logs_dir / f"pir_{ts}{suffix}.log"

    logger = logging.getLogger("send_pir")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Log: %s", log_path)
    return logger


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Send TPIA PIR emails to Texas charter school districts."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview emails to console; no sends, no sheet writes",
    )
    parser.add_argument(
        "--groups", action="store_true",
        help="Print grouping summary (who shares an address) then exit — no emails, no sheet writes",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process at most N email groups (not N districts)",
    )
    parser.add_argument(
        "--district-number", type=str, default=None,
        help="Send to a single district by District_Number (always a single email, not grouped)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-send even if Date_Sent is already populated (use with --district-number for corrections)",
    )
    parser.add_argument(
        "--update", nargs="+", metavar="FIELD=VALUE",
        help=(
            "Update sheet fields before sending. Always use this instead of editing the sheet "
            "manually when correcting a contact, so email and name stay in sync. "
            "Supported fields: email, full_name, first_name, last_name. "
            "Example: --district-number 57828 --update email=dstaples@wacsd.com "
            "full_name='Deirdre Staples' first_name=Deirdre last_name=Staples --force"
        ),
    )
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args   = parse_args()
    logger = setup_logger(args.dry_run)

    load_dotenv()

    sender_email    = os.environ.get("GMAIL_SENDER_EMAIL",     "").strip()
    sender_name     = os.environ.get("PIR_SENDER_NAME",        "").strip()
    gmail_creds_raw = os.environ.get("GMAIL_CREDENTIALS_JSON", "gmail-api.json").strip()
    creds_path_raw  = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    spreadsheet_id  = os.environ.get("SPREADSHEET_ID",         "").strip()

    script_dir  = Path(__file__).parent
    gmail_creds = str(script_dir / gmail_creds_raw if not Path(gmail_creds_raw).is_absolute() else Path(gmail_creds_raw))
    creds_path  = str(script_dir / creds_path_raw  if creds_path_raw and not Path(creds_path_raw).is_absolute() else Path(creds_path_raw or ""))

    missing = []
    if not sender_email:   missing.append("GMAIL_SENDER_EMAIL")
    if not sender_name:    missing.append("PIR_SENDER_NAME")
    if not spreadsheet_id: missing.append("SPREADSHEET_ID")
    if not creds_path_raw: missing.append("GOOGLE_SHEETS_CREDS_JSON")
    if missing:
        print(f"ERROR: Missing .env vars: {', '.join(missing)}")
        sys.exit(1)

    if not GSPREAD_AVAILABLE:
        print("ERROR: gspread not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    if not args.dry_run and not GMAIL_AVAILABLE:
        print("ERROR: Gmail API libraries not installed.")
        print("Run: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
        sys.exit(1)

    # Load sheet
    logger.info("Opening PIR_Tracking sheet")
    sheet = open_tracking_sheet(creds_path, spreadsheet_id)
    rows  = load_tracking_rows(sheet)
    logger.info("Loaded %d rows from PIR_Tracking", len(rows))

    # Apply --update field writes before doing anything else
    if args.update:
        if not args.district_number:
            print("ERROR: --update requires --district-number.")
            sys.exit(1)
        matching_u = [r for r in rows if normalize_district_number(r.get("District_Number", "")) == normalize_district_number(args.district_number)]
        if not matching_u:
            print(f"ERROR: District_Number {args.district_number!r} not found.")
            sys.exit(1)
        sheet_row_u = matching_u[0]["_row"]

        field_map = {
            "email":      col_letter(COL_PIO_EMAIL),
            "full_name":  col_letter(COL_FULL_NAME),
            "first_name": col_letter(COL_FIRST_NAME),
            "last_name":  col_letter(COL_LAST_NAME),
        }
        updates = []
        parsed_fields = {}
        for token in args.update:
            if "=" not in token:
                print(f"ERROR: --update values must be FIELD=VALUE, got {token!r}")
                sys.exit(1)
            field, value = token.split("=", 1)
            field = field.lower().strip()
            if field not in field_map:
                print(f"ERROR: Unknown --update field {field!r}. Supported: {', '.join(field_map)}")
                sys.exit(1)
            parsed_fields[field] = value
            updates.append({"range": f"{field_map[field]}{sheet_row_u}", "values": [[value]]})

        # If email is being changed, require name fields too — this is the whole point
        if "email" in parsed_fields and not {"full_name", "first_name", "last_name"}.issubset(parsed_fields):
            missing_name_fields = [f for f in ("full_name", "first_name", "last_name") if f not in parsed_fields]
            print(
                f"ERROR: Updating email without updating name fields ({', '.join(missing_name_fields)}) "
                f"would leave a stale salutation. Either provide all name fields or set them to empty "
                f"strings (e.g. full_name= first_name= last_name=) to fall back to role-based salutation."
            )
            sys.exit(1)

        # Also clear Date_Sent and Send_Status so the corrected record gets a fresh send
        updates.append({"range": f"{col_letter(COL_DATE_SENT)}{sheet_row_u}",   "values": [[""]]})
        updates.append({"range": f"{col_letter(COL_SEND_STATUS)}{sheet_row_u}", "values": [[""]]})

        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        for field, value in parsed_fields.items():
            print(f"  Updated {field}: {value!r}")
        print(f"  Date_Sent and Send_Status cleared.")

        # Reload rows so the send loop sees the updated values
        rows = load_tracking_rows(sheet)

    # Filter: PIO_FOUND + no Date_Sent yet
    if args.district_number:
        # Single-district mode: bypass grouping, send just this one
        matching = [r for r in rows if normalize_district_number(r.get("District_Number", "")) == normalize_district_number(args.district_number)]
        if not matching:
            print(f"ERROR: District_Number {args.district_number!r} not found in PIR_Tracking.")
            sys.exit(1)
        row = matching[0]
        if row.get("Status", "").strip() != "PIO_FOUND":
            print(f"ERROR: District {args.district_number} status is {row.get('Status')!r}, not PIO_FOUND.")
            sys.exit(1)
        if row.get("Date_Sent", "").strip() and not args.force:
            print(f"District {args.district_number} already sent on {row['Date_Sent']}. Add --force to resend.")
            sys.exit(1)
        groups = [[row]]
    else:
        eligible = [
            r for r in rows
            if r.get("Status", "").strip() == "PIO_FOUND"
            and not r.get("Date_Sent", "").strip()
        ]
        skipped_no_pio = sum(1 for r in rows if r.get("Status", "").strip() != "PIO_FOUND")
        skipped_sent   = sum(1 for r in rows if r.get("Status", "").strip() == "PIO_FOUND" and r.get("Date_Sent", "").strip())

        groups = group_by_email(eligible)

        multi = sum(1 for g in groups if len(g) > 1)
        print(f"Eligible districts: {len(eligible)}  |  Email groups: {len(groups)}  ({multi} grouped)")
        if skipped_no_pio:  print(f"  Skipped (no PIO found):  {skipped_no_pio}")
        if skipped_sent:    print(f"  Skipped (already sent):  {skipped_sent}")

        if not groups:
            print("Nothing to send.")
            return

        if args.groups:
            print()
            print(f"{'#':<4}  {'Type':<8}  {'Email':<45}  Districts")
            print("─" * 100)
            for i, group in enumerate(groups, 1):
                email = group[0].get("PIO_Email", "").strip()
                gtype = "GROUPED" if len(group) > 1 else "single"
                names = ", ".join(r.get("District_Name", "").strip() for r in group)
                print(f"{i:<4}  {gtype:<8}  {email:<45}  {names}")
            print()
            print(f"Total: {len(groups)} emails will be sent covering {len(eligible)} districts.")
            return

        if len(groups) > MAX_PER_DAY:
            print(f"Capping at {MAX_PER_DAY} groups/day (had {len(groups)}).")
            groups = groups[:MAX_PER_DAY]

        if args.limit:
            groups = groups[:args.limit]

    # Authenticate Gmail
    gmail_service = None
    if not args.dry_run:
        print("Authenticating with Gmail...")
        gmail_service = get_gmail_service(gmail_creds)
        print("Gmail authenticated.")

    sent_count   = 0
    failed_count = 0
    start_time   = time.time()

    for i, group in enumerate(groups, 1):
        pio_email  = group[0].get("PIO_Email",   "").strip()
        email_role = group[0].get("Email_Role",  "").strip()
        first_name = group[0].get("First_Name",  "").strip()
        last_name  = group[0].get("Last_Name",   "").strip()
        sheet_rows = [r["_row"] for r in group]

        salutation_name = resolve_salutation(first_name, last_name, email_role)

        if len(group) == 1:
            row           = group[0]
            district_name = row.get("District_Name",   "").strip()
            district_num  = row.get("District_Number", "").strip()
            subject, plain_body, html_body = build_single_email(
                district_name   = district_name,
                district_number = district_num,
                salutation_name = salutation_name,
                sender_name     = sender_name,
                sender_email    = sender_email,
            )
            label = district_name
        else:
            districts_info = [
                {"District_Name": r.get("District_Name", "").strip(),
                 "District_Number": r.get("District_Number", "").strip()}
                for r in group
            ]
            subject, plain_body, html_body = build_grouped_email(
                districts       = districts_info,
                salutation_name = salutation_name,
                sender_name     = sender_name,
                sender_email    = sender_email,
            )
            label = f"{len(group)} districts → {pio_email}"

        print(f"\n[{i}/{len(groups)}] {label}")
        print(f"  To:         {pio_email}")
        print(f"  Salutation: Dear {salutation_name},")
        print(f"  Subject:    {subject}")

        if args.dry_run:
            print("  ─── EMAIL PREVIEW ───────────────────────────────────────────")
            print(plain_body)
            print("  ─────────────────────────────────────────────────────────────")
            write_send_result(sheet, sheet_rows, "", "DRY_RUN", dry_run=True, logger=logger)
            sent_count += 1
            continue

        raw_msg  = build_mime_message(sender_email, pio_email, subject, plain_body, html_body)
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            gmail_service.users().messages().send(
                userId="me",
                body={"raw": raw_msg},
            ).execute()

            n_districts = len(group)
            status_str  = "Sent" if n_districts == 1 else f"Sent (grouped: {n_districts})"
            logger.info("Sent to %s, %d district(s), rows %s", pio_email, n_districts, sheet_rows)
            print(f"  Sent OK  ({n_districts} district(s))")
            write_send_result(sheet, sheet_rows, date_str, status_str, dry_run=False, logger=logger)
            sent_count += 1

        except Exception as exc:
            logger.error("Send failed for %s: %s", pio_email, exc)
            print(f"  FAILED: {exc}")
            write_send_result(sheet, sheet_rows, "", f"Failed: {exc}", dry_run=False, logger=logger)
            failed_count += 1

        if i < len(groups):
            time.sleep(SEND_DELAY_SECS)

    # Summary
    elapsed = int(time.time() - start_time)
    m, s    = divmod(elapsed, 60)
    print(f"\n{'='*60}")
    print(f"Run complete in {m}m {s:02d}s")
    print(f"  Groups sent: {sent_count}")
    if failed_count:
        print(f"  Failed:      {failed_count}  (check logs/ for details)")
    if not args.dry_run and sent_count > 0:
        print(f"  Date_Sent and Send_Status written to PIR_Tracking cols G and N.")


if __name__ == "__main__":
    main()
