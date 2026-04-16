"""
check_pir_responses.py — Scan Gmail inbox for PIR replies and update PIR_Tracking.

Writes Response_Date (col O) and Response_Status (col P) for matched replies.
Optionally sends a short follow-up email asking whether the salary schedule is
publicly available on their website (col Q: Followup_Sent).

On first run (or after deleting token_reader.json), a browser window opens once
to authorize Gmail access. Credentials are saved to token_reader.json.

Usage:
    python check_pir_responses.py                    # scan + update sheet + print report
    python check_pir_responses.py --send-followups   # scan + update + send follow-ups
    python check_pir_responses.py --dry-run          # scan + print, no sheet writes, no sends
    python check_pir_responses.py --report           # deadline report only, skip Gmail scan
"""

import argparse
import base64
import io
import os
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv

# Force UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import gspread
import tldextract
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials as ServiceCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


# ── Constants ──────────────────────────────────────────────────────────────────

TRACKING_TAB  = "PIR_Tracking"
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GMAIL_SCOPES  = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
]
TOKEN_FILE = "token_reader.json"   # separate from send_pir.py's token.json

# PIR_Tracking column indices (1-based)
COL_DISTRICT_NAME    = 2   # B
COL_PIO_EMAIL        = 4   # D
COL_EMAIL_ROLE       = 5   # E
COL_DATE_SENT        = 7   # G
COL_STATUS           = 8   # H  (PIO_FOUND / PIO_NOT_FOUND)
COL_FIRST_NAME       = 12  # L
COL_LAST_NAME        = 13  # M
COL_RESPONSE_DATE      = 15  # O — written by this script
COL_RESPONSE_STATUS    = 16  # P — written by this script
COL_FOLLOWUP_SENT      = 17  # Q — written by this script
COL_FOLLOWUP_RESPONSE  = 18  # R — written by this script ("Yes" / "No")

WRITE_COLS = {COL_RESPONSE_DATE, COL_RESPONSE_STATUS, COL_FOLLOWUP_SENT, COL_FOLLOWUP_RESPONSE}

# Response classification keywords (checked against lowercased body text)
PORTAL_KEYWORDS = ["submit online", "online form", "request form", "public portal",
                   "records portal", "govqa", "nextrequest"]
DENIAL_KEYWORDS = ["deny", "denied", "denial", "exempt", "exemption",
                   "attorney general", " ag opinion", "confidential",
                   "not subject to", "not a governmental"]
DELAY_KEYWORDS  = ["additional time", "extend", "unable to produce",
                   "10 business days", "ten business days", "working days",
                   "more time", "still gathering", "processing your request",
                   "will be available"]

ATTACHMENT_EXTS = {".pdf", ".xlsx", ".xls", ".docx", ".doc", ".csv"}

ROLE_SALUTATION = {
    "Human Resources":             "Human Resources Director",
    "CFO/Business Manager":        "Chief Financial Officer",
    "Secretary to Superintendent": "Superintendent's Office",
    "Asst. Superintendent":        "Assistant Superintendent",
    "Assoc. Superintendent":       "Associate Superintendent",
    "Deputy Superintendent":       "Deputy Superintendent",
    "PEIMS Coordinator":           "PEIMS Coordinator",
    "School Email (Directory)":    "there",
    "Manual":                      "there",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def col_letter(col_1based: int) -> str:
    result = ""
    n = col_1based
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _assert_writable(col: int) -> None:
    if col not in WRITE_COLS:
        raise RuntimeError(
            f"SAFETY HALT: attempted write to column {col} ({col_letter(col)}). "
            f"Allowed write columns: {sorted(WRITE_COLS)} "
            f"({col_letter(COL_RESPONSE_DATE)}, {col_letter(COL_RESPONSE_STATUS)}, "
            f"{col_letter(COL_FOLLOWUP_SENT)})."
        )


def format_date() -> str:
    dt = datetime.now()
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}"


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def business_days_elapsed(sent_date_str: str) -> int:
    """Count Mon-Fri business days between the send date and today."""
    try:
        sent = datetime.strptime(sent_date_str.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return 0
    now     = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    current = sent.replace(hour=0, minute=0, second=0, microsecond=0)
    days    = 0
    while current < now:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def extract_domain(email_addr: str) -> str:
    try:
        host = email_addr.partition("@")[2]
        return tldextract.extract(host).registered_domain.lower()
    except Exception:
        return ""


def hi_name(first: str, last: str, role: str) -> str:
    """Return 'FirstName' for Hi greeting, falling back to role or 'there'."""
    first = (first or "").strip()
    if first:
        return first
    return ROLE_SALUTATION.get((role or "").strip(), "there")


# ── Gmail auth ─────────────────────────────────────────────────────────────────

def get_gmail_service(credentials_json_path: str):
    """
    Authenticate with Gmail (read + send scopes). Saves token to token_reader.json,
    separate from send_pir.py's token.json.
    """
    creds      = None
    token_path = Path(__file__).parent / TOKEN_FILE

    if token_path.exists():
        try:
            creds = OAuthCredentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)
        except Exception:
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
            sys.exit(1)
        flow  = InstalledAppFlow.from_client_secrets_file(credentials_json_path, GMAIL_SCOPES)
        creds = flow.run_local_server(port=0)

    token_path.write_text(creds.to_json())
    return build("gmail", "v1", credentials=creds)


# ── Sheet helpers ──────────────────────────────────────────────────────────────

def open_sheet(creds_path: str, spreadsheet_id: str):
    creds = ServiceCredentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds).open_by_key(spreadsheet_id).worksheet(TRACKING_TAB)


def load_rows(sheet) -> list[dict]:
    all_values = sheet.get_all_values()
    rows = []
    for i, row in enumerate(all_values[1:], start=2):
        while len(row) < COL_FOLLOWUP_RESPONSE:
            row.append("")
        rows.append({
            "_row":               i,
            "_district_name":     row[COL_DISTRICT_NAME - 1],
            "_pio_email":         row[COL_PIO_EMAIL - 1],
            "_email_role":        row[COL_EMAIL_ROLE - 1],
            "_date_sent":         row[COL_DATE_SENT - 1],
            "_pir_status":        row[COL_STATUS - 1],
            "_first_name":        row[COL_FIRST_NAME - 1],
            "_last_name":         row[COL_LAST_NAME - 1],
            "_response_date":     row[COL_RESPONSE_DATE - 1],
            "_response_status":   row[COL_RESPONSE_STATUS - 1],
            "_followup_sent":     row[COL_FOLLOWUP_SENT - 1],
            "_followup_response": row[COL_FOLLOWUP_RESPONSE - 1],
        })
    return rows


def ensure_response_headers(sheet) -> None:
    """Write O1/P1/Q1 headers if blank."""
    header_row = sheet.row_values(1)
    while len(header_row) < COL_FOLLOWUP_SENT:
        header_row.append("")
    needed = {
        COL_RESPONSE_DATE:     "Response_Date",
        COL_RESPONSE_STATUS:   "Response_Status",
        COL_FOLLOWUP_SENT:     "Followup_Sent",
        COL_FOLLOWUP_RESPONSE: "Followup_Response",
    }
    updates = [
        {"range": f"{col_letter(c)}1", "values": [[h]]}
        for c, h in needed.items()
        if not header_row[c - 1].strip()
    ]
    if updates:
        for u in updates:
            col = ord(u["range"][0]) - 64   # A=1, B=2, ...
            _assert_writable(col)
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        cols_written = ", ".join(u["range"] for u in updates)
        print(f"Wrote headers: {cols_written}")


def write_response(sheet, sheet_row: int, reply_date: str, status: str, dry_run: bool) -> None:
    _assert_writable(COL_RESPONSE_DATE)
    _assert_writable(COL_RESPONSE_STATUS)
    if dry_run:
        print(f"    [DRY-RUN] Row {sheet_row}: Response_Date={reply_date!r}  Response_Status={status!r}")
        return
    sheet.batch_update([
        {"range": f"{col_letter(COL_RESPONSE_DATE)}{sheet_row}",   "values": [[reply_date]]},
        {"range": f"{col_letter(COL_RESPONSE_STATUS)}{sheet_row}", "values": [[status]]},
    ], value_input_option="USER_ENTERED")


def write_followup_sent(sheet, sheet_row: int, date_str: str, dry_run: bool) -> None:
    _assert_writable(COL_FOLLOWUP_SENT)
    if dry_run:
        print(f"    [DRY-RUN] Row {sheet_row}: Followup_Sent={date_str!r}")
        return
    sheet.batch_update([
        {"range": f"{col_letter(COL_FOLLOWUP_SENT)}{sheet_row}", "values": [[date_str]]},
    ], value_input_option="USER_ENTERED")


def write_followup_response(sheet, sheet_row: int, answer: str, dry_run: bool) -> None:
    _assert_writable(COL_FOLLOWUP_RESPONSE)
    if dry_run:
        print(f"    [DRY-RUN] Row {sheet_row}: Followup_Response={answer!r}")
        return
    sheet.batch_update([
        {"range": f"{col_letter(COL_FOLLOWUP_RESPONSE)}{sheet_row}", "values": [[answer]]},
    ], value_input_option="USER_ENTERED")


# ── Gmail scanning ─────────────────────────────────────────────────────────────

def get_label_id_map(service, names: list[str]) -> dict[str, str]:
    """Return {label_name: label_id} for the given label names (single API call)."""
    result  = service.users().labels().list(userId="me").execute()
    name_map = {l["name"]: l["id"] for l in result.get("labels", [])}
    return {name: name_map.get(name, "") for name in names}


def fetch_replies(service, sender_email: str) -> list[dict]:
    """Fetch inbox messages that look like PIR replies (not from our sender address).

    Also searches all four Done-* labels so archived threads are included.
    Gmail replaces spaces with hyphens in label search queries.
    """
    query  = (
        f'((in:inbox subject:"Public Information Request") OR label:Done-Attachment'
        f' OR label:Done-URL OR label:Done-Follow-Up-Yes OR label:Done-Follow-Up-No)'
        f' -from:{sender_email}'
    )
    result = service.users().messages().list(userId="me", q=query, maxResults=200).execute()
    msg_ids = [m["id"] for m in result.get("messages", [])]
    if not msg_ids:
        return []
    return [
        service.users().messages().get(userId="me", id=mid, format="full").execute()
        for mid in msg_ids
    ]


def parse_message(msg: dict) -> dict:
    """Extract sender, date, thread info, attachment flag, and body text."""
    headers        = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    from_raw       = headers.get("from", "")
    subject        = headers.get("subject", "")
    date_raw       = headers.get("date", "")
    message_id_hdr = headers.get("message-id", "")   # RFC 2822 ID for threading
    thread_id      = msg.get("threadId", "")

    if "<" in from_raw and ">" in from_raw:
        from_email = from_raw.split("<")[1].rstrip(">").strip().lower()
    else:
        from_email = from_raw.strip().lower()

    try:
        reply_date = parsedate_to_datetime(date_raw).strftime("%Y-%m-%d")
    except Exception:
        reply_date = datetime.now().strftime("%Y-%m-%d")

    has_attachment = False
    body_text      = ""

    def walk(parts):
        nonlocal has_attachment, body_text
        for part in parts:
            fname = part.get("filename", "")
            if fname and Path(fname).suffix.lower() in ATTACHMENT_EXTS:
                has_attachment = True
            if part.get("mimeType") == "text/plain" and not body_text:
                data = part.get("body", {}).get("data", "")
                if data:
                    try:
                        body_text = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    except Exception:
                        pass
            if "parts" in part:
                walk(part["parts"])

    payload = msg.get("payload", {})
    if "parts" in payload:
        walk(payload["parts"])
    elif payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            try:
                body_text = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            except Exception:
                pass

    return {
        "from_email":       from_email,
        "from_raw":         from_raw,
        "subject":          subject,
        "reply_date":       reply_date,
        "has_attachment":   has_attachment,
        "body_lower":       body_text.lower(),
        "thread_id":        thread_id,
        "message_id_hdr":   message_id_hdr,
        "label_ids":        set(msg.get("labelIds", [])),
    }


def classify(parsed: dict, label_id_map: dict[str, str] | None = None) -> str:
    lids = parsed.get("label_ids", set())
    if label_id_map:
        if label_id_map.get("Done-Attachment") and label_id_map["Done-Attachment"] in lids:
            return "Doc Received"
        if label_id_map.get("Done-URL") and label_id_map["Done-URL"] in lids:
            return "URL Received"
    if parsed["has_attachment"]:
        return "Doc Received"
    body = parsed["body_lower"]
    for kw in PORTAL_KEYWORDS:
        if kw in body:
            return "See Portal"
    for kw in DENIAL_KEYWORDS:
        if kw in body:
            return "Denial"
    for kw in DELAY_KEYWORDS:
        if kw in body:
            return "Delay Notice"
    return "Responded"


def match_rows(parsed: dict, pir_rows: list[dict]) -> list[dict]:
    """Return PIR_Tracking rows that match this reply."""
    from_email  = parsed["from_email"]
    from_domain = extract_domain(from_email)
    subject_lc  = parsed["subject"].lower()

    matches = [r for r in pir_rows if r["_pio_email"].strip().lower() == from_email]
    if matches:
        return matches

    if from_domain:
        matches = [r for r in pir_rows if extract_domain(r["_pio_email"]) == from_domain]
        if matches:
            return matches

    for r in pir_rows:
        name = r["_district_name"].strip().lower()
        if name and name in subject_lc:
            return [r]

    return []


# ── Follow-up email ────────────────────────────────────────────────────────────

def build_followup_mime(
    sender_email:   str,
    recipient_email: str,
    subject:        str,
    body:           str,
    in_reply_to:    str,
    references:     str,
) -> tuple[str, dict]:
    """Return (base64url raw, send-body dict) for the Gmail API send call."""
    msg = MIMEMultipart("alternative")
    msg["From"]       = sender_email
    msg["To"]         = recipient_email
    msg["Subject"]    = subject
    msg["Reply-To"]   = sender_email
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"]  = references or in_reply_to
    paragraphs = body.split("\n\n")
    html_paras  = "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs)
    html_body   = (
        "<html><body>"
        '<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;max-width:600px">'
        f"{html_paras}</div></body></html>"
    )
    msg.attach(MIMEText(body,      "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html",  "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return raw


def send_followup(
    service,
    matched_rows:    list[dict],
    parsed:          dict,
    response_status: str,
    sender_email:    str,
    sender_name:     str,
    dry_run:         bool,
) -> bool:
    """Send a short follow-up into the existing email thread. Returns True on success."""
    pio_email  = matched_rows[0]["_pio_email"].strip()
    first_name = matched_rows[0]["_first_name"].strip()
    last_name  = matched_rows[0]["_last_name"].strip()
    role       = matched_rows[0]["_email_role"].strip()
    greeting   = hi_name(first_name, last_name, role)

    orig_subject  = parsed["subject"]
    reply_subject = orig_subject if orig_subject.lower().startswith("re:") else f"Re: {orig_subject}"

    if len(matched_rows) == 1:
        district_ref = f"{matched_rows[0]['_district_name']}'s salary schedule"
    else:
        district_ref = "the salary schedules"

    if response_status == "URL Received":
        body = (
            f"Hi {greeting},\n\n"
            f"Thanks for pointing me to that link. I appreciate it.\n\n"
            f"Quick follow-up: is that the standard location where you post the salary schedule each year? "
            f"I want to make note of it so I can check there directly in the future.\n\n"
            f"Thanks again,\n\n"
            f"{sender_name}\n"
            f"{sender_email}"
        )
    else:
        body = (
            f"Hi {greeting},\n\n"
            f"Thanks for sending that over. I appreciate the quick turnaround.\n\n"
            f"One quick question: is this information typically posted somewhere on your website? "
            f"I want to make sure I'm not overlooking a public resource for future reference.\n\n"
            f"Thanks again,\n\n"
            f"{sender_name}\n"
            f"{sender_email}"
        )

    if dry_run:
        print(f"    [DRY-RUN] Follow-up to {pio_email}")
        print(f"      Subject: {reply_subject}")
        print(f"      Body preview: {body[:120].replace(chr(10), ' ')}...")
        return True

    raw = build_followup_mime(
        sender_email    = sender_email,
        recipient_email = pio_email,
        subject         = reply_subject,
        body            = body,
        in_reply_to     = parsed["message_id_hdr"],
        references      = parsed["message_id_hdr"],
    )
    send_body = {"raw": raw}
    if parsed["thread_id"]:
        send_body["threadId"] = parsed["thread_id"]

    try:
        service.users().messages().send(userId="me", body=send_body).execute()
        return True
    except Exception as exc:
        print(f"    FOLLOW-UP FAILED for {pio_email}: {exc}")
        return False


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(rows: list[dict]) -> None:
    sent = [r for r in rows if r["_date_sent"].strip()]
    if not sent:
        print("No sent rows found in PIR_Tracking.")
        return

    overdue     = []
    approaching = []
    responded   = []
    pending     = []

    for r in sent:
        if r["_response_status"].strip():
            responded.append(r)
        else:
            d = business_days_elapsed(r["_date_sent"])
            if d > 10:
                overdue.append((r, d))
            elif d >= 8:
                approaching.append((r, d))
            else:
                pending.append((r, d))

    followups_sent    = sum(1 for r in responded if r["_followup_sent"].strip())
    followups_pending  = sum(1 for r in responded if not r["_followup_sent"].strip()
                            and r["_response_status"] in ("Doc Received", "URL Received"))
    fu_yes = sum(1 for r in responded if r["_followup_response"].strip() == "Yes")
    fu_no  = sum(1 for r in responded if r["_followup_response"].strip() == "No")

    print(f"\nPIR Response Report — {format_date()}")
    print("=" * 62)

    print(f"\nOVERDUE — past 10 business days, no response  ({len(overdue)})")
    if overdue:
        for r, d in sorted(overdue, key=lambda x: -x[1]):
            print(f"  ! {r['_district_name']:<52}  {d} biz days")
    else:
        print("  None")

    print(f"\nAPPROACHING — 8-10 business days, no response  ({len(approaching)})")
    if approaching:
        for r, d in sorted(approaching, key=lambda x: -x[1]):
            print(f"  ~ {r['_district_name']:<52}  {d} biz days")
    else:
        print("  None")

    print(f"\nRESPONDED  ({len(responded)})")
    if responded:
        for r in responded:
            fu = f"  [followup sent {r['_followup_sent']}]" if r["_followup_sent"].strip() else ""
            print(f"  v {r['_district_name']:<52}  {r['_response_status']}  ({r['_response_date']}){fu}")
    else:
        print("  None yet")

    if fu_yes or fu_no:
        print(f"\n  Follow-up responses: {fu_yes} said Yes (public URL exists), {fu_no} said No")
    if followups_pending:
        print(f"\n  {followups_pending} 'Doc Received' / 'URL Received' row(s) have no follow-up sent yet.")
        print(f"  Run with --send-followups to send them.")

    print(f"\nPENDING — sent, within 10 business days  ({len(pending)})")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Check Gmail for PIR replies and update PIR_Tracking.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scan and print; no sheet writes, no emails sent")
    parser.add_argument("--report", action="store_true",
                        help="Print deadline report from sheet data only — skip Gmail scan")
    parser.add_argument("--send-followups", action="store_true",
                        help="Send follow-up emails for Doc Received responses that haven't had one yet")
    return parser.parse_args()


def main():
    args = parse_args()
    load_dotenv()

    gmail_creds_raw = os.environ.get("GMAIL_CREDENTIALS_JSON", "gmail-api.json").strip()
    creds_path_raw  = os.environ.get("GOOGLE_SHEETS_CREDS_JSON", "").strip()
    spreadsheet_id  = os.environ.get("SPREADSHEET_ID", "").strip()
    sender_email    = os.environ.get("GMAIL_SENDER_EMAIL", "").strip()
    sender_name     = os.environ.get("PIR_SENDER_NAME", "").strip()

    script_dir  = Path(__file__).parent
    gmail_creds = str(script_dir / gmail_creds_raw if not Path(gmail_creds_raw).is_absolute() else gmail_creds_raw)
    creds_path  = str(script_dir / creds_path_raw  if creds_path_raw and not Path(creds_path_raw).is_absolute() else creds_path_raw)

    sheet = open_sheet(creds_path, spreadsheet_id)
    rows  = load_rows(sheet)

    if args.report:
        print_report(rows)
        return

    if not args.dry_run:
        ensure_response_headers(sheet)

    print("Authenticating with Gmail...")
    service = get_gmail_service(gmail_creds)
    print("Gmail authenticated.\n")

    messages      = fetch_replies(service, sender_email)
    label_id_map  = get_label_id_map(service, [
        "Done-Attachment", "Done-URL",
        "Done-Follow Up Yes", "Done-Follow Up No",
    ])
    print(f"Found {len(messages)} inbox message(s) matching PIR subject.\n")

    sent_rows   = [r for r in rows if r["_date_sent"].strip()]
    unresponded = [r for r in sent_rows if not r["_response_status"].strip()]

    followup_sent_emails:     set[str] = set()  # dedup follow-up sends per PIO email
    followup_response_rows:   set[int] = set()  # dedup follow-up response writes per row

    matched_count        = 0
    unmatched_pir_count  = 0
    followup_count       = 0
    fu_response_count    = 0

    for msg in messages:
        parsed = parse_message(msg)
        lids   = parsed["label_ids"]

        fu_yes_id = label_id_map.get("Done-Follow Up Yes", "")
        fu_no_id  = label_id_map.get("Done-Follow Up No",  "")
        is_fu_yes = bool(fu_yes_id) and fu_yes_id in lids
        is_fu_no  = bool(fu_no_id)  and fu_no_id  in lids

        # ── Follow-up response detection (independent of PIR response logic) ──
        if is_fu_yes or is_fu_no:
            fu_answer = "Yes" if is_fu_yes else "No"
            fu_matches = match_rows(parsed, sent_rows)
            for r in fu_matches:
                if r["_row"] not in followup_response_rows and not r["_followup_response"].strip():
                    print(f"  FOLLOW-UP RESPONSE ({fu_answer}): {r['_district_name']}")
                    write_followup_response(sheet, r["_row"], fu_answer, args.dry_run)
                    followup_response_rows.add(r["_row"])
                    fu_response_count += 1

        # ── PIR response detection ─────────────────────────────────────────────
        matches = match_rows(parsed, unresponded)
        if not matches:
            # Only report as unmatched if it's not purely a follow-up response label
            if not (is_fu_yes or is_fu_no):
                print(f"  UNMATCHED: from={parsed['from_raw']!r}  subj={parsed['subject']!r}")
                unmatched_pir_count += 1
            continue

        status = classify(parsed, label_id_map)
        names  = ", ".join(r["_district_name"] for r in matches)
        print(f"  MATCHED ({status}): {names}")
        print(f"    from={parsed['from_email']}  date={parsed['reply_date']}")

        for r in matches:
            write_response(sheet, r["_row"], parsed["reply_date"], status, args.dry_run)
            unresponded = [x for x in unresponded if x["_row"] != r["_row"]]

        matched_count += 1

        if args.send_followups and status in ("Doc Received", "URL Received"):
            pio_email = matches[0]["_pio_email"].strip().lower()
            already_sent = any(r["_followup_sent"].strip() for r in matches)
            if not already_sent and pio_email not in followup_sent_emails:
                print(f"    Sending follow-up to {pio_email}...")
                ok = send_followup(service, matches, parsed, status, sender_email, sender_name, args.dry_run)
                if ok:
                    date = today_str()
                    for r in matches:
                        write_followup_sent(sheet, r["_row"], date, args.dry_run)
                    followup_sent_emails.add(pio_email)
                    followup_count += 1

    print(f"\n{matched_count} PIR response(s) matched, {unmatched_pir_count} unmatched.")
    if unmatched_pir_count:
        print("  Review unmatched messages manually — they may be auto-replies or unrelated inbox mail.")
    if followup_count:
        print(f"  Follow-up emails sent: {followup_count}")
    if fu_response_count:
        print(f"  Follow-up responses recorded: {fu_response_count}")

    rows = load_rows(sheet)
    print_report(rows)


if __name__ == "__main__":
    main()
