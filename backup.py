#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, logging, re, sys
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, Union
import requests
import psycopg2
from dotenv import load_dotenv

# ----------------------------
# Load .env (robust order)
# ----------------------------
ROOT = Path(__file__).resolve().parent
candidates = [
    ROOT / ".env.local",   # highest priority (developer local)
    ROOT / ".env",         # project .env (same folder as backup.py)
    ROOT.parent / ".env",  # parent .env (fallback)
]
loaded_any = False
for p in candidates:
    if p.exists():
        load_dotenv(dotenv_path=p, override=True)
        loaded_any = True

if not loaded_any:
    print("WARNING: No .env found. Looked at:\n - " + "\n - ".join(map(str, candidates)))

# ----------------------------
# Config (from .env)
# ----------------------------
Z_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
Z_EMAIL     = os.getenv("ZENDESK_EMAIL")
Z_TOKEN     = os.getenv("ZENDESK_API_TOKEN") or os.getenv("ZENDESK_TOKEN")  # support either

if not (Z_SUBDOMAIN and Z_EMAIL and Z_TOKEN):
    print("DEBUG .env:", {
        "cwd": os.getcwd(),
        "script_dir": str(ROOT),
        "ZENDESK_SUBDOMAIN": Z_SUBDOMAIN,
        "ZENDESK_EMAIL": Z_EMAIL,
        "ZENDESK_API_TOKEN?": bool(os.getenv("ZENDESK_API_TOKEN")),
        "ZENDESK_TOKEN?": bool(os.getenv("ZENDESK_TOKEN")),
    })
    raise AssertionError("Zendesk credentials missing in .env (need ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN)")

# Postgres / Supabase
PG_CFG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "sslmode": os.getenv("PGSSLMODE", "require"),  # Supabase requires SSL
}
DATABASE_URL = os.getenv("DATABASE_URL")  # if you prefer a single connection string

# Fail early if no DB creds at all
assert (PG_CFG["user"] and PG_CFG["password"]) or DATABASE_URL, \
    "Postgres creds missing in .env (set DATABASE_URL OR PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD)"

Z_BASE = f"https://{Z_SUBDOMAIN}.zendesk.com"
PER_PAGE = int(os.getenv("ZENDESK_PER_PAGE", "500"))
INCLUDE = os.getenv("ZENDESK_INCLUDE", "")
EXCLUDE_DELETED = os.getenv("ZENDESK_EXCLUDE_DELETED", "false").lower() == "true"
BOOTSTRAP_HOURS = int(os.getenv("ZENDESK_BOOTSTRAP_START_HOURS", "24"))

# Tickets/Comments toggles
CLOSED_TICKETS_ONLY = os.getenv("CLOSED_TICKETS_ONLY", "false").lower() == "true"
USE_TICKET_EVENTS_FOR_COMMENTS = os.getenv("USE_TICKET_EVENTS_FOR_COMMENTS", "false").lower() == "true"
PRUNE_REOPENED_FROM_DB = os.getenv("PRUNE_REOPENED_FROM_DB", "false").lower() == "true"

# Attachments
DOWNLOAD_ATTACHMENTS = os.getenv("DOWNLOAD_ATTACHMENTS", "true").lower() == "true"
ATTACHMENTS_DIR = os.getenv("ATTACHMENTS_DIR", "./attachments")

# Orgs throttling
ORG_PER_PAGE = int(os.getenv("ORG_PER_PAGE", "100"))                 # 25..100
ORG_PAGE_DELAY_SECS = float(os.getenv("ORG_PAGE_DELAY_SECS", "0.2")) # small jitter between pages
ORG_RETRY_CAP_SECS = float(os.getenv("ORG_RETRY_CAP_SECS", "4"))     # cap Retry-After sleeps

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

SESSION = requests.Session()
SESSION.auth = (f"{Z_EMAIL}/token", Z_TOKEN)
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "zendesk-backup/1.0 (+python-requests)"
})
RETRY_STATUS = {429, 500, 502, 503, 504}

# ----------------------------
# DB helpers
# ----------------------------
def get_db():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    return psycopg2.connect(
        host=PG_CFG["host"],
        port=PG_CFG["port"],
        dbname=PG_CFG["dbname"],
        user=PG_CFG["user"],
        password=PG_CFG["password"],
        sslmode=PG_CFG["sslmode"],
    )

SCHEMA_SQL = """
-- State for incremental cursors
CREATE TABLE IF NOT EXISTS sync_state (
  resource TEXT PRIMARY KEY,
  cursor_token TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw payloads for recovery/replay
CREATE TABLE IF NOT EXISTS raw_snapshots (
  resource TEXT NOT NULL,
  entity_id BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NULL,
  payload_json JSONB NOT NULL,
  PRIMARY KEY (resource, entity_id)
);

-- Users
CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY,
  name TEXT,
  email TEXT,
  role TEXT,
  role_type INT NULL,
  active BOOLEAN,
  suspended BOOLEAN,
  organization_id BIGINT NULL,
  phone TEXT NULL,
  locale TEXT NULL,
  time_zone TEXT NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL,
  last_login_at TIMESTAMPTZ NULL,
  tags_json JSONB NULL,
  user_fields_json JSONB NULL,
  photo_json JSONB NULL
);

-- Organizations
CREATE TABLE IF NOT EXISTS organizations (
  id BIGINT PRIMARY KEY,
  name TEXT,
  external_id TEXT NULL,
  group_id BIGINT NULL,
  details TEXT NULL,
  notes TEXT NULL,
  shared_tickets BOOLEAN NULL,
  shared_comments BOOLEAN NULL,
  domain_names_json JSONB NULL,
  tags_json JSONB NULL,
  organization_fields_json JSONB NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL
);

-- Tickets
CREATE TABLE IF NOT EXISTS tickets (
  id BIGINT PRIMARY KEY,
  subject TEXT,
  description TEXT,
  status TEXT,
  priority TEXT,
  type TEXT,
  requester_id BIGINT,
  assignee_id BIGINT,
  organization_id BIGINT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  due_at TIMESTAMPTZ NULL
);
CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
CREATE INDEX IF NOT EXISTS idx_tickets_updated_at ON tickets(updated_at);

-- Ticket comments
CREATE TABLE IF NOT EXISTS ticket_comments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT NOT NULL,
  author_id BIGINT NULL,
  public BOOLEAN NOT NULL,
  body TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_comments_ticket ON ticket_comments(ticket_id);

-- Attachments
CREATE TABLE IF NOT EXISTS attachments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT,
  comment_id BIGINT,
  file_name TEXT,
  content_url TEXT,
  local_path TEXT NULL,
  content_type TEXT,
  size BIGINT,
  thumbnails_json JSONB NULL,
  created_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_attachments_ticket ON attachments(ticket_id);
CREATE INDEX IF NOT EXISTS idx_attachments_comment ON attachments(comment_id);

-- Views
CREATE TABLE IF NOT EXISTS views (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT NULL,
  active BOOLEAN,
  position INT NULL,
  default_view BOOLEAN NULL,
  restriction_json JSONB NULL,
  execution_json JSONB NULL,
  conditions_json JSONB NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL
);

-- Triggers
CREATE TABLE IF NOT EXISTS triggers (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT NULL,
  active BOOLEAN,
  position INT NULL,
  category_id TEXT NULL,
  raw_title TEXT NULL,
  default_trigger BOOLEAN NULL,
  conditions_json JSONB NOT NULL,
  actions_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL
);

-- Trigger categories (Zendesk returns string IDs, but typically numeric)
CREATE TABLE IF NOT EXISTS trigger_categories (
  id TEXT PRIMARY KEY,
  name TEXT,
  position INT NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL
);

-- Macros
CREATE TABLE IF NOT EXISTS macros (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT NULL,
  active BOOLEAN,
  position INT NULL,
  default_macro BOOLEAN NULL,
  restriction_json JSONB NULL,
  actions_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NULL
);
"""

def init_schema():
    conn = get_db()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.close()

# ----------------------------
# HTTP helpers
# ----------------------------
def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name or "")
    return name[:180] if len(name) > 180 else name

def initial_start_time_epoch() -> int:
    return int(time.time()) - max(BOOTSTRAP_HOURS * 3600, 120)

def parse_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s: return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def get_with_retry(url: str, params: Dict[str, Any] = None, stream: bool = False) -> Union[requests.Response, Dict[str, Any]]:
    backoff = 1.0
    for _ in range(8):
        resp = SESSION.get(url, params=params, timeout=120, stream=stream)
        if resp.status_code == 200:
            return resp if stream else resp.json()
        if resp.status_code in RETRY_STATUS:
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else backoff
            logging.warning("Retryable %s for %s. Sleeping %.1fs", resp.status_code, url, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 30.0)
            continue
        try:
            detail = resp.json()
        except Exception:
            detail = {"text": resp.text[:300]}
        raise RuntimeError(f"GET {url} failed [{resp.status_code}]: {detail}")
    raise RuntimeError(f"GET {url} exhausted retries")

def iter_list_pages(url: str, params: Dict[str, Any] = None) -> Iterable[Dict[str, Any]]:
    page = get_with_retry(url, params=params or {})
    while True:
        yield page
        next_url = page.get("next_page") or page.get("links", {}).get("next")
        if not next_url: break
        page = get_with_retry(next_url)

def iter_cursor_page(first_page_json: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    page = first_page_json
    while True:
        yield page
        after_url = page.get("after_url") or page.get("links", {}).get("next")
        if not after_url: break
        page = get_with_retry(after_url)

# ----------------------------
# Cursor state
# ----------------------------
def get_cursor_val(conn, resource: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT cursor_token FROM sync_state WHERE resource=%s", (resource,))
        row = cur.fetchone()
        return row[0] if row else None

def set_cursor_val(conn, resource: str, token: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sync_state (resource, cursor_token, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (resource) DO UPDATE
              SET cursor_token=EXCLUDED.cursor_token,
                  updated_at=NOW()
        """, (resource, token))

# ----------------------------
# Raw payload upsert
# ----------------------------
def upsert_raw(conn, resource: str, entity_id: int, updated_at: Optional[str], payload: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_snapshots (resource, entity_id, updated_at, payload_json)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (resource, entity_id) DO UPDATE
              SET updated_at=EXCLUDED.updated_at,
                  payload_json=EXCLUDED.payload_json
        """, (resource, entity_id, parse_dt(updated_at), json.dumps(payload, ensure_ascii=False)))

# ----------------------------
# UPSERT helpers (structured tables)
# ----------------------------
def upsert_user(conn, u: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (
              id, name, email, role, role_type, active, suspended, organization_id, phone, locale, time_zone,
              created_at, updated_at, last_login_at, tags_json, user_fields_json, photo_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name, email=EXCLUDED.email, role=EXCLUDED.role, role_type=EXCLUDED.role_type,
              active=EXCLUDED.active, suspended=EXCLUDED.suspended, organization_id=EXCLUDED.organization_id,
              phone=EXCLUDED.phone, locale=EXCLUDED.locale, time_zone=EXCLUDED.time_zone,
              created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at, last_login_at=EXCLUDED.last_login_at,
              tags_json=EXCLUDED.tags_json, user_fields_json=EXCLUDED.user_fields_json, photo_json=EXCLUDED.photo_json
        """, (
            u.get("id"), u.get("name"), u.get("email"), u.get("role"), u.get("role_type"),
            bool(u.get("active")), bool(u.get("suspended")), u.get("organization_id"),
            u.get("phone"), u.get("locale"), u.get("time_zone"),
            parse_dt(u.get("created_at")), parse_dt(u.get("updated_at")), parse_dt(u.get("last_login_at")),
            json.dumps(u.get("tags") or []), json.dumps(u.get("user_fields") or {}), json.dumps(u.get("photo") or {})
        ))
    upsert_raw(conn, "users", int(u["id"]), u.get("updated_at"), u)

def upsert_org(conn, o: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO organizations (
              id, name, external_id, group_id, details, notes, shared_tickets, shared_comments,
              domain_names_json, tags_json, organization_fields_json, created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name, external_id=EXCLUDED.external_id, group_id=EXCLUDED.group_id, details=EXCLUDED.details,
              notes=EXCLUDED.notes, shared_tickets=EXCLUDED.shared_tickets, shared_comments=EXCLUDED.shared_comments,
              domain_names_json=EXCLUDED.domain_names_json, tags_json=EXCLUDED.tags_json,
              organization_fields_json=EXCLUDED.organization_fields_json, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            o.get("id"), o.get("name"), o.get("external_id"), o.get("group_id"), o.get("details"), o.get("notes"),
            bool(o.get("shared_tickets")), bool(o.get("shared_comments")),
            json.dumps(o.get("domain_names") or []), json.dumps(o.get("tags") or []),
            json.dumps(o.get("organization_fields") or {}),
            parse_dt(o.get("created_at")), parse_dt(o.get("updated_at"))
        ))
    upsert_raw(conn, "organizations", int(o["id"]), o.get("updated_at"), o)

def upsert_ticket(conn, t: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tickets (
              id, subject, description, status, priority, type, requester_id, assignee_id, organization_id,
              created_at, updated_at, due_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              subject=EXCLUDED.subject, description=EXCLUDED.description, status=EXCLUDED.status, priority=EXCLUDED.priority,
              type=EXCLUDED.type, requester_id=EXCLUDED.requester_id, assignee_id=EXCLUDED.assignee_id,
              organization_id=EXCLUDED.organization_id, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at,
              due_at=EXCLUDED.due_at
        """, (
            t.get("id"), t.get("subject"), t.get("description"), t.get("status"), t.get("priority"), t.get("type"),
            t.get("requester_id"), t.get("assignee_id"), t.get("organization_id"),
            parse_dt(t.get("created_at")), parse_dt(t.get("updated_at")), parse_dt(t.get("due_at"))
        ))
    upsert_raw(conn, "tickets", int(t["id"]), t.get("updated_at"), t)

def delete_ticket(conn, ticket_id: int):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM attachments WHERE ticket_id=%s", (ticket_id,))
        cur.execute("DELETE FROM ticket_comments WHERE ticket_id=%s", (ticket_id,))
        cur.execute("DELETE FROM tickets WHERE id=%s", (ticket_id,))

def upsert_comment(conn, ticket_id: int, c: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ticket_comments (id, ticket_id, author_id, public, body, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              ticket_id=EXCLUDED.ticket_id, author_id=EXCLUDED.author_id, public=EXCLUDED.public, body=EXCLUDED.body,
              created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            c.get("id"), ticket_id, c.get("author_id"), bool(c.get("public")),
            c.get("body"), parse_dt(c.get("created_at")), parse_dt(c.get("updated_at") or c.get("created_at"))
        ))
    upsert_raw(conn, "comments", int(c["id"]), c.get("updated_at") or c.get("created_at"), c)

def upsert_attachment(conn, ticket_id: int, comment_id: Optional[int], a: Dict[str, Any], local_path: Optional[str] = None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO attachments (id, ticket_id, comment_id, file_name, content_url, local_path, content_type, size, thumbnails_json, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
            ON CONFLICT (id) DO UPDATE SET
              ticket_id=EXCLUDED.ticket_id, comment_id=EXCLUDED.comment_id, file_name=EXCLUDED.file_name,
              content_url=EXCLUDED.content_url, local_path=EXCLUDED.local_path, content_type=EXCLUDED.content_type,
              size=EXCLUDED.size, thumbnails_json=EXCLUDED.thumbnails_json, created_at=EXCLUDED.created_at
        """, (
            a.get("id"), ticket_id, comment_id, a.get("file_name"), a.get("content_url"),
            local_path, a.get("content_type"), a.get("size"),
            json.dumps(a.get("thumbnails") or []), parse_dt(a.get("created_at"))
        ))
    upsert_raw(conn, "attachments", int(a["id"]), a.get("created_at"), a)

def upsert_view(conn, v: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO views (id, title, description, active, position, default_view, restriction_json, execution_json, conditions_json,
                               created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               default_view=EXCLUDED.default_view, restriction_json=EXCLUDED.restriction_json,
               execution_json=EXCLUDED.execution_json, conditions_json=EXCLUDED.conditions_json,
               created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            v.get("id"), v.get("title"), v.get("description"), bool(v.get("active")), v.get("position"),
            bool(v.get("default")), json.dumps(v.get("restriction")),
            json.dumps(v.get("execution")), json.dumps(v.get("conditions")),
            parse_dt(v.get("created_at")), parse_dt(v.get("updated_at"))
        ))
    upsert_raw(conn, "views", int(v["id"]), v.get("updated_at"), v)

def upsert_trigger(conn, t: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO triggers (id, title, description, active, position, category_id, raw_title, default_trigger,
                                  conditions_json, actions_json, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               category_id=EXCLUDED.category_id, raw_title=EXCLUDED.raw_title, default_trigger=EXCLUDED.default_trigger,
               conditions_json=EXCLUDED.conditions_json, actions_json=EXCLUDED.actions_json,
               created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            t.get("id"), t.get("title"), t.get("description"), bool(t.get("active")), t.get("position"),
            t.get("category_id"), t.get("raw_title"), bool(t.get("default")),
            json.dumps(t.get("conditions") or {}), json.dumps(t.get("actions") or []),
            parse_dt(t.get("created_at")), parse_dt(t.get("updated_at"))
        ))
    upsert_raw(conn, "triggers", int(t["id"]), t.get("updated_at"), t)

def upsert_trigger_category(conn, c: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trigger_categories (id, name, position, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               name=EXCLUDED.name, position=EXCLUDED.position,
               created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (str(c.get("id")), c.get("name"), c.get("position"),
              parse_dt(c.get("created_at")), parse_dt(c.get("updated_at"))))
    try:
        entity_id = int(c["id"])
    except Exception:
        entity_id = abs(hash(str(c["id"]))) % (10**15)
    upsert_raw(conn, "trigger_categories", entity_id, c.get("updated_at"), c)

def upsert_macro(conn, m: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO macros (id, title, description, active, position, default_macro, restriction_json, actions_json, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               default_macro=EXCLUDED.default_macro, restriction_json=EXCLUDED.restriction_json,
               actions_json=EXCLUDED.actions_json, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            m.get("id"), m.get("title"), m.get("description"), bool(m.get("active")), m.get("position"),
            bool(m.get("default")), json.dumps(m.get("restriction")), json.dumps(m.get("actions") or []),
            parse_dt(m.get("created_at")), parse_dt(m.get("updated_at"))
        ))
    upsert_raw(conn, "macros", int(m["id"]), m.get("updated_at"), m)

# ----------------------------
# Downloads (NEW)
# ----------------------------
def download_attachment(ticket_id: int, comment_id: Optional[int], att: Dict[str, Any]) -> Optional[str]:
    """
    Download the binary to ATTACHMENTS_DIR/<ticket_id>/<attachment_id>__<safe_filename>
    Uses the same authenticated SESSION (works for private attachments).
    Returns absolute local path or None on skip/failure.
    """
    if not DOWNLOAD_ATTACHMENTS:
        return None

    url = att.get("content_url")
    if not url:
        return None

    rid = str(att.get("id") or "")
    fname = safe_filename(att.get("file_name") or f"attachment_{rid}")
    base_dir = Path(ATTACHMENTS_DIR) / str(ticket_id)
    ensure_dir(str(base_dir))
    target = base_dir / f"{rid}__{fname}"

    # If already downloaded, reuse
    try:
        if target.exists() and target.stat().st_size > 0:
            return str(target.resolve())
    except Exception:
        pass

    try:
        # Use the same SESSION (Basic auth via email/token if the file is private)
        resp = SESSION.get(url, timeout=180, stream=True)
        if resp.status_code != 200:
            # Some Zendesk accounts serve attachments via S3 pre-signed URL (no auth needed),
            # but if the link is expired, log it and skip gracefully.
            logging.warning("Attachment GET failed [%s] for ticket %s att %s (%s)",
                            resp.status_code, ticket_id, rid, url)
            return None

        with open(target, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return str(target.resolve())

    except Exception as e:
        logging.warning("Attachment download failed (ticket %s, comment %s, att %s): %s",
                        ticket_id, comment_id, rid, e)
        return None

# ----------------------------
# Sync functions
# ----------------------------
def sync_users(conn):
    url = f"{Z_BASE}/api/v2/incremental/users/cursor.json"
    last = get_cursor_val(conn, "users")
    params = {"per_page": PER_PAGE}
    if last: params["cursor"] = last
    else:    params["start_time"] = initial_start_time_epoch()

    first = get_with_retry(url, params=params)
    for page in iter_cursor_page(first):
        for u in page.get("users", []):
            upsert_user(conn, u)
        ac = page.get("after_cursor")
        if ac: set_cursor_val(conn, "users", ac)
        if page.get("end_of_stream"): break
    logging.info("Users: sync complete.")

def sync_organizations(conn):
    import random
    force_snapshot = os.getenv("FORCE_ORG_SNAPSHOT", "false").lower() == "true"
    per_page = min(max(ORG_PER_PAGE, 25), 100)
    page_delay = max(ORG_PAGE_DELAY_SECS, 0.0)
    retry_cap  = max(ORG_RETRY_CAP_SECS, 0.5)

    def set_now_cursor():
        set_cursor_val(conn, "organizations", str(int(time.time()) - 60))

    def snapshot_once():
        url = f"{Z_BASE}/api/v2/organizations.json"
        params = {"per_page": per_page}
        seen = 0
        logging.info("Organizations: snapshot mode (per_page=%s)", per_page)
        page = get_with_retry(url, params=params)
        while True:
            for o in page.get("organizations", []):
                upsert_org(conn, o); seen += 1
            nxt = page.get("next_page") or page.get("links", {}).get("next")
            if not nxt: break
            page = get_with_retry(nxt)
        set_now_cursor()
        logging.info("Organizations: snapshot complete (rows=%d).", seen)

    last_epoch = get_cursor_val(conn, "organizations")
    if force_snapshot or not last_epoch:
        snapshot_once(); return

    url = f"{Z_BASE}/api/v2/incremental/organizations.json"
    params = {"per_page": per_page, "start_time": int(last_epoch)}
    processed = 0; pages_seen = 0; seen_ids = set(); dup_only_streak = 0
    logging.info("Organizations: incremental start (start_time=%s)", params["start_time"])

    while True:
        try:
            resp = SESSION.get(url, params=params, headers={"Accept": "application/json"}, timeout=45)
        except requests.RequestException as e:
            logging.warning("Organizations: network error %s — retrying", e)
            time.sleep(min(1.0, retry_cap)); continue

        if resp.status_code == 200:
            page = resp.json(); orgs = page.get("organizations", [])
            before = len(seen_ids)
            for o in orgs:
                sid = o.get("id")
                if sid is not None:
                    try: seen_ids.add(int(sid))
                    except Exception: pass
                upsert_org(conn, o)
            added_unique = len(seen_ids) - before
            processed += len(orgs); pages_seen += 1
            end_time = page.get("end_time")
            if end_time: set_cursor_val(conn, "organizations", str(end_time))
            logging.info("Organizations: page %d (+%d, +%d unique, total %d)", pages_seen, len(orgs), added_unique, processed)

            if len(orgs) > 0 and added_unique == 0: dup_only_streak += 1
            else: dup_only_streak = 0
            if dup_only_streak >= 3:
                logging.info("Organizations: duplicate-only pages → switch to snapshot.")
                snapshot_once(); return

            next_page = page.get("next_page")
            if not next_page: break
            if page_delay > 0:
                time.sleep(page_delay + random.uniform(0, min(0.25, page_delay)))
            url = next_page; params = {}; continue

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            suggested = float(ra) if ra else page_delay * 2 or 1.0
            sleep_for = min(max(0.5, suggested), retry_cap)
            logging.warning("Organizations: 429, sleeping %.2fs", sleep_for)
            time.sleep(sleep_for); continue

        if resp.status_code in RETRY_STATUS:
            time.sleep(min(retry_cap, 1.0)); continue

        try: detail = resp.json()
        except Exception: detail = {"text": resp.text[:300]}
        raise RuntimeError(f"Organizations GET failed [{resp.status_code}]: {detail}")

    logging.info("Organizations: incremental complete (rows=%d, pages=%d).", processed, pages_seen)

def ticket_initial_params(last_cursor: Optional[str]) -> Dict[str, Any]:
    p = {"per_page": PER_PAGE}
    if INCLUDE: p["include"] = INCLUDE
    if EXCLUDE_DELETED: p["exclude_deleted"] = "true"
    if last_cursor: p["cursor"] = last_cursor
    else: p["start_time"] = initial_start_time_epoch()
    return p

def sync_tickets_comments_attachments(conn):
    import random
    url = f"{Z_BASE}/api/v2/incremental/tickets/cursor.json"
    last = get_cursor_val(conn, "tickets")
    per_page = int(os.getenv("ZENDESK_PER_PAGE", "100"))
    page_delay = float(os.getenv("TICKETS_PAGE_DELAY_SECS", "0.2"))
    retry_cap  = float(os.getenv("TICKETS_RETRY_CAP_SECS", "6"))

    params = ticket_initial_params(last); params["per_page"] = per_page
    processed = 0; pages = 0
    logging.info("Tickets: start (per_page=%s)", per_page)

    while True:
        try:
            resp = SESSION.get(url, params=params, timeout=90)
        except requests.RequestException as e:
            logging.warning("Tickets: network error %s — wait", e)
            time.sleep(min(1.0, retry_cap)); continue

        if resp.status_code == 200:
            page = resp.json(); tickets = page.get("tickets", []); pages += 1
            for t in tickets:
                status = (t.get("status") or "").lower()
                is_closed = (status == "closed")
                if CLOSED_TICKETS_ONLY and not is_closed:
                    if PRUNE_REOPENED_FROM_DB:
                        delete_ticket(conn, int(t["id"]))
                    continue
                upsert_ticket(conn, t)

                if not USE_TICKET_EVENTS_FOR_COMMENTS:
                    comments_url = f"{Z_BASE}/api/v2/tickets/{t['id']}/comments.json"
                    for cpage in iter_list_pages(comments_url, params={"per_page": 100}):
                        for c in cpage.get("comments", []):
                            upsert_comment(conn, int(t["id"]), c)
                            for a in (c.get("attachments") or []):
                                local_path = download_attachment(int(t["id"]), int(c.get("id") or 0), a)
                                upsert_attachment(conn, int(t["id"]), int(c.get("id") or 0), a, local_path)

            processed += len(tickets)
            ac = page.get("after_cursor")
            if ac: set_cursor_val(conn, "tickets", ac)
            logging.info("Tickets: page %d (+%d, total %d)", pages, len(tickets), processed)

            if page.get("end_of_stream"): break
            if page_delay > 0:
                time.sleep(page_delay + random.uniform(0, min(0.25, page_delay)))
            url = page.get("after_url") or page.get("links", {}).get("next") or url
            params = {}; continue

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            suggested = float(ra) if ra else (page_delay * 2 or 1.0)
            sleep_for = min(max(0.5, suggested), retry_cap)
            logging.warning("Tickets: 429, sleeping %.1fs", sleep_for)
            time.sleep(sleep_for); continue

        if resp.status_code in RETRY_STATUS:
            time.sleep(min(1.0, retry_cap)); continue

        try: detail = resp.json()
        except Exception: detail = {"text": resp.text[:300]}
        raise RuntimeError(f"Tickets GET failed [{resp.status_code}]: {detail}")

    logging.info("Tickets (+comments:%s, attachments:%s): sync complete.",
                 "events" if USE_TICKET_EVENTS_FOR_COMMENTS else "per-ticket",
                 DOWNLOAD_ATTACHMENTS)

def sync_ticket_events_for_comments(conn):
    url = f"{Z_BASE}/api/v2/incremental/ticket_events.json"
    last_epoch = get_cursor_val(conn, "ticket_events")
    params = {"start_time": int(last_epoch) if last_epoch else initial_start_time_epoch(),
              "per_page": PER_PAGE, "include": "comment_events"}

    while True:
        page = get_with_retry(url, params=params)
        for ev in page.get("ticket_events", []):
            tid = int(ev.get("ticket_id"))
            if CLOSED_TICKETS_ONLY:
                t = get_with_retry(f"{Z_BASE}/api/v2/tickets/{tid}.json").get("ticket", {})
                if (t.get("status") or "").lower() != "closed":
                    continue
            for ce in (ev.get("child_events") or []):
                if (ce.get("event_type") or ce.get("type")) == "Comment":
                    c = {
                        "id": ce["id"],
                        "author_id": ce.get("author_id"),
                        "public": ce.get("public", False),
                        "body": ce.get("body"),
                        "created_at": ce.get("created_at"),
                        "updated_at": ce.get("created_at"),
                        "attachments": ce.get("attachments") or []
                    }
                    upsert_comment(conn, tid, c)
                    for a in (ce.get("attachments") or []):
                        local_path = download_attachment(tid, int(ce["id"]), a)
                        upsert_attachment(conn, tid, int(ce["id"]), a, local_path)
        next_page = page.get("next_page")
        if next_page:
            url = next_page; params = {}
        else:
            end_time = page.get("end_time")
            if end_time: set_cursor_val(conn, "ticket_events", str(end_time))
            break
    logging.info("Ticket events (comments): sync complete.")

def sync_views(conn):
    url = f"{Z_BASE}/api/v2/views.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE: params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for v in page.get("views", []):
            upsert_view(conn, v)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Views: sync complete.")

def sync_triggers(conn):
    url = f"{Z_BASE}/api/v2/triggers.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for t in page.get("triggers", []):
            upsert_trigger(conn, t)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Triggers: sync complete.")

def sync_trigger_categories(conn):
    url = f"{Z_BASE}/api/v2/trigger_categories.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for c in page.get("trigger_categories", []):
            upsert_trigger_category(conn, c)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Trigger categories: sync complete.")

def sync_macros(conn):
    url = f"{Z_BASE}/api/v2/macros.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE: params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for m in page.get("macros", []):
            upsert_macro(conn, m)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Macros: sync complete.")

# ----------------------------
# Strong smoke test (agent/admin required)
# ----------------------------
def smoke_assert_agent():
    r = SESSION.get(f"{Z_BASE}/api/v2/users/me.json", timeout=30)
    try:
        data = r.json()
    except Exception:
        raise SystemExit(f"Auth smoke failed [{r.status_code}]: {r.text[:300]}")
    user = (data or {}).get("user") or {}
    print("SMOKE STATUS:", r.status_code, "| email:", user.get("email"), "| role:", user.get("role"))
    if r.status_code != 200 or not user.get("id"):
        raise SystemExit("Zendesk auth failed: check ZENDESK_EMAIL/token/subdomain.")
    if user.get("role") not in ("admin", "agent"):
        raise SystemExit("Authenticated but not an agent/admin. Use an AGENT/ADMIN email + API token.")

# ----------------------------
# Main
# ----------------------------
def main():
    # 1) verify Zendesk auth & role
    smoke_assert_agent()

    # 2) attachments folder
    if DOWNLOAD_ATTACHMENTS:
        ensure_dir(ATTACHMENTS_DIR)
        logging.info("Attachment download enabled → %s", ATTACHMENTS_DIR)

    # 3) schema
    init_schema()
    conn = get_db(); conn.autocommit = True

    # 4) syncs
    logging.info("Starting USERS…"); sync_users(conn)
    logging.info("Starting ORGANIZATIONS…"); sync_organizations(conn)
    logging.info("Starting TICKETS (+comments, attachments)…"); sync_tickets_comments_attachments(conn)
    if USE_TICKET_EVENTS_FOR_COMMENTS:
        logging.info("Starting TICKET EVENTS for comments…"); sync_ticket_events_for_comments(conn)
    logging.info("Starting VIEWS…"); sync_views(conn)
    logging.info("Starting TRIGGERS…"); sync_triggers(conn)
    logging.info("Starting TRIGGER CATEGORIES…"); sync_trigger_categories(conn)
    logging.info("Starting MACROS…"); sync_macros(conn)

    conn.close()
    logging.info("✅ Zendesk incremental backup complete.")

if __name__ == "__main__":
    main()
