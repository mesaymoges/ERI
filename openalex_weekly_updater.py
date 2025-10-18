#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weekly OpenAlex updater for Ethiopian-affiliated works (Supabase Postgres).

- Pass A: updated_date window (catch metadata changes & late indexing)
- Pass B: publication_date window (catch newly published items)
- Filter: institutions.country_code:ET
- Upserts: works, authors, institutions, authorships
- Rebuild abstract_text from abstract_inverted_index
- Maintains ingest_state keys:
    last_updated_cutoff (ISO date)
    last_pubdate_checked (ISO date)

Env (provided via GitHub Actions Secrets or local .env):
  DATABASE_URL        postgresql://...pooler.supabase.com:6543/postgres?sslmode=require
  OPENALEX_MAILTO     you@dasesa.co
  OPENALEX_MAX_DAYS   default 30 (fallback window if state is missing)
"""

import os
import sys
import time
import json
import datetime as dt

import requests
from tenacity import retry, wait_exponential, stop_after_attempt
import psycopg2
from psycopg2.extras import execute_batch

OPENALEX_BASE = "https://api.openalex.org/works"

DB_URL = os.getenv("DATABASE_URL")
MAILTO = os.getenv("OPENALEX_MAILTO", "")
MAX_DAYS = int(os.getenv("OPENALEX_MAX_DAYS", "30"))

if not DB_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "User-Agent": f"ERI weekly updater (mailto:{MAILTO})" if MAILTO else "ERI weekly updater"
}

# Flush thresholds
BATCH = 300                   # when to flush per-table buffers to DB
BATCH_COMMIT_ROWS = 50_000    # commit after this many upserts to keep tx small
API_PAGE_SLEEP = 0.35         # polite delay between OpenAlex pages (seconds)

# ----------------- helpers -----------------

def iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def today() -> dt.date:
    return dt.date.today()

def build_abstract(inv_idx):
    if not inv_idx:
        return None
    max_pos = max(p for positions in inv_idx.values() for p in positions)
    words = [None] * (max_pos + 1)
    for word, positions in inv_idx.items():
        for p in positions:
            words[p] = word
    return " ".join(w for w in words if w)

# ----------------- db helpers -----------------

def db():
    return psycopg2.connect(DB_URL)

def ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_state (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
    conn.commit()

def get_state(conn, key, default=None):
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM ingest_state WHERE key=%s", (key,))
        row = cur.fetchone()
    return row[0] if row else default

def set_state(conn, key, value):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO ingest_state(key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now();
        """, (key, value))
    conn.commit()

def get_max_pubdate(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(publication_date) FROM works;")
        row = cur.fetchone()
    return row[0]  # date or None

# ----------------- SQL -----------------

SQL_UPSERT_WORKS = """
INSERT INTO works
 (openalex_id, doi, title, abstract_text, publication_year, publication_date,
  cited_by_count, is_retracted, is_oa, host_venue, concepts, raw_json, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb, now())
ON CONFLICT (openalex_id) DO UPDATE SET
  doi = EXCLUDED.doi,
  title = EXCLUDED.title,
  abstract_text = EXCLUDED.abstract_text,
  publication_year = EXCLUDED.publication_year,
  publication_date = EXCLUDED.publication_date,
  cited_by_count = EXCLUDED.cited_by_count,
  is_retracted = EXCLUDED.is_retracted,
  is_oa = EXCLUDED.is_oa,
  host_venue = EXCLUDED.host_venue,
  concepts = EXCLUDED.concepts,
  raw_json = EXCLUDED.raw_json,
  updated_at = now();
"""

SQL_UPSERT_AUTHOR = """
INSERT INTO authors (id, openalex_id, display_name, orcid, updated_at)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  openalex_id = COALESCE(authors.openalex_id, EXCLUDED.openalex_id),
  display_name = EXCLUDED.display_name,
  orcid = EXCLUDED.orcid,
  updated_at = now();
"""

SQL_UPSERT_INST = """
INSERT INTO institutions (id, display_name, country_code, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  country_code = EXCLUDED.country_code,
  updated_at = now();
"""

SQL_UPSERT_AUTHORSHIP = """
INSERT INTO authorships (work_id, author_id, institution_id, author_position, is_corresponding)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (work_id, author_id, institution_id) DO UPDATE SET
  author_position = EXCLUDED.author_position,
  is_corresponding = EXCLUDED.is_corresponding;
"""

# ----------------- API fetch -----------------

@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def fetch_page(params):
    r = requests.get(OPENALEX_BASE, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()

def process_window(conn, base_filter: str, extra_filters: dict, label: str, seen_ids: set):
    """
    base_filter: 'institutions.country_code:ET'
    extra_filters: dict of { 'from_updated_date': 'YYYY-MM-DD', 'to_updated_date': 'YYYY-MM-DD' } or
                          { 'from_publication_date': ..., 'to_publication_date': ... }
    """
    params = {
        "filter": ",".join([base_filter] + [f"{k}:{v}" for (k, v) in extra_filters.items()]),
        "per-page": 200,
        "cursor": "*",
        "sort": "publication_date:desc"
    }

    total = 0
    work_rows, inst_rows, auth_rows, author_rows = [], [], [], []
    pages = 0
    rows_since_commit = 0

    while True:
        data = fetch_page(params)
        results = data.get("results", [])
        next_cursor = data.get("meta", {}).get("next_cursor")
        pages += 1
        print(f"[{label}] page {pages:>4}  +{len(results)} results  total~{total + len(results):,}")

        with conn.cursor() as cur:
            for w in results:
                wid = w.get("id")
                if not wid or wid in seen_ids:
                    continue
                seen_ids.add(wid)
                total += 1

                doi = w.get("doi")
                title = w.get("title") or w.get("display_name")
                pub_year = w.get("publication_year")
                pub_date = w.get("publication_date")
                cited = w.get("cited_by_count") or 0
                is_retracted = bool(w.get("is_retracted"))
                is_oa = (w.get("open_access") or {}).get("is_oa", False)
                host_venue = ((w.get("primary_location") or {}).get("source") or {}).get("display_name")
                concepts = json.dumps(w.get("concepts") or [])
                abstract_text = build_abstract(w.get("abstract_inverted_index"))

                work_rows.append((
                    wid, doi, title, abstract_text, pub_year, pub_date,
                    cited, is_retracted, is_oa, host_venue, concepts, json.dumps(w)
                ))

                # authorships
                for pos, a in enumerate(w.get("authorships") or [], start=1):
                    author = (a or {}).get("author") or {}
                    aid = author.get("id")
                    if aid:
                        author_rows.append((aid, aid, author.get("display_name"), author.get("orcid")))
                    for inst in (a.get("institutions") or []):
                        iid = inst.get("id")
                        if iid:
                            inst_rows.append((iid, inst.get("display_name"), inst.get("country_code")))
                        if aid:
                            auth_rows.append((wid, aid, iid, pos, bool(a.get("is_corresponding"))))

                # flush in chunks to keep memory stable
                if len(work_rows) >= BATCH:
                    execute_batch(cur, SQL_UPSERT_WORKS, work_rows); rows_since_commit += len(work_rows); work_rows.clear()
                if len(inst_rows) >= BATCH:
                    execute_batch(cur, SQL_UPSERT_INST, inst_rows); rows_since_commit += len(inst_rows); inst_rows.clear()
                if len(author_rows) >= BATCH:
                    execute_batch(cur, SQL_UPSERT_AUTHOR, author_rows, page_size=1000); rows_since_commit += len(author_rows); author_rows.clear()
                if len(auth_rows) >= BATCH:
                    execute_batch(cur, SQL_UPSERT_AUTHORSHIP, auth_rows); rows_since_commit += len(auth_rows); auth_rows.clear()

                if rows_since_commit >= BATCH_COMMIT_ROWS:
                    conn.commit()
                    print(f"[commit] committed_total += {rows_since_commit:,}")
                    rows_since_commit = 0

            # small flush each page
            if work_rows:
                execute_batch(cur, SQL_UPSERT_WORKS, work_rows); rows_since_commit += len(work_rows); work_rows.clear()
            if inst_rows:
                execute_batch(cur, SQL_UPSERT_INST, inst_rows); rows_since_commit += len(inst_rows); inst_rows.clear()
            if author_rows:
                execute_batch(cur, SQL_UPSERT_AUTHOR, author_rows, page_size=1000); rows_since_commit += len(author_rows); author_rows.clear()
            if auth_rows:
                execute_batch(cur, SQL_UPSERT_AUTHORSHIP, auth_rows); rows_since_commit += len(auth_rows); auth_rows.clear()

        # commit at least once per page so progress is visible
        if rows_since_commit:
            conn.commit()
            print(f"[commit] committed_total += {rows_since_commit:,}")
            rows_since_commit = 0

        if not next_cursor:
            break
        params["cursor"] = next_cursor
        time.sleep(API_PAGE_SLEEP)

    print(f"[{label}] done. Upserted ~{total:,} works in this pass.")
    return total

def main():
    base_filter = "institutions.country_code:ET"
    today_str = iso_date(today())

    with db() as conn:
        # make the session resilient to large batches
        with conn.cursor() as cur:
            cur.execute("""
                SET statement_timeout = 0;
                SET lock_timeout = 0;
                SET idle_in_transaction_session_timeout = 0;
                SET synchronous_commit = off;
            """)
        conn.commit()

        ensure_state_table(conn)

        # -------- Pass A: updated_date window --------
        last_updated_cutoff = get_state(conn, "last_updated_cutoff")
        if last_updated_cutoff:
            from_updated = last_updated_cutoff
        else:
            from_updated = iso_date(today() - dt.timedelta(days=MAX_DAYS))

        print(f"Pass A (updated_date): {from_updated} → {today_str}")
        seen_ids = set()
        try:
            process_window(
                conn,
                base_filter,
                {"from_updated_date": from_updated, "to_updated_date": today_str},
                label="updated_date",
                seen_ids=seen_ids
            )
            set_state(conn, "last_updated_cutoff", today_str)
        except Exception as e:
            print("ERROR in Pass A:", e, file=sys.stderr)

        # -------- Pass B: publication_date window --------
        max_pub = get_max_pubdate(conn)
        last_pub_state = get_state(conn, "last_pubdate_checked")
        if max_pub:
            from_pub = iso_date(max_pub)
        elif last_pub_state:
            from_pub = last_pub_state
        else:
            from_pub = iso_date(today() - dt.timedelta(days=MAX_DAYS))

        print(f"Pass B (publication_date): {from_pub} → {today_str}")
        try:
            process_window(
                conn,
                base_filter,
                {"from_publication_date": from_pub, "to_publication_date": today_str},
                label="publication_date",
                seen_ids=seen_ids
            )
            set_state(conn, "last_pubdate_checked", today_str)
        except Exception as e:
            print("ERROR in Pass B:", e, file=sys.stderr)

    print("Weekly update complete.")

if __name__ == "__main__":
    main()

