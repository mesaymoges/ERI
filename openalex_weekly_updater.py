#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OpenAlex weekly updater for Ethiopian Research Index (ERI)

- Reads window start/end from your database/state (NOT from "today" only)
- Runs two passes so you don't miss anything:
  A) by updated_date   (catches backfills/edits)
  B) by publication_date (catches brand-new publications)
- Filters to Ethiopia via institutions.country_code:ET
- Upserts into tables: works, authors, institutions, authorships
- Reconstructs abstract_text from OpenAlex abstract_inverted_index
- Advances ingest_state keys after successful run:
    last_updated_cutoff, last_pubdate_checked

Env vars expected:
  DATABASE_URL        -> psycopg2 URI (use your Supabase *pooler* with sslmode=require)
  OPENALEX_MAILTO     -> "you@dasesa.co" (polite user-agent)
  OPENALEX_MAX_DAYS   -> fallback lookback (default 30)
  OPENALEX_OVERLAP_DAYS -> overlap buffer for windows (default 2)
"""

import os
import time
import json
import requests
from datetime import date, timedelta
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch
from tenacity import retry, wait_exponential, stop_after_attempt

# ----------------- Config -----------------
OPENALEX_BASE = "https://api.openalex.org/works"
MAILTO = os.getenv("OPENALEX_MAILTO", "")
DB_URL = os.getenv("DATABASE_URL")
MAX_DAYS = int(os.getenv("OPENALEX_MAX_DAYS", "30"))
OVERLAP_DAYS = int(os.getenv("OPENALEX_OVERLAP_DAYS", "2"))
PER_PAGE = 200
SLEEP_BETWEEN_PAGES = 0.3  # seconds
TIMEOUT = 60

if not DB_URL:
    raise SystemExit("ERROR: DATABASE_URL is not set.")

# ----------------- Helpers -----------------

def log(*a):
    print(*a, flush=True)

def build_abstract(inv_idx: Optional[dict]) -> Optional[str]:
    if not inv_idx:
        return None
    max_pos = max(p for positions in inv_idx.values() for p in positions)
    words = [None] * (max_pos + 1)
    for w, pos_list in inv_idx.items():
        for p in pos_list:
            words[p] = w
    return " ".join(x for x in words if x)

def today_utc() -> date:
    # good enough for daily windows
    return date.today()

def get_db_dates(conn) -> Tuple[Optional[date], Optional[date]]:
    """Return (max_pubdate_in_db, last_updated_cutoff_state)."""
    with conn.cursor() as cur:
        cur.execute("SELECT max(publication_date) FROM works;")
        max_pub = cur.fetchone()[0]  # date or None
        cur.execute("SELECT value FROM ingest_state WHERE key='last_updated_cutoff';")
        row = cur.fetchone()
        last_upd = None
        if row and row[0]:
            try:
                last_upd = date.fromisoformat(row[0])
            except Exception:
                last_upd = None
    return max_pub, last_upd

def compute_windows(conn) -> dict:
    """Compute Pass A/B windows from DB + state with overlap and safety caps."""
    t = today_utc()

    max_pub, last_upd = get_db_dates(conn)

    # Pass B (publication_date)
    if max_pub:
        pub_start = max_pub + timedelta(days=1) - timedelta(days=OVERLAP_DAYS)
    else:
        pub_start = t - timedelta(days=MAX_DAYS)
    pub_end = t
    if pub_start > pub_end:
        pub_start = pub_end - timedelta(days=MAX_DAYS)

    # Pass A (updated_date)
    if last_upd:
        upd_start = last_upd - timedelta(days=OVERLAP_DAYS)
    else:
        upd_start = t - timedelta(days=MAX_DAYS)
    upd_end = t
    if upd_start > upd_end:
        upd_start = upd_end - timedelta(days=MAX_DAYS)

    return {
        "pub_start": pub_start.isoformat(),
        "pub_end": pub_end.isoformat(),
        "upd_start": upd_start.isoformat(),
        "upd_end": upd_end.isoformat(),
    }

def save_state(conn, pub_end_iso: str, upd_end_iso: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_state(key, value, updated_at)
            VALUES ('last_pubdate_checked', %s, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
        """, (pub_end_iso,))
        cur.execute("""
            INSERT INTO ingest_state(key, value, updated_at)
            VALUES ('last_updated_cutoff', %s, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
        """, (upd_end_iso,))
    conn.commit()

# ----------------- DB upserts -----------------

SQL_UPSERT_WORK = """
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

SQL_UPSERT_AUTHOR_MIN = """
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
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (work_id, author_id, institution_id) DO UPDATE SET
  author_position = EXCLUDED.author_position,
  is_corresponding = EXCLUDED.is_corresponding;
"""

# ----------------- OpenAlex fetch -----------------

def headers():
    ua = "ERI weekly updater"
    if MAILTO:
        ua += f" (mailto:{MAILTO})"
    return {"User-Agent": ua}

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_page(params: dict) -> dict:
    r = requests.get(OPENALEX_BASE, params=params, headers=headers(), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def run_pass(conn, pass_name: str, filter_str: str, sort: str):
    """Generic pass loop with cursor pagination and upserts."""
    cursor = "*"
    total_seen = 0
    page = 0

    while cursor:
        page += 1
        params = {
            "filter": filter_str,
            "per-page": PER_PAGE,
            "cursor": cursor,
            "sort": sort,
        }
        data = fetch_page(params)

        results = data.get("results", [])
        cursor = data.get("meta", {}).get("next_cursor")

        if not results:
            if page == 1:
                log(f"[{pass_name}] page {page:4d}  +0 results  total~{total_seen}")
            break

        # --- build batches ---
        work_rows = []
        inst_rows = []
        auth_rows = []

        for w in results:
            openalex_id = w.get("id")
            doi = w.get("doi")
            title = w.get("title") or w.get("display_name")
            pub_year = w.get("publication_year")
            pub_date = w.get("publication_date")
            cited = w.get("cited_by_count") or 0
            is_retracted = bool(w.get("is_retracted"))
            is_oa = (w.get("open_access") or {}).get("is_oa")
            host_source = (w.get("primary_location") or {}).get("source") or {}
            host_name = host_source.get("display_name")
            concepts = w.get("concepts") or []
            abstract_text = build_abstract(w.get("abstract_inverted_index"))

            work_rows.append((
                openalex_id, doi, title, abstract_text, pub_year, pub_date,
                cited, is_retracted, is_oa, host_name,
                json.dumps(concepts), json.dumps(w)
            ))

        # First insert/update works
        with conn.cursor() as cur:
            if work_rows:
                execute_batch(cur, SQL_UPSERT_WORK, work_rows, page_size=200)

        # Now authors, institutions, authorships
        with conn.cursor() as cur:
            for w in results:
                w_id = w.get("id")
                for pos, a in enumerate(w.get("authorships") or [], start=1):
                    author = a.get("author") or {}
                    author_id = author.get("id")
                    is_corr = bool(a.get("is_corresponding"))

                    # ensure author exists (fill both id and openalex_id with same URL)
                    if author_id:
                        execute_batch(cur, SQL_UPSERT_AUTHOR_MIN,
                                      [(author_id, author_id, author.get("display_name"), author.get("orcid"))],
                                      page_size=1)

                    # institutions + authorships
                    for inst in (a.get("institutions") or []):
                        inst_id = inst.get("id")
                        if inst_id:
                            inst_rows.append((inst_id, inst.get("display_name"), inst.get("country_code")))
                        if author_id:
                            auth_rows.append((w_id, author_id, inst_id, pos, is_corr))

            if inst_rows:
                execute_batch(cur, SQL_UPSERT_INST, inst_rows, page_size=200)
            if auth_rows:
                execute_batch(cur, SQL_UPSERT_AUTHORSHIP, auth_rows, page_size=500)

        conn.commit()
        total_seen += len(results)
        log(f"[{pass_name}] page {page:4d}  +{len(results)} results  total~{total_seen}")
        time.sleep(SLEEP_BETWEEN_PAGES)

    log(f"[{pass_name}] done. Upserted ~{total_seen} works in this pass.")

# ----------------- main -----------------

def main():
    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = False

        windows = compute_windows(conn)
        pub_start = windows["pub_start"]; pub_end = windows["pub_end"]
        upd_start = windows["upd_start"]; upd_end = windows["upd_end"]

        # PASS A: updated_date (backfills and metadata edits)
        filter_a = f"institutions.country_code:ET,from_updated_date:{upd_start},to_updated_date:{upd_end}"
        log(f"Pass A (updated_date): {upd_start} → {upd_end}")
        try:
            run_pass(conn, "updated_date", filter_a, "updated_date:asc")
        except Exception as e:
            log(f"ERROR in Pass A: {e}")

        # PASS B: publication_date (brand-new publications)
        filter_b = f"institutions.country_code:ET,from_publication_date:{pub_start},to_publication_date:{pub_end}"
        log(f"Pass B (publication_date): {pub_start} → {pub_end}")
        run_pass(conn, "publication_date", filter_b, "publication_date:asc")

        # advance state to 'today'
        save_state(conn, pub_end, upd_end)
        log("Weekly update complete.")

if __name__ == "__main__":
    main()
