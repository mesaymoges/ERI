#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weekly OpenAlex updater for Ethiopian-affiliated works (Supabase Postgres).

- Two passes (deduped):
  A) by updated_date    → picks up metadata changes (citations, OA flags, etc.)
  B) by publication_date→ picks up late-indexed new works

- Ethiopian filter: institutions.country_code:ET

- Robust state handling with CLAMP so bad/future windows don't return 0 rows.
- Idempotent upserts into: works, authors, institutions, authorships
- Reconstruct abstract_text from abstract_inverted_index when present.

ENV
----
DATABASE_URL        : Postgres URI (e.g., Supabase pooler)
OPENALEX_MAILTO     : you@dasesa.co  (polite User-Agent)
OPENALEX_MAX_DAYS   : max lookback days for window guards (default 30)
"""

import os, time, json, math, sys
from datetime import date, timedelta
from typing import Optional, Dict, Any, Iterable

import requests
import psycopg2
from psycopg2.extras import execute_batch
from tenacity import retry, wait_exponential, stop_after_attempt

OPENALEX_BASE = "https://api.openalex.org/works"
DB_URL = os.getenv("DATABASE_URL")
MAILTO = os.getenv("OPENALEX_MAILTO", "")
MAX_DAYS = int(os.getenv("OPENALEX_MAX_DAYS", "30"))

if not DB_URL:
    print("ERROR: DATABASE_URL env is required", file=sys.stderr)
    sys.exit(1)

# ---------------------------
# Helpers
# ---------------------------
def today_str() -> str:
    return date.today().isoformat()

def dstr(d: date) -> str:
    return d.isoformat()

def parse_date(s: Optional[str]) -> Optional[date]:
    if not s: return None
    return date.fromisoformat(s)

def clamp_window(from_d: date, to_d: date, max_days: int) -> tuple[date, date]:
    """Guard against future/invalid windows."""
    today = date.today()
    to_d = min(to_d, today)
    if from_d > to_d:
        from_d = to_d - timedelta(days=max_days)
    # also cap very large gaps
    if (to_d - from_d).days > max_days:
        from_d = to_d - timedelta(days=max_days)
    return from_d, to_d

def build_abstract(inv_idx: Optional[Dict[str, Any]]) -> Optional[str]:
    if not inv_idx:
        return None
    max_pos = max(p for positions in inv_idx.values() for p in positions)
    words = [None] * (max_pos + 1)
    for word, positions in inv_idx.items():
        for p in positions:
            words[p] = word
    return " ".join(w for w in words if w)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_page(params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"User-Agent": f"ERI updater (mailto:{MAILTO})"} if MAILTO else {}
    r = requests.get(OPENALEX_BASE, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

# ---------------------------
# SQL (schema matches your Supabase)
# ---------------------------
UPSERT_WORK = """
INSERT INTO works (openalex_id, doi, title, abstract_text, publication_year, publication_date,
                   cited_by_count, is_retracted, is_oa, host_venue, concepts, raw_json, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s::jsonb, %s::jsonb, now())
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

UPSERT_AUTHOR_MIN = """
INSERT INTO authors (id, openalex_id, display_name, orcid, updated_at)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  openalex_id = COALESCE(authors.openalex_id, EXCLUDED.openalex_id),
  display_name = EXCLUDED.display_name,
  orcid = EXCLUDED.orcid,
  updated_at = now();
"""

UPSERT_INST = """
INSERT INTO institutions (id, display_name, country_code, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  country_code = EXCLUDED.country_code,
  updated_at = now();
"""

UPSERT_AUTHORSHIP = """
INSERT INTO authorships (work_id, author_id, institution_id, author_position, is_corresponding)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (work_id, author_id, institution_id) DO UPDATE SET
  author_position = EXCLUDED.author_position,
  is_corresponding = EXCLUDED.is_corresponding;
"""

GET_STATE = "SELECT value FROM ingest_state WHERE key = %s"
SET_STATE = """
INSERT INTO ingest_state (key, value, updated_at)
VALUES (%s, %s, now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
"""

GET_MAX_PUBDATE = "SELECT COALESCE(MAX(publication_date), current_date - INTERVAL '30 days')::date FROM works"

# ---------------------------
# Core
# ---------------------------
def get_state(cur, key: str) -> Optional[str]:
    cur.execute(GET_STATE, (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_state(cur, key: str, value: str) -> None:
    cur.execute(SET_STATE, (key, value))

def process_results(cur, results: Iterable[Dict[str, Any]], seen: set[str]) -> int:
    """Upsert works + linked entities; returns number of new works touched this call (deduped by openalex_id)."""
    BATCH = 500
    works_buf, inst_buf, auth_buf, count = [], [], [], 0

    for w in results:
        wid = w.get("id")
        if not wid or wid in seen:
            continue
        seen.add(wid)
        title = w.get("title") or w.get("display_name")
        doi = w.get("doi")
        pub_year = w.get("publication_year")
        pub_date = w.get("publication_date")
        cited = w.get("cited_by_count") or 0
        is_retracted = bool(w.get("is_retracted"))
        is_oa = (w.get("open_access") or {}).get("is_oa")
        host_name = ((w.get("primary_location") or {}).get("source") or {}).get("display_name")
        concepts = w.get("concepts") or []
        abstract_text = build_abstract(w.get("abstract_inverted_index"))

        works_buf.append((
            wid, doi, title, abstract_text, pub_year, pub_date,
            cited, is_retracted, is_oa, host_name, json.dumps(concepts), json.dumps(w)
        ))

        # authorships, authors, institutions
        for pos, a in enumerate(w.get("authorships") or [], start=1):
            author = a.get("author") or {}
            author_id = author.get("id")
            if author_id:
                execute_batch(cur, UPSERT_AUTHOR_MIN, [
                    (author_id, author_id, author.get("display_name"), author.get("orcid"))
                ], page_size=1)
            is_corr = bool(a.get("is_corresponding"))
            for inst in (a.get("institutions") or []):
                inst_id = inst.get("id")
                if inst_id:
                    inst_buf.append((inst_id, inst.get("display_name"), inst.get("country_code")))
                if author_id:
                    auth_buf.append((wid, author_id, inst_id, pos, is_corr))

        if len(works_buf) >= BATCH:
            execute_batch(cur, UPSERT_WORK, works_buf); works_buf.clear()
            if inst_buf: execute_batch(cur, UPSERT_INST, inst_buf); inst_buf.clear()
            if auth_buf: execute_batch(cur, UPSERT_AUTHORSHIP, auth_buf); auth_buf.clear()
            count += BATCH

    # flush
    if works_buf: execute_batch(cur, UPSERT_WORK, works_buf); count += len(works_buf)
    if inst_buf:  execute_batch(cur, UPSERT_INST, inst_buf)
    if auth_buf:  execute_batch(cur, UPSERT_AUTHORSHIP, auth_buf)

    return count

def run_pass(cur, pass_kind: str, start_d: date, end_d: date, seen: set[str]) -> int:
    """pass_kind in {'updated_date','publication_date'}"""
    params = {
        "filter": f"institutions.country_code:ET,from_{pass_kind}:{dstr(start_d)},to_{pass_kind}:{dstr(end_d)}",
        "per-page": 200,
        "cursor": "*",
        "sort": ( "publication_date:asc" if pass_kind == "publication_date" else "updated_date:asc" ),
    }
    total_new = 0
    page = 0
    while True:
        page += 1
        data = fetch_page(params)
        results = data.get("results", [])
        if not results:
            print(f"[{pass_kind}] page {page:4d}  +0 results  total~{total_new}")
            break
        total_new += process_results(cur, results, seen)
        print(f"[{pass_kind}] page {page:4d}  +{len(results)} results  total~{total_new}")
        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
        time.sleep(0.3)  # be polite
    return total_new

def main():
    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # ---- derive windows (with clamps) ----
            # Pass A: updated_date
            last_updated_cutoff = parse_date(get_state(cur, "last_updated_cutoff"))
            if not last_updated_cutoff:
                last_updated_cutoff = date.today() - timedelta(days=MAX_DAYS)
            a_from, a_to = clamp_window(last_updated_cutoff, date.today(), MAX_DAYS)
            print(f"Pass A (updated_date): {dstr(a_from)} → {dstr(a_to)}")

            # Pass B: publication_date
            last_pub_checked = parse_date(get_state(cur, "last_pubdate_checked"))
            if not last_pub_checked:
                cur.execute(GET_MAX_PUBDATE)
                base = cur.fetchone()[0]  # date
                last_pub_checked = base if isinstance(base, date) else date.today() - timedelta(days=MAX_DAYS)
            b_from, b_to = clamp_window(last_pub_checked, date.today(), MAX_DAYS)
            print(f"Pass B (publication_date): {dstr(b_from)} → {dstr(b_to)}")

            # ---- run passes ----
            seen: set[str] = set()
            # A: updated_date (tolerate API hiccups, we won't abort B)
            try:
                _ = run_pass(cur, "updated_date", a_from, a_to, seen)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"ERROR in Pass A: {e}", file=sys.stderr)

            # B: publication_date
            new_b = run_pass(cur, "publication_date", b_from, b_to, seen)
            conn.commit()

            # ---- advance state conservatively ----
            set_state(cur, "last_updated_cutoff", dstr(a_to))
            set_state(cur, "last_pubdate_checked", dstr(b_to))
            conn.commit()

            print("Weekly update complete.")

if __name__ == "__main__":
    main()
