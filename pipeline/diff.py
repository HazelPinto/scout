import os
from typing import Optional, Dict, Any, List, Tuple

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB = os.environ["DATABASE_URL"]


def _best_source_url_for_object(cur, object_type: str, object_id: str) -> Optional[str]:
    """
    Evidence-first: pick the most recent evidence url for that object.
    Assumes evidence has object_type/object_id + url column.
    """
    try:
        cur.execute(
            """
            SELECT url
            FROM evidence
            WHERE object_type=%s AND object_id=%s::uuid AND url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (object_type, object_id),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _insert_change(
    cur,
    company_id: str,
    change_type: str,
    object_type: str,
    object_id: str,
    source_url: Optional[str],
    details: Optional[Dict[str, Any]] = None,
) -> bool:
    cur.execute(
        """
        INSERT INTO changes(company_id, change_type, object_type, object_id, source_url, details)
        VALUES (%s::uuid, %s, %s, %s::uuid, %s, %s::jsonb)
        ON CONFLICT DO NOTHING
        """,
        (company_id, change_type, object_type, object_id, source_url, None if details is None else __import__("json").dumps(details)),
    )
    return (cur.rowcount or 0) == 1


# ---------------- People changes ----------------

def detect_people_changes(cur, company_id: str) -> int:
    """
    MVP logic:
      - new person inserted since last diff run => change
      - role changed compared to last seen => change
    We need a 'baseline'. We'll use 'changes' table as watermark:
      - last_detected_at per company
      - treat records created/updated after that as candidates
    Assumptions:
      people table has: person_id, company_id, name, role, updated_at, created_at
    """
    # watermark
    cur.execute("SELECT COALESCE(MAX(detected_at), '1970-01-01') FROM changes WHERE company_id=%s::uuid", (company_id,))
    watermark = cur.fetchone()[0]

    # 1) new people (created after watermark)
    new_count = 0
    try:
        cur.execute(
            """
            SELECT person_id::text
            FROM people
            WHERE company_id=%s::uuid AND created_at > %s
            """,
            (company_id, watermark),
        )
        for (person_id,) in cur.fetchall():
            url = _best_source_url_for_object(cur, "person", person_id)
            if _insert_change(cur, company_id, "new_person", "person", person_id, url, None):
                new_count += 1
    except Exception:
        # If created_at doesn't exist, skip "new" detection.
        pass

    # 2) role updates (updated after watermark)
    upd_count = 0
    try:
        cur.execute(
            """
            SELECT person_id::text, role
            FROM people
            WHERE company_id=%s::uuid AND updated_at > %s
            """,
            (company_id, watermark),
        )
        candidates = cur.fetchall()
    except Exception:
        candidates = []

    for person_id, role in candidates:
        # Compare with last recorded role change for this person
        cur.execute(
            """
            SELECT details
            FROM changes
            WHERE company_id=%s::uuid AND object_type='person' AND object_id=%s::uuid
              AND change_type='updated_role'
            ORDER BY detected_at DESC
            LIMIT 1
            """,
            (company_id, person_id),
        )
        row = cur.fetchone()
        last_to = None
        if row and row[0]:
            try:
                last_to = row[0].get("to")
            except Exception:
                last_to = None

        # If we never recorded, we still want to detect role change vs previous people snapshot
        # We'll pull previous role from an earlier people state by looking at evidence is too heavy.
        # MVP: if last_to exists and differs, record. If last_to absent, record update event once.
        if last_to is None:
            url = _best_source_url_for_object(cur, "person", person_id)
            if _insert_change(cur, company_id, "updated_role", "person", person_id, url, {"field": "role", "from": None, "to": role}):
                upd_count += 1
        else:
            if (role or "") != (last_to or ""):
                url = _best_source_url_for_object(cur, "person", person_id)
                if _insert_change(cur, company_id, "updated_role", "person", person_id, url, {"field": "role", "from": last_to, "to": role}):
                    upd_count += 1

    return new_count + upd_count


# ---------------- Events changes ----------------

def detect_event_changes(cur, company_id: str) -> int:
    """
    new event inserted since watermark => change.
    Assumes events table has: event_id, company_id, created_at
    """
    cur.execute("SELECT COALESCE(MAX(detected_at), '1970-01-01') FROM changes WHERE company_id=%s::uuid", (company_id,))
    watermark = cur.fetchone()[0]

    count = 0
    try:
        cur.execute(
            """
            SELECT event_id::text
            FROM events
            WHERE company_id=%s::uuid AND created_at > %s
            """,
            (company_id, watermark),
        )
        for (event_id,) in cur.fetchall():
            url = _best_source_url_for_object(cur, "event", event_id)
            if _insert_change(cur, company_id, "new_event", "event", event_id, url, None):
                count += 1
    except Exception:
        pass
    return count


# ---------------- Funding changes (optional) ----------------

def detect_funding_changes(cur, company_id: str) -> int:
    """
    If you have funding_rounds table: new rows since watermark => change.
    """
    cur.execute("SELECT COALESCE(MAX(detected_at), '1970-01-01') FROM changes WHERE company_id=%s::uuid", (company_id,))
    watermark = cur.fetchone()[0]

    count = 0
    try:
        cur.execute(
            """
            SELECT funding_round_id::text
            FROM funding_rounds
            WHERE company_id=%s::uuid AND created_at > %s
            """,
            (company_id, watermark),
        )
        for (fr_id,) in cur.fetchall():
            url = _best_source_url_for_object(cur, "funding_round", fr_id)
            if _insert_change(cur, company_id, "new_funding_round", "funding_round", fr_id, url, None):
                count += 1
    except Exception:
        # table not present is ok
        pass
    return count


def run_diff(company_id: str) -> Dict[str, int]:
    conn = psycopg2.connect(DB)
    stats = {"people": 0, "events": 0, "funding_rounds": 0, "total": 0}
    try:
        with conn:
            with conn.cursor() as cur:
                p = detect_people_changes(cur, company_id)
                e = detect_event_changes(cur, company_id)
                f = detect_funding_changes(cur, company_id)

                stats["people"] = p
                stats["events"] = e
                stats["funding_rounds"] = f
                stats["total"] = p + e + f
    finally:
        conn.close()
    return stats