import hashlib
from typing import Dict, Any, Optional

def _norm_name(name: str) -> str:
    return " ".join((name or "").strip().lower().split())

def _title_hash(title: str) -> str:
    return hashlib.sha256((title or "").strip().lower().encode("utf-8")).hexdigest()[:16]


def persist_accepted_extraction(conn, company_id: str, source_id: str, url: str, accepted: Dict[str, Any]) -> Dict[str, int]:
    """
    Persists accepted extraction deterministically.
    Returns stats dict for logging.
    """
    people_upserted = 0
    events_upserted = 0
    funding_upserted = 0
    evidence_inserted = 0

    extractor_version = accepted.get("extractor_version", "unknown")

    with conn:
        with conn.cursor() as cur:

            # -------- PEOPLE --------
            for p in accepted.get("people", []) or []:
                name = (p.get("name") or "").strip()
                role = (p.get("role") or "").strip()
                linkedin_url = (p.get("linkedin_url") or None)
                conf = float(p.get("confidence") or 0.0)
                quote = (p.get("evidence_quote") or "").strip()

                if not name or not quote:
                    continue

                normalized_name = _norm_name(name)

                # upsert person
                cur.execute(
                    """
                    INSERT INTO people(company_id, name, normalized_name, role, linkedin_url, needs_review)
                    VALUES (%s::uuid, %s, %s, %s, %s, FALSE)
                    ON CONFLICT(company_id, normalized_name)
                    DO UPDATE SET
                        role = EXCLUDED.role,
                        linkedin_url = COALESCE(EXCLUDED.linkedin_url, people.linkedin_url)
                    RETURNING person_id::text
                    """,
                    (company_id, name, normalized_name, role, linkedin_url),
                )
                person_id = cur.fetchone()[0]
                people_upserted += 1

                # insert evidence (append-only)
                cur.execute(
                    """
                    INSERT INTO evidence(object_type, object_id, field, value, source_id, url, quote, confidence, extractor_version)
                    VALUES ('person', %s::uuid, 'person.role', %s, %s::uuid, %s, %s, %s, %s)
                    """,
                    (person_id, role, source_id, url, quote, conf, extractor_version),
                )
                evidence_inserted += 1

            # -------- EVENTS --------
            for e in accepted.get("events", []) or []:
                etype = (e.get("type") or "").strip()
                date = e.get("date")  # may be None
                title = (e.get("title") or "").strip()
                summary = (e.get("summary") or "").strip()
                conf = float(e.get("confidence") or 0.0)
                quote = (e.get("evidence_quote") or "").strip()

                if not etype or not title or not quote:
                    continue

                title_h = _title_hash(title)

                cur.execute(
                    """
                    INSERT INTO events(company_id, type, date, title, title_hash, summary, needs_review)
                    VALUES (%s::uuid, %s::event_type_enum, %s, %s, %s, %s, FALSE)
                    ON CONFLICT(company_id, type, date, title_hash)
                    DO UPDATE SET
                        summary = EXCLUDED.summary
                    RETURNING event_id::text
                    """,
                    (company_id, etype, date, title, title_h, summary),
                )
                event_id = cur.fetchone()[0]
                events_upserted += 1

                cur.execute(
                    """
                    INSERT INTO evidence(object_type, object_id, field, value, source_id, url, quote, confidence, extractor_version)
                    VALUES ('event', %s::uuid, 'event.summary', %s, %s::uuid, %s, %s, %s, %s)
                    """,
                    (event_id, summary, source_id, url, quote, conf, extractor_version),
                )
                evidence_inserted += 1

            # -------- FUNDING ROUNDS --------
            for fr in accepted.get("funding_rounds", []) or []:
                round_type = (fr.get("round_type") or "").strip()
                amount = fr.get("amount")
                currency = fr.get("currency")
                date = fr.get("date")
                investors = fr.get("investors") or []
                conf = float(fr.get("confidence") or 0.0)
                quote = (fr.get("evidence_quote") or "").strip()

                if not round_type or not quote:
                    continue

                # you may have a funding_rounds table, but if not, store as event
                # Minimal approach: store funding as event(type='funding')
                title = f"{round_type.upper()} round"
                title_h = _title_hash(title)

                summary = f"Round={round_type}; amount={amount} {currency}; investors={', '.join(investors)}"

                cur.execute(
                    """
                    INSERT INTO events(company_id, type, date, title, title_hash, summary, needs_review)
                    VALUES (%s::uuid, 'funding'::event_type_enum, %s, %s, %s, %s, FALSE)
                    ON CONFLICT(company_id, type, date, title_hash)
                    DO UPDATE SET summary = EXCLUDED.summary
                    RETURNING event_id::text
                    """,
                    (company_id, date, title, title_h, summary),
                )
                event_id = cur.fetchone()[0]
                funding_upserted += 1

                cur.execute(
                    """
                    INSERT INTO evidence(object_type, object_id, field, value, source_id, url, quote, confidence, extractor_version)
                    VALUES ('event', %s::uuid, 'funding.round', %s, %s::uuid, %s, %s, %s, %s)
                    """,
                    (event_id, round_type, source_id, url, quote, conf, extractor_version),
                )
                evidence_inserted += 1

    return {
        "people_upserted": people_upserted,
        "events_upserted": events_upserted,
        "funding_upserted": funding_upserted,
        "evidence_inserted": evidence_inserted,
    }