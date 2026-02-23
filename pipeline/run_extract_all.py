import os
import sys
import psycopg2
from dotenv import load_dotenv

from pipeline.chunk import chunk_text
from pipeline.extract import extract_chunk
from pipeline.validate import validate_extraction_for_chunk
from pipeline.upsert import persist_accepted_extraction

try:
    from pipeline.triage import triage_chunk
except Exception:
    triage_chunk = None

load_dotenv()
DB = os.environ["DATABASE_URL"]


def load_company(conn, company_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM companies WHERE company_id=%s::uuid",
            (company_id,),
        )
        row = cur.fetchone()
        return row[0] if row else "UNKNOWN"


def load_sources(conn, company_id: str, include_types):
    """
    FIX: source_type is ENUM in DB (source_type_enum).
    We cast source_type to text so we can compare with a text array.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_id::text, url, source_type::text, clean_text
            FROM sources
            WHERE company_id=%s::uuid
              AND clean_text IS NOT NULL
              AND LENGTH(clean_text) >= 500
              AND source_type::text = ANY(%s)
            ORDER BY fetched_at DESC NULLS LAST
            """,
            (company_id, include_types),
        )
        return cur.fetchall()


def main(company_id: str):
    include_types = ["about", "ai_discovered", "news", "web_search", "website"]
    max_chunks_per_source = int(os.environ.get("MAX_CHUNKS_PER_SOURCE", "3"))

    conn = psycopg2.connect(DB)
    try:
        company_name = load_company(conn, company_id)
        sources = load_sources(conn, company_id, include_types)

        print(
            f"[run_extract_all] company_id={company_id} company={company_name} "
            f"sources_loaded={len(sources)} include_types={include_types} "
            f"max_chunks_per_source={max_chunks_per_source}"
        )

        total_persisted = {"people": 0, "events": 0, "funding": 0, "evidence": 0}
        total_rejected_steps = 0

        for (source_id, url, source_type, clean_text) in sources:
            print("\n" + "=" * 90)
            print(f"[SOURCE] type={source_type} url={url} source_id={source_id} chars={len(clean_text)}")

            chunks = chunk_text(clean_text, max_chars=2400, max_chunks_per_source=max_chunks_per_source)
            print(f"[CHUNKS] {len(chunks)}")

            if not chunks:
                continue

            for ch in chunks:
                heading = ch["heading"]
                text = ch["text"]

                # triage (best effort)
                if triage_chunk:
                    t = triage_chunk(text)
                    label = (t.get("labels") or "irrelevant")
                    conf = float(t.get("confidence") or 0.0)
                    reason = t.get("reason") or ""
                    print(f"  - triage heading='{heading}' label={label} conf={conf:.2f} reason={reason}")
                    if label == "irrelevant":
                        continue
                else:
                    print(f"  - triage heading='{heading}' label=SKIPPED(no_triage)")

                # extract
                try:
                    extraction = extract_chunk(company_name, url, text)
                except Exception as e:
                    print(f"  [EXTRACT_FAIL] {type(e).__name__}: {e}")
                    total_rejected_steps += 1
                    continue

                # validate
                accepted, stats = validate_extraction_for_chunk(extraction, text)
                print(
                    f"  [VALIDATE] people_ok={stats['people_ok']} events_ok={stats['events_ok']} "
                    f"funding_ok={stats['funding_ok']} rejected={stats['rejected']}"
                )

                if stats["people_ok"] == 0 and stats["events_ok"] == 0 and stats["funding_ok"] == 0:
                    continue

                # persist
                try:
                    persisted = persist_accepted_extraction(
                        conn=conn,
                        company_id=company_id,
                        source_id=source_id,
                        url=url,
                        accepted=accepted,
                    )
                    print(f"  [UPSERT] {persisted}")

                    total_persisted["people"] += int(persisted.get("people_upserted", 0))
                    total_persisted["events"] += int(persisted.get("events_upserted", 0))
                    total_persisted["funding"] += int(persisted.get("funding_upserted", 0))
                    total_persisted["evidence"] += int(persisted.get("evidence_inserted", 0))

                except Exception as e:
                    print(f"  [UPSERT_FAIL] {type(e).__name__}: {e}")
                    total_rejected_steps += 1
                    continue

        print("\n" + "=" * 90)
        print(f"[run_extract_all] DONE persisted={total_persisted} rejected_steps={total_rejected_steps}")

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.run_extract_all <company_uuid>")
        raise SystemExit(1)
    main(sys.argv[1].strip())