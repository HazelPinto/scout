print("RUN_TRIAGE_START")

import os
import sys
import psycopg2
from dotenv import load_dotenv

from pipeline.chunk import chunk_text
from pipeline.triage import triage_chunk

load_dotenv()
DB = os.environ["DATABASE_URL"]

def load_sources(company_id: str):
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_id::text, url, source_type::text, clean_text
                FROM sources
                WHERE company_id = %s::uuid
                  AND clean_text IS NOT NULL
                ORDER BY fetched_at DESC NULLS LAST
                """,
                (company_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()

def main():
    # Priority: CLI arg > env var
    if len(sys.argv) >= 2:
        company_id = sys.argv[1].strip()
    else:
        company_id = os.environ.get("COMPANY_ID", "").strip()

    print(f"COMPANY_ID='{company_id}'")

    if not company_id:
        print("ERROR: missing company_id. Run like:")
        print("  python -m pipeline.run_triage <COMPANY_UUID>")
        return

    rows = load_sources(company_id)
    print(f"[run_triage] sources={len(rows)} company_id={company_id}")

    for source_id, url, source_type, clean_text in rows:
        print("\n" + "=" * 90)
        print(f"[SOURCE] {url} type={source_type} source_id={source_id}")

        chunks = chunk_text(clean_text, max_chars=1600, min_chars=300)
        print(f"[CHUNKS] {len(chunks)}")

        for i, (heading, ctext) in enumerate(chunks, start=1):
            # Heading boost: include heading into triage text
            tri = triage_chunk(f"{heading}\n{ctext}")
            labels = ",".join(tri["labels"])
            conf = tri["confidence"]
            reason = tri["reason"]
            preview = ctext[:140].replace("\n", " ")
            print(f"  - chunk#{i:02d} heading='{heading}' labels={labels} conf={conf:.2f} reason={reason}")
            print(f"    preview: {preview}...")

if __name__ == "__main__":
    main()