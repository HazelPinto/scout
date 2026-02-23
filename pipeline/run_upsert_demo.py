import os
import psycopg2
from dotenv import load_dotenv

from pipeline.upsert import persist_accepted_extraction

load_dotenv()
DB = os.environ["DATABASE_URL"]

def main():
    company_id = os.environ.get("COMPANY_ID", "").strip()
    source_id = os.environ.get("SOURCE_ID", "").strip()

    if not company_id or not source_id:
        print("Set COMPANY_ID and SOURCE_ID")
        print("  set COMPANY_ID=<uuid>")
        print("  set SOURCE_ID=<uuid>")
        return

    # Fetch URL for source_id
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM sources WHERE source_id=%s::uuid", (source_id,))
            row = cur.fetchone()
            if not row:
                print("source_id not found")
                return
            url = row[0]

        accepted = {
            "extractor_version": "v0.1.0",
            "people": [
                {
                    "name": "Example Person",
                    "role": "Founder & CEO",
                    "linkedin_url": None,
                    "confidence": 0.90,
                    "evidence_quote": "Founder & CEO",  # must exist in chunk in real pipeline
                }
            ],
            "events": [
                {
                    "type": "product_launch",
                    "date": None,
                    "title": "Product mentioned",
                    "summary": "Product/value proposition detected.",
                    "confidence": 0.80,
                    "evidence_quote": "AI-native platform",  # must exist in chunk in real pipeline
                }
            ],
            "funding_rounds": [],
        }

        # NOTE: In real run, evidence_quote must be substring of the *chunk* validated.
        # This demo will work only if those phrases exist in your stored clean_text chunks.
        stats = persist_accepted_extraction(conn, company_id, source_id, url, accepted)
        print("STATS:", stats)

    finally:
        conn.close()

if __name__ == "__main__":
    main()