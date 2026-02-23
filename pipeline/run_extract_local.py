import os
import json
import psycopg2
from dotenv import load_dotenv

from pipeline.chunk import chunk_text
from pipeline.validate import validate_extraction

load_dotenv()
DB = os.environ["DATABASE_URL"]

def load_one_source_text(source_id: str):
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT url, clean_text FROM sources WHERE source_id=%s::uuid", (source_id,))
            row = cur.fetchone()
            return row
    finally:
        conn.close()

def main():
    source_id = os.environ.get("SOURCE_ID", "").strip()
    if not source_id:
        print("Set SOURCE_ID env var")
        print("  set SOURCE_ID=<uuid>")
        return

    url, text = load_one_source_text(source_id)
    print(f"[SOURCE] {url} source_id={source_id} text_len={len(text)}")

    chunks = chunk_text(text, max_chars=1600, min_chars=300)
    heading, chunk = chunks[0]
    print(f"[CHUNK] heading={heading} len={len(chunk)}")

    print("\nPaste your LLM JSON output into a file 'tmp_output.json' and run again.")
    if not os.path.exists("tmp_output.json"):
        return

    payload = json.load(open("tmp_output.json", "r", encoding="utf-8"))
    accepted, rejected = validate_extraction(payload, chunk)

    print("\nACCEPTED:")
    print(json.dumps(accepted, indent=2))

    print("\nREJECTED:")
    print(json.dumps(rejected, indent=2))

if __name__ == "__main__":
    main()