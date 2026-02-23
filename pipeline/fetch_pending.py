import os
import sys
import psycopg2
from dotenv import load_dotenv
import httpx

load_dotenv()
DB = os.environ["DATABASE_URL"]

from pipeline.fetch import fetch_and_store  # your existing function


def mark_fetch_error(conn, source_id: str, error_code: str, error_msg: str):
    """
    Optional: if you don't have these cols, comment this out.
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sources
                    SET fetched_at = now()
                    WHERE source_id=%s::uuid
                    """,
                    (source_id,),
                )
    except Exception:
        # don't let telemetry break pipeline
        pass


def fetch_pending(company_id: str, limit: int = 15):
    conn = psycopg2.connect(DB)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_id::text, url, source_type
                    FROM sources
                    WHERE company_id=%s::uuid
                      AND (clean_text IS NULL OR content_hash IS NULL)
                    ORDER BY source_type, url
                    LIMIT %s
                    """,
                    (company_id, limit),
                )
                rows = cur.fetchall()

        print(f"[fetch_pending] company_id={company_id} pending={len(rows)} limit={limit}")

        ok = 0
        skipped = 0
        failed = 0

        for source_id, url, source_type in rows:
            print(f"[fetch_pending] fetching source_id={source_id} type={source_type} url={url}")

            try:
                fetch_and_store(company_id, url, source_type)
                ok += 1

            except httpx.HTTPStatusError as e:
                status = e.response.status_code if e.response else None

                # common anti-bot/paywall/blocked cases
                if status in (401, 402, 403, 404, 410, 429):
                    print(f"[SKIP_HTTP] status={status} url={url}")
                    skipped += 1
                    mark_fetch_error(conn, source_id, f"http_{status}", str(e))
                    continue

                print(f"[FAIL_HTTP] status={status} url={url}")
                failed += 1
                mark_fetch_error(conn, source_id, f"http_{status}", str(e))
                continue

            except Exception as e:
                print(f"[FAIL] url={url} err={type(e).__name__}: {e}")
                failed += 1
                mark_fetch_error(conn, source_id, "exception", str(e))
                continue

        print(f"[fetch_pending] done ok={ok} skipped={skipped} failed={failed}")

    finally:
        conn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.fetch_pending <company_uuid> [limit]")
        raise SystemExit(1)

    company_id = sys.argv[1].strip()
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    fetch_pending(company_id, limit)


if __name__ == "__main__":
    main()