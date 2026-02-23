import os
import hashlib
import time
from typing import Optional, Tuple

import httpx
import psycopg2
import trafilatura
from dotenv import load_dotenv

load_dotenv()
DB = os.environ["DATABASE_URL"]

UA = "ScoutBot/0.1 (+evidence-first; contact: internal)"
DEFAULT_TIMEOUT = 25.0


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_text(text: str) -> str:
    # normalize whitespace + drop empty lines
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def extract_main_text(html: str) -> Optional[str]:
    """
    Prefer trafilatura extraction. Fallback to crude text if it fails.
    """
    text = None
    try:
        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
        )
    except Exception:
        text = None

    if not text:
        # crude fallback: return None so we don't store junk
        return None

    text = normalize_text(text)
    return text if text else None


def get_existing_hash(cur, company_id: str, url: str) -> Optional[str]:
    cur.execute(
        "SELECT content_hash FROM sources WHERE company_id=%s::uuid AND url=%s",
        (company_id, url),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_source(
    cur,
    company_id: str,
    url: str,
    source_type: str,
    content_hash: str,
    clean_text: str,
) -> None:
    """
    Uses ON CONFLICT(company_id, url). Ensure UNIQUE(company_id, url) exists.
    """
    cur.execute(
        """
        INSERT INTO sources(company_id, url, source_type, fetched_at, content_hash, clean_text)
        VALUES (%s::uuid, %s, %s, now(), %s, %s)
        ON CONFLICT(company_id, url)
        DO UPDATE SET
          source_type = EXCLUDED.source_type,
          fetched_at = now(),
          content_hash = EXCLUDED.content_hash,
          clean_text = EXCLUDED.clean_text
        """,
        (company_id, url, source_type, content_hash, clean_text),
    )


def fetch_url(client: httpx.Client, url: str, retries: int = 2) -> Tuple[Optional[str], Optional[int]]:
    """
    Returns (html, status_code). html is None on failure or blocked.
    """
    last_status = None
    for attempt in range(retries + 1):
        try:
            r = client.get(url)
            last_status = r.status_code

            # common blocked/paywalled/anti-bot statuses:
            if r.status_code in (401, 402, 403):
                return None, r.status_code

            # not found / gone
            if r.status_code in (404, 410):
                return None, r.status_code

            # rate limited
            if r.status_code == 429:
                # backoff and retry a bit
                if attempt < retries:
                    time.sleep(1.0 + attempt * 1.5)
                    continue
                return None, r.status_code

            r.raise_for_status()
            return r.text, r.status_code

        except httpx.TimeoutException:
            if attempt < retries:
                time.sleep(0.8 + attempt * 1.2)
                continue
            return None, last_status

        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response else last_status
            # treat as non-fatal (caller decides)
            return None, status

        except Exception:
            if attempt < retries:
                time.sleep(0.5 + attempt)
                continue
            return None, last_status

    return None, last_status


def fetch_and_store(company_id: str, url: str, source_type: str = "website") -> None:
    """
    Fetch -> clean -> hash -> store (idempotent)
    Non-fatal on blocked URLs; prints SKIP and returns.
    """
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True, headers=headers) as client:
        html, status = fetch_url(client, url, retries=2)

    # blocked/paywalled/etc
    if html is None:
        if status in (401, 402, 403):
            print(f"[SKIP] blocked/paywalled status={status}: {url}")
            return
        if status in (404, 410):
            print(f"[SKIP] not_found status={status}: {url}")
            return
        if status == 429:
            print(f"[SKIP] rate_limited status=429: {url}")
            return

        print(f"[SKIP] fetch_failed status={status}: {url}")
        return

    clean_text = extract_main_text(html)
    if not clean_text or len(clean_text) < 500:
        print(f"[SKIP] too short/junk: {url}")
        return

    content_hash = sha256_hex(clean_text)

    conn = psycopg2.connect(DB)
    try:
        with conn:
            with conn.cursor() as cur:
                old_hash = get_existing_hash(cur, company_id, url)
                if old_hash == content_hash:
                    print(f"[NOCHANGE] {url}")
                    return

                upsert_source(cur, company_id, url, source_type, content_hash, clean_text)
                print(f"[STORED] {url} hash={content_hash[:10]}...")
    finally:
        conn.close()


if __name__ == "__main__":
    # quick local test (optional)
    COMPANY_ID = os.environ.get("TEST_COMPANY_ID", "").strip()

    if not COMPANY_ID:
        print("Set TEST_COMPANY_ID env var to run fetch.py directly.")
        raise SystemExit(0)

    urls = [
        "https://9fin.com/",
        "https://www.prnewswire.com/news-releases/9fin-expands-debt-market-intelligence-platform-to-latin-america-302549581.html",
        "https://www.bloomberg.com/",  # will likely SKIP blocked
    ]

    for u in urls:
        fetch_and_store(COMPANY_ID, u, "website")