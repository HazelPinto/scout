import os
import sys
import time
import hashlib
from typing import List, Dict, Tuple
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv
load_dotenv()

import httpx
import psycopg2

DB = os.environ["DATABASE_URL"]
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

if not SERPAPI_KEY:
    raise RuntimeError("Missing SERPAPI_KEY in env")

UA = "ScoutBot/0.1 (+evidence-first)"

# Very lightweight domain allowlist (extend later)
ALLOW_DOMAINS = {
    "techcrunch.com",
    "crunchbase.com",
    "prnewswire.com",
    "globenewswire.com",
    "businesswire.com",
    "reuters.com",
    "bloomberg.com",
    "forbes.com",
    "venturebeat.com",
    "ft.com",
    "wsj.com",
    "theinformation.com",
}

QUERIES = [
    '{name} funding round',
    '{name} raised seed series',
    '{name} partnership',
    '{name} launches product',
    '{name} expands to',
]


def canonicalize(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", "", ""))


def domain_ok(url: str) -> bool:
    d = urlparse(url).netloc.lower()
    for a in ALLOW_DOMAINS:
        if d.endswith(a):
            return True
    return False


def serpapi_search(query: str, count: int = 8) -> List[Dict]:
    params = {
        "engine": "google",
        "q": query,
        "num": count,
        "api_key": SERPAPI_KEY,
    }

    with httpx.Client(timeout=25, headers={"User-Agent": UA}) as client:
        r = client.get("https://serpapi.com/search", params=params)
        r.raise_for_status()
        data = r.json()

    out = []
    for x in data.get("organic_results", [])[:count]:
        if x.get("link"):
            out.append(
                {
                    "url": x["link"],
                    "title": x.get("title"),
                    "snippet": x.get("snippet"),
                }
            )
    return out


def get_company(cur, company_id: str):
    cur.execute("SELECT name, domain FROM companies WHERE company_id=%s", (company_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Company not found")
    return row[0], row[1]


def source_exists(cur, company_id: str, url: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sources WHERE company_id=%s AND url=%s",
        (company_id, url),
    )
    return cur.fetchone() is not None


def insert_source(cur, company_id: str, url: str, query: str):
    cur.execute(
        """
        INSERT INTO sources(company_id, url, source_type, fetched_at, content_hash, clean_text, discovery_query)
        VALUES (%s, %s, 'web_search', NULL, NULL, NULL, %s)
        ON CONFLICT(company_id, url) DO NOTHING
        """,
        (company_id, url, query),
    )


def web_search_company(company_id: str, per_query: int = 8):
    conn = psycopg2.connect(DB)
    total_inserted = 0

    with conn:
        with conn.cursor() as cur:
            name, domain = get_company(cur, company_id)
            print(f"[web_search] company={name} domain={domain}")

            for tmpl in QUERIES:
                q = tmpl.format(name=name)
                results = serpapi_search(q, count=per_query)

                inserted = 0
                for r in results:
                    u = canonicalize(r["url"])

                    # skip own domain
                    if domain and domain in u:
                        continue

                    # allowlist
                    if not domain_ok(u):
                        continue

                    if source_exists(cur, company_id, u):
                        continue

                    insert_source(cur, company_id, u, q)
                    inserted += 1

                total_inserted += inserted
                print(f"  - query='{q}' results={len(results)} inserted={inserted}")

    conn.close()
    print(f"[web_search] total_inserted={total_inserted}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.web_search <company_id>")
        sys.exit(1)

    web_search_company(sys.argv[1].strip())


if __name__ == "__main__":
    main()