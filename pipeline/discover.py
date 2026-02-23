import os
import re
import sys
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, urljoin, urlunparse

import httpx
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB = os.environ["DATABASE_URL"]

UA = "ScoutBot/0.1 (+evidence-first; contact: internal)"
TIMEOUT = 20.0

# If you have OpenAI wired:
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

# Optional: use your existing llm helper if present
try:
    from pipeline.llm import call_llm_json  # expects dict output
except Exception:
    call_llm_json = None


# ----------------------------
# URL utilities
# ----------------------------
def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/")[0]
    return d


def canonicalize_url(url: str) -> str:
    """
    Basic canonicalization:
      - force https if scheme missing
      - drop fragments
      - strip trailing slash (except root)
    """
    u = (url or "").strip()
    if not u:
        return ""

    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u

    p = urlparse(u)
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    netloc = re.sub(r"^www\.", "", netloc)

    path = p.path or "/"
    # remove fragments
    fragment = ""
    query = p.query or ""

    # normalize trailing slash
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", query, fragment))


def is_same_domain(url: str, domain: str) -> bool:
    d = normalize_domain(domain)
    try:
        p = urlparse(url)
        host = re.sub(r"^www\.", "", (p.netloc or "").lower())
        return host == d or host.endswith("." + d)
    except Exception:
        return False


# ----------------------------
# DB helpers
# ----------------------------
def q_one(conn, query: str, params: tuple) -> Optional[tuple]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def q_all(conn, query: str, params: tuple) -> List[tuple]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def insert_sources(conn, company_id: str, rows: List[Tuple[str, str]]) -> int:
    """
    rows: list of (url, source_type)
    Inserts with ON CONFLICT(company_id,url) DO NOTHING
    """
    if not rows:
        return 0

    inserted = 0
    with conn:
        with conn.cursor() as cur:
            for url, source_type in rows:
                if not url:
                    continue

                cur.execute(
                    """
                    INSERT INTO sources(company_id, url, source_type, fetched_at, content_hash, clean_text)
                    VALUES (%s::uuid, %s, %s, NULL, NULL, NULL)
                    ON CONFLICT(company_id, url) DO NOTHING
                    """,
                    # If enum: use %s::source_type_enum
                    (company_id, url, source_type),
                )
                # rowcount is 1 if inserted
                if cur.rowcount == 1:
                    inserted += 1

    return inserted


# ----------------------------
# Fetch homepage + crawl
# ----------------------------
def fetch_homepage_html(base_url: str) -> str:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    try:
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers=headers) as client:
            r = client.get(base_url)
            r.raise_for_status()
            return r.text
    except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as e:
        print(f"[discover] SKIP_HOME_CONNECT base={base_url} err={type(e).__name__}: {e}")
        return ""
    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response else None
        print(f"[discover] SKIP_HOME_HTTP base={base_url} status={status}")
        return ""
    except Exception as e:
        print(f"[discover] SKIP_HOME_OTHER base={base_url} err={type(e).__name__}: {e}")
        return ""


ANCHOR_RE = re.compile(r'href=[\'"]([^\'"]+)[\'"]', re.I)

KEYWORDS = re.compile(r"(about|team|company|press|news|blog|careers|investor|funding)", re.I)


def crawl_homepage_links(homepage_html: str, base_url: str, domain: str, max_links: int = 30) -> List[str]:
    """
    Very lightweight crawl: parse hrefs from HTML and pick same-domain links containing keywords.
    """
    if not homepage_html:
        return []

    found = []
    for m in ANCHOR_RE.finditer(homepage_html):
        href = m.group(1).strip()
        if not href:
            continue
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        abs_url = urljoin(base_url, href)
        abs_url = canonicalize_url(abs_url)
        if not abs_url:
            continue
        if not is_same_domain(abs_url, domain):
            continue

        # must contain keywords in path
        try:
            p = urlparse(abs_url)
            if not KEYWORDS.search(p.path):
                continue
        except Exception:
            continue

        found.append(abs_url)
        if len(found) >= max_links:
            break

    # dedupe preserve order
    dedup = []
    seen = set()
    for u in found:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


# ----------------------------
# Deterministic URL generation
# ----------------------------
def deterministic_paths(base: str) -> List[Tuple[str, str]]:
    """
    Returns list of (url, source_type)
    """
    paths_about = ["/about", "/team", "/company", "/careers"]
    paths_news = ["/press", "/news", "/blog"]

    out = []
    for p in paths_about:
        out.append((canonicalize_url(urljoin(base, p)), "about"))
    for p in paths_news:
        out.append((canonicalize_url(urljoin(base, p)), "news"))
    return out


# ----------------------------
# AI assist (optional)
# ----------------------------
def propose_urls_ai(company_name: str, domain: str) -> List[Tuple[str, str]]:
    """
    Uses LLM to propose URLs.
    Returns list of (url, source_type='ai_discovered')
    """
    if not (OPENAI_API_KEY and call_llm_json):
        return []

    prompt = f"""
You are helping discover the best pages for company intelligence.
Company: {company_name}
Domain: {domain}

Return STRICT JSON:
{{
  "urls": [
    {{"url": "https://{domain}/about", "why": "...", "category": "founders_team|funding|news|commercial"}}
  ]
}}

Rules:
- Only return URLs that belong to the same domain: {domain}
- Prefer: about/team, press/news/blog, funding/investors, product/solutions
- Provide 8-12 URLs max.
"""
    try:
        payload = call_llm_json(prompt)
        items = payload.get("urls", []) if isinstance(payload, dict) else []
        out = []
        for it in items:
            u = canonicalize_url(it.get("url", ""))
            if not u:
                continue
            if not is_same_domain(u, domain):
                continue
            out.append((u, "ai_discovered"))
        # dedupe
        dedup = []
        seen = set()
        for u, st in out:
            if u not in seen:
                seen.add(u)
                dedup.append((u, st))
        return dedup[:12]
    except Exception as e:
        print(f"[discover] ai_failed err={type(e).__name__}: {e}")
        return []


# ----------------------------
# Main
# ----------------------------
def discover_sources(company_id: str) -> None:
    conn = psycopg2.connect(DB)
    try:
        row = q_one(
            conn,
            "SELECT name, website, domain FROM companies WHERE company_id=%s::uuid",
            (company_id,),
        )
        if not row:
            print(f"[discover] company_not_found company_id={company_id}")
            return

        name, website, domain = row
        name = (name or "").strip()
        website = (website or "").strip()
        domain = normalize_domain(domain or website)

        if not domain:
            print(f"[discover] missing_domain company_id={company_id}")
            return

        base = canonicalize_url(website or ("https://" + domain))
        if not base:
            base = "https://" + domain

        print(f"[discover] company={name} domain={domain}")

        # 1) deterministic urls
        det = deterministic_paths(base)

        # 2) homepage crawl (best effort)
        html = fetch_homepage_html(base)
        crawled_urls = crawl_homepage_links(html, base, domain) if html else []
        crawled = [(u, "website") for u in crawled_urls]

        # 3) AI urls (optional)
        ai = propose_urls_ai(name or domain, domain)

        # merge + dedupe with priority order: deterministic -> crawled -> ai
        merged: List[Tuple[str, str]] = []
        seen = set()

        def add_many(rows):
            for u, st in rows:
                if not u:
                    continue
                u = canonicalize_url(u)
                if not u or u in seen:
                    continue
                seen.add(u)
                merged.append((u, st))

        add_many(det)
        add_many(crawled)
        add_many(ai)

        # Insert
        inserted = insert_sources(conn, company_id, merged)

        print(f"[discover] deterministic={len(det)} crawled={len(crawled)} ai={len(ai)}")
        print(f"[discover] queued_sources_inserted={inserted}")

        # sample
        print("[discover] sample:")
        for u, st in merged[:10]:
            print(f"  - {st}: {u}")

    finally:
        conn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.discover <company_uuid>")
        raise SystemExit(1)
    discover_sources(sys.argv[1].strip())


if __name__ == "__main__":
    main()