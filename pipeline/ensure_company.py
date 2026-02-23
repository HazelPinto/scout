import os
import re
import sys
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB = os.environ["DATABASE_URL"]

def norm_domain(website: str) -> str:
    if not website:
        return ""
    if not website.startswith("http"):
        website = "https://" + website
    p = urlparse(website)
    d = (p.netloc or "").lower()
    d = re.sub(r"^www\.", "", d)
    return d

def ensure_company(name: str, website: str) -> str:
    domain = norm_domain(website)
    conn = psycopg2.connect(DB)
    try:
        with conn:
            with conn.cursor() as cur:
                # upsert by domain if present else by name
                if domain:
                    cur.execute(
                        """
                        INSERT INTO companies(name, website, domain)
                        VALUES (%s, %s, %s)
                        ON CONFLICT(domain)
                        DO UPDATE SET name=EXCLUDED.name, website=EXCLUDED.website, updated_at=now()
                        RETURNING company_id::text
                        """,
                        (name, website, domain),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO companies(name, website, domain)
                        VALUES (%s, %s, %s)
                        RETURNING company_id::text
                        """,
                        (name, website, domain),
                    )
                return cur.fetchone()[0]
    finally:
        conn.close()

def main():
    if len(sys.argv) < 3:
        print("Usage: python -m pipeline.ensure_company <name> <website>")
        raise SystemExit(1)
    name = sys.argv[1].strip()
    website = sys.argv[2].strip()
    cid = ensure_company(name, website)
    print(cid)

if __name__ == "__main__":
    main()