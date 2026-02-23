import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import streamlit as st

load_dotenv()
DB = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Scout Console", layout="wide")

@st.cache_data(ttl=30)
def load_companies(q: str = "") -> pd.DataFrame:
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            if q:
                cur.execute(
                    """
                    SELECT company_id::text, name, website, domain, updated_at
                    FROM companies
                    WHERE lower(name) LIKE %s OR lower(domain) LIKE %s
                    ORDER BY updated_at DESC
                    LIMIT 200
                    """,
                    (f"%{q.lower()}%", f"%{q.lower()}%"),
                )
            else:
                cur.execute(
                    """
                    SELECT company_id::text, name, website, domain, updated_at
                    FROM companies
                    ORDER BY updated_at DESC
                    LIMIT 200
                    """
                )
            rows = cur.fetchall()
    finally:
        conn.close()

    return pd.DataFrame(rows, columns=["company_id", "name", "website", "domain", "updated_at"])

st.title("Scout Console")
st.caption("Internal console: companies, sources, evidence-first facts.")

q = st.text_input("Search company (name or domain)", value="")
df = load_companies(q)

st.subheader(f"Companies ({len(df)})")
st.dataframe(df, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Open company")

if len(df) == 0:
    st.info("No companies yet. Insert one in Neon or via pipeline.")
else:
    default_idx = 0
    selected = st.selectbox(
        "Select a company",
        options=list(range(len(df))),
        format_func=lambda i: f"{df.iloc[i]['name']} — {df.iloc[i]['domain'] or ''}",
        index=default_idx,
    )
    company_id = df.iloc[selected]["company_id"]
    st.page_link("pages/Company.py", label="Go to Company Detail", icon="➡️", disabled=False)
    st.code(f"Company ID: {company_id}")
    st.caption("Open Company Detail and paste the Company ID there (simple MVP).")
