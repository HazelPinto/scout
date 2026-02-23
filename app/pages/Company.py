import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import streamlit as st

load_dotenv()
DB = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Company Detail", layout="wide")


def q_one(query: str, params=()):
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
    finally:
        conn.close()


def q_all(query: str, params=()):
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


st.title("Company Detail")

company_id = st.text_input(
    "Company ID (uuid)",
    value="",
    placeholder="paste company_id here (from Home / Neon)",
)

if not company_id:
    st.info("Paste a company_id from Home or Neon.")
    st.stop()

# --- Company metadata ---
company = q_one(
    """
    SELECT company_id::text, name, website, domain, created_at, updated_at
    FROM companies
    WHERE company_id = %s::uuid
    """,
    (company_id,),
)

if not company:
    st.error("Company not found.")
    st.stop()

(cid, name, website, domain, created_at, updated_at) = company

col1, col2 = st.columns([2, 1])
with col1:
    st.header(name)
    st.write(f"**Domain:** {domain or '—'}")
    st.write(f"**Website:** {website or '—'}")
with col2:
    st.caption("Metadata")
    st.write(f"Created: {created_at}")
    st.write(f"Updated: {updated_at}")

st.divider()

# --- Sources ---
st.subheader("Sources fetched")

sources_rows = q_all(
    """
    SELECT url,
           source_type::text,
           fetched_at,
           LEFT(content_hash, 12) AS h12,
           CASE WHEN clean_text IS NULL THEN 0 ELSE LENGTH(clean_text) END AS text_len,
           source_id::text
    FROM sources
    WHERE company_id = %s::uuid
    ORDER BY fetched_at DESC NULLS LAST
    """,
    (company_id,),
)

df_sources = pd.DataFrame(
    sources_rows,
    columns=["url", "source_type", "fetched_at", "hash12", "text_len", "source_id"],
)

st.dataframe(df_sources, use_container_width=True, hide_index=True)

with st.expander("Peek clean_text for a source (optional)"):
    src_pick = st.text_input("source_id (to preview clean_text)", value="")
    if src_pick:
        row = q_one(
            "SELECT url, LEFT(clean_text, 4000) FROM sources WHERE source_id=%s::uuid",
            (src_pick,),
        )
        if not row:
            st.warning("source_id not found.")
        else:
            u, preview = row
            st.write(f"**URL:** {u}")
            st.code(preview or "", language="text")
            st.caption("Preview shows first 4000 chars.")

st.divider()

# --- People ---
st.subheader("Founders / Team")

people_rows = q_all(
    """
    SELECT person_id::text, name, role, linkedin_url, needs_review, is_final, updated_at
    FROM people
    WHERE company_id = %s::uuid
    ORDER BY is_final DESC, updated_at DESC
    """,
    (company_id,),
)

df_people = pd.DataFrame(
    people_rows,
    columns=["person_id", "name", "role", "linkedin_url", "needs_review", "is_final", "updated_at"],
)

if len(df_people) == 0:
    st.info("No people yet. Run extraction + upsert to populate.")
else:
    st.dataframe(df_people, use_container_width=True, hide_index=True)

st.caption("View evidence for a person (paste person_id):")
person_pick = st.text_input("person_id", value="")
if person_pick:
    ev = q_all(
        """
        SELECT field, value, url, quote, confidence, extractor_version, created_at
        FROM evidence
        WHERE object_type='person' AND object_id=%s::uuid
        ORDER BY created_at DESC
        """,
        (person_pick,),
    )
    df_ev = pd.DataFrame(ev, columns=["field", "value", "url", "quote", "confidence", "extractor_version", "created_at"])
    if len(df_ev) == 0:
        st.warning("No evidence for that person_id.")
    else:
        st.dataframe(df_ev, use_container_width=True, hide_index=True)

st.divider()

# --- Events ---
st.subheader("Events")

events_rows = q_all(
    """
    SELECT event_id::text, event_type::text, event_date, title, summary, needs_review, is_final, updated_at
    FROM events
    WHERE company_id = %s::uuid
    ORDER BY is_final DESC, event_date DESC NULLS LAST, updated_at DESC
    """,
    (company_id,),
)

df_events = pd.DataFrame(
    events_rows,
    columns=["event_id", "type", "date", "title", "summary", "needs_review", "is_final", "updated_at"],
)

if len(df_events) == 0:
    st.info("No events yet. Run extraction + upsert to populate.")
else:
    st.dataframe(df_events, use_container_width=True, hide_index=True)

st.caption("View evidence for an event (paste event_id):")
event_pick = st.text_input("event_id", value="")
if event_pick:
    ev = q_all(
        """
        SELECT field, value, url, quote, confidence, extractor_version, created_at
        FROM evidence
        WHERE object_type='event' AND object_id=%s::uuid
        ORDER BY created_at DESC
        """,
        (event_pick,),
    )
    df_ev = pd.DataFrame(ev, columns=["field", "value", "url", "quote", "confidence", "extractor_version", "created_at"])
    if len(df_ev) == 0:
        st.warning("No evidence for that event_id.")
    else:
        st.dataframe(df_ev, use_container_width=True, hide_index=True)