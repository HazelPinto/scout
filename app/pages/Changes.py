import os
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
DB = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Changes", layout="wide")
st.title("Latest changes")

def q_all(query, params=None):
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return cols, rows
    finally:
        conn.close()

# Optional filter: choose company
cols, companies = q_all("SELECT company_id::text, name FROM companies ORDER BY name LIMIT 200")

company_map = {name: cid for cid, name in companies}
picked_name = st.selectbox("Company", options=list(company_map.keys()))
company_id = company_map[picked_name]

st.caption(f"Company ID: {company_id}")

cols2, rows = q_all(
    """
    SELECT detected_at, change_type, object_type, object_id::text, source_url, details
    FROM changes
    WHERE company_id=%s::uuid
    ORDER BY detected_at DESC
    LIMIT 100
    """,
    (company_id,),
)

if not rows:
    st.info("No changes recorded yet. Run the diff job after new upserts.")
else:
    for detected_at, change_type, object_type, object_id, source_url, details in rows:
        left, right = st.columns([3, 2])
        with left:
            st.write(f"**{change_type}** — `{object_type}` `{object_id}`")
            st.write(f"{detected_at}")
            if details:
                st.json(details)
        with right:
            if source_url:
                st.link_button("Open source", source_url)
            else:
                st.write("No source_url")
        st.divider()