"""
app.py
-------
Streamlit entry point for the Vertex AI Pipeline Operations Intelligence Hub.
Renders the sidebar, tenant selector, and routes to the correct view.

Run:
    streamlit run src/dashboards/app.py
"""

import streamlit as st
import duckdb
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import DUCKDB_PATH, TENANTS, DEFAULT_TENANT

st.set_page_config(
    page_title="Vertex Ops Hub",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global styles ──────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* App shell */
    .stApp {
        background: #0b1020 !important;
        color: #c9d6f0;
    }

    /* Top bar — remove bright white strip (Streamlit header / toolbar) */
    header[data-testid="stHeader"] {
        background: #0a0f1e !important;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    div[data-testid="stToolbar"] {
        background: transparent !important;
    }
    div[data-testid="stDecoration"] {
        display: none;
    }
    [data-testid="stHeader"] button,
    [data-testid="stHeader"] [data-baseweb="button"] {
        color: #c9d6f0 !important;
    }

    /* Main content */
    .block-container { padding-top: 1.25rem; max-width: 1400px; }
    .stMainBlockContainer { color: #c9d6f0; }
    .stMarkdown, .stMarkdown p, .stMarkdown li { color: #c9d6f0 !important; }
    [data-testid="stCaption"] { color: #8b9ab8 !important; }

    /* sidebar */
    [data-testid="stSidebar"] { background: #0a0f1e !important; }
    [data-testid="stSidebar"] * { color: #c9d6f0 !important; }
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] label { color: #e8eeff !important; }

    /* metric cards */
    [data-testid="metric-container"] {
        background: #111827 !important;
        border: 0.5px solid rgba(255,255,255,0.08);
        border-radius: 10px;
        padding: 1rem;
    }
    [data-testid="metric-container"] label { color: #8b9ab8 !important; }
    div[data-testid="stMetricValue"] { color: #e8eeff !important; font-size: 28px !important; }
    div[data-testid="stMetricDelta"] { color: #94a3b8 !important; }

    /* headings */
    h1, h2, h3, h4, h5 { color: #e8eeff !important; font-weight: 600 !important; }

    /* dataframe */
    [data-testid="stDataFrame"] { border-radius: 10px; }

    /* tab bar */
    .stTabs [data-baseweb="tab-list"] {
        background: #111827;
        border-radius: 8px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] { color: #8b9ab8; }
    .stTabs [aria-selected="true"] {
        background: #1c2540 !important;
        color: #e8eeff !important;
        border-radius: 6px;
    }

    /* severity badges */
    .badge-critical { background:#7f1d1d; color:#fca5a5; padding:2px 8px; border-radius:4px; font-size:12px; }
    .badge-high     { background:#7c2d12; color:#fdba74; padding:2px 8px; border-radius:4px; font-size:12px; }
    .badge-medium   { background:#713f12; color:#fde68a; padding:2px 8px; border-radius:4px; font-size:12px; }
    .badge-low      { background:#14532d; color:#86efac; padding:2px 8px; border-radius:4px; font-size:12px; }

    /* Alerts / info boxes */
    .stAlert { background: #111827 !important; border: 1px solid rgba(255,255,255,0.08); color: #c9d6f0 !important; }

    /* Expanders */
    .streamlit-expanderHeader { color: #e8eeff !important; }
</style>
""", unsafe_allow_html=True)


# ── DB connection ──────────────────────────────────────────────────────────────
# Fresh read-only handle per query so new rows (e.g. after seed_demo_data.py)
# show up without relying on a long-lived cached connection snapshot.

def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as conn:
            return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### ⬡ Vertex Ops Hub")
    st.markdown("---")

    tenant_labels = {k: v["display_name"] for k, v in TENANTS.items()}
    selected_key  = st.selectbox(
        "Tenant",
        options=list(tenant_labels.keys()),
        format_func=lambda k: tenant_labels[k],
        index=list(tenant_labels.keys()).index(DEFAULT_TENANT),
    )

    st.markdown("---")
    view = st.radio(
        "View",
        options=["Technical", "Finance"],
        index=0,
    )

    st.markdown("---")
    date_range = st.slider(
        "Look-back (days)",
        min_value=1,
        max_value=30,
        value=7,
    )
    since = date.today() - timedelta(days=date_range)

    st.markdown("---")
    if st.button("Refresh data"):
        st.rerun()

    st.caption(f"DuckDB · {Path(DUCKDB_PATH).name}")
    with st.expander("Database path", expanded=False):
        st.code(DUCKDB_PATH, language=None)


# ── Route to view ──────────────────────────────────────────────────────────────

if view == "Technical":
    from src.dashboards.views.technical import render
    render(tenant_id=selected_key, since=since, query_fn=query)
else:
    from src.dashboards.views.finance import render
    render(tenant_id=selected_key, since=since, query_fn=query)