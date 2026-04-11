"""Streamlit overview for Vertex pipeline operations."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DUCKDB_PATH, TENANTS  # noqa: E402

st.set_page_config(page_title="Vertex Ops", layout="wide")
st.title("Vertex pipeline operations")

try:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    events = conn.execute("SELECT COUNT(*) FROM vertex_job_events").fetchone()[0]
    incidents = conn.execute(
        "SELECT COUNT(*) FROM pipeline_incidents WHERE status = 'OPEN'"
    ).fetchone()[0]
    conn.close()
    c1, c2 = st.columns(2)
    c1.metric("Job events (total)", f"{events:,}")
    c2.metric("Open incidents", f"{incidents:,}")
except Exception as exc:  # noqa: BLE001
    st.warning(f"DuckDB not ready: {exc}")

st.subheader("Tenants")
st.json({k: v.get("display_name", k) for k, v in TENANTS.items()})
