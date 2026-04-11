"""
technical.py
-------------
Technical stakeholder view — for the DE / platform engineer audience.

Sections:
  1. KPI strip — total jobs, success rate, p95 latency, open incidents
  2. Job volume + success rate over time (dual-axis)
  3. Latency distribution (box plot by job type)
  4. Failure heatmap (hour × day of week)
  5. Open incidents table with severity badges
  6. GPU vs. CPU cost breakdown
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

COLORS = {
    "blue":   "#3b82f6",
    "teal":   "#14b8a6",
    "amber":  "#f59e0b",
    "red":    "#ef4444",
    "green":  "#22c55e",
    "purple": "#8b5cf6",
    "muted":  "#334155",
    "surface": "#111827",
    "border": "rgba(255,255,255,0.08)",
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#8b9ab8", size=12),
    margin=dict(l=0, r=0, t=32, b=0),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#c9d6f0")),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(0,0,0,0)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(0,0,0,0)"),
)


def _num(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        x = float(val)
        if math.isnan(x):
            return default
        return x
    except (TypeError, ValueError):
        return default


def _int(val: Any) -> int:
    return int(round(_num(val, 0.0)))


def render(tenant_id: str, since: date, query_fn: Callable):
    st.markdown("## Pipeline Health — Technical View")
    st.caption(f"Tenant: **{tenant_id}** · Since {since}")

    since_str = since.isoformat()

    # ── 1. KPI strip ──────────────────────────────────────────────────────────

    kpi_df = query_fn(f"""
        SELECT
            COUNT(*)::BIGINT                                              AS total_jobs,
            COALESCE(
                ROUND(100.0 * SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 1),
                0
            )                                                             AS success_rate,
            COALESCE(
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                      (ORDER BY duration_seconds) / 3600.0, 2),
                0
            )                                                             AS p95_hours,
            COALESCE(SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END), 0)::BIGINT
                                                                          AS total_failures
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
    """)

    open_incidents = query_fn(f"""
        SELECT COUNT(*) AS cnt
        FROM pipeline_incidents
        WHERE tenant_id = '{tenant_id}'
          AND status    = 'OPEN'
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    if kpi_df.empty:
        row = None
    else:
        row = kpi_df.iloc[0]
    c1.metric("Total Jobs", f"{_int(row.total_jobs) if row is not None else 0:,}")
    c2.metric("Success Rate", f"{_num(row.success_rate if row is not None else 0):.1f}%")
    c3.metric("p95 Duration", f"{_num(row.p95_hours if row is not None else 0):.2f}h")
    c4.metric("Failed Jobs", f"{_int(row.total_failures) if row is not None else 0:,}")
    cnt = _int(open_incidents.iloc[0]["cnt"]) if not open_incidents.empty else 0
    c5.metric("Open Incidents", f"{cnt:,}")

    st.markdown("---")

    # ── 2. Job volume + success rate over time ─────────────────────────────────

    trend_df = query_fn(f"""
        SELECT
            CAST(started_at AS DATE)                                        AS day,
            COUNT(*)::BIGINT                                                AS total_jobs,
            COALESCE(
                ROUND(100.0 * SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 1),
                0
            )                                                               AS success_rate
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
        GROUP BY 1
        ORDER BY 1
    """)

    col_a, col_b = st.columns([3, 2])

    with col_a:
        st.markdown("##### Job volume & success rate")
        if not trend_df.empty:
            fig = go.Figure()
            fig.add_bar(
                x=trend_df["day"], y=trend_df["total_jobs"],
                name="Total jobs", marker_color=COLORS["blue"],
                opacity=0.7,
            )
            fig.add_scatter(
                x=trend_df["day"], y=trend_df["success_rate"],
                name="Success rate %", yaxis="y2",
                line=dict(color=COLORS["green"], width=2),
                mode="lines+markers",
            )
            fig.update_layout(
                **PLOTLY_LAYOUT,
                yaxis2=dict(
                    overlaying="y", side="right",
                    range=[0, 105],
                    gridcolor="rgba(0,0,0,0)",
                    ticksuffix="%",
                    color="#8b9ab8",
                ),
                height=280,
                barmode="group",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet. Start the producer to generate events.")

    # ── 3. Latency distribution by job type ────────────────────────────────────

    with col_b:
        st.markdown("##### Latency by job type (hours)")
        latency_df = query_fn(f"""
            SELECT job_type, ROUND(duration_seconds / 3600.0, 3) AS duration_hours
            FROM vertex_job_events
            WHERE tenant_id  = '{tenant_id}'
              AND started_at >= '{since_str}'
              AND status IN ('SUCCEEDED', 'FAILED')
        """)
        if not latency_df.empty:
            fig = px.box(
                latency_df, x="job_type", y="duration_hours",
                color="job_type",
                color_discrete_sequence=[COLORS["blue"], COLORS["teal"], COLORS["purple"]],
            )
            fig.update_layout(**PLOTLY_LAYOUT, height=280, showlegend=False)
            fig.update_traces(marker_color=COLORS["blue"])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Awaiting data.")

    st.markdown("---")

    # ── 4. Failure heatmap — hour × weekday ────────────────────────────────────

    st.markdown("##### Failure heatmap (hour of day × weekday)")
    heat_df = query_fn(f"""
        SELECT
            EXTRACT(DOW  FROM started_at) AS weekday,
            EXTRACT(HOUR FROM started_at) AS hour,
            COUNT(*) AS failures
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
          AND status     = 'FAILED'
        GROUP BY 1, 2
    """)

    if not heat_df.empty:
        day_map = {0:"Sun",1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri",6:"Sat"}
        heat_df["weekday_name"] = heat_df["weekday"].astype(int).map(day_map)
        pivot = heat_df.pivot_table(
            index="weekday_name", columns="hour",
            values="failures", aggfunc="sum", fill_value=0,
        )
        ordered_days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        pivot = pivot.reindex([d for d in ordered_days if d in pivot.index])
        fig = px.imshow(
            pivot,
            color_continuous_scale=[[0,"#0d1221"],[0.5,"#7c2d12"],[1.0,"#ef4444"]],
            aspect="auto",
            labels=dict(x="Hour of day", y="", color="Failures"),
        )
        fig.update_layout(**PLOTLY_LAYOUT, height=200)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No failures recorded in this window.")

    st.markdown("---")

    # ── 5. Open incidents ──────────────────────────────────────────────────────

    st.markdown("##### Open incidents")
    inc_df = query_fn(f"""
        SELECT
            detected_at,
            severity,
            incident_type,
            title,
            recommended_action
        FROM pipeline_incidents
        WHERE tenant_id = '{tenant_id}'
          AND status    = 'OPEN'
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 0
                WHEN 'HIGH'     THEN 1
                WHEN 'MEDIUM'   THEN 2
                ELSE 3
            END,
            detected_at DESC
        LIMIT 20
    """)

    if inc_df.empty:
        st.success("No open incidents.")
    else:
        for _, row in inc_df.iterrows():
            sev_raw = str(row["severity"])
            sev_key = sev_raw.lower()
            title_short = str(row["title"])[:100] + ("…" if len(str(row["title"])) > 100 else "")
            # Expander labels do not render HTML — use plain prefix; badge inside body.
            with st.expander(f"[{sev_raw}] {title_short}", expanded=False):
                st.markdown(
                    f'<span class="badge-{sev_key}">{sev_raw}</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(f"**Title:** {row['title']}")
                st.markdown(f"**Type:** `{row['incident_type']}`")
                st.markdown(f"**Detected:** {row['detected_at']}")
                if row["recommended_action"]:
                    st.markdown(f"**Action:** {row['recommended_action']}")

    # ── 6. GPU vs CPU cost split ───────────────────────────────────────────────

    st.markdown("---")
    st.markdown("##### GPU vs CPU cost breakdown")
    cost_df = query_fn(f"""
        SELECT
            CASE WHEN gpu_count > 0 THEN 'GPU' ELSE 'CPU-only' END AS resource_type,
            job_type,
            ROUND(SUM(cost_usd), 4)  AS total_cost,
            COUNT(*)                 AS job_count
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
        GROUP BY 1, 2
    """)

    if not cost_df.empty:
        fig = px.bar(
            cost_df, x="job_type", y="total_cost",
            color="resource_type", barmode="group",
            color_discrete_map={"GPU": COLORS["amber"], "CPU-only": COLORS["blue"]},
            labels={"total_cost": "Cost (USD)", "job_type": "Job type"},
        )
        fig.update_layout(**PLOTLY_LAYOUT, height=260)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Awaiting cost data.")