"""
finance.py
-----------
Finance stakeholder view — for the budget owner / finance director audience.

Written to be readable without knowing what a Spark job is.

Sections:
  1. Budget health banner
  2. KPI strip — total spend, daily avg, projected monthly, % of budget
  3. Weekly spend trend vs. budget line
  4. Cost by job type (pie)
  5. Top 10 most expensive jobs
  6. Daily spend table (exportable)
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from config.settings import TENANTS

COLORS = {
    "blue":    "#3b82f6",
    "teal":    "#14b8a6",
    "amber":   "#f59e0b",
    "red":     "#ef4444",
    "green":   "#22c55e",
    "surface": "#111827",
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


def _budget_status(spend: float, budget: float) -> tuple[str, str, str]:
    """Returns (status_label, color, banner_style)."""
    pct = 100 * spend / budget if budget else 0
    if pct < 80:
        return "On track", COLORS["green"], "background:#14532d;border-radius:8px;padding:12px 16px;"
    if pct < 100:
        return "At risk", COLORS["amber"], "background:#713f12;border-radius:8px;padding:12px 16px;"
    return "Over budget", COLORS["red"], "background:#7f1d1d;border-radius:8px;padding:12px 16px;"


def render(tenant_id: str, since: date, query_fn: Callable):
    tenant_cfg    = TENANTS[tenant_id]
    weekly_budget = tenant_cfg["budget_threshold_weekly"]
    daily_budget  = weekly_budget / 7.0
    since_str     = since.isoformat()
    days_in_range = (date.today() - since).days or 1

    st.markdown("## Spend & Budget — Finance View")
    st.caption(f"Tenant: **{tenant_cfg['display_name']}** · Since {since}")

    # ── 1. Budget health banner ────────────────────────────────────────────────

    spend_this_week = query_fn(f"""
        SELECT COALESCE(SUM(cost_usd), 0) AS spend
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= CURRENT_DATE - INTERVAL '7 days'
    """)

    week_spend = float(spend_this_week.iloc[0]["spend"]) if not spend_this_week.empty else 0.0
    status_label, status_color, banner_style = _budget_status(week_spend, weekly_budget)

    pct_used = 100 * week_spend / weekly_budget if weekly_budget else 0
    remaining = max(weekly_budget - week_spend, 0)

    st.markdown(
        f"""
        <div style="{banner_style}color:#f1f5f9;font-size:15px;">
            <strong>Weekly budget status: {status_label}</strong> &nbsp;—&nbsp;
            ${week_spend:,.2f} spent of ${weekly_budget:,.0f} budget
            ({pct_used:.0f}%) &nbsp;·&nbsp; ${remaining:,.2f} remaining
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(" ")

    # ── 2. KPI strip ──────────────────────────────────────────────────────────

    total_spend_df = query_fn(f"""
        SELECT COALESCE(SUM(cost_usd), 0) AS total_spend
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
    """)

    total_spend  = float(total_spend_df.iloc[0]["total_spend"]) if not total_spend_df.empty else 0.0
    daily_avg    = total_spend / days_in_range
    monthly_proj = daily_avg * 30
    budget_monthly = daily_budget * 30
    vs_budget_pct = 100 * monthly_proj / budget_monthly if budget_monthly else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total spend",          f"${total_spend:,.2f}",
              delta=f"{days_in_range}d window")
    c2.metric("Daily average",        f"${daily_avg:,.2f}",
              delta=f"Budget: ${daily_budget:,.2f}/day")
    c3.metric("Projected (30d)",      f"${monthly_proj:,.0f}")
    c4.metric("vs. Monthly budget",   f"{vs_budget_pct:.0f}%",
              delta=f"${monthly_proj - budget_monthly:+,.0f}",
              delta_color="inverse")

    st.markdown("---")

    # ── 3. Weekly spend trend vs. budget line ──────────────────────────────────

    st.markdown("##### Weekly spend vs. budget")
    weekly_df = query_fn(f"""
        SELECT
            DATE_TRUNC('week', started_at)  AS week_start,
            ROUND(SUM(cost_usd), 2)         AS total_cost
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
        GROUP BY 1
        ORDER BY 1
    """)

    if not weekly_df.empty:
        fig = go.Figure()
        fig.add_bar(
            x=weekly_df["week_start"], y=weekly_df["total_cost"],
            name="Actual spend",
            marker_color=COLORS["blue"],
            opacity=0.8,
        )
        fig.add_scatter(
            x=weekly_df["week_start"],
            y=[weekly_budget] * len(weekly_df),
            name="Weekly budget",
            mode="lines",
            line=dict(color=COLORS["red"], width=1.5, dash="dash"),
        )
        fig.update_layout(
            **PLOTLY_LAYOUT,
            height=280,
            yaxis_tickprefix="$",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No spend data available for this period.")

    st.markdown("---")

    col_a, col_b = st.columns([2, 3])

    # ── 4. Cost by job type (pie) ──────────────────────────────────────────────

    with col_a:
        st.markdown("##### Spend by workload type")
        pie_df = query_fn(f"""
            SELECT
                job_type,
                ROUND(SUM(cost_usd), 4) AS cost
            FROM vertex_job_events
            WHERE tenant_id  = '{tenant_id}'
              AND started_at >= '{since_str}'
            GROUP BY 1
        """)
        if not pie_df.empty:
            fig = px.pie(
                pie_df, values="cost", names="job_type",
                color_discrete_sequence=[COLORS["blue"], COLORS["teal"], COLORS["amber"]],
                hole=0.55,
            )
            fig.update_layout(**PLOTLY_LAYOUT, height=280, showlegend=True)
            fig.update_traces(textinfo="percent+label", textfont_color="#c9d6f0")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data.")

    # ── 5. Top 10 most expensive jobs ──────────────────────────────────────────

    with col_b:
        st.markdown("##### Top 10 most expensive jobs")
        top_df = query_fn(f"""
            SELECT
                job_name,
                job_type,
                machine_type,
                ROUND(cost_usd, 4)        AS cost_usd,
                ROUND(duration_seconds / 3600.0, 2) AS hours,
                status,
                CAST(started_at AS DATE)  AS run_date
            FROM vertex_job_events
            WHERE tenant_id  = '{tenant_id}'
              AND started_at >= '{since_str}'
            ORDER BY cost_usd DESC
            LIMIT 10
        """)
        if not top_df.empty:
            top_df["cost_usd"] = top_df["cost_usd"].apply(lambda x: f"${x:,.4f}")
            top_df["hours"]    = top_df["hours"].apply(lambda x: f"{x:.2f}h")
            st.dataframe(
                top_df,
                use_container_width=True,
                height=260,
                hide_index=True,
                column_config={
                    "job_name":    st.column_config.TextColumn("Job"),
                    "job_type":    st.column_config.TextColumn("Type"),
                    "machine_type":st.column_config.TextColumn("Machine"),
                    "cost_usd":    st.column_config.TextColumn("Cost"),
                    "hours":       st.column_config.TextColumn("Duration"),
                    "status":      st.column_config.TextColumn("Status"),
                    "run_date":    st.column_config.DateColumn("Date"),
                },
            )
        else:
            st.info("No data.")

    st.markdown("---")

    # ── 6. Daily spend table (downloadable) ───────────────────────────────────

    st.markdown("##### Daily spend breakdown")
    daily_df = query_fn(f"""
        SELECT
            CAST(started_at AS DATE)                        AS date,
            COUNT(*)                                        AS jobs,
            SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status='FAILED'    THEN 1 ELSE 0 END) AS failed,
            ROUND(SUM(gpu_hours), 2)                        AS gpu_hours,
            ROUND(SUM(cpu_hours), 2)                        AS cpu_hours,
            ROUND(SUM(cost_usd),  4)                        AS total_cost_usd,
            ROUND({daily_budget}, 2)                        AS daily_budget,
            ROUND(100.0 * SUM(cost_usd) / {daily_budget}, 1) AS pct_of_budget
        FROM vertex_job_events
        WHERE tenant_id  = '{tenant_id}'
          AND started_at >= '{since_str}'
        GROUP BY 1
        ORDER BY 1 DESC
    """)

    if not daily_df.empty:
        st.dataframe(
            daily_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "date":           st.column_config.DateColumn("Date"),
                "jobs":           st.column_config.NumberColumn("Jobs"),
                "succeeded":      st.column_config.NumberColumn("Succeeded"),
                "failed":         st.column_config.NumberColumn("Failed"),
                "gpu_hours":      st.column_config.NumberColumn("GPU hrs"),
                "cpu_hours":      st.column_config.NumberColumn("CPU hrs"),
                "total_cost_usd": st.column_config.NumberColumn("Spend ($)", format="$%.4f"),
                "daily_budget":   st.column_config.NumberColumn("Budget ($)", format="$%.2f"),
                "pct_of_budget":  st.column_config.ProgressColumn(
                    "% of budget", min_value=0, max_value=150, format="%.1f%%"
                ),
            },
        )

        csv = daily_df.to_csv(index=False)
        st.download_button(
            label="Export CSV",
            data=csv,
            file_name=f"vertex_spend_{tenant_id}_{since_str}.csv",
            mime="text/csv",
        )
    else:
        st.info("No daily data available.")