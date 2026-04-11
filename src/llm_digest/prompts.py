"""
prompts.py
----------
Structured system and user prompts for LLM digest generation (Claude / similar).
Kept separate from digest_generator for testing and reuse.
"""

from __future__ import annotations

from datetime import date
from typing import Any

# Instructs the model to return JSON matching the digest contract.
SYSTEM_PROMPT = """You are a senior data engineer summarizing Vertex AI pipeline health for executives.

Respond with a single JSON object only (no markdown fences). Use this shape:
{
  "budget_status": "ON_TRACK" | "AT_RISK" | "OVER_BUDGET",
  "summary_text": "plain English, 3-5 sentences",
  "recommendations": ["actionable item 1", "actionable item 2"]
}

budget_status must reflect spend vs. budget and incident load. Prefer ON_TRACK when metrics are healthy.
"""


def _fmt_usd(n: float) -> str:
    return f"${n:,.2f}"


def build_daily_prompt(
    tenant_name: str,
    digest_date: date,
    metrics: dict[str, Any],
    incidents: list[dict[str, Any]],
    weekly_budget: float,
    daily_budget: float,
) -> str:
    """Build the user message for a daily digest."""
    lines = [
        f"Tenant: {tenant_name}",
        f"Digest date: {digest_date.isoformat()}",
        "",
        "## Spend & budget",
        f"- Spend today: {_fmt_usd(float(metrics['spend_today']))}",
        f"- Spend (7d): {_fmt_usd(float(metrics['spend_week']))}",
        f"- Weekly budget: {_fmt_usd(weekly_budget)}",
        f"- Daily budget (prorated): {_fmt_usd(daily_budget)}",
        f"- Budget % used (rolling): {metrics['budget_pct_used']:.1f}%",
        f"- Monthly projection: {_fmt_usd(float(metrics['monthly_projection']))}",
        "",
        "## Jobs (24h)",
        f"- Total jobs: {metrics['total_jobs_today']}",
        f"- SUCCEEDED: {metrics['succeeded_today']}",
        f"- Failed: {metrics['failed_today']}",
        f"- Success rate today: {metrics['success_rate_today']:.1f}%",
        f"- Success rate (7d): {metrics['success_rate_7d']:.1f}%",
        f"- p95 duration (h): {metrics['p95_hours_today']:.2f}",
        f"- Avg cost / job: {_fmt_usd(float(metrics['avg_cost_today']))}",
        "",
        "## Utilization",
        f"- GPU hours: {metrics['gpu_hours_today']:.1f}",
        f"- CPU hours: {metrics['cpu_hours_today']:.1f}",
        "",
        "## Incidents",
        f"- Open incidents: {metrics['open_incidents']}",
    ]
    if incidents:
        lines.append("- Recent incidents (severity / type / title):")
        for inc in incidents:
            lines.append(
                f"  - {inc.get('severity')}: {inc.get('incident_type')} — {inc.get('title')}"
            )
    lines.append("")
    lines.append(
        "Produce the JSON object described in the system prompt. "
        "Be specific and cite numbers from above."
    )
    return "\n".join(lines)


def build_weekly_prompt(
    tenant_name: str,
    week_start: date,
    week_end: date,
    metrics: dict[str, Any],
    incidents: list[dict[str, Any]],
    weekly_budget: float,
) -> str:
    """Build the user message for a weekly digest."""
    lines = [
        f"Tenant: {tenant_name}",
        f"Week window: {week_start.isoformat()} through {week_end.isoformat()}",
        "",
        "## Spend",
        f"- Spend (week): {_fmt_usd(float(metrics['spend_week']))}",
        f"- Weekly budget: {_fmt_usd(weekly_budget)}",
        f"- Budget % used: {metrics['budget_pct_used']:.1f}%",
        f"- Budget remaining: {_fmt_usd(float(metrics['budget_remaining']))}",
        "",
        "## Utilization",
        f"- GPU hours (week): {metrics['gpu_hours_week']:.1f}",
        f"- CPU hours (week): {metrics['cpu_hours_week']:.1f}",
        f"- GPU share of cost: {metrics['gpu_cost_pct']:.1f}%",
        "",
        "## Jobs",
        f"- Total jobs: {metrics['total_jobs_week']}",
        f"- SUCCEEDED (week): {metrics['succeeded_week']}",
        f"- Failed (week): {metrics['failed_week']}",
        f"- Success rate: {metrics['success_rate_week']:.1f}%",
        "",
        "## Peaks",
        f"- Peak day: {metrics['peak_day']} ({_fmt_usd(float(metrics['peak_day_cost']))})",
        f"- Trough day: {metrics['trough_day']} ({_fmt_usd(float(metrics['trough_day_cost']))})",
        "",
        "## Incidents (week)",
        f"- Total: {metrics['incidents_total']}, CRITICAL: {metrics['incidents_critical']}, HIGH: {metrics['incidents_high']}",
    ]
    if incidents:
        lines.append("- Detail:")
        for inc in incidents:
            lines.append(f"  - {inc}")
    lines.append("")
    lines.append("Produce the JSON object described in the system prompt.")
    return "\n".join(lines)
