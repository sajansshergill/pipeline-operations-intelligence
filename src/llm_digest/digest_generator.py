"""Generate short operational digests; uses Claude when ANTHROPIC_API_KEY is set."""

from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Any

import duckdb

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import ANTHROPIC_API_KEY, DUCKDB_PATH, TENANTS

log = logging.getLogger(__name__)


class DigestGenerator:
    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id

    def _metrics_snapshot(self) -> dict[str, Any]:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            inc = conn.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(metric_value), 0)
                FROM pipeline_incidents
                WHERE tenant_id = ? AND detected_at > now() - INTERVAL '24 hours'
                """,
                [self.tenant_id],
            ).fetchone()
            jobs = conn.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(cost_usd), 0)
                FROM vertex_job_events
                WHERE tenant_id = ? AND started_at > now() - INTERVAL '24 hours'
                """,
                [self.tenant_id],
            ).fetchone()
        finally:
            conn.close()
        return {
            "incidents_24h": int(inc[0]),
            "incident_metric_sum": float(inc[1]),
            "jobs_24h": int(jobs[0]),
            "cost_24h": float(jobs[1]),
        }

    def _persist(
        self,
        digest_id: str,
        period: str,
        summary_text: str,
        incidents_count: int,
        total_cost_usd: float,
        budget_status: str,
    ) -> None:
        conn = duckdb.connect(DUCKDB_PATH)
        try:
            conn.execute(
                """
                INSERT INTO llm_digests (
                    digest_id, tenant_id, digest_date, period, summary_text,
                    incidents_count, total_cost_usd, budget_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    digest_id,
                    self.tenant_id,
                    date.today(),
                    period,
                    summary_text,
                    incidents_count,
                    total_cost_usd,
                    budget_status,
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def generate(self, period: str = "DAILY") -> dict[str, Any]:
        snap = self._metrics_snapshot()
        digest_id = str(uuid.uuid4())

        if ANTHROPIC_API_KEY:
            summary = self._generate_claude(period, snap)
            budget_status = "AT_RISK" if snap["incidents_24h"] > 2 else "ON_TRACK"
        else:
            summary = (
                f"[{self.tenant_id}] {period} digest (no ANTHROPIC_API_KEY): "
                f"{snap['jobs_24h']} jobs in 24h, ${snap['cost_24h']:.2f} cost, "
                f"{snap['incidents_24h']} incidents."
            )
            budget_status = "ON_TRACK"
            log.info("Skipping Claude call — ANTHROPIC_API_KEY not set")

        self._persist(
            digest_id,
            period,
            summary,
            snap["incidents_24h"],
            snap["cost_24h"],
            budget_status,
        )
        return {"summary_text": summary, "digest_id": digest_id}

    def _generate_claude(self, period: str, snap: dict[str, Any]) -> str:
        try:
            import anthropic
        except ImportError:
            return (
                f"Anthropic SDK unavailable; stub summary for {self.tenant_id}: {snap}"
            )

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        display = TENANTS.get(self.tenant_id, {}).get("display_name", self.tenant_id)
        user_msg = (
            f"Tenant: {display} ({self.tenant_id}), period={period}.\n"
            f"Jobs (24h): {snap['jobs_24h']}, cost: ${snap['cost_24h']:.2f}, "
            f"incidents: {snap['incidents_24h']}.\n"
            "Write 3–5 sentences for operators: tone factual, no markdown headings."
        )
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=512,
            messages=[{"role": "user", "content": user_msg}],
        )
        block = msg.content[0]
        if hasattr(block, "text"):
            return block.text
        return str(block)
