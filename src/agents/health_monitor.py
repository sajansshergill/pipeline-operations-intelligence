"""
health_monitor.py
------------------
Checks pipeline health across configurable thresholds and writes
structured incident records to DuckDB.

Checks performed:
  - Job success rate (last 1h, last 24h)
  - SLA breach detection (p95 latency vs. threshold)
  - Daily budget burn rate
  - GPU utilization floor
  - Pipeline staleness (no events in N minutes)

Can be run standalone or invoked by health_check_dag.py.

Run:
    python src/agents/health_monitor.py
    python src/agents/health_monitor.py --tenant client_acme
"""

from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    DUCKDB_PATH,
    TENANTS,
    DEFAULT_TENANT,
    COST_OVERRUN_PCT,
    GPU_UTILIZATION_FLOOR_PCT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HEALTH MONITOR] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────

MIN_SUCCESS_RATE_1H  = 85.0   # %
MIN_SUCCESS_RATE_24H = 90.0   # %
MAX_P95_LATENCY_SEC  = 10800  # 3 hours
STALENESS_MINUTES    = 30     # flag if no events in this window


@dataclass
class CheckResult:
    name:       str
    passed:     bool
    severity:   str         # LOW | MEDIUM | HIGH | CRITICAL
    metric:     float | None = None
    threshold:  float | None = None
    message:    str          = ""
    action:     str          = ""


@dataclass
class MonitorReport:
    tenant_id:        str
    run_at:           str = field(default_factory=lambda: datetime.utcnow().isoformat())
    checks_total:     int = 0
    checks_passed:    int = 0
    incidents_opened: int = 0
    results:          list[CheckResult] = field(default_factory=list)


# ── Health Monitor ─────────────────────────────────────────────────────────────

class HealthMonitor:

    def __init__(self, tenant_id: str = DEFAULT_TENANT):
        self.tenant_id  = tenant_id
        self.tenant_cfg = TENANTS[tenant_id]
        self.budget_daily = self.tenant_cfg["budget_threshold_weekly"] / 7.0
        self.conn: duckdb.DuckDBPyConnection | None = None

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _connect(self):
        self.conn = duckdb.connect(DUCKDB_PATH)

    def _close(self):
        if self.conn:
            self.conn.close()

    def _scalar(self, sql: str) -> Any:
        row = self.conn.execute(sql).fetchone()
        return row[0] if row else None

    # ── Individual checks ──────────────────────────────────────────────────────

    def check_success_rate_1h(self) -> CheckResult:
        total = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND started_at > now() - INTERVAL '1 hour'
        """) or 0

        if total == 0:
            return CheckResult(
                name="success_rate_1h", passed=True,
                severity="LOW", metric=None,
                message="No jobs in last hour — skipping.",
            )

        succeeded = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND status    = 'SUCCEEDED'
              AND started_at > now() - INTERVAL '1 hour'
        """) or 0

        rate = 100.0 * succeeded / total
        passed = rate >= MIN_SUCCESS_RATE_1H
        return CheckResult(
            name="success_rate_1h",
            passed=passed,
            severity="HIGH" if rate < 70 else "MEDIUM",
            metric=round(rate, 2),
            threshold=MIN_SUCCESS_RATE_1H,
            message=f"1h success rate: {rate:.1f}% ({succeeded}/{total} jobs)",
            action=(
                "Investigate recent job failures. Check Vertex AI logs for OOM, "
                "dataset schema errors, or infrastructure issues."
                if not passed else ""
            ),
        )

    def check_success_rate_24h(self) -> CheckResult:
        total = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND started_at > now() - INTERVAL '24 hours'
        """) or 0

        if total == 0:
            return CheckResult(
                name="success_rate_24h", passed=True,
                severity="LOW", message="No jobs in last 24h.",
            )

        succeeded = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND status    = 'SUCCEEDED'
              AND started_at > now() - INTERVAL '24 hours'
        """) or 0

        rate = 100.0 * succeeded / total
        passed = rate >= MIN_SUCCESS_RATE_24H
        return CheckResult(
            name="success_rate_24h",
            passed=passed,
            severity="MEDIUM",
            metric=round(rate, 2),
            threshold=MIN_SUCCESS_RATE_24H,
            message=f"24h success rate: {rate:.1f}% ({succeeded}/{total} jobs)",
            action=(
                "Review 24h failure trend. Consider rollback if rate dropped sharply."
                if not passed else ""
            ),
        )

    def check_p95_latency(self) -> CheckResult:
        p95 = self._scalar(f"""
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds)
            FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND started_at > now() - INTERVAL '24 hours'
              AND status IN ('SUCCEEDED', 'FAILED')
        """)

        if p95 is None:
            return CheckResult(
                name="p95_latency", passed=True,
                severity="LOW", message="Insufficient data for p95 latency.",
            )

        passed = p95 <= MAX_P95_LATENCY_SEC
        return CheckResult(
            name="p95_latency",
            passed=passed,
            severity="HIGH" if p95 > MAX_P95_LATENCY_SEC * 1.5 else "MEDIUM",
            metric=round(p95, 1),
            threshold=float(MAX_P95_LATENCY_SEC),
            message=f"p95 job duration: {p95/3600:.2f}h (threshold: {MAX_P95_LATENCY_SEC/3600:.1f}h)",
            action=(
                "p95 latency exceeds SLA. Investigate data volume growth, "
                "hardware provisioning, or batch size configuration."
                if not passed else ""
            ),
        )

    def check_daily_budget(self) -> CheckResult:
        spend_today = self._scalar(f"""
            SELECT COALESCE(SUM(cost_usd), 0)
            FROM vertex_job_events
            WHERE tenant_id  = '{self.tenant_id}'
              AND CAST(started_at AS DATE) = CURRENT_DATE
        """) or 0.0

        threshold = self.budget_daily * (1 + COST_OVERRUN_PCT / 100)
        passed = spend_today <= threshold
        pct_of_budget = 100 * spend_today / self.budget_daily if self.budget_daily else 0

        return CheckResult(
            name="daily_budget",
            passed=passed,
            severity=(
                "CRITICAL" if pct_of_budget > 150
                else "HIGH"   if pct_of_budget > 120
                else "MEDIUM"
            ),
            metric=round(spend_today, 4),
            threshold=round(threshold, 4),
            message=(
                f"Daily spend: ${spend_today:.2f} "
                f"({pct_of_budget:.0f}% of ${self.budget_daily:.2f} daily budget)"
            ),
            action=(
                f"Budget overrun by {pct_of_budget - 100:.0f}%. "
                "Suspend non-critical training jobs and review GPU machine type selection."
                if not passed else ""
            ),
        )

    def check_pipeline_staleness(self) -> CheckResult:
        minutes_since_last = self._scalar(f"""
            SELECT DATEDIFF('minute', MAX(ingested_at), now())
            FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
        """)

        if minutes_since_last is None:
            return CheckResult(
                name="pipeline_staleness",
                passed=False,
                severity="HIGH",
                message="No events found for this tenant — pipeline may never have run.",
                action="Check Kafka producer and consumer are running.",
            )

        passed = minutes_since_last <= STALENESS_MINUTES
        return CheckResult(
            name="pipeline_staleness",
            passed=passed,
            severity="HIGH" if minutes_since_last > 60 else "MEDIUM",
            metric=float(minutes_since_last),
            threshold=float(STALENESS_MINUTES),
            message=f"Last event ingested {minutes_since_last} minutes ago.",
            action=(
                "Pipeline appears stale. Verify Kafka consumer is running "
                "and Kafka topic has active producers."
                if not passed else ""
            ),
        )

    def check_gpu_cost_efficiency(self) -> CheckResult:
        """
        Flag jobs where GPU count > 0 but duration is suspiciously short
        (likely GPU underutilization — paying for GPU without using it).
        """
        total_gpu_jobs = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND gpu_count > 0
              AND started_at > now() - INTERVAL '24 hours'
        """) or 0

        if total_gpu_jobs == 0:
            return CheckResult(
                name="gpu_cost_efficiency", passed=True,
                severity="LOW", message="No GPU jobs in last 24h.",
            )

        # Jobs finishing in < 5 min with GPU provisioned = waste
        wasteful = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id      = '{self.tenant_id}'
              AND gpu_count       > 0
              AND duration_seconds < 300
              AND started_at      > now() - INTERVAL '24 hours'
        """) or 0

        waste_pct = 100.0 * wasteful / total_gpu_jobs
        passed = waste_pct < GPU_UTILIZATION_FLOOR_PCT

        return CheckResult(
            name="gpu_cost_efficiency",
            passed=passed,
            severity="MEDIUM",
            metric=round(waste_pct, 1),
            threshold=GPU_UTILIZATION_FLOOR_PCT,
            message=(
                f"{wasteful}/{total_gpu_jobs} GPU jobs completed in <5min "
                f"({waste_pct:.1f}% potential waste)"
            ),
            action=(
                "Consider switching short GPU jobs to CPU-only machine types. "
                "GPU spin-up overhead dominates jobs under 5 minutes."
                if not passed else ""
            ),
        )

    # ── Main run ───────────────────────────────────────────────────────────────

    def run(self) -> dict:
        self._connect()
        report = MonitorReport(tenant_id=self.tenant_id)

        checks = [
            self.check_success_rate_1h,
            self.check_success_rate_24h,
            self.check_p95_latency,
            self.check_daily_budget,
            self.check_pipeline_staleness,
            self.check_gpu_cost_efficiency,
        ]

        for check_fn in checks:
            result = check_fn()
            report.results.append(result)
            report.checks_total += 1
            if result.passed:
                report.checks_passed += 1
                log.info(f"[PASS] {result.name}: {result.message}")
            else:
                log.warning(f"[FAIL] {result.name}: {result.message}")
                self._write_incident(result)
                report.incidents_opened += 1

        self.conn.commit()
        self._close()

        log.info(
            f"Health check complete — tenant={self.tenant_id} "
            f"passed={report.checks_passed}/{report.checks_total} "
            f"incidents={report.incidents_opened}"
        )

        return {
            "tenant_id":        report.tenant_id,
            "run_at":           report.run_at,
            "checks_total":     report.checks_total,
            "checks_passed":    report.checks_passed,
            "incidents_opened": report.incidents_opened,
        }

    def _write_incident(self, result: CheckResult):
        self.conn.execute(
            """
            INSERT OR IGNORE INTO pipeline_incidents
            (incident_id, tenant_id, incident_type, severity,
             title, description, metric_value, threshold_value, recommended_action)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                self.tenant_id,
                result.name.upper(),
                result.severity,
                f"[{self.tenant_id}] {result.name}: {result.message[:120]}",
                result.message,
                result.metric,
                result.threshold,
                result.action or None,
            ),
        )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertex AI Health Monitor")
    parser.add_argument("--tenant", default=DEFAULT_TENANT, choices=list(TENANTS.keys()))
    parser.add_argument("--all-tenants", action="store_true",
                        help="Run checks for every configured tenant")
    args = parser.parse_args()

    targets = list(TENANTS.keys()) if args.all_tenants else [args.tenant]
    for t in targets:
        monitor = HealthMonitor(tenant_id=t)
        monitor.run()