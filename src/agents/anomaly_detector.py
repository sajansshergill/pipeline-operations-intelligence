"""
anomaly_detector.py
--------------------
Detects statistical anomalies in Vertex AI pipeline metrics using
z-score and IQR-based outlier detection directly on DuckDB.

Anomaly types detected:
  - Cost spikes        — job cost > 3σ from 7-day rolling mean
  - Duration outliers  — job duration in top 1% of IQR fence
  - Failure bursts     — sudden spike in failure rate vs. rolling baseline
  - GPU waste events   — GPU jobs finishing under 5min (repeated pattern)

Can be run standalone or called by health_check_dag.py.

Run:
    python src/agents/anomaly_detector.py
    python src/agents/anomaly_detector.py --tenant client_acme --window 48
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

from config.settings import DUCKDB_PATH, TENANTS, DEFAULT_TENANT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANOMALY DETECTOR] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

Z_SCORE_THRESHOLD    = 3.0    # flag if value > 3 standard deviations from mean
IQR_FENCE_MULTIPLIER = 2.5    # IQR upper fence multiplier (less aggressive than 3.0)
MIN_SAMPLE_SIZE      = 10     # need at least N jobs before statistical checks are valid
FAILURE_BURST_DELTA  = 20.0   # pp rise in failure rate vs. baseline = burst


@dataclass
class Anomaly:
    anomaly_id:   str = field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id:    str = ""
    job_id:       str | None = None
    anomaly_type: str = ""
    severity:     str = "MEDIUM"
    title:        str = ""
    description:  str = ""
    metric_value: float | None = None
    threshold:    float | None = None
    action:       str = ""
    detected_at:  str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ── Anomaly Detector ───────────────────────────────────────────────────────────

class AnomalyDetector:

    def __init__(self, tenant_id: str = DEFAULT_TENANT, window_hours: int = 24):
        self.tenant_id    = tenant_id
        self.window_hours = window_hours
        self.conn: duckdb.DuckDBPyConnection | None = None

    def _connect(self):
        self.conn = duckdb.connect(DUCKDB_PATH)

    def _close(self):
        if self.conn:
            self.conn.close()

    def _fetch(self, sql: str) -> list[tuple]:
        return self.conn.execute(sql).fetchall()

    def _scalar(self, sql: str) -> Any:
        row = self.conn.execute(sql).fetchone()
        return row[0] if row else None

    # ── Detection methods ──────────────────────────────────────────────────────

    def detect_cost_spikes(self) -> list[Anomaly]:
        """
        Flag individual jobs where cost_usd is > Z_SCORE_THRESHOLD standard
        deviations above the 7-day rolling mean for that job_type + machine_type.
        """
        # Compute per-group stats over last 7 days
        rows = self._fetch(f"""
            WITH stats AS (
                SELECT
                    job_type,
                    machine_type,
                    AVG(cost_usd)    AS mean_cost,
                    STDDEV(cost_usd) AS std_cost,
                    COUNT(*)         AS n
                FROM vertex_job_events
                WHERE tenant_id = '{self.tenant_id}'
                  AND started_at > now() - INTERVAL '7 days'
                GROUP BY job_type, machine_type
                HAVING COUNT(*) >= {MIN_SAMPLE_SIZE}
            )
            SELECT
                e.event_id, e.job_id, e.job_name, e.job_type,
                e.machine_type, e.cost_usd,
                s.mean_cost, s.std_cost,
                (e.cost_usd - s.mean_cost) / NULLIF(s.std_cost, 0) AS z_score
            FROM vertex_job_events e
            JOIN stats s
              ON e.job_type      = s.job_type
             AND e.machine_type  = s.machine_type
            WHERE e.tenant_id  = '{self.tenant_id}'
              AND e.started_at  > now() - INTERVAL '{self.window_hours} hours'
              AND (e.cost_usd - s.mean_cost) / NULLIF(s.std_cost, 0) > {Z_SCORE_THRESHOLD}
            ORDER BY z_score DESC
            LIMIT 20
        """)

        anomalies = []
        for event_id, job_id, job_name, job_type, machine, cost, mean, std, z in rows:
            anomalies.append(Anomaly(
                tenant_id=self.tenant_id,
                job_id=job_id,
                anomaly_type="COST_SPIKE",
                severity="CRITICAL" if z > 5.0 else "HIGH",
                title=f"Cost spike: {job_name} (z={z:.1f}σ, ${cost:.2f})",
                description=(
                    f"Job {job_id} ({job_type} on {machine}) cost ${cost:.4f} — "
                    f"{z:.1f}σ above the 7-day mean of ${mean:.4f} (σ=${std:.4f})."
                ),
                metric_value=round(cost, 6),
                threshold=round(mean + Z_SCORE_THRESHOLD * std, 6),
                action=(
                    "Review job configuration. Check for unintended data volume growth, "
                    "replica count misconfiguration, or runaway training loops."
                ),
            ))

        log.info(f"[{self.tenant_id}] cost_spikes: {len(anomalies)} detected")
        return anomalies

    def detect_duration_outliers(self) -> list[Anomaly]:
        """
        Use IQR fence to flag jobs with extreme duration.
        Upper fence = Q3 + IQR_FENCE_MULTIPLIER * IQR
        """
        fence = self._scalar(f"""
            WITH quartiles AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY duration_seconds) AS q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration_seconds) AS q3
                FROM vertex_job_events
                WHERE tenant_id = '{self.tenant_id}'
                  AND started_at > now() - INTERVAL '7 days'
            )
            SELECT q3 + {IQR_FENCE_MULTIPLIER} * (q3 - q1) FROM quartiles
        """)

        if fence is None:
            return []

        rows = self._fetch(f"""
            SELECT event_id, job_id, job_name, job_type, duration_seconds
            FROM vertex_job_events
            WHERE tenant_id       = '{self.tenant_id}'
              AND started_at       > now() - INTERVAL '{self.window_hours} hours'
              AND duration_seconds > {fence}
            ORDER BY duration_seconds DESC
            LIMIT 10
        """)

        anomalies = []
        for event_id, job_id, job_name, job_type, duration in rows:
            anomalies.append(Anomaly(
                tenant_id=self.tenant_id,
                job_id=job_id,
                anomaly_type="DURATION_OUTLIER",
                severity="MEDIUM",
                title=f"Duration outlier: {job_name} ({duration/3600:.1f}h)",
                description=(
                    f"Job {job_id} ({job_type}) ran for {duration}s ({duration/3600:.2f}h), "
                    f"exceeding IQR upper fence of {fence:.0f}s ({fence/3600:.2f}h)."
                ),
                metric_value=float(duration),
                threshold=round(fence, 1),
                action=(
                    "Investigate data volume changes, model architecture changes, "
                    "or infrastructure degradation. Consider setting a job timeout."
                ),
            ))

        log.info(f"[{self.tenant_id}] duration_outliers: {len(anomalies)} detected")
        return anomalies

    def detect_failure_bursts(self) -> list[Anomaly]:
        """
        Compare failure rate in the last 1h vs. the 7-day rolling baseline.
        Flag if the delta exceeds FAILURE_BURST_DELTA percentage points.
        """
        baseline_rate = self._scalar(f"""
            SELECT
                100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
            FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND started_at > now() - INTERVAL '7 days'
              AND started_at < now() - INTERVAL '1 hour'
        """)

        recent_rate = self._scalar(f"""
            SELECT
                100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
            FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND started_at > now() - INTERVAL '1 hour'
        """)

        if baseline_rate is None or recent_rate is None:
            return []

        delta = recent_rate - baseline_rate
        if delta <= FAILURE_BURST_DELTA:
            log.info(f"[{self.tenant_id}] failure_burst: no burst (delta={delta:.1f}pp)")
            return []

        anomaly = Anomaly(
            tenant_id=self.tenant_id,
            anomaly_type="FAILURE_BURST",
            severity="CRITICAL" if delta > 40 else "HIGH",
            title=f"Failure burst: rate jumped +{delta:.1f}pp in last 1h",
            description=(
                f"Failure rate spiked from {baseline_rate:.1f}% (7-day baseline) "
                f"to {recent_rate:.1f}% in the last hour (+{delta:.1f} percentage points)."
            ),
            metric_value=round(recent_rate, 2),
            threshold=round(baseline_rate + FAILURE_BURST_DELTA, 2),
            action=(
                "Immediate investigation required. Check for infrastructure incidents, "
                "recent code deploys, or upstream data schema changes."
            ),
        )

        log.warning(f"[{self.tenant_id}] failure_burst: DETECTED — delta={delta:.1f}pp")
        return [anomaly]

    def detect_gpu_waste_pattern(self) -> list[Anomaly]:
        """
        If > 20% of GPU jobs in the window finished under 5 min,
        flag as a systematic GPU waste pattern (not just one-off incidents).
        """
        total_gpu = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id = '{self.tenant_id}'
              AND gpu_count > 0
              AND started_at > now() - INTERVAL '{self.window_hours} hours'
        """) or 0

        if total_gpu < MIN_SAMPLE_SIZE:
            return []

        wasteful = self._scalar(f"""
            SELECT COUNT(*) FROM vertex_job_events
            WHERE tenant_id       = '{self.tenant_id}'
              AND gpu_count        > 0
              AND duration_seconds < 300
              AND started_at       > now() - INTERVAL '{self.window_hours} hours'
        """) or 0

        waste_pct = 100.0 * wasteful / total_gpu
        if waste_pct < 20.0:
            return []

        avg_cost_wasted = self._scalar(f"""
            SELECT AVG(cost_usd) FROM vertex_job_events
            WHERE tenant_id       = '{self.tenant_id}'
              AND gpu_count        > 0
              AND duration_seconds < 300
              AND started_at       > now() - INTERVAL '{self.window_hours} hours'
        """) or 0.0

        anomaly = Anomaly(
            tenant_id=self.tenant_id,
            anomaly_type="GPU_WASTE_PATTERN",
            severity="MEDIUM",
            title=f"Systematic GPU waste: {waste_pct:.0f}% of GPU jobs < 5min",
            description=(
                f"{wasteful}/{total_gpu} GPU jobs finished in under 5 minutes "
                f"({waste_pct:.1f}% waste rate). "
                f"Average cost per wasted job: ${avg_cost_wasted:.4f}."
            ),
            metric_value=round(waste_pct, 1),
            threshold=20.0,
            action=(
                "Audit job configurations. Migrate sub-5-minute workloads to n1-standard "
                "CPU-only machines. GPU startup overhead exceeds compute time for short jobs."
            ),
        )

        log.warning(f"[{self.tenant_id}] gpu_waste_pattern: {waste_pct:.1f}% waste rate")
        return [anomaly]

    # ── Write anomalies as incidents ───────────────────────────────────────────

    def _write_anomalies(self, anomalies: list[Anomaly]):
        if not anomalies:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO pipeline_incidents
            (incident_id, tenant_id, job_id, incident_type, severity,
             title, description, metric_value, threshold_value, recommended_action)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    a.anomaly_id, a.tenant_id, a.job_id, a.anomaly_type, a.severity,
                    a.title, a.description, a.metric_value, a.threshold, a.action,
                )
                for a in anomalies
            ],
        )
        self.conn.commit()
        log.info(f"Wrote {len(anomalies)} anomaly incident(s) for tenant={self.tenant_id}")

    # ── Main run ───────────────────────────────────────────────────────────────

    def run(self) -> list[dict]:
        self._connect()

        all_anomalies: list[Anomaly] = []
        all_anomalies.extend(self.detect_cost_spikes())
        all_anomalies.extend(self.detect_duration_outliers())
        all_anomalies.extend(self.detect_failure_bursts())
        all_anomalies.extend(self.detect_gpu_waste_pattern())

        self._write_anomalies(all_anomalies)
        self._close()

        log.info(
            f"Anomaly detection complete — tenant={self.tenant_id} "
            f"total_anomalies={len(all_anomalies)}"
        )
        return [
            {
                "anomaly_id":   a.anomaly_id,
                "anomaly_type": a.anomaly_type,
                "severity":     a.severity,
                "title":        a.title,
            }
            for a in all_anomalies
        ]


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertex AI Anomaly Detector")
    parser.add_argument("--tenant",     default=DEFAULT_TENANT, choices=list(TENANTS.keys()))
    parser.add_argument("--window",     type=int, default=24, help="Look-back window in hours")
    parser.add_argument("--all-tenants", action="store_true")
    args = parser.parse_args()

    targets = list(TENANTS.keys()) if args.all_tenants else [args.tenant]
    for t in targets:
        detector = AnomalyDetector(tenant_id=t, window_hours=args.window)
        results = detector.run()
        for r in results:
            print(f"  [{r['severity']}] {r['anomaly_type']}: {r['title']}")