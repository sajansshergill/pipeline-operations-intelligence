"""
health_check_dag.py
--------------------
Runs the full health monitoring + anomaly detection cycle on a schedule:
  1. Run health monitor across all tenants
  2. Run anomaly detector — flags statistical outliers
  3. Escalate CRITICAL incidents
  4. Trigger LLM digest generation (daily only)

Schedule: every 30 minutes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.airflow_compat import (
    EmptyOperator,
    PythonOperator,
    ShortCircuitOperator,
    TriggerRule,
    dag_schedule_kwargs,
)
from config.settings import TENANTS, DEFAULT_TENANT, DUCKDB_PATH

DEFAULT_ARGS = {
    "owner":             "vertex-ops",
    "depends_on_past":   False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=1),
    "email_on_failure":  False,
    "execution_timeout": timedelta(minutes=8),
}


# ── Task functions ─────────────────────────────────────────────────────────────

def run_health_monitor(**ctx):
    """Invoke health_monitor.py for all tenants and push results to XCom."""
    from src.agents.health_monitor import HealthMonitor

    all_results = {}
    for tenant_id in TENANTS:
        monitor = HealthMonitor(tenant_id=tenant_id)
        results = monitor.run()
        all_results[tenant_id] = results
        print(f"[{tenant_id}] incidents_opened={results['incidents_opened']} "
              f"ok={results['checks_passed']}/{results['checks_total']}")

    ctx["ti"].xcom_push(key="health_results", value=all_results)
    return all_results


def run_anomaly_detector(**ctx):
    """Invoke anomaly_detector.py to surface statistical outliers."""
    from src.agents.anomaly_detector import AnomalyDetector

    all_anomalies = {}
    for tenant_id in TENANTS:
        detector = AnomalyDetector(tenant_id=tenant_id)
        anomalies = detector.run()
        all_anomalies[tenant_id] = anomalies
        print(f"[{tenant_id}] anomalies_detected={len(anomalies)}")

    ctx["ti"].xcom_push(key="anomalies", value=all_anomalies)
    return all_anomalies


def escalate_critical_incidents(**ctx):
    """
    Pull CRITICAL open incidents and log them prominently.
    In production: trigger PagerDuty / Slack alert via HTTP operator.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    criticals = conn.execute("""
        SELECT incident_id, tenant_id, incident_type, title, detected_at
        FROM pipeline_incidents
        WHERE severity = 'CRITICAL'
          AND status   = 'OPEN'
          AND detected_at > now() - INTERVAL '1 hour'
        ORDER BY detected_at DESC
    """).fetchall()
    conn.close()

    if not criticals:
        print("No CRITICAL incidents in the last hour.")
        return

    print(f"ESCALATING {len(criticals)} CRITICAL incident(s):")
    for row in criticals:
        incident_id, tenant_id, inc_type, title, detected_at = row
        print(f"  [{tenant_id}] {inc_type} — {title} (detected {detected_at})")
        # Production hook: requests.post(PAGERDUTY_URL, json={...})


def should_generate_digest(**ctx) -> bool:
    """
    ShortCircuit — only generate the LLM digest once per day.
    Returns True if this is the first run after 08:00 local time today.
    """
    now = datetime.now(timezone.utc)
    return now.hour == 8 and now.minute < 30


def generate_llm_digest(**ctx):
    """Trigger the Claude API digest generator for all tenants."""
    from src.llm_digest.digest_generator import DigestGenerator

    for tenant_id in TENANTS:
        generator = DigestGenerator(tenant_id=tenant_id)
        digest = generator.generate(period="DAILY")
        print(f"[{tenant_id}] Digest generated — {len(digest['summary_text'])} chars")


def summarise_health_run(**ctx):
    """Print a structured summary of the full health check run."""
    health_results = ctx["ti"].xcom_pull(key="health_results") or {}
    anomalies      = ctx["ti"].xcom_pull(key="anomalies")      or {}

    total_incidents = sum(
        v.get("incidents_opened", 0) for v in health_results.values()
    )
    total_anomalies = sum(len(v) for v in anomalies.values())

    print("=" * 60)
    print(f"Health check complete @ {datetime.now(timezone.utc).isoformat()}")
    print(f"  Tenants checked   : {len(TENANTS)}")
    print(f"  Incidents opened  : {total_incidents}")
    print(f"  Anomalies flagged : {total_anomalies}")
    print("=" * 60)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="vertex_health_check",
    description="Health monitoring and anomaly detection across all Vertex AI tenants",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vertex-ops", "health", "monitoring"],
    **dag_schedule_kwargs("*/30 * * * *"),
) as dag:

    start = EmptyOperator(task_id="start")

    health_check = PythonOperator(
        task_id="run_health_monitor",
        python_callable=run_health_monitor,
    )

    anomaly_check = PythonOperator(
        task_id="run_anomaly_detector",
        python_callable=run_anomaly_detector,
    )

    escalate = PythonOperator(
        task_id="escalate_critical_incidents",
        python_callable=escalate_critical_incidents,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    digest_gate = ShortCircuitOperator(
        task_id="should_generate_digest",
        python_callable=should_generate_digest,
    )

    digest = PythonOperator(
        task_id="generate_llm_digest",
        python_callable=generate_llm_digest,
    )

    summarise = PythonOperator(
        task_id="summarise_health_run",
        python_callable=summarise_health_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DAG flow ───────────────────────────────────────────────────────────────
    start >> [health_check, anomaly_check]
    [health_check, anomaly_check] >> escalate
    escalate >> digest_gate >> digest
    [escalate, digest] >> summarise >> end