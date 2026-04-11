"""
vertex_telemetry_dag.py
------------------------
Orchestrates the full Vertex AI telemetry ingestion cycle:
  1. Validate Kafka topic is reachable
  2. Trigger PySpark consumer for a bounded batch window
  3. Run Great Expectations checkpoint on landed data
  4. Refresh daily metrics aggregation
  5. Notify on failure

Schedule: every 15 minutes during business hours, hourly overnight.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.airflow_compat import (
    BranchPythonOperator,
    EmptyOperator,
    PythonOperator,
    TriggerRule,
    dag_schedule_kwargs,
)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TELEMETRY,
    DUCKDB_PATH,
    SQL_DIR,
    TENANTS,
    DEFAULT_TENANT,
)

# ── DAG defaults ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "vertex-ops",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=10),
}

# ── Task functions ─────────────────────────────────────────────────────────────

def check_kafka_connectivity(**ctx) -> str:
    """Verify Kafka broker is reachable before triggering consumer."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
            connections_max_idle_ms=6000,
        )
        topics = consumer.topics()
        consumer.close()
        print(f"Kafka reachable. Topics: {topics}")
        return "run_consumer_batch"
    except NoBrokersAvailable:
        print(f"Kafka unreachable at {KAFKA_BOOTSTRAP_SERVERS}")
        return "kafka_unavailable"


def run_consumer_batch(**ctx):
    """
    Run the PySpark consumer for a fixed batch window (non-streaming mode).
    In production this would be replaced by a DataprocSubmitJobOperator
    pointing at a GCS-staged PySpark jar.
    """
    import os
    import subprocess

    tenant = ctx["dag_run"].conf.get("tenant_id", DEFAULT_TENANT)
    repo_root = Path(__file__).resolve().parent.parent
    consumer_path = repo_root / "src" / "ingestion" / "consumer.py"
    result = subprocess.run(
        [
            sys.executable,
            str(consumer_path),
            "--tenant",
            tenant,
        ],
        capture_output=True,
        text=True,
        timeout=480,
        cwd=str(repo_root),
        env={**os.environ, "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS},
    )
    if result.returncode != 0:
        raise RuntimeError(f"Consumer failed:\n{result.stderr}")
    print(result.stdout)


def validate_data_quality(**ctx):
    """
    Run lightweight Great Expectations-style checks directly on DuckDB.
    In production: ge_checkpoint.run() against a configured datasource.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    failures = []

    checks = [
        # No null event_ids
        ("NULL event_ids",
         "SELECT COUNT(*) FROM vertex_job_events WHERE event_id IS NULL",
         lambda n: n == 0),

        # No negative costs
        ("Negative cost_usd",
         "SELECT COUNT(*) FROM vertex_job_events WHERE cost_usd < 0",
         lambda n: n == 0),

        # No future started_at timestamps
        ("Future started_at",
         "SELECT COUNT(*) FROM vertex_job_events WHERE started_at > now()",
         lambda n: n == 0),

        # Events landed in last 30 min (freshness check)
        ("Stale data — no recent events",
         "SELECT COUNT(*) FROM vertex_job_events WHERE ingested_at > now() - INTERVAL '30 minutes'",
         lambda n: n > 0),

        # Job duration sanity (no negative durations)
        ("Negative duration_seconds",
         "SELECT COUNT(*) FROM vertex_job_events WHERE duration_seconds < 0",
         lambda n: n == 0),
    ]

    for name, query, assertion in checks:
        result = conn.execute(query).fetchone()[0]
        passed = assertion(result)
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {name} — value={result}")
        if not passed:
            failures.append(name)

    conn.close()

    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")

    print("All data quality checks passed.")


def refresh_all_tenant_metrics(**ctx):
    """Recompute daily metrics for every configured tenant."""
    import duckdb
    from datetime import date

    conn = duckdb.connect(DUCKDB_PATH)
    today = date.today().isoformat()

    for tenant_id, tenant_cfg in TENANTS.items():
        budget_daily = tenant_cfg["budget_threshold_weekly"] / 7.0

        conn.execute(f"""
            DELETE FROM vertex_daily_metrics
            WHERE metric_date = '{today}' AND tenant_id = '{tenant_id}'
        """)

        conn.execute(f"""
            INSERT INTO vertex_daily_metrics
            SELECT
                CAST(started_at AS DATE)                                            AS metric_date,
                tenant_id,
                job_type,
                COUNT(*)                                                            AS total_jobs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)              AS succeeded_jobs,
                SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END)              AS failed_jobs,
                ROUND(SUM(gpu_hours), 4)                                            AS total_gpu_hours,
                ROUND(SUM(cpu_hours), 4)                                            AS total_cpu_hours,
                ROUND(SUM(cost_usd), 4)                                             AS total_cost_usd,
                ROUND(AVG(duration_seconds), 1)                                     AS avg_duration_sec,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                      (ORDER BY duration_seconds), 1)                               AS p95_duration_sec,
                {budget_daily}                                                      AS budget_threshold,
                SUM(cost_usd) > {budget_daily} * 1.2                               AS is_over_budget
            FROM vertex_job_events
            WHERE CAST(started_at AS DATE) = '{today}'
              AND tenant_id = '{tenant_id}'
            GROUP BY 1, 2, 3
        """)
        print(f"Metrics refreshed: tenant={tenant_id} date={today}")

    conn.commit()
    conn.close()


def report_pipeline_status(**ctx):
    """Push a summary of this run to the incidents table."""
    import duckdb
    import uuid
    from datetime import datetime

    conn = duckdb.connect(DUCKDB_PATH)

    total_events = conn.execute(
        "SELECT COUNT(*) FROM vertex_job_events WHERE ingested_at > now() - INTERVAL '20 minutes'"
    ).fetchone()[0]

    failed_jobs = conn.execute(
        "SELECT COUNT(*) FROM vertex_job_events "
        "WHERE status = 'FAILED' AND ingested_at > now() - INTERVAL '20 minutes'"
    ).fetchone()[0]

    print(f"Pipeline run complete — events ingested: {total_events}, failures: {failed_jobs}")

    if failed_jobs > 0:
        conn.execute(
            """
            INSERT OR IGNORE INTO pipeline_incidents
            (incident_id, tenant_id, incident_type, severity, title, description, metric_value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                "system",
                "JOB_FAILURE",
                "HIGH" if failed_jobs > 3 else "MEDIUM",
                f"{failed_jobs} job failure(s) detected in last ingestion window",
                f"Airflow telemetry DAG detected {failed_jobs} FAILED jobs out of {total_events} total.",
                float(failed_jobs),
            ),
        )
        conn.commit()

    conn.close()


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="vertex_telemetry_pipeline",
    description="Ingest, validate, and aggregate Vertex AI job telemetry",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vertex-ops", "ingestion", "telemetry"],
    params={"tenant_id": DEFAULT_TENANT},
    **dag_schedule_kwargs("*/15 6-22 * * 1-5"),
) as dag:

    start = EmptyOperator(task_id="start")

    check_kafka = BranchPythonOperator(
        task_id="check_kafka_connectivity",
        python_callable=check_kafka_connectivity,
    )

    kafka_unavailable = EmptyOperator(task_id="kafka_unavailable")

    consume = PythonOperator(
        task_id="run_consumer_batch",
        python_callable=run_consumer_batch,
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    refresh_metrics = PythonOperator(
        task_id="refresh_all_tenant_metrics",
        python_callable=refresh_all_tenant_metrics,
    )

    report = PythonOperator(
        task_id="report_pipeline_status",
        python_callable=report_pipeline_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DAG flow ───────────────────────────────────────────────────────────────
    start >> check_kafka >> [consume, kafka_unavailable]
    consume >> validate >> refresh_metrics >> report >> end
    kafka_unavailable >> end