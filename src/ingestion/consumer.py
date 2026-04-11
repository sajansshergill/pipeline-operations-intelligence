"""
Vertex AI Telemetry Consumer
------------------------------
Reads from Kafka topic vertex.job.telemetry using PySpark Structured
Streaming, applies schema validation + transforms, and writes to DuckDB
via foreachBatch sink.

Also computes rolling daily metrics and flags anomalies inline —
lightweight substitute for a full Dataflow pipeline in a local portfolio env.

Run:
    python src/ingestion/consumer.py
    python src/ingestion/consumer.py --tenant client_acme
"""

import argparse
import logging
import uuid
from datetime import date

import duckdb
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TELEMETRY,
    KAFKA_CONSUMER_GROUP,
    DUCKDB_PATH,
    SQL_DIR,
    LATENCY_SPIKE_MULTIPLIER,
    COST_OVERRUN_PCT,
    TENANTS,
    DEFAULT_TENANT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Kafka event schema ─────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("job_id",           StringType(),  True),
    StructField("job_name",         StringType(),  True),
    StructField("job_type",         StringType(),  True),
    StructField("tenant_id",        StringType(),  True),
    StructField("project_id",       StringType(),  True),
    StructField("region",           StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("model_id",         StringType(),  True),
    StructField("framework",        StringType(),  True),
    StructField("machine_type",     StringType(),  True),
    StructField("gpu_type",         StringType(),  True),
    StructField("gpu_count",        IntegerType(), True),
    StructField("replica_count",    IntegerType(), True),
    StructField("batch_size",       IntegerType(), True),
    StructField("dataset_size_gb",  DoubleType(),  True),
    StructField("started_at",       StringType(),  True),
    StructField("completed_at",     StringType(),  True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("gpu_hours",        DoubleType(),  True),
    StructField("cpu_hours",        DoubleType(),  True),
    StructField("cost_usd",         DoubleType(),  True),
    StructField("is_anomaly",       BooleanType(), True),
    StructField("emitted_at",       StringType(),  True),
])


# ── DuckDB helpers ─────────────────────────────────────────────────────────────

def init_db() -> duckdb.DuckDBPyConnection:
    """Bootstrap DuckDB schema on first run."""
    conn = duckdb.connect(DUCKDB_PATH)
    schema_sql = (SQL_DIR / "schema.sql").read_text()
    conn.execute(schema_sql)
    log.info(f"DuckDB initialized at {DUCKDB_PATH}")
    return conn


def upsert_events(conn: duckdb.DuckDBPyConnection, rows: list[dict]):
    """Insert job events, skip duplicates on event_id."""
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO vertex_job_events (
            event_id, job_id, job_name, job_type, tenant_id, project_id,
            region, status, model_id, framework, machine_type, gpu_type,
            gpu_count, replica_count, batch_size, dataset_size_gb,
            started_at, completed_at, duration_seconds,
            gpu_hours, cpu_hours, cost_usd
        ) VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        """,
        [
            (
                r["event_id"], r["job_id"], r["job_name"], r["job_type"],
                r["tenant_id"], r["project_id"], r["region"], r["status"],
                r.get("model_id"), r.get("framework"), r["machine_type"],
                r.get("gpu_type"), r.get("gpu_count", 0), r.get("replica_count", 1),
                r.get("batch_size"), r.get("dataset_size_gb"),
                r.get("started_at"), r.get("completed_at"), r.get("duration_seconds"),
                r.get("gpu_hours", 0.0), r.get("cpu_hours", 0.0), r.get("cost_usd", 0.0),
            )
            for r in rows
        ],
    )
    log.info(f"Upserted {len(rows)} events into vertex_job_events")


def refresh_daily_metrics(conn: duckdb.DuckDBPyConnection, tenant_id: str):
    """Recompute today's daily metrics row for this tenant."""
    today = date.today().isoformat()
    tenant_cfg = TENANTS.get(tenant_id, {})
    budget = tenant_cfg.get("budget_threshold_weekly", 5000.0)

    conn.execute(
        f"""
        DELETE FROM vertex_daily_metrics
        WHERE metric_date = '{today}' AND tenant_id = '{tenant_id}'
        """
    )
    conn.execute(
        f"""
        INSERT INTO vertex_daily_metrics
        SELECT
            CAST(started_at AS DATE)                                         AS metric_date,
            tenant_id,
            job_type,
            COUNT(*)                                                         AS total_jobs,
            SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)           AS succeeded_jobs,
            SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END)           AS failed_jobs,
            ROUND(SUM(gpu_hours), 4)                                         AS total_gpu_hours,
            ROUND(SUM(cpu_hours), 4)                                         AS total_cpu_hours,
            ROUND(SUM(cost_usd), 4)                                          AS total_cost_usd,
            ROUND(AVG(duration_seconds), 1)                                  AS avg_duration_sec,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY duration_seconds), 1)                            AS p95_duration_sec,
            {budget / 7.0}                                                   AS budget_threshold,
            SUM(cost_usd) > {budget / 7.0} * (1 + {COST_OVERRUN_PCT} / 100) AS is_over_budget
        FROM vertex_job_events
        WHERE CAST(started_at AS DATE) = '{today}'
          AND tenant_id = '{tenant_id}'
        GROUP BY 1, 2, 3
        """
    )
    log.info(f"Daily metrics refreshed for tenant={tenant_id} date={today}")


def detect_and_write_incidents(conn: duckdb.DuckDBPyConnection, rows: list[dict]):
    """
    Inline anomaly detection on each micro-batch.
    Writes incident records for: FAILED jobs, latency spikes, cost overruns.
    """
    incidents = []

    for r in rows:
        tenant_cfg = TENANTS.get(r["tenant_id"], {})
        budget_daily = tenant_cfg.get("budget_threshold_weekly", 5000.0) / 7.0

        # 1. Job failure
        if r.get("status") == "FAILED":
            incidents.append({
                "incident_id":        str(uuid.uuid4()),
                "tenant_id":          r["tenant_id"],
                "job_id":             r["job_id"],
                "incident_type":      "JOB_FAILURE",
                "severity":           "HIGH",
                "title":              f"Job failed: {r['job_name']}",
                "description":        (
                    f"Job {r['job_id']} ({r['job_type']}) failed after "
                    f"{r.get('duration_seconds', 0)}s on {r['machine_type']}."
                ),
                "metric_value":       float(r.get("duration_seconds", 0)),
                "threshold_value":    None,
                "recommended_action": (
                    "Check Vertex AI logs for OOM errors or dataset schema mismatches. "
                    "Verify GPU memory is sufficient for the configured batch size."
                ),
            })

        # 2. Latency spike — compare against a rough heuristic baseline (3600s = 1hr)
        duration = r.get("duration_seconds", 0)
        baseline = 3600
        if duration > baseline * LATENCY_SPIKE_MULTIPLIER:
            incidents.append({
                "incident_id":        str(uuid.uuid4()),
                "tenant_id":          r["tenant_id"],
                "job_id":             r["job_id"],
                "incident_type":      "LATENCY_SPIKE",
                "severity":           "MEDIUM",
                "title":              f"Latency spike: {r['job_name']}",
                "description":        (
                    f"Job ran for {duration}s ({duration/3600:.1f}h), "
                    f"{duration/baseline:.1f}x above baseline of {baseline}s."
                ),
                "metric_value":       float(duration),
                "threshold_value":    float(baseline * LATENCY_SPIKE_MULTIPLIER),
                "recommended_action": (
                    "Investigate data pipeline bottlenecks. "
                    "Consider increasing replica count or reducing dataset partition size."
                ),
            })

        # 3. Single-job cost overrun (job cost > 10% of daily budget)
        cost = r.get("cost_usd", 0.0)
        if cost > budget_daily * 0.10:
            incidents.append({
                "incident_id":        str(uuid.uuid4()),
                "tenant_id":          r["tenant_id"],
                "job_id":             r["job_id"],
                "incident_type":      "COST_OVERRUN",
                "severity":           "HIGH" if cost > budget_daily * 0.25 else "MEDIUM",
                "title":              f"High-cost job: {r['job_name']} (${cost:.2f})",
                "description":        (
                    f"Single job consumed ${cost:.4f} — "
                    f"{100 * cost / budget_daily:.1f}% of daily budget (${budget_daily:.2f})."
                ),
                "metric_value":       cost,
                "threshold_value":    budget_daily * 0.10,
                "recommended_action": (
                    "Review GPU configuration. "
                    "Consider switching from A100 to T4 for non-production training runs."
                ),
            })

    if not incidents:
        return

    conn.executemany(
        """
        INSERT OR IGNORE INTO pipeline_incidents (
            incident_id, tenant_id, job_id, incident_type, severity,
            title, description, metric_value, threshold_value, recommended_action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                i["incident_id"], i["tenant_id"], i.get("job_id"),
                i["incident_type"], i["severity"], i["title"],
                i.get("description"), i.get("metric_value"),
                i.get("threshold_value"), i.get("recommended_action"),
            )
            for i in incidents
        ],
    )
    log.info(f"Wrote {len(incidents)} incident(s) to pipeline_incidents")


# ── PySpark streaming ──────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VertexOpsConsumer")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/vertex_ops_checkpoint")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink — called once per micro-batch."""
    if batch_df.isEmpty():
        return

    rows = [r.asDict() for r in batch_df.collect()]
    log.info(f"Processing batch {batch_id} — {len(rows)} records")

    conn = duckdb.connect(DUCKDB_PATH)
    try:
        upsert_events(conn, rows)
        detect_and_write_incidents(conn, rows)

        # Refresh daily metrics for each tenant seen in this batch
        for tenant_id in {r["tenant_id"] for r in rows}:
            refresh_daily_metrics(conn, tenant_id)

        conn.commit()
    finally:
        conn.close()


def run(tenant_id: str):
    init_db()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Starting Spark consumer | topic={KAFKA_TOPIC_TELEMETRY}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_TELEMETRY)
        .option("group.id", KAFKA_CONSUMER_GROUP)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("event_id").isNotNull())
        # Filter to this tenant only (multi-tenant: run one consumer per tenant,
        # or remove this filter to consume all tenants in a single process)
        .filter(F.col("tenant_id") == tenant_id)
        .withColumn("started_at",   F.to_timestamp("started_at"))
        .withColumn("completed_at", F.to_timestamp("completed_at"))
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Consumer streaming — press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertex AI Telemetry Consumer")
    parser.add_argument("--tenant", default=DEFAULT_TENANT, choices=list(TENANTS.keys()))
    args = parser.parse_args()
    run(tenant_id=args.tenant)