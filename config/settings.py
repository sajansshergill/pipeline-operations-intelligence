import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR    = Path(__file__).resolve().parent.parent
SQL_DIR     = ROOT_DIR / "sql"
DATA_DIR    = ROOT_DIR / "data"
OUTPUT_DIR  = ROOT_DIR / "outputs" / "digests"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TELEMETRY   = "vertex.job.telemetry"
KAFKA_TOPIC_INCIDENTS   = "vertex.pipeline.incidents"
KAFKA_CONSUMER_GROUP    = "vertex-ops-consumer"

# ── DuckDB ─────────────────────────────────────────────────────────────────────
DUCKDB_PATH = str(DATA_DIR / "vertex_ops.duckdb")

# ── Anthropic ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-20250514"

# ── Tenants ────────────────────────────────────────────────────────────────────
TENANTS = {
    "client_acme": {
        "display_name":           "ACME Corp",
        "budget_threshold_weekly": 5_000.0,
        "project_id":             "acme-vertex-prod",
        "region":                 "us-central1",
    },
    "client_demo": {
        "display_name":           "Demo Client",
        "budget_threshold_weekly": 2_500.0,
        "project_id":             "demo-vertex-dev",
        "region":                 "us-east1",
    },
    "client_internal": {
        "display_name":           "Google Internal",
        "budget_threshold_weekly": 15_000.0,
        "project_id":             "gcp-ps-internal",
        "region":                 "us-west1",
    },
}

DEFAULT_TENANT = "client_demo"

# ── Thresholds ─────────────────────────────────────────────────────────────────
LATENCY_SPIKE_MULTIPLIER   = 2.5   # flag job if duration > 2.5x rolling avg
GPU_UTILIZATION_FLOOR_PCT  = 30.0  # flag if GPU util drops below 30%
COST_OVERRUN_PCT           = 20.0  # flag if weekly spend > budget + 20%

# ── Producer simulation ────────────────────────────────────────────────────────
PRODUCER_EVENTS_PER_BATCH  = 10
PRODUCER_INTERVAL_SECONDS  = 5
PRODUCER_FAILURE_RATE      = 0.08  # 8% of jobs simulate failure
PRODUCER_ANOMALY_RATE      = 0.05  # 5% of jobs simulate GPU anomaly