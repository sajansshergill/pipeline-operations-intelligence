#!/usr/bin/env python3
"""
Insert synthetic vertex_job_events into DuckDB for dashboard / agent demos.

Does not require Kafka. Writes to config.settings DUCKDB_PATH (usually
./data/vertex_ops.duckdb). With docker-compose, ./data is bind-mounted at
/app/data, so seeding on the host updates the same file the dashboard uses.

Examples:
    python scripts/seed_demo_data.py
    python scripts/seed_demo_data.py --tenant client_demo --jobs 400 --days 14

Inside Docker (after ``docker compose build`` so /app/scripts exists):
    docker compose exec dashboard python scripts/seed_demo_data.py --tenant client_demo
"""

from __future__ import annotations

import argparse
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from config.settings import DUCKDB_PATH, SQL_DIR, TENANTS  # noqa: E402

# Same pools as producer (trimmed) for believable telemetry
MACHINE_TYPES = [
    "n1-standard-4",
    "n1-standard-8",
    "n1-highmem-16",
    "a2-highgpu-1g",
    "a2-highgpu-4g",
]
GPU_TYPES = {
    "a2-highgpu-1g": ("NVIDIA_TESLA_A100", 1),
    "a2-highgpu-4g": ("NVIDIA_TESLA_A100", 4),
    "n1-standard-4": (None, 0),
    "n1-standard-8": (None, 0),
    "n1-highmem-16": ("NVIDIA_TESLA_T4", 1),
}
FRAMEWORKS = ["tensorflow", "pytorch", "sklearn", "xgboost"]
JOB_TYPES = ["TRAINING", "BATCH_PREDICTION", "PIPELINE_RUN"]
JOB_NAMES = [
    "image-classification-resnet",
    "fraud-detection-xgb",
    "demand-forecast-lstm",
    "click-through-rate-dnn",
]
GPU_COST = {"NVIDIA_TESLA_A100": 2.93, "NVIDIA_TESLA_T4": 0.35, None: 0.0}
CPU_COST = {
    "n1-standard-4": 0.19,
    "n1-standard-8": 0.38,
    "n1-highmem-16": 0.64,
    "a2-highgpu-1g": 0.0,
    "a2-highgpu-4g": 0.0,
}
FAILURE_RATE = 0.12


def _random_row(tenant_id: str, cfg: dict, end: datetime, spread_days: int) -> tuple:
    machine = random.choice(MACHINE_TYPES)
    gpu_type, gpu_count = GPU_TYPES[machine]
    framework = random.choice(FRAMEWORKS)
    job_type = random.choice(JOB_TYPES)
    job_name = random.choice(JOB_NAMES)

    base_duration = random.randint(300, 9000)
    if random.random() < 0.06:
        base_duration = int(base_duration * random.uniform(2.5, 4.0))

    offset = random.uniform(0, spread_days * 86400)
    completed_at = end - timedelta(seconds=offset)
    started_at = completed_at - timedelta(seconds=base_duration)

    status = "FAILED" if random.random() < FAILURE_RATE else "SUCCEEDED"

    duration_hrs = base_duration / 3600.0
    gpu_hours = round(gpu_count * duration_hrs, 4) if gpu_count else 0.0
    cpu_hours = round(duration_hrs, 4)
    gpu_cost = gpu_hours * GPU_COST.get(gpu_type, 0.0)
    cpu_cost = cpu_hours * CPU_COST.get(machine, 0.15)
    total_cost = round(gpu_cost + cpu_cost, 6)

    return (
        str(uuid.uuid4()),
        f"job-{uuid.uuid4().hex[:12]}",
        job_name,
        job_type,
        tenant_id,
        cfg["project_id"],
        cfg["region"],
        status,
        f"model-{uuid.uuid4().hex[:8]}",
        framework,
        machine,
        gpu_type,
        gpu_count,
        random.choice([1, 2, 4]),
        random.choice([32, 64, 128, 256]),
        round(random.uniform(1.0, 80.0), 2),
        started_at,
        completed_at,
        base_duration,
        gpu_hours,
        cpu_hours,
        total_cost,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Seed DuckDB with demo job telemetry")
    p.add_argument("--tenant", default=None, help=f"Single tenant id (default: all {list(TENANTS)})")
    p.add_argument("--jobs", type=int, default=350, help="Approximate rows per tenant")
    p.add_argument("--days", type=int, default=14, help="Spread completed_at over this many days")
    args = p.parse_args()

    targets = [args.tenant] if args.tenant else list(TENANTS.keys())
    for t in targets:
        if t not in TENANTS:
            raise SystemExit(f"Unknown tenant {t}. Choices: {list(TENANTS)}")

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute((SQL_DIR / "schema.sql").read_text())

    end = datetime.now(timezone.utc).replace(tzinfo=None)
    insert_sql = """
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
    """

    total = 0
    for tenant_id in targets:
        cfg = TENANTS[tenant_id]
        batch = [_random_row(tenant_id, cfg, end, args.days) for _ in range(args.jobs)]
        conn.executemany(insert_sql, batch)
        total += len(batch)
        print(f"Inserted up to {args.jobs} rows for {tenant_id}")

    # A few OPEN incidents so the Technical view "Open incidents" section is populated.
    demo_incidents = [
        (
            "seed-demo-stale-001",
            "client_demo",
            None,
            "PIPELINE_STALENESS",
            "HIGH",
            "Demo incident: review pipeline freshness",
            "Bundled with seed_demo_data.py for dashboard demos.",
            None,
            None,
            "Run the Kafka producer/consumer stack or re-seed job events.",
            "OPEN",
        ),
        (
            "seed-demo-budget-002",
            "client_demo",
            None,
            "COST_OVERRUN",
            "MEDIUM",
            "Demo incident: weekly spend approaching cap",
            "Synthetic row for finance/technical views.",
            2450.0,
            2500.0,
            "Validate budget thresholds in config/settings.py.",
            "OPEN",
        ),
        (
            "seed-demo-acme-003",
            "client_acme",
            None,
            "JOB_FAILURE",
            "LOW",
            "Demo incident: isolated training failure",
            "Synthetic row for multi-tenant demos.",
            1.0,
            None,
            "Check GPU memory for large batch sizes.",
            "OPEN",
        ),
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO pipeline_incidents (
            incident_id, tenant_id, job_id, incident_type, severity,
            title, description, metric_value, threshold_value,
            recommended_action, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        demo_incidents,
    )

    conn.commit()
    conn.close()
    print(f"Done — {total} insert attempts into {DUCKDB_PATH}")
    print("Refresh the Streamlit app (sidebar → Refresh data).")


if __name__ == "__main__":
    main()
