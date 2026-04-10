"""
Vertex AI Telemetry Producer
-----------------------------
Simulates Vertex AI training job lifecycle events and publishes them
to Kafka topic: vertex.job.telemetry

Each event represents a job reaching a terminal state (SUCCEEDED / FAILED)
or a mid-run heartbeat (RUNNING) with resource utilization metrics.

Run:
    python src/ingestion/producer.py
    python src/ingestion/producer.py --tenant client_acme --batches 50
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TELEMETRY,
    PRODUCER_EVENTS_PER_BATCH,
    PRODUCER_INTERVAL_SECONDS,
    PRODUCER_FAILURE_RATE,
    PRODUCER_ANOMALY_RATE,
    TENANTS,
    DEFAULT_TENANT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Simulation config ──────────────────────────────────────────────────────────

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

FRAMEWORKS = ["tensorflow", "pytorch", "sklearn", "xgboost", "jax"]

JOB_TYPES = ["TRAINING", "BATCH_PREDICTION", "PIPELINE_RUN"]

JOB_NAMES = [
    "image-classification-resnet",
    "text-embedding-bert",
    "fraud-detection-xgb",
    "churn-propensity-lgbm",
    "recommendation-collab-filter",
    "demand-forecast-lstm",
    "sentiment-analysis-distilbert",
    "anomaly-detection-autoencoder",
    "click-through-rate-dnn",
    "customer-segmentation-kmeans",
]

# GPU cost per hour by type
GPU_COST_PER_HOUR = {
    "NVIDIA_TESLA_A100": 2.93,
    "NVIDIA_TESLA_T4":   0.35,
    None: 0.0,
}

CPU_COST_PER_HOUR = {
    "n1-standard-4":  0.19,
    "n1-standard-8":  0.38,
    "n1-highmem-16":  0.64,
    "a2-highgpu-1g":  0.0,   # bundled into GPU cost
    "a2-highgpu-4g":  0.0,
}


def build_event(tenant_id: str, is_anomaly: bool = False) -> dict:
    """Generate a single realistic Vertex AI job telemetry event."""
    machine  = random.choice(MACHINE_TYPES)
    gpu_type, gpu_count = GPU_TYPES[machine]
    framework = random.choice(FRAMEWORKS)
    job_type  = random.choice(JOB_TYPES)
    job_name  = random.choice(JOB_NAMES)

    # Duration: normal jobs 10-120 min; anomalous jobs 3-5x longer
    base_duration = random.randint(600, 7200)
    if is_anomaly:
        base_duration = int(base_duration * random.uniform(3.0, 5.0))

    started_at   = datetime.utcnow() - timedelta(seconds=base_duration)
    completed_at = datetime.utcnow()

    # Status
    status = "FAILED" if random.random() < PRODUCER_FAILURE_RATE else "SUCCEEDED"

    # Resource consumption
    duration_hrs = base_duration / 3600
    gpu_hours    = round(gpu_count * duration_hrs, 4) if gpu_count else 0.0
    cpu_hours    = round(duration_hrs, 4)

    gpu_cost  = gpu_hours * GPU_COST_PER_HOUR.get(gpu_type, 0.0)
    cpu_cost  = cpu_hours * CPU_COST_PER_HOUR.get(machine, 0.15)
    total_cost = round(gpu_cost + cpu_cost, 6)

    tenant_cfg = TENANTS[tenant_id]

    return {
        "event_id":        str(uuid.uuid4()),
        "job_id":          f"job-{uuid.uuid4().hex[:12]}",
        "job_name":        job_name,
        "job_type":        job_type,
        "tenant_id":       tenant_id,
        "project_id":      tenant_cfg["project_id"],
        "region":          tenant_cfg["region"],
        "status":          status,
        "model_id":        f"model-{uuid.uuid4().hex[:8]}",
        "framework":       framework,
        "machine_type":    machine,
        "gpu_type":        gpu_type,
        "gpu_count":       gpu_count,
        "replica_count":   random.choice([1, 2, 4]),
        "batch_size":      random.choice([32, 64, 128, 256, 512]),
        "dataset_size_gb": round(random.uniform(0.5, 200.0), 2),
        "started_at":      started_at.isoformat(),
        "completed_at":    completed_at.isoformat(),
        "duration_seconds": base_duration,
        "gpu_hours":       gpu_hours,
        "cpu_hours":       cpu_hours,
        "cost_usd":        total_cost,
        "is_anomaly":      is_anomaly,
        "emitted_at":      datetime.utcnow().isoformat(),
    }


def create_producer(retries: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=16384,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready (attempt {attempt}/{retries}), retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries. Is Docker running?")


def run(tenant_id: str, batches: int = -1):
    """
    Continuously emit telemetry events.
    batches=-1 runs indefinitely; set a number for finite runs (useful in tests).
    """
    producer = create_producer()
    batch_count = 0

    log.info(f"Starting producer | tenant={tenant_id} | topic={KAFKA_TOPIC_TELEMETRY}")
    log.info(f"Emitting {PRODUCER_EVENTS_PER_BATCH} events every {PRODUCER_INTERVAL_SECONDS}s")

    try:
        while batches == -1 or batch_count < batches:
            batch_count += 1
            sent = 0

            for _ in range(PRODUCER_EVENTS_PER_BATCH):
                is_anomaly = random.random() < PRODUCER_ANOMALY_RATE
                event = build_event(tenant_id, is_anomaly=is_anomaly)

                producer.send(
                    topic=KAFKA_TOPIC_TELEMETRY,
                    key=event["tenant_id"],
                    value=event,
                )
                sent += 1

            producer.flush()
            log.info(f"Batch {batch_count:04d} — sent {sent} events to {KAFKA_TOPIC_TELEMETRY}")

            if batches == -1 or batch_count < batches:
                time.sleep(PRODUCER_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
    finally:
        producer.close()
        log.info(f"Producer closed. Total batches sent: {batch_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertex AI Telemetry Producer")
    parser.add_argument("--tenant",  default=DEFAULT_TENANT, choices=list(TENANTS.keys()))
    parser.add_argument("--batches", type=int, default=-1,
                        help="Number of batches to emit (-1 = infinite)")
    args = parser.parse_args()
    run(tenant_id=args.tenant, batches=args.batches)