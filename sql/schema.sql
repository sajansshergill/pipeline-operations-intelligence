-- Vertex AI job telemetry — raw events from Kafka consumer
CREATE TABLE IF NOT EXISTS vertex_job_events (
    event_id          VARCHAR PRIMARY KEY,
    job_id            VARCHAR NOT NULL,
    job_name          VARCHAR NOT NULL,
    job_type          VARCHAR NOT NULL,   -- TRAINING | BATCH_PREDICTION | PIPELINE_RUN
    tenant_id         VARCHAR NOT NULL,
    project_id        VARCHAR NOT NULL,
    region            VARCHAR NOT NULL,
    status            VARCHAR NOT NULL,   -- QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
    model_id          VARCHAR,
    framework         VARCHAR,           -- tensorflow | pytorch | sklearn | xgboost
    machine_type      VARCHAR NOT NULL,
    gpu_type          VARCHAR,           -- NVIDIA_TESLA_T4 | NVIDIA_TESLA_A100 | None
    gpu_count         INTEGER DEFAULT 0,
    replica_count     INTEGER DEFAULT 1,
    batch_size        INTEGER,
    dataset_size_gb   DOUBLE,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    duration_seconds  INTEGER,
    gpu_hours         DOUBLE DEFAULT 0.0,
    cpu_hours         DOUBLE DEFAULT 0.0,
    cost_usd          DOUBLE DEFAULT 0.0,
    ingested_at       TIMESTAMP DEFAULT current_timestamp
);

-- Aggregated job metrics per day per tenant
CREATE TABLE IF NOT EXISTS vertex_daily_metrics (
    metric_date       DATE NOT NULL,
    tenant_id         VARCHAR NOT NULL,
    job_type          VARCHAR NOT NULL,
    total_jobs        INTEGER DEFAULT 0,
    succeeded_jobs    INTEGER DEFAULT 0,
    failed_jobs       INTEGER DEFAULT 0,
    total_gpu_hours   DOUBLE DEFAULT 0.0,
    total_cpu_hours   DOUBLE DEFAULT 0.0,
    total_cost_usd    DOUBLE DEFAULT 0.0,
    avg_duration_sec  DOUBLE DEFAULT 0.0,
    p95_duration_sec  DOUBLE DEFAULT 0.0,
    budget_threshold  DOUBLE DEFAULT 0.0,
    is_over_budget    BOOLEAN DEFAULT FALSE,
    computed_at       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (metric_date, tenant_id, job_type)
);

-- Incident records written by the health monitoring agent
CREATE TABLE IF NOT EXISTS pipeline_incidents (
    incident_id       VARCHAR PRIMARY KEY,
    tenant_id         VARCHAR NOT NULL,
    job_id            VARCHAR,
    incident_type     VARCHAR NOT NULL,  -- JOB_FAILURE | LATENCY_SPIKE | COST_OVERRUN | GPU_ANOMALY
    severity          VARCHAR NOT NULL,  -- LOW | MEDIUM | HIGH | CRITICAL
    title             VARCHAR NOT NULL,
    description       TEXT,
    metric_value      DOUBLE,
    threshold_value   DOUBLE,
    recommended_action TEXT,
    status            VARCHAR DEFAULT 'OPEN',  -- OPEN | ACKNOWLEDGED | RESOLVED
    detected_at       TIMESTAMP DEFAULT current_timestamp,
    resolved_at       TIMESTAMP
);

-- LLM digest history
CREATE TABLE IF NOT EXISTS llm_digests (
    digest_id         VARCHAR PRIMARY KEY,
    tenant_id         VARCHAR NOT NULL,
    digest_date       DATE NOT NULL,
    period            VARCHAR NOT NULL,  -- DAILY | WEEKLY
    summary_text      TEXT NOT NULL,
    incidents_count   INTEGER DEFAULT 0,
    total_cost_usd    DOUBLE DEFAULT 0.0,
    budget_status     VARCHAR,           -- ON_TRACK | AT_RISK | OVER_BUDGET
    generated_at      TIMESTAMP DEFAULT current_timestamp
);

-- Useful views
CREATE VIEW IF NOT EXISTS v_job_success_rate AS
SELECT
    tenant_id,
    DATE_TRUNC('day', started_at) AS day,
    job_type,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
    SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
    ROUND(SUM(cost_usd), 4) AS total_cost_usd
FROM vertex_job_events
GROUP BY 1, 2, 3;

CREATE VIEW IF NOT EXISTS v_gpu_spend_weekly AS
SELECT
    tenant_id,
    DATE_TRUNC('week', started_at) AS week_start,
    SUM(gpu_hours)  AS total_gpu_hours,
    SUM(cost_usd)   AS total_cost_usd,
    AVG(gpu_count)  AS avg_gpu_count
FROM vertex_job_events
WHERE gpu_count > 0
GROUP BY 1, 2
ORDER BY 1, 2;