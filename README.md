# Vertex AI Pipeline Operations Intelligence Hub

A self-healing data pipeline monitoring system that replicates the operational layer between raw GCP telemetry anc C-suite cost visibility —— build to match a Google Cloud Data Engineer would own on day one.

## The Problem
Enterprises buying Vertex AI can't tell if their pipelines are running efficiently, burning budget, or silently failing. Google's Professional Services teams sells the platform —— then needs engineers who can own the operational visibility layter between data lands in GCP and finance director understands what it costs.

This project replicates exactly that layer.

## What It Builds
Five production-grade components that a Data Engineer on the Google Cloud PS team would be hired to build, monitor, and hand off to enterprise clients.

**01 —— Vertex AI Telemetry Ingestion**
Kafka producer that emits realistic Vertex AI training job events —— job start/end, GPU hours consumed, resource utilization, model registry pushes. PySpark consumer transforms this stream into structured job-level metrics stroed in DuckDB (local) / BigQuery (cloud).

**02 —— Pipeline Health Monitoring Agent**
Airflow schedule agent that checks for: job failures, latency spikes, anomalous GPU utilizatio, and cost overruns vs. budget thresholds. Flags issues and write structures incident resources. Directly maps to the DE responsibility of building automated agents to monitor system health and streamline operational workflows.

**03 —— Dual-Stakeholder Dashboard**
Two views, one system:
- **Technical view** —— pipeline DAG status, job latency distribution, error rates, SQL breaches
- **Finance view** —— cost-per-training-run, GPU spend trend, budget turn vs. planned

Built in Streamlit + Plotly. Designed so a finance director and a platform engineer can sit in the same meeting and reference the same source of truth.

**04 —— LLM-Powered Natural Language Health Digest**
Claude API generates a daily-plain English pipeline summary written for a stakeholder who has never seen a Spark job. Struvtured, export-resourced, and LLM-readable by design —— built with Answer Engine Optimization (AEO) principles so outputs can be consumed by downstream AI assistants or voice interfaces..

Example output:
>> "This week, 3 Vertex AI training pipelines exceeded GPU budget by 28%. Root cause: batch size misconfiguration in the image classification workflow. Recommended action: reduce batch size from 512 -> 128. Estimated savings: $340/week".

**05 —— Multi-Tenane Scalability Layer**
Configuration-driven architecture where each "client" (enterprise customer on GCP) gets their own isolated pipeline namespace, dashboard view, and cost boundary. Designed to scale from a personal demo to a PS consultant's resusable deilvery asset across 5+ clients —— D2C to B2B2C.

## Technology Stack
<img width="1046" height="702" alt="image" src="https://github.com/user-attachments/assets/06a049cb-a79d-40a0-a964-c5b26426b5e9" />

## Repository Structure
<img width="1188" height="1502" alt="image" src="https://github.com/user-attachments/assets/6fab34a1-f493-4eb1-9c79-8b0832644e37" />

## Getting Started
**Prerequisites**
- Python 3.11+
- Docker + Docker Compose
- Anthropic API key (for LLM digest)

## Quickstart
bash# Clone the repo
git clone https://github.com/sajansshergill/vertex-ops-intelligence.git
cd vertex-ops-intelligence

### Start infrastructure (Kafka, Airflow, DuckDB)
docker-compose up -d

### Install dependencies
pip install -r requirements.txt

### Set environment variables
export ANTHROPIC_API_KEY=your_key_here
export TENANT_ID=client_demo

### Start Kafka telemetry producer
python src/ingestion/producer.py

### Launch dashboard
streamlit run src/dashboards/app.py
Run Tests
bashpytest tests/ -v
great_expectations checkpoint run vertex_pipeline_checkpoint

## Dashboards
**Technical View**
Monitors pipeline DAG status, job latecny distribution (p50/p95/p99), error rates, and SLA breach counts in real time.

**Finance View**
Tracks cost-per training-run, GPU spend trend by week, budget burn rate vs. planned, and projected monthly coverage.

Both views are avilable at http://localhost:8501 after launching the Streamlit app. Toggle between views using the sidebar selector.

## LLM Digest
The digest runs daily via an Airflow DAG and outputs a structures plain-English summary of pipeline health. Designed to be:
- **Human-readbable** —— written for finance and operations stakeholders
- **LLM-readable** —— structured for consumption by downstream AI assistants (AEO-compliant)
- **Actionable** —— every digest includes root cause + recommended action + estimated savings

Digests are stored in outputs/digests/ and optionally pushed to Slack or email via Airflow notification operators.

## Multi-Tenant Architecture
tenant_id: client_acme
display_name: ACME Corp
pipeline_namespace: acme_vertex_prod
budget_threshold_weekly_usd: 5000
alert_email: dataops@acme.com
dashboard_view: finance_primary

This isolates pipeline namespaces, cost boundaries, and dashboard defaults per client —— making the system deployable as a PS consulting deliverable, not just a personal demo.

## Why This Project
Google Cloud Professional Services is growing because enterprises are buying Vertex AI bit struggling with operational visibility and cost governance. This project demonstrates ownership of the exact layer a DE on this team would be hired to build —— from pipeline telemetry to executive readout.

The LLM digest layer signals beyond pipeline engineering: the ability yo translate technical metrics into business language, which is the core skills requirement for client-facing consulting work in this role.
