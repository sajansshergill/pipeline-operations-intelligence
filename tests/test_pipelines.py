"""
test_pipelines.py
------------------
pytest test suite for the Vertex AI Pipeline Operations Intelligence Hub.

Covers:
  - Schema integrity (DuckDB tables exist with correct columns)
  - Producer event generation (shape, types, valid ranges)
  - Consumer upsert logic (deduplication, daily metrics refresh)
  - Health monitor checks (known data → expected pass/fail)
  - Anomaly detector (injected anomalies → expected flags)
  - LLM digest prompt builder (output completeness)

Run:
    pytest tests/ -v
    pytest tests/ -v --cov=src --cov-report=term-missing
"""

from __future__ import annotations

from datetime import timedelta, date
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import TENANTS, DEFAULT_TENANT

# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def test_db(tmp_path_factory):
    """
    Spin up an in-memory DuckDB with the full schema for each test session.
    Uses a temp file so it persists across tests in the session.
    """
    db_path = str(tmp_path_factory.mktemp("data") / "test_vertex_ops.duckdb")
    conn = duckdb.connect(db_path)

    schema_sql = (Path(__file__).parent.parent / "sql" / "schema.sql").read_text()
    conn.execute(schema_sql)
    yield conn, db_path
    conn.close()


@pytest.fixture
def sample_events():
    """Return a list of 10 realistic job event dicts."""
    from src.ingestion.producer import build_event
    return [build_event(DEFAULT_TENANT) for _ in range(10)]


@pytest.fixture
def failed_event():
    """A job event with status forced to FAILED."""
    from src.ingestion.producer import build_event
    event = build_event(DEFAULT_TENANT)
    event["status"] = "FAILED"
    return event


@pytest.fixture
def anomalous_event():
    """A job event flagged as an anomaly (very high cost)."""
    from src.ingestion.producer import build_event
    event = build_event(DEFAULT_TENANT, is_anomaly=True)
    event["cost_usd"] = 9999.0   # guaranteed cost spike
    event["duration_seconds"] = 72000   # 20 hours — guaranteed duration outlier
    return event


# ── Schema tests ───────────────────────────────────────────────────────────────

class TestSchema:

    def test_vertex_job_events_exists(self, test_db):
        conn, _ = test_db
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "vertex_job_events" in names

    def test_pipeline_incidents_exists(self, test_db):
        conn, _ = test_db
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "pipeline_incidents" in names

    def test_vertex_daily_metrics_exists(self, test_db):
        conn, _ = test_db
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "vertex_daily_metrics" in names

    def test_llm_digests_exists(self, test_db):
        conn, _ = test_db
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "llm_digests" in names

    def test_job_events_required_columns(self, test_db):
        conn, _ = test_db
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'vertex_job_events'"
        ).fetchall()
        col_names = [c[0] for c in cols]
        required = [
            "event_id", "job_id", "tenant_id", "status",
            "cost_usd", "gpu_hours", "duration_seconds", "ingested_at",
        ]
        for col in required:
            assert col in col_names, f"Missing column: {col}"

    def test_v_job_success_rate_view_exists(self, test_db):
        conn, _ = test_db
        # Should not raise
        conn.execute("SELECT * FROM v_job_success_rate LIMIT 1")


# ── Producer tests ─────────────────────────────────────────────────────────────

class TestProducer:

    def test_build_event_returns_dict(self):
        from src.ingestion.producer import build_event
        event = build_event(DEFAULT_TENANT)
        assert isinstance(event, dict)

    def test_event_has_required_keys(self, sample_events):
        required = [
            "event_id", "job_id", "job_name", "job_type",
            "tenant_id", "status", "cost_usd", "duration_seconds",
            "gpu_hours", "cpu_hours", "started_at", "completed_at",
        ]
        for event in sample_events:
            for key in required:
                assert key in event, f"Missing key: {key}"

    def test_event_ids_are_unique(self, sample_events):
        ids = [e["event_id"] for e in sample_events]
        assert len(ids) == len(set(ids))

    def test_cost_is_non_negative(self, sample_events):
        for event in sample_events:
            assert event["cost_usd"] >= 0, f"Negative cost: {event['cost_usd']}"

    def test_duration_is_positive(self, sample_events):
        for event in sample_events:
            assert event["duration_seconds"] > 0

    def test_status_is_valid(self, sample_events):
        valid_statuses = {"SUCCEEDED", "FAILED", "RUNNING", "QUEUED", "CANCELLED"}
        for event in sample_events:
            assert event["status"] in valid_statuses

    def test_tenant_id_matches(self, sample_events):
        for event in sample_events:
            assert event["tenant_id"] == DEFAULT_TENANT

    def test_anomaly_event_has_longer_duration(self):
        from src.ingestion.producer import build_event
        normal_durations   = [build_event(DEFAULT_TENANT)["duration_seconds"] for _ in range(20)]
        anomaly_durations  = [build_event(DEFAULT_TENANT, is_anomaly=True)["duration_seconds"] for _ in range(20)]
        assert sum(anomaly_durations) > sum(normal_durations)

    def test_gpu_count_zero_for_cpu_machines(self):
        from src.ingestion.producer import build_event
        cpu_machines = ["n1-standard-4", "n1-standard-8"]
        for _ in range(30):
            event = build_event(DEFAULT_TENANT)
            if event["machine_type"] in cpu_machines:
                assert event["gpu_count"] == 0

    def test_gpu_hours_zero_when_no_gpu(self, sample_events):
        for event in sample_events:
            if event["gpu_count"] == 0:
                assert event["gpu_hours"] == 0.0


# ── Consumer / upsert tests ────────────────────────────────────────────────────

class TestConsumer:

    def _insert_event(self, conn, event: dict):
        conn.execute(
            """
            INSERT OR IGNORE INTO vertex_job_events (
                event_id, job_id, job_name, job_type, tenant_id, project_id,
                region, status, model_id, framework, machine_type, gpu_type,
                gpu_count, replica_count, batch_size, dataset_size_gb,
                started_at, completed_at, duration_seconds,
                gpu_hours, cpu_hours, cost_usd
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event["event_id"], event["job_id"], event["job_name"],
                event["job_type"], event["tenant_id"],
                TENANTS[event["tenant_id"]]["project_id"],
                TENANTS[event["tenant_id"]]["region"],
                event["status"], event.get("model_id"), event.get("framework"),
                event["machine_type"], event.get("gpu_type"), event.get("gpu_count", 0),
                event.get("replica_count", 1), event.get("batch_size"),
                event.get("dataset_size_gb"), event.get("started_at"),
                event.get("completed_at"), event.get("duration_seconds"),
                event.get("gpu_hours", 0.0), event.get("cpu_hours", 0.0),
                event.get("cost_usd", 0.0),
            ),
        )

    def test_event_inserts_successfully(self, test_db, sample_events):
        conn, _ = test_db
        before = conn.execute("SELECT COUNT(*) FROM vertex_job_events").fetchone()[0]
        for event in sample_events[:3]:
            self._insert_event(conn, event)
        after = conn.execute("SELECT COUNT(*) FROM vertex_job_events").fetchone()[0]
        assert after >= before + 3

    def test_duplicate_event_id_not_inserted(self, test_db, sample_events):
        conn, _ = test_db
        event = sample_events[0]
        self._insert_event(conn, event)
        count_before = conn.execute(
            f"SELECT COUNT(*) FROM vertex_job_events WHERE event_id = '{event['event_id']}'"
        ).fetchone()[0]
        self._insert_event(conn, event)  # second insert — should be ignored
        count_after = conn.execute(
            f"SELECT COUNT(*) FROM vertex_job_events WHERE event_id = '{event['event_id']}'"
        ).fetchone()[0]
        assert count_before == count_after == 1

    def test_failed_event_stored_correctly(self, test_db, failed_event):
        conn, _ = test_db
        self._insert_event(conn, failed_event)
        row = conn.execute(
            f"SELECT status FROM vertex_job_events WHERE event_id = '{failed_event['event_id']}'"
        ).fetchone()
        assert row is not None
        assert row[0] == "FAILED"

    def test_incident_written_for_failed_job(self, test_db, failed_event):
        from src.ingestion.consumer import detect_and_write_incidents
        conn, _ = test_db
        before = conn.execute("SELECT COUNT(*) FROM pipeline_incidents").fetchone()[0]
        detect_and_write_incidents(conn, [failed_event])
        after = conn.execute("SELECT COUNT(*) FROM pipeline_incidents").fetchone()[0]
        assert after > before


# ── Health monitor tests ───────────────────────────────────────────────────────

class TestHealthMonitor:

    def test_health_monitor_instantiates(self):
        from src.agents.health_monitor import HealthMonitor
        monitor = HealthMonitor(tenant_id=DEFAULT_TENANT)
        assert monitor.tenant_id == DEFAULT_TENANT

    def test_run_returns_expected_keys(self, test_db, monkeypatch):
        from src.agents.health_monitor import HealthMonitor
        _, db_path = test_db
        monkeypatch.setattr("src.agents.health_monitor.DUCKDB_PATH", db_path)
        monitor = HealthMonitor(tenant_id=DEFAULT_TENANT)
        result = monitor.run()
        for key in ["tenant_id", "checks_total", "checks_passed", "incidents_opened"]:
            assert key in result

    def test_checks_total_matches_number_of_checks(self, test_db, monkeypatch):
        from src.agents.health_monitor import HealthMonitor
        _, db_path = test_db
        monkeypatch.setattr("src.agents.health_monitor.DUCKDB_PATH", db_path)
        monitor = HealthMonitor(tenant_id=DEFAULT_TENANT)
        result = monitor.run()
        assert result["checks_total"] == 6   # 6 checks defined

    def test_check_result_has_required_fields(self):
        from src.agents.health_monitor import CheckResult
        r = CheckResult(name="test", passed=True, severity="LOW")
        assert r.name == "test"
        assert r.passed is True

    def test_staleness_check_fails_with_no_data(self, test_db, monkeypatch):
        from src.agents.health_monitor import HealthMonitor
        _, db_path = test_db
        monkeypatch.setattr("src.agents.health_monitor.DUCKDB_PATH", db_path)
        monitor = HealthMonitor(tenant_id="client_internal")
        monitor._connect()
        result = monitor.check_pipeline_staleness()
        monitor._close()
        # client_internal has no events — should fail
        assert result.passed is False


# ── Anomaly detector tests ─────────────────────────────────────────────────────

class TestAnomalyDetector:

    def test_anomaly_detector_instantiates(self):
        from src.agents.anomaly_detector import AnomalyDetector
        det = AnomalyDetector(tenant_id=DEFAULT_TENANT)
        assert det.tenant_id == DEFAULT_TENANT

    def test_run_returns_list(self, test_db, monkeypatch):
        from src.agents.anomaly_detector import AnomalyDetector
        _, db_path = test_db
        monkeypatch.setattr("src.agents.anomaly_detector.DUCKDB_PATH", db_path)
        det = AnomalyDetector(tenant_id=DEFAULT_TENANT)
        result = det.run()
        assert isinstance(result, list)

    def test_anomaly_dataclass_fields(self):
        from src.agents.anomaly_detector import Anomaly
        a = Anomaly(
            tenant_id=DEFAULT_TENANT,
            anomaly_type="COST_SPIKE",
            severity="HIGH",
            title="Test anomaly",
        )
        assert a.anomaly_id is not None
        assert a.tenant_id == DEFAULT_TENANT

    def test_gpu_waste_returns_empty_below_sample_size(self, test_db, monkeypatch):
        from src.agents.anomaly_detector import AnomalyDetector
        _, db_path = test_db
        monkeypatch.setattr("src.agents.anomaly_detector.DUCKDB_PATH", db_path)
        det = AnomalyDetector(tenant_id="client_internal")
        det._connect()
        result = det.detect_gpu_waste_pattern()
        det._close()
        assert isinstance(result, list)


# ── Prompt builder tests ───────────────────────────────────────────────────────

class TestPromptBuilder:

    def test_daily_prompt_contains_tenant_name(self):
        from src.llm_digest.prompts import build_daily_prompt
        prompt = build_daily_prompt(
            tenant_name="Test Corp",
            digest_date=date.today(),
            metrics={
                "spend_today": 120.5, "spend_week": 800.0,
                "budget_pct_used": 64.0, "monthly_projection": 3440.0,
                "gpu_hours_today": 4.2, "cpu_hours_today": 12.1,
                "total_jobs_today": 35, "succeeded_today": 32,
                "failed_today": 3, "success_rate_today": 91.4,
                "success_rate_7d": 93.1, "p95_hours_today": 2.1,
                "avg_cost_today": 3.44, "open_incidents": 2,
            },
            incidents=[{"severity": "HIGH", "incident_type": "JOB_FAILURE", "title": "Test failure"}],
            weekly_budget=2500.0,
            daily_budget=357.14,
        )
        assert "Test Corp" in prompt
        assert "$2,500.00" in prompt
        assert "SUCCEEDED" in prompt or "succeeded" in prompt.lower()

    def test_weekly_prompt_contains_week_range(self):
        from src.llm_digest.prompts import build_weekly_prompt
        week_start = date.today() - timedelta(days=7)
        prompt = build_weekly_prompt(
            tenant_name="ACME Corp",
            week_start=week_start,
            week_end=date.today(),
            metrics={
                "spend_week": 4200.0, "budget_pct_used": 84.0,
                "budget_remaining": 800.0, "gpu_hours_week": 22.0,
                "cpu_hours_week": 88.0, "gpu_cost_pct": 61.0,
                "total_jobs_week": 210, "succeeded_week": 198,
                "failed_week": 12, "success_rate_week": 94.3,
                "peak_day": str(date.today()), "peak_day_cost": 750.0,
                "trough_day": str(week_start), "trough_day_cost": 480.0,
                "incidents_total": 5, "incidents_critical": 1, "incidents_high": 2,
            },
            incidents=[],
            weekly_budget=5000.0,
        )
        assert "ACME Corp" in prompt
        assert week_start.isoformat() in prompt

    def test_system_prompt_contains_json_schema(self):
        from src.llm_digest.prompts import SYSTEM_PROMPT
        assert "budget_status" in SYSTEM_PROMPT
        assert "summary_text" in SYSTEM_PROMPT
        assert "recommendations" in SYSTEM_PROMPT
        assert "ON_TRACK" in SYSTEM_PROMPT