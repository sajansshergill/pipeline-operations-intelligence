"""
health_monitor.py
--------------------------------
Checks pipeline health across configurable thresholds and writes
structured incident records to DuckDB.

Checks performed:
    - Job success rate (last 1h, last 24h)
    - SLA breach detection (p95 latency vs. threshold)
    - Daily budget burn rate
    - GPU utilization floor
    - Pipeline staleness (no events in N minutes)

Can be run standalone or invoked by health_check_dag.py.

Run:
    python src/agent/health_monitor.py
    python src/agent/health_monitor.py --tenant client_acme
"""