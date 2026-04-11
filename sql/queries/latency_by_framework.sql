-- latency_by_framework.sql
-- p50 / p95 / p99 job duration broken down by ML framework.

SELECT
    framework,
    job_type,
    COUNT(*)                                                                        AS jobs,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_seconds) / 3600.0, 3) AS p50_hours,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) / 3600.0, 3) AS p95_hours,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_seconds) / 3600.0, 3) AS p99_hours,
    ROUND(AVG(duration_seconds) / 3600.0, 3)                                       AS avg_hours,
    ROUND(AVG(cost_usd), 6)                                                         AS avg_cost_usd
FROM vertex_job_events
WHERE tenant_id  = :tenant_id
  AND started_at >= :since_date
  AND status IN ('SUCCEEDED', 'FAILED')
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY p95_hours DESC;