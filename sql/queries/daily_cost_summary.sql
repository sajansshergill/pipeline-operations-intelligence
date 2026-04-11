-- daily_cost_summary.sql
-- Returns per-tenant daily cost breakdown with budget variance.
-- Parameterise :tenant_id and :since_date before running.

SELECT
    CAST(started_at AS DATE)                                            AS run_date,
    tenant_id,
    job_type,
    COUNT(*)                                                            AS total_jobs,
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)              AS succeeded,
    SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END)              AS failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                                     AS success_rate_pct,
    ROUND(SUM(gpu_hours),  4)                                           AS total_gpu_hours,
    ROUND(SUM(cpu_hours),  4)                                           AS total_cpu_hours,
    ROUND(SUM(cost_usd),   4)                                           AS total_cost_usd,
    ROUND(AVG(cost_usd),   6)                                           AS avg_cost_per_job,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_seconds) / 3600.0, 3) AS p50_hours,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) / 3600.0, 3) AS p95_hours
FROM vertex_job_events
WHERE tenant_id  = :tenant_id
  AND started_at >= :since_date
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 3;