-- weekly_gpu_spend.sql
-- GPU cost efficiency trend by week and machine type.

SELECT
    DATE_TRUNC('week', started_at)                                      AS week_start,
    tenant_id,
    machine_type,
    gpu_type,
    COUNT(*)                                                            AS gpu_jobs,
    ROUND(SUM(gpu_hours),  3)                                           AS total_gpu_hours,
    ROUND(SUM(cost_usd),   4)                                           AS total_cost_usd,
    ROUND(AVG(cost_usd),   6)                                           AS avg_cost_per_job,
    SUM(CASE WHEN duration_seconds < 300 THEN 1 ELSE 0 END)            AS sub_5min_jobs,
    ROUND(100.0 * SUM(CASE WHEN duration_seconds < 300 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)                                     AS waste_rate_pct
FROM vertex_job_events
WHERE tenant_id = :tenant_id
  AND gpu_count > 0
  AND started_at >= :since_date
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 6 DESC;