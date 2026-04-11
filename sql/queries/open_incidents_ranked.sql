-- open_incidents_ranked.sql
-- All open incidents ordered by severity then recency.

SELECT
    incident_id,
    tenant_id,
    job_id,
    incident_type,
    severity,
    title,
    description,
    metric_value,
    threshold_value,
    recommended_action,
    detected_at,
    DATEDIFF('minute', detected_at, now())                              AS age_minutes
FROM pipeline_incidents
WHERE status = 'OPEN'
  AND tenant_id = :tenant_id
ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 0
        WHEN 'HIGH'     THEN 1
        WHEN 'MEDIUM'   THEN 2
        ELSE 3
    END,
    detected_at DESC;