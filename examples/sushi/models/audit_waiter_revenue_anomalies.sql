MODEL (
    name sushi.audit_waiter_revenue_anomalies,
    kind AUDIT_ONLY (
        blocking FALSE,
        max_failing_rows 50
    ),
    depends_on [sushi.waiter_revenue_by_day],
    cron '@daily',
    owner 'analytics_team',
    tags ['validation', 'revenue', 'daily'],
    description 'Detects anomalies in daily waiter revenue that may indicate data quality issues'
);

-- Detect anomalies in waiter daily revenue
-- Only flag extreme outliers (>5 std dev) or negative revenue
WITH revenue_stats AS (
    SELECT 
        AVG(revenue) as avg_revenue,
        STDDEV(revenue) as stddev_revenue
    FROM sushi.waiter_revenue_by_day
    WHERE revenue > 0  -- Exclude zeros from stats calculation
),
anomalies AS (
    SELECT 
        w.waiter_id,
        w.event_date,
        w.revenue,
        r.avg_revenue,
        r.stddev_revenue,
        CASE 
            WHEN w.revenue < 0 THEN 'Negative revenue'
            WHEN w.revenue > r.avg_revenue + (5 * r.stddev_revenue) THEN 'Extremely high revenue (>5 std dev)'
        END as anomaly_type
    FROM sushi.waiter_revenue_by_day w
    CROSS JOIN revenue_stats r
    WHERE 
        w.revenue < 0 
        OR w.revenue > r.avg_revenue + (5 * r.stddev_revenue)  -- Only flag extreme outliers
)
SELECT 
    waiter_id,
    event_date,
    revenue,
    anomaly_type,
    CONCAT('Waiter ', waiter_id::TEXT, ' has ', anomaly_type, ' on ', event_date::TEXT) as issue_description
FROM anomalies
ORDER BY event_date DESC, waiter_id