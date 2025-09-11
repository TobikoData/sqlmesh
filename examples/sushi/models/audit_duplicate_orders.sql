MODEL (
    name sushi.audit_duplicate_orders,
    kind AUDIT_ONLY (
        blocking FALSE,
        max_failing_rows 100
    ),
    depends_on [sushi.orders],
    cron '@hourly',
    owner 'data_engineering',
    tags ['validation', 'duplicates', 'data_quality'],
    description 'Detects potential duplicate orders based on customer, waiter, and timing'
);

-- Find potential duplicate orders
-- Orders from the same customer to the same waiter within 5 minutes might be duplicates
WITH potential_duplicates AS (
    SELECT 
        o1.id as order_id_1,
        o2.id as order_id_2,
        o1.customer_id,
        o1.waiter_id,
        o1.start_ts as order_1_time,
        o2.start_ts as order_2_time,
        ABS(o1.start_ts - o2.start_ts) as seconds_apart
    FROM sushi.orders o1
    INNER JOIN sushi.orders o2
        ON o1.customer_id = o2.customer_id
        AND o1.waiter_id = o2.waiter_id
        AND o1.id < o2.id  -- Avoid comparing order with itself and duplicating pairs
        AND o1.event_date = o2.event_date  -- Same day
    WHERE ABS(o1.start_ts - o2.start_ts) <= 300  -- Within 5 minutes (300 seconds)
)
SELECT 
    order_id_1,
    order_id_2,
    customer_id,
    waiter_id,
    seconds_apart,
    CONCAT('Orders ', order_id_1::TEXT, ' and ', order_id_2::TEXT, 
           ' from customer ', customer_id::TEXT, 
           ' are only ', seconds_apart::TEXT, ' seconds apart') as issue_description
FROM potential_duplicates
ORDER BY seconds_apart, order_id_1