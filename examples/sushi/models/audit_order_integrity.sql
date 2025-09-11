MODEL (
    name sushi.audit_order_integrity,
    kind AUDIT_ONLY (
        blocking FALSE,  -- Set to non-blocking for example/demo purposes
        max_failing_rows 20
    ),
    depends_on [sushi.orders, sushi.customers],
    cron '@daily',
    owner 'data_quality_team',
    tags ['validation', 'referential_integrity', 'critical'],
    description 'Validates referential integrity between orders and customers tables'
);

-- Check for orders with non-existent customer IDs
-- This should return no rows if all orders have valid customers
SELECT 
    o.id as order_id,
    o.customer_id,
    o.event_date,
    'Missing customer record' as issue_type,
    CONCAT('Order ', o.id::TEXT, ' references non-existent customer ', o.customer_id::TEXT) as issue_description
FROM sushi.orders o
LEFT JOIN sushi.customers c 
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL