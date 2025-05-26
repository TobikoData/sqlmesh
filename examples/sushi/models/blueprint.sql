MODEL (
    name @name,
    kind FULL,
    description "Count of customers by status, done with a fancy unnecessary blueprint",
    grain status,
    blueprints (
        (
            name := sushi.count_customers_active,
            blueprint_status := 'active',
        ),
        (
            name := sushi.count_customers_inactive,
            blueprint_status := 'inactive',
        )
    )
);

SELECT 
   COUNT(customer_id) AS count_customers
FROM sushi.customers
WHERE status = @blueprint_status;