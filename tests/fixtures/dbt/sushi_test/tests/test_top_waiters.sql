-- Check that revenue is positive
SELECT waiter_id
FROM {{ ref('top_waiters') }}
WHERE revenue < 0
