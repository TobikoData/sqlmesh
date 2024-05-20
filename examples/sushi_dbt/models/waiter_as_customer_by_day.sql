{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    cluster_by=['ds'],
    time_column='ds',
  )
}}

SELECT
  w.waiter_id as waiter_id,
  wn.name as waiter_name,
  w.ds as ds,
FROM {{ ref('waiters') }} AS w
JOIN {{ ref('customers') }} as c ON w.waiter_id = c.customer_id
JOIN {{ ref('waiter_names') }} as wn ON w.waiter_id = wn.id
