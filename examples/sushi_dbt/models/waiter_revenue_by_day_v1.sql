{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    cluster_by=['ds'],
    time_column='ds',
  )
}}

SELECT
  o.waiter_id::INT AS waiter_id, /* Waiter id */
  SUM(oi.quantity * i.price)::DOUBLE AS revenue, /* Revenue from orders taken by this waiter */
  o.ds::TEXT AS ds /* Date */
FROM {{ ref('orders') }} AS o
LEFT JOIN {{ ref('order_items') }} AS oi
  ON o.id = oi.order_id AND o.ds = oi.ds
LEFT JOIN {{ ref('items') }} AS i
  ON oi.item_id = i.id AND oi.ds = i.ds
{% if is_incremental() %}
  WHERE
    o.ds > (select max(ds) from {{ this }})
{% endif %}
{% if sqlmesh_incremental is defined %}
  WHERE
      o.ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
{% endif %}
GROUP BY
  o.waiter_id,
  o.ds
