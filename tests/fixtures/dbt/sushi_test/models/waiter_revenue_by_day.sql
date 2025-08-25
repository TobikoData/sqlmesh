{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        cluster_by=['ds'],
        time_column='ds',
        dialect="bigquery"
    )
}}

{{ log_value(5) }}
{{ log(adapter.dispatch('current_engine', 'customers')()) }}

{{ test_dependencies() }}

{% set var_name = "dynamic_" + "test_" + "var" %}
{% set results = run_query('select ' ~ dynamic_var_name_dependency(var_name) ~ ' as constant') %}

SELECT
  o.waiter_id::INT AS waiter_id, /* Waiter id */
  SUM(oi.quantity * i.price)::DOUBLE AS revenue, /* Revenue from orders taken by this waiter */
  o.ds::TEXT AS ds, /* Date */
  {% if execute %}
    {{ results.columns[0].values()[0] }}::INT AS constant /* Constant */
  {% endif %}
FROM {{ source('streaming', 'orders') }} AS o
LEFT JOIN {{ source('streaming', 'order_items') }} AS oi
  ON o.id = oi.order_id AND o.ds = oi.ds
LEFT JOIN {{ source('streaming', 'items') }} AS i
  ON oi.item_id = i.id AND oi.ds = i.ds
{% if is_incremental() %}
  WHERE
    o.ds > (select CAST(max(ds) AS DATE) from {{ this }})
{% endif %}
GROUP BY
  o.waiter_id,
  o.ds
