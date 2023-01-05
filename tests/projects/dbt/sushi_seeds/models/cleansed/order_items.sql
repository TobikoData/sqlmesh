{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        cluster_by=['ds'],
        time_column='ds',
    )
}}

SELECT
  id::INT AS id, /* Primary key */
  order_id::INT AS order_id, /* Order id */
  item_id::INT AS item_id, /* Item id */
  quantity::INT AS quantity, /* Quantity of items ordered */
  ds::TEXT AS ds /* Date of order */
FROM {{ ref('raw_order_items') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
