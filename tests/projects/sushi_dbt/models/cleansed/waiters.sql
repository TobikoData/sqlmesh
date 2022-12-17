{{
    config(
        materialized='incremental_by_time_range',
        incremental_strategy='delete+insert',
        time_column='ds',
        cluster_by=['ds'],
        unique_key=['ds']
    )
}}

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
