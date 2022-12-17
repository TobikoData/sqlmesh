{{
    config(
        materialized='incremental_by_time_range',
        incremental_strategy='delete+insert',
        cluster_by=['ds'],
        unique_key=['ds'],
    )
}}

SELECT
  id::DOUBLE AS id, /* Primary key */
  name::TEXT AS name, /* Name of the sushi */
  price::DOUBLE AS price, /* Price of the sushi */
  ds::TEXT AS ds /* Date */
FROM {{ source('raw', 'items') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
