{{
    config(
        materialized='ephemeral',
    )
}}

SELECT DISTINCT
   *
FROM {{ source('streaming', 'orders') }}
{{ incremental_by_time('ds', 'ds') }}
