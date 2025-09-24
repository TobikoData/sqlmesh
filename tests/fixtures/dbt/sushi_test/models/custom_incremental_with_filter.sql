{{ config(
    materialized='custom_incremental',
    time_column='created_at',
    interval='2 day'
) }}

SELECT
    CAST('{{ run_started_at }}' AS TIMESTAMP) as created_at,
    hash('{{ run_started_at }}') as id,