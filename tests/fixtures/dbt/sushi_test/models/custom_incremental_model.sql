{{ config(
    materialized='custom_incremental',
    pre_hook=[
        "CREATE TABLE IF NOT EXISTS hook_table (id INTEGER, length_col TEXT, updated_at TIMESTAMP)"
    ],
    post_hook=[
        """
        INSERT INTO hook_table
        SELECT
            COALESCE(MAX(id), 0) + 1 AS id,
            '{{ model.raw_code | length }}' AS length_col,
            CURRENT_TIMESTAMP AS updated_at
        FROM hook_table
        """
    ]
) }}

SELECT
    current_timestamp as created_at,
    hash(current_timestamp) as id,