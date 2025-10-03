{{
  config(
    materialized = 'table',

    pre_hook = [
      {
        "sql": "
          CREATE TABLE IF NOT EXISTS hook_outside_pre_table AS
          SELECT
            1 AS id,
            'outside' AS location,
            1 AS execution_order,
            NOW() AS created_at
        ",
        "transaction": false
      },

      before_begin("
        CREATE TABLE IF NOT EXISTS shared_hook_table (
          id INT,
          hook_name VARCHAR,
          execution_order INT,
          created_at TIMESTAMPTZ
        )
      "),

      {
        "sql": "{{ insert_into_shared_hook_table('inside_pre') }}",
        "transaction": true
      }
    ],

    post_hook = [
      {
        "sql": "{{ insert_into_shared_hook_table('inside_post') }}",
        "transaction": true
      },

      {
        "sql": "
          CREATE TABLE IF NOT EXISTS hook_outside_post_table AS
          SELECT
            5 AS id,
            'outside' AS location,
            5 AS execution_order,
            NOW() AS created_at
        ",
        "transaction": false
      },

      after_commit("{{ insert_into_shared_hook_table('after_commit') }}")
    ]
  )
}}

SELECT 1 AS id, 'test' AS name;
