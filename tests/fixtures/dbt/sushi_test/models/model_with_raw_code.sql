{{
  config(
    pre_hook=['CREATE TABLE IF NOT EXISTS t AS SELECT \'Length is {{ model.raw_code|length }}\' AS length_col']
  )
}}

{{ check_model_is_table(model) }}
{{ check_model_is_table_alt(model) }}

SELECT
  1 AS c
