SELECT DISTINCT
  col_a
FROM {{ source("bronze", "b") }}
