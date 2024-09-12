SELECT DISTINCT
  {{ round_dollars('col_a') }} as rounded_col_a
FROM {{ source("sqlmesh_repo", "b") }}
