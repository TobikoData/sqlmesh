MODEL (
  name sqlmesh_repo.a
);

JINJA_QUERY_BEGIN;
SELECT
  {{ round_dollars('col_a') }} as col_a, 
  col_b
FROM dbt_repo.e;
JINJA_END;