from __future__ import annotations

from sqlmesh.core.audit.definition import Audit

not_null_audit = Audit(
    name="not_null",
    query="""
SELECT *
FROM @this_model
WHERE @REDUCE(
  @EACH(
    @columns,
    c -> @SQL('@c IS NULL')
  ),
  (l, r) -> l OR r
)
    """,
)


unique_values_audit = Audit(
    name="unique_values",
    query="""
SELECT *
FROM (
  SELECT
    @EACH(
      @columns,
      c -> @SQL('row_number() OVER (PARTITION BY @c ORDER BY 1) AS @{c}_rank')
    )
  FROM @this_model
)
WHERE @reduce(
  @EACH(
    @columns,
    c -> @SQL('@{c}_rank > 1')
  ),
  (l, r) -> l OR r
)
    """,
)


accepted_values_audit = Audit(
    name="accepted_values",
    query="""
SELECT *
FROM @this_model
WHERE @SQL('@column') NOT IN @values
""",
)
