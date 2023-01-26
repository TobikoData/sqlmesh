from __future__ import annotations

from sqlmesh.core.audit.definition import Audit

not_null_audit = Audit(
    name="not_null",
    query="""
SELECT *
FROM @this_model
WHERE @reduce(
  @each(
    @columns,
    c -> @sql('@c IS NULL')
  ),
  (l, r) -> @sql('@l OR @r')
)
    """,
)


unique_keys_audit = Audit(
    name="unique_keys",
    query="""
SELECT *
FROM (
  SELECT
    @each(
      @columns,
      c -> @sql('row_number() OVER (PARTITION BY @c ORDER BY 1) AS @{c}_rank')
    )
  FROM @this_model
)
WHERE @reduce(
  @each(
    @columns,
    c -> @sql('@{c}_rank > 1')
  ),
  (l, r) -> @sql('@l OR @r')
)
    """,
)


accepted_values_audit = Audit(
    name="accepted_values",
    query="""
SELECT *
FROM @this_model
WHERE @to_identifier(@column) NOT IN @values
""",
)
