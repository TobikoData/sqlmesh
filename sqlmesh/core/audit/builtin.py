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
    c -> c IS NULL
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
      c -> row_number() OVER (PARTITION BY c ORDER BY 1) AS @SQL('@{c}_rank')
    )
  FROM @this_model
)
WHERE @REDUCE(
  @EACH(
    @columns,
    c -> @SQL('@{c}_rank') > 1
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
WHERE @column NOT IN @is_in
""",
)


number_of_rows_audit = Audit(
    name="number_of_rows",
    query="""
SELECT 1
FROM @this_model
LIMIT @threshold + 1
HAVING COUNT(*) <= @threshold
    """,
)


forall_audit = Audit(
    name="forall",
    query="""
SELECT *
FROM @this_model
WHERE @REDUCE(
  @EACH(
    @criteria,
    c -> NOT (c)
  ),
  (l, r) -> l OR r
)
    """,
)
