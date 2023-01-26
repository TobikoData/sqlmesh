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
