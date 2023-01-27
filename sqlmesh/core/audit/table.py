from __future__ import annotations

from sqlmesh.core.audit.definition import Audit

number_of_rows_audit = Audit(
    name="number_of_rows",
    query="""
SELECT 1
FROM @this_model
LIMIT @SQL('@threshold + 1')
HAVING COUNT(*) <= @threshold
    """,
)
