from __future__ import annotations

from sqlmesh.core.audit.definition import Audit

number_of_rows_audit = Audit(
    name="number_of_rows",
    query="""
SELECT 1
FROM @this_model
HAVING COUNT(*) <= @threshold
    """,
)
