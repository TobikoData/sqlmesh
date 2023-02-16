from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.diff import ChangeDistiller, Insert, Keep

from sqlmesh.core.snapshot.definition import Snapshot, SnapshotChangeCategory


def categorize_change(new: Snapshot, old: Snapshot) -> t.Optional[SnapshotChangeCategory]:
    """Attempts to automatically categorize a change between two snapshots.

    Presently the implementation only returns the NON_BREAKING category iff
    a new projections have been added to one or more SELECT statement(s). In
    all other cases None is returned.

    Args:
        new: The new snapshot.
        old: The old snapshot.

    Returns:
        The change category or None if the category can't be determined automatically.

    """
    old_model = old.model
    new_model = new.model

    if (
        not new_model.is_sql
        or not old_model.is_sql
        or new.fingerprint.data_hash == old.fingerprint.data_hash
    ):
        return None

    edits = ChangeDistiller(t=0.5).diff(old_model.render_query(), new_model.render_query())
    inserted_expressions = {e.expression for e in edits if isinstance(e, Insert)}

    for edit in edits:
        if isinstance(edit, Insert):
            expr = edit.expression
            if _is_udtf(expr) or (
                not _is_projection(expr) and expr.parent not in inserted_expressions
            ):
                return None
        elif not isinstance(edit, Keep):
            return None

    return SnapshotChangeCategory.NON_BREAKING


def _is_projection(expr: exp.Expression) -> bool:
    parent = expr.parent
    return isinstance(parent, exp.Select) and expr in parent.expressions


def _is_udtf(expr: exp.Expression) -> bool:
    return isinstance(expr, (exp.Explode, exp.Posexplode, exp.Unnest)) or (
        isinstance(expr, exp.Anonymous)
        and expr.this.upper() in ("EXPLODE_OUTER", "POSEXPLODE_OUTER", "UNNEST")
    )
