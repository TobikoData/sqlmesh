from __future__ import annotations

import typing as t

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from sqlglot import exp
from sqlglot.diff import ChangeDistiller, Insert, Keep

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
from sqlmesh.core.model import SeedModel
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotChangeCategory


def categorize_change(
    new: Snapshot, old: Snapshot, config: t.Optional[CategorizerConfig] = None
) -> t.Optional[SnapshotChangeCategory]:
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

    config = config or CategorizerConfig()
    mode = config.dict().get(new_model.source_type, AutoCategorizationMode.OFF)
    if mode == AutoCategorizationMode.OFF:
        return None

    default_category = (
        SnapshotChangeCategory.BREAKING if mode == AutoCategorizationMode.FULL else None
    )

    if (
        new_model.source_type != old_model.source_type
        or new_model.is_python
        or new.fingerprint.data_hash == old.fingerprint.data_hash
    ):
        return default_category

    if new_model.is_sql:
        return _categorize_sql_change(
            new_model.render_query(), old_model.render_query(), default_category
        )

    if isinstance(new_model, SeedModel) and isinstance(old_model, SeedModel):
        return _categorize_seed_change(
            new_model.seed.read(), old_model.seed.read(), default_category
        )

    return default_category


def _categorize_seed_change(
    new_dfs: t.Generator[pd.DataFrame, None, None],
    old_dfs: t.Generator[pd.DataFrame, None, None],
    default: t.Optional[SnapshotChangeCategory],
) -> t.Optional[SnapshotChangeCategory]:
    new_df = pd.concat([df for df in new_dfs])
    old_df = pd.concat([df for df in old_dfs])

    new_columns = set(new_df.columns)
    old_columns = set(old_df.columns)

    if not new_columns.issuperset(old_columns):
        return default

    for col in old_columns:
        if new_df[col].dtype != old_df[col].dtype or new_df[col].shape != old_df[col].shape:
            return default
        elif is_numeric_dtype(new_df[col]):
            if not all(np.isclose(new_df[col], old_df[col])):
                return default
        else:
            if not new_df[col].equals(old_df[col]):
                return default

    return SnapshotChangeCategory.NON_BREAKING


def _categorize_sql_change(
    new_query: exp.Expression,
    old_query: exp.Expression,
    default: t.Optional[SnapshotChangeCategory],
) -> t.Optional[SnapshotChangeCategory]:
    edits = ChangeDistiller(t=0.5).diff(old_query, new_query)
    inserted_expressions = {e.expression for e in edits if isinstance(e, Insert)}

    for edit in edits:
        if isinstance(edit, Insert):
            expr = edit.expression
            if _is_udtf(expr) or (
                not _is_projection(expr) and expr.parent not in inserted_expressions
            ):
                return default
        elif not isinstance(edit, Keep):
            return default

    return SnapshotChangeCategory.NON_BREAKING


def _is_projection(expr: exp.Expression) -> bool:
    parent = expr.parent
    return isinstance(parent, exp.Select) and expr in parent.expressions


def _is_udtf(expr: exp.Expression) -> bool:
    return isinstance(expr, (exp.Explode, exp.Posexplode, exp.Unnest)) or (
        isinstance(expr, exp.Anonymous)
        and expr.this.upper() in ("EXPLODE_OUTER", "POSEXPLODE_OUTER", "UNNEST")
    )
