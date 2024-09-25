from __future__ import annotations

import typing as t

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotChangeCategory
from sqlmesh.utils.errors import SQLMeshError


def categorize_change(
    new: Snapshot,
    old: Snapshot,
    config: t.Optional[CategorizerConfig] = None,
    is_breaking_change: t.Optional[t.Callable[..., t.Optional[bool]]] = None,
    **kwargs: t.Any,
) -> t.Optional[SnapshotChangeCategory]:
    """Attempts to automatically categorize a change between two snapshots.

    Presently the implementation only returns the NON_BREAKING category iff
    a new projections have been added to one or more SELECT statement(s). In
    all other cases None is returned.

    Args:
        new: The new snapshot.
        old: The old snapshot.
        config: Configuration for the automatic categorizer of snapshot changes.
        is_breaking_change: Callable that compares two models (new, old) and determines
            whether there is a breaking change between them.
        kwargs: Additional arguments to pass to is_breaking_change.

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

    if type(new_model) != type(old_model):
        return default_category

    if new.fingerprint.data_hash == old.fingerprint.data_hash:
        if new.fingerprint.metadata_hash == old.fingerprint.metadata_hash:
            raise SQLMeshError(
                f"{new} is unmodified or indirectly modified and should not be categorized"
            )
        if new.fingerprint.parent_data_hash == old.fingerprint.parent_data_hash:
            return SnapshotChangeCategory.NON_BREAKING
        return None

    breaking_change = (
        is_breaking_change(new_model, old_model, **kwargs)
        if is_breaking_change
        else new_model.is_breaking_change(old_model)
    )
    if breaking_change is None:
        return default_category

    return (
        SnapshotChangeCategory.BREAKING if breaking_change else SnapshotChangeCategory.NON_BREAKING
    )
