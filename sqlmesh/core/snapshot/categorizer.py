from __future__ import annotations

import typing as t

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
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

    if type(new_model) != type(old_model) or new.fingerprint.data_hash == old.fingerprint.data_hash:
        return default_category

    is_breaking_change = new_model.is_breaking_change(old_model)
    if is_breaking_change is None:
        return default_category
    return (
        SnapshotChangeCategory.BREAKING
        if is_breaking_change
        else SnapshotChangeCategory.NON_BREAKING
    )
