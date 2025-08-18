from __future__ import annotations

from sqlmesh.core.snapshot import Snapshot


def should_force_rebuild(old: Snapshot, new: Snapshot) -> bool:
    if new.is_view and new.is_indirect_non_breaking and not new.is_forward_only:
        # View models always need to be rebuilt to reflect updated upstream dependencies.
        return True
    return is_breaking_kind_change(old, new)


def is_breaking_kind_change(old: Snapshot, new: Snapshot) -> bool:
    if old.virtual_environment_mode != new.virtual_environment_mode:
        # If the virtual environment mode has changed, then we need to rebuild
        return True
    if old.model.kind.name == new.model.kind.name:
        # If the kind hasn't changed, then we don't need to rebuild
        return False
    if not old.is_incremental or not new.is_incremental:
        # If either is not incremental, then we need to rebuild
        return True
    if old.model.partitioned_by == new.model.partitioned_by:
        # If the partitioning hasn't changed, then we don't need to rebuild
        return False
    return True
