from __future__ import annotations

from sqlmesh.core.config.base import BaseConfig


class MigrationConfig(BaseConfig):
    """Configuration for the SQLMesh state migration.

    Args:
        promoted_snapshots_only: If True, only snapshots that are part of at least one environment will be migrated.
            Otherwise, all snapshots will be migrated.
    """

    promoted_snapshots_only: bool = True
