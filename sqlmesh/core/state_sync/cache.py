from __future__ import annotations

import sys
import typing as t

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import SeedModel
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotTableInfo,
)
from sqlmesh.core.state_sync.base import StateSync, Versions
from sqlmesh.utils.date import TimeLike, now_timestamp

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class CachingStateSync(StateSync):
    """In memory cache for snapshots that implements the state sync api.

    Args:
        state_sync: The base state sync.
        ttl: The number of seconds a snapshot should be cached.
    """

    def __init__(self, state_sync: StateSync, ttl: int = 30):
        self.state_sync = state_sync
        # The cache can contain a snapshot or False or None.
        # False means that the snapshot does not exist in the state sync but has been requested before
        # None means that the snapshot has not been requested.
        self.snapshot_cache: t.Dict[
            SnapshotId, t.Tuple[t.Optional[Snapshot | Literal[False]], int]
        ] = {}

        self.ttl = ttl

    def _from_cache(
        self, snapshot_id: SnapshotId, now: int
    ) -> t.Optional[Snapshot | Literal[False]]:
        snapshot: t.Optional[Snapshot | Literal[False]] = None
        snapshot_expiration = self.snapshot_cache.get(snapshot_id)

        if snapshot_expiration and snapshot_expiration[1] >= now:
            snapshot = snapshot_expiration[0]

        return snapshot

    def get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]],
        hydrate_seeds: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        if not snapshot_ids:
            return self.state_sync.get_snapshots(snapshot_ids, hydrate_seeds)

        existing = {}
        missing = set()
        now = now_timestamp()
        expire_at = now + self.ttl * 1000

        for s in snapshot_ids:
            snapshot_id = s.snapshot_id
            snapshot = self._from_cache(snapshot_id, now)

            if snapshot is None:
                self.snapshot_cache[snapshot_id] = (False, expire_at)
                missing.add(snapshot_id)
            elif snapshot:
                if (
                    hydrate_seeds
                    and isinstance(snapshot.model, SeedModel)
                    and not snapshot.model.is_hydrated
                ):
                    missing.add(snapshot_id)
                else:
                    existing[snapshot_id] = snapshot

        if missing:
            existing.update(self.state_sync.get_snapshots(missing, hydrate_seeds))

        for snapshot_id, snapshot in existing.items():
            cached = self._from_cache(snapshot_id, now)
            if cached and (not isinstance(cached.model, SeedModel) or cached.model.is_hydrated):
                continue
            self.snapshot_cache[snapshot_id] = (snapshot, expire_at)

        return existing

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        existing = set()
        missing = set()
        now = now_timestamp()

        for s in snapshot_ids:
            snapshot_id = s.snapshot_id
            snapshot = self._from_cache(snapshot_id, now)
            if snapshot:
                existing.add(snapshot_id)
            elif snapshot is None:
                missing.add(snapshot_id)

        if missing:
            existing.update(self.state_sync.snapshots_exist(missing))

        return existing

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        return self.state_sync.nodes_exist(names, exclude_external)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self.state_sync.get_environment(environment)

    def get_environments(self) -> t.List[Environment]:
        return self.state_sync.get_environments()

    def recycle(self) -> None:
        self.state_sync.recycle()

    def close(self) -> None:
        self.state_sync.close()

    def get_versions(self, validate: bool = True) -> Versions:
        return self.state_sync.get_versions(validate)

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        return self.state_sync._get_versions(lock_for_update)

    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        snapshots = tuple(snapshots)

        for snapshot in snapshots:
            self.snapshot_cache.pop(snapshot.snapshot_id, None)

        self.state_sync.push_snapshots(snapshots)

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        snapshot_ids = tuple(snapshot_ids)

        for s in snapshot_ids:
            self.snapshot_cache.pop(s.snapshot_id, None)
        self.state_sync.delete_snapshots(snapshot_ids)

    def delete_expired_snapshots(self) -> t.List[Snapshot]:
        self.snapshot_cache.clear()
        return self.state_sync.delete_expired_snapshots()

    def invalidate_environment(self, name: str) -> None:
        self.state_sync.invalidate_environment(name)

    def add_interval(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        is_dev: bool = False,
    ) -> None:
        self.snapshot_cache.pop(snapshot.snapshot_id, None)
        self.state_sync.add_interval(snapshot, start, end, is_dev)

    def remove_interval(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        start: TimeLike,
        end: TimeLike,
        all_snapshots: t.Optional[t.Iterable[Snapshot]] = None,
    ) -> None:
        snapshots = tuple(snapshots)
        for s in snapshots:
            self.snapshot_cache.pop(s.snapshot_id, None)
        self.state_sync.remove_interval(snapshots, start, end, all_snapshots)

    def promote(
        self, environment: Environment, no_gaps: bool = False
    ) -> t.Tuple[t.List[SnapshotTableInfo], t.List[SnapshotTableInfo]]:
        return self.state_sync.promote(environment, no_gaps)

    def finalize(self, environment: Environment) -> None:
        self.state_sync.finalize(environment)

    def delete_expired_environments(self) -> t.List[Environment]:
        return self.state_sync.delete_expired_environments()

    def unpause_snapshots(
        self, snapshots: t.Iterable[SnapshotInfoLike], unpaused_dt: TimeLike
    ) -> None:
        self.state_sync.unpause_snapshots(snapshots, unpaused_dt)

    def compact_intervals(self) -> None:
        self.state_sync.compact_intervals()

    def migrate(self, skip_backup: bool = False) -> None:
        self.state_sync.migrate(skip_backup)

    def rollback(self) -> None:
        self.state_sync.rollback()
