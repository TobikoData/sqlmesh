from __future__ import annotations

import sys
import typing as t

from sqlmesh.core.model import SeedModel
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotIdLike, SnapshotInfoLike
from sqlmesh.core.snapshot.definition import Interval
from sqlmesh.core.state_sync.base import DelegatingStateSync, StateSync
from sqlmesh.utils.date import TimeLike, now_timestamp

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class CachingStateSync(DelegatingStateSync):
    """In memory cache for snapshots that implements the state sync api.

    Args:
        state_sync: The base state sync.
        ttl: The number of seconds a snapshot should be cached.
    """

    def __init__(self, state_sync: StateSync, ttl: int = 30):
        super().__init__(state_sync)
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
                    and isinstance(snapshot.node, SeedModel)
                    and not snapshot.node.is_hydrated
                ):
                    missing.add(snapshot_id)
                else:
                    existing[snapshot_id] = snapshot

        if missing:
            existing.update(self.state_sync.get_snapshots(missing, hydrate_seeds))

        for snapshot_id, snapshot in existing.items():
            cached = self._from_cache(snapshot_id, now)
            if cached and (not isinstance(cached.node, SeedModel) or cached.node.is_hydrated):
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
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        execution_time: t.Optional[TimeLike] = None,
        remove_shared_versions: bool = False,
    ) -> None:
        for s, _ in snapshot_intervals:
            self.snapshot_cache.pop(s.snapshot_id, None)
        self.state_sync.remove_interval(snapshot_intervals, execution_time, remove_shared_versions)
