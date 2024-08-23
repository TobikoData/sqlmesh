from __future__ import annotations

import typing as t

from pathlib import Path
from sqlmesh.core.model.cache import OptimizedQueryCache
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotId
from sqlmesh.utils.cache import FileCache


class SnapshotCache:
    def __init__(self, path: Path):
        self._snapshot_cache: FileCache[Snapshot] = FileCache(path, prefix="snapshot")
        self._optimized_query_cache = OptimizedQueryCache(path)

    def get_or_load(
        self,
        snapshot_ids: t.Set[SnapshotId],
        loader: t.Callable[[t.Set[SnapshotId]], t.Collection[Snapshot]],
    ) -> t.Tuple[t.Dict[SnapshotId, Snapshot], t.Set[SnapshotId]]:
        """Fetches the target snapshots from cache or loads them using the provided loader on cache miss.

        Args:
            snapshot_ids: Target snapshot IDs to fetch.
            loader: The loader to load snapshot records that are missing in the cache.

        Returns:
            A tuple where the first value represents the fetched snapshots, and the second value is a set of
            snapshot IDs for which records were retrieved from the cache.

        """
        snapshots = {}
        cache_hits: t.Set[SnapshotId] = set()
        for s_id in snapshot_ids:
            snapshot = self._snapshot_cache.get(self._entry_name(s_id))
            if snapshot:
                if snapshot.is_model:
                    self._optimized_query_cache.with_optimized_query(snapshot.model)
                self._update_node_hash_cache(snapshot)
                snapshot.intervals = []
                snapshot.dev_intervals = []
                snapshots[s_id] = snapshot
                cache_hits.add(s_id)

        snapshot_ids_to_load = snapshot_ids - snapshots.keys()
        if snapshot_ids_to_load:
            loaded_snapshots = loader(snapshot_ids_to_load)
            for snapshot in loaded_snapshots:
                self._update_node_hash_cache(snapshot)
                self.put(snapshot)
                snapshots[snapshot.snapshot_id] = snapshot

        return snapshots, cache_hits

    def put(self, snapshot: Snapshot) -> None:
        if snapshot.is_model:
            self._optimized_query_cache.put(snapshot.model)
            # make sure we preload full_depends_on
            snapshot.model.full_depends_on
        self._snapshot_cache.put(self._entry_name(snapshot.snapshot_id), value=snapshot)

    def clear(self) -> None:
        self._snapshot_cache.clear()

    @staticmethod
    def _entry_name(snapshot_id: SnapshotId) -> str:
        return f"{snapshot_id.name}_{snapshot_id.identifier}"

    @staticmethod
    def _update_node_hash_cache(snapshot: Snapshot) -> None:
        snapshot.node._data_hash = snapshot.fingerprint.data_hash
        snapshot.node._metadata_hash = snapshot.fingerprint.metadata_hash
