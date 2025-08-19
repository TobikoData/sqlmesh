from __future__ import annotations

import typing as t
import json
import logging
from pathlib import Path
from collections import defaultdict
from sqlglot import exp
from pydantic import Field

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.state_sync.db.utils import (
    snapshot_name_filter,
    snapshot_name_version_filter,
    snapshot_id_filter,
    fetchone,
    fetchall,
    create_batches,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import SeedModel, ModelKindName
from sqlmesh.core.snapshot.cache import SnapshotCache
from sqlmesh.core.snapshot import (
    SnapshotIdLike,
    SnapshotNameVersionLike,
    SnapshotTableCleanupTask,
    SnapshotNameVersion,
    SnapshotInfoLike,
    Snapshot,
    SnapshotId,
    SnapshotFingerprint,
    SnapshotChangeCategory,
)
from sqlmesh.utils.migration import index_text_type, blob_text_type
from sqlmesh.utils.date import now_timestamp, TimeLike, to_timestamp
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils import unique

if t.TYPE_CHECKING:
    import pandas as pd


logger = logging.getLogger(__name__)


class SnapshotState:
    SNAPSHOT_BATCH_SIZE = 1000

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str] = None,
        cache_dir: Path = Path(),
    ):
        self.engine_adapter = engine_adapter
        self.snapshots_table = exp.table_("_snapshots", db=schema)
        self.auto_restatements_table = exp.table_("_auto_restatements", db=schema)

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)
        self._snapshot_columns_to_types = {
            "name": exp.DataType.build(index_type),
            "identifier": exp.DataType.build(index_type),
            "version": exp.DataType.build(index_type),
            "snapshot": exp.DataType.build(blob_type),
            "kind_name": exp.DataType.build("text"),
            "updated_ts": exp.DataType.build("bigint"),
            "unpaused_ts": exp.DataType.build("bigint"),
            "ttl_ms": exp.DataType.build("bigint"),
            "unrestorable": exp.DataType.build("boolean"),
            "forward_only": exp.DataType.build("boolean"),
        }

        self._auto_restatement_columns_to_types = {
            "snapshot_name": exp.DataType.build(index_type),
            "snapshot_version": exp.DataType.build(index_type),
            "next_auto_restatement_ts": exp.DataType.build("bigint"),
        }

        self._snapshot_cache = SnapshotCache(cache_dir)

    def push_snapshots(self, snapshots: t.Iterable[Snapshot], overwrite: bool = False) -> None:
        """Pushes snapshots to the state store.

        Args:
            snapshots: The snapshots to push.
            overwrite: Whether to overwrite existing snapshots.
        """
        if overwrite:
            snapshots = tuple(snapshots)
            self.delete_snapshots(snapshots)

        snapshots_to_store = []

        for snapshot in snapshots:
            if isinstance(snapshot.node, SeedModel):
                seed_model = t.cast(SeedModel, snapshot.node)
                snapshot = snapshot.copy(update={"node": seed_model.to_dehydrated()})
            snapshots_to_store.append(snapshot)

        self.engine_adapter.insert_append(
            self.snapshots_table,
            _snapshots_to_df(snapshots_to_store),
            target_columns_to_types=self._snapshot_columns_to_types,
            track_rows_processed=False,
        )

        for snapshot in snapshots:
            self._snapshot_cache.put(snapshot)

    def unpause_snapshots(
        self,
        snapshots: t.Collection[SnapshotInfoLike],
        unpaused_dt: TimeLike,
    ) -> None:
        unrestorable_snapshots_by_forward_only: t.Dict[bool, t.List[SnapshotNameVersion]] = (
            defaultdict(list)
        )

        for snapshot in snapshots:
            # We need to mark all other snapshots that have forward-only opposite to the target snapshot as unrestorable
            unrestorable_snapshots_by_forward_only[not snapshot.is_forward_only].append(
                snapshot.name_version
            )

        updated_ts = now_timestamp()
        unpaused_ts = to_timestamp(unpaused_dt)

        # Pause all snapshots with target names first
        for where in snapshot_name_filter(
            [s.name for s in snapshots],
            batch_size=self.SNAPSHOT_BATCH_SIZE,
        ):
            self.engine_adapter.update_table(
                self.snapshots_table,
                {"unpaused_ts": None, "updated_ts": updated_ts},
                where=where,
            )

        # Now unpause the target snapshots
        self._update_snapshots(
            [s.snapshot_id for s in snapshots],
            unpaused_ts=unpaused_ts,
            updated_ts=updated_ts,
        )

        # Mark unrestorable snapshots
        for forward_only, snapshot_name_versions in unrestorable_snapshots_by_forward_only.items():
            forward_only_exp = exp.column("forward_only").is_(exp.convert(forward_only))
            for where in snapshot_name_version_filter(
                self.engine_adapter,
                snapshot_name_versions,
                batch_size=self.SNAPSHOT_BATCH_SIZE,
                alias=None,
            ):
                self.engine_adapter.update_table(
                    self.snapshots_table,
                    {"unrestorable": True, "updated_ts": updated_ts},
                    where=forward_only_exp.and_(where),
                )

    def get_expired_snapshots(
        self,
        environments: t.Iterable[Environment],
        current_ts: int,
        ignore_ttl: bool = False,
    ) -> t.List[SnapshotTableCleanupTask]:
        """Aggregates the id's of the expired snapshots and creates a list of table cleanup tasks.

        Expired snapshots are snapshots that have exceeded their time-to-live
        and are no longer in use within an environment.

        Returns:
            The set of expired snapshot ids.
            The list of table cleanup tasks.
        """
        _, cleanup_targets = self._get_expired_snapshots(
            environments=environments,
            current_ts=current_ts,
            ignore_ttl=ignore_ttl,
        )
        return cleanup_targets

    def _get_expired_snapshots(
        self,
        environments: t.Iterable[Environment],
        current_ts: int,
        ignore_ttl: bool = False,
    ) -> t.Tuple[t.Set[SnapshotId], t.List[SnapshotTableCleanupTask]]:
        expired_query = exp.select("name", "identifier", "version").from_(self.snapshots_table)

        if not ignore_ttl:
            expired_query = expired_query.where(
                (exp.column("updated_ts") + exp.column("ttl_ms")) <= current_ts
            )

        expired_candidates = {
            SnapshotId(name=name, identifier=identifier): SnapshotNameVersion(
                name=name, version=version
            )
            for name, identifier, version in fetchall(self.engine_adapter, expired_query)
        }
        if not expired_candidates:
            return set(), []

        promoted_snapshot_ids = {
            snapshot.snapshot_id
            for environment in environments
            for snapshot in environment.snapshots
        }

        def _is_snapshot_used(snapshot: SharedVersionSnapshot) -> bool:
            return (
                snapshot.snapshot_id in promoted_snapshot_ids
                or snapshot.snapshot_id not in expired_candidates
            )

        unique_expired_versions = unique(expired_candidates.values())
        version_batches = create_batches(
            unique_expired_versions, batch_size=self.SNAPSHOT_BATCH_SIZE
        )
        cleanup_targets = []
        expired_snapshot_ids = set()
        for versions_batch in version_batches:
            snapshots = self._get_snapshots_with_same_version(versions_batch)

            snapshots_by_version = defaultdict(set)
            snapshots_by_dev_version = defaultdict(set)
            for s in snapshots:
                snapshots_by_version[(s.name, s.version)].add(s.snapshot_id)
                snapshots_by_dev_version[(s.name, s.dev_version)].add(s.snapshot_id)

            expired_snapshots = [s for s in snapshots if not _is_snapshot_used(s)]
            expired_snapshot_ids.update([s.snapshot_id for s in expired_snapshots])

            for snapshot in expired_snapshots:
                shared_version_snapshots = snapshots_by_version[(snapshot.name, snapshot.version)]
                shared_version_snapshots.discard(snapshot.snapshot_id)

                shared_dev_version_snapshots = snapshots_by_dev_version[
                    (snapshot.name, snapshot.dev_version)
                ]
                shared_dev_version_snapshots.discard(snapshot.snapshot_id)

                if not shared_dev_version_snapshots:
                    cleanup_targets.append(
                        SnapshotTableCleanupTask(
                            snapshot=snapshot.full_snapshot.table_info,
                            dev_table_only=bool(shared_version_snapshots),
                        )
                    )

        return expired_snapshot_ids, cleanup_targets

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        """Deletes snapshots.

        Args:
            snapshot_ids: The snapshot IDs to delete.
        """
        if not snapshot_ids:
            return
        for where in snapshot_id_filter(
            self.engine_adapter, snapshot_ids, batch_size=self.SNAPSHOT_BATCH_SIZE
        ):
            self.engine_adapter.delete_from(self.snapshots_table, where=where)

    def touch_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        """Touch snapshots to set their updated_ts to the current timestamp.

        Args:
            snapshot_ids: The snapshot IDs to touch.
        """
        self._update_snapshots(snapshot_ids)

    def get_snapshots(
        self,
        snapshot_ids: t.Iterable[SnapshotIdLike],
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches snapshots.

        Args:
            snapshot_ids: The snapshot IDs to fetch.

        Returns:
            A dictionary of snapshot IDs to snapshots.
        """
        return self._get_snapshots(snapshot_ids)

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        """Checks if snapshots exist.

        Args:
            snapshot_ids: The snapshot IDs to check.

        Returns:
            A set of snapshot IDs to check for existence.
        """
        return {
            SnapshotId(name=name, identifier=identifier)
            for where in snapshot_id_filter(
                self.engine_adapter, snapshot_ids, batch_size=self.SNAPSHOT_BATCH_SIZE
            )
            for name, identifier in fetchall(
                self.engine_adapter,
                exp.select("name", "identifier").from_(self.snapshots_table).where(where),
            )
        }

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        """Checks if nodes with given names exist.

        Args:
            names: The node names to check.
            exclude_external: Whether to exclude external nodes.

        Returns:
            A set of node names that exist.
        """
        names = set(names)

        if not names:
            return names

        query = (
            exp.select("name")
            .from_(self.snapshots_table)
            .where(exp.column("name").isin(*names))
            .distinct()
        )
        if exclude_external:
            query = query.where(exp.column("kind_name").neq(ModelKindName.EXTERNAL.value))
        return {name for (name,) in fetchall(self.engine_adapter, query)}

    def update_auto_restatements(
        self, next_auto_restatement_ts: t.Dict[SnapshotNameVersion, t.Optional[int]]
    ) -> None:
        """Updates the auto restatement timestamps.

        Args:
            next_auto_restatement_ts: A dictionary of snapshot name version to the next auto restatement timestamp.
        """
        next_auto_restatement_ts_deleted = []
        next_auto_restatement_ts_filtered = {}
        for k, v in next_auto_restatement_ts.items():
            if v is None:
                next_auto_restatement_ts_deleted.append(k)
            else:
                next_auto_restatement_ts_filtered[k] = v

        for where in snapshot_name_version_filter(
            self.engine_adapter,
            next_auto_restatement_ts_deleted,
            column_prefix="snapshot",
            alias=None,
            batch_size=self.SNAPSHOT_BATCH_SIZE,
        ):
            self.engine_adapter.delete_from(self.auto_restatements_table, where=where)

        if not next_auto_restatement_ts_filtered:
            return

        self.engine_adapter.merge(
            self.auto_restatements_table,
            _auto_restatements_to_df(next_auto_restatement_ts_filtered),
            target_columns_to_types=self._auto_restatement_columns_to_types,
            unique_key=(exp.column("snapshot_name"), exp.column("snapshot_version")),
        )

    def count(self) -> int:
        """Counts the number of snapshots in the state."""
        result = fetchone(self.engine_adapter, exp.select("COUNT(*)").from_(self.snapshots_table))
        return result[0] if result else 0

    def clear_cache(self) -> None:
        """Clears the snapshot cache."""
        self._snapshot_cache.clear()

    def _update_snapshots(
        self,
        snapshots: t.Iterable[SnapshotIdLike],
        **kwargs: t.Any,
    ) -> None:
        properties = kwargs
        if "updated_ts" not in properties:
            properties["updated_ts"] = now_timestamp()

        for where in snapshot_id_filter(
            self.engine_adapter, snapshots, batch_size=self.SNAPSHOT_BATCH_SIZE
        ):
            self.engine_adapter.update_table(
                self.snapshots_table,
                properties,
                where=where,
            )

    def _push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        snapshots_to_store = []
        for snapshot in snapshots:
            if isinstance(snapshot.node, SeedModel):
                seed_model = t.cast(SeedModel, snapshot.node)
                snapshot = snapshot.copy(update={"node": seed_model.to_dehydrated()})
            snapshots_to_store.append(snapshot)

        self.engine_adapter.insert_append(
            self.snapshots_table,
            _snapshots_to_df(snapshots_to_store),
            target_columns_to_types=self._snapshot_columns_to_types,
            track_rows_processed=False,
        )

    def _get_snapshots(
        self,
        snapshot_ids: t.Iterable[SnapshotIdLike],
        lock_for_update: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        duplicates: t.Dict[SnapshotId, Snapshot] = {}

        def _loader(snapshot_ids_to_load: t.Set[SnapshotId]) -> t.Collection[Snapshot]:
            fetched_snapshots: t.Dict[SnapshotId, Snapshot] = {}
            for query in self._get_snapshots_expressions(snapshot_ids_to_load, lock_for_update):
                for (
                    serialized_snapshot,
                    _,
                    _,
                    _,
                    updated_ts,
                    unpaused_ts,
                    unrestorable,
                    forward_only,
                    next_auto_restatement_ts,
                ) in fetchall(self.engine_adapter, query):
                    snapshot = parse_snapshot(
                        serialized_snapshot=serialized_snapshot,
                        updated_ts=updated_ts,
                        unpaused_ts=unpaused_ts,
                        unrestorable=unrestorable,
                        forward_only=forward_only,
                        next_auto_restatement_ts=next_auto_restatement_ts,
                    )
                    snapshot_id = snapshot.snapshot_id
                    if snapshot_id in fetched_snapshots:
                        other = duplicates.get(snapshot_id, fetched_snapshots[snapshot_id])
                        duplicates[snapshot_id] = (
                            snapshot if snapshot.updated_ts > other.updated_ts else other
                        )
                        fetched_snapshots[snapshot_id] = duplicates[snapshot_id]
                    else:
                        fetched_snapshots[snapshot_id] = snapshot
            return fetched_snapshots.values()

        snapshots, cached_snapshots = self._snapshot_cache.get_or_load(
            {s.snapshot_id for s in snapshot_ids}, _loader
        )

        if cached_snapshots:
            cached_snapshots_in_state: t.Set[SnapshotId] = set()
            for where in snapshot_id_filter(
                self.engine_adapter, cached_snapshots, batch_size=self.SNAPSHOT_BATCH_SIZE
            ):
                query = (
                    exp.select(
                        "name",
                        "identifier",
                        "updated_ts",
                        "unpaused_ts",
                        "unrestorable",
                        "forward_only",
                        "next_auto_restatement_ts",
                    )
                    .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                    .join(
                        exp.to_table(self.auto_restatements_table).as_("auto_restatements"),
                        on=exp.and_(
                            exp.column("name", table="snapshots").eq(
                                exp.column("snapshot_name", table="auto_restatements")
                            ),
                            exp.column("version", table="snapshots").eq(
                                exp.column("snapshot_version", table="auto_restatements")
                            ),
                        ),
                        join_type="left",
                        copy=False,
                    )
                    .where(where)
                )
                if lock_for_update:
                    query = query.lock(copy=False)
                for (
                    name,
                    identifier,
                    updated_ts,
                    unpaused_ts,
                    unrestorable,
                    forward_only,
                    next_auto_restatement_ts,
                ) in fetchall(self.engine_adapter, query):
                    snapshot_id = SnapshotId(name=name, identifier=identifier)
                    snapshot = snapshots[snapshot_id]
                    snapshot.updated_ts = updated_ts
                    snapshot.unpaused_ts = unpaused_ts
                    snapshot.unrestorable = unrestorable
                    snapshot.forward_only = forward_only
                    snapshot.next_auto_restatement_ts = next_auto_restatement_ts
                    cached_snapshots_in_state.add(snapshot_id)

            missing_cached_snapshots = cached_snapshots - cached_snapshots_in_state
            for missing_cached_snapshot_id in missing_cached_snapshots:
                snapshots.pop(missing_cached_snapshot_id, None)

        if duplicates:
            self.push_snapshots(duplicates.values(), overwrite=True)
            logger.error("Found duplicate snapshots in the state store.")

        return snapshots

    def _get_snapshots_expressions(
        self,
        snapshot_ids: t.Iterable[SnapshotIdLike],
        lock_for_update: bool = False,
    ) -> t.Iterator[exp.Expression]:
        for where in snapshot_id_filter(
            self.engine_adapter,
            snapshot_ids,
            alias="snapshots",
            batch_size=self.SNAPSHOT_BATCH_SIZE,
        ):
            query = (
                exp.select(
                    "snapshots.snapshot",
                    "snapshots.name",
                    "snapshots.identifier",
                    "snapshots.version",
                    "snapshots.updated_ts",
                    "snapshots.unpaused_ts",
                    "snapshots.unrestorable",
                    "snapshots.forward_only",
                    "auto_restatements.next_auto_restatement_ts",
                )
                .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                .join(
                    exp.to_table(self.auto_restatements_table).as_("auto_restatements"),
                    on=exp.and_(
                        exp.column("name", table="snapshots").eq(
                            exp.column("snapshot_name", table="auto_restatements")
                        ),
                        exp.column("version", table="snapshots").eq(
                            exp.column("snapshot_version", table="auto_restatements")
                        ),
                    ),
                    join_type="left",
                    copy=False,
                )
                .where(where)
            )
            if lock_for_update:
                query = query.lock(copy=False)
            yield query

    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Collection[SnapshotNameVersionLike],
        lock_for_update: bool = False,
    ) -> t.List[SharedVersionSnapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified identifiers.

        Args:
            snapshots: The collection of target name / version pairs.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The list of Snapshot objects.
        """
        if not snapshots:
            return []

        snapshot_rows = []

        for where in snapshot_name_version_filter(
            self.engine_adapter, snapshots, batch_size=self.SNAPSHOT_BATCH_SIZE
        ):
            query = (
                exp.select(
                    "snapshot",
                    "name",
                    "identifier",
                    "version",
                    "updated_ts",
                    "unpaused_ts",
                    "unrestorable",
                    "forward_only",
                )
                .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                .where(where)
            )
            if lock_for_update:
                query = query.lock(copy=False)

            snapshot_rows.extend(fetchall(self.engine_adapter, query))

        return [
            SharedVersionSnapshot.from_snapshot_record(
                name=name,
                identifier=identifier,
                version=version,
                updated_ts=updated_ts,
                unpaused_ts=unpaused_ts,
                unrestorable=unrestorable,
                forward_only=forward_only,
                snapshot=snapshot,
            )
            for snapshot, name, identifier, version, updated_ts, unpaused_ts, unrestorable, forward_only in snapshot_rows
        ]


def parse_snapshot(
    serialized_snapshot: str,
    updated_ts: int,
    unpaused_ts: t.Optional[int],
    unrestorable: bool,
    forward_only: bool,
    next_auto_restatement_ts: t.Optional[int],
) -> Snapshot:
    return Snapshot(
        **{
            **json.loads(serialized_snapshot),
            "updated_ts": updated_ts,
            "unpaused_ts": unpaused_ts,
            "unrestorable": unrestorable,
            "forward_only": forward_only,
            "next_auto_restatement_ts": next_auto_restatement_ts,
        }
    )


def _snapshot_to_json(snapshot: Snapshot) -> str:
    return snapshot.json(
        exclude={
            "intervals",
            "dev_intervals",
            "pending_restatement_intervals",
            "updated_ts",
            "unpaused_ts",
            "unrestorable",
            "forward_only",
            "next_auto_restatement_ts",
        }
    )


def _snapshots_to_df(snapshots: t.Iterable[Snapshot]) -> pd.DataFrame:
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "name": snapshot.name,
                "identifier": snapshot.identifier,
                "version": snapshot.version,
                "snapshot": _snapshot_to_json(snapshot),
                "kind_name": snapshot.model_kind_name.value if snapshot.model_kind_name else None,
                "updated_ts": snapshot.updated_ts,
                "unpaused_ts": snapshot.unpaused_ts,
                "ttl_ms": snapshot.ttl_ms,
                "unrestorable": snapshot.unrestorable,
                "forward_only": snapshot.forward_only,
            }
            for snapshot in snapshots
        ]
    )


def _auto_restatements_to_df(auto_restatements: t.Dict[SnapshotNameVersion, int]) -> pd.DataFrame:
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "snapshot_name": name_version.name,
                "snapshot_version": name_version.version,
                "next_auto_restatement_ts": ts,
            }
            for name_version, ts in auto_restatements.items()
        ]
    )


class SharedVersionSnapshot(PydanticModel):
    """A stripped down version of a snapshot that is used for fetching snapshots that share the same version
    with a significantly reduced parsing overhead.
    """

    name: str
    version: str
    dev_version_: t.Optional[str] = Field(alias="dev_version")
    identifier: str
    fingerprint: SnapshotFingerprint
    interval_unit: IntervalUnit
    change_category: SnapshotChangeCategory
    updated_ts: int
    unpaused_ts: t.Optional[int]
    unrestorable: bool
    disable_restatement: bool
    effective_from: t.Optional[TimeLike]
    raw_snapshot: t.Dict[str, t.Any]
    forward_only: bool

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

    @property
    def is_forward_only(self) -> bool:
        return self.forward_only or self.change_category == SnapshotChangeCategory.FORWARD_ONLY

    @property
    def normalized_effective_from_ts(self) -> t.Optional[int]:
        return (
            to_timestamp(self.interval_unit.cron_floor(self.effective_from))
            if self.effective_from
            else None
        )

    @property
    def dev_version(self) -> str:
        return self.dev_version_ or self.fingerprint.to_version()

    @property
    def full_snapshot(self) -> Snapshot:
        return Snapshot(
            **{
                **self.raw_snapshot,
                "updated_ts": self.updated_ts,
                "unpaused_ts": self.unpaused_ts,
                "unrestorable": self.unrestorable,
                "forward_only": self.forward_only,
            }
        )

    @classmethod
    def from_snapshot_record(
        cls,
        *,
        name: str,
        identifier: str,
        version: str,
        updated_ts: int,
        unpaused_ts: t.Optional[int],
        unrestorable: bool,
        forward_only: bool,
        snapshot: str,
    ) -> SharedVersionSnapshot:
        raw_snapshot = json.loads(snapshot)
        raw_node = raw_snapshot["node"]
        return SharedVersionSnapshot(
            name=name,
            version=version,
            dev_version=raw_snapshot.get("dev_version"),
            identifier=identifier,
            fingerprint=raw_snapshot["fingerprint"],
            interval_unit=raw_node.get("interval_unit", IntervalUnit.from_cron(raw_node["cron"])),
            change_category=raw_snapshot["change_category"],
            updated_ts=updated_ts,
            unpaused_ts=unpaused_ts,
            unrestorable=unrestorable,
            disable_restatement=raw_node.get("kind", {}).get("disable_restatement", False),
            effective_from=raw_snapshot.get("effective_from"),
            raw_snapshot=raw_snapshot,
            forward_only=forward_only,
        )
