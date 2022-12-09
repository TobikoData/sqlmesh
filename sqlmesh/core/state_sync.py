"""
# StateSync

State sync is how SQLMesh keeps track of environments and their states, e.g. snapshots.

# StateReader

StateReader provides a subset of the functionalities of the StateSync class. As its name
implies, it only allows for read-only operations on snapshots and environment states.

# EngineAdapterStateSync

The provided `sqlmesh.core.state_sync.EngineAdapterStateSync` leverages an existing engine
adapter to read and write state to the underlying data store.
"""
from __future__ import annotations

import abc
import json
import logging
import typing as t
from collections import defaultdict

from sqlglot import exp

from sqlmesh.core import scheduler
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotTableInfo,
)
from sqlmesh.utils.date import TimeLike, now, to_datetime
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.file_cache import FileCache

logger = logging.getLogger(__name__)


class StateReader(abc.ABC):
    """Abstract base class for read-only operations on snapshot and environment state."""

    @abc.abstractmethod
    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Bulk fetch snapshots given the corresponding snapshot ids.

        Args:
            snapshot_ids: Iterable of snapshot ids to get. If not provided all
                available snapshots will be returned.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """

    @abc.abstractmethod
    def get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotInfoLike]
    ) -> t.List[Snapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified fingerprints.

        Args:
            snapshots: The list of snapshots or table infos.

        Returns:
            The list of Snapshot objects.
        """

    @abc.abstractmethod
    def snapshots_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Set[SnapshotId]:
        """Checks if multiple snapshots exist in the state sync.

        Args:
            snapshot_ids: Iterable of snapshot ids to bulk check.

        Returns:
            A set of all the existing snapshot ids.
        """

    @abc.abstractmethod
    def get_environment(self, environment: str) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The environment

        Returns:
            The environment object.
        """

    @abc.abstractmethod
    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """

    @abc.abstractmethod
    def get_snapshots_by_models(self, *names: str) -> t.List[Snapshot]:
        """Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """

    def missing_intervals(
        self,
        env_or_snapshots: str | Environment | t.Iterable[Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Iterable[str]] = None,
    ) -> t.Dict[Snapshot, Intervals]:
        """Find missing intervals for an environment or a list of snapshots.

        Args:
            env_or_snapshots: The environment or snapshots to find missing intervals for.
            start: The start of the time range to look for.
            end: The end of the time range to look for.
            latest: The latest datetime to use for non-incremental queries.

        Returns:
            A dictionary of SnapshotId to Intervals.
        """

        if isinstance(env_or_snapshots, str):
            env = self.get_environment(env_or_snapshots)
        elif isinstance(env_or_snapshots, Environment):
            env = env_or_snapshots
        else:
            env = None

        if env:
            snapshots_by_id = self.get_snapshots(env.snapshots)
            start = start or env.start
            end = end or env.end
        elif isinstance(env_or_snapshots, str):
            snapshots_by_id = {}
        elif not isinstance(env_or_snapshots, Environment):
            snapshots_by_id = {
                snapshot.snapshot_id: snapshot for snapshot in env_or_snapshots
            }
        else:
            raise SQLMeshError("This shouldn't be possible.")

        if not snapshots_by_id:
            return {}

        unversioned = [
            snapshot for snapshot in snapshots_by_id.values() if not snapshot.version
        ]

        snapshots_by_id = {
            **snapshots_by_id,
            **(self.get_snapshots(unversioned) if unversioned else {}),
        }

        all_snapshots = {
            **snapshots_by_id,
            **{
                snapshot.snapshot_id: snapshot
                for snapshot in self.get_snapshots_with_same_version(
                    snapshot
                    for snapshot in snapshots_by_id.values()
                    if snapshot.version
                )
            },
        }

        missing = {}
        start_date = to_datetime(
            start or scheduler.earliest_start_date(snapshots_by_id.values())
        )
        end_date = end or now()
        restatements = set(restatements or [])

        for snapshot in Snapshot.merge_snapshots(snapshots_by_id, all_snapshots):
            if snapshot.name in restatements:
                snapshot.remove_interval(start_date, end_date)
            intervals = snapshot.missing_intervals(
                max(
                    start_date,
                    to_datetime(
                        scheduler.start_date(snapshot, snapshots_by_id.values())
                        or start_date
                    ),
                ),
                end_date,
                latest=latest,
            )
            if intervals:
                missing[snapshot] = intervals
        return missing


class StateSync(StateReader, abc.ABC):
    """Abstract base class for snapshot and environment state management."""

    @abc.abstractmethod
    def init_schema(self) -> None:
        """Optional initialization of the sync."""

    @abc.abstractmethod
    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Push snapshots into the state sync.

        This method only allows for pushing new snapshots. If existing snapshots are found,
        this method should raise an error.

        Raises:
            SQLMeshError when existing snapshots are pushed.

        Args:
            snapshots: A list of snapshots to save in the state sync.
        """

    @abc.abstractmethod
    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        """Delete snapshots from the state sync.

        Args:
            snapshot_ids: A list of snapshot like objects to delete.
        """

    @abc.abstractmethod
    def add_interval(
        self, snapshot_id: SnapshotIdLike, start: TimeLike, end: TimeLike
    ) -> None:
        """Add an interval to a snapshot and sync it to the store.

        Snapshots must be pushed before adding intervals to them.

        Args:
            snapshot_id: The snapshot like object to add an interval to.
            start: The start of the interval to add.
            end: The end of the interval to add.
        """

    @abc.abstractmethod
    def remove_interval(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        start: TimeLike,
        end: TimeLike,
        all_snapshots: t.Optional[t.Iterable[Snapshot]] = None,
    ) -> None:
        """Remove an interval from a list of snapshots and sync it to the store.

        Because multiple snapshots can be pointing to the same version or physical table, this method
        can also grab all snapshots tied to the passed in version.

        Args:
            snapshots: The snapshot info like object to remove intervals from.
            start: The start of the interval to add.
            end: The end of the interval to add.
            all_snapshots: All snapshots can be passed in to skip fetching matching snapshot versions.
        """

    @abc.abstractmethod
    def promote(
        self, environment: Environment, no_gaps: bool = False
    ) -> t.Tuple[t.List[SnapshotTableInfo], t.List[SnapshotTableInfo]]:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
           A tuple of (added snapshot table infos, removed snapshot table infos)
        """

    @abc.abstractmethod
    def unpause_snapshots(
        self, snapshot_ids: t.Iterable[SnapshotIdLike], unpaused_dt: TimeLike
    ) -> None:
        """Unpauses target snapshots.

        Unpaused snapshots are scheduled for evaluation on a recurring basis.
        Once unpaused a snapshot can't be paused again.

        Args:
            snapshot_ids: Target snapshot IDs.
            unpaused_dt: The datetime object which indicates when target snapshots
                were unpaused.
        """

    @abc.abstractmethod
    def remove_expired_snapshots(self) -> t.List[Snapshot]:
        """Removes expired snapshots.

        Expired snapshots are snapshots that have exceeded their time-to-live
        and are no longer in use within an environment.

        Returns:
            The list of removed snapshots.
        """


class CommonStateSyncMixin(StateSync):
    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots(snapshot_ids)

    def get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotInfoLike]
    ) -> t.List[Snapshot]:
        return self._get_snapshots_with_same_version(snapshots)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self._get_environment(environment)

    def get_snapshots_by_models(
        self, *names: str, lock_for_update=False
    ) -> t.List[Snapshot]:
        """
        Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """
        return [
            snapshot
            for snapshot in self._get_snapshots(
                lock_for_update=lock_for_update
            ).values()
            if snapshot.name in names
        ]

    def promote(
        self, environment: Environment, no_gaps: bool = False
    ) -> t.Tuple[t.List[SnapshotTableInfo], t.List[SnapshotTableInfo]]:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
           A tuple of (added snapshot table infos, removed snapshot table infos)
        """
        snapshot_ids = set(snapshot.snapshot_id for snapshot in environment.snapshots)
        snapshots = self._get_snapshots(snapshot_ids, lock_for_update=True).values()
        missing = snapshot_ids - {snapshot.snapshot_id for snapshot in snapshots}
        if missing:
            raise SQLMeshError(
                f"Missing snapshots {missing}. Make sure to push and backfill your snapshots."
            )

        existing_environment = self._get_environment(
            environment.name, lock_for_update=True
        )

        if existing_environment:
            if environment.previous_plan_id != existing_environment.plan_id:
                raise SQLMeshError(
                    f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                    f"Expected previous plan ID: '{environment.previous_plan_id}', actual previous plan ID: '{existing_environment.plan_id}'. "
                    "Please recreate the plan and try again"
                )

            if no_gaps:
                self._ensure_no_gaps(snapshots, existing_environment)

            existing_table_infos = {
                table_info.name: table_info
                for table_info in existing_environment.snapshots
            }
        else:
            existing_table_infos = {}

        missing_models = set(existing_table_infos) - {
            snapshot.name for snapshot in snapshots
        }

        for snapshot in snapshots:
            existing_table_infos.get(snapshot.name)
            for parent in snapshot.parents:
                if parent not in snapshot_ids:
                    raise SQLMeshError(
                        f"Cannot promote snapshot `{snapshot.name}` because its parent `{parent.name}:{parent.fingerprint}` is not promoted. Did you mean to promote all snapshots instead of a subset?"
                    )

        table_infos = [s.table_info for s in snapshots]
        self._update_environment(environment)
        return table_infos, [existing_table_infos[name] for name in missing_models]

    def remove_expired_snapshots(self) -> t.List[Snapshot]:
        current_time = now()

        snapshots_by_version = defaultdict(list)
        for s in self._get_snapshots().values():
            snapshots_by_version[(s.name, s.version)].append(s)

        promoted_snapshot_ids = {
            snapshot.snapshot_id
            for environment in self.get_environments()
            for snapshot in environment.snapshots
        }

        def _is_snapshot_used(snapshot):
            return (
                snapshot.snapshot_id in promoted_snapshot_ids
                or to_datetime(
                    snapshot.ttl, relative_base=to_datetime(snapshot.updated_ts)
                )
                > current_time
            )

        expired_snapshots = []

        for snapshots in snapshots_by_version.values():
            if any(map(_is_snapshot_used, snapshots)):
                continue

            for snapshot in snapshots:
                expired_snapshots.append(snapshot)

        if expired_snapshots:
            self.delete_snapshots(expired_snapshots)

        return expired_snapshots

    def add_interval(
        self, snapshot_id: SnapshotIdLike, start: TimeLike, end: TimeLike
    ) -> None:
        snapshot_id = snapshot_id.snapshot_id
        logger.info("Adding interval for snapshot %s", snapshot_id)

        stored_snapshots = self._get_snapshots([snapshot_id], lock_for_update=True)
        if snapshot_id not in stored_snapshots:
            raise SQLMeshError(f"Snapshot {snapshot_id} was not found")

        stored_snapshot = stored_snapshots[snapshot_id]
        stored_snapshot.add_interval(start, end)
        self._update_snapshot(stored_snapshot)

    def remove_interval(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        start: TimeLike,
        end: TimeLike,
        all_snapshots: t.Optional[t.Iterable[Snapshot]] = None,
    ) -> None:
        all_snapshots = all_snapshots or self._get_snapshots_with_same_version(
            snapshots, lock_for_update=True
        )
        for snapshot in all_snapshots:
            logger.info("Removing interval for snapshot %s", snapshot.snapshot_id)
            snapshot.remove_interval(start, end)
            self._update_snapshot(snapshot)

    def unpause_snapshots(
        self, snapshot_ids: t.Iterable[SnapshotIdLike], unpaused_dt: TimeLike
    ) -> None:
        snapshots = self._get_snapshots(snapshot_ids=snapshot_ids, lock_for_update=True)
        for snapshot in snapshots.values():
            snapshot.set_unpaused_ts(unpaused_dt)
            self._update_snapshot(snapshot)

    def _ensure_no_gaps(
        self, target_snapshots: t.Iterable[Snapshot], target_environment: Environment
    ) -> None:
        target_snapshots_by_name = {s.name: s for s in target_snapshots}

        changed_version_prev_snapshots_by_name = {
            s.name: s
            for s in target_environment.snapshots
            if s.name in target_snapshots_by_name
            and target_snapshots_by_name[s.name].version != s.version
        }

        changed_version_target_snapshots = [
            t
            for t in target_snapshots
            if t.name in changed_version_prev_snapshots_by_name
        ]

        all_snapshots = {
            s.snapshot_id: s
            for s in self._get_snapshots_with_same_version(
                [
                    *changed_version_prev_snapshots_by_name.values(),
                    *changed_version_target_snapshots,
                ]
            )
        }

        merged_prev_snapshots = Snapshot.merge_snapshots(
            changed_version_prev_snapshots_by_name.values(), all_snapshots
        )
        merged_target_snapshots = Snapshot.merge_snapshots(
            changed_version_target_snapshots, all_snapshots
        )
        merged_target_snapshots_by_name = {s.name: s for s in merged_target_snapshots}

        for prev_snapshot in merged_prev_snapshots:
            target_snapshot = merged_target_snapshots_by_name[prev_snapshot.name]
            if (
                target_snapshot.is_incremental_kind
                and prev_snapshot.is_incremental_kind
                and prev_snapshot.intervals
            ):
                missing_intervals = target_snapshot.missing_intervals(
                    prev_snapshot.intervals[0][0],
                    prev_snapshot.intervals[-1][1],
                )
                if missing_intervals:
                    raise SQLMeshError(
                        f"Detected gaps in snapshot {target_snapshot.snapshot_id}: {missing_intervals}"
                    )

    @abc.abstractmethod
    def _update_environment(self, environment: Environment) -> None:
        """Overwrites the target environment with a given environment.

        Args:
            environment: The new environment.
        """

    @abc.abstractmethod
    def _update_snapshot(self, snapshot: Snapshot) -> None:
        """Updates the target snapshot.

        Args:
            snapshot: The target snapshot.
        """

    @abc.abstractmethod
    def _get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots.

        Args:
            snapshot_ids: The collection of IDs of snapshots to fetch
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """

    @abc.abstractmethod
    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        lock_for_update: bool = False,
    ) -> t.List[Snapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified fingerprints.

        Args:
            snapshots: The list of snapshots or table infos.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The list of Snapshot objects.
        """

    @abc.abstractmethod
    def _get_environment(
        self, environment: str, lock_for_update: bool = False
    ) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The target environment name.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The target environment.
        """


class EngineAdapterStateSync(CommonStateSyncMixin, StateSync):
    """Manages state of models and snapshot with an existing engine adapter.

    This state sync is convenient to use because it requires no additional setup.
    You can reuse the same engine/warehouse that your data is stored in.

    Args:
        engine_adapter: The EngineAdapter to use to store and fetch snapshots.
        schema: The schema to store state metadata in.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: str,
        table_info_cache: FileCache,
    ):
        self.engine_adapter = engine_adapter
        self.snapshots_table = f"{schema}._snapshots"
        self.environments_table = f"{schema}._environments"
        self.table_info_cache = table_info_cache

    def init_schema(self) -> None:
        """Creates the schema and table to store state."""
        self.engine_adapter.create_schema(self.snapshots_table)

        self.engine_adapter.create_table(
            self.snapshots_table,
            {
                "name": exp.DataType.build("text"),
                "fingerprint": exp.DataType.build("text"),
                "version": exp.DataType.build("text"),
                "snapshot": exp.DataType.build("text"),
            },
        )
        self.engine_adapter.create_table(
            self.environments_table,
            {
                "name": exp.DataType.build("text"),
                "snapshots": exp.DataType.build("text"),
                "start": exp.DataType.build("text"),
                "end": exp.DataType.build("text"),
                "plan_id": exp.DataType.build("text"),
                "previous_plan_id": exp.DataType.build("text"),
            },
        )

    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Pushes snapshots to the state store, merging them with existing ones.

        This method first finds all existing snapshots in the store and merges them with
        the local snapshots. It will then delete all existing snapshots and then
        insert all the local snapshots. This can be made safer with locks or merge/upsert.

        Args:
            snapshot_ids: Iterable of snapshot ids to bulk push.
        """
        snapshots_by_id = {}
        for snapshot in snapshots:
            if not snapshot.version:
                raise SQLMeshError(
                    f"Snapshot {snapshot} has not been versioned yet. Create a plan before pushing a snapshot."
                )
            snapshots_by_id[snapshot.snapshot_id] = snapshot

        existing = self.snapshots_exist(snapshots_by_id)

        if existing:
            raise SQLMeshError(f"Snapshots {existing} already exists.")

        snapshots = snapshots_by_id.values()

        if snapshots:
            self._update_cache(snapshots)
            self._push_snapshots(snapshots)

    def _push_snapshots(self, snapshots: t.Iterable[Snapshot], overwrite=False) -> None:
        if overwrite:
            snapshots = tuple(snapshots)
            self.delete_snapshots(snapshots)

        self.engine_adapter.insert_append(
            self.snapshots_table,
            exp.values(
                [
                    (
                        snapshot.name,
                        snapshot.fingerprint,
                        snapshot.version,
                        snapshot.json(),
                    )
                    for snapshot in snapshots
                ]
            ),
        )

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        self.engine_adapter.delete_from(
            self.snapshots_table, where=self._filter_condition(snapshot_ids)
        )

    def snapshots_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Set[SnapshotId]:
        return {
            SnapshotId(name=name, fingerprint=fingerprint)
            for name, fingerprint in self.engine_adapter.fetchall(
                exp.select("name", "fingerprint")
                .from_(self.snapshots_table)
                .where(self._filter_condition(snapshot_ids))
            )
        }

    def _update_environment(self, environment: Environment) -> None:
        self.engine_adapter.delete_from(
            self.environments_table,
            where=f"name = '{environment.name}'",
        )

        self.engine_adapter.insert_append(
            self.environments_table,
            exp.values(
                [
                    (
                        environment.name,
                        [snapshot.json() for snapshot in environment.snapshots],
                        environment.start,
                        environment.end,
                        environment.plan_id,
                        environment.previous_plan_id,
                    )
                ]
            ),
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        self.engine_adapter.update_table(
            self.snapshots_table,
            {"snapshot": snapshot.json()},
            where=self._filter_condition([snapshot.snapshot_id]),
        )

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return [
            self._environment_from_row(row)
            for row in self.engine_adapter.fetchall(self._environments_query())
        ]

    def _environment_from_row(self, row: t.Tuple[str, ...]) -> Environment:
        return Environment(
            **{field: row[i] for i, field in enumerate(Environment.__fields__)}
        )

    def _environments_query(self, where=None) -> exp.Select:
        return (
            exp.select(*(f'"{field}"' for field in Environment.__fields__))
            .from_(self.environments_table)
            .where(where)
        )

    def remove_expired_snapshots(self) -> t.List[Snapshot]:
        expired_snapshots = super().remove_expired_snapshots()
        for snapshot in expired_snapshots:
            self.engine_adapter.drop_table(snapshot.table_name)

        return expired_snapshots

    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        snapshots = super().get_snapshots(snapshot_ids)
        self._update_cache(snapshots.values())
        return snapshots

    def _get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        expression = (
            exp.select("snapshot")
            .from_(self.snapshots_table)
            .where(
                None if snapshot_ids is None else self._filter_condition(snapshot_ids)
            )
        )

        snapshots: t.Dict[SnapshotId, Snapshot] = {}
        duplicates: t.Dict[SnapshotId, Snapshot] = {}

        for row in self.engine_adapter.fetchall(expression):
            snapshot = Snapshot.parse_raw(row[0])
            snapshot_id = snapshot.snapshot_id
            if snapshot_id in snapshots:
                other = duplicates.get(snapshot_id, snapshots[snapshot_id])
                duplicates[snapshot_id] = (
                    snapshot if snapshot.updated_ts > other.updated_ts else other
                )
                snapshots[snapshot_id] = duplicates[snapshot_id]
            else:
                snapshots[snapshot_id] = snapshot

        if duplicates:
            self._push_snapshots(duplicates.values(), overwrite=True)
            logger.error("Found duplicate snapshots in the state store.")

        return snapshots

    def _get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotInfoLike], lock_for_update: bool = False
    ) -> t.List[Snapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified fingerprints.

        Args:
            snapshots: The list of snapshots or table infos.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The list of Snapshot objects.
        """
        if not snapshots:
            return []

        snapshot_rows = self.engine_adapter.fetchall(
            exp.select("snapshot")
            .from_(self.snapshots_table)
            .where(
                exp.In(
                    this=exp.to_identifier("version"),
                    expressions=[
                        exp.convert(snapshot.version) for snapshot in snapshots
                    ],
                )
            )
        )
        return [Snapshot(**json.loads(row[0])) for row in snapshot_rows]

    def _get_environment(
        self, environment: str, lock_for_update: bool = False
    ) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The environment
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The environment object.
        """
        row = self.engine_adapter.fetchone(
            self._environments_query(f"name = '{environment}'")
        )

        if not row:
            return None

        env = self._environment_from_row(row)
        self._update_cache(env.snapshots)
        return env

    def _filter_condition(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Union[exp.Or, exp.Boolean]:
        if not snapshot_ids:
            return exp.FALSE

        return exp.or_(
            *(
                exp.and_(
                    f"name = '{snapshot_id.name}'",
                    f"fingerprint = '{snapshot_id.fingerprint}'",
                )
                for snapshot_id in snapshot_ids
            )
        )

    def _update_cache(self, snapshots: t.Iterable[SnapshotInfoLike]) -> None:
        with self.table_info_cache:
            self.table_info_cache.update(
                {snapshot.snapshot_id: snapshot.table_info for snapshot in snapshots}
            )
