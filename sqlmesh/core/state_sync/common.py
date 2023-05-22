from __future__ import annotations

import abc
import logging
import typing as t
from collections import defaultdict
from functools import wraps

from sqlmesh.core.engine_adapter.shared import TransactionType
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotNameVersionLike,
    SnapshotTableInfo,
)
from sqlmesh.core.state_sync.base import StateSync
from sqlmesh.utils.date import TimeLike, now, now_timestamp, to_datetime
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


def transactional(
    transaction_type: TransactionType = TransactionType.DML,
) -> t.Callable[[t.Callable], t.Callable]:
    def decorator(func: t.Callable) -> t.Callable:
        @wraps(func)
        def wrapper(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
            if not hasattr(self, "_transaction"):
                return func(self, *args, **kwargs)

            with self._transaction(transaction_type):
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


class CommonStateSyncMixin(StateSync):
    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]], hydrate_seeds: bool = False
    ) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots(snapshot_ids, hydrate_seeds=hydrate_seeds)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self._get_environment(environment)

    def get_snapshots_by_models(
        self, *names: str, lock_for_update: bool = False
    ) -> t.List[Snapshot]:
        """
        Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """
        return [
            snapshot
            for snapshot in self._get_snapshots(lock_for_update=lock_for_update).values()
            if snapshot.name in names
        ]

    @transactional()
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
        logger.info("Promoting environment '%s'", environment.name)

        snapshot_ids = set(snapshot.snapshot_id for snapshot in environment.snapshots)
        snapshots = self._get_snapshots(snapshot_ids, lock_for_update=True).values()
        missing = snapshot_ids - {snapshot.snapshot_id for snapshot in snapshots}
        if missing:
            raise SQLMeshError(
                f"Missing snapshots {missing}. Make sure to push and backfill your snapshots."
            )

        existing_environment = self._get_environment(environment.name, lock_for_update=True)

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
                table_info.name: table_info for table_info in existing_environment.snapshots
            }
        else:
            existing_table_infos = {}

        missing_models = set(existing_table_infos) - {snapshot.name for snapshot in snapshots}

        for snapshot in snapshots:
            existing_table_infos.get(snapshot.name)
            for parent in snapshot.parents:
                if parent not in snapshot_ids:
                    raise SQLMeshError(
                        f"Cannot promote snapshot `{snapshot.name}` because its parent `{parent.name}:{parent.identifier}` is not promoted. Did you mean to promote all snapshots instead of a subset?"
                    )

        table_infos = [s.table_info for s in snapshots]
        self._update_environment(environment)
        return table_infos, [existing_table_infos[name] for name in missing_models]

    @transactional()
    def finalize(self, environment: Environment) -> None:
        """Finalize the target environment, indicating that this environment has been
        fully promoted and is ready for use.

        Args:
            environment: The target environment to finalize.
        """
        logger.info("Finalizing environment '%s'", environment)

        stored_environment = self._get_environment(environment.name, lock_for_update=True)
        if stored_environment and stored_environment.plan_id != environment.plan_id:
            raise SQLMeshError(
                f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                f"Stored plan ID: '{stored_environment.plan_id}'. Please recreate the plan and try again"
            )

        environment.finalized_ts = now_timestamp()
        self._update_environment(environment)

    @transactional()
    def delete_expired_snapshots(self) -> t.List[Snapshot]:
        current_time = now()

        snapshots_by_version = defaultdict(list)
        for s in self._get_snapshots().values():
            snapshots_by_version[(s.name, s.version)].append(s)

        promoted_snapshot_ids = {
            snapshot.snapshot_id
            for environment in self.get_environments()
            for snapshot in environment.snapshots
        }

        def _is_snapshot_used(snapshot: Snapshot) -> bool:
            return (
                snapshot.snapshot_id in promoted_snapshot_ids
                or to_datetime(snapshot.ttl, relative_base=to_datetime(snapshot.updated_ts))
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

    @transactional()
    def unpause_snapshots(
        self, snapshots: t.Iterable[SnapshotInfoLike], unpaused_dt: TimeLike
    ) -> None:
        current_ts = now()

        target_snapshot_ids = {s.snapshot_id for s in snapshots}
        snapshots = self._get_snapshots_with_same_version(snapshots, lock_for_update=True)
        target_snapshots_by_version = {
            (s.name, s.version): s for s in snapshots if s.snapshot_id in target_snapshot_ids
        }

        for snapshot in snapshots:
            is_target_snapshot = snapshot.snapshot_id in target_snapshot_ids
            if is_target_snapshot and not snapshot.unpaused_ts:
                logger.info("Unpausing snapshot %s", snapshot.snapshot_id)
                snapshot.set_unpaused_ts(unpaused_dt)
                self._update_snapshot(snapshot)
            elif not is_target_snapshot:
                target_snapshot = target_snapshots_by_version[(snapshot.name, snapshot.version)]
                if target_snapshot.normalized_effective_from_ts:
                    # Making sure that there are no overlapping intervals.
                    effective_from_ts = target_snapshot.normalized_effective_from_ts
                    logger.info(
                        "Removing all intervals after '%s' for snapshot %s, superseded by snapshot %s",
                        target_snapshot.effective_from,
                        snapshot.snapshot_id,
                        target_snapshot.snapshot_id,
                    )
                    self.remove_interval(
                        [], effective_from_ts, current_ts, all_snapshots=[snapshot]
                    )

                if snapshot.unpaused_ts:
                    logger.info("Pausing snapshot %s", snapshot.snapshot_id)
                    snapshot.set_unpaused_ts(None)
                    self._update_snapshot(snapshot)

    def _ensure_no_gaps(
        self, target_snapshots: t.Iterable[Snapshot], target_environment: Environment
    ) -> None:
        from sqlmesh.core.scheduler import start_date

        target_snapshots_by_name = {s.name: s for s in target_snapshots}

        changed_version_prev_snapshots_by_name = {
            s.name: s
            for s in target_environment.snapshots
            if s.name in target_snapshots_by_name
            and target_snapshots_by_name[s.name].version != s.version
        }

        changed_version_target_snapshots = [
            t for t in target_snapshots if t.name in changed_version_prev_snapshots_by_name
        ]

        all_snapshot_intervals = self.get_snapshot_intervals(
            [*changed_version_prev_snapshots_by_name.values(), *changed_version_target_snapshots],
        )

        hydrated_prev_snapshots = Snapshot.hydrate_with_intervals_by_version(
            self.get_snapshots(changed_version_prev_snapshots_by_name.values()).values(),
            all_snapshot_intervals,
        )
        hydrated_target_snapshots = Snapshot.hydrate_with_intervals_by_version(
            changed_version_target_snapshots, all_snapshot_intervals
        )
        hydrated_target_snapshots_by_name = {s.name: s for s in hydrated_target_snapshots}

        for prev_snapshot in hydrated_prev_snapshots:
            target_snapshot = hydrated_target_snapshots_by_name[prev_snapshot.name]
            if (
                target_snapshot.is_incremental_by_time_range
                and prev_snapshot.is_incremental_by_time_range
                and prev_snapshot.intervals
            ):
                missing_intervals = target_snapshot.missing_intervals(
                    start_date(target_snapshot, target_snapshots_by_name.values())
                    or prev_snapshot.intervals[0][0],
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
        hydrate_seeds: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots.

        Args:
            snapshot_ids: The collection of IDs of snapshots to fetch
            lock_for_update: Lock the snapshot rows for future update
            hydrate_seeds: Whether to hydrate seed snapshots with the content.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """

    @abc.abstractmethod
    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Iterable[SnapshotNameVersionLike],
        lock_for_update: bool = False,
    ) -> t.List[Snapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified version.

        Args:
            snapshots: The collection of target name / version pairs.
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
