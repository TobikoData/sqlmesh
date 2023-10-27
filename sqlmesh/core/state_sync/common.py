from __future__ import annotations

import abc
import logging
import typing as t
from collections import defaultdict
from datetime import datetime
from functools import wraps

from sqlmesh.core.dialect import schema_
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotNameVersionLike,
    SnapshotTableCleanupTask,
    start_date,
)
from sqlmesh.core.state_sync.base import (
    PromotionResult,
    SnapshotTableCleanupTask,
    StateSync,
)
from sqlmesh.utils.date import TimeLike, now, now_timestamp, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter

logger = logging.getLogger(__name__)


def cleanup_expired_views(adapter: EngineAdapter, environments: t.List[Environment]) -> None:
    expired_schema_environments = [
        environment for environment in environments if environment.suffix_target.is_schema
    ]
    expired_table_environments = [
        environment for environment in environments if environment.suffix_target.is_table
    ]
    for expired_catalog, expired_schema in {
        (
            snapshot.qualified_view_name.catalog,
            snapshot.qualified_view_name.schema_for_environment(environment.naming_info),
        )
        for environment in expired_schema_environments
        for snapshot in environment.snapshots
        if snapshot.is_model
    }:
        schema = schema_(expired_schema, expired_catalog)
        try:
            adapter.drop_schema(
                schema,
                ignore_if_not_exists=True,
                cascade=True,
            )
        except Exception as e:
            logger.warning("Falied to drop the expired environment schema '%s': %s", schema, e)
    for expired_view in {
        snapshot.qualified_view_name.for_environment(environment.naming_info)
        for environment in expired_table_environments
        for snapshot in environment.snapshots
        if snapshot.is_model
    }:
        try:
            adapter.drop_view(expired_view, ignore_if_not_exists=True)
        except Exception as e:
            logger.warning("Falied to drop the expired environment view '%s': %s", expired_view, e)


def transactional() -> t.Callable[[t.Callable], t.Callable]:
    def decorator(func: t.Callable) -> t.Callable:
        @wraps(func)
        def wrapper(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
            if not hasattr(self, "_transaction"):
                return func(self, *args, **kwargs)

            with self._transaction():
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


class CommonStateSyncMixin(StateSync):
    def get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]],
        hydrate_seeds: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots(snapshot_ids, hydrate_seeds=hydrate_seeds)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self._get_environment(environment)

    @transactional()
    def promote(
        self,
        environment: Environment,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        no_gaps: bool = False,
    ) -> PromotionResult:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            deployability_index: Determines snapshots that are deployable in the context of this promotion.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
           A tuple of (added snapshot table infos, removed snapshot table infos, and environment target suffix for the removed table infos)
        """
        logger.info("Promoting environment '%s'", environment.name)

        missing = {s.snapshot_id for s in environment.snapshots} - self.snapshots_exist(
            environment.snapshots
        )
        if missing:
            raise SQLMeshError(
                f"Missing snapshots {missing}. Make sure to push and backfill your snapshots."
            )

        existing_environment = self._get_environment(environment.name, lock_for_update=True)

        environment_suffix_target_changed = False
        removed_environment_naming_info = environment.naming_info
        if existing_environment:
            environment_suffix_target_changed = (
                environment.suffix_target != existing_environment.suffix_target
            )
            if environment_suffix_target_changed:
                removed_environment_naming_info.suffix_target = existing_environment.suffix_target
            if environment.previous_plan_id != existing_environment.plan_id:
                raise SQLMeshError(
                    f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                    f"Expected previous plan ID: '{environment.previous_plan_id}', actual previous plan ID: '{existing_environment.plan_id}'. "
                    "Please recreate the plan and try again"
                )

            if no_gaps:
                snapshots = self._get_snapshots(environment.snapshots).values()
                self._ensure_no_gaps(
                    snapshots,
                    existing_environment,
                    deployability_index or DeployabilityIndex.all_deployable(),
                )

            existing_table_infos = {
                table_info.name: table_info
                for table_info in existing_environment.promoted_snapshots
            }

            demoted_snapshots = set(existing_environment.snapshots) - set(environment.snapshots)
            for demoted_snapshot in self._get_snapshots(demoted_snapshots).values():
                # Update the updated_at attribute.
                self._update_snapshot(demoted_snapshot)
        else:
            existing_table_infos = {}

        missing_models = set(existing_table_infos) - {
            snapshot.name for snapshot in environment.promoted_snapshots
        }

        table_infos = set(environment.promoted_snapshots)
        if (
            existing_environment
            and existing_environment.finalized_ts
            and not environment_suffix_target_changed
        ):
            # Only promote new snapshots.
            table_infos -= set(existing_environment.promoted_snapshots)

        self._update_environment(environment)

        removed = (
            list(existing_table_infos.values())
            if environment_suffix_target_changed
            else [existing_table_infos[name] for name in missing_models]
        )

        return PromotionResult(
            added=sorted(table_infos),
            removed=removed,
            removed_environment_naming_info=removed_environment_naming_info if removed else None,
        )

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
    def delete_expired_snapshots(self) -> t.List[SnapshotTableCleanupTask]:
        current_time = now(minute_floor=False)

        snapshots = self._get_snapshots(hydrate_intervals=False)

        snapshots_by_version = defaultdict(set)
        for s in snapshots.values():
            snapshots_by_version[(s.name, s.version)].add(s.snapshot_id)

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

        expired_snapshots = [s for s in snapshots.values() if not _is_snapshot_used(s)]

        if expired_snapshots:
            self.delete_snapshots(expired_snapshots)

        cleanup_targets = []
        for snapshot in expired_snapshots:
            shared_version_snapshots = snapshots_by_version[(snapshot.name, snapshot.version)]
            shared_version_snapshots.discard(snapshot.snapshot_id)
            cleanup_targets.append(
                SnapshotTableCleanupTask(
                    snapshot=snapshot.table_info, dev_table_only=bool(shared_version_snapshots)
                )
            )

        return cleanup_targets

    @transactional()
    def unpause_snapshots(
        self, snapshots: t.Collection[SnapshotInfoLike], unpaused_dt: TimeLike
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
                        [(snapshot, snapshot.get_removal_interval(effective_from_ts, current_ts))]
                    )

                if snapshot.unpaused_ts:
                    logger.info("Pausing snapshot %s", snapshot.snapshot_id)
                    snapshot.set_unpaused_ts(None)
                    if not snapshot.is_forward_only and target_snapshot.is_forward_only:
                        snapshot.unrestorable = True
                    self._update_snapshot(snapshot)

    def _ensure_no_gaps(
        self,
        target_snapshots: t.Iterable[Snapshot],
        target_environment: Environment,
        deployability_index: DeployabilityIndex,
    ) -> None:
        target_snapshots_by_name = {s.name: s for s in target_snapshots}

        changed_version_prev_snapshots_by_name = {
            s.name: s
            for s in target_environment.snapshots
            if s.name in target_snapshots_by_name
            and target_snapshots_by_name[s.name].version != s.version
        }

        prev_snapshots = self._get_snapshots(
            changed_version_prev_snapshots_by_name.values()
        ).values()
        cache: t.Dict[str, datetime] = {}

        for prev_snapshot in prev_snapshots:
            target_snapshot = target_snapshots_by_name[prev_snapshot.name]
            if (
                deployability_index.is_representative(target_snapshot)
                and target_snapshot.is_incremental
                and prev_snapshot.is_incremental
                and prev_snapshot.intervals
            ):
                start = to_timestamp(
                    start_date(target_snapshot, target_snapshots_by_name.values(), cache)
                )
                end = prev_snapshot.intervals[-1][1]

                if start < end:
                    missing_intervals = target_snapshot.missing_intervals(start, end)

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
        hydrate_intervals: bool = True,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots.

        Args:
            snapshot_ids: The collection of IDs of snapshots to fetch
            lock_for_update: Lock the snapshot rows for future update
            hydrate_seeds: Whether to hydrate seed snapshots with the content.
            hydrate_intervals: Whether to hydrate result snapshots with intervals.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """

    @abc.abstractmethod
    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Collection[SnapshotNameVersionLike],
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
