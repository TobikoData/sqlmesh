from __future__ import annotations

import abc
import logging
import typing as t
from datetime import datetime
from functools import wraps

from sqlmesh.core.console import Console
from sqlmesh.core.dialect import schema_
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotNameVersionLike,
    SnapshotTableInfo,
    start_date,
)
from sqlmesh.core.state_sync.base import PromotionResult, StateSync
from sqlmesh.utils.date import TimeLike, now, now_timestamp, to_timestamp
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter

logger = logging.getLogger(__name__)


def cleanup_expired_views(
    adapter: EngineAdapter, environments: t.List[Environment], console: t.Optional[Console] = None
) -> None:
    expired_schema_environments = [
        environment for environment in environments if environment.suffix_target.is_schema
    ]
    expired_table_environments = [
        environment for environment in environments if environment.suffix_target.is_table
    ]
    for expired_catalog, expired_schema in {
        (
            snapshot.qualified_view_name.catalog_for_environment(environment.naming_info),
            snapshot.qualified_view_name.schema_for_environment(
                environment.naming_info, dialect=adapter.dialect
            ),
        )
        for environment in expired_schema_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        schema = schema_(expired_schema, expired_catalog)
        try:
            adapter.drop_schema(
                schema,
                ignore_if_not_exists=True,
                cascade=True,
            )
            if console:
                console.update_cleanup_progress(schema.sql(dialect=adapter.dialect))
        except Exception as e:
            logger.warning("Falied to drop the expired environment schema '%s': %s", schema, e)
    for expired_view in {
        snapshot.qualified_view_name.for_environment(
            environment.naming_info, dialect=adapter.dialect
        )
        for environment in expired_table_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        try:
            adapter.drop_view(expired_view, ignore_if_not_exists=True)
            if console:
                console.update_cleanup_progress(expired_view)
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
    ) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots(snapshot_ids)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self._get_environment(environment)

    @transactional()
    def promote(
        self,
        environment: Environment,
        no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> PromotionResult:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            no_gaps_snapshot_names: A set of snapshot names to check for data gaps. If None,
                all snapshots will be checked. The data gap check ensures that models that are already a
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

        existing_table_infos = (
            {table_info.name: table_info for table_info in existing_environment.promoted_snapshots}
            if existing_environment
            else {}
        )
        table_infos = {table_info.name: table_info for table_info in environment.promoted_snapshots}
        views_that_changed_location: t.Set[SnapshotTableInfo] = set()
        if existing_environment:
            views_that_changed_location = {
                existing_table_info
                for name, existing_table_info in existing_table_infos.items()
                if name in table_infos
                and existing_table_info.qualified_view_name.for_environment(
                    existing_environment.naming_info
                )
                != table_infos[name].qualified_view_name.for_environment(environment.naming_info)
            }
            if environment.previous_plan_id != existing_environment.plan_id:
                raise SQLMeshError(
                    f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                    f"Expected previous plan ID: '{environment.previous_plan_id}', actual previous plan ID: '{existing_environment.plan_id}'. "
                    "Please recreate the plan and try again"
                )
            if no_gaps_snapshot_names != set():
                snapshots = self._get_snapshots(environment.snapshots).values()
                self._ensure_no_gaps(
                    snapshots,
                    existing_environment,
                    no_gaps_snapshot_names,
                )
            demoted_snapshots = set(existing_environment.snapshots) - set(environment.snapshots)
            for demoted_snapshot in self._get_snapshots(demoted_snapshots).values():
                # Update the updated_at attribute.
                self._update_snapshot(demoted_snapshot)

        missing_models = set(existing_table_infos) - {
            snapshot.name for snapshot in environment.promoted_snapshots
        }

        added_table_infos = set(table_infos.values())
        if existing_environment and existing_environment.finalized_ts:
            # Only promote new snapshots.
            added_table_infos -= set(existing_environment.promoted_snapshots)

        self._update_environment(environment)

        removed = {existing_table_infos[name] for name in missing_models}.union(
            views_that_changed_location
        )

        return PromotionResult(
            added=sorted(added_table_infos),
            removed=list(removed),
            removed_environment_naming_info=(
                existing_environment.naming_info if removed and existing_environment else None
            ),
        )

    @transactional()
    def finalize(self, environment: Environment) -> None:
        """Finalize the target environment, indicating that this environment has been
        fully promoted and is ready for use.

        Args:
            environment: The target environment to finalize.
        """
        logger.info("Finalizing environment '%s'", environment.name)

        stored_environment = self._get_environment(environment.name, lock_for_update=True)
        if stored_environment and stored_environment.plan_id != environment.plan_id:
            raise SQLMeshError(
                f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                f"Stored plan ID: '{stored_environment.plan_id}'. Please recreate the plan and try again"
            )

        environment.finalized_ts = now_timestamp()
        self._update_environment(environment)

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

                update_required = False

                if snapshot.unpaused_ts:
                    logger.info("Pausing snapshot %s", snapshot.snapshot_id)
                    snapshot.set_unpaused_ts(None)
                    update_required = True

                if (
                    not snapshot.is_forward_only
                    and target_snapshot.is_forward_only
                    and not snapshot.unrestorable
                ):
                    logger.info("Marking snapshot %s as unrestorable", snapshot.snapshot_id)
                    snapshot.unrestorable = True
                    update_required = True

                if update_required:
                    self._update_snapshot(snapshot)

    def _ensure_no_gaps(
        self,
        target_snapshots: t.Iterable[Snapshot],
        target_environment: Environment,
        snapshot_names: t.Optional[t.Set[str]],
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
                (snapshot_names is None or prev_snapshot.name in snapshot_names)
                and target_snapshot.is_incremental
                and prev_snapshot.is_incremental
                and prev_snapshot.intervals
            ):
                start = to_timestamp(
                    start_date(target_snapshot, target_snapshots_by_name.values(), cache)
                )
                end = prev_snapshot.intervals[-1][1]

                if start < end:
                    missing_intervals = target_snapshot.missing_intervals(
                        start, end, end_bounded=True
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
        hydrate_intervals: bool = True,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots.

        Args:
            snapshot_ids: The collection of IDs of snapshots to fetch
            lock_for_update: Lock the snapshot rows for future update
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
