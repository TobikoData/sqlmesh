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

import contextlib
import logging
import typing as t
from pathlib import Path
from datetime import datetime

from sqlglot import exp

from sqlmesh.core.console import Console, get_console
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment, EnvironmentStatements
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotNameVersion,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
    start_date,
)
from sqlmesh.core.snapshot.definition import (
    Interval,
)
from sqlmesh.core.state_sync.base import (
    PromotionResult,
    StateSync,
    Versions,
)
from sqlmesh.core.state_sync.common import transactional
from sqlmesh.core.state_sync.db.interval import IntervalState
from sqlmesh.core.state_sync.db.environment import EnvironmentState
from sqlmesh.core.state_sync.db.snapshot import SnapshotState
from sqlmesh.core.state_sync.db.version import VersionState
from sqlmesh.core.state_sync.db.migrator import StateMigrator
from sqlmesh.utils.date import TimeLike, to_timestamp, time_like_to_str
from sqlmesh.utils.errors import ConflictingPlanError, SQLMeshError

logger = logging.getLogger(__name__)


T = t.TypeVar("T")


if t.TYPE_CHECKING:
    pass


class EngineAdapterStateSync(StateSync):
    """Manages state of nodes and snapshot with an existing engine adapter.

    This state sync is convenient to use because it requires no additional setup.
    You can reuse the same engine/warehouse that your data is stored in.

    Args:
        engine_adapter: The EngineAdapter to use to store and fetch snapshots.
        schema: The schema to store state metadata in. If None or empty string then no schema is defined
        console: The console to log information to.
        context_path: The context path, used for caching snapshot models.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str],
        console: t.Optional[Console] = None,
        context_path: Path = Path(),
    ):
        self.plan_dags_table = exp.table_("_plan_dags", db=schema)
        self.interval_state = IntervalState(engine_adapter, schema=schema)
        self.environment_state = EnvironmentState(engine_adapter, schema=schema)
        self.snapshot_state = SnapshotState(
            engine_adapter, schema=schema, context_path=context_path
        )
        self.version_state = VersionState(engine_adapter, schema=schema)
        self.migrator = StateMigrator(
            engine_adapter,
            version_state=self.version_state,
            snapshot_state=self.snapshot_state,
            environment_state=self.environment_state,
            interval_state=self.interval_state,
            plan_dags_table=self.plan_dags_table,
            console=console,
        )
        # Make sure that if an empty string is provided that we treat it as None
        self.schema = schema or None
        self.engine_adapter = engine_adapter
        self.console = console or get_console()

    @transactional()
    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Pushes snapshots to the state store, merging them with existing ones.

        This method first finds all existing snapshots in the store and merges them with
        the local snapshots. It will then delete all existing snapshots and then
        insert all the local snapshots. This can be made safer with locks or merge/upsert.

        Args:
            snapshots: The snapshots to push.
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
            logger.error(
                "Snapshots %s already exists. This could be due to a concurrent plan or a hash collision. If this is a hash collision, add a stamp to your model.",
                str(existing),
            )

            for sid in tuple(snapshots_by_id):
                if sid in existing:
                    snapshots_by_id.pop(sid)

        snapshots = snapshots_by_id.values()
        if snapshots:
            self.snapshot_state.push_snapshots(snapshots)

    @transactional()
    def promote(
        self,
        environment: Environment,
        no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
        environment_statements: t.Optional[t.List[EnvironmentStatements]] = None,
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

        existing_environment = self.environment_state.get_environment(
            environment.name, lock_for_update=True
        )

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
            if not existing_environment.expired:
                if environment.previous_plan_id != existing_environment.plan_id:
                    raise ConflictingPlanError(
                        f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                        f"Expected previous plan ID: '{environment.previous_plan_id}', actual previous plan ID: '{existing_environment.plan_id}'. "
                        "Please recreate the plan and try again"
                    )
                if no_gaps_snapshot_names != set():
                    snapshots = self.get_snapshots(environment.snapshots).values()
                    self._ensure_no_gaps(
                        snapshots,
                        existing_environment,
                        no_gaps_snapshot_names,
                    )
            demoted_snapshots = set(existing_environment.snapshots) - set(environment.snapshots)
            # Update the updated_at attribute.
            self.snapshot_state.touch_snapshots(demoted_snapshots)

        missing_models = set(existing_table_infos) - {
            snapshot.name for snapshot in environment.promoted_snapshots
        }

        added_table_infos = set(table_infos.values())
        if (
            existing_environment
            and existing_environment.finalized_ts
            and not existing_environment.expired
        ):
            # Only promote new snapshots.
            added_table_infos -= set(existing_environment.promoted_snapshots)

        self.environment_state.update_environment(environment)

        # If it is an empty list, we want to update the environment statements
        # To reflect there are no statements anymore in this environment
        if environment_statements is not None:
            self.environment_state.update_environment_statements(
                environment.name, environment.plan_id, environment_statements
            )

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
        self.environment_state.finalize(environment)

    @transactional()
    def unpause_snapshots(
        self, snapshots: t.Collection[SnapshotInfoLike], unpaused_dt: TimeLike
    ) -> None:
        self.snapshot_state.unpause_snapshots(snapshots, unpaused_dt, self.interval_state)

    def invalidate_environment(self, name: str) -> None:
        self.environment_state.invalidate_environment(name)

    @transactional()
    def delete_expired_snapshots(
        self, ignore_ttl: bool = False
    ) -> t.List[SnapshotTableCleanupTask]:
        expired_snapshot_ids, cleanup_targets = self.snapshot_state.delete_expired_snapshots(
            self.environment_state.get_environments(), ignore_ttl=ignore_ttl
        )
        self.interval_state.cleanup_intervals(cleanup_targets, expired_snapshot_ids)
        return cleanup_targets

    @transactional()
    def delete_expired_environments(self) -> t.List[Environment]:
        return self.environment_state.delete_expired_environments()

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        self.snapshot_state.delete_snapshots(snapshot_ids)

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        return self.snapshot_state.snapshots_exist(snapshot_ids)

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        return self.snapshot_state.nodes_exist(names, exclude_external)

    def reset(self, default_catalog: t.Optional[str]) -> None:
        """Resets the state store to the state when it was first initialized."""
        for table in (
            self.snapshot_state.snapshots_table,
            self.snapshot_state.auto_restatements_table,
            self.environment_state.environments_table,
            self.interval_state.intervals_table,
            self.plan_dags_table,
            self.version_state.versions_table,
        ):
            self.engine_adapter.drop_table(table)
        self.snapshot_state.clear_cache()
        self.migrate(default_catalog)

    @transactional()
    def update_auto_restatements(
        self, next_auto_restatement_ts: t.Dict[SnapshotNameVersion, t.Optional[int]]
    ) -> None:
        self.snapshot_state.update_auto_restatements(next_auto_restatement_ts)

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        return self.environment_state.get_environment(environment)

    def get_environment_statements(self, environment: str) -> t.List[EnvironmentStatements]:
        return self.environment_state.get_environment_statements(environment)

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return self.environment_state.get_environments()

    def get_environments_summary(self) -> t.Dict[str, int]:
        """Fetches all environment names along with expiry datetime.

        Returns:
            A dict of all environment names along with expiry datetime.
        """
        return self.environment_state.get_environments_summary()

    def get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]],
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches snapshots from the state.

        Args:
            snapshot_ids: The snapshot IDs to fetch.

        Returns:
            A dict of snapshots.
        """
        snapshots = self.snapshot_state.get_snapshots(snapshot_ids)
        intervals = self.interval_state.get_snapshot_intervals(snapshots.values())
        Snapshot.hydrate_with_intervals_by_version(snapshots.values(), intervals)
        return snapshots

    @transactional()
    def add_interval(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        is_dev: bool = False,
    ) -> None:
        super().add_interval(snapshot, start, end, is_dev)

    @transactional()
    def add_snapshots_intervals(self, snapshots_intervals: t.Sequence[SnapshotIntervals]) -> None:
        intervals_to_insert = []
        for snapshot_intervals in snapshots_intervals:
            snapshot_intervals = snapshot_intervals.copy(
                update={
                    "intervals": _remove_partial_intervals(
                        snapshot_intervals.intervals, snapshot_intervals.snapshot_id, is_dev=False
                    ),
                    "dev_intervals": _remove_partial_intervals(
                        snapshot_intervals.dev_intervals,
                        snapshot_intervals.snapshot_id,
                        is_dev=True,
                    ),
                }
            )
            if not snapshot_intervals.is_empty():
                intervals_to_insert.append(snapshot_intervals)
        if intervals_to_insert:
            self.interval_state.add_snapshots_intervals(intervals_to_insert)

    @transactional()
    def remove_intervals(
        self,
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        remove_shared_versions: bool = False,
    ) -> None:
        self.interval_state.remove_intervals(snapshot_intervals, remove_shared_versions)

    @transactional()
    def compact_intervals(self) -> None:
        self.interval_state.compact_intervals()

    def refresh_snapshot_intervals(self, snapshots: t.Collection[Snapshot]) -> t.List[Snapshot]:
        return self.interval_state.refresh_snapshot_intervals(snapshots)

    def max_interval_end_per_model(
        self,
        environment: str,
        models: t.Optional[t.Set[str]] = None,
        ensure_finalized_snapshots: bool = False,
    ) -> t.Dict[str, int]:
        env = self.get_environment(environment)
        if not env:
            return {}

        snapshots = (
            env.snapshots if not ensure_finalized_snapshots else env.finalized_or_current_snapshots
        )
        if models is not None:
            snapshots = [s for s in snapshots if s.name in models]

        if not snapshots:
            return {}

        return self.interval_state.max_interval_end_per_model(snapshots)

    def recycle(self) -> None:
        self.engine_adapter.recycle()

    def close(self) -> None:
        self.engine_adapter.close()

    @transactional()
    def migrate(
        self,
        default_catalog: t.Optional[str],
        skip_backup: bool = False,
        promoted_snapshots_only: bool = True,
    ) -> None:
        """Migrate the state sync to the latest SQLMesh / SQLGlot version."""
        self.migrator.migrate(
            self,
            default_catalog,
            skip_backup=skip_backup,
            promoted_snapshots_only=promoted_snapshots_only,
        )

    @transactional()
    def rollback(self) -> None:
        """Rollback to the previous migration."""
        self.migrator.rollback()

    def state_type(self) -> str:
        return self.engine_adapter.dialect

    def _get_versions(self) -> Versions:
        return self.version_state.get_versions()

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

        prev_snapshots = self.get_snapshots(
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

    @contextlib.contextmanager
    def _transaction(self) -> t.Iterator[None]:
        with self.engine_adapter.transaction():
            yield


def _remove_partial_intervals(
    intervals: t.List[Interval], snapshot_id: t.Optional[SnapshotId], *, is_dev: bool
) -> t.List[Interval]:
    results = []
    for start_ts, end_ts in intervals:
        if start_ts < end_ts:
            logger.info(
                "Adding %s (%s, %s) for snapshot %s",
                "dev interval" if is_dev else "interval",
                time_like_to_str(start_ts),
                time_like_to_str(end_ts),
                snapshot_id,
            )
            results.append((start_ts, end_ts))
        else:
            logger.info(
                "Skipping partial interval (%s, %s) for snapshot %s",
                start_ts,
                end_ts,
                snapshot_id,
            )
    return results
