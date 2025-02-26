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
import json
import logging
import time
import typing as t
from copy import deepcopy
from pathlib import Path
from datetime import datetime

from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot import exp

from sqlmesh.core import analytics
from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Node,
    Snapshot,
    SnapshotFingerprint,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotNameVersion,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
    fingerprint_from_node,
    start_date,
)
from sqlmesh.core.snapshot.definition import (
    Interval,
    _parents_from_node,
)
from sqlmesh.core.state_sync.base import (
    MIGRATIONS,
    PromotionResult,
    StateSync,
    Versions,
)
from sqlmesh.core.state_sync.common import transactional
from sqlmesh.core.state_sync.engine_adapter.interval import IntervalState
from sqlmesh.core.state_sync.engine_adapter.environment import EnvironmentState
from sqlmesh.core.state_sync.engine_adapter.snapshot import SnapshotState
from sqlmesh.core.state_sync.engine_adapter.version import VersionState
from sqlmesh.core.state_sync.engine_adapter.utils import snapshot_id_filter, SQLMESH_VERSION
from sqlmesh.utils import major_minor
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now_timestamp, to_timestamp
from sqlmesh.utils.errors import ConflictingPlanError, SQLMeshError

logger = logging.getLogger(__name__)


T = t.TypeVar("T")


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


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

    SNAPSHOT_BATCH_SIZE = 1000
    SNAPSHOT_MIGRATION_BATCH_SIZE = 500

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str],
        console: t.Optional[Console] = None,
        context_path: Path = Path(),
    ):
        self.interval_state = IntervalState(engine_adapter, schema=schema)
        self.environment_state = EnvironmentState(engine_adapter, schema=schema)
        self.snapshot_state = SnapshotState(
            engine_adapter, schema=schema, context_path=context_path
        )
        self.version_state = VersionState(engine_adapter, schema=schema)
        # Make sure that if an empty string is provided that we treat it as None
        self.schema = schema or None
        self.engine_adapter = engine_adapter
        self.console = console or get_console()
        self.plan_dags_table = exp.table_("_plan_dags", db=self.schema)

    def _fetchone(self, query: t.Union[exp.Expression, str]) -> t.Optional[t.Tuple]:
        return self.engine_adapter.fetchone(
            query, ignore_unsupported_errors=True, quote_identifiers=True
        )

    def _fetchall(self, query: t.Union[exp.Expression, str]) -> t.List[t.Tuple]:
        return self.engine_adapter.fetchall(
            query, ignore_unsupported_errors=True, quote_identifiers=True
        )

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
            self._push_snapshots(snapshots)

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
        self.interval_state.add_snapshots_intervals(snapshots_intervals)

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

    def _get_versions(self) -> Versions:
        return self.version_state.get_versions()

    def _restore_table(
        self,
        table_name: TableName,
        backup_table_name: TableName,
    ) -> None:
        self.engine_adapter.drop_table(table_name)
        self.engine_adapter.rename_table(
            old_table_name=backup_table_name,
            new_table_name=table_name,
        )

    @transactional()
    def migrate(
        self,
        default_catalog: t.Optional[str],
        skip_backup: bool = False,
        promoted_snapshots_only: bool = True,
    ) -> None:
        """Migrate the state sync to the latest SQLMesh / SQLGlot version."""
        versions = self.get_versions(validate=False)

        migration_start_ts = time.perf_counter()

        try:
            migrate_rows = self._apply_migrations(default_catalog, skip_backup)

            if not migrate_rows and major_minor(SQLMESH_VERSION) == versions.minor_sqlmesh_version:
                return

            if migrate_rows:
                self._migrate_rows(promoted_snapshots_only)
                # Cleanup plan DAGs since we currently don't migrate snapshot records that are in there.
                self.engine_adapter.delete_from(self.plan_dags_table, "TRUE")
            self.version_state.update_versions()

            analytics.collector.on_migration_end(
                from_sqlmesh_version=versions.sqlmesh_version,
                state_sync_type=self.state_type(),
                migration_time_sec=time.perf_counter() - migration_start_ts,
            )
        except Exception as e:
            if skip_backup:
                logger.error("Backup was skipped so no rollback was attempted.")
            else:
                self.rollback()

            analytics.collector.on_migration_end(
                from_sqlmesh_version=versions.sqlmesh_version,
                state_sync_type=self.state_type(),
                migration_time_sec=time.perf_counter() - migration_start_ts,
                error=e,
            )

            self.console.log_migration_status(success=False)
            raise SQLMeshError("SQLMesh migration failed.") from e

        self.console.log_migration_status()

    @transactional()
    def rollback(self) -> None:
        """Rollback to the previous migration."""
        logger.info("Starting migration rollback.")
        tables = (
            self.snapshot_state.snapshots_table,
            self.environment_state.environments_table,
            self.version_state.versions_table,
        )
        optional_tables = (
            self.interval_state.intervals_table,
            self.plan_dags_table,
            self.snapshot_state.auto_restatements_table,
        )
        versions = self.get_versions(validate=False)
        if versions.schema_version == 0:
            # Clean up state tables
            for table in tables + optional_tables:
                self.engine_adapter.drop_table(table)
        else:
            if not all(
                self.engine_adapter.table_exists(_backup_table_name(table)) for table in tables
            ):
                raise SQLMeshError("There are no prior migrations to roll back to.")
            for table in tables:
                self._restore_table(table, _backup_table_name(table))

            for optional_table in optional_tables:
                if self.engine_adapter.table_exists(_backup_table_name(optional_table)):
                    self._restore_table(optional_table, _backup_table_name(optional_table))

        logger.info("Migration rollback successful.")

    def state_type(self) -> str:
        return self.engine_adapter.dialect

    @transactional()
    def _backup_state(self) -> None:
        for table in (
            self.snapshot_state.snapshots_table,
            self.environment_state.environments_table,
            self.version_state.versions_table,
            self.interval_state.intervals_table,
            self.plan_dags_table,
            self.snapshot_state.auto_restatements_table,
        ):
            if self.engine_adapter.table_exists(table):
                backup_name = _backup_table_name(table)
                self.engine_adapter.drop_table(backup_name)
                self.engine_adapter.create_table_like(backup_name, table)
                self.engine_adapter.insert_append(backup_name, exp.select("*").from_(table))

    def _apply_migrations(
        self,
        default_catalog: t.Optional[str],
        skip_backup: bool,
    ) -> bool:
        versions = self.get_versions(validate=False)
        migrations = MIGRATIONS[versions.schema_version :]
        should_backup = any(
            [
                migrations,
                major_minor(SQLGLOT_VERSION) != versions.minor_sqlglot_version,
                major_minor(SQLMESH_VERSION) != versions.minor_sqlmesh_version,
            ]
        )
        if not skip_backup and should_backup:
            self._backup_state()

        snapshot_count_before = self.snapshot_state.count() if versions.schema_version else None

        for migration in migrations:
            logger.info(f"Applying migration {migration}")
            migration.migrate(self, default_catalog=default_catalog)

        snapshot_count_after = self.snapshot_state.count()

        if snapshot_count_before is not None and snapshot_count_before != snapshot_count_after:
            scripts = f"{versions.schema_version} - {versions.schema_version + len(migrations)}"
            raise SQLMeshError(
                f"Number of snapshots before ({snapshot_count_before}) and after "
                f"({snapshot_count_after}) applying migration scripts {scripts} does not match. "
                "Please file an issue issue at https://github.com/TobikoData/sqlmesh/issues/new."
            )

        migrate_snapshots_and_environments = (
            bool(migrations) or major_minor(SQLGLOT_VERSION) != versions.minor_sqlglot_version
        )
        return migrate_snapshots_and_environments

    def _migrate_rows(self, promoted_snapshots_only: bool) -> None:
        logger.info("Fetching environments")
        environments = self.get_environments()
        # Only migrate snapshots that are part of at least one environment.
        snapshots_to_migrate = (
            {s.snapshot_id for e in environments for s in e.snapshots}
            if promoted_snapshots_only
            else None
        )
        snapshot_mapping = self._migrate_snapshot_rows(snapshots_to_migrate)
        if not snapshot_mapping:
            logger.info("No changes to snapshots detected")
            return
        self._migrate_environment_rows(environments, snapshot_mapping)

    def _migrate_snapshot_rows(
        self, snapshots: t.Optional[t.Set[SnapshotId]]
    ) -> t.Dict[SnapshotId, SnapshotTableInfo]:
        logger.info("Migrating snapshot rows...")
        raw_snapshots = {
            SnapshotId(name=name, identifier=identifier): {
                **json.loads(raw_snapshot),
                "updated_ts": updated_ts,
                "unpaused_ts": unpaused_ts,
                "unrestorable": unrestorable,
            }
            for where in (self._snapshot_id_filter(snapshots) if snapshots is not None else [None])
            for name, identifier, raw_snapshot, updated_ts, unpaused_ts, unrestorable in self._fetchall(
                exp.select(
                    "name", "identifier", "snapshot", "updated_ts", "unpaused_ts", "unrestorable"
                )
                .from_(self.snapshot_state.snapshots_table)
                .where(where)
                .lock()
            )
        }
        if not raw_snapshots:
            return {}

        dag: DAG[SnapshotId] = DAG()
        for snapshot_id, raw_snapshot in raw_snapshots.items():
            parent_ids = [SnapshotId.parse_obj(p_id) for p_id in raw_snapshot.get("parents", [])]
            dag.add(snapshot_id, [p_id for p_id in parent_ids if p_id in raw_snapshots])

        reversed_dag_raw = dag.reversed.graph

        self.console.start_snapshot_migration_progress(len(raw_snapshots))

        parsed_snapshots = LazilyParsedSnapshots(raw_snapshots)
        all_snapshot_mapping: t.Dict[SnapshotId, SnapshotTableInfo] = {}
        snapshot_id_mapping: t.Dict[SnapshotId, SnapshotId] = {}
        new_snapshots: t.Dict[SnapshotId, Snapshot] = {}
        visited: t.Set[SnapshotId] = set()

        def _push_new_snapshots() -> None:
            all_snapshot_mapping.update(
                {
                    from_id: new_snapshots[to_id].table_info
                    for from_id, to_id in snapshot_id_mapping.items()
                }
            )

            existing_new_snapshots = self.snapshots_exist(new_snapshots)
            new_snapshots_to_push = [
                s for s in new_snapshots.values() if s.snapshot_id not in existing_new_snapshots
            ]
            if new_snapshots_to_push:
                logger.info("Pushing %s migrated snapshots", len(new_snapshots_to_push))
                self._push_snapshots(new_snapshots_to_push)
            new_snapshots.clear()
            snapshot_id_mapping.clear()

        def _visit(
            snapshot_id: SnapshotId, fingerprint_cache: t.Dict[str, SnapshotFingerprint]
        ) -> None:
            if snapshot_id in visited or snapshot_id not in raw_snapshots:
                return
            visited.add(snapshot_id)

            snapshot = parsed_snapshots[snapshot_id]
            node = snapshot.node

            node_seen = set()
            node_queue = {snapshot_id}
            nodes: t.Dict[str, Node] = {}
            while node_queue:
                next_snapshot_id = node_queue.pop()
                next_snapshot = parsed_snapshots.get(next_snapshot_id)

                if next_snapshot_id in node_seen or not next_snapshot:
                    continue

                node_seen.add(next_snapshot_id)
                node_queue.update(next_snapshot.parents)
                nodes[next_snapshot.name] = next_snapshot.node

            new_snapshot = deepcopy(snapshot)
            try:
                new_snapshot.fingerprint = fingerprint_from_node(
                    node,
                    nodes=nodes,
                    cache=fingerprint_cache,
                )
                new_snapshot.parents = tuple(
                    SnapshotId(
                        name=parent_node.fqn,
                        identifier=fingerprint_from_node(
                            parent_node,
                            nodes=nodes,
                            cache=fingerprint_cache,
                        ).to_identifier(),
                    )
                    for parent_node in _parents_from_node(node, nodes).values()
                )
            except Exception:
                logger.exception("Could not compute fingerprint for %s", snapshot.snapshot_id)
                return

            # Reset the effective_from date for the new snapshot to avoid unexpected backfills.
            new_snapshot.effective_from = None
            new_snapshot.previous_versions = snapshot.all_versions
            new_snapshot.migrated = True
            if not new_snapshot.dev_version_:
                new_snapshot.dev_version_ = snapshot.dev_version

            self.console.update_snapshot_migration_progress(1)

            # Visit children and evict them from the parsed_snapshots cache after.
            for child in reversed_dag_raw.get(snapshot_id, []):
                # Make sure to copy the fingerprint cache to avoid sharing it between different child snapshots with the same name.
                _visit(child, fingerprint_cache.copy())
                parsed_snapshots.evict(child)

            if new_snapshot.fingerprint == snapshot.fingerprint:
                logger.debug(f"{new_snapshot.snapshot_id} is unchanged.")
                return

            new_snapshot_id = new_snapshot.snapshot_id

            if new_snapshot_id in raw_snapshots:
                # Mapped to an existing snapshot.
                new_snapshots[new_snapshot_id] = parsed_snapshots[new_snapshot_id]
                logger.debug("Migrated snapshot %s already exists", new_snapshot_id)
            elif (
                new_snapshot_id not in new_snapshots
                or new_snapshot.updated_ts > new_snapshots[new_snapshot_id].updated_ts
            ):
                new_snapshots[new_snapshot_id] = new_snapshot

            snapshot_id_mapping[snapshot.snapshot_id] = new_snapshot_id
            logger.debug("%s mapped to %s", snapshot.snapshot_id, new_snapshot_id)

            if len(new_snapshots) >= self.SNAPSHOT_MIGRATION_BATCH_SIZE:
                _push_new_snapshots()

        for root_snapshot_id in dag.roots:
            _visit(root_snapshot_id, {})

        if new_snapshots:
            _push_new_snapshots()

        self.console.stop_snapshot_migration_progress()
        return all_snapshot_mapping

    def _migrate_environment_rows(
        self,
        environments: t.List[Environment],
        snapshot_mapping: t.Dict[SnapshotId, SnapshotTableInfo],
    ) -> None:
        logger.info("Migrating environment rows...")

        updated_prod_environment: t.Optional[Environment] = None
        updated_environments = []
        for environment in environments:
            snapshots = [
                (
                    snapshot_mapping[info.snapshot_id]
                    if info.snapshot_id in snapshot_mapping
                    else info
                )
                for info in environment.snapshots
            ]

            if snapshots != environment.snapshots:
                environment.snapshots_ = snapshots
                updated_environments.append(environment)
                if environment.name == c.PROD:
                    updated_prod_environment = environment
        self.console.start_env_migration_progress(len(updated_environments))

        for environment in updated_environments:
            self.environment_state.update_environment(environment)
            self.console.update_env_migration_progress(1)

        if updated_prod_environment:
            try:
                self.unpause_snapshots(updated_prod_environment.snapshots, now_timestamp())
            except Exception:
                logger.warning("Failed to unpause migrated snapshots", exc_info=True)

        self.console.stop_env_migration_progress()

    def _snapshot_id_filter(
        self,
        snapshot_ids: t.Iterable[SnapshotIdLike],
        alias: t.Optional[str] = None,
    ) -> t.Iterator[exp.Condition]:
        yield from snapshot_id_filter(
            self.engine_adapter,
            snapshot_ids,
            alias=alias,
            batch_size=self.SNAPSHOT_BATCH_SIZE,
        )

    def _push_snapshots(self, snapshots: t.Iterable[Snapshot], overwrite: bool = False) -> None:
        self.snapshot_state.push_snapshots(snapshots, overwrite=overwrite)

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


def _backup_table_name(table_name: TableName) -> exp.Table:
    table = exp.to_table(table_name).copy()
    table.set("this", exp.to_identifier(table.name + "_backup"))
    return table


class LazilyParsedSnapshots:
    def __init__(self, raw_snapshots: t.Dict[SnapshotId, t.Dict[str, t.Any]]):
        self._raw_snapshots = raw_snapshots
        self._parsed_snapshots: t.Dict[SnapshotId, t.Optional[Snapshot]] = {}

    def get(self, snapshot_id: SnapshotId) -> t.Optional[Snapshot]:
        if snapshot_id not in self._parsed_snapshots:
            raw_snapshot = self._raw_snapshots.get(snapshot_id)
            if raw_snapshot:
                self._parsed_snapshots[snapshot_id] = Snapshot.parse_obj(raw_snapshot)
            else:
                self._parsed_snapshots[snapshot_id] = None
        return self._parsed_snapshots[snapshot_id]

    def evict(self, snapshot_id: SnapshotId) -> None:
        self._parsed_snapshots.pop(snapshot_id, None)

    def __getitem__(self, snapshot_id: SnapshotId) -> Snapshot:
        snapshot = self.get(snapshot_id)
        if snapshot is None:
            raise KeyError(snapshot_id)
        return snapshot
