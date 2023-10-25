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
import typing as t
from collections import defaultdict
from copy import deepcopy

import pandas as pd
from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.audit import ModelAudit
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import ModelKindName, SeedModel
from sqlmesh.core.snapshot import (
    Intervals,
    Node,
    Snapshot,
    SnapshotDataVersion,
    SnapshotFingerprint,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotNameVersionLike,
    fingerprint_from_node,
)
from sqlmesh.core.snapshot.definition import (
    Interval,
    _parents_from_node,
    merge_intervals,
    remove_interval,
)
from sqlmesh.core.state_sync.base import MIGRATIONS, SCHEMA_VERSION, StateSync, Versions
from sqlmesh.core.state_sync.common import CommonStateSyncMixin, transactional
from sqlmesh.utils import major_minor, nullsafe_join, random_id
from sqlmesh.utils.date import TimeLike, now_timestamp, time_like_to_str
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


class EngineAdapterStateSync(CommonStateSyncMixin, StateSync):
    """Manages state of nodes and snapshot with an existing engine adapter.

    This state sync is convenient to use because it requires no additional setup.
    You can reuse the same engine/warehouse that your data is stored in.

    Args:
        engine_adapter: The EngineAdapter to use to store and fetch snapshots.
        schema: The schema to store state metadata in. If None or empty string then no schema is defined
    """

    SNAPSHOT_BATCH_SIZE = 1000

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str],
        console: t.Optional[Console] = None,
    ):
        # Make sure that if an empty string is provided that we treat it as None
        self.schema = schema or None
        self.engine_adapter = engine_adapter
        self.console = console or get_console()
        self.snapshots_table = nullsafe_join(".", self.schema, "_snapshots")
        self.environments_table = nullsafe_join(".", self.schema, "_environments")
        self.seeds_table = nullsafe_join(".", self.schema, "_seeds")
        self.intervals_table = nullsafe_join(".", self.schema, "_intervals")
        self.plan_dags_table = nullsafe_join(".", self.schema, "_plan_dags")
        self.versions_table = nullsafe_join(".", self.schema, "_versions")

        self._snapshot_columns_to_types = {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
            "kind_name": exp.DataType.build("text"),
        }

        self._environment_columns_to_types = {
            "name": exp.DataType.build("text"),
            "snapshots": exp.DataType.build("text"),
            "start_at": exp.DataType.build("text"),
            "end_at": exp.DataType.build("text"),
            "plan_id": exp.DataType.build("text"),
            "previous_plan_id": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
            "finalized_ts": exp.DataType.build("bigint"),
            "promoted_snapshot_ids": exp.DataType.build("text"),
            "suffix_target": exp.DataType.build("text"),
        }

        self._seed_columns_to_types = {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "content": exp.DataType.build("text"),
        }

        self._interval_columns_to_types = {
            "id": exp.DataType.build("text"),
            "created_ts": exp.DataType.build("bigint"),
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "start_ts": exp.DataType.build("bigint"),
            "end_ts": exp.DataType.build("bigint"),
            "is_dev": exp.DataType.build("boolean"),
            "is_removed": exp.DataType.build("boolean"),
            "is_compacted": exp.DataType.build("boolean"),
        }

        self._version_columns_to_types = {
            "schema_version": exp.DataType.build("int"),
            "sqlglot_version": exp.DataType.build("text"),
        }

    @transactional()
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
            self._push_snapshots(snapshots)

    def _push_snapshots(self, snapshots: t.Iterable[Snapshot], overwrite: bool = False) -> None:
        if overwrite:
            snapshots = tuple(snapshots)
            self.delete_snapshots(snapshots)

        seed_contents = []
        snapshots_to_store = []

        for snapshot in snapshots:
            if isinstance(snapshot.node, SeedModel):
                seed_model = t.cast(SeedModel, snapshot.node)
                seed_contents.append(
                    {
                        "name": snapshot.name,
                        "identifier": snapshot.identifier,
                        "content": seed_model.seed.content,
                    }
                )
                snapshot = snapshot.copy(update={"node": seed_model.to_dehydrated()})
            snapshots_to_store.append(snapshot)

        self.engine_adapter.insert_append(
            self.snapshots_table,
            _snapshots_to_df(snapshots_to_store),
            columns_to_types=self._snapshot_columns_to_types,
            contains_json=True,
        )

        if seed_contents:
            self.engine_adapter.insert_append(
                self.seeds_table,
                pd.DataFrame(seed_contents),
                columns_to_types=self._seed_columns_to_types,
                contains_json=True,
            )

    def _update_versions(
        self,
        schema_version: int = SCHEMA_VERSION,
        sqlglot_version: str = SQLGLOT_VERSION,
    ) -> None:
        self.engine_adapter.delete_from(self.versions_table, "TRUE")

        self.engine_adapter.insert_append(
            self.versions_table,
            pd.DataFrame([{"schema_version": schema_version, "sqlglot_version": sqlglot_version}]),
            columns_to_types=self._version_columns_to_types,
        )

    def invalidate_environment(self, name: str) -> None:
        name = name.lower()
        if name == c.PROD:
            raise SQLMeshError("Cannot invalidate the production environment.")

        filter_expr = exp.column("name").eq(name)

        self.engine_adapter.update_table(
            self.environments_table,
            {"expiration_ts": now_timestamp()},
            where=filter_expr,
        )

    def delete_expired_environments(self) -> t.List[Environment]:
        now_ts = now_timestamp()
        filter_expr = exp.LTE(
            this=exp.column("expiration_ts"),
            expression=exp.Literal.number(now_ts),
        )

        rows = self.engine_adapter.fetchall(
            self._environments_query(
                where=filter_expr,
                lock_for_update=True,
            ),
            ignore_unsupported_errors=True,
            quote_identifiers=True,
        )
        environments = [self._environment_from_row(r) for r in rows]

        self.engine_adapter.delete_from(
            self.environments_table,
            where=filter_expr,
        )

        return environments

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        for where in self._snapshot_id_filter(snapshot_ids):
            self.engine_adapter.delete_from(self.snapshots_table, where=where)

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        return {
            SnapshotId(name=name, identifier=identifier)
            for where in self._snapshot_id_filter(snapshot_ids)
            for name, identifier in self.engine_adapter.fetchall(
                exp.select("name", "identifier").from_(self.snapshots_table).where(where),
                quote_identifiers=True,
            )
        }

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
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
        return {name for name, in self.engine_adapter.fetchall(query, quote_identifiers=True)}

    def reset(self) -> None:
        """Resets the state store to the state when it was first initialized."""
        self.engine_adapter.drop_table(self.snapshots_table)
        self.engine_adapter.drop_table(self.environments_table)
        self.engine_adapter.drop_table(self.versions_table)
        self.migrate()

    def _update_environment(self, environment: Environment) -> None:
        self.engine_adapter.delete_from(
            self.environments_table,
            where=exp.EQ(
                this=exp.column("name"),
                expression=exp.Literal.string(environment.name),
            ),
        )

        self.engine_adapter.insert_append(
            self.environments_table,
            _environment_to_df(environment),
            columns_to_types=self._environment_columns_to_types,
            contains_json=True,
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        snapshot.updated_ts = now_timestamp()
        for where in self._snapshot_id_filter([snapshot.snapshot_id]):
            self.engine_adapter.update_table(
                self.snapshots_table,
                {"snapshot": _snapshot_to_json(snapshot)},
                where=where,
                contains_json=True,
            )

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return [
            self._environment_from_row(row)
            for row in self.engine_adapter.fetchall(
                self._environments_query(), ignore_unsupported_errors=True, quote_identifiers=True
            )
        ]

    def _environment_from_row(self, row: t.Tuple[str, ...]) -> Environment:
        return Environment(**{field: row[i] for i, field in enumerate(Environment.all_fields())})

    def _environments_query(
        self,
        where: t.Optional[str | exp.Expression] = None,
        lock_for_update: bool = False,
    ) -> exp.Select:
        query = (
            exp.select(*(exp.to_identifier(field) for field in Environment.all_fields()))
            .from_(self.environments_table)
            .where(where)
        )
        if lock_for_update:
            return query.lock(copy=False)
        return query

    def _get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
        hydrate_seeds: bool = False,
        hydrate_intervals: bool = True,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update
            hydrate_seeds: Whether to hydrate seed snapshots with the content.
            hydrate_intervals: Whether to hydrate result snapshots with intervals.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        snapshots: t.Dict[SnapshotId, Snapshot] = {}
        duplicates: t.Dict[SnapshotId, Snapshot] = {}

        for where in (
            [None] if snapshot_ids is None else self._snapshot_id_filter(snapshot_ids, "snapshots")
        ):
            query = (
                exp.select(exp.column("snapshot", table="snapshots"))
                .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                .where(where)
            )
            if hydrate_seeds:
                query = query.select(exp.column("content", table="seeds")).join(
                    exp.to_table(self.seeds_table).as_("seeds"),
                    on=exp.and_(
                        exp.column("name", table="snapshots").eq(exp.column("name", table="seeds")),
                        exp.column("identifier", table="snapshots").eq(
                            exp.column("identifier", table="seeds")
                        ),
                    ),
                    join_type="left",
                )
            elif lock_for_update:
                query = query.lock(copy=False)

            for row in self.engine_adapter.fetchall(
                query, ignore_unsupported_errors=True, quote_identifiers=True
            ):
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

                if hydrate_seeds and isinstance(snapshot.node, SeedModel) and row[1]:
                    snapshot.node = t.cast(SeedModel, snapshot.node).to_hydrated(row[1])

        if snapshots and hydrate_intervals:
            _, intervals = self._get_snapshot_intervals(snapshots.values())
            Snapshot.hydrate_with_intervals_by_version(snapshots.values(), intervals)

        if duplicates:
            self._push_snapshots(duplicates.values(), overwrite=True)
            logger.error("Found duplicate snapshots in the state store.")

        return snapshots

    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Collection[SnapshotNameVersionLike],
        lock_for_update: bool = False,
    ) -> t.List[Snapshot]:
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

        for where in self._snapshot_name_version_filter(snapshots):
            query = (
                exp.select("snapshot")
                .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                .where(where)
            )
            if lock_for_update:
                query = query.lock(copy=False)

            snapshot_rows.extend(
                self.engine_adapter.fetchall(
                    query, ignore_unsupported_errors=True, quote_identifiers=True
                )
            )

        return [Snapshot(**json.loads(row[0])) for row in snapshot_rows]

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        no_version = Versions(schema_version=0, sqlglot_version="0.0.0")

        if not self.engine_adapter.table_exists(self.versions_table):
            return no_version

        query = exp.select("*").from_(self.versions_table)
        if lock_for_update:
            query.lock(copy=False)
        row = self.engine_adapter.fetchone(query, quote_identifiers=True)
        if not row:
            return no_version
        return Versions(schema_version=row[0], sqlglot_version=row[1])

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
            self._environments_query(
                where=exp.EQ(
                    this=exp.column("name"),
                    expression=exp.Literal.string(environment),
                ),
                lock_for_update=lock_for_update,
            ),
            ignore_unsupported_errors=True,
            quote_identifiers=True,
        )

        if not row:
            return None

        env = self._environment_from_row(row)
        return env

    @transactional()
    def add_interval(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        is_dev: bool = False,
    ) -> None:
        start_ts, end_ts = snapshot.inclusive_exclusive(start, end, strict=False)
        if start_ts >= end_ts:
            logger.info(
                "Skipping partial interval (%s, %s) for snapshot %s",
                start,
                end,
                snapshot.snapshot_id,
            )
            return

        logger.info(
            "Adding interval (%s, %s) for snapshot %s", start_ts, end_ts, snapshot.snapshot_id
        )

        is_dev = snapshot.is_temporary_table(is_dev)
        self.engine_adapter.insert_append(
            self.intervals_table,
            _intervals_to_df([(snapshot, (start_ts, end_ts))], is_dev, False),
            columns_to_types=self._interval_columns_to_types,
        )

    @transactional()
    def remove_interval(
        self,
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        execution_time: t.Optional[TimeLike] = None,
        remove_shared_versions: bool = False,
    ) -> None:
        if remove_shared_versions:
            name_version_mapping = {
                s.name_version: (s, interval) for s, interval in snapshot_intervals
            }
            all_snapshots = self._get_snapshots_with_same_version(
                [s[0] for s in snapshot_intervals]
            )
            snapshot_intervals = [
                (snapshot, name_version_mapping[snapshot.name_version][1])
                for snapshot in all_snapshots
            ]

        if logger.isEnabledFor(logging.INFO):
            snapshot_ids = ", ".join(str(s.snapshot_id) for s, _ in snapshot_intervals)
            logger.info("Removing interval for snapshots: %s", snapshot_ids)

        for is_dev in (True, False):
            self.engine_adapter.insert_append(
                self.intervals_table,
                _intervals_to_df(snapshot_intervals, is_dev=is_dev, is_removed=True),
                columns_to_types=self._interval_columns_to_types,
            )

    @transactional()
    def compact_intervals(self) -> None:
        interval_ids, snapshot_intervals = self._get_snapshot_intervals(uncompacted_only=True)

        logger.info(
            "Compacting %s intervals for %s snapshots", len(interval_ids), len(snapshot_intervals)
        )

        self._push_snapshot_intervals(snapshot_intervals)

        if interval_ids:
            self.engine_adapter.delete_from(
                self.intervals_table, exp.column("id").isin(*interval_ids)
            )

    def refresh_snapshot_intervals(self, snapshots: t.Collection[Snapshot]) -> t.List[Snapshot]:
        if not snapshots:
            return []

        _, intervals = self._get_snapshot_intervals(snapshots)
        for s in snapshots:
            s.intervals = []
            s.dev_intervals = []
        return Snapshot.hydrate_with_intervals_by_version(snapshots, intervals)

    def max_interval_end_for_environment(self, environment: str) -> t.Optional[int]:
        env = self._get_environment(environment)
        if not env:
            return None

        max_end = None

        for where in self._snapshot_name_version_filter(env.snapshots, "intervals"):
            end = self.engine_adapter.fetchone(
                exp.select(exp.func("MAX", exp.to_column("end_ts")))
                .from_(exp.to_table(self.intervals_table).as_("intervals"))
                .where(where, copy=False)
                .where(exp.to_column("is_dev").not_(), copy=False),
                quote_identifiers=True,
            )[0]

            if max_end is None:
                max_end = end
            elif end is not None:
                max_end = max(max_end, end)

        return max_end

    def recycle(self) -> None:
        self.engine_adapter.recycle()

    def close(self) -> None:
        self.engine_adapter.close()

    def _get_snapshot_intervals(
        self,
        snapshots: t.Optional[t.Collection[SnapshotNameVersionLike]] = None,
        uncompacted_only: bool = False,
    ) -> t.Tuple[t.Set[str], t.List[SnapshotIntervals]]:
        query = (
            exp.select(
                "id",
                exp.column("name", table="intervals"),
                exp.column("identifier", table="intervals"),
                "version",
                "start_ts",
                "end_ts",
                "is_dev",
                "is_removed",
            )
            .from_(exp.to_table(self.intervals_table).as_("intervals"))
            .order_by(
                exp.column("name", table="intervals"),
                exp.column("identifier", table="intervals"),
                "created_ts",
                "is_removed",
            )
        )

        if uncompacted_only:
            query.join(
                exp.select("name", "identifier")
                .from_(exp.to_table(self.intervals_table).as_("intervals"))
                .where(exp.column("is_compacted").not_())
                .distinct()
                .subquery(alias="uncompacted"),
                on=exp.and_(
                    exp.column("name", table="intervals").eq(
                        exp.column("name", table="uncompacted")
                    ),
                    exp.column("identifier", table="intervals").eq(
                        exp.column("identifier", table="uncompacted")
                    ),
                ),
                copy=False,
            )

        if not snapshots and snapshots is not None:
            return (set(), [])

        interval_ids: t.Set[str] = set()
        snapshot_intervals = []

        for where in (
            self._snapshot_name_version_filter(snapshots, "intervals") if snapshots else [None]
        ):
            rows = self.engine_adapter.fetchall(query.where(where), quote_identifiers=True)
            interval_ids.update(row[0] for row in rows)

            intervals: t.Dict[t.Tuple[str, str, str], Intervals] = defaultdict(list)
            dev_intervals: t.Dict[t.Tuple[str, str, str], Intervals] = defaultdict(list)
            for row in rows:
                _, name, identifier, version, start, end, is_dev, is_removed = row
                intervals_key = (name, identifier, version)
                target_intervals = intervals if not is_dev else dev_intervals
                if is_removed:
                    target_intervals[intervals_key] = remove_interval(
                        target_intervals[intervals_key], start, end
                    )
                else:
                    target_intervals[intervals_key] = merge_intervals(
                        [*target_intervals[intervals_key], (start, end)]
                    )

            for name, identifier, version in {**intervals, **dev_intervals}:
                snapshot_intervals.append(
                    SnapshotIntervals(
                        name=name,
                        identifier=identifier,
                        version=version,
                        intervals=intervals.get((name, identifier, version), []),
                        dev_intervals=dev_intervals.get((name, identifier, version), []),
                    )
                )

        return interval_ids, snapshot_intervals

    def _push_snapshot_intervals(
        self, snapshots: t.Iterable[t.Union[Snapshot, SnapshotIntervals]]
    ) -> None:
        new_intervals = []
        for snapshot in snapshots:
            logger.info("Pushing intervals for snapshot %s", snapshot.snapshot_id)
            for start_ts, end_ts in snapshot.intervals:
                new_intervals.append(
                    _interval_to_df(snapshot, start_ts, end_ts, is_dev=False, is_compacted=True)
                )
            for start_ts, end_ts in snapshot.dev_intervals:
                new_intervals.append(
                    _interval_to_df(snapshot, start_ts, end_ts, is_dev=True, is_compacted=True)
                )

        if new_intervals:
            self.engine_adapter.insert_append(
                self.intervals_table,
                pd.DataFrame(new_intervals),
                columns_to_types=self._interval_columns_to_types,
            )

    def _restore_table(
        self,
        table_name: str,
        backup_table_name: str,
    ) -> None:
        self.engine_adapter.drop_table(table_name)
        self.engine_adapter.rename_table(
            old_table_name=backup_table_name,
            new_table_name=table_name,
        )

    @transactional()
    def migrate(self, skip_backup: bool = False) -> None:
        """Migrate the state sync to the latest SQLMesh / SQLGlot version."""
        versions = self.get_versions(validate=False)
        migrations = MIGRATIONS[versions.schema_version :]

        if not migrations and major_minor(SQLGLOT_VERSION) == versions.minor_sqlglot_version:
            return

        if not skip_backup:
            self._backup_state()

        try:
            for migration in migrations:
                logger.info(f"Applying migration {migration}")
                migration.migrate(self)

            self._migrate_rows()
            self._update_versions()
        except Exception as e:
            if skip_backup:
                logger.error("Backup was skipped so no rollback was attempted.")
            else:
                self.rollback()

            self.console.stop_migration_progress(success=False)
            raise SQLMeshError("SQLMesh migration failed.") from e

        self.console.stop_migration_progress()

    @transactional()
    def rollback(self) -> None:
        """Rollback to the previous migration."""
        logger.info("Starting migration rollback.")
        tables = (self.snapshots_table, self.environments_table, self.versions_table)
        optional_tables = (self.seeds_table, self.intervals_table, self.plan_dags_table)
        versions = self.get_versions(validate=False)
        if versions.schema_version == 0:
            # Clean up state tables
            for table in tables + optional_tables:
                self.engine_adapter.drop_table(table)
        else:
            if not all(self.engine_adapter.table_exists(f"{table}_backup") for table in tables):
                raise SQLMeshError("There are no prior migrations to roll back to.")
            for table in tables:
                self._restore_table(table, _backup_table_name(table))

            for optional_table in optional_tables:
                if self.engine_adapter.table_exists(_backup_table_name(optional_table)):
                    self._restore_table(optional_table, _backup_table_name(optional_table))

        logger.info("Migration rollback successful.")

    def _backup_state(self) -> None:
        for table in (
            self.snapshots_table,
            self.environments_table,
            self.versions_table,
            self.seeds_table,
            self.intervals_table,
            self.plan_dags_table,
        ):
            if self.engine_adapter.table_exists(table):
                with self.engine_adapter.transaction():
                    backup_name = _backup_table_name(table)
                    self.engine_adapter.drop_table(backup_name)
                    self.engine_adapter.ctas(
                        backup_name, exp.select("*").from_(table), exists=False
                    )

    def _migrate_rows(self) -> None:
        all_snapshots = self._get_snapshots(
            lock_for_update=True, hydrate_seeds=True, hydrate_intervals=False
        )
        environments = self.get_environments()

        snapshot_mapping = {}

        if all_snapshots:
            self.console.start_migration_progress(len(all_snapshots))

        for snapshot in all_snapshots.values():
            seen = set()
            queue = {snapshot.snapshot_id}
            node = snapshot.node
            nodes: t.Dict[str, Node] = {}
            audits: t.Dict[str, ModelAudit] = {}

            while queue:
                snapshot_id = queue.pop()

                if snapshot_id in seen:
                    continue

                seen.add(snapshot_id)

                s = all_snapshots.get(snapshot_id)

                if not s:
                    continue

                queue.update(s.parents)
                nodes[s.name] = s.node
                for audit in s.audits:
                    audits[audit.name] = audit

            new_snapshot = deepcopy(snapshot)

            fingerprint_cache: t.Dict[str, SnapshotFingerprint] = {}

            try:
                new_snapshot.fingerprint = fingerprint_from_node(
                    node,
                    nodes=nodes,
                    audits=audits,
                )
                new_snapshot.parents = tuple(
                    SnapshotId(
                        name=name,
                        identifier=fingerprint_from_node(
                            nodes[name],
                            nodes=nodes,
                            audits=audits,
                            cache=fingerprint_cache,
                        ).to_identifier(),
                    )
                    for name in _parents_from_node(node, nodes)
                )
            except Exception:
                logger.exception("Could not compute fingerprint for %s", snapshot.snapshot_id)
                continue

            new_snapshot.previous_versions = snapshot.all_versions
            new_snapshot.migrated = True
            if not new_snapshot.temp_version:
                new_snapshot.temp_version = snapshot.fingerprint.to_version()

            self.console.update_migration_progress(1)

            if new_snapshot.fingerprint == snapshot.fingerprint:
                logger.debug(f"{new_snapshot.snapshot_id} is unchanged.")
                continue
            if new_snapshot.snapshot_id in all_snapshots:
                logger.debug(f"{new_snapshot.snapshot_id} exists.")
                continue

            snapshot_mapping[snapshot.snapshot_id] = new_snapshot
            logger.debug(f"{snapshot.snapshot_id} mapped to {new_snapshot.snapshot_id}.")

        if not snapshot_mapping:
            logger.debug("No changes to snapshots detected.")
            return

        def map_data_versions(
            name: str, versions: t.Sequence[SnapshotDataVersion]
        ) -> t.Tuple[SnapshotDataVersion, ...]:
            version_ids = ((version.snapshot_id(name), version) for version in versions)

            return tuple(
                snapshot_mapping[version_id].data_version
                if version_id in snapshot_mapping
                else version
                for version_id, version in version_ids
            )

        for from_snapshot_id, to_snapshot in snapshot_mapping.items():
            from_snapshot = all_snapshots[from_snapshot_id]
            to_snapshot.previous_versions = map_data_versions(
                from_snapshot.name, from_snapshot.previous_versions
            )
            to_snapshot.indirect_versions = {
                name: map_data_versions(name, versions)
                for name, versions in from_snapshot.indirect_versions.items()
            }

        new_snapshots = set(snapshot_mapping.values())
        self._push_snapshots(new_snapshots, overwrite=True)

        updated_prod_environment: t.Optional[Environment] = None
        updated_environments = []
        for environment in environments:
            snapshots = [
                snapshot_mapping[info.snapshot_id].table_info
                if info.snapshot_id in snapshot_mapping
                else info
                for info in environment.snapshots
            ]

            if snapshots != environment.snapshots:
                environment.snapshots = snapshots
                updated_environments.append(environment)
                if environment.name == c.PROD:
                    updated_prod_environment = environment

        for environment in environments:
            self._update_environment(environment)

        if updated_prod_environment:
            self.unpause_snapshots(updated_prod_environment.snapshots, now_timestamp())

    def _snapshot_id_filter(
        self, snapshot_ids: t.Iterable[SnapshotIdLike], alias: t.Optional[str] = None
    ) -> t.Iterator[exp.Condition]:
        name_identifiers = sorted(
            {(snapshot_id.name, snapshot_id.identifier) for snapshot_id in snapshot_ids}
        )

        batches = [
            name_identifiers[i : i + self.SNAPSHOT_BATCH_SIZE]
            for i in range(0, len(name_identifiers), self.SNAPSHOT_BATCH_SIZE)
        ]

        if not name_identifiers:
            yield exp.false()
        elif self.engine_adapter.SUPPORTS_TUPLE_IN:
            for identifiers in batches:
                yield t.cast(
                    exp.Tuple,
                    exp.convert(
                        (
                            exp.column("name", table=alias),
                            exp.column("identifier", table=alias),
                        )
                    ),
                ).isin(*identifiers)
        else:
            for identifiers in batches:
                yield exp.or_(
                    *[
                        exp.and_(
                            exp.column("name", table=alias).eq(name),
                            exp.column("identifier", table=alias).eq(identifier),
                        )
                        for name, identifier in identifiers
                    ]
                )

    def _snapshot_name_version_filter(
        self, snapshot_name_versions: t.Iterable[SnapshotNameVersionLike], alias: str = "snapshots"
    ) -> t.Iterator[exp.Condition]:
        name_versions = sorted({(s.name, s.version) for s in snapshot_name_versions})

        batches = [
            name_versions[i : i + self.SNAPSHOT_BATCH_SIZE]
            for i in range(0, len(name_versions), self.SNAPSHOT_BATCH_SIZE)
        ]

        if not name_versions:
            return exp.false()
        elif self.engine_adapter.SUPPORTS_TUPLE_IN:
            for versions in batches:
                yield t.cast(
                    exp.Tuple,
                    exp.convert(
                        (
                            exp.column("name", table=alias),
                            exp.column("version", table=alias),
                        )
                    ),
                ).isin(*versions)
        else:
            for versions in batches:
                yield exp.or_(
                    *[
                        exp.and_(
                            exp.column("name", table=alias).eq(name),
                            exp.column("version", table=alias).eq(version),
                        )
                        for name, version in versions
                    ]
                )

    @contextlib.contextmanager
    def _transaction(self) -> t.Iterator[None]:
        with self.engine_adapter.transaction():
            yield


def _intervals_to_df(
    snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
    is_dev: bool,
    is_removed: bool,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _interval_to_df(
                s,
                *interval,
                is_dev=is_dev,
                is_removed=is_removed,
            )
            for s, interval in snapshot_intervals
        ]
    )


def _interval_to_df(
    snapshot: t.Union[SnapshotInfoLike, SnapshotIntervals],
    start_ts: int,
    end_ts: int,
    is_dev: bool = False,
    is_removed: bool = False,
    is_compacted: bool = False,
) -> t.Dict[str, t.Any]:
    return {
        "id": random_id(),
        "created_ts": now_timestamp(),
        "name": snapshot.name,
        "identifier": snapshot.identifier,
        "version": snapshot.version,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "is_dev": is_dev,
        "is_removed": is_removed,
        "is_compacted": is_compacted,
    }


def _snapshots_to_df(snapshots: t.Iterable[Snapshot]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": snapshot.name,
                "identifier": snapshot.identifier,
                "version": snapshot.version,
                "snapshot": _snapshot_to_json(snapshot),
                "kind_name": snapshot.model_kind_name.value if snapshot.model_kind_name else None,
            }
            for snapshot in snapshots
        ]
    )


def _environment_to_df(environment: Environment) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": environment.name,
                "snapshots": json.dumps([snapshot.dict() for snapshot in environment.snapshots]),
                "start_at": time_like_to_str(environment.start_at),
                "end_at": time_like_to_str(environment.end_at) if environment.end_at else None,
                "plan_id": environment.plan_id,
                "previous_plan_id": environment.previous_plan_id,
                "expiration_ts": environment.expiration_ts,
                "finalized_ts": environment.finalized_ts,
                "promoted_snapshot_ids": json.dumps(
                    [s.dict() for s in environment.promoted_snapshot_ids]
                )
                if environment.promoted_snapshot_ids is not None
                else None,
                "suffix_target": environment.suffix_target.value,
            }
        ]
    )


def _backup_table_name(table_name: str) -> str:
    return f"{table_name}_backup"


def _snapshot_to_json(snapshot: Snapshot) -> str:
    return snapshot.json(exclude={"intervals", "dev_intervals"})
