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
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

import pandas as pd
from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core import analytics
from sqlmesh.core import constants as c
from sqlmesh.core.audit import ModelAudit
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import ModelCache, ModelKindName, SeedModel
from sqlmesh.core.snapshot import (
    Intervals,
    Node,
    Snapshot,
    SnapshotFingerprint,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotNameVersion,
    SnapshotNameVersionLike,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
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
from sqlmesh.utils import major_minor, random_id, unique
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now_timestamp, time_like_to_str
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import parse_obj_as

logger = logging.getLogger(__name__)


T = t.TypeVar("T")


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName

try:
    # We can't import directly from the root package due to circular dependency
    from sqlmesh._version import __version__ as SQLMESH_VERSION  # type: ignore
except ImportError:
    logger.error(
        'Unable to set __version__, run "pip install -e ." or "python setup.py develop" first.'
    )


class EngineAdapterStateSync(CommonStateSyncMixin, StateSync):
    """Manages state of nodes and snapshot with an existing engine adapter.

    This state sync is convenient to use because it requires no additional setup.
    You can reuse the same engine/warehouse that your data is stored in.

    Args:
        engine_adapter: The EngineAdapter to use to store and fetch snapshots.
        schema: The schema to store state metadata in. If None or empty string then no schema is defined
        console: The console to log information to.
        context_path: The context path, used for caching snapshot models.
    """

    INTERVAL_BATCH_SIZE = 1000
    SNAPSHOT_BATCH_SIZE = 1000
    SNAPSHOT_MIGRATION_BATCH_SIZE = 500

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str],
        console: t.Optional[Console] = None,
        context_path: Path = Path(),
    ):
        # Make sure that if an empty string is provided that we treat it as None
        self.schema = schema or None
        self.engine_adapter = engine_adapter
        self._context_path = context_path
        self.console = console or get_console()
        self.snapshots_table = exp.table_("_snapshots", db=self.schema)
        self.environments_table = exp.table_("_environments", db=self.schema)
        self.intervals_table = exp.table_("_intervals", db=self.schema)
        self.plan_dags_table = exp.table_("_plan_dags", db=self.schema)
        self.versions_table = exp.table_("_versions", db=self.schema)

        self._snapshot_columns_to_types = {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
            "kind_name": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
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
            "catalog_name_override": exp.DataType.build("text"),
            "previous_finalized_snapshots": exp.DataType.build("text"),
            "normalize_name": exp.DataType.build("boolean"),
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
            "sqlmesh_version": exp.DataType.build("text"),
        }

    def _fetchone(self, query: t.Union[exp.Expression, str]) -> t.Tuple:
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

        snapshots_to_store = []

        for snapshot in snapshots:
            if isinstance(snapshot.node, SeedModel):
                seed_model = t.cast(SeedModel, snapshot.node)
                snapshot = snapshot.copy(update={"node": seed_model.to_dehydrated()})
            snapshots_to_store.append(snapshot)

        self.engine_adapter.insert_append(
            self.snapshots_table,
            _snapshots_to_df(snapshots_to_store),
            columns_to_types=self._snapshot_columns_to_types,
        )

    def _update_versions(
        self,
        schema_version: int = SCHEMA_VERSION,
        sqlglot_version: str = SQLGLOT_VERSION,
        sqlmesh_version: str = SQLMESH_VERSION,
    ) -> None:
        self.engine_adapter.delete_from(self.versions_table, "TRUE")

        self.engine_adapter.insert_append(
            self.versions_table,
            pd.DataFrame(
                [
                    {
                        "schema_version": schema_version,
                        "sqlglot_version": sqlglot_version,
                        "sqlmesh_version": sqlmesh_version,
                    }
                ]
            ),
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

    @transactional()
    def delete_expired_snapshots(
        self, ignore_ttl: bool = False
    ) -> t.List[SnapshotTableCleanupTask]:
        current_ts = now_timestamp(minute_floor=False)

        expired_query = exp.select("name", "identifier", "version").from_(self.snapshots_table)

        if not ignore_ttl:
            expired_query = expired_query.where(exp.column("expiration_ts") <= current_ts)

        expired_candidates = {
            SnapshotId(name=name, identifier=identifier): SnapshotNameVersion(
                name=name, version=version
            )
            for name, identifier, version in self._fetchall(expired_query)
        }
        if not expired_candidates:
            return []

        promoted_snapshot_ids = {
            snapshot.snapshot_id
            for environment in self.get_environments()
            for snapshot in environment.snapshots
        }

        def _is_snapshot_used(snapshot: Snapshot) -> bool:
            return (
                snapshot.snapshot_id in promoted_snapshot_ids
                or snapshot.snapshot_id not in expired_candidates
            )

        unique_expired_versions = unique(expired_candidates.values())
        version_batches = self._batches(unique_expired_versions)
        cleanup_targets = []
        for versions_batch in version_batches:
            snapshots = self._get_snapshots_with_same_version(versions_batch)

            snapshots_by_version = defaultdict(set)
            snapshots_by_temp_version = defaultdict(set)
            for s in snapshots:
                snapshots_by_version[(s.name, s.version)].add(s.snapshot_id)
                snapshots_by_temp_version[(s.name, s.temp_version_get_or_generate())].add(
                    s.snapshot_id
                )

            expired_snapshots = [s for s in snapshots if not _is_snapshot_used(s)]

            if expired_snapshots:
                self.delete_snapshots(expired_snapshots)

            for snapshot in expired_snapshots:
                shared_version_snapshots = snapshots_by_version[(snapshot.name, snapshot.version)]
                shared_version_snapshots.discard(snapshot.snapshot_id)

                shared_temp_version_snapshots = snapshots_by_temp_version[
                    (snapshot.name, snapshot.temp_version_get_or_generate())
                ]
                shared_temp_version_snapshots.discard(snapshot.snapshot_id)

                if not shared_temp_version_snapshots:
                    cleanup_targets.append(
                        SnapshotTableCleanupTask(
                            snapshot=snapshot.table_info,
                            dev_table_only=bool(shared_version_snapshots),
                        )
                    )

        return cleanup_targets

    def delete_expired_environments(self) -> t.List[Environment]:
        now_ts = now_timestamp()
        filter_expr = exp.LTE(
            this=exp.column("expiration_ts"),
            expression=exp.Literal.number(now_ts),
        )

        rows = self._fetchall(
            self._environments_query(
                where=filter_expr,
                lock_for_update=True,
            )
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
        return self._snapshot_ids_exist(snapshot_ids, self.snapshots_table)

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
        return {name for (name,) in self._fetchall(query)}

    def reset(self, default_catalog: t.Optional[str]) -> None:
        """Resets the state store to the state when it was first initialized."""
        self.engine_adapter.drop_table(self.snapshots_table)
        self.engine_adapter.drop_table(self.environments_table)
        self.engine_adapter.drop_table(self.versions_table)
        self.migrate(default_catalog)

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
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        snapshot.updated_ts = now_timestamp()
        for where in self._snapshot_id_filter([snapshot.snapshot_id]):
            self.engine_adapter.update_table(
                self.snapshots_table,
                {"snapshot": _snapshot_to_json(snapshot), "expiration_ts": snapshot.expiration_ts},
                where=where,
            )

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return [
            self._environment_from_row(row) for row in self._fetchall(self._environments_query())
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

    def _get_snapshots_expressions(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
        batch_size: t.Optional[int] = None,
    ) -> t.Iterator[exp.Expression]:
        for where in (
            [None]
            if snapshot_ids is None
            else self._snapshot_id_filter(snapshot_ids, alias="snapshots", batch_size=batch_size)
        ):
            query = (
                exp.select(
                    "snapshots.snapshot",
                    "snapshots.name",
                    "snapshots.identifier",
                    "snapshots.version",
                )
                .from_(exp.to_table(self.snapshots_table).as_("snapshots"))
                .where(where)
            )
            if lock_for_update:
                query = query.lock(copy=False)
            yield query

    def _get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
        hydrate_intervals: bool = True,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update
            hydrate_intervals: Whether to hydrate result snapshots with intervals.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        snapshots: t.Dict[SnapshotId, Snapshot] = {}
        duplicates: t.Dict[SnapshotId, Snapshot] = {}
        model_cache = ModelCache(self._context_path / c.CACHE)

        for query in self._get_snapshots_expressions(snapshot_ids, lock_for_update):
            for serialized_snapshot, name, identifier, _ in self._fetchall(query):
                snapshot = parse_snapshot(
                    model_cache,
                    serialized_snapshot=serialized_snapshot,
                    name=name,
                    identifier=identifier,
                )
                snapshot_id = snapshot.snapshot_id
                if snapshot_id in snapshots:
                    other = duplicates.get(snapshot_id, snapshots[snapshot_id])
                    duplicates[snapshot_id] = (
                        snapshot if snapshot.updated_ts > other.updated_ts else other
                    )
                    snapshots[snapshot_id] = duplicates[snapshot_id]
                else:
                    snapshots[snapshot_id] = snapshot

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

            snapshot_rows.extend(self._fetchall(query))

        return [Snapshot(**json.loads(row[0])) for row in snapshot_rows]

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        no_version = Versions()

        if not self.engine_adapter.table_exists(self.versions_table):
            return no_version

        query = exp.select("*").from_(self.versions_table)
        if lock_for_update:
            query.lock(copy=False)

        row = self._fetchone(query)
        if not row:
            return no_version

        return Versions(
            schema_version=row[0], sqlglot_version=row[1], sqlmesh_version=seq_get(row, 2)
        )

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
        row = self._fetchone(
            self._environments_query(
                where=exp.EQ(
                    this=exp.column("name"),
                    expression=exp.Literal.string(environment),
                ),
                lock_for_update=lock_for_update,
            )
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
        super().add_interval(snapshot, start, end, is_dev)

    @transactional()
    def _add_snapshot_intervals(self, snapshot_intervals: SnapshotIntervals) -> None:
        def remove_partial_intervals(
            intervals: t.List[Interval], snapshot_id: SnapshotId, *, is_dev: bool
        ) -> t.List[Interval]:
            results = []
            for start_ts, end_ts in intervals:
                if start_ts < end_ts:
                    logger.info(
                        "Adding %s (%s, %s) for snapshot %s",
                        "dev interval" if is_dev else "interval",
                        start_ts,
                        end_ts,
                        snapshot_id,
                    )
                    results.append((start_ts, end_ts))
                else:
                    logger.info(
                        "Skipping partial interval (%s, %s) for snapshot %s",
                        start_ts,
                        end_ts,
                        snapshot_intervals.snapshot_id,
                    )
            return results

        snapshot_intervals = snapshot_intervals.copy(
            update={
                "intervals": remove_partial_intervals(
                    snapshot_intervals.intervals, snapshot_intervals.snapshot_id, is_dev=False
                ),
                "dev_intervals": remove_partial_intervals(
                    snapshot_intervals.dev_intervals, snapshot_intervals.snapshot_id, is_dev=True
                ),
            }
        )
        if not snapshot_intervals.intervals and not snapshot_intervals.dev_intervals:
            return
        self.engine_adapter.insert_append(
            self.intervals_table,
            _snapshot_interval_to_df(snapshot_intervals, is_removed=False),
            columns_to_types=self._interval_columns_to_types,
        )

    @transactional()
    def remove_interval(
        self,
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        remove_shared_versions: bool = False,
    ) -> None:
        intervals_to_remove: t.Sequence[
            t.Tuple[t.Union[SnapshotInfoLike, SnapshotIntervals], Interval]
        ] = snapshot_intervals
        if remove_shared_versions:
            name_version_mapping = {s.name_version: interval for s, interval in snapshot_intervals}
            all_snapshots = []
            for where in self._snapshot_name_version_filter(name_version_mapping, alias=None):
                all_snapshots.extend(
                    [
                        SnapshotIntervals(
                            name=r[0], identifier=r[1], version=r[2], intervals=[], dev_intervals=[]
                        )
                        for r in self._fetchall(
                            exp.select("name", "identifier", "version")
                            .from_(self.intervals_table)
                            .where(where)
                        )
                    ]
                )
            intervals_to_remove = [
                (snapshot, name_version_mapping[snapshot.name_version])
                for snapshot in all_snapshots
            ]

        if logger.isEnabledFor(logging.INFO):
            snapshot_ids = ", ".join(str(s.snapshot_id) for s, _ in intervals_to_remove)
            logger.info("Removing interval for snapshots: %s", snapshot_ids)

        for is_dev in (True, False):
            self.engine_adapter.insert_append(
                self.intervals_table,
                _intervals_to_df(intervals_to_remove, is_dev=is_dev, is_removed=True),
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
            for interval_id_batch in self._batches(
                list(interval_ids), batch_size=self.INTERVAL_BATCH_SIZE
            ):
                self.engine_adapter.delete_from(
                    self.intervals_table, exp.column("id").isin(*interval_id_batch)
                )

    def refresh_snapshot_intervals(self, snapshots: t.Collection[Snapshot]) -> t.List[Snapshot]:
        if not snapshots:
            return []

        _, intervals = self._get_snapshot_intervals(snapshots)
        for s in snapshots:
            s.intervals = []
            s.dev_intervals = []
        return Snapshot.hydrate_with_intervals_by_version(snapshots, intervals)

    def max_interval_end_for_environment(
        self, environment: str, ensure_finalized_snapshots: bool = False
    ) -> t.Optional[int]:
        env = self._get_environment(environment)
        if not env:
            return None

        max_end = None
        snapshots = (
            env.snapshots if not ensure_finalized_snapshots else env.finalized_or_current_snapshots
        )
        for where in self._snapshot_name_version_filter(snapshots, "intervals"):
            end = self._fetchone(
                exp.select(exp.func("MAX", exp.to_column("end_ts")))
                .from_(exp.to_table(self.intervals_table).as_("intervals"))
                .where(where, copy=False)
                .where(exp.to_column("is_dev").not_(), copy=False),
            )[0]

            if max_end is None:
                max_end = end
            elif end is not None:
                max_end = max(max_end, end)

        return max_end

    def greatest_common_interval_end(
        self, environment: str, models: t.Set[str], ensure_finalized_snapshots: bool = False
    ) -> t.Optional[int]:
        if not models:
            return None

        env = self._get_environment(environment)
        if not env:
            return None

        snapshots = (
            env.snapshots if not ensure_finalized_snapshots else env.finalized_or_current_snapshots
        )
        snapshots = [s for s in snapshots if s.name in models]
        if not snapshots:
            snapshots = env.snapshots

        greatest_common_end = None

        table_alias = "intervals"
        name_col = exp.column("name", table=table_alias)
        version_col = exp.column("version", table=table_alias)

        for where in self._snapshot_name_version_filter(snapshots, table_alias):
            max_end_subquery = (
                exp.select(
                    name_col,
                    version_col,
                    exp.func("MAX", exp.column("end_ts", table=table_alias)).as_("max_end_ts"),
                )
                .from_(exp.to_table(self.intervals_table).as_(table_alias))
                .where(where, copy=False)
                .where(exp.to_column("is_dev").not_(), copy=False)
                .group_by(name_col, version_col, copy=False)
            )
            query = exp.select(exp.func("MIN", exp.column("max_end_ts"))).from_(
                max_end_subquery.subquery(alias="max_ends")
            )

            end = self._fetchone(query)[0]

            if greatest_common_end is None:
                greatest_common_end = end
            elif end is not None:
                greatest_common_end = min(greatest_common_end, end)

        return greatest_common_end

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
            rows = self._fetchall(query.where(where))
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
            self._update_versions()

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
        tables = (self.snapshots_table, self.environments_table, self.versions_table)
        optional_tables = (self.intervals_table, self.plan_dags_table)
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

    def _backup_state(self) -> None:
        for table in (
            self.snapshots_table,
            self.environments_table,
            self.versions_table,
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

    def _apply_migrations(
        self,
        default_catalog: t.Optional[str],
        skip_backup: bool,
    ) -> bool:
        versions = self.get_versions(validate=False)
        migrations = MIGRATIONS[versions.schema_version :]

        migrate_rows = migrations or major_minor(SQLGLOT_VERSION) != versions.minor_sqlglot_version
        if not skip_backup and migrate_rows:
            self._backup_state()

        for migration in migrations:
            logger.info(f"Applying migration {migration}")
            migration.migrate(self, default_catalog=default_catalog)

        return bool(migrate_rows)

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
            SnapshotId(name=name, identifier=identifier): json.loads(raw_snapshot)
            for where in (self._snapshot_id_filter(snapshots) if snapshots is not None else [None])
            for name, identifier, raw_snapshot in self._fetchall(
                exp.select("name", "identifier", "snapshot")
                .from_(self.snapshots_table)
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
            audits: t.Dict[str, ModelAudit] = {}
            while node_queue:
                next_snapshot_id = node_queue.pop()
                next_snapshot = parsed_snapshots.get(next_snapshot_id)

                if next_snapshot_id in node_seen or not next_snapshot:
                    continue

                node_seen.add(next_snapshot_id)
                node_queue.update(next_snapshot.parents)

                nodes[next_snapshot.name] = next_snapshot.node
                audits.update({a.name: a for a in next_snapshot.audits})

            new_snapshot = deepcopy(snapshot)
            try:
                new_snapshot.fingerprint = fingerprint_from_node(
                    node,
                    nodes=nodes,
                    audits=audits,
                    cache=fingerprint_cache,
                )
                new_snapshot.parents = tuple(
                    SnapshotId(
                        name=parent_node.fqn,
                        identifier=fingerprint_from_node(
                            parent_node,
                            nodes=nodes,
                            audits=audits,
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
            if not new_snapshot.temp_version:
                new_snapshot.temp_version = snapshot.fingerprint.to_version()

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
                environment.snapshots = snapshots
                updated_environments.append(environment)
                if environment.name == c.PROD:
                    updated_prod_environment = environment
        self.console.start_env_migration_progress(len(updated_environments))

        for environment in updated_environments:
            self._update_environment(environment)
            self.console.update_env_migration_progress(1)

        if updated_prod_environment:
            try:
                self.unpause_snapshots(updated_prod_environment.snapshots, now_timestamp())
            except Exception:
                logger.warning("Failed to unpause migrated snapshots", exc_info=True)

        self.console.stop_env_migration_progress()

    def _snapshot_ids_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike], table_name: exp.Table
    ) -> t.Set[SnapshotId]:
        return {
            SnapshotId(name=name, identifier=identifier)
            for where in self._snapshot_id_filter(snapshot_ids)
            for name, identifier in self._fetchall(
                exp.select("name", "identifier").from_(table_name).where(where)
            )
        }

    def _snapshot_id_filter(
        self,
        snapshot_ids: t.Iterable[SnapshotIdLike],
        alias: t.Optional[str] = None,
        batch_size: t.Optional[int] = None,
    ) -> t.Iterator[exp.Condition]:
        name_identifiers = sorted(
            {(snapshot_id.name, snapshot_id.identifier) for snapshot_id in snapshot_ids}
        )
        batches = self._batches(name_identifiers, batch_size=batch_size)

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
        self,
        snapshot_name_versions: t.Iterable[SnapshotNameVersionLike],
        alias: t.Optional[str] = "snapshots",
    ) -> t.Iterator[exp.Condition]:
        name_versions = sorted({(s.name, s.version) for s in snapshot_name_versions})
        batches = self._batches(name_versions)

        if not name_versions:
            yield exp.false()
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

    def _batches(self, l: t.List[T], batch_size: t.Optional[int] = None) -> t.List[t.List[T]]:
        batch_size = batch_size or self.SNAPSHOT_BATCH_SIZE
        return [l[i : i + batch_size] for i in range(0, len(l), batch_size)]

    @contextlib.contextmanager
    def _transaction(self) -> t.Iterator[None]:
        with self.engine_adapter.transaction():
            yield


def _intervals_to_df(
    snapshot_intervals: t.Sequence[t.Tuple[t.Union[SnapshotInfoLike, SnapshotIntervals], Interval]],
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


def _snapshot_interval_to_df(
    snapshot_intervals: SnapshotIntervals,
    is_removed: bool = False,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _interval_to_df(
                snapshot_intervals,
                start_ts,
                end_ts,
                is_dev=is_dev,
                is_removed=is_removed,
            )
            for is_dev in (False, True)
            for start_ts, end_ts in getattr(
                snapshot_intervals, "dev_intervals" if is_dev else "intervals"
            )
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
                "expiration_ts": snapshot.expiration_ts,
            }
            for snapshot in snapshots
        ]
    )


def _environment_to_df(environment: Environment) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": environment.name,
                "snapshots": json.dumps(
                    [snapshot.dict(mode="json") for snapshot in environment.snapshots]
                ),
                "start_at": time_like_to_str(environment.start_at),
                "end_at": time_like_to_str(environment.end_at) if environment.end_at else None,
                "plan_id": environment.plan_id,
                "previous_plan_id": environment.previous_plan_id,
                "expiration_ts": environment.expiration_ts,
                "finalized_ts": environment.finalized_ts,
                "promoted_snapshot_ids": (
                    json.dumps([s.dict() for s in environment.promoted_snapshot_ids])
                    if environment.promoted_snapshot_ids is not None
                    else None
                ),
                "suffix_target": environment.suffix_target.value,
                "catalog_name_override": environment.catalog_name_override,
                "previous_finalized_snapshots": (
                    json.dumps(
                        [
                            snapshot.dict(mode="json")
                            for snapshot in environment.previous_finalized_snapshots
                        ]
                    )
                    if environment.previous_finalized_snapshots is not None
                    else None
                ),
                "normalize_name": environment.normalize_name,
            }
        ]
    )


def _backup_table_name(table_name: TableName) -> exp.Table:
    table = exp.to_table(table_name).copy()
    table.set("this", exp.to_identifier(table.name + "_backup"))
    return table


def _snapshot_to_json(snapshot: Snapshot) -> str:
    return snapshot.json(exclude={"intervals", "dev_intervals"})


def parse_snapshot(
    model_cache: ModelCache,
    serialized_snapshot: str,
    name: str,
    identifier: str,
) -> Snapshot:
    payload = json.loads(serialized_snapshot)

    def loader() -> Node:
        return parse_obj_as(Node, payload["node"])  # type: ignore

    payload["node"] = model_cache.get_or_load(f"{name}_{identifier}", loader=loader)  # type: ignore
    snapshot = Snapshot(**payload)

    return snapshot


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
