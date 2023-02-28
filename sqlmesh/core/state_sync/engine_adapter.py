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

from sqlglot import exp

from sqlmesh.core.dialect import select_from_values
from sqlmesh.core.engine_adapter import EngineAdapter, TransactionType
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotNameVersionLike,
)
from sqlmesh.core.state_sync.base import StateSync
from sqlmesh.core.state_sync.common import CommonStateSyncMixin, transactional
from sqlmesh.utils.date import now_timestamp
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


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
    ):
        self.engine_adapter = engine_adapter
        self.snapshots_table = f"{schema}._snapshots"
        self.environments_table = f"{schema}._environments"

    @property
    def snapshot_columns_to_types(self) -> t.Dict[str, exp.DataType]:
        return {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
        }

    @property
    def environment_columns_to_types(self) -> t.Dict[str, exp.DataType]:
        return {
            "name": exp.DataType.build("text"),
            "snapshots": exp.DataType.build("text"),
            "start_at": exp.DataType.build("text"),
            "end_at": exp.DataType.build("text"),
            "plan_id": exp.DataType.build("text"),
            "previous_plan_id": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
        }

    @transactional(transaction_type=TransactionType.DDL)
    def init_schema(self) -> None:
        """Creates the schema and table to store state."""
        self.engine_adapter.create_schema(self.snapshots_table)

        self.engine_adapter.create_state_table(
            self.snapshots_table,
            self.snapshot_columns_to_types,
            primary_key=("name", "identifier"),
        )

        self.engine_adapter.create_index(
            self.snapshots_table, "name_version_idx", ("name", "version")
        )

        self.engine_adapter.create_state_table(
            self.environments_table,
            self.environment_columns_to_types,
            primary_key=("name",),
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

        self.engine_adapter.insert_append(
            self.snapshots_table,
            next(
                select_from_values(
                    [
                        (
                            snapshot.name,
                            snapshot.identifier,
                            snapshot.version,
                            snapshot.json(),
                        )
                        for snapshot in snapshots
                    ],
                    columns_to_types=self.snapshot_columns_to_types,
                )
            ),
            contains_json=True,
        )

    def delete_expired_environments(self) -> t.List[Environment]:
        now_ts = now_timestamp()
        filter_expr = exp.LTE(
            this=exp.to_column("expiration_ts"),
            expression=exp.Literal.number(now_ts),
        )

        rows = self.engine_adapter.fetchall(
            self._environments_query(
                where=filter_expr,
                lock_for_update=True,
            ),
            ignore_unsupported_errors=True,
        )
        environments = [self._environment_from_row(r) for r in rows]

        self.engine_adapter.delete_from(
            self.environments_table,
            where=filter_expr,
        )

        return environments

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        self.engine_adapter.delete_from(
            self.snapshots_table, where=self._snapshot_id_filter(snapshot_ids)
        )

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        return {
            SnapshotId(name=name, identifier=identifier)
            for name, identifier in self.engine_adapter.fetchall(
                exp.select("name", "identifier")
                .from_(self.snapshots_table)
                .where(self._snapshot_id_filter(snapshot_ids))
            )
        }

    def reset(self) -> None:
        """Resets the state store to the state when it was first initialized."""
        self.engine_adapter.drop_table(self.snapshots_table)
        self.engine_adapter.drop_table(self.environments_table)
        self.init_schema()

    def _update_environment(self, environment: Environment) -> None:
        self.engine_adapter.delete_from(
            self.environments_table,
            where=exp.EQ(
                this=exp.to_column("name"),
                expression=exp.Literal.string(environment.name),
            ),
        )

        self.engine_adapter.insert_append(
            self.environments_table,
            next(
                select_from_values(
                    [
                        (
                            environment.name,
                            json.dumps([snapshot.dict() for snapshot in environment.snapshots]),
                            environment.start_at,
                            environment.end_at,
                            environment.plan_id,
                            environment.previous_plan_id,
                            environment.expiration_ts,
                        )
                    ],
                    columns_to_types=self.environment_columns_to_types,
                )
            ),
            columns_to_types=self.environment_columns_to_types,
            contains_json=True,
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        self.engine_adapter.update_table(
            self.snapshots_table,
            {"snapshot": snapshot.json()},
            where=self._snapshot_id_filter([snapshot.snapshot_id]),
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
                self._environments_query(), ignore_unsupported_errors=True
            )
        ]

    def _environment_from_row(self, row: t.Tuple[str, ...]) -> Environment:
        return Environment(**{field: row[i] for i, field in enumerate(Environment.__fields__)})

    def _environments_query(
        self,
        where: t.Optional[str | exp.Expression] = None,
        lock_for_update: bool = False,
    ) -> exp.Select:
        query = (
            exp.select(*(exp.to_identifier(field) for field in Environment.__fields__))
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
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        query = (
            exp.select("snapshot")
            .from_(self.snapshots_table)
            .where(None if snapshot_ids is None else self._snapshot_id_filter(snapshot_ids))
        )
        if lock_for_update:
            query = query.lock(copy=False)

        snapshots: t.Dict[SnapshotId, Snapshot] = {}
        duplicates: t.Dict[SnapshotId, Snapshot] = {}

        for row in self.engine_adapter.fetchall(query, ignore_unsupported_errors=True):
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
        self,
        snapshots: t.Iterable[SnapshotNameVersionLike],
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

        query = (
            exp.select("snapshot")
            .from_(self.snapshots_table)
            .where(self._snapshot_name_version_filter(snapshots))
        )
        if lock_for_update:
            query = query.lock(copy=False)

        snapshot_rows = self.engine_adapter.fetchall(query, ignore_unsupported_errors=True)
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
            self._environments_query(
                where=exp.EQ(
                    this=exp.to_column("name"),
                    expression=exp.Literal.string(environment),
                ),
                lock_for_update=lock_for_update,
            ),
            ignore_unsupported_errors=True,
        )

        if not row:
            return None

        env = self._environment_from_row(row)
        return env

    def _snapshot_id_filter(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Union[exp.Or, exp.Boolean]:
        if not snapshot_ids:
            return exp.FALSE

        return exp.or_(
            *(
                exp.and_(
                    exp.EQ(
                        this=exp.to_column("name"),
                        expression=exp.Literal.string(snapshot_id.name),
                    ),
                    exp.EQ(
                        this=exp.to_column("identifier"),
                        expression=exp.Literal.string(snapshot_id.identifier),
                    ),
                )
                for snapshot_id in snapshot_ids
            )
        )

    def _snapshot_name_version_filter(
        self, snapshot_name_versions: t.Iterable[SnapshotNameVersionLike]
    ) -> t.Union[exp.Or, exp.Boolean]:
        if not snapshot_name_versions:
            return exp.FALSE

        return exp.or_(
            *(
                exp.and_(
                    exp.EQ(
                        this=exp.to_column("name"),
                        expression=exp.Literal.string(snapshot_name_version.name),
                    ),
                    exp.EQ(
                        this=exp.to_column("version"),
                        expression=exp.Literal.string(snapshot_name_version.version),
                    ),
                )
                for snapshot_name_version in snapshot_name_versions
            )
        )

    @contextlib.contextmanager
    def _transaction(self, transaction_type: TransactionType) -> t.Generator[None, None, None]:
        with self.engine_adapter.transaction(transaction_type=transaction_type):
            yield
