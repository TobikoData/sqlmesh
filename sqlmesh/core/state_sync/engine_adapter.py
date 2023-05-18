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
from copy import deepcopy

import pandas as pd
from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.audit import Audit
from sqlmesh.core.engine_adapter import EngineAdapter, TransactionType
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, SeedModel
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotFingerprint,
    SnapshotId,
    SnapshotIdLike,
    SnapshotNameVersionLike,
    fingerprint_from_model,
)
from sqlmesh.core.snapshot.definition import _parents_from_model
from sqlmesh.core.state_sync.base import SCHEMA_VERSION, StateSync, Versions
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
        schema: str = c.SQLMESH,
    ):
        self.schema = schema
        self.engine_adapter = engine_adapter
        self.snapshots_table = f"{schema}._snapshots"
        self.environments_table = f"{schema}._environments"
        self.seeds_table = f"{schema}._seeds"
        self.versions_table = f"{schema}._versions"

        self._snapshot_columns_to_types = {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
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
        }

        self._seed_columns_to_types = {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "content": exp.DataType.build("text"),
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
            if isinstance(snapshot.model, SeedModel):
                seed_contents.append(
                    {
                        "name": snapshot.name,
                        "identifier": snapshot.identifier,
                        "content": snapshot.model.seed.content,
                    }
                )
                snapshot = snapshot.copy(update={"model": snapshot.model.to_dehydrated()})
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
        self.engine_adapter.drop_table(self.versions_table)
        self.migrate()

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
            _environment_to_df(environment),
            columns_to_types=self._environment_columns_to_types,
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
        hydrate_seeds: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches specified snapshots or all snapshots.

        Args:
            snapshot_ids: The collection of snapshot like objects to fetch.
            lock_for_update: Lock the snapshot rows for future update
            hydrate_seeds: Whether to hydrate seed snapshots with the content.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """
        query = (
            exp.select("snapshot")
            .from_(self.snapshots_table)
            .where(None if snapshot_ids is None else self._snapshot_id_filter(snapshot_ids))
        )
        if hydrate_seeds:
            query = query.select("content").join(
                self.seeds_table, using=["name", "identifier"], join_type="left outer"
            )
        elif lock_for_update:
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

            if hydrate_seeds and isinstance(snapshot.model, SeedModel) and row[1]:
                snapshot.model = snapshot.model.to_hydrated(row[1])

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

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        no_version = Versions(schema_version=0, sqlglot_version="0.0.0")

        if not self.engine_adapter.table_exists(self.versions_table):
            return no_version

        query = exp.select("*").from_(self.versions_table)
        if lock_for_update:
            query.lock(copy=False)
        row = self.engine_adapter.fetchone(query)
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
        super().migrate(skip_backup)

    @transactional()
    def rollback(self) -> None:
        """Rollback to the previous migration."""
        tables = (self.snapshots_table, self.environments_table, self.versions_table)
        if not any(self.engine_adapter.table_exists(f"{table}_backup") for table in tables):
            raise SQLMeshError("There are no prior migrations to roll back to.")
        for table in tables:
            self._restore_table(table, f"{table}_backup")

        if self.engine_adapter.table_exists(f"{self.seeds_table}_backup"):
            self._restore_table(self.seeds_table, f"{self.seeds_table}_backup")

    def _backup_state(self) -> None:
        for table in (
            self.snapshots_table,
            self.environments_table,
            self.versions_table,
            self.seeds_table,
        ):
            if self.engine_adapter.table_exists(table):
                with self.engine_adapter.transaction(TransactionType.DDL):
                    backup_name = f"{table}_backup"
                    self.engine_adapter.drop_table(backup_name)
                    self.engine_adapter.ctas(
                        backup_name, exp.select("*").from_(table), exists=False
                    )

    def _migrate_rows(self) -> None:
        all_snapshots = self._get_snapshots(lock_for_update=True, hydrate_seeds=True)
        environments = self.get_environments()

        snapshot_mapping = {}

        for snapshot in all_snapshots.values():
            seen = set()
            queue = {snapshot.snapshot_id}
            model = snapshot.model
            models: t.Dict[str, Model] = {}
            audits: t.Dict[str, Audit] = {}
            env: t.Dict[str, t.Dict] = {"models": models, "audits": audits}

            while queue:
                snapshot_id = queue.pop()

                if snapshot_id in seen:
                    continue

                seen.add(snapshot_id)

                s = all_snapshots.get(snapshot_id)

                if not s:
                    continue

                queue.update(s.parents)
                models[s.name] = s.model
                for audit in s.audits:
                    audits[audit.name] = audit

            new_snapshot = deepcopy(snapshot)

            fingerprint_cache: t.Dict[str, SnapshotFingerprint] = {}

            new_snapshot.fingerprint = fingerprint_from_model(
                model,
                physical_schema=snapshot.physical_schema,
                models=models,
                audits=audits,
            )
            new_snapshot.parents = tuple(
                SnapshotId(
                    name=name,
                    identifier=fingerprint_from_model(
                        models[name],
                        physical_schema=snapshot.physical_schema,
                        models=models,
                        audits=audits,
                        cache=fingerprint_cache,
                    ).to_identifier(),
                )
                for name in _parents_from_model(model, models)
            )

            # Infer the missing change category to account for SQLMesh versions in which
            # we didn't assign a change category to indirectly modified snapshots.
            if not new_snapshot.change_category:
                new_snapshot.change_category = (
                    SnapshotChangeCategory.INDIRECT_BREAKING
                    if snapshot.fingerprint.to_version() == snapshot.version
                    else SnapshotChangeCategory.INDIRECT_NON_BREAKING
                )

            if not new_snapshot.temp_version:
                new_snapshot.temp_version = snapshot.fingerprint.to_version()

            if new_snapshot == snapshot:
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

        self.delete_snapshots(snapshot_mapping)
        self._push_snapshots(set(snapshot_mapping.values()), overwrite=True)

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

        for environment in environments:
            self._update_environment(environment)

    def _snapshot_id_filter(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Union[exp.In, exp.Boolean]:
        if not snapshot_ids:
            return exp.false()

        return t.cast(exp.Tuple, exp.convert((exp.column("name"), exp.column("identifier")))).isin(
            *[(snapshot_id.name, snapshot_id.identifier) for snapshot_id in snapshot_ids]
        )

    def _snapshot_name_version_filter(
        self, snapshot_name_versions: t.Iterable[SnapshotNameVersionLike]
    ) -> t.Union[exp.In, exp.Boolean]:
        if not snapshot_name_versions:
            return exp.false()

        return t.cast(exp.Tuple, exp.convert((exp.column("name"), exp.column("version")))).isin(
            *[
                (snapshot_name_version.name, snapshot_name_version.version)
                for snapshot_name_version in snapshot_name_versions
            ]
        )

    @contextlib.contextmanager
    def _transaction(self, transaction_type: TransactionType) -> t.Generator[None, None, None]:
        with self.engine_adapter.transaction(transaction_type=transaction_type):
            yield


def _snapshots_to_df(snapshots: t.Iterable[Snapshot]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": snapshot.name,
                "identifier": snapshot.identifier,
                "version": snapshot.version,
                "snapshot": snapshot.json(),
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
                "start_at": environment.start_at,
                "end_at": environment.end_at,
                "plan_id": environment.plan_id,
                "previous_plan_id": environment.previous_plan_id,
                "expiration_ts": environment.expiration_ts,
                "finalized_ts": environment.finalized_ts,
            }
        ]
    )
