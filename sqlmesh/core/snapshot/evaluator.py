"""
# SnapshotEvaluator

A snapshot evaluator is responsible for evaluating a snapshot given some runtime arguments, e.g. start
and end timestamps.

# Evaluation

Snapshot evaluation involves determining the queries necessary to evaluate a snapshot and using
`sqlmesh.core.engine_adapter` to execute the queries. Schemas, tables, and views are created if
they don't exist and data is inserted when applicable.

A snapshot evaluator also promotes and demotes snapshots to a given environment.

# Audits

A snapshot evaluator can also run the audits for a snapshot's node. This is often done after a snapshot
has been evaluated to check for data quality issues.

For more information about audits, see `sqlmesh.core.audit`.
"""

from __future__ import annotations

import abc
import logging
import typing as t
from collections import defaultdict
from contextlib import contextmanager
from functools import reduce

import pandas as pd
from sqlglot import exp, select
from sqlglot.executor import execute

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.audit import Audit, AuditResult
from sqlmesh.core.dialect import schema_
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy
from sqlmesh.core.macros import RuntimeStage
from sqlmesh.core.model import (
    IncrementalUnmanagedKind,
    Model,
    SCDType2ByColumnKind,
    SCDType2ByTimeKind,
    ViewKind,
)
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    QualifiedViewName,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotInfoLike,
    SnapshotTableCleanupTask,
)
from sqlmesh.utils import random_id
from sqlmesh.utils.concurrency import (
    concurrent_apply_to_snapshots,
    concurrent_apply_to_values,
)
from sqlmesh.utils.date import TimeLike, now
from sqlmesh.utils.errors import AuditError, ConfigError, SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF
    from sqlmesh.core.environment import EnvironmentNamingInfo

logger = logging.getLogger(__name__)


class SnapshotEvaluator:
    """Evaluates a snapshot given runtime arguments through an arbitrary EngineAdapter.

    The SnapshotEvaluator contains the business logic to generically evaluate a snapshot.
    It is responsible for delegating queries to the EngineAdapter. The SnapshotEvaluator
    does not directly communicate with the underlying execution engine.

    Args:
        adapter: The adapter that interfaces with the execution engine.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL
            operations (table / view creation, deletion, etc). Default: 1.
    """

    def __init__(self, adapter: EngineAdapter, ddl_concurrent_tasks: int = 1):
        self.adapter = adapter
        self.ddl_concurrent_tasks = ddl_concurrent_tasks

    def evaluate(
        self,
        snapshot: Snapshot,
        *,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
        **kwargs: t.Any,
    ) -> t.Optional[str]:
        """Renders the snapshot's model, executes it and stores the result in the snapshot's physical table.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by name) to use for expansion and mapping of physical locations.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The WAP ID of this evaluation if supported, None otherwise.
        """
        result = self._evaluate_snapshot(
            snapshot,
            start,
            end,
            execution_time,
            snapshots,
            deployability_index=deployability_index,
            **kwargs,
        )
        if result is None or isinstance(result, str):
            return result
        raise SQLMeshError(
            f"Unexpected result {result} when evaluating snapshot {snapshot.snapshot_id}."
        )

    def evaluate_and_fetch(
        self,
        snapshot: Snapshot,
        *,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        snapshots: t.Dict[str, Snapshot],
        limit: int,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        **kwargs: t.Any,
    ) -> DF:
        """Renders the snapshot's model, executes it and returns a dataframe with the result.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by name) to use for expansion and mapping of physical locations.
            limit: The maximum number of rows to fetch.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The result of the evaluation as a dataframe.
        """
        result = self._evaluate_snapshot(
            snapshot,
            start,
            end,
            execution_time,
            snapshots,
            limit=limit,
            deployability_index=deployability_index,
            **kwargs,
        )
        if result is None or isinstance(result, str):
            raise SQLMeshError(
                f"Unexpected result {result} when evaluating snapshot {snapshot.snapshot_id}."
            )
        return result

    def promote(
        self,
        target_snapshots: t.Iterable[Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Promotes the given collection of snapshots in the target environment by replacing a corresponding
        view with a physical table associated with the given snapshot.

        Args:
            target_snapshots: Snapshots to promote.
            environment_naming_info: Naming information for the target environment.
            deployability_index: Determines snapshots that are deployable in the context of this promotion.
            on_complete: A callback to call on each successfully promoted snapshot.
        """
        self._create_schemas(
            [
                s.qualified_view_name.table_for_environment(environment_naming_info)
                for s in target_snapshots
                if s.is_model and not s.is_symbolic
            ]
        )
        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._promote_snapshot(
                    s,
                    environment_naming_info,
                    deployability_index,  # type: ignore
                    on_complete,
                ),
                self.ddl_concurrent_tasks,
            )

    def demote(
        self,
        target_snapshots: t.Iterable[SnapshotInfoLike],
        environment_naming_info: EnvironmentNamingInfo,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Demotes the given collection of snapshots in the target environment by removing its view.

        Args:
            target_snapshots: Snapshots to demote.
            environment_naming_info: Naming info for the target environment.
            on_complete: A callback to call on each successfully demoted snapshot.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._demote_snapshot(s, environment_naming_info, on_complete),
                self.ddl_concurrent_tasks,
            )

    def create(
        self,
        target_snapshots: t.Iterable[Snapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Creates a physical snapshot schema and table for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshots.
            snapshots: Mapping of snapshot ID to snapshot.
            deployability_index: Determines snapshots that are deployable in the context of this creation.
            on_complete: A callback to call on each successfully created snapshot.
        """
        snapshots_with_table_names = defaultdict(set)
        tables_by_schema = defaultdict(set)
        for snapshot in target_snapshots:
            if not snapshot.is_model or snapshot.is_symbolic:
                continue
            for is_deployable in (True, False):
                table = exp.to_table(
                    snapshot.table_name(is_deployable), dialect=snapshot.model.dialect
                )
                snapshots_with_table_names[snapshot].add(table.name)
                tables_by_schema[d.schema_(table.db, catalog=table.catalog)].add(table.name)

        def _get_data_objects(schema: exp.Table) -> t.Set[str]:
            logger.info("Listing data objects in schema %s", schema.sql())
            objs = self.adapter.get_data_objects(schema, tables_by_schema[schema])
            return {obj.name for obj in objs}

        with self.concurrent_context():
            existing_objects = {
                obj
                for objs in concurrent_apply_to_values(
                    list(tables_by_schema), _get_data_objects, self.ddl_concurrent_tasks
                )
                for obj in objs
            }

        snapshots_to_create = []
        for snapshot, table_names in snapshots_with_table_names.items():
            if table_names - existing_objects:
                snapshots_to_create.append(snapshot)
            elif on_complete:
                on_complete(snapshot)

        if not snapshots_to_create:
            return

        self._create_schemas(tables_by_schema)
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                snapshots_to_create,
                lambda s: self._create_snapshot(s, snapshots, deployability_index, on_complete),
                self.ddl_concurrent_tasks,
            )

    def migrate(
        self, target_snapshots: t.Iterable[Snapshot], snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> None:
        """Alters a physical snapshot table to match its snapshot's schema for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshots.
            snapshots: Mapping of snapshot ID to snapshot.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._migrate_snapshot(s, snapshots),
                self.ddl_concurrent_tasks,
            )

    def cleanup(
        self,
        target_snapshots: t.Iterable[SnapshotTableCleanupTask],
        on_complete: t.Optional[t.Callable[[str], None]] = None,
    ) -> None:
        """Cleans up the given snapshots by removing its table

        Args:
            target_snapshots: Snapshots to cleanup.
            on_complete: A callback to call on each successfully deleted database object.
        """
        snapshots_to_dev_table_only = {
            t.snapshot.snapshot_id: t.dev_table_only for t in target_snapshots
        }
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                [t.snapshot for t in target_snapshots],
                lambda s: self._cleanup_snapshot(
                    s, snapshots_to_dev_table_only[s.snapshot_id], on_complete
                ),
                self.ddl_concurrent_tasks,
                reverse_order=True,
            )

    def audit(
        self,
        snapshot: Snapshot,
        *,
        snapshots: t.Dict[str, Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        raise_exception: bool = True,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        wap_id: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.List[AuditResult]:
        """Execute a snapshot's node's audit queries.

        Args:
            snapshot: Snapshot to evaluate.
            snapshots: All upstream snapshots (by name) to use for expansion and mapping of physical locations.
            start: The start datetime to audit. Defaults to epoch start.
            end: The end datetime to audit. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            raise_exception: Whether to raise an exception if the audit fails. Blocking rules determine if an
                AuditError is thrown or if we just warn with logger
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            wap_id: The WAP ID if applicable, None otherwise.
            kwargs: Additional kwargs to pass to the renderer.
        """
        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        if not deployability_index.is_deployable(snapshot):
            # We can't audit a temporary table.
            return []

        if not snapshot.version:
            raise ConfigError(
                f"Cannot audit '{snapshot.name}' because it has not been versioned yet. Apply a plan first."
            )

        if wap_id is not None:
            deployability_index = deployability_index or DeployabilityIndex.all_deployable()
            original_table_name = snapshot.table_name(
                is_deployable=deployability_index.is_deployable(snapshot)
            )
            wap_table_name = self.adapter.wap_table_name(original_table_name, wap_id)
            logger.info(
                "Auditing WAP table '%s', snapshot %s", wap_table_name, snapshot.snapshot_id
            )

            table_mapping = kwargs.get("table_mapping") or {}
            table_mapping[snapshot.name] = wap_table_name
            kwargs["table_mapping"] = table_mapping
            kwargs["this_model"] = exp.to_table(wap_table_name, dialect=self.adapter.dialect)

        results = []

        audits_with_args = snapshot.audits_with_args

        if audits_with_args:
            logger.info("Auditing snapshot %s", snapshot.snapshot_id)

        for audit, audit_args in snapshot.audits_with_args:
            results.append(
                self._audit(
                    audit=audit,
                    audit_args=audit_args,
                    snapshot=snapshot,
                    snapshots=snapshots,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    raise_exception=raise_exception,
                    deployability_index=deployability_index,
                    **kwargs,
                )
            )

        if wap_id is not None:
            logger.info(
                "Publishing evalaution results for snapshot %s, WAP ID '%s'",
                snapshot.snapshot_id,
                wap_id,
            )
            self._wap_publish_snapshot(snapshot, wap_id, deployability_index)

        return results

    @contextmanager
    def concurrent_context(self) -> t.Iterator[None]:
        try:
            yield
        finally:
            self.recycle()

    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        try:
            self.adapter.recycle()
        except Exception:
            logger.exception("Failed to recycle Snapshot Evaluator")

    def close(self) -> None:
        """Closes all open connections and releases all allocated resources."""
        try:
            self.adapter.close()
        except Exception:
            logger.exception("Failed to close Snapshot Evaluator")

    def _evaluate_snapshot(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        snapshots: t.Dict[str, Snapshot],
        limit: t.Optional[int] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        **kwargs: t.Any,
    ) -> DF | str | None:
        """Renders the snapshot's model and executes it. The return value depends on whether the limit was specified.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots to use for expansion and mapping of physical locations.
            limit: If limit is not None, the query will not be persisted but evaluated and returned as a dataframe.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            kwargs: Additional kwargs to pass to the renderer.
        """
        if not snapshot.is_model:
            return None

        model = snapshot.model

        logger.info("Evaluating snapshot %s", snapshot.snapshot_id)

        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        table_name = (
            ""
            if limit is not None
            else snapshot.table_name(is_deployable=deployability_index.is_deployable(snapshot))
        )

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        def apply(query_or_df: QueryOrDF, index: int = 0) -> None:
            if index > 0:
                evaluation_strategy.append(
                    snapshot,
                    table_name,
                    query_or_df,
                    snapshots,
                    deployability_index,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                )
            else:
                logger.info("Inserting batch (%s, %s) into %s'", start, end, table_name)
                evaluation_strategy.insert(
                    snapshot,
                    table_name,
                    query_or_df,
                    snapshots,
                    deployability_index,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                )

        from sqlmesh.core.context import ExecutionContext

        common_render_kwargs = dict(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshot=snapshot,
            runtime_stage=RuntimeStage.EVALUATING,
            **kwargs,
        )

        render_statements_kwargs = dict(
            engine_adapter=self.adapter,
            snapshots=snapshots,
            deployability_index=deployability_index,
            **common_render_kwargs,
        )

        with self.adapter.transaction(), self.adapter.session(snapshot.model.session_properties):
            wap_id: t.Optional[str] = None
            if (
                table_name
                and snapshot.is_materialized
                and (model.wap_supported or self.adapter.wap_supported(table_name))
            ):
                wap_id = random_id()[0:8]
                logger.info("Using WAP ID '%s' for snapshot %s", wap_id, snapshot.snapshot_id)
                table_name = self.adapter.wap_prepare(table_name, wap_id)

            if limit is None:
                self.adapter.execute(model.render_pre_statements(**render_statements_kwargs))

            queries_or_dfs = model.render(
                context=ExecutionContext(
                    self.adapter,
                    snapshots,
                    deployability_index,
                    default_dialect=model.dialect,
                    default_catalog=model.default_catalog,
                ),
                **common_render_kwargs,
            )

            if limit is not None:
                query_or_df = next(queries_or_dfs)
                if isinstance(query_or_df, exp.Select):
                    existing_limit = query_or_df.args.get("limit")
                    if existing_limit:
                        limit = min(
                            limit,
                            execute(exp.select(existing_limit.expression)).rows[0][0],
                        )
                return query_or_df.head(limit) if hasattr(query_or_df, "head") else self.adapter._fetch_native_df(query_or_df.limit(limit))  # type: ignore
            # DataFrames, unlike SQL expressions, can provide partial results by yielding dataframes. As a result,
            # if the engine supports INSERT OVERWRITE or REPLACE WHERE and the snapshot is incremental by time range, we risk
            # having a partial result since each dataframe write can re-truncate partitions. To avoid this, we
            # union all the dataframes together before writing. For pandas this could result in OOM and a potential
            # workaround for that would be to serialize pandas to disk and then read it back with Spark.
            # Note: We assume that if multiple things are yielded from `queries_or_dfs` that they are dataframes
            # and not SQL expressions.
            elif (
                self.adapter.INSERT_OVERWRITE_STRATEGY
                in (InsertOverwriteStrategy.INSERT_OVERWRITE, InsertOverwriteStrategy.REPLACE_WHERE)
                and snapshot.is_incremental_by_time_range
            ):
                query_or_df = reduce(
                    lambda a, b: (
                        pd.concat([a, b], ignore_index=True)  # type: ignore
                        if self.adapter.is_pandas_df(a)
                        else a.union_all(b)  # type: ignore
                    ),  # type: ignore
                    queries_or_dfs,
                )
                apply(query_or_df, index=0)
            else:
                for index, query_or_df in enumerate(queries_or_dfs):
                    apply(query_or_df, index)

            if limit is None:
                self.adapter.execute(model.render_post_statements(**render_statements_kwargs))

            return wap_id

    def _create_snapshot(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        if not snapshot.is_model:
            return

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }
        parent_snapshots_by_name[snapshot.name] = snapshot

        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        is_snapshot_deployable = deployability_index.is_deployable(snapshot)

        # Refers to self as non-deployable to successfully create self-referential tables / views.
        deployability_index = deployability_index.with_non_deployable(snapshot)

        render_kwargs: t.Dict[str, t.Any] = dict(
            engine_adapter=self.adapter,
            snapshots=parent_snapshots_by_name,
            deployability_index=deployability_index,
            runtime_stage=RuntimeStage.CREATING,
        )

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        with self.adapter.transaction(), self.adapter.session(snapshot.model.session_properties):
            self.adapter.execute(snapshot.model.render_pre_statements(**render_kwargs))

            if (
                snapshot.is_forward_only
                and snapshot.is_materialized
                and snapshot.previous_versions
                and self.adapter.SUPPORTS_CLONING
            ):
                target_table_name = snapshot.table_name(is_deployable=False)
                tmp_table_name = f"{target_table_name}__schema_migration_source"
                source_table_name = snapshot.table_name()

                logger.info(f"Cloning table '{source_table_name}' into '{target_table_name}'")

                evaluation_strategy.create(
                    snapshot, tmp_table_name, False, is_snapshot_deployable, **render_kwargs
                )
                try:
                    self.adapter.clone_table(target_table_name, snapshot.table_name(), replace=True)
                    self.adapter.alter_table(target_table_name, tmp_table_name)
                finally:
                    self.adapter.drop_table(tmp_table_name)
            else:
                table_deployability_flags = [False]
                if not snapshot.is_indirect_non_breaking and not snapshot.is_forward_only:
                    table_deployability_flags.append(True)
                for is_table_deployable in table_deployability_flags:
                    evaluation_strategy.create(
                        snapshot,
                        snapshot.table_name(is_deployable=is_table_deployable),
                        is_table_deployable,
                        is_snapshot_deployable,
                        **render_kwargs,
                    )

            self.adapter.execute(snapshot.model.render_post_statements(**render_kwargs))

        if on_complete is not None:
            on_complete(snapshot)

    def _migrate_snapshot(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> None:
        if (
            not snapshot.is_paused
            or snapshot.change_category
            not in (
                SnapshotChangeCategory.FORWARD_ONLY,
                SnapshotChangeCategory.INDIRECT_NON_BREAKING,
            )
            or not snapshot.is_model
        ):
            return

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }
        parent_snapshots_by_name[snapshot.name] = snapshot

        tmp_table_name = snapshot.table_name(is_deployable=False)
        target_table_name = snapshot.table_name()
        _evaluation_strategy(snapshot, self.adapter).migrate(
            snapshot, parent_snapshots_by_name, target_table_name, tmp_table_name
        )

    def _promote_snapshot(
        self,
        snapshot: Snapshot,
        environment_naming_info: EnvironmentNamingInfo,
        deployability_index: DeployabilityIndex,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        if snapshot.is_model:
            table_name = snapshot.table_name(deployability_index.is_representative(snapshot))
            _evaluation_strategy(snapshot, self.adapter).promote(
                snapshot.qualified_view_name, environment_naming_info, table_name, snapshot
            )

        if on_complete is not None:
            on_complete(snapshot)

    def _demote_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        environment_naming_info: EnvironmentNamingInfo,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        _evaluation_strategy(snapshot, self.adapter).demote(
            snapshot.qualified_view_name, environment_naming_info
        )

        if on_complete is not None:
            on_complete(snapshot)

    def _cleanup_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        dev_table_only: bool,
        on_complete: t.Optional[t.Callable[[str], None]],
    ) -> None:
        snapshot = snapshot.table_info

        table_names = [snapshot.table_name(is_deployable=False)]
        if not dev_table_only:
            table_names.append(snapshot.table_name(is_deployable=True))

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        for table_name in table_names:
            table = exp.to_table(table_name)
            if table.db != snapshot.physical_schema:
                raise SQLMeshError(
                    f"Table '{table_name}' is not a part of the physical schema '{snapshot.physical_schema}' and so can't be dropped."
                )
            evaluation_strategy.delete(table_name)

            if on_complete is not None:
                on_complete(table_name)

    def _wap_publish_snapshot(
        self,
        snapshot: Snapshot,
        wap_id: str,
        deployability_index: t.Optional[DeployabilityIndex],
    ) -> None:
        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        table_name = snapshot.table_name(is_deployable=deployability_index.is_deployable(snapshot))
        self.adapter.wap_publish(table_name, wap_id)

    def _audit(
        self,
        audit: Audit,
        audit_args: t.Dict[t.Any, t.Any],
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        start: t.Optional[TimeLike],
        end: t.Optional[TimeLike],
        execution_time: t.Optional[TimeLike],
        raise_exception: bool,
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> AuditResult:
        if audit.skip:
            return AuditResult(
                audit=audit,
                model=snapshot.model_or_none,
                skipped=True,
            )

        query = audit.render_query(
            snapshot,
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            deployability_index=deployability_index,
            engine_adapter=self.adapter,
            **audit_args,
            **kwargs,
        )
        count, *_ = self.adapter.fetchone(
            select("COUNT(*)").from_(query.subquery("audit")),
            quote_identifiers=True,
        )
        if count and raise_exception:
            audit_error = AuditError(
                audit_name=audit.name,
                model=snapshot.model_or_none,
                count=count,
                query=query,
                adapter_dialect=self.adapter.dialect,
            )
            if audit.blocking:
                raise audit_error
            else:
                logger.warning(f"{audit_error}\nAudit is warn only so proceeding with execution.")
        return AuditResult(
            audit=audit,
            model=snapshot.model_or_none,
            count=count,
            query=query,
        )

    def _create_schemas(self, tables: t.Iterable[t.Union[exp.Table, str]]) -> None:
        table_exprs = [exp.to_table(t) for t in tables]
        unique_schemas = {(t.args["db"], t.args.get("catalog")) for t in table_exprs if t and t.db}
        # Create schemas sequentially, since some engines (eg. Postgres) may not support concurrent creation
        # of schemas with the same name.
        for schema_name, catalog in unique_schemas:
            schema = schema_(schema_name, catalog)
            logger.info("Creating schema '%s'", schema)
            self.adapter.create_schema(schema)


def _evaluation_strategy(snapshot: SnapshotInfoLike, adapter: EngineAdapter) -> EvaluationStrategy:
    klass: t.Type
    if snapshot.is_embedded:
        klass = EmbeddedStrategy
    elif snapshot.is_symbolic or snapshot.is_audit:
        klass = SymbolicStrategy
    elif snapshot.is_full or snapshot.is_seed:
        klass = FullRefreshStrategy
    elif snapshot.is_incremental_by_time_range:
        klass = IncrementalByTimeRangeStrategy
    elif snapshot.is_incremental_by_unique_key:
        klass = IncrementalByUniqueKeyStrategy
    elif snapshot.is_incremental_unmanaged:
        klass = IncrementalUnmanagedStrategy
    elif snapshot.is_view:
        klass = ViewStrategy
    elif snapshot.is_scd_type_2:
        klass = SCDType2Strategy
    else:
        raise SQLMeshError(f"Unexpected snapshot: {snapshot}")

    return klass(adapter)


class EvaluationStrategy(abc.ABC):
    def __init__(self, adapter: EngineAdapter):
        self.adapter = adapter

    @abc.abstractmethod
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        """Inserts the given query or a DataFrame into the target table or replaces a view.

        Args:
            snapshot: The target snapshot.
            name: The name of the target table or view.
            query_or_df: The query or DataFrame to insert.
            snapshots: Parent snapshots.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
        """

    @abc.abstractmethod
    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        """Appends the given query or a DataFrame to the existing table.

        Args:
            snapshot: The target snapshot.
            table_name: The target table name.
            query_or_df: The query or DataFrame to insert.
            snapshots: Parent snapshots.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
        """

    @abc.abstractmethod
    def create(
        self,
        snapshot: Snapshot,
        name: str,
        is_table_deployable: bool,
        is_snapshot_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        """Creates the target table or view.

        Args:
            snapshot: The target snapshot.
            name: The name of a table or a view.
            is_table_deployable: Whether the table that is being created is deployable.
            is_snapshot_deployable: Whether the snapshot is considered deployable.
            render_kwargs: Additional kwargs for node rendering.
        """

    @abc.abstractmethod
    def migrate(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        target_table_name: str,
        source_table_name: str,
    ) -> None:
        """Migrates the target table schema so that it corresponds to the source table schema.

        Args:
            snapshot: The target snapshot.
            snapshots: Parent snapshots.
            target_table_name: The target table name.
            source_table_name: The source table name.
        """

    @abc.abstractmethod
    def delete(self, name: str) -> None:
        """Deletes a target table or a view.

        Args:
            name: The name of a table or a view.
        """

    @abc.abstractmethod
    def promote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
        table_name: str,
        snapshot: Snapshot,
    ) -> None:
        """Updates the target view to point to the target table.

        Args:
            view_name: The name of the target view.
            environment_naming_info: The naming information for the target environment
            table_name: The name of the target table.
            snapshot: The target snapshot. Not ucrrently used but can be used by others if needed.
        """

    @abc.abstractmethod
    def demote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        """Deletes the target view.

        Args:
            view_name: The name of the target view.
            environment_naming_info: Naming information for the target environment.
        """


class SymbolicStrategy(EvaluationStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        pass

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        pass

    def create(
        self,
        snapshot: Snapshot,
        name: str,
        is_table_deployable: bool,
        is_snapshot_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        pass

    def migrate(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        target_table_name: str,
        source_table_name: str,
    ) -> None:
        pass

    def delete(self, name: str) -> None:
        pass

    def promote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
        table_name: str,
        snapshot: Snapshot,
    ) -> None:
        pass

    def demote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        pass


class EmbeddedStrategy(SymbolicStrategy):
    def promote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
        table_name: str,
        snapshot: Snapshot,
    ) -> None:
        target_name = view_name.for_environment(environment_naming_info)
        logger.info("Dropping view '%s' for non-materialized table", target_name)
        self.adapter.drop_view(target_name)


class PromotableStrategy(EvaluationStrategy):
    def promote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
        table_name: str,
        snapshot: Snapshot,
    ) -> None:
        target_name = view_name.for_environment(environment_naming_info)
        is_prod = environment_naming_info.name.lower() == c.PROD
        logger.info("Updating view '%s' to point at table '%s'", target_name, table_name)
        self.adapter.create_view(
            target_name,
            exp.select("*").from_(table_name, dialect=self.adapter.dialect),
            table_description=snapshot.model.description if is_prod else None,
            column_descriptions=snapshot.model.column_descriptions if is_prod else None,
        )

    def demote(
        self,
        view_name: QualifiedViewName,
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        target_name = view_name.for_environment(environment_naming_info)
        logger.info("Dropping view '%s'", target_name)
        self.adapter.drop_view(target_name)


class MaterializableStrategy(PromotableStrategy):
    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.insert_append(table_name, query_or_df, columns_to_types=model.columns_to_types)

    def create(
        self,
        snapshot: Snapshot,
        name: str,
        is_table_deployable: bool,
        is_snapshot_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        ctas_query = model.ctas_query(**render_kwargs)

        logger.info("Creating table '%s'", name)
        if model.annotated:
            self.adapter.create_table(
                name,
                columns_to_types=model.columns_to_types_or_raise,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.table_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )

            # only sql models have queries that can be tested
            # additionally, we always create temp tables and sometimes
            # we additionally created prod tables, so we only need to test one.
            if model.is_sql and not is_table_deployable:
                logger.info("Dry running model '%s'", model.name)
                self.adapter.fetchall(ctas_query)
        else:
            self.adapter.ctas(
                name,
                ctas_query,
                model.columns_to_types,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.table_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )

    def migrate(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        target_table_name: str,
        source_table_name: str,
    ) -> None:
        logger.info(f"Altering table '{target_table_name}'")
        self.adapter.alter_table(target_table_name, source_table_name)

    def delete(self, table_name: str) -> None:
        self.adapter.drop_table(table_name)
        logger.info("Dropped table '%s'", table_name)


class IncrementalByTimeRangeStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        assert model.time_column
        self.adapter.insert_overwrite_by_time_partition(
            name,
            query_or_df,
            time_formatter=model.convert_to_time_column,
            time_column=model.time_column,
            columns_to_types=model.columns_to_types,
            **kwargs,
        )


class IncrementalByUniqueKeyStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.merge(
            name,
            query_or_df,
            columns_to_types=model.columns_to_types,
            unique_key=model.unique_key,
            when_matched=model.when_matched,
        )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.merge(
            table_name,
            query_or_df,
            columns_to_types=model.columns_to_types,
            unique_key=model.unique_key,
            when_matched=model.when_matched,
        )


class IncrementalUnmanagedStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        if isinstance(model.kind, IncrementalUnmanagedKind) and model.kind.insert_overwrite:
            self.adapter.insert_overwrite_by_partition(
                name,
                query_or_df,
                model.partitioned_by,
                columns_to_types=model.columns_to_types,
            )
        else:
            self.append(
                snapshot,
                name,
                query_or_df,
                snapshots,
                deployability_index,
                **kwargs,
            )


class FullRefreshStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.replace_query(
            name,
            query_or_df,
            columns_to_types=model.columns_to_types if model.annotated else None,
            storage_format=model.storage_format,
            partitioned_by=model.partitioned_by,
            partition_interval_unit=model.interval_unit,
            clustered_by=model.clustered_by,
            table_properties=model.table_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )


class SCDType2Strategy(MaterializableStrategy):
    def create(
        self,
        snapshot: Snapshot,
        name: str,
        is_table_deployable: bool,
        is_snapshot_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        assert isinstance(model.kind, (SCDType2ByTimeKind, SCDType2ByColumnKind))
        if model.annotated:
            logger.info("Creating table '%s'", name)
            columns_to_types = model.columns_to_types_or_raise
            if isinstance(model.kind, SCDType2ByTimeKind):
                columns_to_types[model.kind.updated_at_name] = model.kind.time_data_type
            self.adapter.create_table(
                name,
                columns_to_types=columns_to_types,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.table_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )
        else:
            # We assume that the data type for `updated_at_name` matches the data type that is defined for
            # `time_data_type`. If that isn't the case, then the user might get an error about not being able
            # to do comparisons across different data types
            super().create(
                snapshot,
                name,
                is_table_deployable,
                is_snapshot_deployable,
                **render_kwargs,
            )

    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        if isinstance(model.kind, SCDType2ByTimeKind):
            self.adapter.scd_type_2_by_time(
                target_table=name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_name=model.kind.valid_from_name,
                valid_to_name=model.kind.valid_to_name,
                updated_at_name=model.kind.updated_at_name,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                updated_at_as_valid_from=model.kind.updated_at_as_valid_from,
                columns_to_types=model.columns_to_types,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                **kwargs,
            )
        elif isinstance(model.kind, SCDType2ByColumnKind):
            self.adapter.scd_type_2_by_column(
                target_table=name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_name=model.kind.valid_from_name,
                valid_to_name=model.kind.valid_to_name,
                check_columns=model.kind.columns,
                columns_to_types=model.columns_to_types,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                execution_time_as_valid_from=model.kind.execution_time_as_valid_from,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                **kwargs,
            )
        else:
            raise SQLMeshError(
                f"Unexpected SCD Type 2 kind: {model.kind}. This is not expected and please report this as a bug."
            )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        if isinstance(model.kind, SCDType2ByTimeKind):
            self.adapter.scd_type_2_by_time(
                target_table=table_name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_name=model.kind.valid_from_name,
                valid_to_name=model.kind.valid_to_name,
                updated_at_name=model.kind.updated_at_name,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                updated_at_as_valid_from=model.kind.updated_at_as_valid_from,
                columns_to_types=model.columns_to_types,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                **kwargs,
            )
        elif isinstance(model.kind, SCDType2ByColumnKind):
            self.adapter.scd_type_2_by_column(
                target_table=table_name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_name=model.kind.valid_from_name,
                valid_to_name=model.kind.valid_to_name,
                check_columns=model.kind.columns,
                columns_to_types=model.columns_to_types,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                execution_time_as_valid_from=model.kind.execution_time_as_valid_from,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                **kwargs,
            )
        else:
            raise SQLMeshError(
                f"Unexpected SCD Type 2 kind: {model.kind}. This is not expected and please report this as a bug."
            )


class ViewStrategy(PromotableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        if (
            (
                (
                    isinstance(query_or_df, exp.Expression)
                    and snapshot.is_materialized_view
                    and deployability_index.is_deployable(snapshot)
                    and model.render_query(
                        snapshots=snapshots,
                        deployability_index=deployability_index,
                        engine_adapter=self.adapter,
                    )
                    == query_or_df
                )
                or self.adapter.HAS_VIEW_BINDING
            )
            and snapshot.intervals  # Re-create the view during the first evaluation.
            and self.adapter.table_exists(name)
        ):
            logger.info("Skipping creation of the view '%s'", name)
            return

        logger.info("Replacing view '%s'", name)
        self.adapter.create_view(
            name,
            query_or_df,
            model.columns_to_types,
            materialized=self._is_materialized_view(model),
            table_properties=model.table_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex],
        **kwargs: t.Any,
    ) -> None:
        raise ConfigError(f"Cannot append to a view '{table_name}'.")

    def create(
        self,
        snapshot: Snapshot,
        name: str,
        is_table_deployable: bool,
        is_snapshot_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        model = snapshot.model

        logger.info("Creating view '%s'", name)
        self.adapter.create_view(
            name,
            model.render_query_or_raise(**render_kwargs),
            materialized=self._is_materialized_view(model),
            table_properties=model.table_properties,
            table_description=model.description if is_table_deployable else None,
            column_descriptions=model.column_descriptions if is_table_deployable else None,
        )

    def migrate(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        target_table_name: str,
        source_table_name: str,
    ) -> None:
        logger.info("Migrating view '%s'", target_table_name)
        model = snapshot.model
        self.adapter.create_view(
            target_table_name,
            model.render_query_or_raise(
                execution_time=now(), snapshots=snapshots, engine_adapter=self.adapter
            ),
            model.columns_to_types,
            materialized=self._is_materialized_view(model),
            table_properties=model.table_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )

    def delete(self, name: str) -> None:
        try:
            self.adapter.drop_view(name)
        except Exception:
            logger.debug(
                "Failed to drop view '%s'. Trying to drop the materialized view instead",
                name,
                exc_info=True,
            )
            self.adapter.drop_view(name, materialized=True)
        logger.info("Dropped view '%s'", name)

    def _is_materialized_view(self, model: Model) -> bool:
        return isinstance(model.kind, ViewKind) and model.kind.materialized
