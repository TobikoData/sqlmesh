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
import sys
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
    SeedModel,
    SCDType2ByColumnKind,
    SCDType2ByTimeKind,
    ViewKind,
)
from sqlmesh.core.schema_diff import has_drop_alteration
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Intervals,
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

if sys.version_info >= (3, 12):
    from importlib import metadata
else:
    import importlib_metadata as metadata  # type: ignore

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
        batch_index: int = 0,
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
            batch_index: If the snapshot is part of a batch of related snapshots; which index in the batch is it
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
            batch_index=batch_index,
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
                s.qualified_view_name.table_for_environment(
                    environment_naming_info, dialect=self.adapter.dialect
                )
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
        allow_destructive_snapshots: t.Set[str] = set(),
    ) -> None:
        """Creates a physical snapshot schema and table for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshots.
            snapshots: Mapping of snapshot ID to snapshot.
            deployability_index: Determines snapshots that are deployable in the context of this creation.
            on_complete: A callback to call on each successfully created snapshot.
            allow_destructive_snapshots: Set of snapshots that are allowed to have destructive schema changes.
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
            if table_names - existing_objects or (snapshot.is_seed and not snapshot.intervals):
                snapshots_to_create.append(snapshot)
            elif on_complete:
                on_complete(snapshot)

        if not snapshots_to_create:
            return

        self._create_schemas(tables_by_schema)
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                snapshots_to_create,
                lambda s: self._create_snapshot(
                    s, snapshots, deployability_index, on_complete, allow_destructive_snapshots
                ),
                self.ddl_concurrent_tasks,
            )

    def migrate(
        self,
        target_snapshots: t.Iterable[Snapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
        allow_destructive_snapshots: t.Set[str] = set(),
    ) -> None:
        """Alters a physical snapshot table to match its snapshot's schema for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshots.
            snapshots: Mapping of snapshot ID to snapshot.
            allow_destructive_snapshots: Set of snapshots that are allowed to have destructive schema changes.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._migrate_snapshot(s, snapshots, allow_destructive_snapshots),
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
        batch_index: int = 0,
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
            batch_index: If the snapshot is part of a batch of related snapshots; which index in the batch is it
            kwargs: Additional kwargs to pass to the renderer.
        """
        if not snapshot.is_model or snapshot.is_seed:
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

        # https://github.com/TobikoData/sqlmesh/issues/2609
        # If there are no existing intervals yet; only consider this a first insert for the first snapshot in the batch
        is_first_insert = not _intervals(snapshot, deployability_index) and batch_index == 0

        def apply(query_or_df: QueryOrDF, index: int = 0) -> None:
            if index > 0:
                evaluation_strategy.append(
                    table_name=table_name,
                    query_or_df=query_or_df,
                    model=snapshot.model,
                    snapshot=snapshot,
                    snapshots=snapshots,
                    deployability_index=deployability_index,
                    batch_index=batch_index,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                )
            else:
                logger.info("Inserting batch (%s, %s) into %s'", start, end, table_name)
                evaluation_strategy.insert(
                    table_name=table_name,
                    query_or_df=query_or_df,
                    is_first_insert=is_first_insert,
                    model=snapshot.model,
                    snapshot=snapshot,
                    snapshots=snapshots,
                    deployability_index=deployability_index,
                    batch_index=batch_index,
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
                if isinstance(query_or_df, pd.DataFrame):
                    return query_or_df.head(limit)
                if not isinstance(query_or_df, exp.Expression):
                    # We assume that if this branch is reached, `query_or_df` is a pyspark / snowpark dataframe,
                    # so we use `limit` instead of `head` to get back a dataframe instead of List[Row]
                    # https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.head.html#pyspark.sql.DataFrame.head
                    return query_or_df.limit(limit)

                assert isinstance(query_or_df, exp.Query)

                existing_limit = query_or_df.args.get("limit")
                if existing_limit:
                    limit = min(limit, execute(exp.select(existing_limit.expression)).rows[0][0])
                    assert limit is not None

                return self.adapter._fetch_native_df(query_or_df.limit(limit))

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
                        if isinstance(a, pd.DataFrame)
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
        allow_destructive_snapshots: t.Set[str],
    ) -> None:
        if not snapshot.is_model:
            return

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }
        parent_snapshots_by_name[snapshot.name] = snapshot

        deployability_index = deployability_index or DeployabilityIndex.all_deployable()

        common_render_kwargs: t.Dict[str, t.Any] = dict(
            engine_adapter=self.adapter,
            snapshots=parent_snapshots_by_name,
            runtime_stage=RuntimeStage.CREATING,
        )
        pre_post_render_kwargs = dict(
            **common_render_kwargs,
            deployability_index=deployability_index.with_deployable(snapshot),
        )
        create_render_kwargs = dict(
            **common_render_kwargs,
            # Refers to self as non-deployable to successfully create self-referential tables / views.
            deployability_index=deployability_index.with_non_deployable(snapshot),
        )

        # It can still be useful for some strategies to know if the snapshot was actually deployable
        is_snapshot_deployable = deployability_index.is_deployable(snapshot)

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        with self.adapter.transaction(), self.adapter.session(snapshot.model.session_properties):
            self.adapter.execute(snapshot.model.render_pre_statements(**pre_post_render_kwargs))

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
                    table_name=tmp_table_name,
                    model=snapshot.model,
                    is_table_deployable=False,
                    render_kwargs=dict(
                        table_mapping={snapshot.name: tmp_table_name},
                        **create_render_kwargs,
                    ),
                    is_snapshot_deployable=is_snapshot_deployable,
                )
                try:
                    self.adapter.clone_table(target_table_name, snapshot.table_name(), replace=True)
                    alter_expressions = self.adapter.get_alter_expressions(
                        target_table_name, tmp_table_name
                    )
                    _check_destructive_schema_change(
                        snapshot, alter_expressions, allow_destructive_snapshots
                    )
                    self.adapter.alter_table(alter_expressions)
                except Exception:
                    self.adapter.drop_table(target_table_name)
                    raise
                finally:
                    self.adapter.drop_table(tmp_table_name)
            else:
                table_deployability_flags = [False]
                if not snapshot.reuses_previous_version:
                    table_deployability_flags.append(True)
                for is_table_deployable in table_deployability_flags:
                    evaluation_strategy.create(
                        table_name=snapshot.table_name(is_deployable=is_table_deployable),
                        model=snapshot.model,
                        is_table_deployable=is_table_deployable,
                        render_kwargs=create_render_kwargs,
                        is_snapshot_deployable=is_snapshot_deployable,
                    )

            self.adapter.execute(snapshot.model.render_post_statements(**pre_post_render_kwargs))

        if on_complete is not None:
            on_complete(snapshot)

    def _migrate_snapshot(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[SnapshotId, Snapshot],
        allow_destructive_snapshots: t.Set[str],
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
            target_table_name=target_table_name,
            source_table_name=tmp_table_name,
            snapshot=snapshot,
            snapshots=parent_snapshots_by_name,
            allow_destructive_snapshots=allow_destructive_snapshots,
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
            view_name = snapshot.qualified_view_name.for_environment(
                environment_naming_info, dialect=self.adapter.dialect
            )
            _evaluation_strategy(snapshot, self.adapter).promote(
                table_name=table_name,
                view_name=view_name,
                model=snapshot.model,
                environment=environment_naming_info.name,
            )

        if on_complete is not None:
            on_complete(snapshot)

    def _demote_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        environment_naming_info: EnvironmentNamingInfo,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        view_name = snapshot.qualified_view_name.for_environment(
            environment_naming_info, dialect=self.adapter.dialect
        )
        _evaluation_strategy(snapshot, self.adapter).demote(view_name)

        if on_complete is not None:
            on_complete(snapshot)

    def _cleanup_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        dev_table_only: bool,
        on_complete: t.Optional[t.Callable[[str], None]],
    ) -> None:
        snapshot = snapshot.table_info

        table_names = [(False, snapshot.table_name(is_deployable=False))]
        if not dev_table_only:
            table_names.append((True, snapshot.table_name(is_deployable=True)))

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        for is_table_deployable, table_name in table_names:
            evaluation_strategy.delete(
                table_name,
                is_table_deployable=is_table_deployable,
                physical_schema=snapshot.physical_schema,
            )

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

        # Model's "blocking" argument takes precedence over the audit's default setting
        blocking = audit_args.pop("blocking", None)
        blocking = blocking == exp.true() if blocking else audit.blocking

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
        )  # type: ignore
        if count and raise_exception:
            audit_error = AuditError(
                audit_name=audit.name,
                model=snapshot.model_or_none,
                count=count,
                query=query,
                adapter_dialect=self.adapter.dialect,
            )
            if blocking:
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
    elif snapshot.is_full:
        klass = FullRefreshStrategy
    elif snapshot.is_seed:
        klass = SeedStrategy
    elif snapshot.is_incremental_by_time_range:
        klass = IncrementalByTimeRangeStrategy
    elif snapshot.is_incremental_by_unique_key:
        klass = IncrementalByUniqueKeyStrategy
    elif snapshot.is_incremental_by_partition:
        klass = IncrementalByPartitionStrategy
    elif snapshot.is_incremental_unmanaged:
        klass = IncrementalUnmanagedStrategy
    elif snapshot.is_view:
        klass = ViewStrategy
    elif snapshot.is_scd_type_2:
        klass = SCDType2Strategy
    elif snapshot.is_custom:
        if snapshot.custom_materialization is None:
            raise SQLMeshError(
                f"Missing the name of a custom evaluation strategy in model '{snapshot.name}'."
            )
        klass = get_custom_materialization_type(snapshot.custom_materialization)
        return klass(adapter)
    elif snapshot.is_managed:
        klass = EngineManagedStrategy
    else:
        raise SQLMeshError(f"Unexpected snapshot: {snapshot}")

    return klass(adapter)


class EvaluationStrategy(abc.ABC):
    def __init__(self, adapter: EngineAdapter):
        self.adapter = adapter

    @abc.abstractmethod
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        """Inserts the given query or a DataFrame into the target table or a view.

        Args:
            table_name: The name of the target table or view.
            query_or_df: A query or a DataFrame to insert.
            model: The target model.
            is_first_insert: Whether this is the first insert for this version of a model. This value is set to True
                if no data has been previously inserted into the target table, or when the entire history of the target model has
                been restated. Note that in the latter case, the table might contain data from previous executions, and it is the
                responsibility of a specific evaluation strategy to handle the truncation of the table if necessary.
        """

    @abc.abstractmethod
    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        """Appends the given query or a DataFrame to the existing table.

        Args:
            table_name: The target table name.
            query_or_df: A query or a DataFrame to insert.
            model: The target model.
        """

    @abc.abstractmethod
    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        """Creates the target table or view.

        Note that the intention here is to just create the table structure, data is loaded in insert() and append()

        Args:
            table_name: The name of a table or a view.
            model: The target model.
            is_table_deployable: True if this creation request is for the "main" table that *might* be deployed to a production environment.
                False if this creation request is for the "dev preview" table. Note that this flag is not related to the DeployabilityIndex
                which determines if the snapshot is deployable to production or not
            render_kwargs: Additional key-value arguments to pass when rendering the model's query.
        """

    @abc.abstractmethod
    def migrate(
        self,
        target_table_name: str,
        source_table_name: str,
        snapshot: Snapshot,
        **kwargs: t.Any,
    ) -> None:
        """Migrates the target table schema so that it corresponds to the source table schema.

        Args:
            target_table_name: The target table name.
            source_table_name: The source table name.
            snapshot: The target snapshot.
        """

    @abc.abstractmethod
    def delete(self, name: str, **kwargs: t.Any) -> None:
        """Deletes a target table or a view.

        Args:
            name: The name of a table or a view.
        """

    @abc.abstractmethod
    def promote(
        self,
        table_name: str,
        view_name: str,
        model: Model,
        environment: str,
        **kwargs: t.Any,
    ) -> None:
        """Updates the target view to point to the target table.

        Args:
            table_name: The name of a table in the physical layer that is being promoted.
            view_name: The name of the target view in the virtual layer.
            model: The model that is being promoted.
            environment: The name of the target environment.
        """

    @abc.abstractmethod
    def demote(self, view_name: str, **kwargs: t.Any) -> None:
        """Deletes the target view in the virtual layer.

        Args:
            view_name: The name of the target view in the virtual layer.
        """

    def _replace_query_for_model(self, model: Model, name: str, query_or_df: QueryOrDF) -> None:
        """Replaces the table for the given model.

        Args:
            model: The target model.
            name: The name of the target table.
            query_or_df: The query or DataFrame to replace the target table with.
        """
        self.adapter.replace_query(
            name,
            query_or_df,
            columns_to_types=model.columns_to_types if model.annotated else None,
            storage_format=model.storage_format,
            partitioned_by=model.partitioned_by,
            partition_interval_unit=model.interval_unit,
            clustered_by=model.clustered_by,
            table_properties=model.physical_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )


class SymbolicStrategy(EvaluationStrategy):
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        pass

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        pass

    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        pass

    def migrate(
        self,
        target_table_name: str,
        source_table_name: str,
        snapshot: Snapshot,
        **kwarg: t.Any,
    ) -> None:
        pass

    def delete(self, name: str, **kwargs: t.Any) -> None:
        pass

    def promote(
        self,
        table_name: str,
        view_name: str,
        model: Model,
        environment: str,
        **kwargs: t.Any,
    ) -> None:
        pass

    def demote(self, view_name: str, **kwargs: t.Any) -> None:
        pass


class EmbeddedStrategy(SymbolicStrategy):
    def promote(
        self,
        table_name: str,
        view_name: str,
        model: Model,
        environment: str,
        **kwargs: t.Any,
    ) -> None:
        logger.info("Dropping view '%s' for non-materialized table", view_name)
        self.adapter.drop_view(view_name, cascade=False)


class PromotableStrategy(EvaluationStrategy):
    def promote(
        self,
        table_name: str,
        view_name: str,
        model: Model,
        environment: str,
        **kwargs: t.Any,
    ) -> None:
        is_prod = environment == c.PROD
        logger.info("Updating view '%s' to point at table '%s'", view_name, table_name)
        self.adapter.create_view(
            view_name,
            exp.select("*").from_(table_name, dialect=self.adapter.dialect),
            table_description=model.description if is_prod else None,
            column_descriptions=model.column_descriptions if is_prod else None,
            view_properties=model.virtual_properties,
        )

    def demote(self, view_name: str, **kwargs: t.Any) -> None:
        logger.info("Dropping view '%s'", view_name)
        self.adapter.drop_view(view_name, cascade=False)


class MaterializableStrategy(PromotableStrategy):
    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        self.adapter.insert_append(table_name, query_or_df, columns_to_types=model.columns_to_types)

    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        ctas_query = model.ctas_query(**render_kwargs)

        logger.info("Creating table '%s'", table_name)
        if model.annotated:
            self.adapter.create_table(
                table_name,
                columns_to_types=model.columns_to_types_or_raise,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )

            # Only sql models have queries that can be tested.
            # Additionally, we always create temp tables and sometimes we additionally created prod tables,
            # we need to make sure that we only dry run once.
            # We also need to make sure that we don't dry run on Redshift because its planner / optimizer sometimes
            # breaks on our CTAS queries due to us relying on the WHERE FALSE LIMIT 0 combo.
            if model.is_sql and not is_table_deployable and self.adapter.dialect != "redshift":
                logger.info("Dry running model '%s'", model.name)
                self.adapter.fetchall(ctas_query)
        else:
            self.adapter.ctas(
                table_name,
                ctas_query,
                model.columns_to_types,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )

    def migrate(
        self,
        target_table_name: str,
        source_table_name: str,
        snapshot: Snapshot,
        **kwargs: t.Any,
    ) -> None:
        logger.info(f"Altering table '{target_table_name}'")
        alter_expressions = self.adapter.get_alter_expressions(target_table_name, source_table_name)
        _check_destructive_schema_change(
            snapshot, alter_expressions, kwargs["allow_destructive_snapshots"]
        )
        self.adapter.alter_table(alter_expressions)

    def delete(self, name: str, **kwargs: t.Any) -> None:
        _check_table_db_is_physical_schema(name, kwargs["physical_schema"])
        self.adapter.drop_table(name)
        logger.info("Dropped table '%s'", name)


class IncrementalByPartitionStrategy(MaterializableStrategy):
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        self.adapter.insert_overwrite_by_partition(
            table_name,
            query_or_df,
            partitioned_by=model.partitioned_by,
            columns_to_types=model.columns_to_types,
        )


class IncrementalByTimeRangeStrategy(MaterializableStrategy):
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        assert model.time_column
        self.adapter.insert_overwrite_by_time_partition(
            table_name,
            query_or_df,
            time_formatter=model.convert_to_time_column,
            time_column=model.time_column,
            columns_to_types=model.columns_to_types,
            **kwargs,
        )


class IncrementalByUniqueKeyStrategy(MaterializableStrategy):
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        if is_first_insert:
            self._replace_query_for_model(model, table_name, query_or_df)
        else:
            self.adapter.merge(
                table_name,
                query_or_df,
                columns_to_types=model.columns_to_types,
                unique_key=model.unique_key,
                when_matched=model.when_matched,
            )

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
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
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        if is_first_insert:
            self._replace_query_for_model(model, table_name, query_or_df)
        elif isinstance(model.kind, IncrementalUnmanagedKind) and model.kind.insert_overwrite:
            self.adapter.insert_overwrite_by_partition(
                table_name,
                query_or_df,
                model.partitioned_by,
                columns_to_types=model.columns_to_types,
            )
        else:
            self.append(
                table_name,
                query_or_df,
                model,
                **kwargs,
            )


class FullRefreshStrategy(MaterializableStrategy):
    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        self._replace_query_for_model(model, table_name, query_or_df)


class SeedStrategy(MaterializableStrategy):
    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        model = t.cast(SeedModel, model)
        if not model.is_hydrated and self.adapter.table_exists(table_name):
            # This likely means that the table was created and populated previously, but the evaluation stage
            # failed before the interval could be added for this model.
            logger.warning(
                "Seed model '%s' is not hydrated, but the table '%s' exists. Skipping creation",
                model.name,
                table_name,
            )
            return

        super().create(table_name, model, is_table_deployable, render_kwargs, **kwargs)
        if is_table_deployable:
            # For seeds we insert data at the time of table creation.
            try:
                for index, df in enumerate(model.render_seed()):
                    if index == 0:
                        self._replace_query_for_model(model, table_name, df)
                    else:
                        self.adapter.insert_append(
                            table_name, df, columns_to_types=model.columns_to_types
                        )
            except Exception:
                self.adapter.drop_table(table_name)
                raise

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        # Data has already been inserted at the time of table creation.
        pass


class SCDType2Strategy(MaterializableStrategy):
    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        assert isinstance(model.kind, (SCDType2ByTimeKind, SCDType2ByColumnKind))
        if model.annotated:
            logger.info("Creating table '%s'", table_name)
            columns_to_types = model.columns_to_types_or_raise
            if isinstance(model.kind, SCDType2ByTimeKind):
                columns_to_types[model.kind.updated_at_name.name] = model.kind.time_data_type
            self.adapter.create_table(
                table_name,
                columns_to_types=columns_to_types,
                storage_format=model.storage_format,
                partitioned_by=model.partitioned_by,
                partition_interval_unit=model.interval_unit,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description if is_table_deployable else None,
                column_descriptions=model.column_descriptions if is_table_deployable else None,
            )
        else:
            # We assume that the data type for `updated_at_name` matches the data type that is defined for
            # `time_data_type`. If that isn't the case, then the user might get an error about not being able
            # to do comparisons across different data types
            super().create(
                table_name,
                model,
                is_table_deployable,
                render_kwargs,
                **kwargs,
            )

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        if isinstance(model.kind, SCDType2ByTimeKind):
            self.adapter.scd_type_2_by_time(
                target_table=table_name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_col=model.kind.valid_from_name,
                valid_to_col=model.kind.valid_to_name,
                execution_time=kwargs["execution_time"],
                updated_at_col=model.kind.updated_at_name,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                updated_at_as_valid_from=model.kind.updated_at_as_valid_from,
                columns_to_types=model.columns_to_types,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                truncate=is_first_insert,
            )
        elif isinstance(model.kind, SCDType2ByColumnKind):
            self.adapter.scd_type_2_by_column(
                target_table=table_name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_col=model.kind.valid_from_name,
                valid_to_col=model.kind.valid_to_name,
                execution_time=kwargs["execution_time"],
                check_columns=model.kind.columns,
                invalidate_hard_deletes=model.kind.invalidate_hard_deletes,
                execution_time_as_valid_from=model.kind.execution_time_as_valid_from,
                columns_to_types=model.columns_to_types,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
                truncate=is_first_insert,
            )
        else:
            raise SQLMeshError(
                f"Unexpected SCD Type 2 kind: {model.kind}. This is not expected and please report this as a bug."
            )

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        if isinstance(model.kind, SCDType2ByTimeKind):
            self.adapter.scd_type_2_by_time(
                target_table=table_name,
                source_table=query_or_df,
                unique_key=model.unique_key,
                valid_from_col=model.kind.valid_from_name,
                valid_to_col=model.kind.valid_to_name,
                updated_at_col=model.kind.updated_at_name,
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
                valid_from_col=model.kind.valid_from_name,
                valid_to_col=model.kind.valid_to_name,
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
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        deployability_index = (
            kwargs.get("deployability_index") or DeployabilityIndex.all_deployable()
        )
        snapshot = kwargs["snapshot"]
        snapshots = kwargs["snapshots"]
        if (
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
        ) and self.adapter.table_exists(table_name):
            logger.info("Skipping creation of the view '%s'", table_name)
            return

        logger.info("Replacing view '%s'", table_name)
        self.adapter.create_view(
            table_name,
            query_or_df,
            model.columns_to_types,
            replace=not self.adapter.HAS_VIEW_BINDING,
            materialized=self._is_materialized_view(model),
            view_properties=model.physical_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        raise ConfigError(f"Cannot append to a view '{table_name}'.")

    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        is_snapshot_deployable: bool = kwargs["is_snapshot_deployable"]
        if not is_snapshot_deployable and is_table_deployable:
            # If the snapshot is not deployable, the query may contain references to non-deployable tables or views.
            # Therefore, we postpone the creation of the deployable view until the snapshot is deployed to production.
            logger.info(
                "Skipping creation of the deployable view '%s' for the non-deployable snapshot",
                table_name,
            )
            return

        if self.adapter.table_exists(table_name):
            # Make sure we don't recreate the view to prevent deletion of downstream views in engines with no late
            # binding support (because of DROP CASCADE).
            logger.info("View '%s' already exists", table_name)
            return

        logger.info("Creating view '%s'", table_name)
        self.adapter.create_view(
            table_name,
            model.render_query_or_raise(**render_kwargs),
            # Make sure we never replace the view during creation to avoid race conditions in engines with no late binding support.
            replace=False,
            materialized=self._is_materialized_view(model),
            view_properties=model.physical_properties,
            table_description=model.description if is_table_deployable else None,
            column_descriptions=model.column_descriptions if is_table_deployable else None,
        )

    def migrate(
        self,
        target_table_name: str,
        source_table_name: str,
        snapshot: Snapshot,
        **kwargs: t.Any,
    ) -> None:
        logger.info("Migrating view '%s'", target_table_name)
        model = snapshot.model
        self.adapter.create_view(
            target_table_name,
            model.render_query_or_raise(
                execution_time=now(), snapshots=kwargs["snapshots"], engine_adapter=self.adapter
            ),
            model.columns_to_types,
            materialized=self._is_materialized_view(model),
            view_properties=model.physical_properties,
            table_description=model.description,
            column_descriptions=model.column_descriptions,
        )

    def delete(self, name: str, **kwargs: t.Any) -> None:
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


class CustomMaterialization(MaterializableStrategy):
    """Base class for custom materializations."""

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        """Inserts the given query or a DataFrame into the target table or a view.

        Args:
            table_name: The name of the target table or view.
            query_or_df: A query or a DataFrame to insert.
            model: The target model.
            is_first_insert: Whether this is the first insert for this version of a model. This value is set to True
                if no data has been previously inserted into the target table, or when the entire history of the target model has
                been restated. Note that in the latter case, the table might contain data from previous executions, and it is the
                responsibility of a specific evaluation strategy to handle the truncation of the table if necessary.
        """
        raise NotImplementedError(
            "Custom materialization strategies must implement the 'insert' method."
        )


_custom_materialization_type_cache: t.Optional[t.Dict[str, t.Type[CustomMaterialization]]] = None


def get_custom_materialization_type(name: str) -> t.Type[CustomMaterialization]:
    global _custom_materialization_type_cache

    strategy_key = name.lower()

    if (
        _custom_materialization_type_cache is None
        or strategy_key not in _custom_materialization_type_cache
    ):
        strategy_types = list(CustomMaterialization.__subclasses__())

        entry_points = metadata.entry_points(group="sqlmesh.materializations")
        for entry_point in entry_points:
            strategy_type = entry_point.load()
            if not issubclass(strategy_type, CustomMaterialization):
                raise SQLMeshError(
                    f"Custom materialization entry point '{entry_point.name}' must be a subclass of CustomMaterialization."
                )
            strategy_types.append(strategy_type)

        _custom_materialization_type_cache = {
            getattr(strategy_type, "NAME", strategy_type.__name__).lower(): strategy_type
            for strategy_type in strategy_types
        }

    if strategy_key not in _custom_materialization_type_cache:
        raise ConfigError(f"Materialization strategy with name '{name}' was not found.")

    strategy_type = _custom_materialization_type_cache[strategy_key]
    logger.debug("Resolved custom materialization '%s' to '%s'", name, strategy_type)
    return strategy_type


class EngineManagedStrategy(MaterializableStrategy):
    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        is_snapshot_deployable: bool = kwargs["is_snapshot_deployable"]

        if is_table_deployable and is_snapshot_deployable:
            # We could deploy this to prod; create a proper managed table
            logger.info("Creating managed table: %s", table_name)
            self.adapter.create_managed_table(
                table_name=table_name,
                query=model.render_query_or_raise(**render_kwargs),
                columns_to_types=model.columns_to_types,
                partitioned_by=model.partitioned_by,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
            )
        elif not is_table_deployable:
            # Only create the dev preview table as a normal table.
            # For the main table, if the snapshot is cant be deployed to prod (eg upstream is forward-only) do nothing.
            # Any downstream models that reference it will be updated to point to the dev preview table.
            # If the user eventually tries to deploy it, the logic in insert() will see it doesnt exist and create it
            super().create(
                table_name=table_name,
                model=model,
                is_table_deployable=is_table_deployable,
                render_kwargs=render_kwargs,
                **kwargs,
            )

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        deployability_index: DeployabilityIndex = kwargs["deployability_index"]
        snapshot: Snapshot = kwargs["snapshot"]
        is_snapshot_deployable = deployability_index.is_deployable(snapshot)

        if is_first_insert and is_snapshot_deployable and not self.adapter.table_exists(table_name):
            self.adapter.create_managed_table(
                table_name=table_name,
                query=query_or_df,  # type: ignore
                columns_to_types=model.columns_to_types,
                partitioned_by=model.partitioned_by,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description,
                column_descriptions=model.column_descriptions,
            )
        elif not is_snapshot_deployable:
            # Snapshot isnt deployable; update the preview table instead
            # If the snapshot was deployable, then data would have already been loaded in create() because a managed table would have been created
            logger.info(
                "Updating preview table: %s (for managed model: %s)", table_name, model.name
            )
            self._replace_query_for_model(model=model, name=table_name, query_or_df=query_or_df)

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        raise ConfigError(f"Cannot append to a managed table '{table_name}'.")

    def migrate(
        self,
        target_table_name: str,
        source_table_name: str,
        snapshot: Snapshot,
        **kwargs: t.Any,
    ) -> None:
        # Not entirely true, many engines support modifying some of the metadata fields on a managed table
        # eg Snowflake allows you to ALTER DYNAMIC TABLE foo SET WAREHOUSE=my_other_wh;
        raise ConfigError(f"Cannot mutate managed table: {target_table_name}")

    def delete(self, name: str, **kwargs: t.Any) -> None:
        # a dev preview table is created as a normal table, so it needs to be dropped as a normal table
        _check_table_db_is_physical_schema(name, kwargs["physical_schema"])
        if kwargs["is_table_deployable"]:
            self.adapter.drop_managed_table(name)
            logger.info("Dropped managed table '%s'", name)
        else:
            self.adapter.drop_table(name)
            logger.info("Dropped dev preview for managed table '%s'", name)


def _intervals(snapshot: Snapshot, deployability_index: DeployabilityIndex) -> Intervals:
    return (
        snapshot.intervals
        if deployability_index.is_deployable(snapshot)
        else snapshot.dev_intervals
    )


def _check_destructive_schema_change(
    snapshot: Snapshot,
    alter_expressions: t.List[exp.Alter],
    allow_destructive_snapshots: t.Set[str],
) -> None:
    if snapshot.needs_destructive_check(allow_destructive_snapshots) and has_drop_alteration(
        alter_expressions
    ):
        warning_msg = (
            f"Plan results in a destructive change to forward-only table '{snapshot.name}'s schema."
        )
        if snapshot.model.on_destructive_change.is_warn:
            logger.warning(warning_msg)
            return
        raise SQLMeshError(
            f"{warning_msg} To allow this, change the model's `on_destructive_change` setting to `warn` or `allow` or include it in the plan's `--allow-destructive-model` option."
        )


def _check_table_db_is_physical_schema(table_name: str, physical_schema: str) -> None:
    table = exp.to_table(table_name)
    if table.db != physical_schema:
        raise SQLMeshError(
            f"Table '{table_name}' is not a part of the physical schema '{physical_schema}' and so can't be dropped."
        )
