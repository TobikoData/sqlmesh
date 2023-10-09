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
from contextlib import contextmanager
from functools import reduce

import pandas as pd
from sqlglot import exp, select
from sqlglot.executor import execute

from sqlmesh.core.audit import Audit, AuditResult
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.model import IncrementalUnmanagedKind, Model, SCDType2Kind, ViewKind
from sqlmesh.core.snapshot import (
    QualifiedViewName,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotInfoLike,
)
from sqlmesh.utils.concurrency import concurrent_apply_to_snapshots
from sqlmesh.utils.date import TimeLike, now
from sqlmesh.utils.errors import AuditError, ConfigError, SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.console import Console
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
        ddl_concurrent_task: The number of concurrent tasks used for DDL
            operations (table / view creation, deletion, etc). Default: 1.
    """

    def __init__(
        self,
        adapter: EngineAdapter,
        ddl_concurrent_tasks: int = 1,
        console: t.Optional[Console] = None,
    ):
        self.adapter = adapter
        self.ddl_concurrent_tasks = ddl_concurrent_tasks

        from sqlmesh.core.console import get_console

        self.console = console or get_console()

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        snapshots: t.Dict[str, Snapshot],
        limit: t.Optional[int] = None,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> t.Optional[DF]:
        """Evaluate a snapshot, creating its schema and table if it doesn't exist and then inserting it.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by name) to use for expansion and mapping of physical locations.
            limit: If limit is > 0, the query will not be persisted but evaluated and returned as a dataframe.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.
        """
        if not snapshot.is_model:
            return None

        model = snapshot.model

        if not limit and not snapshot.is_forward_only:
            self._ensure_no_paused_forward_only_upstream(snapshot, snapshots)

        logger.info("Evaluating snapshot %s", snapshot.snapshot_id)

        table_name = "" if limit else snapshot.table_name(is_dev=is_dev)

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        def apply(query_or_df: QueryOrDF, index: int = 0) -> None:
            if index > 0:
                evaluation_strategy.append(
                    snapshot,
                    table_name,
                    query_or_df,
                    snapshots,
                    is_dev,
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
                    is_dev,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                )

        from sqlmesh.core.context import ExecutionContext

        common_render_kwargs = dict(
            start=start,
            end=end,
            execution_time=execution_time,
            has_intervals=bool(snapshot.intervals),
            **kwargs,
        )

        render_statements_kwargs = dict(
            engine_adapter=self.adapter,
            snapshots=snapshots,
            is_dev=is_dev,
            **common_render_kwargs,
        )

        with self.adapter.transaction(), self.adapter.session():
            if not limit:
                self.adapter.execute(model.render_pre_statements(**render_statements_kwargs))

            queries_or_dfs = model.render(
                context=ExecutionContext(self.adapter, snapshots, is_dev), **common_render_kwargs
            )

            if limit and limit > 0:
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
                    lambda a, b: pd.concat([a, b], ignore_index=True)  # type: ignore
                    if self.adapter.is_pandas_df(a)
                    else a.union_all(b),  # type: ignore
                    queries_or_dfs,
                )
                apply(query_or_df, index=0)
            else:
                for index, query_or_df in enumerate(queries_or_dfs):
                    apply(query_or_df, index)

            if not limit:
                self.adapter.execute(model.render_post_statements(**render_statements_kwargs))

            return None

    def promote(
        self,
        target_snapshots: t.Iterable[Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        is_dev: bool = False,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Promotes the given collection of snapshots in the target environment by replacing a corresponding
        view with a physical table associated with the given snapshot.

        Args:
            target_snapshots: Snapshots to promote.
            environment_naming_info: Naming information for the target environment.
            is_dev: Indicates whether the promotion happens in the development mode and temporary
                tables / table clones should be used where applicable.
            on_complete: A callback to call on each successfully promoted snapshot.
        """
        self._create_schemas(
            [
                s.qualified_view_name.table_for_environment(environment_naming_info)
                for s in target_snapshots
                if s.is_model and not s.is_symbolic
            ]
        )
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._promote_snapshot(s, environment_naming_info, is_dev, on_complete),
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
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Creates a physical snapshot schema and table for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshots.
            snapshots: Mapping of snapshot ID to snapshot.
            on_complete: A callback to call on each successfully created snapshot.
        """
        self._create_schemas(
            [s.table_name() for s in target_snapshots if s.is_model and not s.is_symbolic]
        )
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._create_snapshot(s, snapshots, on_complete),
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

    def cleanup(self, target_snapshots: t.Iterable[SnapshotInfoLike]) -> None:
        """Cleans up the given snapshots by removing its table

        Args:
            target_snapshots: Snapshots to cleanup.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                self._cleanup_snapshot,
                self.ddl_concurrent_tasks,
                reverse_order=True,
            )

    def audit(
        self,
        *,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        raise_exception: bool = True,
        is_dev: bool = False,
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
            is_dev: Indicates whether the auditing happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.
        """
        if snapshot.is_temporary_table(is_dev):
            # We can't audit a temporary table.
            return []

        if not snapshot.version:
            raise ConfigError(
                f"Cannot audit '{snapshot.name}' because it has not been versioned yet. Apply a plan first."
            )

        logger.info("Auditing snapshot %s", snapshot.snapshot_id)

        results = []
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
                    is_dev=is_dev,
                    **kwargs,
                )
            )
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

    def _create_snapshot(
        self,
        snapshot: Snapshot,
        snapshots: t.Dict[SnapshotId, Snapshot],
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        if not snapshot.is_model:
            return

        # If a snapshot reuses an existing version we assume that the table for that version
        # has already been created, so we only need to create a temporary table or a clone.
        is_dev = snapshot.is_forward_only or snapshot.is_indirect_non_breaking
        target_table_name = snapshot.table_name(is_dev=is_dev)

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }
        parent_snapshots_by_name[snapshot.name] = snapshot

        render_kwargs: t.Dict[str, t.Any] = dict(
            engine_adapter=self.adapter,
            snapshots=parent_snapshots_by_name,
            is_dev=is_dev,
        )

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        with self.adapter.transaction(), self.adapter.session():
            self.adapter.execute(snapshot.model.render_pre_statements(**render_kwargs))

            if is_dev and not snapshot.previous_versions:
                # This is a FORWARD_ONLY snapshot which represents an added model. This means
                # that the physical table associated with it doesn't exist yet.
                logger.info(
                    f"Detected a forward-only snapshot without previous versions: {snapshot.snapshot_id}"
                )
                evaluation_strategy.create(snapshot, target_table_name, **render_kwargs)
                evaluation_strategy.create(snapshot, snapshot.table_name(), **render_kwargs)
            elif is_dev and snapshot.is_materialized and self.adapter.SUPPORTS_CLONING:
                tmp_table_name = f"{target_table_name}__schema_migration_source"
                source_table_name = snapshot.table_name()

                logger.info(f"Cloning table '{source_table_name}' into '{target_table_name}'")

                evaluation_strategy.create(snapshot, tmp_table_name, **render_kwargs)
                try:
                    self.adapter.clone_table(target_table_name, snapshot.table_name(), replace=True)
                    self.adapter.alter_table(target_table_name, tmp_table_name)
                finally:
                    self.adapter.drop_table(tmp_table_name)
            else:
                evaluation_strategy.create(snapshot, target_table_name, **render_kwargs)

            self.adapter.execute(snapshot.model.render_post_statements(**render_kwargs))

        if on_complete is not None:
            on_complete(snapshot)

    def _migrate_snapshot(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> None:
        if not snapshot.is_paused or snapshot.change_category not in (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        ):
            return

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }
        parent_snapshots_by_name[snapshot.name] = snapshot

        tmp_table_name = snapshot.table_name(is_dev=True)
        target_table_name = snapshot.table_name()
        _evaluation_strategy(snapshot, self.adapter).migrate(
            snapshot, parent_snapshots_by_name, target_table_name, tmp_table_name
        )

    def _promote_snapshot(
        self,
        snapshot: Snapshot,
        environment_naming_info: EnvironmentNamingInfo,
        is_dev: bool,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        table_name = snapshot.table_name_for_mapping(is_dev=is_dev)
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

    def _cleanup_snapshot(self, snapshot: SnapshotInfoLike) -> None:
        snapshot = snapshot.table_info

        table_name = snapshot.table_name()
        dev_table_name = snapshot.table_name(is_dev=True)

        table_names = [table_name]
        if table_name != dev_table_name:
            table_names.append(dev_table_name)

        evaluation_strategy = _evaluation_strategy(snapshot, self.adapter)

        for table_name in table_names:
            table = exp.to_table(table_name)
            if table.db != snapshot.physical_schema:
                raise SQLMeshError(
                    f"Table '{table_name}' is not a part of the physical schema '{snapshot.physical_schema}' and so can't be dropped."
                )
            evaluation_strategy.delete(table_name)

    def _ensure_no_paused_forward_only_upstream(
        self, snapshot: Snapshot, parent_snapshots: t.Dict[str, Snapshot]
    ) -> None:
        for p in parent_snapshots.values():
            if p.is_forward_only and p.is_paused:
                raise SQLMeshError(
                    f"Snapshot {snapshot.snapshot_id} depends on a paused forward-only snapshot {p.snapshot_id}. Create and apply a new plan to fix this issue."
                )

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
        is_dev: bool,
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
            is_dev=is_dev,
            engine_adapter=self.adapter,
            **audit_args,
            **kwargs,
        )
        count, *_ = self.adapter.fetchone(
            select("COUNT(*)").from_(query.subquery("audit")), quote_identifiers=True
        )
        if count and raise_exception:
            audit_error = AuditError(
                audit_name=audit.name,
                model=snapshot.model_or_none,
                count=count,
                query=query,
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
        unique_schemas = {(t.db, t.args.get("catalog")) for t in table_exprs if t and t.db}
        # Create schemas sequentially, since some engines (eg. Postgres) may not support concurrent creation
        # of schemas with the same name.
        for schema, catalog in unique_schemas:
            logger.info("Creating schema '%s'", schema)
            self.adapter.create_schema(schema, catalog_name=catalog)


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
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        """Inserts the given query or a DataFrame into the target table or replaces a view.

        Args:
            snapshot: The target snapshot.
            name: The name of the target table or view.
            query_or_df: The query or DataFrame to insert.
            snapshots: Parent snapshots.
            is_dev: Whether the insert is for the dev table.
        """

    @abc.abstractmethod
    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        """Appends the given query or a DataFrame to the existing table.

        Args:
            snapshot: The target snapshot.
            table_name: The target table name.
            query_or_df: The query or DataFrame to insert.
            snapshots: Parent snapshots.
            is_dev: Whether the insert is for the dev table.
        """

    @abc.abstractmethod
    def create(
        self,
        snapshot: Snapshot,
        name: str,
        **render_kwargs: t.Any,
    ) -> None:
        """Creates the target table or view.

        Args:
            snapshot: The target snapshot.
            name: The name of a table or a view.
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
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        pass

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        pass

    def create(
        self,
        snapshot: Snapshot,
        name: str,
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
        logger.info("Updating view '%s' to point at table '%s'", target_name, table_name)
        self.adapter.create_view(
            target_name, exp.select("*").from_(table_name, dialect=self.adapter.dialect)
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
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.insert_append(table_name, query_or_df, columns_to_types=model.columns_to_types)

    def create(
        self,
        snapshot: Snapshot,
        name: str,
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
            )

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
        is_dev: bool,
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
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.merge(
            name,
            query_or_df,
            columns_to_types=model.columns_to_types,
            unique_key=model.unique_key,
        )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        self.adapter.merge(
            table_name,
            query_or_df,
            columns_to_types=model.columns_to_types,
            unique_key=model.unique_key,
        )


class IncrementalUnmanagedStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        if isinstance(model.kind, IncrementalUnmanagedKind) and model.kind.insert_overwrite:
            self.adapter.insert_overwrite_by_partition(
                name, query_or_df, model.partitioned_by, columns_to_types=model.columns_to_types
            )
        else:
            self.append(snapshot, name, query_or_df, snapshots, is_dev, **kwargs)


class FullRefreshStrategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
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
        )


class SCDType2Strategy(MaterializableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        assert isinstance(model.kind, SCDType2Kind)
        self.adapter.scd_type_2(
            target_table=name,
            source_table=query_or_df,
            unique_key=model.unique_key,
            valid_from_name=model.kind.valid_from_name,
            valid_to_name=model.kind.valid_to_name,
            updated_at_name=model.kind.updated_at_name,
            columns_to_types=model.columns_to_types,
            **kwargs,
        )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        assert isinstance(model.kind, SCDType2Kind)
        self.adapter.scd_type_2(
            target_table=table_name,
            source_table=query_or_df,
            unique_key=model.unique_key,
            valid_from_name=model.kind.valid_from_name,
            valid_to_name=model.kind.valid_to_name,
            updated_at_name=model.kind.updated_at_name,
            columns_to_types=model.columns_to_types,
            **kwargs,
        )


class ViewStrategy(PromotableStrategy):
    def insert(
        self,
        snapshot: Snapshot,
        name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        model = snapshot.model
        if (
            isinstance(query_or_df, exp.Expression)
            and model.render_query(snapshots=snapshots, is_dev=is_dev, engine_adapter=self.adapter)
            == query_or_df
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
        )

    def append(
        self,
        snapshot: Snapshot,
        table_name: str,
        query_or_df: QueryOrDF,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
        **kwargs: t.Any,
    ) -> None:
        raise ConfigError(f"Cannot append to a view '{table_name}'.")

    def create(
        self,
        snapshot: Snapshot,
        name: str,
        **render_kwargs: t.Any,
    ) -> None:
        model = snapshot.model

        logger.info("Creating view '%s'", name)
        self.adapter.create_view(
            name,
            model.render_query_or_raise(**render_kwargs),
            materialized=self._is_materialized_view(model),
            table_properties=model.table_properties,
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
