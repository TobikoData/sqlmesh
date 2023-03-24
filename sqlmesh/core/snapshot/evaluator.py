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

A snapshot evaluator can also run the audits for a snapshot's model. This is often done after a snapshot
has been evaluated to check for data quality issues.

For more information about audits, see `sqlmesh.core.audit`.
"""
from __future__ import annotations

import logging
import typing as t
from contextlib import contextmanager

from sqlglot import exp, select
from sqlglot.executor import execute

from sqlmesh.core.audit import BUILT_IN_AUDITS, AuditResult
from sqlmesh.core.engine_adapter import EngineAdapter, TransactionType
from sqlmesh.core.schema_diff import SchemaDeltaOp, SchemaDiffCalculator
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotInfoLike
from sqlmesh.utils.concurrency import concurrent_apply_to_snapshots
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import AuditError, ConfigError, SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF

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

    def __init__(self, adapter: EngineAdapter, ddl_concurrent_tasks: int = 1):
        self.adapter = adapter
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self._schema_diff_calculator = SchemaDiffCalculator(self.adapter)

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
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
            latest: The latest datetime to use for non-incremental queries.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            limit: If limit is > 0, the query will not be persisted but evaluated and returned as a dataframe.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.
        """
        if snapshot.is_embedded_kind:
            return None

        if not limit and not snapshot.is_forward_only:
            self._ensure_no_paused_forward_only_upstream(snapshot, snapshots)

        logger.info("Evaluating snapshot %s", snapshot.snapshot_id)

        model = snapshot.model
        columns_to_types = model.columns_to_types
        table_name = "" if limit else snapshot.table_name(is_dev=is_dev)

        def apply(query_or_df: QueryOrDF, index: int = 0) -> None:
            if snapshot.is_view_kind:
                if index > 0:
                    raise ConfigError("Cannot batch view creation.")
                logger.info("Replacing view '%s'", table_name)
                self.adapter.create_view(table_name, query_or_df, columns_to_types)
            elif index > 0:
                self.adapter.insert_append(
                    table_name, query_or_df, columns_to_types=columns_to_types
                )
            elif snapshot.is_full_kind or snapshot.is_seed_kind:
                self.adapter.replace_query(table_name, query_or_df, columns_to_types)
            else:
                logger.info("Inserting batch (%s, %s) into %s'", start, end, table_name)
                if snapshot.is_incremental_by_time_range_kind:
                    # A model's time_column could be None but
                    # it shouldn't be for an incremental by time range model
                    assert model.time_column
                    self.adapter.insert_overwrite_by_time_partition(
                        table_name,
                        query_or_df,
                        start=start,
                        end=end,
                        time_formatter=model.convert_to_time_column,
                        time_column=model.time_column,
                        columns_to_types=columns_to_types,
                    )
                elif snapshot.is_incremental_by_unique_key_kind:
                    self.adapter.merge(
                        table_name,
                        query_or_df,
                        column_names=columns_to_types.keys(),
                        unique_key=model.unique_key,
                    )
                else:
                    raise SQLMeshError(f"Unexpected SnapshotKind: {snapshot.model.kind}")

        for sql_statement in model.sql_statements:
            self.adapter.execute(sql_statement)

        from sqlmesh.core.context import ExecutionContext

        context = ExecutionContext(self.adapter, snapshots, is_dev)

        model.run_pre_hooks(
            context=context,
            start=start,
            end=end,
            latest=latest,
            **kwargs,
        )

        queries_or_dfs = model.render(
            context,
            start=start,
            end=end,
            latest=latest,
            engine_adapter=self.adapter,
            **kwargs,
        )

        with self.adapter.transaction(
            transaction_type=TransactionType.DDL
            if model.kind.is_view or model.kind.is_full
            else TransactionType.DML
        ):
            for index, query_or_df in enumerate(queries_or_dfs):
                if limit and limit > 0:
                    if isinstance(query_or_df, exp.Select):
                        existing_limit = query_or_df.args.get("limit")
                        if existing_limit:
                            limit = min(
                                limit,
                                execute(exp.select(existing_limit.expression)).rows[0][0],
                            )
                    return query_or_df.head(limit) if hasattr(query_or_df, "head") else self.adapter._fetch_native_df(query_or_df.limit(limit))  # type: ignore

                apply(query_or_df, index)

            model.run_post_hooks(
                context=context,
                start=start,
                end=end,
                latest=latest,
                **kwargs,
            )
            return None

    def promote(
        self,
        target_snapshots: t.Iterable[SnapshotInfoLike],
        environment: str,
        is_dev: bool = False,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Promotes the given collection of snapshots in the target environment by replacing a corresponding
        view with a physical table associated with the given snapshot.

        Args:
            target_snapshots: Snapshots to promote.
            environment: The target environment.
            is_dev: Indicates whether the promotion happens in the development mode and temporary
                tables / table clones should be used where applicable.
            on_complete: a callback to call on each successfully promoted snapshot.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._promote_snapshot(s, environment, is_dev, on_complete),
                self.ddl_concurrent_tasks,
            )

    def demote(
        self,
        target_snapshots: t.Iterable[SnapshotInfoLike],
        environment: str,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        """Demotes the given collection of snapshots in the target environment by removing its view.

        Args:
            target_snapshots: Snapshots to demote.
            environment: The target environment.
            on_complete: a callback to call on each successfully demoted snapshot.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._demote_snapshot(s, environment, on_complete),
                self.ddl_concurrent_tasks,
            )

    def create(
        self,
        target_snapshots: t.Iterable[Snapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> None:
        """Creates a physical snapshot schema and table for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshosts.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._create_snapshot(s, snapshots),
                self.ddl_concurrent_tasks,
            )

    def migrate(self, target_snapshots: t.Iterable[SnapshotInfoLike]) -> None:
        """Alters a physical snapshot table to match its snapshot's schema for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshosts.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._migrate_snapshot(s),
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
        latest: t.Optional[TimeLike] = None,
        raise_exception: bool = True,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> t.List[AuditResult]:
        """Execute a snapshot's model's audit queries.

        Args:
            snapshot: Snapshot to evaluate.  start: The start datetime to audit. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            start: The start datetime to audit. Defaults to epoch start.
            end: The end datetime to audit. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
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

        audits_by_name = {**BUILT_IN_AUDITS, **{a.name: a for a in snapshot.audits}}

        results = []
        for audit_name, audit_args in snapshot.model.audits:
            audit = audits_by_name[audit_name]
            query = audit.render_query(
                snapshot,
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                is_dev=is_dev,
                engine_adapter=self.adapter,
                **audit_args,
                **kwargs,
            )
            count, *_ = self.adapter.fetchone(select("COUNT(*)").from_(query.subquery()))
            if count and raise_exception:
                message = f"Audit '{audit_name}' for model '{snapshot.model.name}' failed.\nGot {count} results, expected 0.\n{query}"
                if audit.blocking:
                    raise AuditError(message)
                else:
                    logger.warning(f"{message}\nAudit is warn only so proceeding with execution.")
            results.append(AuditResult(audit=audit, count=count, query=query))
        return results

    @contextmanager
    def concurrent_context(self) -> t.Generator[None, None, None]:
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

    def _create_snapshot(self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]) -> None:
        if snapshot.is_embedded_kind:
            return

        self.adapter.create_schema(snapshot.physical_schema)

        # If a snapshot reuses an existing version we assume that the table for that version
        # has already been created, so we only need to create a temporary table or a clone.
        is_dev = not snapshot.is_new_version
        table_name = snapshot.table_name(is_dev=is_dev)

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }

        if snapshot.is_view_kind:
            logger.info("Creating view '%s'", table_name)
            self.adapter.create_view(
                table_name,
                snapshot.model.render_query(snapshots=parent_snapshots_by_name, is_dev=is_dev),
            )
        else:
            logger.info("Creating table '%s'", table_name)
            self.adapter.create_table(
                table_name,
                query_or_columns_to_types=snapshot.model.columns_to_types
                if snapshot.model.annotated
                else snapshot.model.ctas_query(parent_snapshots_by_name, is_dev=is_dev),
                storage_format=snapshot.model.storage_format,
                partitioned_by=snapshot.model.partitioned_by,
                partition_interval_unit=snapshot.model.interval_unit(),
            )

    def _migrate_snapshot(self, snapshot: SnapshotInfoLike) -> None:
        if not snapshot.is_materialized or snapshot.is_new_version:
            return

        tmp_table_name = snapshot.table_name(is_dev=True)
        target_table_name = snapshot.table_name()

        schema_deltas = self._schema_diff_calculator.calculate(target_table_name, tmp_table_name)
        if not schema_deltas:
            return

        added_columns = {}
        dropped_columns = []
        for delta in schema_deltas:
            if delta.op == SchemaDeltaOp.ADD:
                added_columns[delta.column_name] = delta.column_type
            elif delta.op == SchemaDeltaOp.DROP:
                dropped_columns.append(delta.column_name)
            else:
                raise ConfigError(f"Unsupported schema delta operation: {delta.op}")

        logger.info(
            "Altering table '%s'. Added columns: %s; dropped columns: %s",
            target_table_name,
            added_columns,
            dropped_columns,
        )
        self.adapter.alter_table(target_table_name, added_columns, dropped_columns)

    def _promote_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        environment: str,
        is_dev: bool,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        qualified_view_name = snapshot.qualified_view_name
        schema = qualified_view_name.schema_for_environment(environment=environment)
        if schema is not None:
            self.adapter.create_schema(schema)

        view_name = qualified_view_name.for_environment(environment=environment)
        if not snapshot.is_embedded_kind:
            table_name = snapshot.table_name(is_dev=is_dev, for_read=True)
            logger.info("Updating view '%s' to point at table '%s'", view_name, table_name)
            self.adapter.create_view(view_name, exp.select("*").from_(table_name))
        else:
            logger.info("Dropping view '%s' for non-materialized table", view_name)
            self.adapter.drop_view(view_name)

        if on_complete is not None:
            on_complete(snapshot)

    def _demote_snapshot(
        self,
        snapshot: SnapshotInfoLike,
        environment: str,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]],
    ) -> None:
        view_name = snapshot.qualified_view_name.for_environment(environment=environment)
        logger.info("Dropping view '%s'", view_name)
        self.adapter.drop_view(view_name)

        if on_complete is not None:
            on_complete(snapshot)

    def _cleanup_snapshot(self, snapshot: SnapshotInfoLike) -> None:
        if snapshot.is_embedded_kind:
            return

        snapshot = snapshot.table_info
        table_names = [snapshot.table_name()]
        if snapshot.version != snapshot.fingerprint:
            table_names.append(snapshot.table_name(is_dev=True))

        for table_name in table_names:
            if snapshot.is_materialized:
                self.adapter.drop_table(table_name)
                logger.info("Dropped table '%s'", table_name)
            else:
                self.adapter.drop_view(table_name)
                logger.info("Dropped view '%s'", table_name)

    def _ensure_no_paused_forward_only_upstream(
        self, snapshot: Snapshot, parent_snapshots: t.Dict[str, Snapshot]
    ) -> None:
        for p in parent_snapshots.values():
            if p.is_forward_only and p.is_paused:
                raise SQLMeshError(
                    f"Snapshot {snapshot.snapshot_id} depends on a paused forward-only snapshot {p.snapshot_id}. Create and apply a new plan to fix this issue."
                )
