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

from sqlmesh.core.audit import AuditResult
from sqlmesh.core.engine_adapter import DF, EngineAdapter
from sqlmesh.core.schema_diff import SchemaDeltaOp, SchemaDiffCalculator
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotInfoLike
from sqlmesh.utils.concurrency import concurrent_apply_to_snapshots
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import AuditError, ConfigError

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
        mapping: t.Dict[str, str],
        limit: int = 0,
        **kwargs,
    ) -> t.Optional[DF]:
        """Evaluate a snapshot, creating its schema and table if it doesn't exist and then inserting it.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            latest: The latest datetime to use for non-incremental queries.
            mapping: Mapping of model references to physical snapshots.
            limit: If limit is >= 0, the query will not be persisted but evaluated and returned
                as a dataframe.
            kwargs: Additional kwargs to pass to the renderer.
        """
        logger.info("Evaluating snapshot %s", snapshot.snapshot_id)
        if snapshot.is_embedded_kind:
            return None

        model = snapshot.model

        for sql_statement in model.sql_statements:
            self.adapter.execute(sql_statement)

        if model.is_sql:
            query_or_df = model.render_query(
                start=start,
                end=end,
                latest=latest,
                mapping=mapping,
                **kwargs,
            )
        else:
            from sqlmesh.core.context import ExecutionContext

            query_or_df = model.exec_python(
                ExecutionContext(self.adapter, mapping),
                start=start,
                end=end,
                latest=latest,
                **kwargs,
            )

        if limit > 0:
            if isinstance(query_or_df, exp.Expression):
                query_or_df = self.adapter.fetchdf(query_or_df.limit(limit))
            return query_or_df.head(limit)

        table_name = snapshot.table_name

        if snapshot.is_view_kind:
            logger.info("Replacing view '%s'", table_name)
            self.adapter.create_view(table_name, query_or_df, model.columns)
        elif snapshot.is_full_kind:
            self.adapter.replace_query(table_name, query_or_df, model.columns)
        else:
            logger.info("Inserting batch (%s, %s) into %s'", start, end, table_name)
            columns = model.columns
            if self.adapter.supports_partitions:
                self.adapter.insert_overwrite(table_name, query_or_df, columns=columns)
            elif snapshot.is_incremental_kind:
                # A model's time_column could be None but it shouldn't be for an incremental model
                assert model.time_column
                where = exp.Between(
                    this=exp.to_column(model.time_column.column),
                    low=model.convert_to_time_column(start),
                    high=model.convert_to_time_column(end),
                )
                self.adapter.delete_insert_query(
                    table_name, query_or_df, where=where, columns=columns
                )
            else:
                self.adapter.insert_append(table_name, query_or_df, columns=columns)
        return None

    def promote(
        self, target_snapshots: t.Iterable[SnapshotInfoLike], environment: str
    ) -> None:
        """Promotes the given collection of snapshots in the target environment by replacing a corresponding
        view with a physical table associated with the given snapshot.

        Args:
            target_snapshots: Snapshots to promote.
            environment: The target environment.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._promote_snapshot(s, environment),
                self.ddl_concurrent_tasks,
            )

    def demote(
        self, target_snapshots: t.Iterable[SnapshotInfoLike], environment: str
    ) -> None:
        """Demotes the given collection of snapshots in the target environment by removing its view.

        Args:
            target_snapshots: Snapshots to demote.
            environment: The target environment.
        """
        with self.concurrent_context():
            concurrent_apply_to_snapshots(
                target_snapshots,
                lambda s: self._demote_snapshot(s, environment),
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

    def migrate(
        self,
        target_snapshots: t.Iterable[Snapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> None:
        """Alters a physical snapshot table to match its snapshot's schema for the given collection of snapshots.

        Args:
            target_snapshots: Target snapshosts.
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
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        raise_exception: bool = True,
        **kwargs,
    ) -> t.List[AuditResult]:
        """Execute a snapshot's model's audit queries.

        Args:
            snapshot: Snapshot to evaluate.  start: The start datetime to audit. Defaults to epoch start.
            end: The end datetime to audit. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            mapping: Mapping of model references to physical snapshots.
            collection_exceptions:
            kwargs: Additional kwargs to pass to the renderer.
        """
        logger.info("Auditing snapshot %s", snapshot.snapshot_id)
        results = []
        for audit, query in snapshot.model.render_audit_queries(
            start=start,
            end=end,
            latest=latest,
            mapping=mapping,
            **kwargs,
        ):
            count, *_ = self.adapter.fetchone(select("COUNT(*)").from_(f"({query})"))
            if count and raise_exception:
                message = f"Audit {audit.name} for model {audit.model} failed.\nGot {count} results, expected 0.\n{query}"
                if audit.blocking:
                    raise AuditError(message)
                else:
                    logger.warning(
                        f"{message}\nAudit is warn only so proceeding with execution."
                    )
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

    def _create_snapshot(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> None:
        if snapshot.is_embedded_kind:
            return

        self.adapter.create_schema(snapshot.physical_schema)
        table_name = snapshot.table_name

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }

        if snapshot.is_view_kind:
            logger.info("Creating view '%s'", table_name)
            self.adapter.create_view(
                table_name,
                snapshot.model.render_query(snapshots=parent_snapshots_by_name),
            )
        else:
            logger.info("Creating table '%s'", table_name)
            self.adapter.create_table(
                table_name,
                query_or_columns=snapshot.model.columns
                if snapshot.model.annotated
                else snapshot.model.ctas_query(parent_snapshots_by_name),
                storage_format=snapshot.model.storage_format,
                partitioned_by=snapshot.model.partitioned_by,
            )

    def _migrate_snapshot(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> None:
        if not snapshot.is_materialized:
            return

        tmp_table_name = f"{snapshot.table_name}__tmp__{snapshot.fingerprint}"
        target_table_name = snapshot.table_name

        parent_snapshots_by_name = {
            snapshots[p_sid].name: snapshots[p_sid] for p_sid in snapshot.parents
        }

        logger.info("Creating a temporary table '%s'", tmp_table_name)
        self.adapter.create_table(
            tmp_table_name,
            query_or_columns=snapshot.model.columns
            if snapshot.model.annotated
            else snapshot.model.ctas_query(parent_snapshots_by_name),
            storage_format=snapshot.model.storage_format,
            partitioned_by=snapshot.model.partitioned_by,
        )

        schema_deltas = self._schema_diff_calculator.calculate(
            target_table_name, tmp_table_name
        )
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

        logger.info("Dropping the temporary table '%s'", tmp_table_name)
        self.adapter.drop_table(tmp_table_name)

    def _promote_snapshot(self, snapshot: SnapshotInfoLike, environment: str) -> None:
        qualified_view_name = snapshot.qualified_view_name
        schema = qualified_view_name.schema_for_environment(environment=environment)
        if schema is not None:
            self.adapter.create_schema(schema)

        view_name = qualified_view_name.for_environment(environment=environment)
        table_name = snapshot.table_name
        if self.adapter.table_exists(table_name):
            logger.info(
                "Updating view '%s' to point at table '%s'", view_name, table_name
            )
            self.adapter.create_view(view_name, exp.select("*").from_(table_name))
        else:
            logger.info("Dropping view '%s' for non-materialized table", view_name)
            self.adapter.drop_view(view_name)

    def _demote_snapshot(self, snapshot: SnapshotInfoLike, environment: str) -> None:
        view_name = snapshot.qualified_view_name.for_environment(
            environment=environment
        )
        if self.adapter.table_exists(view_name):
            logger.info("Dropping view '%s'", view_name)
            self.adapter.drop_view(view_name)

    def _cleanup_snapshot(self, snapshot: SnapshotInfoLike) -> None:
        snapshot = snapshot.table_info
        table_name = snapshot.table_name
        if self.adapter.table_exists(table_name):
            try:
                self.adapter.drop_table(table_name)
                logger.info("Dropped table '%s'", table_name)
            except Exception:
                self.adapter.drop_view(table_name)
                logger.info("Dropped view '%s'", table_name)
