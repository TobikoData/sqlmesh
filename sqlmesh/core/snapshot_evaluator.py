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

from sqlglot import exp, select

from sqlmesh.core.audit import AuditResult
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike, SnapshotTableInfo
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import AuditError

logger = logging.getLogger(__name__)


class SnapshotEvaluator:
    """Evaluates a snapshot given runtime arguments through an arbitrary EngineAdapter.

    The SnapshotEvaluator contains the business logic to generically evaluate a snapshot.
    It is responsible for delegating queries to the EngineAdapter. The SnapshotEvaluator
    does not directly communicate with the underlying execution engine.

    Args:
        adapter: The adapter that interfaces with the execution engine.
    """

    def __init__(self, adapter: EngineAdapter):
        self.adapter = adapter

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
        snapshots: t.Dict[str, Snapshot],
        mapping: t.Optional[t.Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """Evaluate a snapshot, creating its schema and table if it doesn't exist and then inserting it.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            latest: The latest datetime to use for non-incremental queries.
            snapshots: All snapshots to use for mapping of physical locations.
            mapping: Mapping of model references to physical snapshots.
            kwargs: Additional kwargs to pass to the renderer.
        """
        if snapshot.is_embedded_kind:
            return

        table_name = snapshot.table_name
        model = snapshot.model

        for sql_statement in model.sql_statements:
            self.adapter.execute(sql_statement)

        if model.is_sql:
            query_or_df = model.render_query(
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                mapping=mapping,
                **kwargs,
            )
        else:
            query_or_df = model.exec_python(
                self.adapter,
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                mapping=mapping,
                **kwargs,
            )

        if snapshot.is_view_kind:
            logger.info("Replacing view '%s'", table_name)
            self.adapter.create_view(table_name, query_or_df, model.columns)
        elif snapshot.is_full_kind:
            self.adapter.replace_query(table_name, query_or_df)
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

    def promote(self, snapshot: SnapshotInfoLike, environment: str) -> None:
        """Promotes the given snapshot in the target environment by replacing a corresponding view with
        a physical table associated with the given snapshot.

        Args:
            snapshot: Snapshot to promote.
            environment: The target environment.
        """
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

    def demote(self, snapshot: SnapshotInfoLike, environment: str) -> None:
        """Demotes the given snapshot in the target environment by removing its view.

        Args:
            snapshot: Snapshot to remove.
            environment: The target environment.
        """
        view_name = snapshot.qualified_view_name.for_environment(
            environment=environment
        )
        if self.adapter.table_exists(view_name):
            logger.info("Dropping view '%s'", view_name)
            self.adapter.drop_view(view_name)

    def create(self, snapshot: Snapshot, snapshots: t.Dict[str, Snapshot]) -> None:
        """Creates a physical snapshot schema and table.

        Args:
            snapshot: Snapshot to create.
        """
        if snapshot.is_embedded_kind:
            return

        self.adapter.create_schema(snapshot.physical_schema)
        table_name = snapshot.table_name

        if snapshot.is_view_kind:
            logger.info("Creating view '%s'", table_name)
            self.adapter.create_view(
                table_name, snapshot.model.render_query(snapshots=snapshots)
            )
        else:
            logger.info("Creating table '%s'", table_name)
            self.adapter.create_table(
                table_name,
                columns=snapshot.model.columns,
                storage_format=snapshot.model.storage_format,
                partitioned_by=snapshot.model.partitioned_by,
            )

    def cleanup(self, snapshots: t.Iterable[SnapshotTableInfo | Snapshot]) -> None:
        """Cleans up the given snapshots by removing its table

        Args:
            snapshots: Snapshots to cleanup.
        """
        for snapshot in snapshots:
            snapshot = snapshot.table_info
            table_name = snapshot.table_name
            if not self.adapter.table_exists(table_name):
                continue

            try:
                self.adapter.drop_table(table_name)
                logger.info("Dropped table '%s'", table_name)
            except Exception:
                self.adapter.drop_view(table_name)
                logger.info("Dropped view '%s'", table_name)

    def audit(
        self,
        *,
        snapshot: Snapshot,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        raise_exception: bool = True,
        **kwargs,
    ) -> t.List[AuditResult]:
        """Execute a snapshot's model's audit queries.

        Args:
            snapshot: Snapshot to evaluate.  start: The start datetime to audit. Defaults to epoch start.
            end: The end datetime to audit. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All snapshots to use for mapping of physical locations.
            mapping: Mapping of model references to physical snapshots.
            collection_exceptions:
            kwargs: Additional kwargs to pass to the renderer.
        """
        results = []
        for audit, query in snapshot.model.render_audit_queries(
            start=start,
            end=end,
            latest=latest,
            snapshots=snapshots,
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
