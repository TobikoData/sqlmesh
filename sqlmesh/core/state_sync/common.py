from __future__ import annotations

import logging
import typing as t
from functools import wraps
import itertools
import abc

from dataclasses import dataclass

from pydantic_core.core_schema import ValidationInfo
from sqlglot import exp

from sqlmesh.core.console import Console
from sqlmesh.core.dialect import schema_
from sqlmesh.utils.pydantic import PydanticModel, field_validator
from sqlmesh.core.environment import Environment, EnvironmentStatements, EnvironmentNamingInfo
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotId,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from sqlmesh.core.state_sync.base import Versions, StateReader, StateSync

logger = logging.getLogger(__name__)

EXPIRED_SNAPSHOT_DEFAULT_BATCH_SIZE = 200


def cleanup_expired_views(
    default_adapter: EngineAdapter,
    engine_adapters: t.Dict[str, EngineAdapter],
    environments: t.List[Environment],
    warn_on_delete_failure: bool = False,
    console: t.Optional[Console] = None,
) -> None:
    expired_schema_or_catalog_environments = [
        environment
        for environment in environments
        if environment.suffix_target.is_schema or environment.suffix_target.is_catalog
    ]
    expired_table_environments = [
        environment for environment in environments if environment.suffix_target.is_table
    ]

    # We have to use the corresponding adapter if the virtual layer is gateway managed
    def get_adapter(gateway_managed: bool, gateway: t.Optional[str] = None) -> EngineAdapter:
        if gateway_managed and gateway:
            return engine_adapters.get(gateway, default_adapter)
        return default_adapter

    catalogs_to_drop: t.Set[t.Tuple[EngineAdapter, str]] = set()
    schemas_to_drop: t.Set[t.Tuple[EngineAdapter, exp.Table]] = set()

    # Collect schemas and catalogs to drop
    for engine_adapter, expired_catalog, expired_schema, suffix_target in {
        (
            (engine_adapter := get_adapter(environment.gateway_managed, snapshot.model_gateway)),
            snapshot.qualified_view_name.catalog_for_environment(
                environment.naming_info, dialect=engine_adapter.dialect
            ),
            snapshot.qualified_view_name.schema_for_environment(
                environment.naming_info, dialect=engine_adapter.dialect
            ),
            environment.suffix_target,
        )
        for environment in expired_schema_or_catalog_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        if suffix_target.is_catalog:
            if expired_catalog:
                catalogs_to_drop.add((engine_adapter, expired_catalog))
        else:
            schema = schema_(expired_schema, expired_catalog)
            schemas_to_drop.add((engine_adapter, schema))

    # Drop the views for the expired environments
    for engine_adapter, expired_view in {
        (
            (engine_adapter := get_adapter(environment.gateway_managed, snapshot.model_gateway)),
            snapshot.qualified_view_name.for_environment(
                environment.naming_info, dialect=engine_adapter.dialect
            ),
        )
        for environment in expired_table_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        try:
            engine_adapter.drop_view(expired_view, ignore_if_not_exists=True)
            if console:
                console.update_cleanup_progress(expired_view)
        except Exception as e:
            message = f"Failed to drop the expired environment view '{expired_view}': {e}"
            if warn_on_delete_failure:
                logger.warning(message)
            else:
                raise SQLMeshError(message) from e

    # Drop the schemas for the expired environments
    for engine_adapter, schema in schemas_to_drop:
        try:
            engine_adapter.drop_schema(
                schema,
                ignore_if_not_exists=True,
                cascade=True,
            )
            if console:
                console.update_cleanup_progress(schema.sql(dialect=engine_adapter.dialect))
        except Exception as e:
            message = f"Failed to drop the expired environment schema '{schema}': {e}"
            if warn_on_delete_failure:
                logger.warning(message)
            else:
                raise SQLMeshError(message) from e

    # Drop any catalogs that were associated with a snapshot where the engine adapter supports dropping catalogs
    # catalogs_to_drop is only populated when environment_suffix_target is set to 'catalog'
    for engine_adapter, catalog in catalogs_to_drop:
        if engine_adapter.SUPPORTS_CREATE_DROP_CATALOG:
            try:
                engine_adapter.drop_catalog(catalog)
                if console:
                    console.update_cleanup_progress(catalog)
            except Exception as e:
                message = f"Failed to drop the expired environment catalog '{catalog}': {e}"
                if warn_on_delete_failure:
                    logger.warning(message)
                else:
                    raise SQLMeshError(message) from e


def transactional() -> t.Callable[[t.Callable], t.Callable]:
    def decorator(func: t.Callable) -> t.Callable:
        @wraps(func)
        def wrapper(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
            if not hasattr(self, "_transaction"):
                return func(self, *args, **kwargs)

            with self._transaction():
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


T = t.TypeVar("T")


def chunk_iterable(iterable: t.Iterable[T], size: int = 10) -> t.Iterable[t.Iterable[T]]:
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


class EnvironmentWithStatements(PydanticModel):
    environment: Environment
    statements: t.List[EnvironmentStatements] = []


@dataclass
class VersionsChunk:
    versions: Versions


class SnapshotsChunk:
    def __init__(self, items: t.Iterator[Snapshot]):
        self.items = items

    def __iter__(self) -> t.Iterator[Snapshot]:
        return self.items


class EnvironmentsChunk:
    def __init__(self, items: t.Iterator[EnvironmentWithStatements]):
        self.items = items

    def __iter__(self) -> t.Iterator[EnvironmentWithStatements]:
        return self.items


StateStreamContents = t.Union[VersionsChunk, SnapshotsChunk, EnvironmentsChunk]


class StateStream(abc.ABC):
    """
    Represents a stream of state either going into the StateSync (perhaps loaded from a file)
    or out of the StateSync (perhaps being dumped to a file)

    Iterating over the stream produces the following chunks:

        VersionsChunk: The versions of the objects contained in this StateStream
        SnapshotsChunk: Is itself an iterator that streams Snapshot objects. Note that they should be fully populated with any relevant Intervals
        EnvironmentsChunk: Is itself an iterator emitting a stream of Environments with any EnvironmentStatements attached

    The idea here is to give some structure to the stream and ensure that callers have the opportunity to process all its components while not
    needing to worry about the order they are emitted in
    """

    @abc.abstractmethod
    def __iter__(self) -> t.Iterator[StateStreamContents]:
        pass

    @classmethod
    def from_iterators(
        cls: t.Type["StateStream"],
        versions: Versions,
        snapshots: t.Iterator[Snapshot],
        environments: t.Iterator[EnvironmentWithStatements],
    ) -> "StateStream":
        class _StateStream(cls):  # type: ignore
            def __iter__(self) -> t.Iterator[StateStreamContents]:
                yield VersionsChunk(versions)

                yield SnapshotsChunk(snapshots)

                yield EnvironmentsChunk(environments)

        return _StateStream()


class ExpiredBatchRange(PydanticModel):
    start: RowBoundary
    end: t.Union[RowBoundary, LimitBoundary]

    @classmethod
    def init_batch_range(cls, batch_size: int) -> ExpiredBatchRange:
        return ExpiredBatchRange(
            start=RowBoundary.lowest_boundary(),
            end=LimitBoundary(batch_size=batch_size),
        )

    @classmethod
    def all_batch_range(cls) -> ExpiredBatchRange:
        return ExpiredBatchRange(
            start=RowBoundary.lowest_boundary(),
            end=RowBoundary.highest_boundary(),
        )

    @classmethod
    def _expanded_tuple_comparison(
        cls,
        columns: t.List[exp.Column],
        values: t.List[exp.Literal],
        operator: t.Type[exp.Expression],
    ) -> exp.Expression:
        """Generate expanded tuple comparison that works across all SQL engines.

        Converts tuple comparisons like (a, b, c) OP (x, y, z) into an expanded form
        that's compatible with all SQL engines, since native tuple comparisons have
        inconsistent support across engines (especially DuckDB, MySQL, SQLite).

        Repro of problem with DuckDB:
            "SELECT * FROM VALUES(1,'2') as test(a,b) WHERE ((a, b) > (1, 'foo')) AND ((a, b) <= (10, 'baz'))"

        Args:
            columns: List of column expressions to compare
            values: List of value expressions to compare against
            operator: The comparison operator class (exp.GT, exp.GTE, exp.LT, exp.LTE)

        Examples:
            (a, b, c) > (x, y, z) expands to:
                a > x OR (a = x AND b > y) OR (a = x AND b = y AND c > z)

            (a, b, c) <= (x, y, z) expands to:
                a < x OR (a = x AND b < y) OR (a = x AND b = y AND c <= z)

            (a, b, c) >= (x, y, z) expands to:
                a > x OR (a = x AND b > y) OR (a = x AND b = y AND c >= z)

        Returns:
            An expanded OR expression representing the tuple comparison
        """
        if operator not in (exp.GT, exp.GTE, exp.LT, exp.LTE):
            raise ValueError(f"Unsupported operator: {operator}. Use GT, GTE, LT, or LTE.")

        # For <= and >=, we use the strict operator for all but the last column
        # e.g., (a, b) <= (x, y) becomes: a < x OR (a = x AND b <= y)
        # For < and >, we use the strict operator throughout
        # e.g., (a, b) > (x, y) becomes: a > x OR (a = x AND b > x)
        strict_operator: t.Type[exp.Expression]
        final_operator: t.Type[exp.Expression]

        if operator in (exp.LTE, exp.GTE):
            # For inclusive operators (<=, >=), use strict form for intermediate columns
            # but keep inclusive form for the last column
            strict_operator = exp.LT if operator == exp.LTE else exp.GT
            final_operator = operator  # Keep LTE/GTE for last column
        else:
            # For strict operators (<, >), use them throughout
            strict_operator = operator
            final_operator = operator

        conditions: t.List[exp.Expression] = []
        for i in range(len(columns)):
            # Build equality conditions for all columns before current
            equality_conditions = [exp.EQ(this=columns[j], expression=values[j]) for j in range(i)]

            # Use the final operator for the last column, strict for others
            comparison_op = final_operator if i == len(columns) - 1 else strict_operator
            comparison_condition = comparison_op(this=columns[i], expression=values[i])

            if equality_conditions:
                conditions.append(exp.and_(*equality_conditions, comparison_condition))
            else:
                conditions.append(comparison_condition)

        return exp.or_(*conditions) if len(conditions) > 1 else conditions[0]

    @property
    def where_filter(self) -> exp.Expression:
        # Use expanded tuple comparisons for cross-engine compatibility
        # Native tuple comparisons like (a, b) > (x, y) don't work reliably across all SQL engines
        columns = [
            exp.column("updated_ts"),
            exp.column("name"),
            exp.column("identifier"),
        ]
        start_values = [
            exp.Literal.number(self.start.updated_ts),
            exp.Literal.string(self.start.name),
            exp.Literal.string(self.start.identifier),
        ]

        start_condition = self._expanded_tuple_comparison(columns, start_values, exp.GT)

        range_filter: exp.Expression
        if isinstance(self.end, RowBoundary):
            end_values = [
                exp.Literal.number(self.end.updated_ts),
                exp.Literal.string(self.end.name),
                exp.Literal.string(self.end.identifier),
            ]
            end_condition = self._expanded_tuple_comparison(columns, end_values, exp.LTE)
            range_filter = exp.and_(start_condition, end_condition)
        else:
            range_filter = start_condition
        return range_filter


class RowBoundary(PydanticModel):
    updated_ts: int
    name: str
    identifier: str

    @classmethod
    def lowest_boundary(cls) -> RowBoundary:
        return RowBoundary(updated_ts=0, name="", identifier="")

    @classmethod
    def highest_boundary(cls) -> RowBoundary:
        # 9999-12-31T23:59:59.999Z in epoch milliseconds
        return RowBoundary(updated_ts=253_402_300_799_999, name="", identifier="")


class LimitBoundary(PydanticModel):
    batch_size: int

    @classmethod
    def init_batch_boundary(cls, batch_size: int) -> LimitBoundary:
        return LimitBoundary(batch_size=batch_size)


class PromotionResult(PydanticModel):
    added: t.List[SnapshotTableInfo]
    removed: t.List[SnapshotTableInfo]
    removed_environment_naming_info: t.Optional[EnvironmentNamingInfo]

    @field_validator("removed_environment_naming_info")
    def _validate_removed_environment_naming_info(
        cls, v: t.Optional[EnvironmentNamingInfo], info: ValidationInfo
    ) -> t.Optional[EnvironmentNamingInfo]:
        if v and not info.data.get("removed"):
            raise ValueError("removed_environment_naming_info must be None if removed is empty")
        return v


class ExpiredSnapshotBatch(PydanticModel):
    """A batch of expired snapshots to be cleaned up."""

    expired_snapshot_ids: t.Set[SnapshotId]
    cleanup_tasks: t.List[SnapshotTableCleanupTask]
    batch_range: ExpiredBatchRange


def iter_expired_snapshot_batches(
    state_reader: StateReader,
    *,
    current_ts: int,
    ignore_ttl: bool = False,
    batch_size: t.Optional[int] = None,
) -> t.Iterator[ExpiredSnapshotBatch]:
    """Yields expired snapshot batches.

    Args:
        state_reader: StateReader instance to query expired snapshots from.
        current_ts: Timestamp used to evaluate expiration.
        ignore_ttl: If True, include snapshots regardless of TTL (only checks if unreferenced).
        batch_size: Maximum number of snapshots to fetch per batch.
    """

    batch_size = batch_size if batch_size is not None else EXPIRED_SNAPSHOT_DEFAULT_BATCH_SIZE
    batch_range = ExpiredBatchRange.init_batch_range(batch_size=batch_size)

    while True:
        batch = state_reader.get_expired_snapshots(
            current_ts=current_ts,
            ignore_ttl=ignore_ttl,
            batch_range=batch_range,
        )

        if batch is None:
            return

        yield batch

        assert isinstance(batch.batch_range.end, RowBoundary), (
            "Only RowBoundary is supported for pagination currently"
        )
        batch_range = ExpiredBatchRange(
            start=batch.batch_range.end,
            end=LimitBoundary(batch_size=batch_size),
        )


def delete_expired_snapshots(
    state_sync: StateSync,
    snapshot_evaluator: SnapshotEvaluator,
    *,
    current_ts: int,
    ignore_ttl: bool = False,
    batch_size: t.Optional[int] = None,
    console: t.Optional[Console] = None,
) -> None:
    """Delete all expired snapshots in batches.

    This helper function encapsulates the logic for deleting expired snapshots in batches,
    eliminating code duplication across different use cases.

    Args:
        state_sync: StateSync instance to query and delete expired snapshots from.
        snapshot_evaluator: SnapshotEvaluator instance to clean up tables associated with snapshots.
        current_ts: Timestamp used to evaluate expiration.
        ignore_ttl: If True, include snapshots regardless of TTL (only checks if unreferenced).
        batch_size: Maximum number of snapshots to fetch per batch.
        console: Optional console for reporting progress.

    Returns:
        The total number of deleted expired snapshots.
    """
    num_expired_snapshots = 0
    for batch in iter_expired_snapshot_batches(
        state_reader=state_sync,
        current_ts=current_ts,
        ignore_ttl=ignore_ttl,
        batch_size=batch_size,
    ):
        end_info = (
            f"updated_ts={batch.batch_range.end.updated_ts}"
            if isinstance(batch.batch_range.end, RowBoundary)
            else f"limit={batch.batch_range.end.batch_size}"
        )
        logger.info(
            "Processing batch of size %s with end %s",
            len(batch.expired_snapshot_ids),
            end_info,
        )
        snapshot_evaluator.cleanup(
            target_snapshots=batch.cleanup_tasks,
            on_complete=console.update_cleanup_progress if console else None,
        )
        state_sync.delete_expired_snapshots(
            batch_range=ExpiredBatchRange(
                start=RowBoundary.lowest_boundary(),
                end=batch.batch_range.end,
            ),
            ignore_ttl=ignore_ttl,
        )
        logger.info("Cleaned up expired snapshots batch")
        num_expired_snapshots += len(batch.expired_snapshot_ids)
    logger.info("Cleaned up %s expired snapshots", num_expired_snapshots)
