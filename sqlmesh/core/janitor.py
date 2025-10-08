from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.console import Console
from sqlmesh.core.dialect import schema_
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import SnapshotEvaluator
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.state_sync.common import (
    logger,
    iter_expired_snapshot_batches,
    RowBoundary,
    ExpiredBatchRange,
)
from sqlmesh.utils.errors import SQLMeshError


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
