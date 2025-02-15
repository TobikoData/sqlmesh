from __future__ import annotations

import logging
import typing as t
from functools import wraps

from sqlmesh.core.console import Console
from sqlmesh.core.dialect import schema_
from sqlmesh.core.environment import Environment

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter

logger = logging.getLogger(__name__)


def cleanup_expired_views(
    adapter: EngineAdapter, environments: t.List[Environment], console: t.Optional[Console] = None
) -> None:
    expired_schema_environments = [
        environment for environment in environments if environment.suffix_target.is_schema
    ]
    expired_table_environments = [
        environment for environment in environments if environment.suffix_target.is_table
    ]
    for expired_catalog, expired_schema in {
        (
            snapshot.qualified_view_name.catalog_for_environment(
                environment.naming_info, dialect=adapter.dialect
            ),
            snapshot.qualified_view_name.schema_for_environment(
                environment.naming_info, dialect=adapter.dialect
            ),
        )
        for environment in expired_schema_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        schema = schema_(expired_schema, expired_catalog)
        try:
            adapter.drop_schema(
                schema,
                ignore_if_not_exists=True,
                cascade=True,
            )
            if console:
                console.update_cleanup_progress(schema.sql(dialect=adapter.dialect))
        except Exception as e:
            logger.warning("Failed to drop the expired environment schema '%s': %s", schema, e)
    for expired_view in {
        snapshot.qualified_view_name.for_environment(
            environment.naming_info, dialect=adapter.dialect
        )
        for environment in expired_table_environments
        for snapshot in environment.snapshots
        if snapshot.is_model and not snapshot.is_symbolic
    }:
        try:
            adapter.drop_view(expired_view, ignore_if_not_exists=True)
            if console:
                console.update_cleanup_progress(expired_view)
        except Exception as e:
            logger.warning("Failed to drop the expired environment view '%s': %s", expired_view, e)


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
