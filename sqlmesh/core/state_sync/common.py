from __future__ import annotations

import logging
import typing as t
from functools import wraps
import itertools
import abc

from sqlmesh.core.console import Console
from sqlmesh.core.dialect import schema_
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.core.environment import Environment, EnvironmentStatements
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.core.snapshot import Snapshot

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from sqlmesh.core.state_sync.base import Versions

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
            raise SQLMeshError(
                f"Failed to drop the expired environment schema '{schema}': {e}"
            ) from e
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
            raise SQLMeshError(
                f"Failed to drop the expired environment view '{expired_view}': {e}"
            ) from e


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


class StateStream(abc.ABC):
    """
    Represents a stream of state either going into the StateSync (perhaps loaded from a file)
    or out of the StateSync (perhaps being dumped to a file)
    """

    @property
    @abc.abstractmethod
    def versions(self) -> Versions:
        """The versions of the objects contained in this StateStream"""

    @property
    @abc.abstractmethod
    def snapshots(self) -> t.Iterable[Snapshot]:
        """A stream of Snapshot objects. Note that they should be fully populated with any relevant Intervals"""

    @property
    @abc.abstractmethod
    def environments(self) -> t.Iterable[EnvironmentWithStatements]:
        """A stream of Environments with any EnvironmentStatements attached"""
