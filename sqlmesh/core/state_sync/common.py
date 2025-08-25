from __future__ import annotations

import logging
import typing as t
from functools import wraps
import itertools
import abc

from dataclasses import dataclass
from sqlglot import exp

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
