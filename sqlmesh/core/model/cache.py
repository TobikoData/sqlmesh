from __future__ import annotations

import logging
import multiprocessing as mp
import typing as t
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from sqlglot import exp
from sqlglot.optimizer.simplify import gen

from sqlmesh.core import constants as c
from sqlmesh.core.model.definition import Model, SqlModel, _Model
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.hashing import crc32

from dataclasses import dataclass

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class ModelCache:
    """File-based cache implementation for model definitions.

    Args:
        path: The path to the cache folder.
    """

    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[Model] = FileCache(
            path,
            prefix="model_definition",
        )

    def get_or_load(self, name: str, entry_id: str = "", *, loader: t.Callable[[], Model]) -> Model:
        """Returns an existing cached model definition or loads and caches a new one.

        Args:
            name: The name of the entry.
            entry_id: The unique entry identifier. Used for cache invalidation.
            loader: Used to load a new model definition when no cached instance was found.

        Returns:
            The model definition.
        """
        cache_entry = self._file_cache.get(name, entry_id)
        if isinstance(cache_entry, _Model):
            return cache_entry

        model = loader()
        if isinstance(model, SqlModel):
            # make sure we preload full_depends_on
            model.full_depends_on
            self._file_cache.put(name, entry_id, value=model)

        return model


@dataclass
class OptimizedQueryCacheEntry:
    optimized_rendered_query: t.Optional[exp.Expression]


class OptimizedQueryCache:
    """File-based cache implementation for optimized model queries.

    Args:
        path: The path to the cache folder.
    """

    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[OptimizedQueryCacheEntry] = FileCache(
            path, prefix="optimized_query"
        )

    def with_optimized_query(self, model: Model, name: t.Optional[str] = None) -> bool:
        """Adds an optimized query to the model's in-memory cache.

        Args:
            model: The model to add the optimized query to.
            name: The cache entry name of the model.
        """
        if not isinstance(model, SqlModel):
            return False

        name = self._entry_name(model) if name is None else name
        cache_entry = self._file_cache.get(name)
        if cache_entry:
            try:
                if cache_entry.optimized_rendered_query:
                    model._query_renderer.update_cache(
                        cache_entry.optimized_rendered_query, optimized=True
                    )
                else:
                    # If the optimized rendered query is None, then there are likely adapter calls in the query
                    # that prevent us from rendering it at load time. This means that we can safely set the
                    # unoptimized cache to None as well to prevent attempts to render it downstream.
                    model._query_renderer.update_cache(None, optimized=False)
                return True
            except Exception as ex:
                logger.warning("Failed to load a cache entry '%s': %s", name, ex)

        self._put(name, model)
        return False

    def put(self, model: Model) -> t.Optional[str]:
        if not isinstance(model, SqlModel):
            return None

        name = self._entry_name(model)

        if self._file_cache.exists(name):
            return name

        self._put(name, model)
        return name

    def _put(self, name: str, model: SqlModel) -> None:
        optimized_query = model.render_query()
        new_entry = OptimizedQueryCacheEntry(optimized_rendered_query=optimized_query)
        self._file_cache.put(name, value=new_entry)

    @staticmethod
    def _entry_name(model: SqlModel) -> str:
        hash_data = _mapping_schema_hash_data(model.mapping_schema)
        hash_data.append(gen(model.query))
        hash_data.append(str([(k, v) for k, v in model.sorted_python_env]))
        hash_data.extend(model.jinja_macros.data_hash_values)
        return f"{model.name}_{crc32(hash_data)}"


def optimized_query_cache_pool(optimized_query_cache: OptimizedQueryCache) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(
        mp_context=mp.get_context("fork"),
        initializer=_init_optimized_query_cache,
        initargs=(optimized_query_cache,),
        max_workers=c.MAX_FORK_WORKERS,
    )


@t.overload
def load_optimized_query_cache(
    model_or_tuple: t.Tuple[Model, T],
) -> t.Tuple[T, t.Optional[str]]: ...


@t.overload
def load_optimized_query_cache(model_or_tuple: Model) -> t.Tuple[str, t.Optional[str]]: ...


def load_optimized_query_cache(model_or_tuple):  # type: ignore
    assert _optimized_query_cache

    if isinstance(model_or_tuple, _Model):
        model = model_or_tuple
        key = None
    else:
        model, key = model_or_tuple

    if isinstance(model, SqlModel):
        entry_name = _optimized_query_cache.put(model)
    else:
        entry_name = None
    return key or model.fqn, entry_name


_optimized_query_cache: t.Optional[OptimizedQueryCache] = None


def _init_optimized_query_cache(optimized_query_cache: OptimizedQueryCache) -> None:
    global _optimized_query_cache
    _optimized_query_cache = optimized_query_cache


def _mapping_schema_hash_data(schema: t.Dict[str, t.Any]) -> t.List[str]:
    keys = sorted(schema) if all(isinstance(v, dict) for v in schema.values()) else schema

    data = []
    for k in keys:
        data.append(k)
        if isinstance(schema[k], dict):
            data.extend(_mapping_schema_hash_data(schema[k]))
        else:
            data.append(str(schema[k]))

    return data
