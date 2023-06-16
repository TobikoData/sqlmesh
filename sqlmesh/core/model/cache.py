from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp

from sqlmesh.core.model.definition import Model, SqlModel
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.hashing import crc32
from sqlmesh.utils.pydantic import PydanticModel


class SqlModelCacheEntry(PydanticModel):
    model: SqlModel
    rendered_query: t.Optional[exp.Expression]


class ModelCache:
    """File-based cache implementation for model definitions.

    Args:
        path: The path to the cache folder.
    """

    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[SqlModelCacheEntry] = FileCache(
            path,
            SqlModelCacheEntry,
            prefix="model_definition",
        )

    def get_or_load(self, name: str, entry_id: str, loader: t.Callable[[], Model]) -> Model:
        """Returns an existing cached model definition or loads and caches a new one.

        Args:
            name: The name of the entry.
            entry_id: The unique entry identifier. Used for cache invalidation.
            loader: Used to load a new model definition when no cached instance was found.

        Returns:
            The model definition.
        """
        cache_entry = self._file_cache.get(name, entry_id)
        if cache_entry:
            model = cache_entry.model
            if cache_entry.rendered_query is not None:
                model._query_renderer.update_cache(cache_entry.rendered_query, optimized=False)
            return model

        loaded_model = loader()
        if isinstance(loaded_model, SqlModel):
            new_entry = SqlModelCacheEntry(
                model=loaded_model, rendered_query=loaded_model.render_query(optimize=False)
            )
            self._file_cache.put(name, entry_id, new_entry)

        return loaded_model


class OptimizedQueryCacheEntry(PydanticModel):
    optimized_rendered_query: exp.Expression


class OptimizedQueryCache:
    """File-based cache implementation for optimized model queries.

    Args:
        path: The path to the cache folder.
    """

    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[OptimizedQueryCacheEntry] = FileCache(
            path, OptimizedQueryCacheEntry, prefix="optimized_query"
        )

    def with_optimized_query(self, model: Model) -> bool:
        """Adds an optimized query to the model's in-memory cache.

        Args:
            model: The model to add the optimized query to.
        """
        if not isinstance(model, SqlModel):
            return False

        unoptimized_query = model.render_query(optimize=False)
        if unoptimized_query is None:
            return False

        entry_id = self._entry_id(model, unoptimized_query)
        cache_entry = self._file_cache.get(model.name, entry_id)
        if cache_entry:
            model._query_renderer.update_cache(cache_entry.optimized_rendered_query, optimized=True)
            return True

        optimized_query = model.render_query(optimize=True)
        if optimized_query is not None:
            new_entry = OptimizedQueryCacheEntry(optimized_rendered_query=optimized_query)
            self._file_cache.put(model.name, entry_id, new_entry)

        return False

    @staticmethod
    def _entry_id(model: SqlModel, unoptimized_query: exp.Expression) -> str:
        data = OptimizedQueryCache._mapping_schema_hash_data(model.mapping_schema)
        data.append(unoptimized_query.sql())
        return crc32(data)

    @staticmethod
    def _mapping_schema_hash_data(schema: t.Dict[str, t.Any]) -> t.List[str]:
        keys = sorted(schema) if all(isinstance(v, dict) for v in schema.values()) else schema

        data = []
        for k in keys:
            data.append(k)
            if isinstance(schema[k], dict):
                data.extend(OptimizedQueryCache._mapping_schema_hash_data(schema[k]))
            else:
                data.append(str(schema[k]))

        return data
