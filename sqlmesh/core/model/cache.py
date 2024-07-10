from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp
from sqlglot.optimizer.simplify import gen

from sqlmesh.core.model.definition import Model, SqlModel
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.hashing import crc32
from sqlmesh.utils.pydantic import PydanticModel


class SqlModelCacheEntry(PydanticModel):
    model: SqlModel
    full_depends_on: t.Set[str]


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
        if cache_entry:
            model = cache_entry.model
            model._full_depends_on = cache_entry.full_depends_on
            return model

        loaded_model = loader()
        if isinstance(loaded_model, SqlModel):
            new_entry = SqlModelCacheEntry(
                model=loaded_model, full_depends_on=loaded_model.full_depends_on
            )
            self._file_cache.put(name, entry_id, value=new_entry)

        return loaded_model


class RenderedQueryCacheEntry(PydanticModel):
    optimized_query: t.Optional[exp.Expression]
    rendered_query: exp.Expression


class RenderedQueryCache:
    """File-based cache implementation for rendered and optimized model queries.

    Args:
        path: The path to the cache folder.
    """

    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[RenderedQueryCacheEntry] = FileCache(
            path, RenderedQueryCacheEntry, prefix="rendered_query"
        )

    def with_rendered_query(self, model: Model) -> bool:
        """Adds the rendered query and the optimized query  to the model's in-memory cache.

        Args:
            model: The model to add the rendered queries to.
        """
        if not isinstance(model, SqlModel):
            return False

        hash_data = _mapping_schema_hash_data(model.mapping_schema)
        hash_data.append(gen(model.query))
        hash_data.append(str([(k, v) for k, v in model.sorted_python_env]))
        hash_data.extend(model.jinja_macros.data_hash_values)

        name = f"{model.name}_{crc32(hash_data)}"
        cache_entry = self._file_cache.get(name)

        if cache_entry:
            model._query_renderer.update_cache(cache_entry.rendered_query, optimized=False)
            if cache_entry.optimized_query:
                model._query_renderer.update_cache(cache_entry.optimized_query, optimized=True)
            return True

        unoptimized_query = model.render_query(optimize=False)
        if unoptimized_query is None:
            return False

        optimized_query = model.render_query(optimize=True)

        new_entry = RenderedQueryCacheEntry(
            optimized_query=optimized_query, rendered_query=unoptimized_query
        )
        self._file_cache.put(name, value=new_entry)

        return False


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
