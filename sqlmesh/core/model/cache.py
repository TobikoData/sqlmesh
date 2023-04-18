from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp

from sqlmesh.core.model.definition import Model, SqlModel
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.pydantic import PydanticModel


class SqlModelCacheEntry(PydanticModel):
    model: SqlModel
    rendered_query: t.Dict


class ModelCache:
    def __init__(self, path: Path):
        self.path = path
        self._file_cache: FileCache[SqlModelCacheEntry] = FileCache(
            path,
            SqlModelCacheEntry,
            prefix="model_definition",
        )

    def get_or_load(self, name: str, entry_id: str, loader: t.Callable[[], Model]) -> Model:
        cache_entry = self._file_cache.get(name, entry_id)
        if cache_entry:
            model = cache_entry.model
            model._query_renderer.update_cache(exp.Expression.load(cache_entry.rendered_query))
            return model

        loaded_model = loader()
        if isinstance(loaded_model, SqlModel):
            new_entry = SqlModelCacheEntry(
                model=loaded_model, rendered_query=loaded_model.render_query().dump()
            )
            self._file_cache.put(name, entry_id, new_entry)

        return loaded_model
