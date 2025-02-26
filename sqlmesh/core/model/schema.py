from __future__ import annotations

import typing as t
from concurrent.futures import as_completed
from pathlib import Path

from sqlglot.errors import SchemaError
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core.model.cache import (
    load_optimized_query_and_mapping,
    optimized_query_cache_pool,
    OptimizedQueryCache,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.model.definition import Model
    from sqlmesh.utils import UniqueKeyDict
    from sqlmesh.utils.dag import DAG


def update_model_schemas(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    context_path: Path,
) -> None:
    schema = MappingSchema(normalize=False)
    optimized_query_cache: OptimizedQueryCache = OptimizedQueryCache(context_path / c.CACHE)

    if c.MAX_FORK_WORKERS == 1:
        _update_model_schemas_sequential(dag, models, schema, optimized_query_cache)
    else:
        _update_model_schemas_parallel(dag, models, schema, optimized_query_cache)


def _update_schema_with_model(schema: MappingSchema, model: Model) -> None:
    columns_to_types = model.columns_to_types
    if columns_to_types:
        try:
            schema.add_table(model.fqn, columns_to_types, dialect=model.dialect)
        except SchemaError as e:
            if "nesting level:" in str(e):
                from sqlmesh.core.console import get_console

                get_console().log_error(
                    "SQLMesh requires all model names and references to have the same level of nesting."
                )
            raise


def _update_model_schemas_sequential(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    schema: MappingSchema,
    optimized_query_cache: OptimizedQueryCache,
) -> None:
    for name in dag.sorted:
        model = models.get(name)

        # External models don't exist in the context, so we need to skip them
        if not model:
            continue

        model.update_schema(schema)
        optimized_query_cache.with_optimized_query(model)
        _update_schema_with_model(schema, model)


def _update_model_schemas_parallel(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    schema: MappingSchema,
    optimized_query_cache: OptimizedQueryCache,
) -> None:
    futures = set()
    graph = {
        model: {dep for dep in deps if dep in models}
        for model, deps in dag._dag.items()
        if model in models
    }

    def process_models(completed_model: t.Optional[Model] = None) -> None:
        for name in list(graph):
            deps = graph[name]

            if completed_model:
                deps.discard(completed_model.fqn)

            if not deps:
                del graph[name]
                model = models[name]
                futures.add(
                    executor.submit(
                        load_optimized_query_and_mapping,
                        model,
                        mapping={
                            parent: models[parent].columns_to_types
                            for parent in model.depends_on
                            if parent in models
                        },
                    )
                )

    with optimized_query_cache_pool(optimized_query_cache) as executor:
        process_models()

        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                fqn, entry_name, data_hash, metadata_hash, mapping_schema = future.result()
                model = models[fqn]
                model._data_hash = data_hash
                model._metadata_hash = metadata_hash
                model.set_mapping_schema(mapping_schema)
                optimized_query_cache.with_optimized_query(model, entry_name)
                _update_schema_with_model(schema, model)
                process_models(completed_model=model)
