from __future__ import annotations

import logging
import typing as t
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlglot.dialects.dialect import DialectType

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import yaml

logger = logging.getLogger(__name__)


def create_schema_file(
    path: Path,
    models: t.Dict[str, Model],
    adapter: EngineAdapter,
    state_reader: StateReader,
    dialect: DialectType,
    max_workers: int = 1,
) -> None:
    """Create or replace a YAML file with model schemas.

    Args:
        path: The path to store the YAML file.
        models: A dictionary of models to fetch columns from the db.
        adapter: The engine adapter.
        state_reader: The state reader.
        dialect: The dialect to serialize the schema as.
        max_workers: The max concurrent workers to fetch columns.
    """
    external_tables = set()

    for model in models.values():
        if model.kind.is_external:
            external_tables.add(model.name)
        for dep in model.depends_on:
            if dep not in models:
                external_tables.add(dep)

    # Make sure we don't convert internal models into external ones.
    existing_models = state_reader.nodes_exist(external_tables, exclude_external=True)
    if existing_models:
        logger.warning(
            "The following models already exist and can't be converted to external: %s."
            "Perhaps these models have been removed, while downstream models that reference them weren't updated accordingly",
            ", ".join(existing_models),
        )
        external_tables -= existing_models

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        schemas = [
            {
                "name": table,
                "columns": {c: t.sql(dialect=dialect) for c, t in columns.items()},
            }
            for table, columns in sorted(
                pool.map(
                    lambda table: (table, adapter.columns(table, include_pseudo_columns=True)),
                    external_tables,
                )
            )
        ]

        with open(path, "w", encoding="utf-8") as file:
            yaml.dump(schemas, file)
