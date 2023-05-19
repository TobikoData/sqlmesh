from __future__ import annotations

import typing as t
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ruamel.yaml import YAML
from sqlglot.dialects.dialect import DialectType

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.utils.date import now


def create_schema_file(
    path: Path,
    models: t.Dict[str, Model],
    adapter: EngineAdapter,
    dialect: DialectType,
    max_workers: int = 1,
) -> None:
    """Create or replace a YAML file with model schemas.

    Args:
        path: The path to store the YAML file.
        models: A dictionary of models to fetch columns from the db.
        adapter: The engine adapter.
        dialect: The dialect to serialize the schema as.
        max_workers: The max concurrent workers to fetch columns.
    """
    external_tables = {
        dep for model in models.values() for dep in model.depends_on if dep not in models
    }

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        schemas = [
            {
                "name": table,
                "columns": {c: t.sql(dialect=dialect) for c, t in columns.items()},
            }
            for table, columns in sorted(
                pool.map(lambda table: (table, adapter.columns(table)), external_tables)
            )
        ]

        with open(path, "w", encoding="utf-8") as file:
            YAML().dump(schemas, file)
