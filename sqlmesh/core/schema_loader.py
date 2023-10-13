from __future__ import annotations

import logging
import typing as t
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model.definition import Model
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import UniqueKeyDict, yaml

logger = logging.getLogger(__name__)


def create_schema_file(
    path: Path,
    models: UniqueKeyDict[str, Model],
    adapter: EngineAdapter,
    state_reader: StateReader,
    dialect: DialectType,
    default_catalog: t.Optional[str],
    max_workers: int = 1,
) -> None:
    """Create or replace a YAML file with model schemas.

    Args:
        path: The path to store the YAML file.
        models: FQN to model
        adapter: The engine adapter.
        state_reader: The state reader.
        dialect: The dialect to serialize the schema as.
        max_workers: The max concurrent workers to fetch columns.
    """
    external_model_fqns = set()
    external_model_names = set()

    for fqn, model in models.items():
        if model.kind.is_external:
            external_model_fqns.add(fqn)
            external_model_names.add(model.name)
        for dep in model.depends_on:
            if dep not in models:
                external_model_fqns.add(dep)
                external_model_names.add(dep)
                external_table_name = exp.to_table(dep)
                if default_catalog and external_table_name.catalog == default_catalog:
                    external_table_name.set("catalog", None)
                    external_model_names.add(
                        normalize_model_name(
                            external_table_name, default_catalog=None, dialect=dialect
                        )
                    )

    # Make sure we don't convert internal models into external ones.
    existing_model_names = state_reader.nodes_exist(external_model_names, exclude_external=True)
    if existing_model_names:
        logger.warning(
            "The following models already exist and can't be converted to external: %s."
            "Perhaps these models have been removed, while downstream models that reference them weren't updated accordingly",
            ", ".join(existing_model_names),
        )
        external_model_fqns -= {
            normalize_model_name(model_name, default_catalog=default_catalog, dialect=dialect)
            for model_name in existing_model_names
        }

    with ThreadPoolExecutor(max_workers=max_workers) as pool:

        def _get_columns(table: str) -> t.Optional[t.Dict[str, t.Any]]:
            try:
                return adapter.columns(table, include_pseudo_columns=True)
            except Exception as e:
                logger.warning(f"Unable to get schema for '{table}': '{e}'.")
                return None

        schemas = [
            {
                "name": exp.to_table(table).sql(dialect=dialect),
                "columns": {c: dtype.sql(dialect=dialect) for c, dtype in columns.items()},
            }
            for table, columns in sorted(
                pool.map(
                    lambda table: (table, _get_columns(table)),
                    external_model_fqns,
                )
            )
            if columns
        ]

        with open(path, "w", encoding="utf-8") as file:
            yaml.dump(schemas, file)
