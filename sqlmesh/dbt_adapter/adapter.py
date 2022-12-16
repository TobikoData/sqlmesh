from __future__ import annotations

import re
from pathlib import Path

from sqlmesh.core.model import Model, ModelKind
from sqlmesh.dbt_adapter.config import Config, Materialization, ModelConfig
from sqlmesh.utils.errors import ConfigError


def dbt_to_sqlmesh():
    """
    Loads a DBT project into sqlmesh. Currently only supports sqlmesh model creation.
    """
    dbt_config = Config()
    model_config = dbt_config.get_model_config()

    [sqlmesh_model(config, path) for (config, path) in model_config.values()]


def sqlmesh_model(config: ModelConfig, path: Path) -> Model:
    """
    Creates sqlmesh model from DBT model

    Args:
        config: The DBT config for the model
        path: The path from the root project folder to the model

    Returns:
        The sqlmesh model
    """
    return Model(name=model_name(config), kind=model_kind(config), query=query(path))


def model_name(config: ModelConfig):
    """
    Get the sqlmesh model name

    Args:
        config: The DBT config for the model

    Returns:
        The sqlmesh model name
    """
    name = ""
    if config.schema_:
        name += config.schema_
    if config.identifier:
        name += "." + config.identifier
    return name


def model_kind(config: ModelConfig) -> ModelKind:
    """
    Get the sqlmesh ModelKind

    Args:
        config: The DBT config for the model

    Returns:
        The sqlmesh ModelKind
    """
    materialization = config.materialized
    if materialization == Materialization.TABLE:
        return ModelKind.FULL
    if materialization == Materialization.VIEW:
        return ModelKind.VIEW
    if materialization == Materialization.INCREMENTAL:
        return ModelKind.INCREMENTAL
    if materialization == Materialization.EPHERMAL:
        return ModelKind.EMBEDDED

    raise ConfigError(f"{materialization.value} materialization not supported")


def query(path: Path) -> str:
    """
    Get the sqlmesh query

    Args:
        path: Path from project root to the DBT model

    Returns:
        sqlmesh query
    """
    with path.open(encoding="utf-8") as file:
        query = file.read()

    return _remove_config_jinja(query)


def _remove_config_jinja(query: str) -> str:
    """
    Removes jinja for config method calls from a query

    args:
        query: The query

    Returns:
        The query without the config method calls
    """
    return re.sub("{{\s*config(.|\s)*?}}", "", query).strip()
