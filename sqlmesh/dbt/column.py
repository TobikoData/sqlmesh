from __future__ import annotations

import typing as t

from sqlglot import exp, parse_one
from sqlglot.helper import ensure_list

from sqlmesh.dbt.common import GeneralConfig
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.pydantic import field_validator


def yaml_to_columns(
    yaml: t.Dict[str, ColumnConfig] | t.List[t.Dict[str, ColumnConfig]],
) -> t.Dict[str, ColumnConfig]:
    columns = {}
    mappings: t.List[t.Dict[str, ColumnConfig]] = ensure_list(yaml)
    for column in mappings:
        config = ColumnConfig(**column)
        columns[config.name] = config

    return columns


def column_types_to_sqlmesh(
    columns: t.Dict[str, ColumnConfig], dialect: t.Optional[str] = None
) -> t.Dict[str, exp.DataType]:
    """
    Get the sqlmesh column types

    Returns:
        A dict of column name to exp.DataType
    """
    return {
        name: parse_one(column.data_type, into=exp.DataType, dialect=dialect or "")
        for name, column in columns.items()
        if column.enabled and column.data_type
    }


def column_descriptions_to_sqlmesh(columns: t.Dict[str, ColumnConfig]) -> t.Dict[str, str]:
    """
    Get the sqlmesh column types

    Returns:
        A dict of column name to description
    """
    return {
        name: column.description
        for name, column in columns.items()
        if column.enabled and column.description
    }


class ColumnConfig(GeneralConfig):
    """
    Column configuration for a DBT project

    Args:
        name: Name of the column
        data_type: The column's data type
        quote: Boolean flag to use quoting for the column name
    """

    name: str
    data_type: t.Optional[str] = None
    quote: t.Optional[bool] = False

    @field_validator("quote", mode="before")
    @classmethod
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)
