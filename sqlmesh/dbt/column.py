from __future__ import annotations

import typing as t

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.dbt.common import GeneralConfig
from sqlmesh.utils.conversions import ensure_bool


def yaml_to_columns(yaml):
    columns = {}
    for column in ensure_list(yaml):
        config = ColumnConfig(**column)
        columns[config.name] = config

    return columns


class ColumnConfig(GeneralConfig):
    """
    Column configuration for a DBT project

    Args:
        name: Name of the column
        data_type: The column's data type
        description: User defined description of the column
        meta: Meta data associated with the column
        quote: Boolean flag to use quoting for the column name
        tests: Tests associated with this column
        tags: Tags associated with this column
    """

    name: str
    data_type: t.Optional[str] = None
    quote: t.Optional[bool] = False

    @validator("quote", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)
