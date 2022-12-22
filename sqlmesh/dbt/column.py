from __future__ import annotations

import typing as t

from pydantic import validator

from sqlmesh.dbt.common import parse_meta
from sqlmesh.utils.datatype import ensure_bool, ensure_list
from sqlmesh.utils.pydantic import PydanticModel


class ColumnConfig(PydanticModel):
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
    description: t.Optional[str] = None
    meta: t.Optional[t.Dict[str, t.Any]] = {}
    quote: t.Optional[bool] = False
    tests: t.Optional[t.List[str]] = []
    tags: t.Optional[t.List[str]] = []

    @validator(
        "tests",
        "tags",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        return parse_meta(v)

    @validator("quote", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)
