from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.utils.pydantic import PydanticModel


class DataObjectType(str, Enum):
    UNKNOWN = "unknown"
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"

    @property
    def is_unknown(self) -> bool:
        return self == DataObjectType.UNKNOWN

    @property
    def is_table(self) -> bool:
        return self == DataObjectType.TABLE

    @property
    def is_view(self) -> bool:
        return self == DataObjectType.VIEW

    @property
    def is_materialized_view(self) -> bool:
        return self == DataObjectType.MATERIALIZED_VIEW

    @classmethod
    def from_str(cls, s: str) -> DataObjectType:
        s = s.lower()
        if s == "table":
            return DataObjectType.TABLE
        if s == "view":
            return DataObjectType.VIEW
        if s == "materialized_view":
            return DataObjectType.MATERIALIZED_VIEW
        return DataObjectType.UNKNOWN


class DataObject(PydanticModel):
    catalog: t.Optional[str] = None
    schema_name: str = Field(alias="schema")
    name: str
    type: DataObjectType
