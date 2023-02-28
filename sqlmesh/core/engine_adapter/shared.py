from __future__ import annotations

from enum import Enum

from pydantic import Field

from sqlmesh.utils.pydantic import PydanticModel


class TransactionType(str, Enum):
    DDL = "DDL"
    DML = "DML"

    @property
    def is_ddl(self) -> bool:
        return self == TransactionType.DDL

    @property
    def is_dml(self) -> bool:
        return self == TransactionType.DML


class DataObjectType(str, Enum):
    UNKNOWN = "unknown"
    TABLE = "table"
    VIEW = "view"

    @classmethod
    def from_str(cls, s: str) -> DataObjectType:
        s = s.lower()
        if s == "table":
            return DataObjectType.TABLE
        if s == "view":
            return DataObjectType.VIEW
        return DataObjectType.UNKNOWN


class DataObject(PydanticModel):
    catalog: str
    schema_name: str = Field(alias="schema")
    name: str
    type: DataObjectType
