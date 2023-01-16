from __future__ import annotations

from enum import Enum


class TransactionType(str, Enum):
    DDL = "DDL"
    DML = "DML"

    @property
    def is_ddl(self) -> bool:
        return self == TransactionType.DDL

    @property
    def is_dml(self) -> bool:
        return self == TransactionType.DML
