from __future__ import annotations

import typing as t
import uuid

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter, EngineAdapterWithIndexSupport

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class PostgresBaseEngineAdapter(EngineAdapter):
    def table_exists(self, table_name: TableName) -> bool:
        """
        Redshift/Postgres doesn't support describe so I'm using what the redshift cursor does to check if a table
        exists. We don't use this directly because we still want all execution to go through our execute method

        Reference: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py#L528-L553
        """
        table = exp.to_table(table_name)

        # Redshift doesn't support catalog
        if table.args.get("catalog"):
            return False

        query = exp.select(1).from_("information_schema.tables")
        where = exp.condition(f"table_name = '{table.alias_or_name}'")

        schema = table.text("db")
        if schema:
            where = where.and_(f"table_schema = '{schema}'")

        self.execute(query.where(where))

        result = self.cursor.fetchone()

        return result[0] == 1 if result is not None else False


class PostgresEngineAdapter(PostgresBaseEngineAdapter, EngineAdapterWithIndexSupport):
    DIALECT = "postgres"
