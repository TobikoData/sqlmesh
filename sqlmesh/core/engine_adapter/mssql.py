"""Contains MSSQLEngineAdapter."""


from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.mixins import (
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class MSSQLEngineAdapter(
    EngineAdapterWithIndexSupport,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
):
    """Implementation of EngineAdapterWithIndexSupport for MsSql compatibility.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT: str = "tsql"

    def table_exists(self, table_name: TableName) -> bool:
        """
        Similar to Postgres, MsSql doesn't support describe so I'm using what
        is used there and what the redshift cursor does to check if a table
        exists. We don't use this directly in order for this to work as a base
        class for other postgres.

        Reference: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py#L528-L553
        """
        table = exp.to_table(table_name)

        catalog_name = table.args.get("catalog") or "master"
        sql = (
            exp.select("1")
            .from_(f"{catalog_name}.information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.args.get("db")
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        self.execute(sql)

        result = self.cursor.fetchone()

        return result[0] == 1 if result is not None else False

    def _get_data_objects(
        self,
        schema_name: str,
        catalog_name: t.Optional[str] = None,
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and catalog.
        """
        catalog_name = f"[{catalog_name}]" if catalog_name else "master"
        query = f"""
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                'TABLE' AS type
            FROM {catalog_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA LIKE '%{schema_name}%'
            UNION ALL
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                'VIEW' AS type
            FROM {catalog_name}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA LIKE '%{schema_name}%'
        """
        dataframe: pd.DataFrame = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in dataframe.itertuples()
        ]
