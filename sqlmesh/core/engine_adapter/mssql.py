"""Contains MSSQLEngineAdapter."""


from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport, SourceQuery
from sqlmesh.core.engine_adapter.mixins import (
    InsertOverwriteWithMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    import pymssql

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query


class MSSQLEngineAdapter(
    EngineAdapterWithIndexSupport,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
    InsertOverwriteWithMergeMixin,
):
    """Implementation of EngineAdapterWithIndexSupport for MsSql compatibility.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT: str = "tsql"
    FALSE_PREDICATE = exp.condition("1=2")

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

        return result[0] == 1 if result else False

    @property
    def connection(self) -> pymssql.Connection:
        return self.cursor.connection

    def drop_schema(
        self, schema_name: str, ignore_if_not_exists: bool = True, cascade: bool = False
    ) -> None:
        """
        MsSql doesn't support CASCADE clause and drops schemas unconditionally.
        """
        if cascade:
            # Note: Assumes all objects in the schema are captured by the `_get_data_objects` call and can be dropped
            # with a `drop_table` call.
            objects = self._get_data_objects(schema_name)
            for obj in objects:
                self.drop_table(obj.name, exists=ignore_if_not_exists)
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        assert isinstance(df, pd.DataFrame)
        temp_table = self._get_temp_table(target_table or "pandas")

        def query_factory() -> Query:
            self.create_table(temp_table, columns_to_types)
            rows: t.List[t.Tuple[t.Any, ...]] = list(df.itertuples(index=False, name=None))  # type: ignore
            conn = self._connection_pool.get()
            conn.bulk_copy(temp_table.sql(dialect=self.dialect), rows)
            return exp.select(*columns_to_types).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

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
