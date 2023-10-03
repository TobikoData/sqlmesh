"""Contains MSSQLEngineAdapter."""


from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_dtype  # type: ignore
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

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
    DEFAULT_CATALOG_NAME = "master"
    SUPPORTS_TUPLE_IN = False
    SUPPORTS_MATERIALIZED_VIEWS = False

    def columns(
        self,
        table_name: TableName,
        include_pseudo_columns: bool = True,
    ) -> t.Dict[str, exp.DataType]:
        """MsSql doesn't support describe so we query information_schema."""

        table = exp.to_table(table_name)

        catalog_name = table.catalog or self.DEFAULT_CATALOG_NAME
        sql = (
            exp.select(
                "column_name",
                "data_type",
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
            )
            .from_(f"{catalog_name}.information_schema.columns")
            .where(f"table_name = '{table.name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        self.execute(sql)
        columns_raw = self.cursor.fetchall()

        def build_var_length_col(row: tuple) -> tuple:
            var_len_chars = ("binary", "varbinary", "char", "varchar", "nchar", "nvarchar")
            if row[1] in var_len_chars and row[2] > 0:
                return (row[0], f"{row[1]}({row[2]})")
            if row[1] in ("varbinary", "varchar") and row[2] == -1:
                return (row[0], f"{row[1]}(max)")
            if row[1] in (
                "decimal",
                "numeric",
            ):
                return (row[0], f"{row[1]}({row[3]}, {row[4]})")
            if row[1] == "float":
                return (row[0], f"{row[1]}({row[3]})")

            return (row[0], row[1])

        columns = [build_var_length_col(col) for col in columns_raw]

        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect)
            for column_name, data_type in columns
        }

    def table_exists(self, table_name: TableName) -> bool:
        """MsSql doesn't support describe so we query information_schema."""
        table = exp.to_table(table_name)

        catalog_name = table.catalog or self.DEFAULT_CATALOG_NAME
        sql = (
            exp.select("1")
            .from_(f"{catalog_name}.information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.db
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
                self.drop_table(
                    ".".join([obj.catalog, obj.schema_name, obj.name]), exists=ignore_if_not_exists  # type: ignore
                )
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
            # pymssql doesn't convert Pandas Timestamp (datetime64) types
            # - this code is based on snowflake adapter implementation
            for column, kind in (columns_to_types or {}).items():
                if kind.is_type("date") and is_datetime64_dtype(df.dtypes[column]):  # type: ignore
                    df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d")  # type: ignore
                elif is_datetime64_dtype(df.dtypes[column]):  # type: ignore
                    df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # type: ignore

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
        catalog_name = f"[{catalog_name}]" if catalog_name else self.DEFAULT_CATALOG_NAME
        query = f"""
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS type
            FROM {catalog_name}.INFORMATION_SCHEMA.TABLES
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

    def _truncate_table(self, table_name: TableName) -> str:
        table = quote_identifiers(exp.to_table(table_name))
        return f"TRUNCATE TABLE {table.sql(dialect=self.dialect)}"

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        sql = super()._to_sql(expression, quote=quote, **kwargs)
        return f"{sql};"
