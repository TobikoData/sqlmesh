"""Contains MSSQLEngineAdapter."""


from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.engine_adapter.base import (
    CatalogSupport,
    EngineAdapterWithIndexSupport,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    InsertOverwriteWithMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType, set_catalog

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF


class MSSQLEngineAdapter(
    EngineAdapterWithIndexSupport,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
    InsertOverwriteWithMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    """Implementation of EngineAdapterWithIndexSupport for MsSql compatibility.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT: str = "tsql"
    SUPPORTS_TUPLE_IN = False
    SUPPORTS_MATERIALIZED_VIEWS = False
    CATALOG_SUPPORT = CatalogSupport.REQUIRES_SET_CATALOG
    CURRENT_CATALOG_EXPRESSION = exp.func("db_name")

    @set_catalog()
    def columns(
        self,
        table_name: TableName,
        include_pseudo_columns: bool = True,
    ) -> t.Dict[str, exp.DataType]:
        """MsSql doesn't support describe so we query information_schema."""

        table = exp.to_table(table_name)

        sql = (
            exp.select(
                "column_name",
                "data_type",
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
            )
            .from_("information_schema.columns")
            .where(f"table_name = '{table.name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        columns_raw = self.fetchall(sql, quote_identifiers=True)

        def build_var_length_col(row: tuple) -> tuple:
            var_len_chars = ("binary", "varbinary", "char", "varchar", "nchar", "nvarchar")
            if row[1] in var_len_chars and row[2] > 0:
                return (row[0], f"{row[1]}({row[2]})")
            if row[1] in ("varbinary", "varchar", "nvarchar") and row[2] == -1:
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

        sql = (
            exp.select("1")
            .from_("information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        result = self.fetchone(sql, quote_identifiers=True)

        return result[0] == 1 if result else False

    def set_current_catalog(self, catalog_name: str) -> None:
        self.execute(exp.Use(this=exp.to_identifier(catalog_name)))

    @set_catalog()
    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        """
        MsSql doesn't support CASCADE clause and drops schemas unconditionally.
        """
        if cascade:
            objects = self._get_data_objects(schema_name)
            for obj in objects:
                # _get_data_objects is catalog-specific, so these can't accidentally drop view/tables in another catalog
                if obj.type == DataObjectType.VIEW:
                    self.drop_view(
                        ".".join([obj.schema_name, obj.name]), ignore_if_not_exists=ignore_if_not_exists  # type: ignore
                    )
                else:
                    self.drop_table(
                        ".".join([obj.schema_name, obj.name]), exists=ignore_if_not_exists  # type: ignore
                    )
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)

    @set_catalog()
    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        super().create_view(
            view_name,
            query_or_df,
            columns_to_types=columns_to_types,
            replace=replace,
            materialized=materialized,
            **create_kwargs,
        )

    @set_catalog()
    def drop_view(
        self, view_name: TableName, ignore_if_not_exists: bool = True, materialized: bool = False
    ) -> None:
        super().drop_view(
            view_name, ignore_if_not_exists=ignore_if_not_exists, materialized=materialized
        )

    def _convert_df_datetime(self, df: DF, columns_to_types: t.Dict[str, exp.DataType]) -> None:
        # pymssql doesn't convert Pandas Timestamp (datetime64) types
        # - this code is based on snowflake adapter implementation
        for column, kind in columns_to_types.items():
            # pymssql errors if the column contains a datetime.date object
            if kind.is_type("date"):  # type: ignore
                df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d")  # type: ignore
            elif is_datetime64_any_dtype(df.dtypes[column]):  # type: ignore
                if getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                    # MSSQL requires a colon in the offset (+00:00) so we use isoformat() instead of strftime()
                    df[column] = pd.to_datetime(df[column]).map(lambda x: x.isoformat(" "))  # type: ignore

                    # bulk_copy() doesn't work with TZ timestamp, so load into string column and cast to
                    # timestamp in SELECT statement
                    columns_to_types[column] = exp.DataType.build("TEXT")
                else:
                    df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # type: ignore

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
            columns_to_types_create = columns_to_types.copy()

            self._convert_df_datetime(df, columns_to_types_create)

            self.create_table(temp_table, columns_to_types_create)
            rows: t.List[t.Tuple[t.Any, ...]] = list(df.itertuples(index=False, name=None))  # type: ignore
            conn = self._connection_pool.get()
            conn.bulk_copy(temp_table.sql(dialect=self.dialect), rows)
            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)  # type: ignore

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

    @set_catalog()
    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and catalog.
        """
        catalog_name = self.get_current_catalog()
        query = f"""
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS type
            FROM INFORMATION_SCHEMA.TABLES
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
