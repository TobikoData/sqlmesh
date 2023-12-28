from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import GetCurrentCatalogFromFunctionMixin
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query


@set_catalog(
    override_mapping={
        "_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG,
        "create_schema": CatalogSupport.REQUIRES_SET_CATALOG,
        "drop_schema": CatalogSupport.REQUIRES_SET_CATALOG,
    }
)
class SnowflakeEngineAdapter(GetCurrentCatalogFromFunctionMixin):
    DIALECT = "snowflake"
    ESCAPE_JSON = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CLONING = True
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")

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
            from snowflake.connector.pandas_tools import write_pandas

            # Workaround for https://github.com/snowflakedb/snowflake-connector-python/issues/1034
            #
            # The above issue has already been fixed upstream, but we keep the following
            # line anyway in order to support a wider range of Snowflake versions.
            self.cursor.execute(f'USE SCHEMA "{temp_table.db}"')

            # See: https://stackoverflow.com/a/75627721
            for column, kind in columns_to_types.items():
                if is_datetime64_any_dtype(df.dtypes[column]):
                    if kind.is_type("date"):  # type: ignore
                        df[column] = pd.to_datetime(df[column]).dt.date  # type: ignore
                    elif getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                        df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")  # type: ignore
                    # https://github.com/snowflakedb/snowflake-connector-python/issues/1677
                    else:  # type: ignore
                        df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # type: ignore
            self.create_table(temp_table, columns_to_types, exists=False)
            write_pandas(
                self._connection_pool.get(),
                df,
                temp_table.name,
                schema=temp_table.db or None,
                database=temp_table.catalog or None,
                chunk_size=self.DEFAULT_BATCH_SIZE,
            )
            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        from snowflake.connector.errors import NotSupportedError

        self.execute(query, quote_identifiers=quote_identifiers)

        try:
            return self.cursor.fetch_pandas_all()
        except NotSupportedError:
            # Sometimes Snowflake will not return results as an Arrow result and the fetch from
            # pandas will fail (Ex: `SHOW TERSE OBJECTS IN SCHEMA`). Therefore we manually convert
            # the result into a DataFrame when this happens.
            rows = self.cursor.fetchall()
            columns = self.cursor._result_set.batches[0].column_names
            return pd.DataFrame([dict(zip(columns, row)) for row in rows])

    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        from snowflake.connector.errors import ProgrammingError

        sql = f'SHOW TERSE OBJECTS IN "{to_schema(schema_name).sql(dialect=self.dialect)}"'
        try:
            df = self.fetchdf(sql, quote_identifiers=True)
        except ProgrammingError as e:
            if "Object does not exist" in str(e):
                return []
            raise e
        if df.empty:
            return []
        return [
            DataObject(
                catalog=row.database_name,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.kind),  # type: ignore
            )
            for row in df[["database_name", "schema_name", "name", "kind"]].itertuples()
        ]

    def set_current_catalog(self, catalog: str) -> None:
        self.execute(exp.Use(this=exp.to_identifier(catalog)))
