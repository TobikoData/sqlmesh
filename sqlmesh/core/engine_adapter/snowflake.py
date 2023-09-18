from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_dtype  # type: ignore
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter, SourceQuery
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType
from sqlmesh.utils import nullsafe_join

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query


class SnowflakeEngineAdapter(EngineAdapter):
    DEFAULT_SQL_GEN_KWARGS = {"identify": False}
    DIALECT = "snowflake"
    ESCAPE_JSON = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CLONING = True

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
                if kind.is_type("date") and is_datetime64_dtype(df.dtypes[column]):  # type: ignore
                    df[column] = pd.to_datetime(df[column]).dt.date  # type: ignore
                # https://github.com/snowflakedb/snowflake-connector-python/issues/1677
                elif is_datetime64_dtype(df.dtypes[column]):  # type: ignore
                    df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # type: ignore
            self.create_table(temp_table, columns_to_types, exists=False)
            write_pandas(
                self._connection_pool.get(),
                df,
                temp_table.name,
                schema=temp_table.db,
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

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        target = nullsafe_join(".", catalog_name, schema_name)
        sql = f"SHOW TERSE OBJECTS IN {target}"
        df = self.fetchdf(sql, quote_identifiers=True)
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
