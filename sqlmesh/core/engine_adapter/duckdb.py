from __future__ import annotations

import math
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import SourceQuery
from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class DuckDBEngineAdapter(LogicalMergeMixin):
    DIALECT = "duckdb"

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        batch_size: int,
        target_table: t.Optional[TableName] = None,
    ) -> t.List[SourceQuery]:
        assert columns_to_types
        temp_table = self._get_temp_table(target_table or "pandas")
        temp_table_sql = exp.select(*columns_to_types).from_("df").sql(dialect=self.dialect)
        self.cursor.sql(f"CREATE TABLE {temp_table} AS {temp_table_sql}")
        return [
            SourceQuery(
                query_factory=lambda: exp.select(*columns_to_types).from_(temp_table),  # type: ignore
            )
        ]

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        """
        We only support Pandas for views on DuckDB in order to provide more robust local testing support.
        """
        if not self.is_pandas_df(query_or_df):
            return super().create_view(
                view_name, query_or_df, columns_to_types, replace, materialized, **create_kwargs
            )
        assert columns_to_types, "Pandas requires columns_to_types to be set"
        values = list(t.cast(pd.DataFrame, query_or_df).itertuples(index=False, name=None))
        sql = self._values_to_sql(
            values,
            columns_to_types,
            batch_start=0,
            batch_end=len(values),
        )
        super().create_view(
            view_name, sql, columns_to_types, replace, materialized, **create_kwargs
        )

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog_name = f"'{catalog_name}'" if catalog_name else "NULL"
        query = f"""
            SELECT
              {catalog_name} as catalog,
              table_name as name,
              table_schema as schema,
              CASE table_type
                WHEN 'BASE TABLE' THEN 'table'
                WHEN 'VIEW' THEN 'view'
                WHEN 'LOCAL TEMPORARY' THEN 'table'
                END as type
            FROM information_schema.tables
            WHERE table_schema = '{ schema_name }'
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=None if math.isnan(row.catalog) else row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]
