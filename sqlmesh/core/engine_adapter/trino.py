from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import (
    CatalogSupport,
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    HiveMetastoreTablePropertiesMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from trino.dbapi import Connection as TrinoConnection

    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF


class TrinoEngineAdapter(
    PandasNativeFetchDFSupportMixin,
    LogicalReplaceQueryMixin,
    HiveMetastoreTablePropertiesMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "trino"
    ESCAPE_JSON = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INTO_IS_OVERWRITE
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    # Trino does technically support transactions but it doesn't work correctly with partition overwrite so we
    # disable transactions. If we need to get them enabled again then we would need to disable auto commit on the
    # connector and then figure out how to get insert/overwrite to work correctly without it.
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_ROW_LEVEL_OP = False
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")

    @property
    def connection(self) -> TrinoConnection:
        return self.cursor.connection

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(f"USE {catalog}.information_schema")

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
    ) -> None:
        self.execute(
            f"SET SESSION {self.get_current_catalog()}.insert_existing_partitions_behavior='OVERWRITE'"
        )
        super()._insert_overwrite_by_condition(table_name, source_queries, columns_to_types, where)
        self.execute(
            f"SET SESSION {self.get_current_catalog()}.insert_existing_partitions_behavior='APPEND'"
        )

    def _truncate_table(self, table_name: TableName) -> str:
        table = quote_identifiers(exp.to_table(table_name))
        return f"DELETE FROM {table.sql(dialect=self.dialect)}"

    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        schema_name = to_schema(schema_name)
        schema = schema_name.db
        catalog = schema_name.catalog or self.get_current_catalog()
        query = f"""
            SELECT
                t.table_catalog AS catalog,
                t.table_name AS name,
                t.table_schema AS schema,
                CASE
                    WHEN mv.name is not null THEN 'materialized_view'
                    WHEN t.table_type = 'BASE TABLE' THEN 'table'
                    ELSE t.table_type
                END AS type
            FROM {catalog}.information_schema.tables t
            LEFT JOIN system.metadata.materialized_views mv
                ON mv.catalog_name = t.table_catalog
                AND mv.schema_name = t.table_schema
                AND mv.name = t.table_name
            WHERE
                t.table_schema = '{schema}'
                AND (mv.catalog_name is null OR mv.catalog_name =  '{catalog}')
                AND (mv.schema_name is null OR mv.schema_name =  '{schema}')
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        # Trino does not accept timestamps in ISOFORMAT that include the "T". `execution_time` is stored in
        # Pandas with that format, so we convert the column to a string with the proper format and CAST to
        # timestamp in Trino.
        for column, kind in (columns_to_types or {}).items():
            if is_datetime64_any_dtype(df.dtypes[column]) and getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                df[column] = pd.to_datetime(df[column]).map(lambda x: x.isoformat(" "))  # type: ignore

        return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)
