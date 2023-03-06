from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.dialect import pandas_to_sql
from sqlmesh.core.engine_adapter._typing import DF_TYPES, Query
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.client import Connection as BigQueryConnection
    from google.cloud.bigquery.table import Table as BigQueryTable

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class BigQueryEngineAdapter(EngineAdapter):
    DIALECT = "bigquery"
    DEFAULT_BATCH_SIZE = 1000
    ESCAPE_JSON = True

    @property
    def client(self) -> BigQueryClient:
        return self.cursor.connection._client

    @property
    def connection(self) -> BigQueryConnection:
        return self.cursor.connection

    def create_schema(self, schema_name: str, ignore_if_exists: bool = True) -> None:
        """Create a schema from a name or qualified table name."""
        from google.cloud.bigquery.dbapi.exceptions import DatabaseError

        try:
            super().create_schema(schema_name, ignore_if_exists=ignore_if_exists)
        except DatabaseError as e:
            for arg in e.args:
                if ignore_if_exists and "Already Exists: " in arg.message:
                    return
            raise e

    def columns(self, table_name: TableName) -> t.Dict[str, str]:
        """Fetches column names and types for the target table."""
        table = self._get_table(table_name)
        return {field.name: field.field_type for field in table.schema}

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        """
        Bigquery does not directly support `INSERT OVERWRITE` but it does support `MERGE` with a `False`
        condition and delete that mimics an `INSERT OVERWRITE`. Based on documentation this should have the
        same runtime performance as `INSERT OVERWRITE`.
        """
        table = exp.to_table(table_name)
        query: t.Union[Query, exp.Select]
        if isinstance(query_or_df, DF_TYPES):
            if not isinstance(query_or_df, pd.DataFrame):
                raise SQLMeshError("BigQuery only supports pandas DataFrames")
            if columns_to_types is None:
                raise SQLMeshError("columns_to_types must be provided when using Pandas DataFrames")
            query = next(
                pandas_to_sql(
                    query_or_df,
                    alias=table.alias_or_name,
                    columns_to_types=columns_to_types,
                )
            )
        else:
            query = t.cast(Query, query_or_df)
        on = exp.FALSE
        when_not_matched_by_source = exp.When(
            matched=False,
            source=True,
            condition=where,
            then=exp.Delete(),
        )
        when_not_matched_by_target = exp.When(matched=False, source=False, then=exp.Insert())
        self._merge(
            target_table=table,
            source_table=query,
            on=on,
            match_expressions=[when_not_matched_by_source, when_not_matched_by_target],
        )

    def table_exists(self, table_name: TableName) -> bool:
        from google.cloud.exceptions import NotFound

        try:
            self._get_table(table_name)
            return True
        except NotFound:
            return False

    def _get_table(self, table_name: TableName) -> BigQueryTable:
        """
        Returns a BigQueryTable object for the given table name.

        Raises: `google.cloud.exceptions.NotFound` if the table does not exist.
        """
        if isinstance(table_name, exp.Table):
            table_name = table_name.sql(dialect=self.dialect)

        return self.client.get_table(table_name)

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        self.execute(query)
        return self.cursor._query_job.to_dataframe()

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
    ) -> t.Optional[exp.Properties]:
        if not partitioned_by:
            return None
        if partition_interval_unit is None:
            raise SQLMeshError("partition_interval_unit is required when partitioning a table")
        if partition_interval_unit == IntervalUnit.MINUTE:
            raise SQLMeshError("BigQuery does not support partitioning by minute")
        if len(partitioned_by) > 1:
            raise SQLMeshError("BigQuery only supports partitioning by a single column")
        partition_col = partitioned_by[0]
        expressions: t.List[exp.Expression] = [exp.to_column(partition_col)]
        if partition_interval_unit == IntervalUnit.HOUR:
            date_func = "DATETIME_TRUNC"
            expressions.append(exp.Var(this=IntervalUnit.HOUR.value.upper()))
        else:
            date_func = "DATE"

        partition_columns_property = exp.PartitionedByProperty(
            this=exp.Anonymous(
                this=date_func,
                expressions=expressions,
            )
        )
        return exp.Properties(expressions=[partition_columns_property])

    def create_state_table(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        self.create_table(
            table_name,
            columns_to_types,
            partitioned_by=primary_key,
        )

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        from google.cloud.bigquery import DatasetReference

        dataset_ref = DatasetReference(
            project=catalog_name or self.client.project, dataset_id=schema_name
        )
        all_tables = self.client.list_tables(dataset_ref)
        return [
            DataObject(
                catalog=table.project,
                schema=table.dataset_id,
                name=table.table_id,
                type=DataObjectType.from_str(table.table_type),
            )
            for table in all_tables
        ]
