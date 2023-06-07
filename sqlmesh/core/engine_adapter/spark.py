from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)
from sqlmesh.utils import nullsafe_join
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        QueryOrDF,
    )
    from sqlmesh.core.model.meta import IntervalUnit


class SparkEngineAdapter(EngineAdapter):
    DIALECT = "spark"
    ESCAPE_JSON = True
    SUPPORTS_INSERT_OVERWRITE = True

    @property
    def spark(self) -> PySparkSession:
        return self._connection_pool.get().spark

    @property
    def _use_spark_session(self) -> bool:
        return True

    def _ensure_pyspark_df(self, generic_df: DF) -> PySparkDataFrame:
        pyspark_df = self.try_get_pyspark_df(generic_df)
        if pyspark_df:
            return pyspark_df
        df = self.try_get_pandas_df(generic_df)
        if df is None:
            raise SQLMeshError("Ensure PySpark DF can only be run on a PySpark or Pandas DataFrame")
        return self.spark.createDataFrame(df)

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        return self.fetch_pyspark_df(query).toPandas()

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> PySparkDataFrame:
        return self._ensure_pyspark_df(self._fetch_native_df(query))

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        df = self.try_get_df(query_or_df)
        if self._use_spark_session and df is not None:
            self._insert_pyspark_df(table_name, self._ensure_pyspark_df(df), overwrite=True)
        else:
            super()._insert_overwrite_by_condition(table_name, query_or_df, where, columns_to_types)

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        df = self.try_get_df(query_or_df)
        if self._use_spark_session and df is not None:
            self._insert_append_pyspark_df(table_name, self._ensure_pyspark_df(df))
        else:
            super().insert_append(table_name, query_or_df, columns_to_types, contains_json)

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Dict[str, exp.DataType],
        unique_key: t.Sequence[str],
    ) -> None:
        column_names = columns_to_types.keys()
        df = self.try_get_df(source_table)
        if self._use_spark_session and df is not None:
            pyspark_df = self._ensure_pyspark_df(df)
            temp_view_name = self._get_temp_table(target_table, table_only=True).sql(
                dialect=self.dialect
            )
            pyspark_df.createOrReplaceTempView(temp_view_name)
            query = exp.select(*column_names).from_(temp_view_name)
            super().merge(target_table, query, columns_to_types, unique_key)
        else:
            super().merge(target_table, source_table, columns_to_types, unique_key)

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        if self._use_spark_session:
            self._insert_pyspark_df(table_name, self._ensure_pyspark_df(df), overwrite=False)
        else:
            super()._insert_append_pandas_df(table_name, df, columns_to_types, contains_json)

    def _insert_append_pyspark_df(
        self,
        table_name: TableName,
        df: PySparkDataFrame,
    ) -> None:
        self._insert_pyspark_df(table_name, df, overwrite=False)

    def _insert_pyspark_df(
        self,
        table_name: TableName,
        df: PySparkDataFrame,
        overwrite: bool = False,
    ) -> None:
        if isinstance(table_name, exp.Table):
            table_name = table_name.sql(dialect=self.dialect)

        df.select(*self.spark.table(table_name).columns).write.insertInto(  # type: ignore
            table_name, overwrite=overwrite
        )

    def _create_table_from_df(
        self,
        table_name: TableName,
        df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = True,
        **kwargs: t.Any,
    ) -> None:
        if self._use_spark_session:
            df = self._ensure_pyspark_df(df)
            if isinstance(table_name, exp.Table):
                table_name = table_name.sql(dialect=self.dialect)
            df.write.saveAsTable(table_name, mode="overwrite")
        else:
            super()._create_table_from_df(table_name, df, columns_to_types, exists, replace)

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        target = nullsafe_join(".", catalog_name, schema_name)
        sql = f"SHOW TABLE EXTENDED IN {target} LIKE '*'"
        results = (
            self.fetch_pyspark_df(sql).collect()
            if self._use_spark_session
            else self.fetchdf(sql).to_dict("records")
        )
        return [
            DataObject(
                catalog=catalog_name,
                schema=schema_name,
                name=row["tableName"],
                type=DataObjectType.VIEW
                if "Type: VIEW" in row["information"]
                else DataObjectType.TABLE,
            )
            for row in results  # type: ignore
        ]

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        # Note: Some storage formats (like Delta and Iceberg) support REPLACE TABLE but since we don't
        # currently check for storage formats we will just do an insert/overwrite.
        return self._insert_overwrite_by_condition(
            table_name, query_or_df, columns_to_types=columns_to_types
        )

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

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        """Create a view with a query or dataframe.

        If a dataframe is passed in, it will be converted into a literal values statement.
        This should only be done if the dataframe is very small!

        Args:
            view_name: The view name.
            query_or_df: A query or dataframe.
            columns_to_types: Columns to use in the view statement.
            replace: Whether or not to replace an existing view defaults to True.
            create_kwargs: Additional kwargs to pass into the Create expression
        """
        pyspark_df = self.try_get_pyspark_df(query_or_df)
        if pyspark_df:
            query_or_df = pyspark_df.toPandas()
        super().create_view(
            view_name, query_or_df, columns_to_types, replace, materialized, **create_kwargs
        )

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
    ) -> t.Optional[exp.Properties]:
        format_property = None
        partition_columns_property = None
        if storage_format:
            format_property = exp.FileFormatProperty(this=exp.Var(this=storage_format))
        if partitioned_by:
            partition_columns_property = exp.PartitionedByProperty(
                this=exp.Schema(
                    expressions=[exp.to_identifier(column) for column in partitioned_by]
                ),
            )
        return exp.Properties(
            expressions=[
                table_property
                for table_property in [format_property, partition_columns_property]
                if table_property
            ]
        )

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False
