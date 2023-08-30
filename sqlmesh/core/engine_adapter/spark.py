from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import (
    EngineAdapter,
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)
from sqlmesh.utils import nullsafe_join
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from pyspark.sql import types as spark_types

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        Query,
    )
    from sqlmesh.core.engine_adapter.base import QueryOrDF
    from sqlmesh.core.node import IntervalUnit


logger = logging.getLogger(__name__)


class SparkEngineAdapter(EngineAdapter):
    DIALECT = "spark"
    ESCAPE_JSON = True
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE

    @property
    def spark(self) -> PySparkSession:
        return self._connection_pool.get().spark

    @property
    def _use_spark_session(self) -> bool:
        return True

    @classmethod
    def convert_columns_to_types_to_pyspark_schema(
        cls, columns_to_types: t.Dict[str, exp.DataType]
    ) -> t.Optional[spark_types.StructType]:
        from pyspark.sql import types as spark_types

        mapping = {
            exp.DataType.Type.BOOLEAN: spark_types.BooleanType,
            exp.DataType.Type.TEXT: spark_types.StringType,
            exp.DataType.Type.INT: spark_types.IntegerType,
            exp.DataType.Type.BIGINT: spark_types.LongType,
            exp.DataType.Type.FLOAT: spark_types.FloatType,
            exp.DataType.Type.DOUBLE: spark_types.DoubleType,
            exp.DataType.Type.DECIMAL: spark_types.DecimalType,
            exp.DataType.Type.DATE: spark_types.DateType,
            exp.DataType.Type.TIMESTAMP: spark_types.TimestampType,
        }

        try:
            return spark_types.StructType(
                [
                    spark_types.StructField(col_name, mapping[col_type.this]())
                    for col_name, col_type in columns_to_types.items()
                ]
            )
        except KeyError as e:
            logger.warning(
                "Tried to convert column data types to Spark data types but got a type that was not supported."
                "Currently nested or complex data types are not supported. Therefore Spark will attempt to infer the"
                f"types from the data itself. Error: {str(e)}"
            )
            return None

    @classmethod
    def is_pyspark_df(cls, value: t.Any) -> bool:
        return hasattr(value, "sparkSession")

    @classmethod
    def try_get_pyspark_df(cls, value: t.Any) -> t.Optional[PySparkDataFrame]:
        if cls.is_pyspark_df(value):
            return value
        return None

    @classmethod
    def try_get_pandas_df(cls, value: t.Any) -> t.Optional[pd.DataFrame]:
        if cls.is_pandas_df(value):
            return value
        return None

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        batch_size: int,
        target_table: t.Optional[TableName] = None,
    ) -> t.List[SourceQuery]:
        def query_factory() -> Query:
            df = self._ensure_pyspark_df(df, columns_to_types)
            temp_table = self._get_temp_table(target_table or "spark", table_only=True)
            df.createOrReplaceTempView(temp_table.sql(dialect=self.dialect))
            temp_table.set("db", "global_temp")
            return exp.select("*").from_(temp_table)

        if self._use_spark_session:
            return [SourceQuery(query_factory=query_factory)]
        else:
            return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)

    def _ensure_pyspark_df(
        self, generic_df: DF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> PySparkDataFrame:
        pyspark_df = self.try_get_pyspark_df(generic_df)
        if pyspark_df:
            return pyspark_df
        df = self.try_get_pandas_df(generic_df)
        if df is None:
            raise SQLMeshError("Ensure PySpark DF can only be run on a PySpark or Pandas DataFrame")
        return self.spark.createDataFrame(  # type: ignore
            df,
            schema=self.convert_columns_to_types_to_pyspark_schema(columns_to_types)  # type: ignore
            if columns_to_types
            else None,
        )

    def fetchdf(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        return self.fetch_pyspark_df(query, quote_identifiers=quote_identifiers).toPandas()

    def fetch_pyspark_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> PySparkDataFrame:
        return self._ensure_pyspark_df(
            self._fetch_native_df(query, quote_identifiers=quote_identifiers)
        )

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
        **kwargs: t.Any,
    ) -> None:
        # Note: Some storage formats (like Delta and Iceberg) support REPLACE TABLE but since we don't
        # currently check for storage formats we will just do an insert/overwrite.
        source_queries, columns_to_types = self._get_source_query_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        return self._insert_overwrite_by_condition(
            table_name, source_queries, columns_to_types, where=exp.condition("1=1")
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
            partitioned_by=[exp.column(x) for x in primary_key] if primary_key else None,
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
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if storage_format:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=storage_format)))

        if partitioned_by:
            for expr in partitioned_by:
                if not isinstance(expr, exp.Column):
                    raise SQLMeshError(
                        f"PARTITIONED BY contains non-column value '{expr.sql(dialect='spark')}'."
                    )
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Schema(expressions=partitioned_by),
                )
            )

        properties.extend(self.__table_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _create_view_properties(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        if not table_properties:
            return None
        return exp.Properties(expressions=self.__table_properties_to_expressions(table_properties))

    @classmethod
    def __table_properties_to_expressions(
        cls, table_properties: t.Optional[t.Dict[str, exp.Expression]] = None
    ) -> t.List[exp.Property]:
        if not table_properties:
            return []
        return [
            exp.Property(this=key, value=value.copy()) for key, value in table_properties.items()
        ]

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False
