from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter._typing import PySparkDataFrame, PySparkSession
from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType
from sqlmesh.utils import nullsafe_join

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class SparkEngineAdapter(BaseSparkEngineAdapter):
    DIALECT = "spark"

    @property
    def spark(self) -> PySparkSession:
        return self._connection_pool.get().spark

    def _ensure_pyspark_df(self, df: DF) -> PySparkDataFrame:
        if not isinstance(df, PySparkDataFrame):
            return self.spark.createDataFrame(df)
        return df

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        return self.fetch_pyspark_df(query).toPandas()

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> PySparkDataFrame:
        return t.cast(PySparkDataFrame, self._fetch_native_df(query))

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(query_or_df, (pd.DataFrame, PySparkDataFrame)):
            self._insert_pyspark_df(
                table_name, self._ensure_pyspark_df(query_or_df), overwrite=True
            )
        else:
            super()._insert_overwrite_by_condition(table_name, query_or_df, where, columns_to_types)

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        if isinstance(query_or_df, PySparkDataFrame):
            self._insert_append_pyspark_df(table_name, query_or_df)
        else:
            super().insert_append(table_name, query_or_df, columns_to_types, contains_json)

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self._insert_pyspark_df(table_name, self._ensure_pyspark_df(df), overwrite=False)

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

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        target = nullsafe_join(".", catalog_name, schema_name)
        df = self.fetch_pyspark_df(f"SHOW TABLE EXTENDED IN {target} LIKE '*'")
        return [
            DataObject(
                catalog=catalog_name,
                schema=schema_name,
                name=row["tableName"],
                type=DataObjectType.VIEW
                if "Type: VIEW" in row["information"]
                else DataObjectType.TABLE,
            )
            for row in df.collect()
        ]
