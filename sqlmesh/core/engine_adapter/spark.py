from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter._typing import PySparkDataFrame, PySparkSession
from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class SparkEngineAdapter(BaseSparkEngineAdapter):
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, "spark", multithreaded=multithreaded)

    @property
    def spark(self) -> PySparkSession:
        return self._connection_pool.get().spark

    def _ensure_pyspark_df(self, df: DF) -> PySparkDataFrame:
        if not isinstance(df, PySparkDataFrame):
            return self.spark.createDataFrame(df)
        return df

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        return t.cast(PySparkDataFrame, self._fetch_native_df(query)).toPandas()

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> PySparkDataFrame:
        return t.cast(PySparkDataFrame, self._fetch_native_df(query))

    def _insert_overwrite_by_condition(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(query_or_df, (pd.DataFrame, PySparkDataFrame)):
            self._insert_pyspark_df(
                table_name, self._ensure_pyspark_df(query_or_df), overwrite=True
            )
        else:
            super()._insert_overwrite_by_condition(
                table_name, query_or_df, where, columns_to_types
            )

    def insert_append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(query_or_df, PySparkDataFrame):
            self._insert_append_pyspark_df(table_name, query_or_df)
        else:
            super().insert_append(table_name, query_or_df, columns_to_types)

    def _insert_append_pandas_df(
        self,
        table_name: str,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self._insert_pyspark_df(
            table_name, self._ensure_pyspark_df(df), overwrite=False
        )

    def _insert_append_pyspark_df(
        self,
        table_name: str,
        df: PySparkDataFrame,
    ) -> None:
        self._insert_pyspark_df(table_name, df, overwrite=False)

    def _insert_pyspark_df(
        self,
        table_name: str,
        df: PySparkDataFrame,
        overwrite: bool = False,
    ) -> None:
        df.select(*self.spark.table(table_name).columns).write.insertInto(  # type: ignore
            table_name, overwrite=overwrite
        )
