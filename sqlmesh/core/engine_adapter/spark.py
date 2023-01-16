from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter._typing import PySparkDataFrame, pyspark
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    TransactionType,
    hive_create_table_properties,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class SparkEngineAdapter(EngineAdapter):
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, "spark", multithreaded=multithreaded)

    @property
    def spark(self) -> pyspark.sql.SparkSession:
        return self._connection_pool.get().spark

    def _ensure_pyspark_df(self, df: DF) -> PySparkDataFrame:
        if not isinstance(df, PySparkDataFrame):
            return self.spark.createDataFrame(df)
        return df

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        return self._fetch_native_df(query).toPandas()  # type: ignore

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> PySparkDataFrame:
        return self._fetch_native_df(query)  # type: ignore

    def _insert_overwrite_by_time_partition(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        where: exp.Condition,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ):
        if isinstance(query_or_df, (pd.DataFrame, PySparkDataFrame)):
            self._insert_pyspark_df(
                table_name, self._ensure_pyspark_df(query_or_df), overwrite=True
            )
        else:
            self.execute(
                exp.Insert(
                    this=self._insert_into_expression(table_name, columns_to_types),
                    expression=query_or_df,
                    overwrite=True,
                )
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
    ):
        self._insert_pyspark_df(
            table_name, self._ensure_pyspark_df(df), overwrite=False
        )

    def _insert_append_pyspark_df(
        self,
        table_name: str,
        df: PySparkDataFrame,
    ):
        self._insert_pyspark_df(table_name, df, overwrite=False)

    def _insert_pyspark_df(
        self,
        table_name: str,
        df: PySparkDataFrame,
        overwrite: bool = False,
    ):
        df.select(*self.spark.table(table_name).columns).write.insertInto(  # type: ignore
            table_name, overwrite=overwrite
        )

    def alter_table(
        self,
        table_name: str,
        added_columns: t.Dict[str, str],
        dropped_columns: t.Sequence[str],
    ) -> None:
        alter_table = exp.AlterTable(this=exp.to_table(table_name))

        if added_columns:
            add_columns = exp.Schema(
                expressions=[
                    exp.ColumnDef(
                        this=exp.to_identifier(column_name),
                        kind=parse_one(column_type, into=exp.DataType),  # type: ignore
                    )
                    for column_name, column_type in added_columns.items()
                ],
            )
            alter_table.set("actions", [add_columns])
            self.execute(alter_table)

        if dropped_columns:
            drop_columns = exp.Drop(
                this=exp.Schema(
                    expressions=[
                        exp.to_identifier(column_name)
                        for column_name in dropped_columns
                    ]
                ),
                kind="COLUMNS",
            )
            alter_table.set("actions", [drop_columns])
            self.execute(alter_table)

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
    ) -> t.Optional[exp.Properties]:
        return hive_create_table_properties(storage_format, partitioned_by)

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False
