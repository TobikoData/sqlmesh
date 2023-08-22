"""Contains MSSQLEngineAdapter."""


from __future__ import annotations

import contextlib
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.mixins import (
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    import pymssql

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import Query, QueryOrDF


class MSSQLEngineAdapter(
    EngineAdapterWithIndexSupport,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
):
    """Implementation of EngineAdapterWithIndexSupport for MsSql compatibility.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT: str = "tsql"

    def table_exists(self, table_name: TableName) -> bool:
        """
        Similar to Postgres, MsSql doesn't support describe so I'm using what
        is used there and what the redshift cursor does to check if a table
        exists. We don't use this directly in order for this to work as a base
        class for other postgres.

        Reference: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py#L528-L553
        """
        table = exp.to_table(table_name)

        catalog_name = table.args.get("catalog") or "master"
        sql = (
            exp.select("1")
            .from_(f"{catalog_name}.information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.args.get("db")
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        self.execute(sql)

        result = self.cursor.fetchone()

        return result[0] == 1 if result is not None else False

    @property
    def connection(self) -> pymssql.Connection:
        pass

        return self.cursor.connection

    @contextlib.contextmanager
    def __try_load_pandas_to_temp_table(
        self,
        reference_table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
    ) -> t.Generator[Query, None, None]:
        reference_table = exp.to_table(reference_table_name)
        df = self.try_get_pandas_df(query_or_df)
        if df is None:
            yield t.cast("Query", query_or_df)
            return
        if columns_to_types is None:
            raise SQLMeshError("columns_to_types must be provided when using Pandas DataFrames")
        if reference_table.db is None:
            raise SQLMeshError("table must be qualified when using Pandas DataFrames")
        with self.temp_table(query_or_df, reference_table) as temp_table:
            rows: t.List[t.Iterable[t.Any]] = list(df.itertuples(False, None))

            conn = self._connection_pool.get()
            conn.bulk_copy(temp_table.name, rows)

            yield exp.select(*columns_to_types).from_(temp_table)

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        """
        SQL Server does not directly support `INSERT OVERWRITE` but it does
        support `MERGE` with a `False` condition and delete that mimics an
        `INSERT OVERWRITE`. Based on documentation this should have the same
        runtime performance as `INSERT OVERWRITE`.

        If a Pandas DataFrame is provided, it will be loaded into a temporary
        table and then merged with the target table. This temporary table is
        deleted after the merge is complete or after it's expiration time has
        passed.
        """
        with self.__try_load_pandas_to_temp_table(
            table_name,
            query_or_df,
            columns_to_types,
        ) as source_table:
            query = self._add_where_to_query(source_table, where)

            columns = [
                exp.to_column(col)
                for col in (columns_to_types or [col.alias_or_name for col in query.expressions])
            ]
            when_not_matched_by_source = exp.When(
                matched=False,
                source=True,
                condition=where,
                then=exp.Delete(),
            )
            when_not_matched_by_target = exp.When(
                matched=False,
                source=False,
                then=exp.Insert(
                    this=exp.Tuple(expressions=columns),
                    expression=exp.Tuple(expressions=columns),
                ),
            )
            self._merge(
                target_table=table_name,
                source_table=query,
                on=exp.condition("1=2"),
                match_expressions=[when_not_matched_by_source, when_not_matched_by_target],
            )

    def _get_data_objects(
        self,
        schema_name: str,
        catalog_name: t.Optional[str] = None,
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and catalog.
        """
        catalog_name = f"[{catalog_name}]" if catalog_name else "master"
        query = f"""
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                'TABLE' AS type
            FROM {catalog_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA LIKE '%{schema_name}%'
            UNION ALL
            SELECT
                '{catalog_name}' AS catalog_name,
                TABLE_NAME AS name,
                TABLE_SCHEMA AS schema_name,
                'VIEW' AS type
            FROM {catalog_name}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA LIKE '%{schema_name}%'
        """
        dataframe: pd.DataFrame = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in dataframe.itertuples()
        ]
