from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType
from sqlmesh.utils import nullsafe_join

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF


class SnowflakeEngineAdapter(EngineAdapter):
    DEFAULT_SQL_GEN_KWARGS = {"identify": False}
    DIALECT = "snowflake"
    ESCAPE_JSON = True
    SUPPORTS_MATERIALIZED_VIEWS = True

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        from snowflake.connector.errors import NotSupportedError

        self.execute(query)

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
        df = self.fetchdf(sql)
        return [
            DataObject(
                catalog=row.database_name,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.kind),  # type: ignore
            )
            for row in df[["database_name", "schema_name", "name", "kind"]].itertuples()
        ]

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        from snowflake.connector.pandas_tools import write_pandas

        table = exp.to_table(table_name)

        table_identifier = table.name
        if not table.this.quoted:
            table_identifier = table_identifier.upper()

        if "db" in table.args:
            table_db = table.db
            if not table.args["db"].quoted:
                table_db = table_db.upper()
        else:
            table_db = None

        new_column_names = {
            col: col if exp.to_identifier(col).quoted else col.upper() for col in df.columns  # type: ignore
        }
        df = df.rename(columns=new_column_names)

        # Workaround for https://github.com/snowflakedb/snowflake-connector-python/issues/1034
        self.cursor.execute(f'USE SCHEMA "{table_db}"')

        write_pandas(
            self._connection_pool.get(),
            df,
            table_identifier,
            schema=table_db,
            chunk_size=self.DEFAULT_BATCH_SIZE,
        )
