from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class DuckDBEngineAdapter(EngineAdapter):
    DIALECT = "duckdb"

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self.execute(
            exp.Insert(
                this=self._insert_into_expression(table_name, columns_to_types),
                expression="SELECT * FROM df",
                overwrite=False,
            )
        )

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        query = f"""
            SELECT
              '{ catalog_name if catalog_name else ''}' as database,
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
                catalog=row.database,
                schema=row.schema,
                name=row.name,
                type=DataObjectType.from_str(row.type),
            )
            for row in df.itertuples()
        ]
