from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter
from sqlglot import exp

import typing as t

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF

class SingleStoreEngineAdapter(MySQLEngineAdapter):
    DIALECT = "mysql"

    def create_view(
        self,
        view_name: t.Any,  # TableName
        query_or_df: t.Any,  # QueryOrDF
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        with self.transaction():
            if replace:
                self.drop_view(view_name, materialized=materialized)
            super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=False,
                materialized=materialized,
                materialized_properties=materialized_properties,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                **create_kwargs,
            )
