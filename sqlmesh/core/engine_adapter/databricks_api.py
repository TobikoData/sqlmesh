from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF


class DatabricksSQLEngineAdapter(BaseSparkEngineAdapter):
    DIALECT = "databricks"

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        """
        Returns a Pandas DataFrame from a query or expression.
        """
        self.execute(query)
        return self.cursor.fetchall_arrow().to_pandas()

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        tables = [x.asDict() for x in self.cursor.tables(catalog_name, schema_name)]
        if any(not row["TABLE_TYPE"] for row in tables):
            if catalog_name:
                self.execute(f"USE CATALOG {catalog_name}")
            view_names = [
                row.viewName for row in self.fetchdf(f"SHOW VIEWS FROM {schema_name}").itertuples()  # type: ignore
            ]
            for table in tables:
                if not table["TABLE_TYPE"]:
                    table["TABLE_TYPE"] = "VIEW" if table["TABLE_NAME"] in view_names else "TABLE"
        return [
            DataObject(
                catalog=row["TABLE_CAT"],
                schema=row["TABLE_SCHEM"],
                name=row["TABLE_NAME"],
                type=DataObjectType.from_str(row["TABLE_TYPE"]),
            )
            for row in tables
        ]
