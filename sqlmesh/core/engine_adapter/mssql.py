"""Contains MsSqlEngineAdapter."""


from __future__ import annotations

import typing as t

import pandas as pd

from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType


class MsSqlEngineAdapter(EngineAdapterWithIndexSupport):
    """Implementation of EngineAdapterWithIndexSupport for MsSql compatibility.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT: str = "tsql"
    SUPPORTS_MATERIALIZED_VIEWS: bool = False

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
            DataObject(  # type: ignore
                catalog=row.catalog_name,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),
            )
            for row in dataframe.itertuples()
        ]
