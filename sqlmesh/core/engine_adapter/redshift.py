from __future__ import annotations

import typing as t
import uuid

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF, SourceQuery


class RedshiftEngineAdapter(BasePostgresEngineAdapter, LogicalReplaceQueryMixin, LogicalMergeMixin):
    DIALECT = "redshift"
    DEFAULT_BATCH_SIZE = 1000
    ESCAPE_JSON = True
    COLUMNS_TABLE = "SVV_COLUMNS"  # Includes late-binding views

    @property
    def cursor(self) -> t.Any:
        # Redshift by default uses a `format` paramstyle that has issues when we try to write our snapshot
        # data to snapshot table. There doesn't seem to be a way to disable parameter overriding so we just
        # set it to `qmark` since that doesn't cause issues.
        cursor = self._connection_pool.get_cursor()
        cursor.paramstyle = "qmark"
        return cursor

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor"""
        self.execute(query, quote_identifiers=quote_identifiers)
        return self.cursor.fetch_dataframe()

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        Redshift doesn't support `CREATE TABLE IF NOT EXISTS AS...` but does support `CREATE TABLE AS...` so
        we check if the exists check exists and if not then we can use the base implementation. Otherwise we
        manually check if it exists and if it does then this is a no-op anyways so we return and if it doesn't
        then we run the query with exists set to False since we just confirmed it doesn't exist.
        """
        if not exists:
            return super()._create_table_from_source_queries(
                table_name, source_queries, columns_to_types, exists, **kwargs
            )
        if self.table_exists(table_name):
            return
        super()._create_table_from_source_queries(
            table_name, source_queries, exists=False, **kwargs
        )

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Redshift doesn't support `CREATE OR REPLACE TABLE...` and it also doesn't support `VALUES` expression so we need to specially
        handle DataFrame replacements.

        If the table doesn't exist then we just create it and load it with insert statements
        If it does exist then we need to do the:
            `CREATE TABLE...`, `INSERT INTO...`, `RENAME TABLE...`, `RENAME TABLE...`, DROP TABLE...`  dance.
        """
        if not self.is_pandas_df(query_or_df) or not self.table_exists(table_name):
            return super().replace_query(table_name, query_or_df, columns_to_types, **kwargs)
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        columns_to_types = columns_to_types or self.columns(table_name)
        target_table = exp.to_table(table_name)
        with self.transaction():
            temp_table_name = f"{target_table.alias_or_name}_temp_{self._short_hash()}"
            temp_table = target_table.copy()
            temp_table.set("this", exp.to_identifier(temp_table_name))
            old_table_name = f"{target_table.alias_or_name}_old_{self._short_hash()}"
            old_table = target_table.copy()
            old_table.set("this", exp.to_identifier(old_table_name))
            self.create_table(temp_table, columns_to_types, exists=False, **kwargs)
            self._insert_append_source_queries(temp_table, source_queries, columns_to_types)
            self.rename_table(target_table, old_table)
            self.rename_table(temp_table, target_table)
            self.drop_table(old_table)

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog_name = f"'{catalog_name}'" if catalog_name else "NULL"
        query = f"""
            SELECT
                {catalog_name} AS catalog_name,
                tablename AS name,
                schemaname AS schema_name,
                'TABLE' AS type
            FROM pg_tables
            WHERE schemaname ILIKE '{schema_name}'
            UNION ALL
            SELECT
                {catalog_name} AS catalog_name,
                viewname AS name,
                schemaname AS schema_name,
                'VIEW' AS type
            FROM pg_views
            WHERE schemaname ILIKE '{schema_name}'
            AND definition not ilike '%create materialized view%'
            UNION ALL
            SELECT
                {catalog_name} AS catalog_name,
                viewname AS name,
                schemaname AS schema_name,
                'MATERIALIZED_VIEW' AS type
            FROM
                pg_views
            WHERE schemaname ILIKE '{schema_name}'
            AND definition ilike '%create materialized view%'
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _short_hash(self) -> str:
        return uuid.uuid4().hex[:8]
