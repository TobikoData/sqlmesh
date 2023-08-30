from __future__ import annotations

import typing as t
import uuid

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import Query
    from sqlmesh.core.engine_adapter.base import QueryOrDF, SourceQuery


class RedshiftEngineAdapter(BasePostgresEngineAdapter):
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

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        """
        Redshift doesn't support `VALUES` expressions outside of a `INSERT` statement. Currently sqlglot cannot
        performantly convert a values expression into a series of `UNION ALL` statements. Therefore we just don't
        support views for Redshift until sqlglot is updated to performantly support large union statements.

        Also Redshift views are "binding" by default to their underlying table which means you can't drop that
        underlying table without dropping the view first. This is a problem for us since we want to be able to
        swap tables out from under views. Therefore we create the view as non-binding.
        """
        if self.is_pandas_df(query_or_df):
            raise NotImplementedError(
                "DataFrames are not supported for Redshift views because Redshift doesn't"
                "support using `VALUES` in a `CREATE VIEW` statement."
            )
        return super().create_view(
            view_name,
            query_or_df,
            replace=replace,
            materialized=materialized,
            no_schema_binding=True,
            **create_kwargs,
        )

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
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
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

    @classmethod
    def _values_to_sql(
        cls,
        values: t.List[t.Tuple[t.Any, ...]],
        columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
        contains_json: bool = False,
    ) -> Query:
        """
        Extracts the `VALUES` expression from the SELECT statement and also removes the alias.
        """
        query = super()._values_to_sql(
            values, columns_to_types, batch_start, batch_end, alias, contains_json
        )
        values_expression = t.cast(exp.Select, query.find(exp.Values))
        values_expression.parent = None
        values_expression.set("alias", None)
        return values_expression

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
        source_queries, columns_to_types = self._get_source_query_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        if not isinstance(query_or_df, pd.DataFrame):
            with self.transaction():
                self.drop_table(table_name, exists=True)
                return self._create_table_from_source_queries(
                    table_name, source_queries, exists=True
                )
        columns_to_types = columns_to_types or self.columns(table_name)
        target_table = exp.to_table(table_name)
        target_exists = self.table_exists(target_table)
        if target_exists:
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
        else:
            self.create_table(target_table, columns_to_types, exists=False, **kwargs)
            self._insert_append_source_queries(target_table, source_queries, columns_to_types)

    def _short_hash(self) -> str:
        return uuid.uuid4().hex[:8]
