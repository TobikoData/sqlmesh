from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    NonTransactionalTruncateMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType, set_catalog

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF, SourceQuery


class RedshiftEngineAdapter(
    BasePostgresEngineAdapter,
    LogicalReplaceQueryMixin,
    LogicalMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    NonTransactionalTruncateMixin,
):
    DIALECT = "redshift"
    ESCAPE_JSON = True
    COLUMNS_TABLE = "svv_columns"  # Includes late-binding views
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")

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

    def _create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        statement = super()._create_table_exp(
            table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            **kwargs,
        )

        if statement.expression:
            # redshift has a bug where CTAS statements have non determistic types. if a limit
            # is applied to a ctas statement, VARCHAR types default to 1 in some instances.
            # this checks the explain plain from redshift and tries to detect when these optimizer
            # bugs occur and force a cast
            sql = statement.sql(
                dialect=self.dialect, identify=True, unsupported_level=ErrorLevel.IGNORE
            )
            plan = parse_plan("\n".join(r[0] for r in self.fetchall(f"EXPLAIN VERBOSE {sql}")))

            if plan:
                select = exp.Select().from_(statement.expression.subquery("_subquery"))
                statement.expression.replace(select)

                for target in plan["targetlist"]:  # type: ignore
                    if target["name"] == "TARGETENTRY":
                        resdom = target["resdom"]
                        # https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
                        if resdom["restype"] == "1043" and resdom["restypmod"] == "- 1":
                            select.select(
                                exp.cast(
                                    exp.to_identifier(resdom["resname"]),
                                    "VARCHAR(MAX)",
                                    dialect=self.dialect,
                                ),
                                copy=False,
                            )
                        else:
                            select.select(resdom["resname"], copy=False)

        return statement

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
        Redshift views are "binding" by default to their underlying table which means you can't drop that
        underlying table without dropping the view first. This is a problem for us since we want to be able to
        swap tables out from under views. Therefore, we create the view as non-binding.
        """
        return super().create_view(
            view_name,
            query_or_df,
            columns_to_types,
            replace,
            materialized,
            no_schema_binding=True,
            **create_kwargs,
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
            temp_table = self._get_temp_table(target_table)
            old_table = self._get_temp_table(target_table)
            self.create_table(temp_table, columns_to_types, exists=False, **kwargs)
            self._insert_append_source_queries(temp_table, source_queries, columns_to_types)
            self.rename_table(target_table, old_table)
            self.rename_table(temp_table, target_table)
            self.drop_table(old_table)

    def set_current_catalog(self, catalog_name: str) -> None:
        self.cursor.connection._database = catalog_name

    @set_catalog()
    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog_name = self.get_current_catalog()
        query = f"""
            SELECT
                '{catalog_name}' AS catalog_name,
                tablename AS name,
                schemaname AS schema_name,
                'TABLE' AS type
            FROM pg_tables
            WHERE schemaname ILIKE '{schema_name}'
            UNION ALL
            SELECT
                '{catalog_name}' AS catalog_name,
                viewname AS name,
                schemaname AS schema_name,
                'VIEW' AS type
            FROM pg_views
            WHERE schemaname ILIKE '{schema_name}'
            AND definition not ilike '%create materialized view%'
            UNION ALL
            SELECT
                '{catalog_name}' AS catalog_name,
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


def parse_plan(plan: str) -> t.Optional[t.Dict]:
    """Parse the output of a redshift explain verbose query plan into a Python dict."""
    from sqlglot import Tokenizer, TokenType
    from sqlglot.tokens import Token

    tokens = Tokenizer().tokenize(plan)
    i = 0
    terminal_tokens = {TokenType.L_PAREN, TokenType.R_PAREN, TokenType.R_BRACE, TokenType.COLON}

    def curr() -> t.Optional[TokenType]:
        return tokens[i].token_type if i < len(tokens) else None

    def advance() -> Token:
        nonlocal i
        i += 1
        return tokens[i - 1]

    def match(token_type: TokenType, raise_unmatched: bool = False) -> t.Optional[Token]:
        if curr() == token_type:
            return advance()
        if raise_unmatched:
            raise Exception(f"Expected {token_type}")
        return None

    def parse_value() -> t.Any:
        if match(TokenType.L_PAREN):
            values = []
            while not match(TokenType.R_PAREN):
                values.append(parse_value())
            return values

        nested = parse_nested()

        if nested:
            return nested

        value = []

        while not curr() in terminal_tokens:
            value.append(advance().text)

        return " ".join(value)

    def parse_nested() -> t.Optional[t.Dict]:
        if not match(TokenType.L_BRACE):
            return None
        query_plan = {}
        query_plan["name"] = advance().text

        while match(TokenType.COLON):
            key = advance().text

            while match(TokenType.DOT):
                key += f".{advance().text}"

            query_plan[key] = parse_value()

        match(TokenType.R_BRACE, True)
        return query_plan

    return parse_nested()
