from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF


@set_catalog()
class RedshiftEngineAdapter(
    BasePostgresEngineAdapter,
    LogicalMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    NonTransactionalTruncateMixin,
):
    DIALECT = "redshift"
    ESCAPE_JSON = True
    COLUMNS_TABLE = "svv_columns"  # Includes late-binding views
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")
    # Redshift doesn't support comments for VIEWs WITH NO SCHEMA BINDING (which we always use)
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SUPPORTS_REPLACE_TABLE = False

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
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
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
                table_name,
                source_queries,
                columns_to_types,
                exists,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )
        if self.table_exists(table_name):
            return
        super()._create_table_from_source_queries(
            table_name,
            source_queries,
            exists=False,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def _build_create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        statement = super()._build_create_table_exp(
            table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            table_description=table_description,
            **kwargs,
        )

        if (
            statement.expression
            and statement.expression.args.get("limit") is not None
            and statement.expression.args["limit"].expression.this == "0"
        ):
            # redshift has a bug where CTAS statements have non determistic types. if a limit
            # is applied to a ctas statement, VARCHAR types default to 1 in some instances.
            # this checks the explain plain from redshift and tries to detect when these optimizer
            # bugs occur and force a cast
            explain_statement = statement.copy()
            for select in explain_statement.find_all(exp.Select):
                if select.args.get("from"):
                    select.set("limit", None)
                    select.set("where", None)

            explain_statement_sql = explain_statement.sql(
                dialect=self.dialect, identify=True, unsupported_level=ErrorLevel.IGNORE, copy=False
            )
            plan = parse_plan(
                "\n".join(r[0] for r in self.fetchall(f"EXPLAIN VERBOSE {explain_statement_sql}"))
            )

            if plan:
                select = exp.Select().from_(statement.expression.subquery("_subquery"))
                statement.expression.replace(select)

                for target in plan["targetlist"]:  # type: ignore
                    if target["name"] == "TARGETENTRY":
                        resdom = target["resdom"]
                        resname = resdom["resname"]
                        if resname == "<>":
                            # A synthetic column added by Redshift to compute a window function.
                            continue
                        if resname == "? column ?":
                            table_name_str = (
                                table_name_or_schema
                                if isinstance(table_name_or_schema, str)
                                else table_name_or_schema.sql(dialect=self.dialect)
                            )
                            raise SQLMeshError(f"Missing column name for table '{table_name_str}'")
                        # https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
                        restype = resdom["restype"]
                        data_type: t.Optional[str] = None
                        if restype == "1043":
                            size = (
                                int(resdom["restypmod"]) - 4
                                if resdom["restypmod"] != "- 1"
                                else "MAX"
                            )
                            # Cast NULL instead of the original projection to trick the planner into assigning a
                            # correct type to the column.
                            data_type = f"VARCHAR({size})"
                        else:
                            data_type = REDSHIFT_PLAN_TYPE_MAPPINGS.get(restype)

                        if data_type:
                            select.select(
                                exp.cast(
                                    exp.null(),
                                    data_type,
                                    dialect=self.dialect,
                                ).as_(resname),
                                copy=False,
                            )
                        else:
                            select.select(resname, copy=False)

        return statement

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
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
            table_description=table_description,
            column_descriptions=column_descriptions,
            no_schema_binding=True,
            **create_kwargs,
        )

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
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
            return super().replace_query(
                table_name,
                query_or_df,
                columns_to_types,
                table_description,
                column_descriptions,
                **kwargs,
            )
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        columns_to_types = columns_to_types or self.columns(table_name)
        target_table = exp.to_table(table_name)
        with self.transaction():
            temp_table = self._get_temp_table(target_table)
            old_table = self._get_temp_table(target_table)
            self.create_table(
                temp_table,
                columns_to_types,
                exists=False,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )
            self._insert_append_source_queries(temp_table, source_queries, columns_to_types)
            self.rename_table(target_table, old_table)
            self.rename_table(temp_table, target_table)
            self.drop_table(old_table)

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog = self.get_current_catalog()
        table_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("tablename").as_("name"),
            exp.Literal.string("TABLE").as_("type"),
        ).from_("pg_tables")
        view_query = (
            exp.select(
                exp.column("schemaname").as_("schema_name"),
                exp.column("viewname").as_("name"),
                exp.Literal.string("VIEW").as_("type"),
            )
            .from_("pg_views")
            .where(exp.column("definition").ilike("%create materialized view%").not_())
        )
        materialized_view_query = (
            exp.select(
                exp.column("schemaname").as_("schema_name"),
                exp.column("viewname").as_("name"),
                exp.Literal.string("MATERIALIZED_VIEW").as_("type"),
            )
            .from_("pg_views")
            .where(exp.column("definition").ilike("%create materialized view%"))
        )
        subquery = exp.union(
            table_query,
            exp.union(view_query, materialized_view_query, distinct=False),
            distinct=False,
        )
        query = (
            exp.select("*")
            .from_(subquery.subquery(alias="objs"))
            .where(exp.column("schema_name").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
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

    while curr():
        nested = parse_nested()

        if nested and nested.get("name") in ("RESULT", "SEQSCAN"):
            return nested
        advance()
    return None


# https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
REDSHIFT_PLAN_TYPE_MAPPINGS = {
    "16": "BOOL",
    "18": "CHAR",
    "21": "SMALLINT",
    "23": "INT",
    "20": "BIGINT",
    "1700": "NUMERIC",
    "700": "FLOAT",
    "701": "DOUBLE",
    "1114": "TIMESTAMP",
    "1184": "TIMESTAMPTZ",
    "1083": "TIME",
    "1266": "TIMETZ",
    "1082": "DATE",
}
