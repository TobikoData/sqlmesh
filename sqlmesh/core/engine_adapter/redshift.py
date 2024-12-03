from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    VarcharSizeWorkaroundMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter.base import QueryOrDF

logger = logging.getLogger(__name__)


@set_catalog()
class RedshiftEngineAdapter(
    BasePostgresEngineAdapter,
    LogicalMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    NonTransactionalTruncateMixin,
    VarcharSizeWorkaroundMixin,
    RowDiffMixin,
):
    DIALECT = "redshift"
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")
    # Redshift doesn't support comments for VIEWs WITH NO SCHEMA BINDING (which we always use)
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SUPPORTS_REPLACE_TABLE = False
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            exp.DataType.build("VARBYTE", dialect=DIALECT).this: [(64000,)],
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(18, 0), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: [(256,)],
            exp.DataType.build("NCHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NVARCHAR", dialect=DIALECT).this: [(256,)],
        },
        max_parameter_length={
            exp.DataType.build("CHAR", dialect=DIALECT).this: 4096,
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: 65535,
        },
        drop_cascade=True,
    )
    VARIABLE_LENGTH_DATA_TYPES = {
        "char",
        "character",
        "nchar",
        "varchar",
        "character varying",
        "nvarchar",
        "varbyte",
        "varbinary",
        "binary varying",
    }

    def columns(
        self,
        table_name: TableName,
        include_pseudo_columns: bool = True,
    ) -> t.Dict[str, exp.DataType]:
        table = exp.to_table(table_name)

        sql = (
            exp.select(
                "column_name",
                "data_type",
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
            )
            .from_("svv_columns")  # Includes late-binding views
            .where(exp.column("table_name").eq(table.alias_or_name))
        )
        if table.args.get("db"):
            sql = sql.where(exp.column("table_schema").eq(table.args["db"].name))

        columns_raw = self.fetchall(sql, quote_identifiers=True)

        def build_var_length_col(
            column_name: str,
            data_type: str,
            character_maximum_length: t.Optional[int] = None,
            numeric_precision: t.Optional[int] = None,
            numeric_scale: t.Optional[int] = None,
        ) -> tuple:
            data_type = data_type.lower()
            if (
                data_type in self.VARIABLE_LENGTH_DATA_TYPES
                and character_maximum_length is not None
            ):
                return (column_name, f"{data_type}({character_maximum_length})")
            if data_type in ("decimal", "numeric"):
                return (column_name, f"{data_type}({numeric_precision}, {numeric_scale})")

            return (column_name, data_type)

        columns = [build_var_length_col(*row) for row in columns_raw]

        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect)
            for column_name, data_type in columns
        }

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

        # We manually build the `DataFrame` here because the driver's `fetch_dataframe`
        # method does not respect the active case-sensitivity configuration.
        #
        # Context: https://github.com/aws/amazon-redshift-python-driver/issues/238
        fetcheddata = self.cursor.fetchall()

        try:
            columns = [column[0] for column in self.cursor.description]
        except Exception:
            columns = None
            logging.warning(
                "No row description was found, pandas dataframe will be missing column labels."
            )

        result = [tuple(row) for row in fetcheddata]
        return pd.DataFrame(result, columns=columns)

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
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

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
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
            materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            no_schema_binding=create_kwargs.pop("no_schema_binding", True),
            view_properties=view_properties,
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
        if not isinstance(query_or_df, pd.DataFrame) or not self.table_exists(table_name):
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
                catalog=catalog,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _normalize_decimal_value(self, expr: exp.Expression, precision: int) -> exp.Expression:
        # Redshift is finicky. It truncates when the data is already in a table, but rounds when the data is generated as part of a SELECT.
        #
        # The following works:
        #  > select cast(cast(3.14159 as decimal(6, 5)) as decimal(6, 3)); --produces '3.142', the value we want / what every other database produces
        #
        # However, if you write that to a table, and then cast it to a less precise decimal, you get _truncation_.
        #  > create table foo (val decimal(6, 5)); insert into foo(val) values (3.14159);
        #  > select cast(val as decimal(6, 3)) from foo; --produces '3.141'
        #
        # So to make up for this, we force it to round by injecting a round() expression
        rounded = exp.func("ROUND", expr, precision)

        return super()._normalize_decimal_value(rounded, precision)
