from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
)
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class BasePostgresEngineAdapter(EngineAdapter):
    DEFAULT_BATCH_SIZE = 400
    COLUMNS_TABLE = "information_schema.columns"
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""
        table = exp.to_table(table_name)
        sql = (
            exp.select("column_name", "data_type")
            .from_(self.COLUMNS_TABLE)
            .where(
                f"table_name = '{table.alias_or_name}' AND table_schema = '{table.args['db'].name}'"
            )
        )
        self.execute(sql)
        resp = self.cursor.fetchall()
        if not resp:
            raise SQLMeshError("Could not get columns for table '%s'. Table not found.", table_name)
        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect, udt=True)
            for column_name, data_type in resp
        }

    def table_exists(self, table_name: TableName) -> bool:
        """
        Postgres doesn't support describe so I'm using what the redshift cursor does to check if a table
        exists. We don't use this directly in order for this to work as a base class for other postgres

        Reference: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py#L528-L553
        """
        table = exp.to_table(table_name)

        sql = (
            exp.select("1")
            .from_("information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"table_schema = '{database_name}'")

        self.execute(sql)

        result = self.cursor.fetchone()

        return result[0] == 1 if result is not None else False

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
        Postgres has very strict rules around view replacement. For example the new query must generate an identical setFormatter
        of columns, using the same column names and data types as the old one. We have to delete the old view instead of replacing it
        to work around these constraints.

        Reference: https://www.postgresql.org/docs/current/sql-createview.html
        """
        with self.transaction():
            if replace:
                self.drop_view(view_name, materialized=materialized, cascade=True)
            super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=False,
                materialized=materialized,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **create_kwargs,
            )

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
        view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("viewname").as_("name"),
            exp.Literal.string("VIEW").as_("type"),
        ).from_("pg_views")
        materialized_view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("matviewname").as_("name"),
            exp.Literal.string("MATERIALIZED_VIEW").as_("type"),
        ).from_("pg_matviews")
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
