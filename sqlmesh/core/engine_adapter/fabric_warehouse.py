from __future__ import annotations

import typing as t
from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class FabricWarehouseAdapter(MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric Warehouses.
    """

    DIALECT = "tsql"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False

    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        self.database = kwargs.get("database")

        super().__init__(*args, **kwargs)

        if not self.database:
            raise ValueError(
                "The 'database' parameter is required in the connection config for the FabricWarehouseAdapter."
            )
        try:
            self.execute(f"USE [{self.database}]")
        except Exception as e:
            raise RuntimeError(f"Failed to set database context to '{self.database}'. Reason: {e}")

    def _get_schema_name(self, name: t.Union[TableName, SchemaName]) -> str:
        """Extracts the schema name from a sqlglot object or string."""
        table = exp.to_table(name)
        schema_part = table.db

        if isinstance(schema_part, exp.Identifier):
            return schema_part.name
        if isinstance(schema_part, str):
            return schema_part

        if schema_part is None and table.this and table.this.is_identifier:
            return table.this.name

        raise ValueError(f"Could not determine schema name from '{name}'")

    def create_schema(self, schema: SchemaName) -> None:
        """
        Creates a schema in a Microsoft Fabric Warehouse.

        Overridden to handle Fabric's specific T-SQL requirements.
        T-SQL's `CREATE SCHEMA` command does not support `IF NOT EXISTS`, so this
        implementation first checks for the schema's existence in the
        `INFORMATION_SCHEMA.SCHEMATA` view.
        """
        sql = (
            exp.select("1")
            .from_(f"{self.database}.INFORMATION_SCHEMA.SCHEMATA")
            .where(f"SCHEMA_NAME = '{schema}'")
        )
        if self.fetchone(sql):
            return
        self.execute(f"USE [{self.database}]")
        self.execute(f"CREATE SCHEMA [{schema}]")

    def _create_table_from_columns(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Creates a table, ensuring the schema exists first and that all
        object names are fully qualified with the database.
        """
        table_exp = exp.to_table(table_name)
        schema_name = self._get_schema_name(table_name)

        self.create_schema(schema_name)

        fully_qualified_table_name = f"[{self.database}].[{schema_name}].[{table_exp.name}]"

        column_defs = ", ".join(
            f"[{col}] {kind.sql(dialect=self.dialect)}" for col, kind in columns_to_types.items()
        )

        create_table_sql = f"CREATE TABLE {fully_qualified_table_name} ({column_defs})"

        if not exists:
            self.execute(create_table_sql)
            return

        if not self.table_exists(table_name):
            self.execute(create_table_sql)

        if table_description and self.comments_enabled:
            qualified_table_for_comment = self._fully_qualify(table_name)
            self._create_table_comment(qualified_table_for_comment, table_description)
            if column_descriptions and self.comments_enabled:
                self._create_column_comments(qualified_table_for_comment, column_descriptions)

    def table_exists(self, table_name: TableName) -> bool:
        """
        Checks if a table exists.

        Overridden to query the uppercase `INFORMATION_SCHEMA` required
        by case-sensitive Fabric environments.
        """
        table = exp.to_table(table_name)
        schema = self._get_schema_name(table_name)

        sql = (
            exp.select("1")
            .from_("INFORMATION_SCHEMA.TABLES")
            .where(f"TABLE_NAME = '{table.alias_or_name}'")
            .where(f"TABLE_SCHEMA = '{schema}'")
        )

        result = self.fetchone(sql, quote_identifiers=True)

        return result[0] == 1 if result else False

    def _fully_qualify(self, name: t.Union[TableName, SchemaName]) -> exp.Table:
        """Ensures an object name is prefixed with the configured database."""
        table = exp.to_table(name)
        return exp.Table(this=table.this, db=table.db, catalog=exp.to_identifier(self.database))

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
        Creates a view from a query or DataFrame.

        Overridden to ensure that the view name and all tables referenced
        in the source query are fully qualified with the database name,
        as required by Fabric.
        """
        view_schema = self._get_schema_name(view_name)
        self.create_schema(view_schema)

        qualified_view_name = self._fully_qualify(view_name)

        if isinstance(query_or_df, exp.Expression):
            for table in query_or_df.find_all(exp.Table):
                if not table.catalog:
                    qualified_table = self._fully_qualify(table)
                    table.replace(qualified_table)

        return super().create_view(
            qualified_view_name,
            query_or_df,
            columns_to_types,
            replace,
            materialized,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            **create_kwargs,
        )

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """
        Fetches column names and types for the target table.

        Overridden to query the uppercase `INFORMATION_SCHEMA.COLUMNS` view
        required by case-sensitive Fabric environments.
        """
        table = exp.to_table(table_name)
        schema = self._get_schema_name(table_name)
        sql = (
            exp.select("COLUMN_NAME", "DATA_TYPE")
            .from_(f"{self.database}.INFORMATION_SCHEMA.COLUMNS")
            .where(f"TABLE_NAME = '{table.name}'")
            .where(f"TABLE_SCHEMA = '{schema}'")
            .order_by("ORDINAL_POSITION")
        )
        df = self.fetchdf(sql)
        return {
            str(row.COLUMN_NAME): exp.DataType.build(str(row.DATA_TYPE), dialect=self.dialect)
            for row in df.itertuples()
        }

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Implements the insert overwrite strategy for Fabric.

        Overridden to enforce a `DELETE`/`INSERT` strategy, as Fabric's
        `MERGE` statement has limitations.
        """

        columns_to_types = columns_to_types or self.columns(table_name)

        self.delete_from(table_name, where=where or exp.true())

        for source_query in source_queries:
            with source_query as query:
                query = self._order_projections_and_filter(query, columns_to_types)
                self._insert_append_query(
                    table_name,
                    query,
                    columns_to_types=columns_to_types,
                    order_projections=False,
                )
