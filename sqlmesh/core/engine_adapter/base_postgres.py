from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class BasePostgresEngineAdapter(EngineAdapter):
    def columns(self, table_name: TableName) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""
        table = exp.to_table(table_name)
        sql = (
            exp.select("column_name", "data_type")
            .from_("information_schema.columns")
            .where(f"table_name = '{table.alias_or_name}' AND table_schema = '{table.args['db']}'")
        )
        self.execute(sql)
        resp = self.cursor.fetchall()
        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect)
            for column_name, data_type in resp
        }

    def table_exists(self, table_name: TableName) -> bool:
        """
        Postgres doesn't support describe so I'm using what the redshift cursor does to check if a table
        exists. We don't use this directly in order for this to work as a base class for other postgres

        Reference: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py#L528-L553
        """
        table = exp.to_table(table_name)

        # Postgres doesn't support catalog
        if table.args.get("catalog"):
            return False

        sql = (
            exp.select("1")
            .from_("information_schema.tables")
            .where(f"table_name = '{table.alias_or_name}'")
        )
        database_name = table.args.get("db")
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
        **create_kwargs: t.Any,
    ) -> None:
        """
        Postgres has very strict rules around view replacement. For example the new query must generate an identical setFormatter
        of columns, using the same column names and data types as the old one. We have to delete the old view instead of replacing it
        to work around these constraints.

        Reference: https://www.postgresql.org/docs/current/sql-createview.html
        """
        with self.transaction(TransactionType.DDL):
            if replace:
                self.drop_view(view_name)
            super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=replace,
                **create_kwargs,
            )

    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        To make sure that inserts and updates take effect we need to commit explicitly unless the
        statement is executed as part of an active transaction.

        Reference: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
        """
        super().execute(expressions, ignore_unsupported_errors=ignore_unsupported_errors, **kwargs)
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()

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
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]
