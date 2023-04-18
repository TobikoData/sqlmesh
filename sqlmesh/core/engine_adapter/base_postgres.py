from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


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
