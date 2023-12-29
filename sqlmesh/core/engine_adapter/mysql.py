from __future__ import annotations

import typing as t

from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType, set_catalog

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName


@set_catalog()
class MySQLEngineAdapter(
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
    NonTransactionalTruncateMixin,
):
    DEFAULT_BATCH_SIZE = 200
    DIALECT = "mysql"
    ESCAPE_JSON = True
    SUPPORTS_INDEXES = True
    SUPPORTS_VIEW_COMMENT = False
    SUPPORTS_CTAS_SCHEMA_COMMENTS = False

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        return None

    def create_index(
        self,
        table_name: TableName,
        index_name: str,
        columns: t.Tuple[str, ...],
        exists: bool = True,
    ) -> None:
        # MySQL doesn't support IF EXISTS clause for indexes.
        super().create_index(table_name, index_name, columns, exists=False)

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        # MySQL doesn't support CASCADE clause and drops schemas unconditionally.
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)

    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        query = f"""
            SELECT
                null AS catalog_name,
                table_name AS name,
                table_schema AS schema_name,
                CASE
                    WHEN table_type = 'BASE TABLE' THEN 'table'
                    WHEN table_type = 'VIEW' THEN 'view'
                    ELSE table_type
                END AS type
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _create_comments(
        self,
        table_name: TableName,
        table_comment: t.Optional[str] = None,
        column_comments: t.Optional[t.Dict[str, str]] = None,
        table_kind: str = "TABLE",
    ) -> None:
        """
        Executes commands to create table and column comments.
        """
        table_sql = exp.to_table(table_name).sql(dialect=self.dialect, identify=True)

        if table_comment:
            self.execute(
                f"ALTER TABLE {table_sql} COMMENT = '{table_comment}'",
            )

        if column_comments:
            # MySQL ALTER TABLE MODIFY completely replaces the column (overwriting options and constraints).
            # self.columns() only returns the column types so doesn't allow us to fully/correctly replace a column definition.
            # To get the full column definition we retrieve and parse the table's CREATE TABLE statement.
            create_table_exp = parse_one(
                self.fetchone(f"SHOW CREATE TABLE {table_sql}")[1], dialect=self.dialect
            )
            col_def_exps = {
                col_def.name: col_def.copy()
                for col_def in create_table_exp.find(exp.Schema).find_all(exp.ColumnDef)  # type: ignore
            }

            for col in column_comments:
                col_def = col_def_exps.get(col)
                col_def.constraints.extend(  # type: ignore
                    self._build_col_comment_exp(col_def.alias_or_name, column_comments)  # type: ignore
                )
                self.execute(
                    f"ALTER TABLE {table_sql} MODIFY {col_def.sql(dialect=self.dialect, identify=True)}",  # type: ignore
                )
