from __future__ import annotations

import logging
import typing as t

from sqlglot import exp, parse_one

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName

logger = logging.getLogger(__name__)


@set_catalog()
class MySQLEngineAdapter(
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    NonTransactionalTruncateMixin,
):
    DEFAULT_BATCH_SIZE = 200
    DIALECT = "mysql"
    ESCAPE_JSON = True
    SUPPORTS_INDEXES = True
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 1024
    SUPPORTS_REPLACE_TABLE = False

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

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        query = (
            exp.select(
                exp.column("table_name").as_("name"),
                exp.column("table_schema").as_("schema_name"),
                exp.case()
                .when(
                    exp.column("table_type").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .when(
                    exp.column("table_type").eq("VIEW"),
                    exp.Literal.string("view"),
                )
                .else_("table_type")
                .as_("type"),
            )
            .from_(exp.table_("tables", db="information_schema"))
            .where(exp.column("table_schema").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)

        return f"ALTER TABLE {table_sql} COMMENT = '{self._truncate_table_comment(table_comment)}'"

    def _create_column_comments(
        self,
        table_name: TableName,
        column_comments: t.Dict[str, str],
        table_kind: str = "TABLE",
    ) -> None:
        table = exp.to_table(table_name)
        table_sql = table.sql(dialect=self.dialect, identify=True)

        try:
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
                col_def.args["constraints"].extend(  # type: ignore
                    self._build_col_comment_exp(col_def.alias_or_name, column_comments)  # type: ignore
                )
                self.execute(
                    f"ALTER TABLE {table_sql} MODIFY {col_def.sql(dialect=self.dialect, identify=True)}",  # type: ignore
                )
        except Exception:
            logger.warning(
                f"Column comments for table '{table.alias_or_name}' not registered - this may be due to limited permissions.",
                exc_info=True,
            )
