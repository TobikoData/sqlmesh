from __future__ import annotations

import typing as t
from duckdb import __version__ as duckdb_version
from sqlglot import exp

from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.utils import major_minor
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF


@set_catalog(override_mapping={"_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG})
class DuckDBEngineAdapter(LogicalMergeMixin, GetCurrentCatalogFromFunctionMixin):
    DIALECT = "duckdb"
    SUPPORTS_TRANSACTIONS = False
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(18, 3), (0,)],
        },
    )

    # TODO: remove once we stop supporting DuckDB 0.9
    COMMENT_CREATION_TABLE, COMMENT_CREATION_VIEW = (
        (CommentCreationTable.UNSUPPORTED, CommentCreationView.UNSUPPORTED)
        if major_minor(duckdb_version) < (0, 10)
        else (CommentCreationTable.COMMENT_COMMAND_ONLY, CommentCreationView.COMMENT_COMMAND_ONLY)
    )

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(exp.Use(this=exp.to_identifier(catalog)))

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(target_table)
        temp_table_sql = (
            exp.select(*self._casted_columns(columns_to_types))
            .from_("df")
            .sql(dialect=self.dialect)
        )
        self.cursor.sql(f"CREATE TABLE {temp_table} AS {temp_table_sql}")
        return [
            SourceQuery(
                query_factory=lambda: self._select_columns(columns_to_types).from_(temp_table),  # type: ignore
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog = self.get_current_catalog()

        if isinstance(schema_name, exp.Table):
            # Ensures we don't generate identifier quotes
            schema_name = ".".join(part.name for part in schema_name.parts)

        query = (
            exp.select(
                exp.column("table_name").as_("name"),
                exp.column("table_schema").as_("schema"),
                exp.case(exp.column("table_type"))
                .when(
                    exp.Literal.string("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .when(
                    exp.Literal.string("VIEW"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.Literal.string("LOCAL TEMPORARY"),
                    exp.Literal.string("table"),
                )
                .as_("type"),
            )
            .from_(exp.to_table("information_schema.tables"))
            .where(
                exp.column("table_catalog").eq(catalog), exp.column("table_schema").eq(schema_name)
            )
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]
