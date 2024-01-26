from __future__ import annotations

import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp

from sqlmesh.core.dialect import schema_, to_schema
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    HiveMetastoreTablePropertiesMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
    SourceQuery,
    set_catalog,
)

if t.TYPE_CHECKING:
    from trino.dbapi import Connection as TrinoConnection

    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF


@set_catalog()
class TrinoEngineAdapter(
    PandasNativeFetchDFSupportMixin,
    HiveMetastoreTablePropertiesMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "trino"
    ESCAPE_JSON = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INTO_IS_OVERWRITE
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    # Trino does technically support transactions but it doesn't work correctly with partition overwrite so we
    # disable transactions. If we need to get them enabled again then we would need to disable auto commit on the
    # connector and then figure out how to get insert/overwrite to work correctly without it.
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_ROW_LEVEL_OP = False
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_REPLACE_TABLE = False

    @property
    def connection(self) -> TrinoConnection:
        return self.cursor.connection

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(exp.Use(this=schema_(db="information_schema", catalog=catalog)))

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
    ) -> None:
        if where:
            self.execute(
                f"SET SESSION {self.get_current_catalog()}.insert_existing_partitions_behavior='OVERWRITE'"
            )
            super()._insert_overwrite_by_condition(
                table_name, source_queries, columns_to_types, where
            )
            self.execute(
                f"SET SESSION {self.get_current_catalog()}.insert_existing_partitions_behavior='APPEND'"
            )
        else:
            super()._insert_overwrite_by_condition(
                table_name,
                source_queries,
                columns_to_types,
                where,
                insert_overwrite_strategy_override=InsertOverwriteStrategy.DELETE_INSERT,
            )

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        # Some trino connectors don't support truncate so we use delete.
        self.execute(f"DELETE FROM {table.sql(dialect=self.dialect, identify=True)}")

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        schema_name = to_schema(schema_name)
        schema = schema_name.db
        catalog = schema_name.catalog or self.get_current_catalog()
        query = (
            exp.select(
                exp.column("table_catalog", table="t").as_("catalog"),
                exp.column("table_schema", table="t").as_("schema"),
                exp.column("table_name", table="t").as_("name"),
                exp.case()
                .when(
                    exp.column("name", table="mv").is_(exp.null()).not_(),
                    exp.Literal.string("materialized_view"),
                )
                .when(
                    exp.column("table_type", table="t").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .else_(exp.column("table_type", table="t"))
                .as_("type"),
            )
            .from_(exp.to_table(f"{catalog}.information_schema.tables", alias="t"))
            .join(
                exp.to_table("system.metadata.materialized_views", alias="mv"),
                on=exp.and_(
                    exp.column("catalog_name", table="mv").eq(
                        exp.column("table_catalog", table="t")
                    ),
                    exp.column("schema_name", table="mv").eq(exp.column("table_schema", table="t")),
                    exp.column("name", table="mv").eq(exp.column("table_name", table="t")),
                ),
                join_type="left",
            )
            .where(
                exp.and_(
                    exp.column("table_schema", table="t").eq(schema),
                    exp.or_(
                        exp.column("catalog_name", table="mv").is_(exp.null()),
                        exp.column("catalog_name", table="mv").eq(catalog),
                    ),
                    exp.or_(
                        exp.column("schema_name", table="mv").is_(exp.null()),
                        exp.column("schema_name", table="mv").eq(schema),
                    ),
                )
            )
        )
        if object_names:
            query = query.where(exp.column("table_name", table="t").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        # Trino does not accept timestamps in ISOFORMAT that include the "T". `execution_time` is stored in
        # Pandas with that format, so we convert the column to a string with the proper format and CAST to
        # timestamp in Trino.
        for column, kind in (columns_to_types or {}).items():
            if is_datetime64_any_dtype(df.dtypes[column]) and getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                df[column] = pd.to_datetime(df[column]).map(lambda x: x.isoformat(" "))  # type: ignore

        return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)
