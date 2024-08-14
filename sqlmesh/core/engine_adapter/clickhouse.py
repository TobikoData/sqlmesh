from __future__ import annotations

import typing as t
import logging
import pandas as pd
from sqlglot import exp
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    SourceQuery,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query

    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class ClickhouseEngineAdapter(EngineAdapter):
    DIALECT = "clickhouse"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_INDEXES = True
    GRAINS_AS_PRIMARY_KEY = True
    TIME_COL_AS_ORDERED_BY = True

    SCHEMA_DIFFER = SchemaDiffer()

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
    ) -> None:
        """Create a Clickhouse database from a name or qualified table name.

        Clickhouse has a two-level naming scheme [database].[table].
        """
        try:
            self.execute(
                exp.Create(
                    this=to_schema(schema_name),
                    kind="DATABASE",
                    exists=ignore_if_exists,
                )
            )
        except Exception as e:
            if not warn_on_error:
                raise
            logger.warning("Failed to create database '%s': %s", schema_name, e)

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        self.execute(
            exp.Drop(
                this=to_schema(schema_name),
                kind="DATABASE",
                exists=ignore_if_not_exists,
                cascade=False,
            )
        )

    # TODO: `RENAME` is valid SQL, but `EXCHANGE` is an atomic swap
    # def _rename_table(
    #     self,
    #     old_table_name: TableName,
    #     new_table_name: TableName,
    # ) -> None:
    #     self.execute(f"EXCHANGE TABLES {old_table_name} AND {new_table_name}")

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor"""
        return self.cursor.client.query_df(
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(target_table)

        def query_factory() -> Query:
            # TODO: should we pick a temp table engine ourselves or use the one passed to the model?
            # - If the latter, figure out how to pass it
            self.create_table(temp_table, columns_to_types, storage_format=exp.Var(this="Log"))

            self.cursor.client.insert_df(temp_table.sql(dialect=self.dialect), df=df)

            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory, cleanup_func=lambda: self.drop_table(temp_table)
            )
        ]

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given database.
        """
        query = (
            exp.select(
                exp.column("database").as_("schema_name"),
                exp.column("name"),
                exp.column("engine").as_("type"),
            )
            .from_("system.tables")
            .where(exp.column("database").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=None,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partitioned_by_user_cols: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        primary_key: t.Optional[t.Tuple[str, ...] | t.List[exp.Expression]] = None,
        ordered_by: t.Optional[t.List[str]] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        # `partitioned_by` automatically includes model `time_column`, but we only want the
        #   columns specified by the user so use `partitioned_by_user_cols` instead
        if partitioned_by_user_cols:
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Schema(expressions=partitioned_by_user_cols),
                )
            )

        # table engine, default `MergeTree`
        table_engine = "MergeTree"
        if storage_format:
            table_engine = (
                storage_format.this if isinstance(storage_format, exp.Var) else storage_format
            )
        properties.append(exp.EngineProperty(this=table_engine))

        # we make a copy of table_properties so we can pop items off below then consume the rest just before returning
        table_properties_copy = {}
        if table_properties:
            table_properties = {k.upper(): v for k, v in table_properties.items()}
            table_properties_copy = table_properties.copy()

        # TODO: gate this appropriately
        if table_engine != "Log":
            # model grains are passed as primary key cols
            # - it is not recommended to include the time_column in the primary key, so we remove it below if present
            # - because we may remove time_column below, we just extract columns and delay appending it to properties
            primary_key_cols = []
            if primary_key and self.SUPPORTS_INDEXES:
                primary_key_cols = (
                    primary_key[0].expressions
                    if len(primary_key) == 1 and isinstance(primary_key[0], exp.Tuple)
                    else primary_key
                )

            # ORDER BY can consist of columns from multiple sources, so we assemble it in parts:
            # 1. The user may pass it via table_properties (we store in ordered_by_physical_properties)
            # 2. If the model has a time_column, it will be in `ordered_by` (we store in ordered_by_time_col)
            # 3. If the model has a grain, it will be in `primary_key` but must also be in ORDER BY (we store in primary_key_cols)
            # 4. It is required for our default MergeTree engine, so if we don't have 1-3 we pass `tuple()`
            ordered_by_physical_properties = table_properties_copy.pop("ORDER_BY", None)
            # 1
            ordered_by_user_cols = []
            if ordered_by_physical_properties:
                # Extract nested list
                ordered_by_physical_properties = (
                    ordered_by_physical_properties[0]
                    if isinstance(ordered_by_physical_properties, list)
                    else ordered_by_physical_properties
                )
                # Unpack Tuple columns
                ordered_by_physical_properties = (
                    ordered_by_physical_properties.expressions
                    if isinstance(ordered_by_physical_properties, exp.Tuple)
                    else ordered_by_physical_properties
                )
                # Ensure list
                ordered_by_user_cols = (
                    ordered_by_physical_properties
                    if isinstance(ordered_by_physical_properties, list)
                    else [ordered_by_physical_properties]
                )

            # 2
            ordered_by_time_col = None
            if ordered_by:
                ordered_by_time_col = (
                    ordered_by.column if isinstance(ordered_by, TimeColumn) else ordered_by
                )
                primary_key_cols = [
                    col
                    for col in primary_key_cols
                    if col.alias_or_name != ordered_by_time_col.alias_or_name
                ]

            # we removed time_column from primary_key_cols so can now add the primary key property
            if primary_key_cols:
                properties.append(
                    exp.PrimaryKey(expressions=[exp.to_column(k) for k in primary_key_cols])
                )

            # 3. primary key must be a leftward subset of ordered by, so we rearrange/add them to the front
            # - time_column will always go last
            ordered_by_cols = primary_key_cols
            ordered_by_cols_names = [col.alias_or_name for col in ordered_by_cols]
            for col in [*ordered_by_user_cols, ordered_by_time_col]:
                if col and col.alias_or_name not in ordered_by_cols_names:
                    ordered_by_cols.append(col)
                    ordered_by_cols_names.append(col.alias_or_name)

            ordered_by_expressions = (
                exp.Tuple(expressions=[exp.to_column(k) for k in ordered_by_cols])
                if ordered_by_cols
                else exp.Literal(this="tuple()", is_string=False)
            )
            properties.append(exp.Order(expressions=[exp.Ordered(this=ordered_by_expressions)]))

        on_cluster = table_properties_copy.pop("CLUSTER", None)
        if on_cluster:
            properties.append(
                exp.OnCluster(this=exp.Literal(this=on_cluster.this, is_string=False))
            )

        if table_properties_copy:
            properties.extend(self._table_or_view_properties_to_expressions(table_properties_copy))

        if properties:
            return exp.Properties(expressions=properties)

        return None
