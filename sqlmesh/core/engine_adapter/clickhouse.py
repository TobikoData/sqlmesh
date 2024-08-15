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
    CommentCreationTable,
    CommentCreationView,
)
from sqlmesh.core.schema_diff import SchemaDiffer
from functools import cached_property

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query

    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class ClickhouseEngineAdapter(EngineAdapter):
    DIALECT = "clickhouse"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_INDEXES = True
    AUTOMATIC_ORDERED_BY = True
    SUPPORTS_VIEW_SCHEMA = False

    # # TODO: delete this after sqlglot implements `CREATE TABLE ... AS SELECT ... COMMENT 'comment'`
    # COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
    # COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED

    SCHEMA_DIFFER = SchemaDiffer()

    @cached_property
    def is_cloud(self) -> bool:
        value = self.fetchone("select value from system.settings where name='cloud_mode'")
        return str(value[0]) == "1"

    @cached_property
    def is_cluster(self) -> bool:
        if self.is_cloud:
            return False

        # you can set cluster config and start some instances, but unless you set up zookeeper as well,
        # trying to create clustered tables will fail with:
        # Code: 139. DB::Exception: There is no Zookeeper configuration in server config.
        value = self.fetchone("show tables in system like 'zookeeper'")
        return str(value[0]) == "zookeeper"

    @cached_property
    def is_standalone(self) -> bool:
        return not self.is_cloud and not self.is_cluster

    @property
    def default_cluster(self) -> str:
        return self._extra_config.get("default_cluster")

    # WORKAROUND for clickhouse-connect bug where `fetchone()` sometimes returns None
    # - consumes all cursor rows, so will cause problems if `fetchone()` is used in
    #     a loop to consume rows one-by-one
    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Tuple:
        with self.transaction():
            self.execute(
                query,
                ignore_unsupported_errors=ignore_unsupported_errors,
                quote_identifiers=quote_identifiers,
            )
            return self.cursor.fetchall()[0]

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.List[exp.Expression] = [],
    ) -> None:
        """Create a Clickhouse database from a name or qualified table name.

        Clickhouse has a two-level naming scheme [database].[table].
        """
        if self.is_cluster:
            properties.append(exp.OnCluster(this=exp.Var(this=self.default_cluster)))

        return self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind="DATABASE",
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expression],
    ) -> None:
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="DATABASE",
            cascade=cascade,
            cluster=self.default_cluster if self.is_cluster else None,
            **drop_args,
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
            self.create_table(
                temp_table, columns_to_types, storage_format=exp.Var(this="MergeTree")
            )

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

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        primary_key: t.Optional[t.Tuple[str, ...] | t.List[exp.Expression]] = None,
        **kwargs: t.Any,
    ) -> None:
        """ Creates a table in the database.

        Clickhouse does not fully support CTAS in "replicated" engines, which are used exclusively
        in Clickhouse Cloud.

        Therefore, we add the `EMPTY` property to the CTAS call to create a table with the proper
        schema, then insert the data with the CTAS query.
        """
        self.execute(
            self._build_create_table_exp(
                table_name_or_schema,
                expression=expression,
                exists=exists,
                replace=replace,
                columns_to_types=columns_to_types,
                table_description=(
                    table_description
                    if self.COMMENT_CREATION_TABLE.supports_schema_def and self.comments_enabled
                    else None
                ),
                table_kind=table_kind,
                primary_key=primary_key,
                empty_ctas=(expression is not None),
                **kwargs,
            )
        )

        if expression:
            self._insert_append_query(
                table_name_or_schema.this if isinstance(table_name_or_schema, exp.Schema) else table_name_or_schema,
                expression,
                columns_to_types or self.columns(table_name_or_schema),
            )

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
        empty_ctas: bool = False,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

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
            primary_key_raw = table_properties_copy.pop("PRIMARY_KEY", None) or primary_key
            if primary_key_raw and self.SUPPORTS_INDEXES:
                primary_key_cols = (
                    primary_key_raw[0].expressions
                    if len(primary_key_raw) == 1 and isinstance(primary_key_raw[0], exp.Tuple)
                    else primary_key_raw
                )

                properties.append(
                    exp.PrimaryKey(expressions=[exp.to_column(k) for k in primary_key_cols])
                )

            ordered_by_raw = table_properties_copy.pop("ORDER_BY", None) or ordered_by
            ordered_by_cols = []
            if ordered_by_raw:
                for col in ordered_by_raw:
                    if col:
                        col = col[0] if isinstance(col, list) and len(col) == 1 else col
                        col = col.column if isinstance(col, TimeColumn) else col
                        col = col.this if isinstance(col, exp.Alias) else col
                        ordered_by_cols.append(col)

                # we have to dedupe by name because we can get the same column parsed in
                #   different ways (e.g., the time column both from `time_column` and `grains`)
                ordered_by_cols_names = []
                ordered_by_cols_dedupe = []
                for col in ordered_by_cols:
                    if col.name not in ordered_by_cols_names:
                        ordered_by_cols_names.append(col.name)
                        ordered_by_cols_dedupe.append(col)

            ordered_by_expressions = (
                exp.Tuple(expressions=[exp.to_column(k) for k in ordered_by_cols])
                if ordered_by_cols
                else exp.Literal(this="tuple()", is_string=False)
            )
            properties.append(exp.Order(expressions=[exp.Ordered(this=ordered_by_expressions)]))

        if self.is_cluster:
            on_cluster = table_properties_copy.pop("CLUSTER", None) or self.default_cluster
            if on_cluster:
                properties.append(
                    exp.OnCluster(
                        this=exp.Literal(
                            this=on_cluster.this
                            if isinstance(on_cluster, exp.Expression)
                            else on_cluster,
                            is_string=False,
                        )
                    )
                )

        if empty_ctas:
            properties.append(exp.EmptyProperty())

        if table_properties_copy:
            properties.extend(self._table_or_view_properties_to_expressions(table_properties_copy))

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if properties:
            return exp.Properties(expressions=properties)

        return None

    def _build_view_properties_exp(
        self,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        view_properties_copy = view_properties.copy()

        # "view_properties" is model.virtual_properties during view promotion but model.physical_properties elsewhen
        # - in the promotion call to create_view, the cluster won't be in view_properties so we pass the
        #     model.physical_properties["CLUSTER"] value to the "physical_cluster" kwarg
        on_cluster = view_properties_copy.pop("CLUSTER", None) or kwargs.pop("physical_cluster", None) or self.default_cluster
        if self.is_cluster:
            properties.append(
                exp.OnCluster(this=exp.Literal(this=on_cluster, is_string=False))
            )

        properties.extend(self._table_or_view_properties_to_expressions(view_properties_copy))

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if properties:
            return exp.Properties(expressions=properties)
        return None
