from __future__ import annotations

import typing as t
import logging
import pandas as pd
from sqlglot import exp
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    EngineRunMode,
    SourceQuery,
    CommentCreationView,
)
from sqlmesh.core.schema_diff import SchemaDiffer
from functools import cached_property

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query

    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class ClickhouseEngineAdapter(EngineAdapterWithIndexSupport, LogicalMergeMixin):
    DIALECT = "clickhouse"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_VIEW_SCHEMA = False
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY

    SCHEMA_DIFFER = SchemaDiffer()

    @cached_property
    def engine_run_mode(self) -> EngineRunMode:
        cloud_query_value = self.fetchone(
            "select value from system.settings where name='cloud_mode'"
        )
        if str(cloud_query_value[0]) == "1":
            return EngineRunMode.CLOUD
        # we use the user's specification of a cluster in the connection config to determine if
        #   the engine is in cluster mode
        if self._extra_config.get("cluster"):
            return EngineRunMode.CLUSTER
        return EngineRunMode.STANDALONE

    @property
    def cluster(self) -> t.Optional[str]:
        return self._extra_config.get("cluster")

    @property
    def auto_order_by(self) -> bool:
        return self._extra_config.get("auto_order_by")  # type: ignore

    # Workaround for clickhouse-connect cursor bug
    # - cursor does not reset row index correctly on `close()`, so `fetchone()` and `fetchmany()`
    #     return the wrong (or no) rows after the very first cursor query that returns rows
    #     in the connection
    # - cursor does reset the data rows correctly on `close()`, so `fetchall()` works because it
    #     doesn't use the row index at all
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
        if self.engine_run_mode.is_cluster:
            properties.append(exp.OnCluster(this=exp.to_identifier(self.cluster)))

        # can't call super() because it will try to set a catalog
        return self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            # sqlglot transpiles CREATE SCHEMA to CREATE DATABASE, but this text is used in an error message
            kind="DATABASE",
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="SCHEMA",
            cascade=None,
            cluster=exp.OnCluster(this=exp.to_identifier(self.cluster))
            if self.engine_run_mode.is_cluster
            else None,
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
            else query,
            use_extended_dtypes=True,
        )

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
        **kwargs: t.Any,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(target_table, **kwargs)

        def query_factory() -> Query:
            # It is possible for the factory to be called multiple times and if so then the temp table will already
            # be created so we skip creating again. This means we are assuming the first call is the same result
            # as later calls.
            if not self.table_exists(temp_table):
                self.create_table(
                    temp_table, columns_to_types, storage_format=exp.Var(this="MergeTree"), **kwargs
                )

                self.cursor.client.insert_df(temp_table.sql(dialect=self.dialect), df=df)

            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table, **kwargs),
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
                exp.case(exp.column("engine"))
                .when(
                    exp.Literal.string("View"),
                    exp.Literal.string("view"),
                )
                .else_(
                    exp.Literal.string("table"),
                )
                .as_("type"),
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

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """
        Create a table like another table or view.
        """
        target_table = exp.to_table(target_table_name)
        source_table = exp.to_table(source_table_name)
        on_cluster_sql = kwargs.get("ON_CLUSTER", None) or (
            self.cluster if self.engine_run_mode.is_cluster else None
        )
        on_cluster_sql = (
            f" ON CLUSTER {exp.to_identifier(on_cluster_sql)} " if on_cluster_sql else " "
        )
        create_sql = f"CREATE TABLE{' IF NOT EXISTS' if exists else ''} {target_table}{on_cluster_sql}AS {source_table}"
        self.execute(create_sql)

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
        **kwargs: t.Any,
    ) -> None:
        """Creates a table in the database.

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
                empty_ctas=(expression is not None),
                **kwargs,
            )
        )

        if expression and table_kind != "VIEW":
            table_name = (
                table_name_or_schema.this
                if isinstance(table_name_or_schema, exp.Schema)
                else table_name_or_schema
            )
            self._insert_append_query(
                table_name,
                expression,  # type: ignore
                columns_to_types or self.columns(table_name),
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
                storage_format.this if isinstance(storage_format, exp.Var) else storage_format  # type: ignore
            )
        properties.append(exp.EngineProperty(this=table_engine))

        # copy of table_properties so we can pop items off below then consume the rest later
        table_properties_copy = {}
        if table_properties:
            table_properties_copy = table_properties.copy()
            table_properties_copy = {k.upper(): v for k, v in table_properties_copy.items()}

        # TODO: gate this appropriately
        if table_engine != "Log":
            primary_key_raw = table_properties_copy.pop("PRIMARY_KEY", None)
            if primary_key_raw and self.SUPPORTS_INDEXES:
                primary_key_raw = (
                    primary_key_raw[0] if isinstance(primary_key_raw, list) else primary_key_raw  # type: ignore
                )
                primary_key_cols = (
                    primary_key_raw.expressions
                    if isinstance(primary_key_raw, exp.Tuple)
                    else primary_key_raw
                )

                if primary_key_cols:
                    properties.append(
                        exp.PrimaryKey(expressions=[exp.to_column(k) for k in primary_key_cols])  # type: ignore
                    )

            ordered_by_raw = table_properties_copy.pop("ORDER_BY", None) or ordered_by
            ordered_by_cols = []
            if ordered_by_raw:
                for col in ordered_by_raw:
                    if col:
                        col = col[0] if isinstance(col, list) and len(col) == 1 else col
                        assert not isinstance(col, list)
                        col = col.column if isinstance(col, TimeColumn) else col
                        col = col.this if isinstance(col, exp.Alias) else col
                        ordered_by_cols.append(col)

                # we have to dedupe by name because we can get the same column parsed in
                #   different ways (e.g., the time column both from `time_column` and `grains`)
                ordered_by_cols_names = []
                ordered_by_cols_dedupe = []
                for col in ordered_by_cols:
                    col_name = col.name if isinstance(col, exp.Column) else col
                    if col_name not in ordered_by_cols_names:
                        ordered_by_cols_names.append(col_name)
                        ordered_by_cols_dedupe.append(col)

            ordered_by_expressions = (
                exp.Tuple(expressions=[exp.to_column(k) for k in ordered_by_cols])  # type: ignore
                if ordered_by_cols
                # default () if no columns provided
                else exp.Literal(this="()", is_string=False)
            )
            properties.append(exp.Order(expressions=[exp.Ordered(this=ordered_by_expressions)]))

        if self.engine_run_mode.is_cluster:
            on_cluster = table_properties_copy.pop("CLUSTER", None) or self.cluster
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
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        view_properties_copy = view_properties.copy() if view_properties else {}

        # `view_properties`` is model.virtual_properties during view promotion but model.physical_properties elsewhen
        # - in the create_view promotion call, the cluster won't be in `view_properties`` so we pass the
        #     model.physical_properties["CLUSTER"] value to the "physical_cluster" kwarg
        if self.engine_run_mode.is_cluster:
            on_cluster = (
                view_properties_copy.pop("CLUSTER", None)
                or kwargs.pop("physical_cluster", None)
                or self.cluster
            )
            properties.append(exp.OnCluster(this=exp.Var(this=on_cluster)))

        if view_properties_copy:
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

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str, **kwargs: t.Any
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)

        on_cluster = kwargs.get("ON_CLUSTER", None)
        on_cluster_sql = " "
        if on_cluster and self.engine_run_mode.is_cluster:
            on_cluster_sql = f" ON CLUSTER {exp.to_identifier(on_cluster)} "

        truncated_comment = self._truncate_table_comment(table_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER TABLE {table_sql}{on_cluster_sql}MODIFY COMMENT {comment_sql}"

    def _build_create_comment_column_exp(
        self,
        table: exp.Table,
        column_name: str,
        column_comment: str,
        table_kind: str = "TABLE",
        **kwargs: t.Any,
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)
        column_sql = exp.to_column(column_name).sql(dialect=self.dialect, identify=True)

        on_cluster = kwargs.get("ON_CLUSTER", None)
        on_cluster_sql = " "
        if on_cluster and self.engine_run_mode.is_cluster:
            on_cluster_sql = f" ON CLUSTER {exp.to_identifier(on_cluster)} "

        truncated_comment = self._truncate_table_comment(column_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER TABLE {table_sql}{on_cluster_sql}COMMENT COLUMN {column_sql} {comment_sql}"
