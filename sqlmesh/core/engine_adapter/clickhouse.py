from __future__ import annotations

import typing as t
import logging
import pandas as pd
import re
from sqlglot import exp, maybe_parse
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

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query

    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class ClickhouseEngineAdapter(EngineAdapterWithIndexSupport, LogicalMergeMixin):
    DIALECT = "clickhouse"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_VIEW_SCHEMA = False
    SUPPORTS_REPLACE_TABLE = False
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY

    SCHEMA_DIFFER = SchemaDiffer()

    DEFAULT_TABLE_ENGINE = "MergeTree"
    ORDER_BY_TABLE_ENGINE_REGEX = "^.*?MergeTree.*$"

    @property
    def engine_run_mode(self) -> EngineRunMode:
        if self._extra_config.get("cloud_mode"):
            return EngineRunMode.CLOUD
        # we use the user's specification of a cluster in the connection config to determine if
        #   the engine is in cluster mode
        if self._extra_config.get("cluster"):
            return EngineRunMode.CLUSTER
        return EngineRunMode.STANDALONE

    @property
    def cluster(self) -> t.Optional[str]:
        return self._extra_config.get("cluster")

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
                    temp_table, columns_to_types, storage_format=exp.var("MergeTree"), **kwargs
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
        properties_copy = properties.copy()
        if self.engine_run_mode.is_cluster:
            properties_copy.append(exp.OnCluster(this=exp.to_identifier(self.cluster)))

        # can't call super() because it will try to set a catalog
        return self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties_copy,
            # sqlglot transpiles CREATE SCHEMA to CREATE DATABASE, but this text is used in an error message
            kind="DATABASE",
        )

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

        Clickhouse Cloud requires doing CTAS in two steps.

        First, we add the `EMPTY` property to the CTAS call to create a table with the proper
        schema, then insert the data with the CTAS query.
        """
        super()._create_table(
            table_name_or_schema,
            expression,
            exists,
            replace,
            columns_to_types,
            table_description,
            column_descriptions,
            table_kind,
            empty_ctas=(self.engine_run_mode.is_cloud and expression is not None),
            **kwargs,
        )

        # execute the second INSERT step if on cloud and creating a table
        # - Additional clause is to avoid clickhouse-connect HTTP client bug where CTAS LIMIT 0
        #     returns a success code but malformed response
        if (
            self.engine_run_mode.is_cloud
            and table_kind != "VIEW"
            and expression
            and not (
                expression.args.get("limit") is not None
                and expression.args["limit"].expression.this == "0"
            )
        ):
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

    def _rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        old_table_sql = exp.to_table(old_table_name).sql(dialect=self.dialect, identify=True)
        new_table_sql = exp.to_table(new_table_name).sql(dialect=self.dialect, identify=True)

        self.execute(f"RENAME TABLE {old_table_sql} TO {new_table_sql}{self._on_cluster_sql()}")

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        delete_expr = exp.delete(table_name, where)
        if self.engine_run_mode.is_cluster:
            delete_expr.set("cluster", exp.OnCluster(this=exp.to_identifier(self.cluster)))
        self.execute(delete_expr)

    def alter_table(
        self,
        alter_expressions: t.List[exp.Alter],
    ) -> None:
        """
        Performs the alter statements to change the current table into the structure of the target table.
        """
        with self.transaction():
            for alter_expression in alter_expressions:
                if self.engine_run_mode.is_cluster:
                    alter_expression.set(
                        "cluster", exp.OnCluster(this=exp.to_identifier(self.cluster))
                    )
                self.execute(alter_expression)

    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        **drop_args: t.Any,
    ) -> None:
        """Drops an object.

        An object could be a DATABASE, SCHEMA, VIEW, TABLE, DYNAMIC TABLE, TEMPORARY TABLE etc depending on the :kind.

        Args:
            name: The name of the table to drop.
            exists: If exists, defaults to True.
            kind: What kind of object to drop. Defaults to TABLE
            **drop_args: Any extra arguments to set on the Drop expression
        """
        drop_args.pop("cascade", None)
        self.execute(
            exp.Drop(
                this=exp.to_table(name),
                kind=kind,
                exists=exists,
                cluster=exp.OnCluster(this=exp.to_identifier(self.cluster))
                if self.engine_run_mode.is_cluster
                else None,
                **drop_args,
            )
        )

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        **kwargs: t.Any,
    ) -> t.Optional[t.Union[exp.PartitionedByProperty, exp.Property]]:
        return exp.PartitionedByProperty(
            this=exp.Schema(expressions=partitioned_by),
        )

    def ensure_nulls_for_unmatched_after_join(
        self,
        query: Query,
    ) -> Query:
        # Set `join_use_nulls = 1` in a query's SETTINGS clause
        query.append("settings", exp.var("join_use_nulls").eq(exp.Literal.number("1")))
        return query

    def use_server_nulls_for_unmatched_after_join(
        self,
        query: Query,
    ) -> Query:
        # Set the `join_use_nulls` server value in a query's SETTINGS clause
        #
        # Use in SCD models:
        #  - The SCD query we build must include the setting `join_use_nulls = 1` to ensure that empty cells in a join
        #      are filled with NULL instead of the default data type value. The default join_use_nulls value is `0`.
        #  - The SCD embeds the user's original query in the `source` CTE
        #  - Settings are dynamically scoped, so our setting may override the server's default setting the user expects
        #      for their query.
        #  - To prevent this, we:
        #     - If the user query sets `join_use_nulls`, we do nothing
        #     - If the user query does not set `join_use_nulls`, we query the server for the current setting
        #       - If the server value is 1, we do nothing
        #       - If the server values is not 1, we inject its `join_use_nulls` value into the user query
        #     - We do not need to check user subqueries because our injected setting operates at the same scope the
        #         server value would normally operate at
        setting_name = "join_use_nulls"
        setting_value = "1"

        user_settings = query.args.get("settings")
        # if user has not already set it explicitly
        if not (
            user_settings
            and any(
                [
                    isinstance(setting, exp.EQ) and setting.name == setting_name
                    for setting in user_settings
                ]
            )
        ):
            server_value = self.fetchone(
                exp.select("value")
                .from_("system.settings")
                .where(exp.column("name").eq(exp.Literal.string(setting_name)))
            )[0]
            # only inject the setting if the server value isn't 1
            inject_setting = setting_value != server_value
            setting_value = server_value if inject_setting else setting_value

            if inject_setting:
                query.append(
                    "settings", exp.var(setting_name).eq(exp.Literal.number(setting_value))
                )

        return query

    def _build_settings_property(
        self, key: str, value: exp.Expression | str | int | float
    ) -> exp.SettingsProperty:
        return exp.SettingsProperty(
            expressions=[
                exp.EQ(
                    this=exp.var(key.lower()),
                    expression=value
                    if isinstance(value, exp.Expression)
                    else exp.Literal(this=value, is_string=isinstance(value, str)),
                )
            ]
        )

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        empty_ctas: bool = False,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        table_engine = self.DEFAULT_TABLE_ENGINE
        if storage_format:
            table_engine = (
                storage_format.this if isinstance(storage_format, exp.Var) else storage_format  # type: ignore
            )
        properties.append(exp.EngineProperty(this=table_engine))

        # copy of table_properties so we can pop items off below then consume the rest later
        table_properties_copy = {
            k.upper(): v for k, v in (table_properties.copy() if table_properties else {}).items()
        }

        mergetree_engine = bool(re.search(self.ORDER_BY_TABLE_ENGINE_REGEX, table_engine))
        ordered_by_raw = table_properties_copy.pop("ORDER_BY", None)
        if mergetree_engine:
            ordered_by_exprs = []
            if ordered_by_raw:
                ordered_by_vals = []

                if isinstance(ordered_by_raw, exp.Tuple):
                    ordered_by_vals = ordered_by_raw.expressions
                if isinstance(ordered_by_raw, exp.Paren):
                    ordered_by_vals = [ordered_by_raw.this]

                if not ordered_by_vals:
                    ordered_by_vals = (
                        ordered_by_raw if isinstance(ordered_by_raw, list) else [ordered_by_raw]
                    )

                for col in ordered_by_vals:
                    ordered_by_exprs.append(
                        col
                        if isinstance(col, exp.Column)
                        else maybe_parse(
                            col.name if isinstance(col, exp.Literal) else col,
                            dialect=self.dialect,
                            into=exp.Ordered,
                        )
                    )

            properties.append(exp.Order(expressions=[exp.Tuple(expressions=ordered_by_exprs)]))

        primary_key = table_properties_copy.pop("PRIMARY_KEY", None)
        if mergetree_engine and primary_key:
            primary_key_vals = []
            if isinstance(primary_key, exp.Tuple):
                primary_key_vals = primary_key.expressions
            if isinstance(ordered_by_raw, exp.Paren):
                primary_key_vals = [primary_key.this]

            if not primary_key_vals:
                primary_key_vals = primary_key if isinstance(primary_key, list) else [primary_key]

            properties.append(
                exp.PrimaryKey(
                    expressions=[
                        exp.to_column(k.name if isinstance(k, exp.Literal) else k)
                        for k in primary_key_vals
                    ]
                )
            )

        if partitioned_by and (
            partitioned_by_prop := self._build_partitioned_by_exp(partitioned_by)
        ):
            properties.append(partitioned_by_prop)

        if self.engine_run_mode.is_cluster:
            properties.append(exp.OnCluster(this=exp.to_identifier(self.cluster)))

        if empty_ctas:
            properties.append(exp.EmptyProperty())

        if table_properties_copy:
            properties.extend(
                [self._build_settings_property(k, v) for k, v in table_properties_copy.items()]
            )

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

        if self.engine_run_mode.is_cluster:
            properties.append(exp.OnCluster(this=exp.to_identifier(self.cluster)))

        if view_properties_copy:
            properties.extend(
                [self._build_settings_property(k, v) for k, v in view_properties_copy.items()]
            )

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

        truncated_comment = self._truncate_table_comment(table_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER TABLE {table_sql}{self._on_cluster_sql()}MODIFY COMMENT {comment_sql}"

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

        truncated_comment = self._truncate_table_comment(column_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER TABLE {table_sql}{self._on_cluster_sql()}COMMENT COLUMN {column_sql} {comment_sql}"

    def _on_cluster_sql(self) -> str:
        if self.engine_run_mode.is_cluster:
            cluster_name = exp.to_identifier(self.cluster, quoted=True).sql(dialect=self.dialect)  #  type: ignore
            return f" ON CLUSTER {cluster_name} "
        return ""
