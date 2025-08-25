from __future__ import annotations

import logging
import typing as t
import re

from sqlglot import exp, parse_one

from sqlmesh.core.dialect import to_schema, transform_values

from sqlmesh.core.engine_adapter.base import (
    InsertOverwriteStrategy,
)
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
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import random_id, get_source_columns_to_types
from sqlmesh.utils.errors import (
    SQLMeshError,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF, Query
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(LogicalMergeMixin, PandasNativeFetchDFSupportMixin, NonTransactionalTruncateMixin):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 200
    SUPPORTS_TRANSACTIONS = False
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_REPLACE_TABLE = False 
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CREATE_DROP_CATALOG = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expression]] = None,
    ) -> None:
        """Create a schema."""
        properties = properties or []
        return super()._create_schema(
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
        """Drop schema in Doris. Note: Doris doesn't support CASCADE clause."""
        # In Doris, a schema is a database
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="DATABASE",
            cascade=False,
            **drop_args,
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema.
        Uses information_schema tables which are compatible with MySQL protocol.
        """
        query = (
            exp.select(
                exp.column("table_schema").as_("schema_name"),
                exp.column("table_name").as_("name"),
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
            # Doris may treat information_schema table_name comparisons as case-sensitive depending on settings.
            # Use LOWER(table_name) to match case-insensitively.
            lowered_names = [name.lower() for name in object_names]
            query = query.where(exp.func("LOWER", exp.column("table_name")).isin(*lowered_names))

        result = []
        rows = self.fetchall(query)
        for schema_name, table_name, table_type in rows:
            try:
                schema = str(schema_name) if schema_name is not None else str(schema_name)
                name = str(table_name) if table_name is not None else "unknown"
                obj_type = str(table_type) if table_type is not None else "table"

                # Normalize type
                if obj_type.upper() == "BASE TABLE":
                    obj_type = "table"
                elif obj_type.upper() == "VIEW":
                    obj_type = "view"

                data_object = DataObject(
                    schema=schema,
                    name=name,
                    type=DataObjectType.from_str(obj_type),
                )
                result.append(data_object)
            except (ValueError, AttributeError) as e:
                logger.error(f"Error processing row: {e}, row: {(schema_name, table_name, table_type)}")
                continue

        return result

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        if replace:
            self.drop_view(
                view_name,
                ignore_if_not_exists=True,
                materialized=materialized,
                view_properties=view_properties,
            )
        if not materialized:
            return super().create_view(
                view_name,
                query_or_df,
                target_columns_to_types,
                replace=False,  # Already dropped if needed
                materialized=False,
                materialized_properties=materialized_properties,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                source_columns=source_columns,
                **create_kwargs,
            )
        self._create_materialized_view(
            view_name,
            query_or_df,
            target_columns_to_types=target_columns_to_types,
            materialized_properties=materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            source_columns=source_columns,
            **create_kwargs,
        )

    def _create_materialized_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        # Convert query_or_df to proper format using base infrastructure
        query_or_df = self._native_df_to_pandas_df(query_or_df)
        import pandas as pd

        if isinstance(query_or_df, pd.DataFrame):
            values: t.List[t.Tuple[t.Any, ...]] = list(query_or_df.itertuples(index=False, name=None))
            target_columns_to_types, source_columns = self._columns_to_types(
                query_or_df, target_columns_to_types, source_columns
            )
            if not target_columns_to_types:
                raise SQLMeshError("columns_to_types must be provided for dataframes")
            query_or_df = self._values_to_sql(
                values,
                target_columns_to_types,
                batch_start=0,
                batch_end=len(values),
            )

        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, target_columns_to_types, batch_size=0, target_table=view_name
        )
        if len(source_queries) != 1:
            raise SQLMeshError("Only one source query is supported for creating materialized views")

        # Build the CREATE MATERIALIZED VIEW statement using SQLGlot AST
        with source_queries[0] as query:
            target_table = exp.to_table(view_name)

            # Build schema for column list and comments if provided
            schema: t.Union[exp.Table, exp.Schema] = target_table
            if target_columns_to_types:
                schema = self._build_schema_exp(
                    target_table,
                    target_columns_to_types,
                    column_descriptions,
                    is_view=True,
                )

            # Partitioning may be specified in different places for materialized views
            partitioned_by = None
            if materialized_properties and materialized_properties.get("partitioned_by"):
                partitioned_by = materialized_properties.get("partitioned_by")
            elif create_kwargs.get("partitioned_by"):
                partitioned_by = create_kwargs.get("partitioned_by")

            # Collect Doris properties using AST where supported
            props: t.List[exp.Expression] = [exp.MaterializedProperty()]

            # Partitioned_by for MATERIALIZED VIEW must use PartitionedByProperty (not range)
            part_by_list: t.Optional[t.List[exp.Expression]] = None
            if partitioned_by:
                part_by_list = partitioned_by if isinstance(partitioned_by, list) else [partitioned_by]
            partition_prop: t.Optional[exp.Expression] = None
            if part_by_list:
                partition_prop = exp.PartitionedByProperty(
                    this=exp.Schema(expressions=part_by_list),
                )

            # Other properties (COMMENT, DISTRIBUTED BY, PROPERTIES) via builder; omit partitioned_by here
            extra_props_node = self._build_table_properties_exp(
                catalog_name=target_table.catalog,
                table_properties=view_properties or None,
                target_columns_to_types=target_columns_to_types,
                table_description=table_description,
                partitioned_by=None,
                partition_interval_unit=(
                    materialized_properties.get("partition_interval_unit") if materialized_properties else None
                ),
                table_kind="MATERIALIZED_VIEW",
            )
            if extra_props_node is not None and getattr(extra_props_node, "expressions", None):
                # Ensure COMMENT comes before PARTITION BY, then other properties
                comment_props = [p for p in extra_props_node.expressions if isinstance(p, exp.SchemaCommentProperty)]
                non_comment_props = [
                    p for p in extra_props_node.expressions if not isinstance(p, exp.SchemaCommentProperty)
                ]
                props.extend(comment_props)
                if partition_prop is not None:
                    props.append(partition_prop)
                props.extend(non_comment_props)
            elif partition_prop is not None:
                props.append(partition_prop)

            create_props = exp.Properties(expressions=props) if props else None

            create_exp = exp.Create(
                this=schema,
                kind="VIEW",
                replace=False,
                expression=query,
                properties=create_props,
            )

            create_sql = create_exp.sql(dialect=self.dialect, identify=True)

            # Insert BUILD / REFRESH / refresh_trigger immediately after the column list
            doris_inline_clauses: t.List[str] = []
            if view_properties:
                build = view_properties.get("build")
                if build is not None:
                    build_value = build.this if isinstance(build, exp.Literal) else str(build)
                    doris_inline_clauses.append(f"BUILD {build_value}")
                refresh = view_properties.get("refresh")
                if refresh is not None:
                    refresh_value = refresh.this if isinstance(refresh, exp.Literal) else str(refresh)
                    doris_inline_clauses.append(f"REFRESH {refresh_value}")
                refresh_trigger = view_properties.get("refresh_trigger")
                if refresh_trigger is not None:
                    refresh_trigger_value = (
                        refresh_trigger.this if isinstance(refresh_trigger, exp.Literal) else str(refresh_trigger)
                    )
                    doris_inline_clauses.append(str(refresh_trigger_value))
                # Doris materialized views use KEY (<cols>) instead of UNIQUE KEY property
                unique_key = view_properties.get("unique_key")
                if unique_key is not None:
                    # Normalize to a list of column expressions
                    key_columns: t.List[exp.Expression] = []
                    if isinstance(unique_key, exp.Tuple):
                        key_columns = list(unique_key.expressions)
                    elif isinstance(unique_key, (exp.Column, exp.Identifier)):
                        key_columns = [unique_key]
                    elif isinstance(unique_key, exp.Literal):
                        key_columns = [exp.to_column(unique_key.this)]
                    elif isinstance(unique_key, str):
                        key_columns = [exp.to_column(unique_key)]
                    else:
                        # Fallback to string conversion
                        key_columns = [exp.to_column(str(unique_key))]

                    cols_sql = ", ".join(col.sql(dialect=self.dialect, identify=True) for col in key_columns)
                    doris_inline_clauses.append(f"KEY ({cols_sql})")

            if doris_inline_clauses:
                insert_text = " ".join(doris_inline_clauses)
                paren_start = create_sql.find("(")
                insert_pos = -1
                if paren_start != -1:
                    depth = 0
                    for i in range(paren_start, len(create_sql)):
                        ch = create_sql[i]
                        if ch == "(":
                            depth += 1
                        elif ch == ")":
                            depth -= 1
                            if depth == 0:
                                insert_pos = i + 1
                                break
                if insert_pos == -1:
                    after_mv = create_sql.find("CREATE MATERIALIZED VIEW")
                    if after_mv != -1:
                        as_idx = create_sql.find(" AS ")
                        insert_pos = as_idx if as_idx != -1 else len(create_sql)
                    else:
                        insert_pos = 0
                create_sql = f"{create_sql[:insert_pos]} {insert_text}{create_sql[insert_pos:]}"

            self.execute(create_sql)

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        Drop view in Doris.
        """
        # Remove cascade from kwargs as Doris doesn't support it
        if materialized and kwargs.get("view_properties"):
            view_properties = kwargs.pop("view_properties")
            if view_properties.get("materialized_type") == "SYNC" and view_properties.get("source_table"):
                # Format the source table name properly for Doris
                source_table = view_properties.get("source_table")
                if isinstance(source_table, exp.Table):
                    source_table_sql = source_table.sql(dialect=self.dialect, identify=True)
                else:
                    source_table_sql = str(source_table)
                drop_sql = f"DROP MATERIALIZED VIEW {'IF EXISTS ' if ignore_if_not_exists else ''}{view_name} ON {source_table_sql}"
                self.execute(drop_sql)
                return
        super().drop_view(view_name, ignore_if_not_exists, materialized, **kwargs)

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            exp.Create(
                this=exp.to_table(target_table_name),
                kind="TABLE",
                exists=exists,
                properties=exp.Properties(
                    expressions=[
                        exp.LikeProperty(
                            this=exp.to_table(source_table_name),
                        ),
                    ],
                ),
            )
        )

    def _create_table_comment(self, table_name: TableName, table_comment: str, table_kind: str = "TABLE") -> None:
        table_sql = exp.to_table(table_name).sql(dialect=self.dialect, identify=True)

        self.execute(f'ALTER TABLE {table_sql} MODIFY COMMENT "{self._truncate_table_comment(table_comment)}"')

    def _build_create_comment_column_exp(
        self, table: exp.Table, column_name: str, column_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)
        return f'ALTER TABLE {table_sql} MODIFY COLUMN {column_name} COMMENT "{self._truncate_column_comment(column_comment)}"'

    def delete_from(self, table_name: TableName, where: t.Optional[t.Union[str, exp.Expression]] = None) -> None:
        """
        Delete from a table.

        Args:
            table_name: The table to delete from.
            where: The where clause to filter rows to delete.
        """
        if not where or where == exp.true():
            table_expr = exp.to_table(table_name) if isinstance(table_name, str) else table_name
            self.execute(f"TRUNCATE TABLE {table_expr.sql(dialect=self.dialect, identify=True)}")
            return

        # Parse where clause if it's a string
        if isinstance(where, str):
            where = parse_one(where, dialect=self.dialect)

        # Check if WHERE contains subqueries with IN/NOT IN operators
        subquery_expr = self._find_subquery_in_condition(where)
        if subquery_expr:
            self._execute_delete_with_subquery(table_name, subquery_expr)
        else:
            # Use base implementation for simple conditions
            super().delete_from(table_name, where)

    def _find_subquery_in_condition(
        self, where: exp.Expression
    ) -> t.Optional[t.Tuple[exp.Expression, exp.Expression, bool]]:
        """
        Find subquery in IN/NOT IN condition.

        Returns:
            Tuple of (column_expr, subquery, is_not_in) or None if no subquery found
        """
        # Check for NOT IN expressions first (NOT wrapping an IN expression)
        for not_expr in where.find_all(exp.Not):
            if isinstance(not_expr.this, exp.In) and self._is_subquery_expression(not_expr.this):
                return not_expr.this.args["this"], not_expr.this.args["query"], True

        # Check for IN expressions with subqueries (only if not already found as NOT IN)
        for expr in where.find_all(exp.In):
            if self._is_subquery_expression(expr):
                return expr.args["this"], expr.args["query"], False

        return None

    def _is_subquery_expression(self, expr: exp.Expression) -> bool:
        """Check if expression contains a subquery."""
        return "query" in expr.args and expr.args["query"] and isinstance(expr.args["query"], exp.Subquery)

    def _execute_delete_with_subquery(
        self, table_name: TableName, subquery_info: t.Tuple[exp.Expression, exp.Expression, bool]
    ) -> None:
        """
        Execute DELETE FROM with subquery using Doris USING syntax.

        Args:
            table_name: Target table name
            subquery_info: Tuple of (column_expr, subquery, is_not_in)
        """
        column_expr, subquery, is_not_in = subquery_info

        # Build join condition
        join_condition = self._build_join_condition(column_expr, is_not_in)

        # Build and execute DELETE statement
        target_sql = f"{exp.to_table(table_name).sql(dialect=self.dialect, identify=True)} AS `_t1`"
        subquery_sql = f"{subquery.sql(dialect=self.dialect, identify=True)} AS `_t2`"
        where_sql = join_condition.sql(dialect=self.dialect, identify=True)

        delete_sql = f"DELETE FROM {target_sql} USING {subquery_sql} WHERE {where_sql}"
        self.execute(delete_sql)

    def _build_join_condition(self, column_expr: exp.Expression, is_not_in: bool) -> exp.Expression:
        """Build join condition for DELETE USING statement."""
        if isinstance(column_expr, exp.Tuple):
            # Multiple columns: (id, name) IN (...)
            join_conditions = []
            for col in column_expr.expressions:
                condition = self._build_column_condition(col, is_not_in)
                join_conditions.append(condition)
            return exp.and_(*join_conditions)
        # Single column: id IN (...)
        return self._build_column_condition(column_expr, is_not_in)

    def _build_column_condition(self, column: exp.Expression, is_not_in: bool) -> exp.Expression:
        """Build condition for a single column."""
        t1_col = exp.column(column.name, table="_t1")
        t2_col = exp.column(column.name, table="_t2")
        return t1_col.neq(t2_col) if is_not_in else t1_col.eq(t2_col)

    def replace_query(
        self,
        table_name: "TableName",
        query_or_df: "QueryOrDF",
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Doris does not support REPLACE TABLE. Avoid CTAS on replace and always perform a
        delete+insert (or engine strategy) to ensure data is written even if the table exists.
        """
        target_table = exp.to_table(table_name)
        source_queries, inferred_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=target_table,
            source_columns=source_columns,
        )
        target_columns_to_types = inferred_columns_to_types or self.columns(target_table)
        # Use the standard insert-overwrite-by-condition path (DELETE/INSERT for Doris by default)
        return self._insert_overwrite_by_condition(
            target_table,
            source_queries,
            target_columns_to_types,
        )

    def _values_to_sql(
        self,
        values: t.List[t.Tuple[t.Any, ...]],
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
        source_columns: t.Optional[t.List[str]] = None,
    ) -> "Query":
        """
        Build a SELECT/UNION ALL subquery for a batch of literal rows.

        Doris (MySQL-compatible) doesn't reliably render SQLGlot's VALUES in FROM when using the
        'doris' dialect, which led to an empty `(SELECT)` subquery. To avoid that, construct a
        dialect-agnostic union of SELECT literals and then cast/order in an outer SELECT.
        """
        source_columns = source_columns or list(target_columns_to_types)
        source_columns_to_types = get_source_columns_to_types(target_columns_to_types, source_columns)

        row_values = values[batch_start:batch_end]

        inner: exp.Query
        if not row_values:
            # Produce a zero-row subquery with the correct schema
            zero_row_select = exp.select(
                *[
                    exp.cast(exp.null(), to=col_type).as_(col, quoted=True)
                    for col, col_type in source_columns_to_types.items()
                ]
            ).where(exp.false())
            inner = zero_row_select
        else:
            # Build UNION ALL of SELECT <literals AS columns>
            selects: t.List[exp.Select] = []
            for row in row_values:
                converted_vals = list(transform_values(row, source_columns_to_types))
                select_exprs = [
                    exp.alias_(val, col, quoted=True)
                    for val, col in zip(converted_vals, source_columns_to_types.keys())
                ]
                selects.append(exp.select(*select_exprs))

            inner = selects[0]
            for s in selects[1:]:
                inner = exp.union(inner, s, distinct=False)

        # Outer select to coerce/order target columns
        casted_columns = [
            exp.alias_(
                exp.cast(
                    exp.column(column, table=alias, quoted=True) if column in source_columns_to_types else exp.Null(),
                    to=kind,
                ),
                column,
                quoted=True,
            )
            for column, kind in target_columns_to_types.items()
        ]

        final_query = exp.select(*casted_columns).from_(
            exp.alias_(exp.Subquery(this=inner), alias, table=True),
            copy=False,
        )

        return final_query

    def _create_table_from_columns(
        self,
        table_name: TableName,
        target_columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Create a table using a DDL statement.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            target_columns_to_types: Mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        table_properties = kwargs.get("table_properties", {})

        # Convert primary_key to unique_key for Doris (Doris doesn't support primary keys)
        if primary_key and "unique_key" not in table_properties:
            # Represent as a Tuple of columns to match downstream handling
            table_properties["unique_key"] = exp.Tuple(expressions=[exp.to_column(col) for col in primary_key])

        # Update kwargs with the modified table_properties
        kwargs["table_properties"] = table_properties

        # Call the parent implementation with primary_key=None since we've converted it to unique_key
        super()._create_table_from_columns(
            table_name=table_name,
            target_columns_to_types=target_columns_to_types,
            primary_key=None,  # Set to None since we've converted it to unique_key
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        *,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[t.Union[exp.PartitionedByProperty, exp.PartitionByRangeProperty, exp.Property]]:
        """Doris supports range and list partition, but sqlglot only supports range partition."""
        partitions = kwargs.get("partitions")
        create_expressions = None

        def to_raw_sql(expr: t.Union[exp.Literal, exp.Var, str, t.Any]) -> exp.Var:
            # If it's a Literal, extract the string and wrap as Var (no quotes)
            if isinstance(expr, exp.Literal):
                return exp.Var(this=expr.this, quoted=False)
            # If it's already a Var, return as is
            if isinstance(expr, exp.Var):
                return expr
            # If it's a string, wrap as Var (no quotes)
            if isinstance(expr, str):
                return exp.Var(this=expr, quoted=False)
            # Fallback: return as is
            return expr

        if partitions:
            if isinstance(partitions, exp.Tuple):
                create_expressions = [
                    exp.Var(this=e.this, quoted=False) if isinstance(e, exp.Literal) else to_raw_sql(e)
                    for e in partitions.expressions
                ]
            elif isinstance(partitions, exp.Literal):
                create_expressions = [exp.Var(this=partitions.this, quoted=False)]
            else:
                create_expressions = [to_raw_sql(partitions)]

        return exp.PartitionByRangeProperty(
            partition_expressions=partitioned_by,
            create_expressions=create_expressions,
        )

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, t.Any]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for ddl."""
        properties: t.List[exp.Expression] = []

        table_properties_copy = dict(table_properties) if table_properties else {}

        # Handle unique_key - only handle Tuple expressions or single Column expressions
        unique_key = table_properties_copy.pop("unique_key", None)
        if unique_key is not None:
            # For materialized views, KEY is rendered inline, not as a UniqueKeyProperty
            if table_kind != "MATERIALIZED_VIEW":
                if isinstance(unique_key, exp.Tuple):
                    # Extract column names from Tuple expressions
                    column_names = []
                    for expr in unique_key.expressions:
                        if isinstance(expr, exp.Column) and hasattr(expr, "this") and hasattr(expr.this, "this"):
                            column_names.append(str(expr.this.this))
                        elif hasattr(expr, "this"):
                            column_names.append(str(expr.this))
                        else:
                            column_names.append(str(expr))
                    properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(k) for k in column_names]))
                elif isinstance(unique_key, exp.Column):
                    # Handle as single column
                    if hasattr(unique_key, "this") and hasattr(unique_key.this, "this"):
                        column_name = str(unique_key.this.this)
                    else:
                        column_name = str(unique_key.this)
                    properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(column_name)]))
                elif isinstance(unique_key, exp.Literal):
                    properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key.this)]))
                elif isinstance(unique_key, str):
                    properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key)]))

        # Handle duplicate_key - only handle Tuple expressions or single Column expressions
        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        if duplicate_key is not None:
            if table_kind != "MATERIALIZED_VIEW":
                if isinstance(duplicate_key, exp.Tuple):
                    # Extract column names from Tuple expressions
                    column_names = []
                    for expr in duplicate_key.expressions:
                        if isinstance(expr, exp.Column) and hasattr(expr, "this") and hasattr(expr.this, "this"):
                            column_names.append(str(expr.this.this))
                        elif hasattr(expr, "this"):
                            column_names.append(str(expr.this))
                        else:
                            column_names.append(str(expr))
                    properties.append(exp.DuplicateKeyProperty(expressions=[exp.to_column(k) for k in column_names]))
                elif isinstance(duplicate_key, exp.Column):
                    # Handle as single column
                    if hasattr(duplicate_key, "this") and hasattr(duplicate_key.this, "this"):
                        column_name = str(duplicate_key.this.this)
                    else:
                        column_name = str(duplicate_key.this)
                    properties.append(exp.DuplicateKeyProperty(expressions=[exp.to_column(column_name)]))
                elif isinstance(duplicate_key, exp.Literal):
                    properties.append(exp.DuplicateKeyProperty(expressions=[exp.to_column(duplicate_key.this)]))
                elif isinstance(duplicate_key, str):
                    properties.append(exp.DuplicateKeyProperty(expressions=[exp.to_column(duplicate_key)]))

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(this=exp.Literal.string(self._truncate_table_comment(table_description)))
            )

        # Handle partitioning
        add_partition = True
        if partitioned_by:
            normalized_partitioned_by: t.List[exp.Expression] = []
            for expr in partitioned_by:
                try:
                    # Handle literal strings like "RANGE(col)" or "LIST(col)"
                    if isinstance(expr, exp.Literal) and getattr(expr, "is_string", False):
                        text = str(expr.this)
                        match = re.match(r"^\s*(RANGE|LIST)\s*\((.*?)\)\s*$", text, flags=re.IGNORECASE)
                        if match:
                            inner = match.group(2)
                            inner_cols = [c.strip().strip("`") for c in inner.split(",") if c.strip()]
                            for col in inner_cols:
                                normalized_partitioned_by.append(exp.to_column(col))
                            continue
                except Exception:
                    # If anything goes wrong, keep the original expr
                    pass
                normalized_partitioned_by.append(expr)

            # Replace with normalized expressions
            partitioned_by = normalized_partitioned_by
            # For tables, check if partitioned_by columns are in unique_key; for materialized views, allow regardless
            if unique_key is not None and table_kind != "MATERIALIZED_VIEW":
                # Extract key column names from unique_key (only Tuple or Column expressions)
                key_cols_set = set()
                if isinstance(unique_key, exp.Tuple):
                    for expr in unique_key.expressions:
                        if isinstance(expr, exp.Column) and hasattr(expr, "this") and hasattr(expr.this, "this"):
                            key_cols_set.add(str(expr.this.this))
                        elif hasattr(expr, "this"):
                            key_cols_set.add(str(expr.this))
                        else:
                            key_cols_set.add(str(expr))
                elif isinstance(unique_key, exp.Column):
                    if hasattr(unique_key, "this") and hasattr(unique_key.this, "this"):
                        key_cols_set.add(str(unique_key.this.this))
                    else:
                        key_cols_set.add(str(unique_key.this))

                partition_cols = set()
                for expr in partitioned_by:
                    if hasattr(expr, "name"):
                        partition_cols.add(str(expr.name))
                    elif hasattr(expr, "this"):
                        partition_cols.add(str(expr.this))
                    else:
                        partition_cols.add(str(expr))
                not_in_key = partition_cols - key_cols_set
                if not_in_key:
                    logger.warning(
                        f"[Doris] UNIQUE KEY model: partitioned_by columns {not_in_key} not in key_cols {key_cols_set}, skip PARTITION BY."
                    )
                    add_partition = False
            if add_partition:
                partitions = table_properties_copy.pop("partitions", None)

                # If partitioned_by is provided but partitions is not, add dynamic partition properties
                if partitioned_by and not partitions:
                    # Define the required dynamic partition properties
                    dynamic_partition_props = {
                        "dynamic_partition.enable": "true",
                        "dynamic_partition.time_unit": "DAY",
                        "dynamic_partition.start": "-490",
                        "dynamic_partition.end": "10",
                        "dynamic_partition.prefix": "p",
                        "dynamic_partition.buckets": "32",
                        "dynamic_partition.create_history_partition": "true",
                    }

                    # Use partition_interval_unit if provided to set the time_unit
                    if partition_interval_unit:
                        if hasattr(partition_interval_unit, "value"):
                            time_unit = partition_interval_unit.value.upper()
                        else:
                            time_unit = str(partition_interval_unit).upper()
                        dynamic_partition_props["dynamic_partition.time_unit"] = time_unit

                    # Add missing dynamic partition properties to table_properties_copy
                    for key, value in dynamic_partition_props.items():
                        if key not in table_properties_copy:
                            table_properties_copy[key] = value

                partition_expr = self._build_partitioned_by_exp(
                    partitioned_by,
                    partition_interval_unit=partition_interval_unit,
                    target_columns_to_types=target_columns_to_types,
                    catalog_name=catalog_name,
                    partitions=partitions,
                )
                if partition_expr:
                    properties.append(partition_expr)

        # Handle distributed_by property - parse Tuple with EQ expressions or Paren with single EQ
        distributed_by = table_properties_copy.pop("distributed_by", None)
        if distributed_by is not None:
            distributed_info = {}

            if isinstance(distributed_by, exp.Tuple):
                # Parse the Tuple with EQ expressions to extract distributed_by info
                for expr in distributed_by.expressions:
                    if isinstance(expr, exp.EQ) and hasattr(expr.this, "this"):
                        # Remove quotes from the key if present
                        key = str(expr.this.this).strip('"')
                        if isinstance(expr.expression, exp.Literal):
                            distributed_info[key] = expr.expression.this
                        elif isinstance(expr.expression, exp.Array):
                            # Handle expressions array
                            distributed_info[key] = [
                                str(e.this) for e in expr.expression.expressions if hasattr(e, "this")
                            ]
                        elif isinstance(expr.expression, exp.Tuple):
                            # Handle expressions tuple (array of strings)
                            distributed_info[key] = [
                                str(e.this) for e in expr.expression.expressions if hasattr(e, "this")
                            ]
                        else:
                            distributed_info[key] = str(expr.expression)
            elif isinstance(distributed_by, exp.Paren) and isinstance(distributed_by.this, exp.EQ):
                # Handle single key-value pair in parentheses (e.g., (kind='RANDOM'))
                expr = distributed_by.this
                if hasattr(expr.this, "this"):
                    # Remove quotes from the key if present
                    key = str(expr.this.this).strip('"')
                    if isinstance(expr.expression, exp.Literal):
                        distributed_info[key] = expr.expression.this
                    else:
                        distributed_info[key] = str(expr.expression)
            elif isinstance(distributed_by, dict):
                # Handle as dictionary (legacy format)
                distributed_info = distributed_by

            # Create DistributedByProperty from parsed info
            if distributed_info:
                kind = distributed_info.get("kind")
                expressions = distributed_info.get("expressions")
                buckets = distributed_info.get("buckets")

                if kind:
                    # Handle buckets - convert string to int if it's a numeric string
                    buckets_expr: t.Optional[exp.Expression] = None
                    if isinstance(buckets, int):
                        buckets_expr = exp.Literal.number(buckets)
                    elif isinstance(buckets, str):
                        if buckets == "AUTO":
                            buckets_expr = exp.Var(this="AUTO")
                        elif buckets.isdigit():
                            buckets_expr = exp.Literal.number(int(buckets))

                    # Handle expressions - convert single string to list if needed
                    expressions_list = None
                    if expressions:
                        if isinstance(expressions, str):
                            expressions_list = [exp.to_column(expressions)]
                        elif isinstance(expressions, list):
                            expressions_list = [exp.to_column(e) for e in expressions]
                        else:
                            expressions_list = [exp.to_column(str(expressions))]

                    prop = exp.DistributedByProperty(
                        kind=exp.Var(this=kind),
                        expressions=expressions_list,
                        buckets=buckets_expr,
                        order=None,
                    )
                    properties.append(prop)
        else:
            unique_key_property = next((prop for prop in properties if isinstance(prop, exp.UniqueKeyProperty)), None)
            if unique_key_property:
                # Use the first column from unique_key as the distribution key
                if unique_key_property.expressions:
                    first_col = unique_key_property.expressions[0]
                    column_name = str(first_col.this) if hasattr(first_col, "this") else str(first_col)
                    logger.info(f"[Doris] Adding default distributed_by using unique_key column: {column_name}")
                    properties.append(
                        exp.DistributedByProperty(
                            expressions=[exp.to_column(column_name)],
                            kind="HASH",
                            buckets=exp.Literal.number(10),
                        )
                    )

        # Only add generic properties if there are any left
        if table_properties_copy:
            properties.extend(self._properties_to_expressions(table_properties_copy))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _get_temp_table_name(self, table: TableName) -> str:
        table_obj = exp.to_table(table)
        return f"temp_{table_obj.name}_{random_id(short=True)}"

    def _properties_to_expressions(self, properties: t.Dict[str, t.Any]) -> t.List[exp.Expression]:
        """Convert a dictionary of properties to a list of exp.Property expressions."""
        expressions: t.List[exp.Expression] = []
        for key, value in properties.items():
            if key in {
                "build",
                "refresh",
                "refresh_trigger",
                "distributed_by",
                "partitioned_by",
                "partitions",
                "unique_key",
                "duplicate_key",
                "properties",
                "materialized_type",
                "source_table",
            }:
                continue
            if not isinstance(value, exp.Expression):
                value = exp.Literal.string(str(value))
            expressions.append(exp.Property(this=exp.Literal.string(str(key)), value=value))
        return expressions
