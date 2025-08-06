from __future__ import annotations

import logging
import typing as t

from sqlglot import exp, parse_one

from sqlmesh.utils import random_id
from sqlmesh.core.dialect import to_schema
from sqlglot.helper import ensure_list
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
from sqlmesh.utils.errors import (
    SQLMeshError,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(
    LogicalMergeMixin, PandasNativeFetchDFSupportMixin, NonTransactionalTruncateMixin
):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 200
    SUPPORTS_TRANSACTIONS = False  # Doris doesn't support transactions
    SUPPORTS_INDEXES = True  # Doris supports various indexes
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_AND_COMMANDS
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_REPLACE_TABLE = False  # Doris doesn't support REPLACE TABLE
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CREATE_DROP_CATALOG = False

    @property
    def supports_indexes(self) -> bool:
        """Doris supports various types of indexes."""
        return self.SUPPORTS_INDEXES

    # Schema differ with Doris-specific data types
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # Doris data type defaults (using standard types that SQLGlot recognizes)
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(0,), (38, 9)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: [(65533,)],
            exp.DataType.build("STRING", dialect=DIALECT).this: [(65533,)],
            exp.DataType.build("DATETIME", dialect=DIALECT).this: [(0,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(0,)],
        },
        types_with_unlimited_length={
            exp.DataType.build("STRING", dialect=DIALECT).this: {
                exp.DataType.build("STRING", dialect=DIALECT).this,
            },
        },
    )

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
            query = query.where(exp.column("table_name").isin(*object_names))
        df = self.fetchdf(query)

        result = []
        for row in df.itertuples(index=False, name=None):
            try:
                # Use positional indexing: (schema_name, name, type)
                schema = str(row[0]) if row[0] is not None else str(schema_name)
                name = str(row[1]) if row[1] is not None else "unknown"
                obj_type = str(row[2]) if row[2] is not None else "table"

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
            except (IndexError, AttributeError) as e:
                logger.error(f"Error processing row: {e}, row: {row}")
                continue

        return result

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        if replace:
            self.drop_view(
                view_name,
                ignore_if_not_exists=True,
                materialized=materialized,
                view_properties=view_properties,
            )
        if not columns_to_types and column_descriptions:
            columns_to_types = self._build_view_query_columns_to_types(query_or_df)

        if not materialized:
            return super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=False,  # Already dropped if needed
                materialized=False,
                materialized_properties=materialized_properties,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                **create_kwargs,
            )
        self._create_materialized_view(
            view_name,
            query_or_df,
            columns_to_types=columns_to_types,
            materialized_properties=materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            **create_kwargs,
        )

    def _create_materialized_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        # Convert query_or_df to proper format using base infrastructure
        query_or_df = self._native_df_to_pandas_df(query_or_df)
        import pandas as pd

        if isinstance(query_or_df, pd.DataFrame):
            values: t.List[t.Tuple[t.Any, ...]] = list(
                query_or_df.itertuples(index=False, name=None)
            )
            columns_to_types = columns_to_types or self._columns_to_types(query_or_df)
            if not columns_to_types:
                raise SQLMeshError("columns_to_types must be provided for dataframes")
            query_or_df = self._values_to_sql(
                values,
                columns_to_types,
                batch_start=0,
                batch_end=len(values),
            )

        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, batch_size=0, target_table=view_name
        )
        if len(source_queries) != 1:
            raise SQLMeshError("Only one source query is supported for creating materialized views")

        # Generate the query SQL
        with source_queries[0] as query:
            query_sql = query.sql(dialect=self.dialect, identify=True)

            # Build the complete SQL following Doris grammar order
            view_name_sql = exp.to_table(view_name).sql(dialect=self.dialect, identify=True)

            # Start with CREATE MATERIALIZED VIEW
            clauses = [f"CREATE MATERIALIZED VIEW {view_name_sql}"]

            # Add column definitions if provided
            if columns_to_types:
                col_defs = []
                for col in columns_to_types:
                    col_expr = exp.to_column(col)
                    col_sql = col_expr.sql(dialect=self.dialect, identify=True)
                    if column_descriptions and col in column_descriptions:
                        comment = column_descriptions[col]
                        col_defs.append(f"{col_sql} COMMENT '{comment}'")
                    else:
                        col_defs.append(col_sql)
                clauses.append(f"({', '.join(col_defs)})")

            # Extract Doris-specific properties from view_properties
            doris_clauses = []

            if view_properties:
                # BUILD clause
                build = view_properties.get("build")
                if build:
                    build_value = build.this if isinstance(build, exp.Literal) else str(build)
                    doris_clauses.append(f"BUILD {build_value}")

                # REFRESH clause
                refresh = view_properties.get("refresh")
                if refresh:
                    refresh_value = (
                        refresh.this if isinstance(refresh, exp.Literal) else str(refresh)
                    )
                    doris_clauses.append(f"REFRESH {refresh_value}")

                # refresh trigger clause
                refresh_trigger = view_properties.get("refresh_trigger")
                if refresh_trigger:
                    refresh_trigger_value = (
                        refresh_trigger.this
                        if isinstance(refresh_trigger, exp.Literal)
                        else str(refresh_trigger)
                    )
                    doris_clauses.append(f"{refresh_trigger_value}")

                # KEY clauses
                unique_key = view_properties.get("unique_key")
                duplicate_key = view_properties.get("duplicate_key")
                if unique_key:
                    if isinstance(unique_key, exp.Column):
                        key_cols = unique_key.sql(dialect=self.dialect, identify=True)
                    else:
                        key_cols = str(unique_key)
                    doris_clauses.append(f"KEY ({key_cols})")
                elif duplicate_key:
                    if isinstance(duplicate_key, exp.Column):
                        key_cols = duplicate_key.sql(dialect=self.dialect, identify=True)
                    else:
                        key_cols = str(duplicate_key)
                    doris_clauses.append(f"DUPLICATE KEY ({key_cols})")

                # COMMENT clause
                if table_description:
                    doris_clauses.append(
                        f"COMMENT '{self._truncate_table_comment(table_description)}'"
                    )

                # PARTITION BY clause - check view_properties first, then create_kwargs
                partitioned_by_expr = view_properties.get("partitioned_by_expr")
                if partitioned_by_expr:
                    partition_value = (
                        partitioned_by_expr.this
                        if isinstance(partitioned_by_expr, exp.Literal)
                        else str(partitioned_by_expr)
                    )
                    doris_clauses.append(f"PARTITION BY ({partition_value})")

                # DISTRIBUTED BY clause
                distributed_by = view_properties.get("distributed_by")
                if distributed_by and isinstance(distributed_by, exp.Tuple):
                    distributed_info = {}
                    for expr in distributed_by.expressions:
                        if isinstance(expr, exp.EQ) and hasattr(expr.this, "this"):
                            key = str(expr.this.this).strip('"')
                            if isinstance(expr.expression, exp.Literal):
                                distributed_info[key] = expr.expression.this
                            elif isinstance(expr.expression, exp.Column):
                                distributed_info[key] = expr.expression
                            else:
                                distributed_info[key] = str(expr.expression)

                    if distributed_info:
                        kind = distributed_info.get("kind")
                        expressions = distributed_info.get("expressions")
                        buckets = distributed_info.get("buckets")

                        if kind and expressions:
                            # Handle expressions
                            if isinstance(expressions, exp.Column):
                                expr_sql = expressions.sql(dialect=self.dialect, identify=True)
                            else:
                                expr_sql = str(expressions)

                            # Handle buckets
                            if buckets is not None:
                                if isinstance(buckets, exp.Literal):
                                    buckets_str = str(buckets.this)
                                else:
                                    buckets_str = str(buckets)
                                distributed_clause = (
                                    f"DISTRIBUTED BY HASH ({expr_sql}) BUCKETS {buckets_str}"
                                )
                            else:
                                distributed_clause = f"DISTRIBUTED BY HASH ({expr_sql})"

                            doris_clauses.append(distributed_clause)

                # PROPERTIES clause
                properties = {}
                for k, v in view_properties.items():
                    if k not in {
                        "build",
                        "refresh",
                        "refresh_trigger",
                        "distributed_by",
                        "partitioned_by",
                        "partitioned_by_expr",
                        "unique_key",
                        "duplicate_key",
                        "properties",
                        "materialized_type",
                        "source_table",
                    }:
                        properties[k] = v

                if properties:
                    props = []
                    for k, v in properties.items():
                        v_value = v.this if isinstance(v, exp.Literal) else str(v)
                        props.append(f"'{k}'='{v_value}'")
                    doris_clauses.append(f"PROPERTIES ({', '.join(props)})")

            # Handle partitioned_by from create_kwargs if not already handled
            if not any("PARTITION BY" in clause for clause in doris_clauses):
                # Check materialized_properties first, then create_kwargs
                partitioned_by = None
                if materialized_properties and materialized_properties.get("partitioned_by"):
                    partitioned_by = materialized_properties.get("partitioned_by")
                elif create_kwargs.get("partitioned_by"):
                    partitioned_by = create_kwargs.get("partitioned_by")

                if partitioned_by:
                    if isinstance(partitioned_by, list):
                        part_cols = ", ".join(
                            [
                                exp.to_column(col).sql(dialect=self.dialect, identify=True)
                                for col in partitioned_by
                            ]
                        )
                    else:
                        part_cols = exp.to_column(partitioned_by).sql(
                            dialect=self.dialect, identify=True
                        )
                    # Insert PARTITION BY clause before DISTRIBUTED BY if it exists
                    distributed_index = -1
                    for i, clause in enumerate(doris_clauses):
                        if "DISTRIBUTED BY" in clause:
                            distributed_index = i
                            break
                    if distributed_index >= 0:
                        doris_clauses.insert(distributed_index, f"PARTITION BY ({part_cols})")
                    else:
                        doris_clauses.append(f"PARTITION BY ({part_cols})")

            # Add Doris-specific clauses
            clauses.extend(doris_clauses)

            # Add the AS clause and query
            clauses.append(f"AS {query_sql}")

            # Join all clauses
            full_sql = " ".join(clauses)

            self.execute(full_sql)

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        Drop view in Doris.
        Doris doesn't support CASCADE clause for DROP VIEW.
        """
        # Remove cascade from kwargs as Doris doesn't support it
        if materialized and kwargs.get("view_properties"):
            view_properties = kwargs.pop("view_properties")
            if view_properties.get("materialized_type") == "SYNC" and view_properties.get(
                "source_table"
            ):
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

    def _rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        old_table_sql = exp.to_table(old_table_name).sql(dialect=self.dialect, identify=True)
        new_table_sql = exp.to_table(new_table_name).sql(dialect=self.dialect, identify=True)

        self.execute(f"ALTER TABLE {old_table_sql} RENAME {new_table_sql}")

    def _create_table_comment(
        self, table_name: TableName, table_comment: str, table_kind: str = "TABLE"
    ) -> None:
        table_sql = exp.to_table(table_name).sql(dialect=self.dialect, identify=True)

        self.execute(
            f'ALTER TABLE {table_sql} MODIFY COMMENT "{self._truncate_table_comment(table_comment)}"'
        )

    def _build_create_comment_column_exp(
        self, table: exp.Table, column_name: str, column_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)
        return f'ALTER TABLE {table_sql} MODIFY COLUMN {column_name} COMMENT "{self._truncate_column_comment(column_comment)}"'

    def _create_column_comments(
        self,
        table_name: TableName,
        column_comments: t.Dict[str, str],
        table_kind: str = "TABLE",
        materialized_view: bool = False,
    ) -> None:
        table = exp.to_table(table_name)

        for col, comment in column_comments.items():
            try:
                self.execute(self._build_create_comment_column_exp(table, col, comment, table_kind))
            except Exception:
                logger.warning(
                    f"Column comments for column '{col}' in table '{table.alias_or_name}' not registered - this may be due to limited permissions",
                    exc_info=True,
                )

    def delete_from(
        self, table_name: TableName, where: t.Optional[t.Union[str, exp.Expression]] = None
    ) -> None:
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
        return (
            "query" in expr.args
            and expr.args["query"]
            and isinstance(expr.args["query"], exp.Subquery)
        )

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

    def _create_table_from_columns(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a DDL statement.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: Mapping between the column name and its data type.
            primary_key: Determines the table primary key (converted to unique_key for Doris).
            exists: If True, will create "IF NOT EXISTS" table.
            table_description: Optional table description.
            column_descriptions: Optional column descriptions.
        """
        table_properties = kwargs.get("table_properties", {})

        # Convert primary_key to unique_key for Doris (Doris doesn't support primary keys)
        if primary_key and "unique_key" not in table_properties:
            table_properties["unique_key"] = primary_key

        # Update kwargs with the modified table_properties
        kwargs["table_properties"] = table_properties

        # Call the parent implementation with primary_key=None since we've converted it to unique_key
        super()._create_table_from_columns(
            table_name=table_name,
            columns_to_types=columns_to_types,
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
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[t.Union[exp.PartitionedByProperty, exp.PartitionByRangeProperty, exp.Property]]:
        """Doris supports range and list partition, but sqlglot only supports range partition, so we use PartitionByRangeProperty."""
        # Handle partitioned_by_expr from kwargs
        partitioned_by_expr = kwargs.get("partitioned_by_expr")
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

        if partitioned_by_expr:
            if isinstance(partitioned_by_expr, exp.Tuple):
                create_expressions = [
                    exp.Var(this=e.this, quoted=False)
                    if isinstance(e, exp.Literal)
                    else to_raw_sql(e)
                    for e in partitioned_by_expr.expressions
                ]
            elif isinstance(partitioned_by_expr, exp.Literal):
                create_expressions = [exp.Var(this=partitioned_by_expr.this, quoted=False)]
            else:
                create_expressions = [to_raw_sql(partitioned_by_expr)]

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
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
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
            if isinstance(unique_key, exp.Tuple):
                # Extract column names from Tuple expressions
                column_names = []
                for expr in unique_key.expressions:
                    if (
                        isinstance(expr, exp.Column)
                        and hasattr(expr, "this")
                        and hasattr(expr.this, "this")
                    ):
                        column_names.append(str(expr.this.this))
                    elif hasattr(expr, "this"):
                        column_names.append(str(expr.this))
                    else:
                        column_names.append(str(expr))
                properties.append(
                    exp.UniqueKeyProperty(expressions=[exp.to_column(k) for k in column_names])
                )
            elif isinstance(unique_key, exp.Column):
                # Handle as single column
                if hasattr(unique_key, "this") and hasattr(unique_key.this, "this"):
                    column_name = str(unique_key.this.this)
                else:
                    column_name = str(unique_key.this)
                properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(column_name)]))
            elif isinstance(unique_key, str):
                properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key)]))

        # Handle duplicate_key - only handle Tuple expressions or single Column expressions
        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        if duplicate_key is not None:
            if isinstance(duplicate_key, exp.Tuple):
                # Extract column names from Tuple expressions
                column_names = []
                for expr in duplicate_key.expressions:
                    if (
                        isinstance(expr, exp.Column)
                        and hasattr(expr, "this")
                        and hasattr(expr.this, "this")
                    ):
                        column_names.append(str(expr.this.this))
                    elif hasattr(expr, "this"):
                        column_names.append(str(expr.this))
                    else:
                        column_names.append(str(expr))
                properties.append(
                    exp.DuplicateKeyProperty(expressions=[exp.to_column(k) for k in column_names])
                )
            elif isinstance(duplicate_key, exp.Column):
                # Handle as single column
                if hasattr(duplicate_key, "this") and hasattr(duplicate_key.this, "this"):
                    column_name = str(duplicate_key.this.this)
                else:
                    column_name = str(duplicate_key.this)
                properties.append(
                    exp.DuplicateKeyProperty(expressions=[exp.to_column(column_name)])
                )
            elif isinstance(duplicate_key, str):
                properties.append(
                    exp.DuplicateKeyProperty(expressions=[exp.to_column(duplicate_key)])
                )

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        # Handle partitioning
        add_partition = True
        if partitioned_by:
            # check if partitioned_by columns are in unique_key, if not, skip PARTITION BY.
            if unique_key is not None:
                # Extract key column names from unique_key (only Tuple or Column expressions)
                key_cols_set = set()
                if isinstance(unique_key, exp.Tuple):
                    for expr in unique_key.expressions:
                        if (
                            isinstance(expr, exp.Column)
                            and hasattr(expr, "this")
                            and hasattr(expr.this, "this")
                        ):
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
                partitioned_by_expr = table_properties_copy.pop("partitioned_by_expr", None)

                # If partitioned_by is provided but partitioned_by_expr is not, add dynamic partition properties
                if partitioned_by and not partitioned_by_expr:
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
                    columns_to_types=columns_to_types,
                    catalog_name=catalog_name,
                    partitioned_by_expr=partitioned_by_expr,
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
                                str(e.this)
                                for e in expr.expression.expressions
                                if hasattr(e, "this")
                            ]
                        elif isinstance(expr.expression, exp.Tuple):
                            # Handle expressions tuple (array of strings)
                            distributed_info[key] = [
                                str(e.this)
                                for e in expr.expression.expressions
                                if hasattr(e, "this")
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
            unique_key_property = next(
                (prop for prop in properties if isinstance(prop, exp.UniqueKeyProperty)), None
            )
            if unique_key_property:
                # Use the first column from unique_key as the distribution key
                if unique_key_property.expressions:
                    first_col = unique_key_property.expressions[0]
                    column_name = (
                        str(first_col.this) if hasattr(first_col, "this") else str(first_col)
                    )
                    logger.info(
                        f"[Doris] Adding default distributed_by using unique_key column: {column_name}"
                    )
                    properties.append(
                        exp.DistributedByProperty(
                            expressions=[exp.to_column(column_name)],
                            kind="HASH",
                            buckets=exp.Literal.number(10),
                        )
                    )

        # Add any remaining properties as generic exp.Property
        table_type = self._pop_creatable_type_from_properties(table_properties_copy)
        properties.extend([p for p in ensure_list(table_type) if p is not None])

        # Remove partitioning-related keys so they don't leak into PROPERTIES
        table_properties_copy.pop("partitioned_by_expr", None)
        table_properties_copy.pop("partitioned_by", None)

        # Remove materialized_type from table_properties_copy
        table_properties_copy.pop("materialized_type", None)
        table_properties_copy.pop("source_table", None)

        # Only add generic properties if there are any left
        generic_properties = []
        for k, v in table_properties_copy.items():
            if not isinstance(v, exp.Expression):
                v = exp.Literal.string(str(v))
            generic_properties.append(exp.Property(this=exp.Literal.string(str(k)), value=v))
        if generic_properties:
            properties.extend(generic_properties)

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _get_temp_table(
        self, table: TableName, table_only: bool = False, quoted: bool = True
    ) -> exp.Table:
        """
        Returns the name of the temp table that should be used for the given table name.
        """
        table = t.cast(exp.Table, exp.to_table(table).copy())
        table.set(
            "this", exp.to_identifier(f"temp_{table.name}_{random_id(short=True)}", quoted=quoted)
        )

        if table_only:
            table.set("db", None)
            table.set("catalog", None)

        return table

    def _build_view_query_columns_to_types(
        self, query_or_df: t.Union[exp.Query, t.Any]
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        """Extract output column names from a SQLGlot Query and return a dict mapping them to unknown type."""
        if isinstance(query_or_df, exp.Query):
            # If any select is a Star (i.e., SELECT * or SELECT a.*), return None
            selects = getattr(query_or_df, "selects", [])
            if any(isinstance(s, exp.Star) for s in selects):
                return None
            output_columns = []
            if hasattr(query_or_df, "named_selects"):
                for expr in query_or_df.named_selects:
                    name = getattr(expr, "alias_or_name", None)
                    if not name:
                        name = getattr(expr, "name", None)
                    if not name:
                        name = str(expr)
                    output_columns.append(name)
            else:
                output_columns = [
                    getattr(s, "alias_or_name", getattr(s, "name", str(s)))
                    for s in getattr(query_or_df, "selects", [])
                ]
            return {col: exp.DataType.build("unknown") for col in output_columns}
        return None
