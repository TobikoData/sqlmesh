from __future__ import annotations

import logging
import typing as t
import re

from sqlglot import exp, parse_one

from sqlmesh.core.dialect import to_schema

from sqlmesh.core.engine_adapter.base import (
    InsertOverwriteStrategy,
)
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    set_catalog,
)
from sqlmesh.utils import random_id
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
    SUPPORTS_TRANSACTIONS = False
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_INDEXES = True
    SUPPORTS_REPLACE_TABLE = False
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CREATE_DROP_CATALOG = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.FULL_SUPPORT

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
        for schema_name, table_name, table_type in self.fetchall(query):
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
                logger.error(
                    f"Error processing row: {e}, row: {(schema_name, table_name, table_type)}"
                )
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
                replace=False,
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
            values: t.List[t.Tuple[t.Any, ...]] = list(
                query_or_df.itertuples(index=False, name=None)
            )
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

            # Use the unified _build_table_properties_exp to handle all properties
            properties_exp = self._build_table_properties_exp(
                catalog_name=target_table.catalog,
                table_properties=view_properties,
                target_columns_to_types=target_columns_to_types,
                table_description=table_description,
                partitioned_by=partitioned_by,
                partition_interval_unit=(
                    materialized_properties.get("partition_interval_unit")
                    if materialized_properties
                    else None
                ),
                table_kind="MATERIALIZED_VIEW",
            )

            self.execute(
                exp.Create(
                    this=schema,
                    kind="VIEW",
                    replace=False,
                    expression=query,
                    properties=properties_exp,
                )
            )

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
            table_properties["unique_key"] = exp.Tuple(
                expressions=[exp.to_column(col) for col in primary_key]
            )

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

    def _parse_partition_expressions(
        self, partitioned_by: t.List[exp.Expression]
    ) -> t.Tuple[t.List[exp.Expression], t.Optional[str]]:
        """Parse partition expressions and extract partition kind and normalized columns.

        Returns:
            Tuple of (normalized_partitioned_by, partition_kind)
        """
        parsed_partitioned_by: t.List[exp.Expression] = []
        partition_kind: t.Optional[str] = None

        for expr in partitioned_by:
            try:
                # Handle Anonymous function calls like RANGE(col) or LIST(col)
                if isinstance(expr, exp.Anonymous) and expr.this:
                    func_name = str(expr.this).upper()
                    if func_name in ("RANGE", "LIST"):
                        partition_kind = func_name
                        # Extract column expressions from function arguments
                        for arg in expr.expressions:
                            if isinstance(arg, exp.Column):
                                parsed_partitioned_by.append(arg)
                            else:
                                # Convert other expressions to columns if possible
                                parsed_partitioned_by.append(exp.to_column(str(arg)))
                        continue

                # Handle literal strings like "RANGE(col)" or "LIST(col)"
                if isinstance(expr, exp.Literal) and getattr(expr, "is_string", False):
                    text = str(expr.this)
                    match = re.match(r"^\s*(RANGE|LIST)\s*\((.*?)\)\s*$", text, flags=re.IGNORECASE)
                    if match:
                        partition_kind = match.group(1).upper()
                        inner = match.group(2)
                        inner_cols = [c.strip().strip("`") for c in inner.split(",") if c.strip()]
                        for col in inner_cols:
                            parsed_partitioned_by.append(exp.to_column(col))
                        continue
            except Exception:
                # If anything goes wrong, keep the original expr
                pass
            parsed_partitioned_by.append(expr)

        return parsed_partitioned_by, partition_kind

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        *,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[
        t.Union[
            exp.PartitionedByProperty,
            exp.PartitionByRangeProperty,
            exp.PartitionByListProperty,
            exp.Property,
        ]
    ]:
        """Build Doris partitioning expression.

        Supports both RANGE and LIST partition syntaxes using sqlglot's doris dialect nodes.
        The partition kind is chosen by:
        - inferred from partitioned_by expressions like 'RANGE(col)' or 'LIST(col)'
        - otherwise inferred from the provided 'partitions' strings: if any contains 'VALUES IN' -> LIST; else RANGE.
        """
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

        # Parse partition kind and columns from partitioned_by expressions
        partitioned_by, partition_kind = self._parse_partition_expressions(partitioned_by)

        if partitions:
            if isinstance(partitions, exp.Tuple):
                create_expressions = [
                    exp.Var(this=e.this, quoted=False)
                    if isinstance(e, exp.Literal)
                    else to_raw_sql(e)
                    for e in partitions.expressions
                ]
            elif isinstance(partitions, exp.Literal):
                create_expressions = [exp.Var(this=partitions.this, quoted=False)]
            else:
                create_expressions = [to_raw_sql(partitions)]

        # Infer partition kind from partitions text if not explicitly provided
        inferred_list = False
        if partition_kind is None and create_expressions:
            try:
                texts = [getattr(e, "this", "").upper() for e in create_expressions]
                inferred_list = any("VALUES IN" in t for t in texts)
            except Exception:
                inferred_list = False
        if partition_kind:
            kind_upper = str(partition_kind).upper()
            is_list = kind_upper == "LIST"
        else:
            is_list = inferred_list

        try:
            if is_list:
                return exp.PartitionByListProperty(
                    partition_expressions=partitioned_by,
                    create_expressions=create_expressions,
                )
            return exp.PartitionByRangeProperty(
                partition_expressions=partitioned_by,
                create_expressions=create_expressions,
            )
        except TypeError:
            if is_list:
                return exp.PartitionByListProperty(
                    partition_expressions=partitioned_by,
                    create_expressions=create_expressions,
                )
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
        is_materialized_view = table_kind == "MATERIALIZED_VIEW"

        # Add MATERIALIZED property first for materialized views
        if is_materialized_view:
            properties.append(exp.MaterializedProperty())

        table_properties_copy = dict(table_properties) if table_properties else {}

        # Helper functions for materialized view properties
        def _to_upper_str(val: t.Any) -> str:
            if isinstance(val, exp.Literal):
                return str(val.this).upper()
            if isinstance(val, exp.Identifier):
                return str(val.this).upper()
            if isinstance(val, str):
                return val.upper()
            if isinstance(val, exp.Expression):
                return val.sql(dialect=self.dialect).upper()
            return str(val).upper()

        def _to_var_upper(val: t.Any) -> exp.Var:
            return exp.Var(this=_to_upper_str(val))

        def _parse_refresh_tuple(
            tpl: exp.Tuple,
        ) -> t.Tuple[
            exp.Var,
            t.Optional[str],
            t.Optional[exp.Literal],
            t.Optional[exp.Var],
            t.Optional[exp.Literal],
        ]:
            method_var: exp.Var = exp.Var(this="AUTO")
            kind_str: t.Optional[str] = None
            every_lit: t.Optional[exp.Literal] = None
            unit_var: t.Optional[exp.Var] = None
            starts_lit: t.Optional[exp.Literal] = None
            for e in tpl.expressions:
                if (
                    isinstance(e, exp.EQ)
                    and isinstance(e.this, exp.Column)
                    and isinstance(e.this.this, exp.Identifier)
                ):
                    key = str(e.this.this.this).strip('"').lower()
                    val = e.expression
                    if key == "method":
                        method_var = _to_var_upper(val)
                    elif key == "kind":
                        kind_str = _to_upper_str(val)
                    elif key == "every":
                        if isinstance(val, exp.Literal):
                            # Keep numeric literal as-is so generator renders it
                            every_lit = val
                        elif isinstance(val, (int, float)):
                            every_lit = exp.Literal.number(int(val))
                    elif key == "unit":
                        unit_var = _to_var_upper(val)
                    elif key == "starts":
                        if isinstance(val, exp.Literal):
                            starts_lit = exp.Literal.string(str(val.this))
                        else:
                            starts_lit = exp.Literal.string(str(val))
            return method_var, kind_str, every_lit, unit_var, starts_lit

        def _parse_trigger_string(
            trigger_text: str,
        ) -> t.Tuple[
            t.Optional[str],
            t.Optional[exp.Literal],
            t.Optional[exp.Var],
            t.Optional[exp.Literal],
        ]:
            text = trigger_text.strip()
            if text.upper().startswith("ON "):
                text = text[3:].strip()
            kind = None
            every_lit: t.Optional[exp.Literal] = None
            unit_var = None
            starts_lit = None
            import re as _re

            m_kind = _re.match(r"^(MANUAL|COMMIT|SCHEDULE)\b", text, flags=_re.IGNORECASE)
            if m_kind:
                kind = m_kind.group(1).upper()
            m_every = _re.search(r"\bEVERY\s+(\d+)\s+(\w+)", text, flags=_re.IGNORECASE)
            if m_every:
                every_lit = exp.Literal.number(int(m_every.group(1)))
                unit_var = exp.Var(this=m_every.group(2).upper())
            m_starts = _re.search(r"\bSTARTS\s+'([^']+)'", text, flags=_re.IGNORECASE)
            if m_starts:
                starts_lit = exp.Literal.string(m_starts.group(1))
            return kind, every_lit, unit_var, starts_lit

        # Handle materialized view specific properties first
        if is_materialized_view:
            # BUILD property
            build_val = table_properties_copy.pop("build", None)
            if build_val is not None:
                properties.append(exp.BuildProperty(this=_to_var_upper(build_val)))

            # REFRESH + optional trigger combined into RefreshTriggerProperty
            refresh_val = table_properties_copy.pop("refresh", None)
            refresh_trigger_val = table_properties_copy.pop("refresh_trigger", None)
            if refresh_val is not None or refresh_trigger_val is not None:
                method_var: exp.Var = _to_var_upper(refresh_val or "AUTO")
                kind_str: t.Optional[str] = None
                every_lit: t.Optional[exp.Literal] = None
                unit_var: t.Optional[exp.Var] = None
                starts_lit: t.Optional[exp.Literal] = None

                if isinstance(refresh_val, exp.Tuple):
                    method_var, kind_str, every_lit, unit_var, starts_lit = _parse_refresh_tuple(
                        refresh_val
                    )
                else:
                    if refresh_trigger_val is not None:
                        trigger_text = (
                            str(refresh_trigger_val.this)
                            if isinstance(refresh_trigger_val, exp.Literal)
                            else str(refresh_trigger_val)
                        )
                        k, e, u, s = _parse_trigger_string(trigger_text)
                        kind_str, every_lit, unit_var, starts_lit = k, e, u, s

                properties.append(
                    exp.RefreshTriggerProperty(
                        method=method_var,
                        kind=kind_str,
                        every=every_lit,
                        unit=unit_var,
                        starts=starts_lit,
                    )
                )

        # Handle unique_key - only handle Tuple expressions or single Column expressions
        unique_key = table_properties_copy.pop("unique_key", None)
        if unique_key is not None:
            # For materialized views, KEY is rendered inline as UniqueKeyProperty, not skipped
            if not is_materialized_view:
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
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(column_name)])
                    )
                elif isinstance(unique_key, exp.Literal):
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key.this)])
                    )
                elif isinstance(unique_key, str):
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key)])
                    )
            else:
                # For materialized views, also add UniqueKeyProperty
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
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(column_name)])
                    )
                elif isinstance(unique_key, exp.Literal):
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key.this)])
                    )
                elif isinstance(unique_key, str):
                    properties.append(
                        exp.UniqueKeyProperty(expressions=[exp.to_column(unique_key)])
                    )

        # Handle duplicate_key - only handle Tuple expressions or single Column expressions
        # Both tables and materialized views support duplicate keys in Doris
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
            elif isinstance(duplicate_key, exp.Literal):
                properties.append(
                    exp.DuplicateKeyProperty(expressions=[exp.to_column(duplicate_key.this)])
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
            # Parse and normalize partition expressions
            partitioned_by, _ = self._parse_partition_expressions(partitioned_by)
            # For tables, check if partitioned_by columns are in unique_key; for materialized views, allow regardless
            if unique_key is not None and not is_materialized_view:
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
                partitions = table_properties_copy.pop("partitions", None)

                # If partitioned_by is provided but partitions is not, add dynamic partition properties
                # Skip dynamic partitions for materialized views as they use different partitioning
                if partitioned_by and not partitions and not is_materialized_view:
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

                # Build partition expression - different for materialized views vs tables
                if is_materialized_view:
                    # For materialized views, use PartitionedByProperty
                    if partitioned_by:
                        properties.append(
                            exp.PartitionedByProperty(
                                this=exp.Schema(expressions=partitioned_by),
                            )
                        )
                else:
                    # For tables, use the existing logic with RANGE/LIST partitioning
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