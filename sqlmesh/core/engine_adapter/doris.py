from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlglot.helper import ensure_list
from sqlmesh.core.engine_adapter.mixins import (
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
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
from sqlmesh.utils import random_id

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
    NonTransactionalTruncateMixin,
):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 200
    SUPPORTS_TRANSACTIONS = False  # Doris doesn't support transactions
    SUPPORTS_INDEXES = True  # Doris supports various indexes (inverted, bloom filter, etc.)
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
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
                # Use positional indexing: (name, schema_name, type)
                name = str(row[0]) if row[0] is not None else "unknown"
                schema = str(row[1]) if row[1] is not None else str(schema_name)
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
        # Add debug logging
        logger.info(
            f"Creating view {view_name} with table description: {table_description}, column descriptions: {column_descriptions}"
        )
        logger.info(f"columns_to_types: {columns_to_types}")
        logger.info(f"materialized: {materialized}")
        logger.info(f"replace: {replace}")
        logger.info(f"columns_to_types: {columns_to_types}")
        """
        Implements Doris CREATE VIEW and CREATE MATERIALIZED VIEW syntax.
        For materialized views, constructs the SQL to match Doris syntax and test expectations.
        """
        if replace:
            self.drop_view(view_name, ignore_if_not_exists=True, materialized=materialized)
        logger.info(
            f"Creating view {view_name} with table description: {table_description}, column descriptions: {column_descriptions}"
        )
        if not materialized:
            logger.info(f"Calling super().create_view for non-materialized view {view_name}")
            logger.info(f"Passing table_description: {table_description}")
            logger.info(f"Passing column_descriptions: {column_descriptions}")
            logger.info(f"Passing columns_to_types: {columns_to_types}")
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

        # Materialized view: delegate to separate method
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
        """
        Creates a Doris materialized view with the specified properties.
        Uses SQLGlot for supported parts and manual SQL generation for unsupported Doris-specific features.
        """
        logger.info(f"[Doris] _create_materialized_view called for view: {view_name}")
        logger.info(f"[Doris] materialized_properties: {materialized_properties}")
        logger.info(f"[Doris] create_kwargs: {create_kwargs}")
        logger.info(f"[Doris] view_properties: {view_properties}")
        logger.info(f"[Doris] table_description: {table_description}")
        logger.info(f"[Doris] query_or_df: {query_or_df}")
        # Convert query_or_df to proper format using base infrastructure
        query_or_df = self._native_df_to_pandas_df(query_or_df)

        # Handle DataFrame case
        import pandas as pd

        if isinstance(query_or_df, pd.DataFrame):
            values: t.List[t.Tuple[t.Any, ...]] = list(query_or_df.itertuples(index=False, name=None))
            columns_to_types = columns_to_types or self._columns_to_types(query_or_df)
            if not columns_to_types:
                raise SQLMeshError("columns_to_types must be provided for dataframes")
            query_or_df = self._values_to_sql(
                values,
                columns_to_types,
                batch_start=0,
                batch_end=len(values),
            )

        # Get source queries using base infrastructure
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, batch_size=0, target_table=view_name
        )
        if len(source_queries) != 1:
            raise SQLMeshError("Only one source query is supported for creating materialized views")

        logger.info(f"[Doris] column_descriptions: {column_descriptions}")
        logger.info(f"[Doris] columns_to_types: {columns_to_types}")

        # Build schema using base infrastructure
        schema: t.Union[exp.Table, exp.Schema] = exp.to_table(view_name)
        if columns_to_types:
            schema = self._build_schema_exp(
                exp.to_table(view_name), columns_to_types, column_descriptions, is_view=True
            )

        # Extract Doris-specific properties that aren't supported by SQLGlot
        doris_specific_clauses = []

        # Check for partitioned_by in create_kwargs first, then view_properties, then materialized_properties
        partitioned_by = (
            create_kwargs.get("partitioned_by")
            or (view_properties or {}).get("partitioned_by")
            or (materialized_properties or {}).get("partitioned_by")
        )

        # Check for partitioned_by_expr if partitioned_by is not provided
        partitioned_by_expr = None
        if not partitioned_by:
            partitioned_by_expr = create_kwargs.get("partitioned_by_expr") or (view_properties or {}).get(
                "partitioned_by_expr"
            )

        # Extract other Doris-specific properties from view_properties
        if view_properties is not None:
            # BUILD clause (order: 3)
            build = view_properties.get("build")
            if build:
                # Extract the string value from Literal expression
                if isinstance(build, exp.Literal):
                    build_value = build.this
                else:
                    build_value = str(build)
                doris_specific_clauses.append(f"BUILD {build_value}")

            # REFRESH clause (order: 4)
            refresh = view_properties.get("refresh")
            if refresh:
                # Extract the string value from Literal expression
                if isinstance(refresh, exp.Literal):
                    refresh_value = refresh.this
                else:
                    refresh_value = str(refresh)
                doris_specific_clauses.append(f"REFRESH {refresh_value}")

            # ON SCHEDULE clause (part of refresh)
            on_schedule = view_properties.get("on_schedule")
            if on_schedule:
                # Extract the string value from Literal expression
                if isinstance(on_schedule, exp.Literal):
                    on_schedule_value = on_schedule.this
                else:
                    on_schedule_value = str(on_schedule)
                doris_specific_clauses.append(f"ON SCHEDULE {on_schedule_value}")

            # KEY clauses (order: 5)
            unique_key = view_properties.get("unique_key")
            duplicate_key = view_properties.get("duplicate_key")
            if unique_key:
                # Handle both single column and list of columns
                if isinstance(unique_key, (list, tuple)):
                    key_cols = ", ".join(
                        [exp.to_column(k).sql(dialect=self.dialect, identify=True) for k in unique_key]
                    )
                else:
                    # Single column case
                    # Extract column name from the expression
                    if isinstance(unique_key, exp.Column):
                        if hasattr(unique_key, "this") and hasattr(unique_key.this, "this"):
                            column_name = str(unique_key.this.this)
                        else:
                            column_name = str(unique_key.this)
                    else:
                        column_name = str(unique_key)
                    key_cols = exp.to_column(column_name).sql(dialect=self.dialect, identify=True)
                doris_specific_clauses.append(f"KEY ({key_cols})")
            elif duplicate_key:
                # Handle both single column and list of columns
                if isinstance(duplicate_key, (list, tuple)):
                    key_cols = ", ".join(
                        [exp.to_column(k).sql(dialect=self.dialect, identify=True) for k in duplicate_key]
                    )
                else:
                    # Single column case
                    # Extract column name from the expression
                    if isinstance(duplicate_key, exp.Column):
                        if hasattr(duplicate_key, "this") and hasattr(duplicate_key.this, "this"):
                            column_name = str(duplicate_key.this.this)
                        else:
                            column_name = str(duplicate_key.this)
                    else:
                        column_name = str(duplicate_key)
                    key_cols = exp.to_column(column_name).sql(dialect=self.dialect, identify=True)
                doris_specific_clauses.append(f"DUPLICATE KEY ({key_cols})")

            # COMMENT clause (order: 6)
            if table_description:
                doris_specific_clauses.append(f"COMMENT '{self._truncate_table_comment(table_description)}'")

            # PROPERTIES clause (order: 9)
            properties = {}
            for k, v in view_properties.items():
                if k not in {
                    "build",
                    "refresh",
                    "on_schedule",
                    "distributed_by",
                    "partitioned_by",
                    "clustered_by",
                    "buckets",
                    "key",
                    "unique_key",
                    "duplicate_key",
                    "properties",
                }:
                    properties[k] = v

            # Merge with explicit 'properties' dict if present
            if "properties" in view_properties and isinstance(view_properties["properties"], dict):
                properties.update(view_properties["properties"])

            if properties:
                props = []
                for k, v in properties.items():
                    # Extract the string value from Literal expression
                    if isinstance(v, exp.Literal):
                        v_value = v.this
                    else:
                        v_value = str(v)
                    props.append(f"'{k}'='{v_value}'")
                doris_specific_clauses.append(f"PROPERTIES ({', '.join(props)})")
        else:
            # COMMENT clause (order: 6) - can be added even without view_properties
            if table_description:
                doris_specific_clauses.append(f"COMMENT '{self._truncate_table_comment(table_description)}'")

        # Handle partitioned_by from create_kwargs if not already handled
        if not partitioned_by and create_kwargs.get("partitioned_by"):
            partitioned_by = create_kwargs.get("partitioned_by")

        # Convert partitioned_by to list of expressions if it's not already
        typed_partitioned_by: t.Optional[t.List[exp.Expression]] = None
        if partitioned_by:
            if not isinstance(partitioned_by, list):
                if isinstance(partitioned_by, str):
                    typed_partitioned_by = [exp.to_column(partitioned_by)]
                else:
                    # Ensure it's an Expression
                    if isinstance(partitioned_by, exp.Expression):
                        typed_partitioned_by = [partitioned_by]
                    else:
                        typed_partitioned_by = [exp.to_column(str(partitioned_by))]
            else:
                typed_partitioned_by = [
                    exp.to_column(expr) if isinstance(expr, str) else expr for expr in partitioned_by
                ]

        # Build properties using the existing _build_table_properties_exp method for SQLGlot-supported parts only
        # We exclude Doris-specific properties that we handle manually
        table_properties = {}
        if view_properties is not None:
            for key, value in view_properties.items():
                # Only include properties that SQLGlot can handle and we don't handle manually
                if key not in {
                    "build",
                    "refresh",
                    "on_schedule",
                    "partitioned_by",
                    "unique_key",
                    "duplicate_key",
                    "distributed_by",
                    "properties",
                }:
                    # Convert to exp.Property similar to _build_table_properties_exp
                    if not isinstance(value, exp.Expression):
                        value = exp.Literal.string(str(value))
                    table_properties[key] = value

        # Build properties using the existing _build_table_properties_exp method
        properties = self._build_table_properties_exp(
            table_properties=table_properties,
            table_description=table_description,
            partitioned_by=typed_partitioned_by,
        )

        # Add materialized property
        if not properties:
            properties = exp.Properties(expressions=[])
        properties.append("expressions", exp.MaterializedProperty())

        # Generate the query SQL
        with source_queries[0] as query:
            query_sql = query.sql(dialect=self.dialect, identify=True)

            # Build the complete SQL following Doris grammar order
            view_name_sql = exp.to_table(view_name).sql(dialect=self.dialect, identify=True)

            # Start with CREATE MATERIALIZED VIEW
            clauses = [f"CREATE MATERIALIZED VIEW {view_name_sql}"]

            # Add column definitions if provided
            logger.info(
                f"[Doris] columns_to_types check: {columns_to_types is not None}, length: {len(columns_to_types) if columns_to_types else 0}"
            )
            # If columns_to_types is None, try to infer columns from the query or DataFrame
            if not columns_to_types:
                output_columns = []
                # Try to get output column names from the SQLGlot Select expression
                select_expr = getattr(source_queries[0], "expression", None)
                if select_expr and hasattr(select_expr, "named_selects"):
                    output_columns = [name for name, _ in select_expr.named_selects]
                else:
                    # Fallback: parse the SQL string for AS <alias>
                    import re

                    sql_str = query.sql(dialect=self.dialect, identify=True)
                    output_columns = re.findall(r'AS [`"]?(\w+)[`"]?', sql_str, re.IGNORECASE)
                if output_columns:
                    columns_to_types = {col: exp.DataType.build("STRING") for col in output_columns}
            if columns_to_types:
                col_defs = []
                for col in columns_to_types:
                    col_expr = exp.to_column(col)
                    col_sql = col_expr.sql(dialect=self.dialect, identify=True)
                    if column_descriptions and col in column_descriptions:
                        comment = column_descriptions[col]
                        col_defs.append(f"{col_sql} COMMENT '{comment}'")
                        logger.info(f"[Doris] Added column with comment: {col_sql} COMMENT '{comment}'")
                    else:
                        col_defs.append(col_sql)
                        logger.info(f"[Doris] Added column without comment: {col_sql}")
                clauses.append(f"({', '.join(col_defs)})")
                logger.info(f"[Doris] Added column definitions clause: ({', '.join(col_defs)})")
            else:
                logger.info(f"[Doris] No column definitions added - columns_to_types is falsy")

            # Extract partition clause from properties if it exists
            partition_clause = None
            logger.info(
                f"[Doris] Checking for partition clause - typed_partitioned_by: {typed_partitioned_by is not None}, partitioned_by_expr: {partitioned_by_expr is not None}"
            )

            # First try to extract from properties object
            if properties and properties.expressions:
                for prop in properties.expressions:
                    if isinstance(prop, exp.PartitionedByProperty):
                        logger.info(f"[Doris] Found partition property in properties: {type(prop).__name__}")
                        # Generate the partition clause manually since SQLGlot might not handle it correctly
                        if typed_partitioned_by:
                            part_cols = ", ".join(
                                [col.sql(dialect=self.dialect, identify=True) for col in typed_partitioned_by]
                            )
                            partition_clause = f"PARTITION BY ({part_cols})"
                        break

            # If no partition clause found in properties, generate it directly from the parameters
            if not partition_clause:
                if typed_partitioned_by:
                    logger.info(f"[Doris] Generating partition clause from partitioned_by")
                    part_cols = ", ".join(
                        [col.sql(dialect=self.dialect, identify=True) for col in typed_partitioned_by]
                    )
                    partition_clause = f"PARTITION BY ({part_cols})"
                    logger.info(f"[Doris] Generated partition clause: {partition_clause}")
                elif partitioned_by_expr:
                    logger.info(f"[Doris] Generating partition clause from partitioned_by_expr")
                    # Extract the string value from Literal expression
                    if isinstance(partitioned_by_expr, exp.Literal):
                        partition_expr_value = partitioned_by_expr.this
                    else:
                        partition_expr_value = str(partitioned_by_expr)
                    partition_clause = f"PARTITION BY ({partition_expr_value})"
                    logger.info(f"[Doris] Generated partition clause: {partition_clause}")

            # DISTRIBUTED BY clause (order: 8) - process outside view_properties block
            distributed_by = (view_properties or {}).get("distributed_by")
            distributed_clause = None
            if distributed_by:
                distributed_info = {}

                if isinstance(distributed_by, exp.Tuple):
                    # Parse the Tuple with EQ expressions to extract distributed_by info
                    for expr in distributed_by.expressions:
                        if isinstance(expr, exp.EQ) and hasattr(expr.this, "this"):
                            # Remove quotes from the key if present
                            key = str(expr.this.this).strip('"')
                            if isinstance(expr.expression, exp.Literal):
                                distributed_info[key] = expr.expression.this
                            elif isinstance(expr.expression, exp.Column):
                                # Preserve the original column expression to maintain quoted identifiers
                                distributed_info[key] = expr.expression
                            else:
                                distributed_info[key] = str(expr.expression)
                elif isinstance(distributed_by, dict):
                    # Handle as dictionary (legacy format)
                    distributed_info = distributed_by

                if distributed_info:
                    kind = distributed_info.get("kind")
                    expressions = distributed_info.get("expressions")
                    buckets = distributed_info.get("buckets")

                    if kind:
                        # Handle buckets - convert string to int if it's a numeric string
                        buckets_expr = None
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
                            if isinstance(expressions, exp.Column):
                                # Use the original column expression directly
                                expressions_list = [expressions]
                            elif isinstance(expressions, str):
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

                        # Use SQLGlot to generate the distributed clause
                        distributed_clause = prop.sql(dialect=self.dialect)
                        logger.info(f"[Doris] Generated distributed clause: {distributed_clause}")

                        # For Doris, we need to ensure column names are quoted with backticks, not double quotes
                        # SQLGlot might not be generating the correct quotes for Doris dialect
                        if expressions_list and len(expressions_list) == 1:
                            # Single column case
                            col_expr = expressions_list[0]
                            if isinstance(col_expr, exp.Column):
                                col_name = col_expr.sql(dialect=self.dialect, identify=True)
                                # Replace double quotes with backticks for Doris
                                if col_name.startswith('"') and col_name.endswith('"'):
                                    col_name = f"`{col_name[1:-1]}`"
                                    # Handle buckets expression
                                    buckets_str = (
                                        str(buckets)
                                        if isinstance(buckets, (int, str))
                                        else buckets.sql(dialect=self.dialect)
                                    )
                                    distributed_clause = f"DISTRIBUTED BY HASH ({col_name}) BUCKETS {buckets_str}"
                                    logger.info(
                                        f"[Doris] Fixed distributed clause with backticks: {distributed_clause}"
                                    )
                        elif expressions_list and len(expressions_list) > 1:
                            # Multiple columns case
                            col_names = []
                            for col_expr in expressions_list:
                                if isinstance(col_expr, exp.Column):
                                    col_name = col_expr.sql(dialect=self.dialect, identify=True)
                                    # Replace double quotes with backticks for Doris
                                    if col_name.startswith('"') and col_name.endswith('"'):
                                        col_name = f"`{col_name[1:-1]}`"
                                    col_names.append(col_name)
                            if col_names:
                                # Handle buckets expression
                                buckets_str = (
                                    str(buckets)
                                    if isinstance(buckets, (int, str))
                                    else buckets.sql(dialect=self.dialect)
                                )
                                distributed_clause = (
                                    f"DISTRIBUTED BY HASH ({', '.join(col_names)}) BUCKETS {buckets_str}"
                                )
                                logger.info(f"[Doris] Fixed distributed clause with backticks: {distributed_clause}")

            # Add Doris-specific clauses in the correct order
            # According to Doris grammar: BUILD, REFRESH, KEY, COMMENT, PARTITION BY, DISTRIBUTED BY, PROPERTIES
            doris_clauses_with_partition = []
            partition_inserted = False
            distributed_inserted = False

            for clause in doris_specific_clauses:
                doris_clauses_with_partition.append(clause)
                # Insert partition clause after COMMENT and before DISTRIBUTED BY
                if not partition_inserted and partition_clause and "DISTRIBUTED BY" in clause:
                    doris_clauses_with_partition.insert(-1, partition_clause)
                    partition_inserted = True
                # Insert distributed clause after partition clause and before PROPERTIES
                elif not distributed_inserted and distributed_clause and "PROPERTIES" in clause:
                    doris_clauses_with_partition.insert(-1, distributed_clause)
                    distributed_inserted = True

            # If partition clause wasn't inserted and we have one, add it before distributed clause
            if not partition_inserted and partition_clause:
                if distributed_clause:
                    # Find the position of distributed clause and insert partition before it
                    for i, clause in enumerate(doris_clauses_with_partition):
                        if "DISTRIBUTED BY" in clause:
                            doris_clauses_with_partition.insert(i, partition_clause)
                            partition_inserted = True
                            break
                    # If distributed clause not found, add partition at the end
                    if not partition_inserted:
                        doris_clauses_with_partition.append(partition_clause)

            # If distributed clause wasn't inserted and we have one, add it at the end
            if not distributed_inserted and distributed_clause:
                doris_clauses_with_partition.append(distributed_clause)

            clauses.extend(doris_clauses_with_partition)

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
        kwargs.pop("cascade", None)
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

    def _create_table_comment(self, table_name: TableName, table_comment: str, table_kind: str = "TABLE") -> None:
        table_sql = exp.to_table(table_name).sql(dialect=self.dialect, identify=True)

        self.execute(f'ALTER TABLE {table_sql} MODIFY COMMENT "{self._truncate_table_comment(table_comment)}"')

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

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        """
        Delete from table in Doris.
        """
        if where == exp.true():
            # Use TRUNCATE TABLE for full table deletion as Doris doesn't support DELETE FROM table WHERE TRUE
            return self.execute(exp.TruncateTable(expressions=[exp.to_table(table_name, dialect=self.dialect)]))

        return super().delete_from(table_name, where)

    # TODO: to delete
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
        """
        Create a table using a DDL statement.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: Mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: If True, will create "IF NOT EXISTS" table.
            table_description: Optional table description.
            column_descriptions: Optional mapping of column name to description.
        """
        logger.info(f"[Doris] _create_table_from_columns called for table: {table_name}")
        logger.info(f"[Doris] kwargs received: {kwargs}")

        # Extract table_properties from kwargs
        table_properties = kwargs.get("table_properties", {})
        logger.info(f"[Doris] table_properties from kwargs: {table_properties}")

        # Check if distributed_by is missing and we have a primary_key
        if not table_properties.get("distributed_by") and primary_key:
            logger.warning(
                f"[Doris] distributed_by not found in table_properties, but primary_key exists: {primary_key}"
            )
            # For Doris, if no distributed_by is specified but we have a primary_key,
            # we should use the primary_key as the distribution key
            if len(primary_key) == 1:
                table_properties["distributed_by"] = {"kind": "HASH", "expressions": primary_key[0], "buckets": 10}
                logger.info(
                    f"[Doris] Added fallback distributed_by using primary_key: {table_properties['distributed_by']}"
                )

        # Update kwargs with the modified table_properties
        kwargs["table_properties"] = table_properties

        # Call the parent implementation
        super()._create_table_from_columns(
            table_name,
            columns_to_types,
            primary_key=primary_key,
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
        logging.info(f"[Doris] _build_partitioned_by_exp called with kwargs: {kwargs}")
        partitioned_by_expr = kwargs.get("partitioned_by_expr")
        create_expressions = None

        def to_raw_sql(expr):
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
                    exp.Var(this=e.this, quoted=False) if isinstance(e, exp.Literal) else to_raw_sql(e)
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

        logger.info(f"table_properties_copy: {table_properties_copy}")
        logger.info(f"[Doris] _build_table_properties_exp called with kwargs: {kwargs}")

        # Handle unique_key - only handle Tuple expressions or single Column expressions
        unique_key = table_properties_copy.pop("unique_key", None)
        if unique_key is not None:
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

        # Handle duplicate_key - only handle Tuple expressions or single Column expressions
        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        if duplicate_key is not None:
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

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(this=exp.Literal.string(self._truncate_table_comment(table_description)))
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
                partitioned_by_expr = table_properties_copy.pop("partitioned_by_expr", None)
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
        logger.info(f"[Doris] distributed_by from table_properties_copy: {type(distributed_by)}")
        logger.info(f"[Doris] distributed_by value: {distributed_by}")
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
                logger.info(f"distributed_info: {distributed_info}")
                kind = distributed_info.get("kind")
                expressions = distributed_info.get("expressions")
                buckets = distributed_info.get("buckets")

                if kind:
                    # Handle buckets - convert string to int if it's a numeric string
                    buckets_expr = None
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

        # Add any remaining properties as generic exp.Property
        table_type = self._pop_creatable_type_from_properties(table_properties_copy)
        properties.extend([p for p in ensure_list(table_type) if p is not None])

        # Remove partitioning-related keys so they don't leak into PROPERTIES
        table_properties_copy.pop("partitioned_by_expr", None)
        table_properties_copy.pop("partitioned_by", None)

        # Only add generic properties if there are any left
        generic_properties = []
        for k, v in table_properties_copy.items():
            if not isinstance(v, exp.Expression):
                v = exp.Literal.string(str(v))
            generic_properties.append(exp.Property(this=exp.Literal.string(str(k)), value=v))
        if generic_properties:
            properties.extend(generic_properties)

        if properties:
            logger.info(f"[Doris] Final properties: {properties}")
            return exp.Properties(expressions=properties)
        logger.info(f"[Doris] No properties generated")
        return None

    def _get_temp_table(self, table: TableName, table_only: bool = False, quoted: bool = True) -> exp.Table:
        """
        Returns the name of the temp table that should be used for the given table name. Doris does not support table name begin with underscore.
        """
        table = t.cast(exp.Table, exp.to_table(table).copy())
        table.set("this", exp.to_identifier(f"temp_{table.name}_{random_id(short=True)}", quoted=quoted))

        if table_only:
            table.set("db", None)
            table.set("catalog", None)

        return table
