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
from sqlmesh.utils import columns_to_types_all_known
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
        """
        Implements Doris CREATE VIEW and CREATE MATERIALIZED VIEW syntax.
        For materialized views, constructs the SQL to match Doris syntax and test expectations.
        """
        if replace:
            self.drop_view(view_name, ignore_if_not_exists=True, materialized=materialized)
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

        # Build schema using base infrastructure
        schema: t.Union[exp.Table, exp.Schema] = exp.to_table(view_name)
        if columns_to_types:
            schema = self._build_schema_exp(
                exp.to_table(view_name), columns_to_types, column_descriptions, is_view=True
            )

        # Extract Doris-specific properties that aren't supported by SQLGlot
        doris_specific_clauses = []

        # Check for partitioned_by in create_kwargs first, then materialized_properties
        partitioned_by = create_kwargs.get("partitioned_by") or (materialized_properties or {}).get("partitioned_by")
        partitioned_by_expr = create_kwargs.get("partitioned_by_expr") or (materialized_properties or {}).get(
            "partitioned_by_expr"
        )

        # Extract other Doris-specific properties from materialized_properties
        if materialized_properties:
            # BUILD clause (order: 3)
            build = materialized_properties.get("build")
            if build:
                doris_specific_clauses.append(f"BUILD {build}")

            # REFRESH clause (order: 4)
            refresh = materialized_properties.get("refresh")
            if refresh:
                doris_specific_clauses.append(f"REFRESH {refresh}")

            # ON SCHEDULE clause (part of refresh)
            on_schedule = materialized_properties.get("on_schedule")
            if on_schedule:
                doris_specific_clauses.append(f"ON SCHEDULE {on_schedule}")

            # KEY clauses (order: 5)
            unique_key = materialized_properties.get("unique_key")
            duplicate_key = materialized_properties.get("duplicate_key")
            if unique_key:
                key_cols = ", ".join([exp.to_column(k).sql(dialect=self.dialect, identify=True) for k in unique_key])
                doris_specific_clauses.append(f"KEY ({key_cols})")
            elif duplicate_key:
                key_cols = ", ".join([exp.to_column(k).sql(dialect=self.dialect, identify=True) for k in duplicate_key])
                doris_specific_clauses.append(f"DUPLICATE KEY ({key_cols})")

            # COMMENT clause (order: 6)
            if table_description:
                doris_specific_clauses.append(f"COMMENT '{self._truncate_table_comment(table_description)}'")

            # PARTITION BY clause (order: 7) - handled by _build_partitioned_by_exp method
            # We'll handle this separately to use the proper method

            # DISTRIBUTED BY clause (order: 8)
            distributed_by = materialized_properties.get("distributed_by")
            if distributed_by and isinstance(distributed_by, dict):
                kind = distributed_by.get("kind")
                exprs = distributed_by.get("expressions")
                buckets = distributed_by.get("buckets")
                if kind and exprs:
                    distributed_sql = f"DISTRIBUTED BY {kind} ({', '.join(exprs)})"
                    if buckets is not None:
                        distributed_sql += f" BUCKETS {buckets}"
                    doris_specific_clauses.append(distributed_sql)

            # PROPERTIES clause (order: 9)
            properties = {}
            for k, v in materialized_properties.items():
                if k not in {
                    "build",
                    "refresh",
                    "on_schedule",
                    "distributed_by",
                    "partitioned_by",
                    "partitioned_by_expr",
                    "clustered_by",
                    "buckets",
                    "key",
                    "unique_key",
                    "duplicate_key",
                    "properties",
                }:
                    properties[k] = v

            # Merge with explicit 'properties' dict if present
            if "properties" in materialized_properties and isinstance(materialized_properties["properties"], dict):
                properties.update(materialized_properties["properties"])

            if properties:
                props = [f"'{k}'='{v}'" for k, v in properties.items()]
                doris_specific_clauses.append(f"PROPERTIES ({', '.join(props)})")

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
        if materialized_properties:
            for key, value in materialized_properties.items():
                # Only include properties that SQLGlot can handle and we don't handle manually
                if key not in {
                    "build",
                    "refresh",
                    "on_schedule",
                    "partitioned_by",
                    "partitioned_by_expr",
                    "unique_key",
                    "duplicate_key",
                    "distributed_by",
                    "properties",
                }:
                    # Convert to exp.Property similar to _build_table_properties_exp
                    if not isinstance(value, exp.Expression):
                        value = exp.Literal.string(str(value))
                    table_properties[key] = value

        # Note: partitioned_by_expr should not be added to table_properties
        # as it's handled separately in the partition clause generation

        # Only pass partitioned_by_expr if it's not already in table_properties
        kwargs_for_properties = {
            "table_properties": table_properties,
            "table_description": table_description,
            "partitioned_by": typed_partitioned_by,
        }
        if partitioned_by_expr and "partitioned_by_expr" not in table_properties:
            kwargs_for_properties["partitioned_by_expr"] = partitioned_by_expr

        properties = self._build_table_properties_exp(**kwargs_for_properties)

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
            if columns_to_types:
                col_defs = []
                for col, dtype in columns_to_types.items():
                    col_expr = exp.to_column(col)
                    col_sql = col_expr.sql(dialect=self.dialect, identify=True)
                    if column_descriptions and col in column_descriptions:
                        comment = column_descriptions[col]
                        col_defs.append(f"{col_sql} COMMENT '{comment}'")
                    else:
                        col_defs.append(col_sql)
                clauses.append(f"({', '.join(col_defs)})")

            # Extract partition clause from properties if it exists
            partition_clause = None
            if properties and properties.expressions:
                for prop in properties.expressions:
                    if isinstance(prop, (exp.PartitionedByProperty, exp.PartitionByRangeProperty)):
                        # Generate the partition clause manually since SQLGlot might not handle it correctly
                        if typed_partitioned_by:
                            if isinstance(prop, exp.PartitionByRangeProperty):
                                part_cols = ", ".join(
                                    [col.sql(dialect=self.dialect, identify=True) for col in typed_partitioned_by]
                                )
                                partition_clause = f"PARTITION BY RANGE ({part_cols})"
                                if partitioned_by_expr:
                                    partition_clause += f" ({partitioned_by_expr})"
                            else:
                                part_cols = ", ".join(
                                    [col.sql(dialect=self.dialect, identify=True) for col in typed_partitioned_by]
                                )
                                partition_clause = f"PARTITION BY ({part_cols})"
                        break

            # Add Doris-specific clauses in the correct order, inserting partition clause at the right position
            # According to Doris grammar: BUILD, REFRESH, KEY, COMMENT, PARTITION BY, DISTRIBUTED BY, PROPERTIES
            doris_clauses_with_partition = []
            partition_inserted = False

            for clause in doris_specific_clauses:
                doris_clauses_with_partition.append(clause)
                # Insert partition clause after COMMENT and before DISTRIBUTED BY
                if not partition_inserted and partition_clause and "DISTRIBUTED BY" in clause:
                    doris_clauses_with_partition.insert(-1, partition_clause)
                    partition_inserted = True

            # If partition clause wasn't inserted and we have one, add it at the end of Doris clauses
            if not partition_inserted and partition_clause:
                doris_clauses_with_partition.append(partition_clause)

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
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        table = exp.to_table(table_name)

        if not columns_to_types_all_known(columns_to_types):
            # It is ok if the columns types are not known if the table already exists and IF NOT EXISTS is set
            if exists and self.table_exists(table_name):
                return
            raise SQLMeshError(
                "Cannot create a table without knowing the column types. "
                "Try casting the columns to an expected type or defining the columns in the model metadata. "
                f"Columns to types: {columns_to_types}"
            )

        # Doris doesn't support primary key. We use Unique Key Model if primary key is provided.
        primary_key_expression: t.List[exp.PrimaryKey] = []
        table_properties = kwargs.get("table_properties")
        if table_properties is None:
            table_properties = {}
            kwargs["table_properties"] = table_properties
        if primary_key and "unique_key" not in table_properties:
            table_properties["unique_key"] = primary_key

        schema = self._build_schema_exp(
            table,
            columns_to_types,
            column_descriptions,
            primary_key_expression,
        )

        self._create_table(
            schema,
            None,
            exists=exists,
            columns_to_types=columns_to_types,
            table_description=table_description,
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

        if partitioned_by_expr:
            if isinstance(partitioned_by_expr, list):
                create_expressions = []
                for expr in partitioned_by_expr:
                    if isinstance(expr, str):
                        create_expressions.append(exp.Var(this=expr))
                    else:
                        create_expressions.append(expr)
            else:
                if isinstance(partitioned_by_expr, str):
                    create_expressions = [exp.Var(this=str(partitioned_by_expr))]
                else:
                    create_expressions = [partitioned_by_expr]

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
        """Creates a SQLGlot table properties expression for ddl, including Doris-specific materialized view clauses."""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(this=exp.Literal.string(self._truncate_table_comment(table_description)))
            )

        table_properties_copy = dict(table_properties) if table_properties else {}

        # Existing logic for unique_key, duplicate_key, distributed_by
        unique_key = table_properties_copy.pop("unique_key", None)
        if unique_key is not None:
            properties.append(exp.UniqueKeyProperty(expressions=[exp.to_column(k) for k in unique_key]))

        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        if duplicate_key is not None:
            properties.append(exp.DuplicateKeyProperty(expressions=[exp.to_column(k) for k in duplicate_key]))

        # Handle partitioning
        add_partition = True
        partitioned_by_expr = table_properties_copy.pop("partitioned_by_expr", None)
        if partitioned_by:
            # check if partitioned_by columns are in unique_key, if not, skip PARTITION BY.
            if unique_key is not None:
                key_cols_set = set([str(c).strip() for c in unique_key])
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
                partition_expr = self._build_partitioned_by_exp(partitioned_by, partitioned_by_expr=partitioned_by_expr)
                if partition_expr:
                    properties.append(partition_expr)

        # Handle distributed_by property from table_properties only
        distributed_by = table_properties_copy.pop("distributed_by", None)
        if distributed_by and isinstance(distributed_by, dict):
            expressions = distributed_by.get("expressions")
            kind = distributed_by.get("kind")
            buckets = distributed_by.get("buckets")
            prop = exp.DistributedByProperty(
                kind=exp.Var(this=kind),
                expressions=[exp.to_column(e) for e in expressions] if expressions else None,
                buckets=exp.Literal.number(buckets)
                if isinstance(buckets, int)
                else exp.Var(this="AUTO")
                if buckets == "AUTO"
                else None,
                order=None,
            )
            properties.append(prop)

        # Add any remaining properties as generic exp.Property
        table_type = self._pop_creatable_type_from_properties(table_properties_copy)
        properties.extend([p for p in ensure_list(table_type) if p is not None])

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
