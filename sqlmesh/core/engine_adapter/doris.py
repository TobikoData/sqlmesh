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

        # Materialized view: implement Doris-specific SQL
        # Build column list with comments if provided
        schema = exp.to_table(view_name)
        col_defs = []
        if columns_to_types:
            for col, dtype in columns_to_types.items():
                col_expr = exp.to_column(col)
                col_sql = col_expr.sql(dialect=self.dialect, identify=True)
                if column_descriptions and col in column_descriptions:
                    comment = column_descriptions[col]
                    col_defs.append(f"{col_sql} COMMENT '{comment}'")
                else:
                    col_defs.append(f"{col_sql}")
            columns_sql = f"({', '.join(col_defs)})"
        else:
            columns_sql = ""

        # Doris-specific materialized view clauses
        build = None
        refresh = None
        on_schedule = None
        distributed_by = None
        buckets = None
        properties = {}
        key_clause = None
        partition_by_clause = None
        comment_sql = ""
        if materialized_properties:
            build = materialized_properties.get("build")
            refresh = materialized_properties.get("refresh")
            on_schedule = materialized_properties.get("on_schedule")
            distributed_by = materialized_properties.get("distributed_by")
            unique_key = materialized_properties.get("unique_key")
            duplicate_key = materialized_properties.get("duplicate_key")
            if unique_key:
                key_cols = ", ".join(
                    [exp.to_column(k).sql(dialect=self.dialect, identify=True) for k in unique_key]
                )
                key_clause = f"KEY ({key_cols})"
            elif duplicate_key:
                key_cols = ", ".join(
                    [
                        exp.to_column(k).sql(dialect=self.dialect, identify=True)
                        for k in duplicate_key
                    ]
                )
                key_clause = f"DUPLICATE KEY ({key_cols})"
            partitioned_by = materialized_properties.get("partitioned_by")
            if partitioned_by:
                if isinstance(partitioned_by, (list, tuple)):
                    part_cols = ", ".join(
                        [
                            exp.to_column(k).sql(dialect=self.dialect, identify=True)
                            for k in partitioned_by
                        ]
                    )
                    partition_by_clause = f"PARTITION BY ({part_cols})"
                else:
                    # If it's a string or single column
                    part_col = exp.to_column(partitioned_by).sql(
                        dialect=self.dialect, identify=True
                    )
                    partition_by_clause = f"PARTITION BY ({part_col})"
            # Collect all extra keys for PROPERTIES
            for k, v in materialized_properties.items():
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
            if "properties" in materialized_properties and isinstance(
                materialized_properties["properties"], dict
            ):
                properties.update(materialized_properties["properties"])
        # Compose distributed by clause
        distributed_sql = ""
        if distributed_by and isinstance(distributed_by, dict):
            kind = distributed_by.get("kind")
            exprs = distributed_by.get("expressions")
            buckets = distributed_by.get("buckets")
            if kind and exprs:
                distributed_sql = f"DISTRIBUTED BY {kind} ({', '.join(exprs)})"
                if buckets is not None:
                    distributed_sql += f" BUCKETS {buckets}"
        # Compose properties clause
        properties_sql = ""
        if properties:
            props = [f"'{k}' = '{v}'" for k, v in properties.items()]
            properties_sql = f"PROPERTIES ({', '.join(props)})"
        # Compose BUILD, REFRESH, ON SCHEDULE
        build_sql = f"BUILD {build}" if build else ""
        refresh_sql = f"REFRESH {refresh}" if refresh else ""
        on_schedule_sql = f"ON SCHEDULE {on_schedule}" if on_schedule else ""
        # Compose COMMENT clause for table comment
        if table_description:
            comment_sql = f"COMMENT '{self._truncate_table_comment(table_description)}'"
        # Compose the full CREATE MATERIALIZED VIEW statement (order per Doris docs)
        view_name_sql = exp.to_table(view_name).sql(dialect=self.dialect, identify=True)
        clauses = [
            f"CREATE MATERIALIZED VIEW {view_name_sql}",
            columns_sql if columns_sql else None,
            build_sql if build_sql else None,
            refresh_sql if refresh_sql else None,
            on_schedule_sql if on_schedule_sql else None,
            key_clause if key_clause else None,
            comment_sql if comment_sql else None,
            partition_by_clause if partition_by_clause else None,
            distributed_sql if distributed_sql else None,
            properties_sql if properties_sql else None,
        ]
        # Remove empty
        clauses = [c for c in clauses if c]
        # The AS clause
        # Render the query as SQL, quoted, to match test expectation
        if hasattr(query_or_df, "sql"):
            query_sql = query_or_df.sql(dialect=self.dialect, identify=True)
        else:
            query_sql = str(query_or_df)
        full_sql = " ".join(clauses) + f" AS {query_sql}"
        self.execute(full_sql)

        # No need to register comments separately for materialized views in Doris

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

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        """
        Delete from table in Doris.
        """
        if where == exp.true():
            # Use TRUNCATE TABLE for full table deletion as Doris doesn't support DELETE FROM table WHERE TRUE
            return self.execute(
                exp.TruncateTable(expressions=[exp.to_table(table_name, dialect=self.dialect)])
            )

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
        return exp.PartitionByRangeProperty(
            partition_expressions=partitioned_by,
            create_expressions=exp.Literal(
                this=kwargs.get("partition_expression", ""), is_string=False
            ),
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
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        table_properties_copy = dict(table_properties) if table_properties else {}

        # Doris-specific materialized view clauses
        build = table_properties_copy.pop("build", None)
        if build:
            properties.append(exp.Property(this=exp.Var(this="BUILD"), value=exp.Var(this=build)))

        refresh = table_properties_copy.pop("refresh", None)
        if refresh:
            properties.append(
                exp.Property(this=exp.Var(this="REFRESH"), value=exp.Var(this=refresh))
            )

        on_schedule = table_properties_copy.pop("on_schedule", None)
        if on_schedule:
            properties.append(
                exp.Property(
                    this=exp.Var(this="ON SCHEDULE"), value=exp.Literal.string(on_schedule)
                )
            )

        key = table_properties_copy.pop("key", None)
        if key:
            properties.append(
                exp.Property(
                    this=exp.Var(this="KEY"),
                    value=exp.Tuple(expressions=[exp.to_column(k) for k in key]),
                )
            )

        # Existing logic for unique_key, duplicate_key, distributed_by
        unique_key = table_properties_copy.pop("unique_key", None)
        if unique_key is not None:
            properties.append(
                exp.UniqueKeyProperty(expressions=[exp.to_column(k) for k in unique_key])
            )

        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        if duplicate_key is not None:
            properties.append(
                exp.DuplicateKeyProperty(expressions=[exp.to_column(k) for k in duplicate_key])
            )

        # Handle partitioning
        add_partition = True
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
                partition_expr = self._build_partitioned_by_exp(partitioned_by)
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
