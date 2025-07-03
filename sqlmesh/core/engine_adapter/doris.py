from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(
    LogicalMergeMixin, PandasNativeFetchDFSupportMixin, NonTransactionalTruncateMixin, RowDiffMixin
):
    DIALECT = "mysql"  # Doris uses MySQL protocol for connection
    DEFAULT_BATCH_SIZE = 200
    SUPPORTS_TRANSACTIONS = False  # Doris doesn't support transactions
    SUPPORTS_INDEXES = True  # Doris supports various indexes (inverted, bloom filter, etc.)
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_REPLACE_TABLE = False  # Doris doesn't support REPLACE TABLE
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_VIEW_SCHEMA = False
    SUPPORTS_CREATE_DROP_CATALOG = False

    # Doris table model constants
    DEFAULT_TABLE_MODEL = "UNIQUE"  # Default to UNIQUE KEY model
    DEFAULT_UNIQUE_KEY_MERGE_ON_WRITE = True  # Enable merge-on-write by default

    @property
    def supports_indexes(self) -> bool:
        """Doris supports various types of indexes."""
        return self.SUPPORTS_INDEXES

    # Schema differ with Doris-specific data types
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # Doris data type defaults (using standard types that SQLGlot recognizes)
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(38, 9), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: [(65533,)],  # Doris VARCHAR max length
            exp.DataType.build("DATETIME", dialect=DIALECT).this: [(0,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(0,)],
        },
        # Doris type compatibility mapping
        compatible_types={
            exp.DataType.build("TINYINT", dialect=DIALECT): {
                exp.DataType.build("BOOLEAN", dialect=DIALECT),
            },
            exp.DataType.build("SMALLINT", dialect=DIALECT): {
                exp.DataType.build("TINYINT", dialect=DIALECT),
            },
            exp.DataType.build("INT", dialect=DIALECT): {
                exp.DataType.build("SMALLINT", dialect=DIALECT),
                exp.DataType.build("TINYINT", dialect=DIALECT),
            },
            exp.DataType.build("BIGINT", dialect=DIALECT): {
                exp.DataType.build("INT", dialect=DIALECT),
                exp.DataType.build("SMALLINT", dialect=DIALECT),
                exp.DataType.build("TINYINT", dialect=DIALECT),
            },
            exp.DataType.build("FLOAT", dialect=DIALECT): {
                exp.DataType.build("DOUBLE", dialect=DIALECT),
            },
            exp.DataType.build("DOUBLE", dialect=DIALECT): {
                exp.DataType.build("FLOAT", dialect=DIALECT),
            },
            exp.DataType.build("STRING", dialect=DIALECT): {  # Doris TEXT/STRING type
                exp.DataType.build("VARCHAR", dialect=DIALECT),
                exp.DataType.build("CHAR", dialect=DIALECT),
            },
            exp.DataType.build("DATETIME", dialect=DIALECT): {
                exp.DataType.build("DATE", dialect=DIALECT),
                exp.DataType.build("TIMESTAMP", dialect=DIALECT),
            },
        },
        types_with_unlimited_length={
            exp.DataType.build("STRING", dialect=DIALECT).this: {
                exp.DataType.build("STRING", dialect=DIALECT).this,
            },
        },
    )

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        return "internal"

    def create_index(
        self,
        table_name: TableName,
        index_name: str,
        columns: t.Tuple[str, ...],
        exists: bool = True,
        index_type: str = "INVERTED",
        properties: t.Optional[t.Dict[str, t.Any]] = None,
        comment: t.Optional[str] = None,
        use_create_index: bool = True,
    ) -> None:
        """
        Create index in Doris using CREATE INDEX (preferred) or ALTER TABLE ... ADD INDEX.
        Supports INVERTED and NGRAM_BF index types.
        Example:
            CREATE INDEX IF NOT EXISTS idx_name ON table_name (col) USING INVERTED PROPERTIES(...) COMMENT '...';
        """
        index_type = index_type.upper()
        if index_type not in ("INVERTED", "BLOOMFILTER", "NGRAM_BF"):
            raise ValueError(f"Doris only supports INVERTED, BLOOMFILTER, and NGRAM_BF index types, got: {index_type}")

        col_sql = ", ".join(f"`{col}`" for col in columns)
        prop_sql = ""
        if properties:
            prop_list = []
            for k, v in properties.items():
                if isinstance(v, bool):
                    v = str(v).lower()
                elif isinstance(v, (int, float)):
                    v = str(v)
                prop_list.append(f'"{k}" = "{v}"')
            prop_sql = f" PROPERTIES ({', '.join(prop_list)})"
        comment_sql = f" COMMENT '{comment}'" if comment else ""
        if use_create_index:
            # CREATE INDEX 语法
            exists_sql = " IF NOT EXISTS" if exists else ""
            using_sql = f" USING {index_type}" if index_type != "BLOOMFILTER" else " USING BLOOMFILTER"
            sql = (
                f"CREATE INDEX{exists_sql} {index_name} ON {table_name} ({col_sql}){using_sql}{prop_sql}{comment_sql};"
            )
        else:
            # 兼容老版本，使用 ALTER TABLE ... ADD INDEX
            sql = f"ALTER TABLE {table_name} ADD INDEX {index_name} ({col_sql}) USING {index_type}{prop_sql}{comment_sql};"
        self.execute(sql)

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expression],
    ) -> None:
        """Drop schema in Doris. Note: Doris doesn't support CASCADE clause."""
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema.
        Uses information_schema tables which are compatible with MySQL protocol.
        """
        query = (
            exp.select(
                exp.column("table_name").as_("name"),
                exp.column("table_schema").as_("schema_name"),
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
        return [
            DataObject(
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
        Create table like another table in Doris.
        Note: Doris CREATE TABLE LIKE has some limitations compared to MySQL.
        """
        # Doris supports CREATE TABLE LIKE but with some limitations
        # It may not copy all Doris-specific properties like distribution and partition
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

    def ping(self) -> None:
        """Ping the Doris connection to check if it's alive."""
        # Use a simple query to check connection
        try:
            self.fetchone("SELECT 1")
        except Exception as e:
            logger.warning(f"Doris connection ping failed: {e}")
            raise

    def _truncate_table_comment(self, comment: str) -> str:
        """Truncate table comment to fit Doris limits."""
        if self.MAX_TABLE_COMMENT_LENGTH and len(comment) > self.MAX_TABLE_COMMENT_LENGTH:
            return comment[: self.MAX_TABLE_COMMENT_LENGTH - 3] + "..."
        return comment

    def _truncate_column_comment(self, comment: str) -> str:
        """Truncate column comment to fit Doris limits."""
        if self.MAX_COLUMN_COMMENT_LENGTH and len(comment) > self.MAX_COLUMN_COMMENT_LENGTH:
            return comment[: self.MAX_COLUMN_COMMENT_LENGTH - 3] + "..."
        return comment

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[str] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Build Doris-specific table properties including table model and distribution."""
        properties: t.List[exp.Expression] = []

        # Copy table_properties to avoid modifying the original
        table_properties_copy = {k.upper(): v for k, v in (table_properties.copy() if table_properties else {}).items()}

        # Set default table model to UNIQUE if not specified
        table_model = table_properties_copy.pop("TABLE_MODEL", self.DEFAULT_TABLE_MODEL).upper()

        # Handle UNIQUE KEY model properties
        if table_model == "UNIQUE":
            # Enable merge-on-write by default for UNIQUE tables
            merge_on_write = table_properties_copy.pop(
                "ENABLE_UNIQUE_KEY_MERGE_ON_WRITE", str(self.DEFAULT_UNIQUE_KEY_MERGE_ON_WRITE).lower()
            )
            table_properties_copy["enable_unique_key_merge_on_write"] = merge_on_write

        # Add DISTRIBUTED BY if not specified - use first column by default
        distributed_by = table_properties_copy.pop("DISTRIBUTED_BY", None)
        if not distributed_by and columns_to_types:
            # Use the first column for distribution if not specified
            first_column = next(iter(columns_to_types.keys()))
            distributed_by = f"HASH({first_column})"

        if distributed_by:
            # DISTRIBUTED BY will be handled in the CREATE TABLE statement directly
            # Store it for later use
            table_properties_copy["_distributed_by"] = distributed_by

        # Set default number of buckets if not specified
        buckets = table_properties_copy.pop("BUCKETS", "10")
        table_properties_copy["buckets"] = buckets

        # Handle partitioning
        if partitioned_by:
            partition_expr = self._build_partitioned_by_exp(partitioned_by)
            if partition_expr:
                properties.append(partition_expr)

        # Add remaining properties as key-value pairs
        if table_properties_copy:
            for key, value in table_properties_copy.items():
                # Skip internal properties
                if not key.startswith("_"):
                    if isinstance(value, str):
                        value_expr = exp.Literal.string(value)
                    elif isinstance(value, (int, float)):
                        value_expr = exp.Literal.number(str(value))
                    elif isinstance(value, bool):
                        value_expr = exp.Literal.string(str(value).lower())
                    elif isinstance(value, exp.Expression):
                        value_expr = value
                    else:
                        value_expr = exp.Literal.string(str(value))

                    # Create property as key = "value"
                    properties.append(exp.Property(this=exp.Identifier(this=key), value=value_expr))

        # Add table description as comment
        if table_description:
            properties.append(
                exp.SchemaCommentProperty(this=exp.Literal.string(self._truncate_table_comment(table_description)))
            )

        return exp.Properties(expressions=properties) if properties else None

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        partition_type: str = "RANGE",
        **kwargs: t.Any,
    ) -> t.Optional[exp.Property]:
        """Build Doris PARTITION BY expression."""
        if not partitioned_by:
            return None

        partition_type = partition_type.upper()
        if partition_type not in ("RANGE", "LIST"):
            raise ValueError(f"Doris only supports RANGE and LIST partitioning, got: {partition_type}")

        return exp.Property(
            this=exp.Identifier(this="PARTITION BY"), value=exp.Func(this=partition_type, expressions=partitioned_by)
        )

    def _build_create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        """Override to add Doris-specific CREATE TABLE syntax."""

        # Extract table properties before calling super()
        table_properties = kwargs.get("table_properties", {})
        table_model = table_properties.get("TABLE_MODEL", self.DEFAULT_TABLE_MODEL).upper()

        # Build table schema with UNIQUE KEY
        if isinstance(table_name_or_schema, exp.Schema):
            schema = table_name_or_schema
            table_name = schema.this
        else:
            table_name = table_name_or_schema
            # Build schema from columns_to_types
            if columns_to_types:
                column_defs = []
                for col_name, col_type in columns_to_types.items():
                    column_defs.append(exp.ColumnDef(this=exp.to_identifier(col_name), kind=col_type))
                schema = exp.Schema(this=table_name, expressions=column_defs)
            else:
                schema = exp.Schema(this=table_name)

        # Add UNIQUE KEY constraint for UNIQUE model
        if table_model == "UNIQUE":
            unique_key_columns = table_properties.get("UNIQUE_KEY", None)
            if not unique_key_columns and columns_to_types:
                # Use first two columns as unique key by default
                column_names = list(columns_to_types.keys())
                unique_key_columns = column_names[: min(2, len(column_names))]

            if unique_key_columns:
                if isinstance(unique_key_columns, str):
                    unique_key_columns = [unique_key_columns]

                # Create UNIQUE KEY clause as a table constraint
                unique_key_constraint = f"UNIQUE KEY({', '.join(unique_key_columns)})"
                # Store this for later SQL generation
                schema.set("_unique_key", unique_key_constraint)

        # Get base CREATE TABLE expression with our modified schema
        create_exp = super()._build_create_table_exp(
            table_name_or_schema=schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            table_description=table_description,
            table_kind=table_kind,
            **kwargs,
        )

        # Store Doris-specific properties for SQL generation
        distributed_by = table_properties.get("DISTRIBUTED_BY")
        if not distributed_by and columns_to_types:
            first_column = next(iter(columns_to_types.keys()))
            distributed_by = f"HASH({first_column})"

        buckets = table_properties.get("BUCKETS", "10")

        # Store these for custom SQL generation
        create_exp.set("_doris_distributed_by", distributed_by)
        create_exp.set("_doris_buckets", buckets)
        create_exp.set("_doris_table_model", table_model)

        return create_exp

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        """Override SQL generation to handle Doris-specific syntax."""

        if isinstance(expression, exp.Create) and expression.kind == "TABLE":
            # Custom Doris CREATE TABLE generation
            return self._generate_doris_create_table_sql(expression, quote=quote)

        return super()._to_sql(expression, quote=quote, **kwargs)

    def _generate_doris_create_table_sql(self, create_exp: exp.Create, quote: bool = True) -> str:
        """Generate Doris-specific CREATE TABLE SQL."""
        parts = []

        # CREATE TABLE [IF NOT EXISTS] table_name
        create_clause = "CREATE TABLE"
        if getattr(create_exp, "exists", None):
            create_clause += " IF NOT EXISTS"

        table_name = create_exp.this.this if hasattr(create_exp.this, "this") else create_exp.this
        if hasattr(table_name, "sql"):
            table_name_sql = table_name.sql(dialect=self.dialect, identify=quote)
        else:
            table_name_sql = str(table_name)

        parts.append(f"{create_clause} {table_name_sql}")

        # Column definitions
        if hasattr(create_exp.this, "expressions") and create_exp.this.expressions:
            column_parts = []
            for expr in create_exp.this.expressions:
                if isinstance(expr, exp.ColumnDef):
                    col_sql = expr.sql(dialect=self.dialect, identify=quote)
                    column_parts.append(f"    {col_sql}")

            if column_parts:
                parts.append("(\n" + ",\n".join(column_parts) + "\n)")

        # UNIQUE KEY clause
        unique_key = None
        if hasattr(create_exp.this, "args") and create_exp.this.args.get("_unique_key"):
            unique_key = create_exp.this.args["_unique_key"]
        elif hasattr(create_exp, "args") and create_exp.args.get("_doris_table_model") == "UNIQUE":
            # Default UNIQUE KEY if not specified - use first two columns
            if hasattr(create_exp.this, "expressions") and create_exp.this.expressions:
                col_names = []
                for expr in create_exp.this.expressions:
                    if isinstance(expr, exp.ColumnDef) and len(col_names) < 2:
                        col_name = expr.this.name if hasattr(expr.this, "name") else str(expr.this)
                        col_names.append(col_name)
                if col_names:
                    unique_key = f"UNIQUE KEY({', '.join(col_names)})"

        if unique_key:
            parts.append(unique_key)

        # DISTRIBUTED BY clause
        distributed_by = None
        buckets = "10"

        if hasattr(create_exp, "args"):
            distributed_by = create_exp.args.get("_doris_distributed_by")
            buckets = create_exp.args.get("_doris_buckets", "10")

        if distributed_by:
            parts.append(f"DISTRIBUTED BY {distributed_by} BUCKETS {buckets}")

        # PROPERTIES clause
        properties_list = []

        # Add enable_unique_key_merge_on_write for UNIQUE tables
        table_model = None
        if hasattr(create_exp, "args"):
            table_model = create_exp.args.get("_doris_table_model")

        if table_model == "UNIQUE":
            properties_list.append('"enable_unique_key_merge_on_write" = "true"')

        # Add other properties from the original expression
        if hasattr(create_exp, "properties") and create_exp.properties:
            for prop in create_exp.properties.expressions:
                if isinstance(prop, exp.Property):
                    key = prop.this.name if hasattr(prop.this, "name") else str(prop.this)
                    if hasattr(prop, "value") and prop.value:
                        if isinstance(prop.value, exp.Literal):
                            value = prop.value.this
                            if prop.value.is_string:
                                value = f'"{value}"'
                        else:
                            value = f'"{str(prop.value)}"'
                        properties_list.append(f'"{key}" = {value}')
                elif isinstance(prop, exp.SchemaCommentProperty):
                    comment = prop.this.this if hasattr(prop.this, "this") else str(prop.this)
                    properties_list.append(f'"comment" = "{comment}"')

        if properties_list:
            properties_sql = "PROPERTIES (\n    " + ",\n    ".join(properties_list) + "\n)"
            parts.append(properties_sql)

        return "\n".join(parts)

    def create_view(
        self,
        view_name: t.Any,
        query_or_df: t.Any,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs,
    ) -> None:
        """
        Create a view or materialized view in Doris.
        """
        # 1. 生成 SELECT SQL
        if hasattr(query_or_df, "sql"):
            select_sql = query_or_df.sql(dialect=self.dialect)
        else:
            select_sql = str(query_or_df)

        # 2. 物化视图
        if materialized:
            sql = f"CREATE MATERIALIZED VIEW"
            if replace:
                sql += " IF NOT EXISTS"
            sql += f" {view_name}"

            # Doris 支持 COMMENT, BUILD, REFRESH
            if table_description:
                sql += f' COMMENT "{table_description}"'
            if materialized_properties:
                if "build" in materialized_properties:
                    sql += f" BUILD {materialized_properties['build']}"
                if "refresh" in materialized_properties:
                    sql += f" REFRESH {materialized_properties['refresh']}"
                # 其他属性可通过 PROPERTIES 传递
                props = materialized_properties.get("properties")
                if props:
                    prop_str = ", ".join(f'"{k}" = "{v}"' for k, v in props.items())
                    sql += f" PROPERTIES ({prop_str})"
            sql += f" AS {select_sql}"
        else:
            # 3. 普通视图
            if replace:
                sql = f"CREATE OR REPLACE VIEW {view_name} AS {select_sql}"
            else:
                sql = f"CREATE VIEW {view_name} AS {select_sql}"

        self.execute(sql)

    def drop_view(
        self,
        view_name: t.Any,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs,
    ) -> None:
        """
        Drop a view or materialized view in Doris.
        """
        if materialized:
            sql = f"DROP MATERIALIZED VIEW {'IF EXISTS ' if ignore_if_not_exists else ''}{view_name}"
        else:
            sql = f"DROP VIEW {'IF EXISTS ' if ignore_if_not_exists else ''}{view_name}"
        self.execute(sql)
