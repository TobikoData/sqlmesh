from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import to_schema
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
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    EngineAdapterWithIndexSupport,
):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 200
    SUPPORTS_TRANSACTIONS = False  # Doris doesn't support transactions
    SUPPORTS_INDEXES = True  # Doris supports various indexes (inverted, bloom filter, etc.)
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_REPLACE_TABLE = False  # Doris doesn't support REPLACE TABLE
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_VIEW_SCHEMA = False
    SUPPORTS_CREATE_DROP_CATALOG = False

    # Doris table model constants
    DEFAULT_TABLE_MODEL = "DUPLICATE"  # Default to DUPLICATE model

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
            raise ValueError(
                f"Doris only supports INVERTED, BLOOMFILTER, and NGRAM_BF index types, got: {index_type}"
            )

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
            exists_sql = " IF NOT EXISTS" if exists else ""
            using_sql = (
                f" USING {index_type}" if index_type != "BLOOMFILTER" else " USING BLOOMFILTER"
            )
            sql = f"CREATE INDEX{exists_sql} {index_name} ON {table_name} ({col_sql}){using_sql}{prop_sql}{comment_sql};"
        else:
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

    def create_table(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a DDL statement with Doris-specific optimizations.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: A mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
                Available table properties:
                - TABLE_MODEL: "DUPLICATE" (default) or "UNIQUE"
                - KEY_COLS: list of key columns
                - DISTRIBUTED_BY: distribution method (default: HASH on first column)
                - BUCKETS: number of buckets (default: 10)
        """
        super().create_table(
            table_name,
            columns_to_types,
            primary_key=primary_key,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def ctas(
        self,
        table_name: TableName,
        query_or_df: "QueryOrDF",
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a CTAS statement with Doris-specific optimizations.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            query_or_df: The SQL query to run or a dataframe for the CTAS.
            columns_to_types: A mapping between the column name and its data type. Required if using a DataFrame.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
                Available table properties:
                - TABLE_MODEL: "DUPLICATE" (default) or "UNIQUE"
                - KEY_COLS: list of key columns
                - DISTRIBUTED_BY: distribution method (default: HASH on first column)
                - BUCKETS: number of buckets (default: 10)
        """
        super().ctas(
            table_name,
            query_or_df,
            columns_to_types=columns_to_types,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

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

    def _extract_value_from_property(self, value: t.Any) -> t.Optional[str]:
        """Extract string value from SQLGlot expression or raw value."""
        if value is None:
            return None
        # Handle SQLGlot Array expression, e.g., ARRAY('recordid')
        if hasattr(value, "args") and "expressions" in value.args:
            elements = []
            for expr in value.args["expressions"]:
                elem_val = self._extract_value_from_property(expr)
                if elem_val is not None:
                    elements.append(elem_val)
            if not elements:
                return None
            return ", ".join(elements)
        if hasattr(value, "this"):
            # SQLGlot Literal expression
            return str(value.this)
        if hasattr(value, "name"):
            # SQLGlot Identifier expression
            return str(value.name)
        if hasattr(value, "expressions"):
            elements = []
            for expr in value.expressions:
                elem_val = self._extract_value_from_property(expr)
                if elem_val is not None:
                    elements.append(elem_val)
            if not elements:
                return None
            return f"[{', '.join(elements)}]"
        # Raw string or other value
        return str(value).strip("'\"")

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Build Doris-specific table properties including table model and distribution.
        Args:
            key_cols: List of key column names for Doris table (UNIQUE, DUPLICATE).
            table_model: Doris table model type (UNIQUE, DUPLICATE).
        """
        properties: t.List[exp.Expression] = []

        # Copy table_properties to avoid modifying the original
        table_properties_copy = {k.upper(): v for k, v in (table_properties or {}).copy().items()}

        # Determine table model - default to DUPLICATE
        table_model = "DUPLICATE"
        if "TABLE_MODEL" in table_properties_copy:
            table_model_value = table_properties_copy.pop("TABLE_MODEL")
            table_model_str = self._extract_value_from_property(table_model_value)
            if table_model_str is not None:
                table_model = table_model_str.upper()

        # Validate table model
        if table_model not in ("DUPLICATE", "UNIQUE"):
            raise ValueError(f"Invalid table model '{table_model}'. Must be DUPLICATE or UNIQUE.")

        properties.append(
            exp.Property(
                this=exp.Identifier(this="_doris_table_model"),
                expression=exp.Literal.string(table_model),
            )
        )

        # Handle DISTRIBUTED BY - extract and store for later use
        distributed_by: str = ""
        if "DISTRIBUTED_BY" in table_properties_copy:
            distributed_by_value = table_properties_copy.pop("DISTRIBUTED_BY")
            distributed_by_str = self._extract_value_from_property(distributed_by_value)
            if distributed_by_str is not None:
                distributed_by = distributed_by_str
        elif columns_to_types:
            first_column = next(iter(columns_to_types.keys()))
            distributed_by = f"HASH({first_column})"

        # Handle BUCKETS
        buckets: str = "10"
        if "BUCKETS" in table_properties_copy:
            buckets_value = table_properties_copy.pop("BUCKETS")
            buckets_str = self._extract_value_from_property(buckets_value)
            if buckets_str is not None:
                buckets = buckets_str

        # Store extracted values for later SQL generation
        if distributed_by:
            properties.append(
                exp.Property(
                    this=exp.Identifier(this="_doris_distributed_by"),
                    expression=exp.Literal.string(distributed_by),
                )
            )

        if buckets:
            properties.append(
                exp.Property(
                    this=exp.Identifier(this="_doris_buckets"),
                    expression=exp.Literal.string(buckets),
                )
            )

        # Handle auto_partition flag
        auto_partition = "false"
        if "AUTO_PARTITION" in table_properties_copy:
            auto_partition_value = table_properties_copy.pop("AUTO_PARTITION")
            auto_partition_str = self._extract_value_from_property(auto_partition_value)
            if auto_partition_str is not None:
                auto_partition = auto_partition_str.lower()

        properties.append(
            exp.Property(
                this=exp.Identifier(this="_doris_auto_partition"),
                expression=exp.Literal.string(auto_partition),
            )
        )

        # Handle partition_function
        partition_function = ""
        if "PARTITION_FUNCTION" in table_properties_copy:
            partition_function_value = table_properties_copy.pop("PARTITION_FUNCTION")
            partition_function_str = self._extract_value_from_property(partition_function_value)
            if partition_function_str is not None:
                partition_function = partition_function_str

        if partition_function:
            properties.append(
                exp.Property(
                    this=exp.Identifier(this="_doris_partition_function"),
                    expression=exp.Literal.string(partition_function),
                )
            )

        partitioned_by_def: str = ""
        if "PARTITIONED_BY_DEF" in table_properties_copy:
            partitioned_by_def_value = table_properties_copy.pop("PARTITIONED_BY_DEF")
            partitioned_by_def_str = self._extract_value_from_property(partitioned_by_def_value)
            if partitioned_by_def_str is not None:
                partitioned_by_def = partitioned_by_def_str
            properties.append(
                exp.Property(
                    this=exp.Identifier(this="_doris_partition_def"),
                    expression=exp.Literal.string(partitioned_by_def),
                )
            )

        # Add key_cols as a property for later SQL generation
        key_cols: str = ""
        if "KEY_COLS" in table_properties_copy:
            key_cols_value = table_properties_copy.pop("KEY_COLS", None)
            key_cols_str = (
                self._extract_value_from_property(key_cols_value)
                if key_cols_value is not None
                else None
            )
            if key_cols_str is not None:
                key_cols = key_cols_str
            if key_cols:
                properties.append(
                    exp.Property(
                        this=exp.Identifier(this="_doris_key_cols"),
                        expression=exp.Literal.string(key_cols),
                    )
                )

        # Handle partitioning
        add_partition = True
        if partitioned_by:
            # 检查 UNIQUE KEY 模型下 partitioned_by 列是否都在 key_cols
            if table_model == "UNIQUE":
                key_cols_set = set()
                if key_cols:
                    if isinstance(key_cols, str):
                        key_cols_set = set([c.strip() for c in key_cols.split(",") if c.strip()])
                    elif isinstance(key_cols, (list, tuple)):
                        key_cols_set = set([str(c).strip() for c in key_cols])
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

        # Add remaining properties as key-value pairs
        for key, value in table_properties_copy.items():
            if not key.startswith("_"):
                value_str = self._extract_value_from_property(value)
                properties.append(
                    exp.Property(
                        this=exp.Identifier(this=key.lower()),
                        expression=exp.Literal.string(value_str),
                    )
                )

        # Add table description as comment
        if table_description:
            properties.append(
                exp.Property(
                    this=exp.Identifier(this="_doris_table_comment"),
                    expression=exp.Literal.string(self._truncate_table_comment(table_description)),
                )
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
            raise ValueError(
                f"Doris only supports RANGE and LIST partitioning, got: {partition_type}"
            )

        return exp.Property(
            this=exp.Identifier(this="PARTITION BY"),
            value=exp.Func(this=partition_type, expressions=partitioned_by),
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

        # Build table schema
        if isinstance(table_name_or_schema, exp.Schema):
            schema = table_name_or_schema
            table_name = schema.this
        else:
            table_name = table_name_or_schema
            # Build schema from columns_to_types
            if columns_to_types:
                column_defs = []
                for col_name, col_type in columns_to_types.items():
                    column_defs.append(
                        exp.ColumnDef(this=exp.to_identifier(col_name), kind=col_type)
                    )
                schema = exp.Schema(this=table_name, expressions=column_defs)
            else:
                schema = exp.Schema(this=table_name)

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

        return create_exp

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        """Override SQL generation to handle Doris-specific syntax."""

        if isinstance(expression, exp.Create) and expression.kind == "TABLE":
            # Custom Doris CREATE TABLE generation
            return self._generate_doris_create_table_sql(expression, quote=quote)

        return super()._to_sql(expression, quote=quote, **kwargs)

    def _generate_doris_create_table_sql(self, create_exp: exp.Create, quote: bool = True) -> str:
        """Generate Doris-specific CREATE TABLE SQL supporting both DDL and CTAS.
        Uses key_cols and table_model for KEY clause generation.
        """
        parts = []

        create_clause = "CREATE TABLE"
        if getattr(create_exp, "exists", None):
            create_clause += " IF NOT EXISTS"

        table_name = create_exp.this.this if hasattr(create_exp.this, "this") else create_exp.this
        if hasattr(table_name, "sql"):
            table_name_sql = table_name.sql(dialect=self.dialect, identify=quote)
        else:
            table_name_sql = str(table_name)

        parts.append(f"{create_clause} {table_name_sql}")

        is_ctas = hasattr(create_exp, "expression") and create_exp.expression is not None

        if not is_ctas and hasattr(create_exp.this, "expressions") and create_exp.this.expressions:
            column_parts = []
            for expr in create_exp.this.expressions:
                if isinstance(expr, exp.ColumnDef):
                    col_sql = expr.sql(dialect=self.dialect, identify=quote)
                    column_parts.append(f"    {col_sql}")
            if column_parts:
                parts.append("(\n" + ",\n".join(column_parts) + "\n)")

        # Extract properties from the create expression
        table_model = None
        distributed_by = None
        buckets = "10"
        key_cols = None
        table_comment = None
        properties_list = []
        partition_by_clause = None
        partitioned_by_def: str = ""
        auto_partition = "false"
        partition_function = ""

        if hasattr(create_exp, "args") and create_exp.args.get("properties"):
            properties = create_exp.args["properties"]
            if hasattr(properties, "expressions"):
                for prop in properties.expressions:
                    if isinstance(prop, exp.Property):
                        key = prop.this.name if hasattr(prop.this, "name") else str(prop.this)
                        value = None
                        if hasattr(prop, "expression") and prop.expression:
                            if isinstance(prop.expression, exp.Literal):
                                value = prop.expression.this
                            else:
                                value = str(prop.expression)
                        if key == "_doris_table_comment":
                            table_comment = str(value) if value is not None else None
                        elif key == "_doris_table_model":
                            table_model = str(value) if value is not None else None
                        elif key == "_doris_distributed_by":
                            distributed_by = str(value) if value is not None else None
                        elif key == "_doris_buckets":
                            buckets = str(value) if value is not None else "10"
                        elif key == "_doris_key_cols":
                            if value and str(value) not in ("None", "null"):
                                key_cols = [v.strip() for v in str(value).split(",") if v.strip()]
                        elif key == "PARTITION BY":
                            partition_by_clause = prop
                        elif key == "_doris_partition_def":
                            partitioned_by_def = str(value) if value is not None else ""
                        elif key == "_doris_auto_partition":
                            auto_partition = str(value) if value is not None else "false"
                        elif key == "_doris_partition_function":
                            partition_function = str(value) if value is not None else ""
                        elif key and value is not None:
                            properties_list.append(f'"{key}" = "{value}"')

        # Generate KEY clause based on table_model and key_cols
        key_clause = None
        if table_model and key_cols:
            key_type = table_model.upper()
            if key_type == "UNIQUE":
                key_clause = f"UNIQUE KEY({', '.join(key_cols)})"
            elif key_type == "DUPLICATE":
                key_clause = f"DUPLICATE KEY({', '.join(key_cols)})"
        elif table_model:
            # Fallback: use first N columns as default key
            if (
                not is_ctas
                and hasattr(create_exp.this, "expressions")
                and create_exp.this.expressions
            ):
                col_names = []
                for expr in create_exp.this.expressions:
                    if isinstance(expr, exp.ColumnDef):
                        col_name = expr.this.name if hasattr(expr.this, "name") else str(expr.this)
                        col_names.append(col_name)
                if table_model.upper() == "UNIQUE" and col_names:
                    key_clause = f"UNIQUE KEY({col_names[0]})"
                elif table_model.upper() == "DUPLICATE" and col_names:
                    key_clause = f"DUPLICATE KEY({', '.join(col_names[0])})"

        if key_clause:
            parts.append(key_clause)

        if table_comment:
            parts.append(f"COMMENT '{table_comment}'")

        if partition_by_clause:
            # Doris PARTITION BY property uses 'value' in args
            part_func = partition_by_clause.args.get("value")
            if part_func and hasattr(part_func, "this") and hasattr(part_func, "expressions"):
                part_type = part_func.this

                # Use partition_function if available, otherwise use column names
                if partition_function:
                    part_exprs = partition_function
                else:
                    part_exprs = ", ".join(
                        e.sql(dialect=self.dialect, identify=quote) for e in part_func.expressions
                    )

                # Add AUTO prefix if auto_partition is enabled
                partition_prefix = "AUTO " if auto_partition.lower() == "true" else ""

                parts.append(
                    f"{partition_prefix}PARTITION BY {part_type}({part_exprs}) ({partitioned_by_def})"
                )

        if distributed_by:
            parts.append(f"DISTRIBUTED BY {distributed_by} BUCKETS {buckets}")

        if properties_list:
            properties_sql = "PROPERTIES (\n    " + ",\n    ".join(properties_list) + "\n)"
            parts.append(properties_sql)

        if is_ctas:
            select_sql = create_exp.expression.sql(dialect=self.dialect, identify=quote)
            parts.append(f"AS {select_sql}")

        sql_result = "\n".join(parts)
        return sql_result

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
        Doris CREATE VIEW syntax:
        - CREATE VIEW [IF NOT EXISTS] [<db_name>.]<view_name> [(<column_definition>)] AS <query_stmt>

        Doris does not support CREATE OR REPLACE VIEW syntax, so we need to drop the view first
        if replace=True.

        Doris supports both regular views and materialized views. But we only support regular views
        because the Doris materialized views does not support the following features:
        - Table alias
        - Schema name in view name
        """
        if replace:
            self.drop_view(view_name, ignore_if_not_exists=True, materialized=materialized)

        super().create_view(
            view_name,
            query_or_df,
            columns_to_types=columns_to_types,
            replace=False,  # Always set to False since we handle replacement manually
            materialized=materialized,
            materialized_properties=materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            **create_kwargs,
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
        Doris doesn't support CASCADE clause for DROP VIEW.
        """
        # Remove cascade from kwargs as Doris doesn't support it
        kwargs.pop("cascade", None)
        super().drop_view(view_name, ignore_if_not_exists, materialized, **kwargs)

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        """
        Delete from table in Doris.
        """
        if where == exp.true():
            # Use TRUNCATE TABLE for full table deletion as Doris doesn't support WHERE TRUE
            return self.execute(
                exp.TruncateTable(expressions=[exp.to_table(table_name, dialect=self.dialect)])
            )

        return super().delete_from(table_name, where)
