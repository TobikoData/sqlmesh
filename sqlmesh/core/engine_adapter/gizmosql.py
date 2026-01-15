from __future__ import annotations

import contextlib
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF


@set_catalog(override_mapping={"_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG})
class GizmoSQLEngineAdapter(
    LogicalMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
):
    """
    GizmoSQL Engine Adapter.

    GizmoSQL is a database server that uses DuckDB as its execution engine and
    exposes an Arrow Flight SQL interface for remote connections. This adapter
    uses ADBC (Arrow Database Connectivity) with the Flight SQL driver to
    communicate with GizmoSQL servers.

    Key characteristics:
    - Uses DuckDB SQL dialect for query generation
    - Connects remotely via Arrow Flight SQL (ADBC)
    - Supports full catalog operations
    - Returns data as Arrow tables for efficient transfer
    """

    DIALECT = "duckdb"
    SUPPORTS_TRANSACTIONS = True
    SCHEMA_DIFFER_KWARGS = {
        "parameterized_type_defaults": {
            exp.DataType.build("DECIMAL", dialect="duckdb").this: [(18, 3), (0,)],
        },
    }
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_CREATE_DROP_CATALOG = True
    SUPPORTED_DROP_CASCADE_OBJECT_KINDS = ["SCHEMA", "TABLE", "VIEW"]

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.FULL_SUPPORT

    # DDL/DML keywords that need fetch to trigger GizmoSQL's lazy execution
    _DDL_DML_KEYWORDS = frozenset({
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", "COMMENT", "USE", "SET",
        "INSERT", "UPDATE", "DELETE", "MERGE", "COPY", "ATTACH", "DETACH",
    })

    def _execute(self, sql: str, track_rows_processed: bool = False, **kwargs: t.Any) -> None:
        """
        Execute a SQL statement.

        GizmoSQL uses lazy execution - statements are not actually executed
        until results are fetched. For DDL/DML statements, we call fetchall()
        to ensure immediate execution. For SELECT queries, we let the caller
        fetch the results.
        """
        self.cursor.execute(sql, **kwargs)

        # For DDL/DML, fetch to trigger GizmoSQL's lazy execution
        sql_upper = sql.strip().upper()
        first_word = sql_upper.split()[0] if sql_upper else ""
        if first_word in self._DDL_DML_KEYWORDS:
            self.cursor.fetchall()

    @contextlib.contextmanager
    def transaction(
        self,
        condition: t.Optional[bool] = None,
    ) -> t.Iterator[None]:
        """
        A transaction context manager using SQL statements.

        GizmoSQL's ADBC connection doesn't support the standard begin/commit/rollback
        methods, so we use explicit SQL statements (BEGIN TRANSACTION, COMMIT, ROLLBACK)
        for transaction control.
        """
        if (
            self._connection_pool.is_transaction_active
            or (condition is not None and not condition)
        ):
            yield
            return

        self._connection_pool.begin()
        self.cursor.execute("BEGIN TRANSACTION")
        self.cursor.fetchall()
        try:
            yield
        except Exception as e:
            self.cursor.execute("ROLLBACK")
            self.cursor.fetchall()
            self._connection_pool.rollback()
            raise e
        else:
            self.cursor.execute("COMMIT")
            self.cursor.fetchall()
            self._connection_pool.commit()

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(exp.Use(this=exp.to_identifier(catalog)))

    def _create_catalog(self, catalog_name: exp.Identifier) -> None:
        """Creates a new catalog (database) in GizmoSQL."""
        self.execute(
            exp.Create(this=exp.Table(this=catalog_name), kind="DATABASE", exists=True)
        )

    def _drop_catalog(self, catalog_name: exp.Identifier) -> None:
        """Drops a catalog (database) from GizmoSQL."""
        self.execute(
            exp.Drop(
                this=exp.Table(this=catalog_name), kind="DATABASE", cascade=True, exists=True
            )
        )

    def _df_to_source_queries(
        self,
        df: DF,
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> t.List[SourceQuery]:
        """
        Convert a DataFrame to source queries for insertion.

        Uses ADBC bulk ingestion (adbc_ingest) for efficient Arrow-native data transfer
        to GizmoSQL, avoiding row-by-row insertion overhead.
        """
        import pyarrow as pa

        # Generate a simple temp table name without schema prefix
        # adbc_ingest creates tables in the current schema and treats the full
        # string as a literal table name (doesn't parse schema.table)
        temp_table = self._get_temp_table(target_table)
        # Extract just the table name without schema/catalog
        temp_table_name = temp_table.name

        # Select only the source columns in the right order
        source_columns_to_types = (
            {col: target_columns_to_types[col] for col in source_columns}
            if source_columns
            else target_columns_to_types
        )
        ordered_df = df[list(source_columns_to_types.keys())]

        # Convert DataFrame to PyArrow Table for bulk ingestion
        arrow_table = pa.Table.from_pandas(ordered_df)

        # Use ADBC bulk ingestion - much faster than row-by-row INSERT
        self.cursor.adbc_ingest(
            table_name=temp_table_name,
            data=arrow_table,
            mode="create",
        )

        # Create a simple table reference for queries (no schema prefix)
        temp_table_ref = exp.to_table(temp_table_name)

        return [
            SourceQuery(
                query_factory=lambda: self._select_columns(target_columns_to_types).from_(
                    temp_table_ref
                ),
                cleanup_func=lambda: self.drop_table(temp_table_ref),
            )
        ]

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog = self.get_current_catalog()

        if isinstance(schema_name, exp.Table):
            # Ensures we don't generate identifier quotes
            schema_name = ".".join(part.name for part in schema_name.parts)

        query = (
            exp.select(
                exp.column("table_name").as_("name"),
                exp.column("table_schema").as_("schema"),
                exp.case(exp.column("table_type"))
                .when(
                    exp.Literal.string("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .when(
                    exp.Literal.string("VIEW"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.Literal.string("LOCAL TEMPORARY"),
                    exp.Literal.string("table"),
                )
                .as_("type"),
            )
            .from_(exp.to_table("information_schema.tables"))
            .where(
                exp.column("table_catalog").eq(catalog), exp.column("table_schema").eq(schema_name)
            )
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _normalize_decimal_value(self, col: exp.Expression, precision: int) -> exp.Expression:
        """
        GizmoSQL (via DuckDB) truncates instead of rounding when casting to decimal.

        other databases: select cast(3.14159 as decimal(38,3)) -> 3.142
        duckdb: select cast(3.14159 as decimal(38,3)) -> 3.141

        however, we can get the behaviour of other databases by casting to double first:
        select cast(cast(3.14159 as double) as decimal(38, 3)) -> 3.142
        """
        return exp.cast(
            exp.cast(col, "DOUBLE"),
            f"DECIMAL(38, {precision})",
        )

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """
        Fetches a Pandas DataFrame from a SQL query.

        GizmoSQL returns Arrow tables which can be efficiently converted to Pandas.
        """
        import pandas as pd

        sql = (
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )

        cursor = self.cursor
        cursor.execute(sql)

        # ADBC cursors support fetch_arrow_table() for efficient Arrow->Pandas conversion
        if hasattr(cursor, "fetch_arrow_table"):
            arrow_table = cursor.fetch_arrow_table()
            return arrow_table.to_pandas()

        # Fallback to standard fetchall + DataFrame construction
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return pd.DataFrame(result, columns=columns)
