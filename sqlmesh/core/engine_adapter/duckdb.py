from __future__ import annotations

import logging
import threading
import time
import typing as t
from sqlglot import exp
from pathlib import Path

from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
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

logger = logging.getLogger(__name__)


@set_catalog(override_mapping={"_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG})
class DuckDBEngineAdapter(LogicalMergeMixin, GetCurrentCatalogFromFunctionMixin, RowDiffMixin):
    DIALECT = "duckdb"
    SUPPORTS_TRANSACTIONS = False
    SCHEMA_DIFFER_KWARGS = {
        "parameterized_type_defaults": {
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(18, 3), (0,)],
        },
    }
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_CREATE_DROP_CATALOG = True
    SUPPORTED_DROP_CASCADE_OBJECT_KINDS = ["SCHEMA", "TABLE", "VIEW"]

    # DuckDB STS AssumeRole tokens expire after 900s (15 min, the AWS minimum).
    # DuckDB's `refresh: auto` does not actually refresh credentials mid-session.
    # We proactively recreate secrets before expiry to avoid HTTP 400 errors.
    # See: https://github.com/duckdb/duckdb-httpfs/pull/165
    _S3_SECRET_REFRESH_INTERVAL_S = 720  # 12 minutes (80% of 900s TTL)

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        super().__init__(*args, **kwargs)
        # s3_secrets_config arrives via _extra_engine_config from DuckDBConnectionConfig
        # and is stored in self._extra_config by the base EngineAdapter.__init__.
        # It propagates through with_settings() automatically via _extra_config.
        self._s3_secrets_config: t.Optional[t.List[t.Tuple[t.Dict[str, t.Any], str]]] = (
            self._extra_config.get("s3_secrets_config")
        )
        self._s3_secret_refresh_lock = threading.Lock()
        self._last_secret_refresh_ts = time.monotonic()

    def _refresh_s3_secrets(self) -> None:
        """Drop and recreate all S3 secrets to obtain fresh STS credentials.

        DuckDB's httpfs extension snapshots STS credentials at secret creation time
        and never re-queries the provider chain. When using AssumeRole with the AWS
        minimum session duration (900s / 15 min), credentials expire mid-run for any
        plan that takes longer than 15 minutes, causing HTTP 400 errors from S3.

        This method forces fresh AssumeRole calls by dropping existing S3 secrets
        and recreating them from the original config.
        """
        cursor = self.cursor

        # Drop all existing S3-type secrets by querying their names from DuckDB
        try:
            cursor.execute(
                "SELECT name FROM duckdb_secrets() WHERE type = 's3'"
            )
            existing_secrets = [row[0] for row in cursor.fetchall()]
        except Exception:
            existing_secrets = []

        for secret_name in existing_secrets:
            try:
                cursor.execute(f"DROP SECRET IF EXISTS {secret_name}")
            except Exception as e:
                logger.warning("Failed to drop secret '%s' during refresh: %s", secret_name, e)

        # Recreate from config
        for secret_dict, secret_name in self._s3_secrets_config:  # type: ignore
            secret_settings = [f"{field} '{setting}'" for field, setting in secret_dict.items()]
            if secret_settings:
                secret_clause = ", ".join(secret_settings)
                try:
                    cursor.execute(
                        f"CREATE OR REPLACE SECRET {secret_name} ({secret_clause});"
                    )
                except Exception as e:
                    logger.error("Failed to recreate secret during refresh: %s", e)
                    raise

        logger.info("Refreshed DuckDB S3 secrets (STS credential rotation)")

    def _maybe_refresh_s3_secrets(self) -> None:
        """Refresh S3 secrets if approaching the STS token expiry threshold."""
        if not self._s3_secrets_config:
            return
        if time.monotonic() - self._last_secret_refresh_ts < self._S3_SECRET_REFRESH_INTERVAL_S:
            return

        with self._s3_secret_refresh_lock:
            # Double-check after acquiring lock — another thread may have refreshed
            if time.monotonic() - self._last_secret_refresh_ts < self._S3_SECRET_REFRESH_INTERVAL_S:
                return

            self._refresh_s3_secrets()
            self._last_secret_refresh_ts = time.monotonic()

    def _execute(self, sql: str, track_rows_processed: bool = False, **kwargs: t.Any) -> None:
        self._maybe_refresh_s3_secrets()
        super()._execute(sql, track_rows_processed, **kwargs)

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.FULL_SUPPORT

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(exp.Use(this=exp.to_identifier(catalog)))

    def _create_catalog(self, catalog_name: exp.Identifier) -> None:
        if not self._is_motherduck:
            db_filename = f"{catalog_name.output_name}.db"
            self.execute(
                exp.Attach(
                    this=exp.alias_(exp.Literal.string(db_filename), catalog_name), exists=True
                )
            )
        else:
            self.execute(
                exp.Create(this=exp.Table(this=catalog_name), kind="DATABASE", exists=True)
            )

    def _drop_catalog(self, catalog_name: exp.Identifier) -> None:
        if not self._is_motherduck:
            db_file_path = Path(f"{catalog_name.output_name}.db")
            self.execute(exp.Detach(this=catalog_name, exists=True))
            if db_file_path.exists():
                db_file_path.unlink()
        else:
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
        temp_table = self._get_temp_table(target_table)
        temp_table_sql = (
            exp.select(*self._casted_columns(target_columns_to_types, source_columns))
            .from_("df")
            .sql(dialect=self.dialect)
        )
        self.cursor.sql(f"CREATE TABLE {temp_table} AS {temp_table_sql}")
        return [
            SourceQuery(
                query_factory=lambda: self._select_columns(target_columns_to_types).from_(
                    temp_table
                ),  # type: ignore
                cleanup_func=lambda: self.drop_table(temp_table),
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
            .from_(exp.to_table("system.information_schema.tables"))
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
        duckdb truncates instead of rounding when casting to decimal.

        other databases: select cast(3.14159 as decimal(38,3)) -> 3.142
        duckdb: select cast(3.14159 as decimal(38,3)) -> 3.141

        however, we can get the behaviour of other databases by casting to double first:
        select cast(cast(3.14159 as double) as decimal(38, 3)) -> 3.142
        """
        return exp.cast(
            exp.cast(col, "DOUBLE"),
            f"DECIMAL(38, {precision})",
        )

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        catalog = self.get_current_catalog()
        catalog_type_tuple = self.fetchone(
            exp.select("type")
            .from_("duckdb_databases()")
            .where(exp.column("database_name").eq(catalog))
        )
        catalog_type = catalog_type_tuple[0] if catalog_type_tuple else None

        partitioned_by_exps = None
        if catalog_type == "ducklake":
            partitioned_by_exps = kwargs.pop("partitioned_by", None)

        super()._create_table(
            table_name_or_schema,
            expression,
            exists,
            replace,
            target_columns_to_types,
            table_description,
            column_descriptions,
            table_kind,
            track_rows_processed=track_rows_processed,
            **kwargs,
        )

        if partitioned_by_exps:
            # Schema object contains column definitions, so we extract Table
            table_name = (
                table_name_or_schema.this
                if isinstance(table_name_or_schema, exp.Schema)
                else table_name_or_schema
            )
            table_name_str = (
                table_name.sql(dialect=self.dialect)
                if isinstance(table_name, exp.Table)
                else table_name
            )
            partitioned_by_str = ", ".join(
                expr.sql(dialect=self.dialect) for expr in partitioned_by_exps
            )
            self.execute(f"ALTER TABLE {table_name_str} SET PARTITIONED BY ({partitioned_by_str});")

    @property
    def _is_motherduck(self) -> bool:
        return self._extra_config.get("is_motherduck", False)
