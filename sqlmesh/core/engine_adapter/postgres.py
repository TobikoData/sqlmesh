from __future__ import annotations

import logging
import re
import typing as t
from functools import cached_property, partial
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
    logical_merge,
    GrantsFromInfoSchemaMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog, DataObjectType
from sqlmesh.utils import random_id

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class HypertableConfig(t.NamedTuple):
    """Configuration for a TimescaleDB hypertable."""

    time_column: str
    chunk_time_interval: t.Optional[str] = None
    partitioning_column: t.Optional[str] = None
    number_partitions: t.Optional[int] = None


logger = logging.getLogger(__name__)


@set_catalog()
class PostgresEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
    GrantsFromInfoSchemaMixin,
):
    DIALECT = "postgres"
    SUPPORTS_GRANTS = True
    SUPPORTS_INDEXES = True
    HAS_VIEW_BINDING = True
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    SUPPORTS_REPLACE_TABLE = False
    MAX_IDENTIFIER_LENGTH: t.Optional[int] = 63
    SUPPORTS_QUERY_EXECUTION_TRACKING = True
    GRANT_INFORMATION_SCHEMA_TABLE_NAME = "role_table_grants"
    CURRENT_USER_OR_ROLE_EXPRESSION: exp.Expression = exp.column("current_role")
    SUPPORTS_MULTIPLE_GRANT_PRINCIPALS = True
    SCHEMA_DIFFER_KWARGS = {
        "parameterized_type_defaults": {
            # DECIMAL without precision is "up to 131072 digits before the decimal point; up to 16383 digits after the decimal point"
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(131072 + 16383, 16383), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIME", dialect=DIALECT).this: [(6,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(6,)],
        },
        "types_with_unlimited_length": {
            # all can ALTER to `TEXT`
            exp.DataType.build("TEXT", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
            },
            # all can ALTER to unparameterized `VARCHAR`
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
                exp.DataType.build("TEXT", dialect=DIALECT).this,
            },
            # parameterized `BPCHAR(n)` can ALTER to unparameterized `BPCHAR`
            exp.DataType.build("BPCHAR", dialect=DIALECT).this: {
                exp.DataType.build("BPCHAR", dialect=DIALECT).this
            },
        },
        "drop_cascade": True,
    }

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """
        `read_sql_query` when using psycopg will result on a hanging transaction that must be committed

        https://github.com/pandas-dev/pandas/pull/42277
        """
        df = super()._fetch_native_df(query, quote_identifiers)
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()
        return df

    def _create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            exp.Create(
                this=exp.Schema(
                    this=exp.to_table(target_table_name),
                    expressions=[
                        exp.LikeProperty(
                            this=exp.to_table(source_table_name),
                            expressions=[exp.Property(this="INCLUDING", value=exp.Var(this="ALL"))],
                        )
                    ],
                ),
                kind="TABLE",
                exists=exists,
            )
        )

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> None:
        # Merge isn't supported until Postgres 15
        major, minor = self.server_version
        merge_impl = super().merge if major >= 15 else partial(logical_merge, self)
        merge_impl(  # type: ignore
            target_table,
            source_table,
            target_columns_to_types,
            unique_key,
            when_matched=when_matched,
            merge_filter=merge_filter,
            source_columns=source_columns,
        )

    @cached_property
    def server_version(self) -> t.Tuple[int, int]:
        """Lazily fetch and cache major and minor server version"""
        if result := self.fetchone("SHOW server_version"):
            server_version, *_ = result
            match = re.search(r"(\d+)\.(\d+)", server_version)
            if match:
                return int(match.group(1)), int(match.group(2))
        return 0, 0

    def _get_dependent_views(self, table_name: TableName) -> t.List[t.Tuple[str, str, str, bool]]:
        """Fetches all views that depend on the given table.

        Uses PostgreSQL system catalogs (pg_depend, pg_rewrite, pg_class) to find
        views that reference the table. This is needed because PostgreSQL views
        store the OID of tables they reference, requiring recreation after table swap.

        Args:
            table_name: The name of the table to find dependents for.

        Returns:
            List of tuples containing (schema_name, view_name, definition, is_materialized).
        """
        table = exp.to_table(table_name)
        schema_name = table.db or self._get_current_schema()
        full_table_name = f"{schema_name}.{table.name}"
        regclass_cast = exp.Cast(this=exp.Literal.string(full_table_name), to="regclass")

        query = (
            exp.select(
                exp.column("nspname", "n"),
                exp.column("relname", "c"),
                exp.func("pg_get_viewdef", exp.column("oid", "c"), exp.true()),
                exp.column("relkind", "c").eq(exp.Literal.string("m")),
            )
            .distinct()
            .from_(exp.table_("pg_depend", "pg_catalog").as_("d"))
            .join(exp.table_("pg_rewrite", "pg_catalog").as_("r"), on="r.oid = d.objid")
            .join(exp.table_("pg_class", "pg_catalog").as_("c"), on="c.oid = r.ev_class")
            .join(exp.table_("pg_namespace", "pg_catalog").as_("n"), on="n.oid = c.relnamespace")
            .where(
                exp.column("refobjid", "d").eq(regclass_cast),
                exp.column("classid", "d").eq(
                    exp.Cast(this=exp.Literal.string("pg_rewrite"), to="regclass")
                ),
                exp.column("deptype", "d").eq(exp.Literal.string("n")),
                exp.column("relkind", "c").isin(exp.Literal.string("v"), exp.Literal.string("m")),
            )
            .order_by("n.nspname", "c.relname")
        )

        logger.info("Fetching dependent views for %s", table_name)
        self.execute(query)
        return self.cursor.fetchall()

    def _recreate_dependent_views(
        self, dependent_views: t.List[t.Tuple[str, str, str, bool]]
    ) -> None:
        """Recreates dependent views after a table swap.

        For regular views, uses CREATE OR REPLACE which preserves grants.
        For materialized views, saves grants before DROP and restores after CREATE.

        Args:
            dependent_views: List of tuples (schema_name, view_name, definition, is_materialized).
        """
        for schema_name, view_name, definition, is_materialized in dependent_views:
            view_table = exp.table_(view_name, db=schema_name)
            full_view_name = f"{schema_name}.{view_name}"
            view_query: exp.Expression = exp.maybe_parse(definition, dialect=self.dialect)

            logger.info("Recreating dependent view '%s'", view_table.sql(dialect=self.dialect))

            if is_materialized:
                view_grants = self._get_table_grants(full_view_name)

                self.execute(
                    exp.Drop(this=view_table, exists=True, kind="MATERIALIZED VIEW", cascade=True)
                )
                self.execute(
                    exp.Create(this=view_table, kind="MATERIALIZED VIEW", expression=view_query)
                )

                if view_grants:
                    logger.info("Restoring grants for materialized view '%s'", full_view_name)
                    self._apply_table_grants(view_table, view_grants)
            else:
                self.execute(
                    exp.Create(this=view_table, kind="VIEW", replace=True, expression=view_query)
                )

    def _get_table_indexes(self, table_name: TableName) -> t.List[str]:
        """Fetches CREATE INDEX statements for all user-defined indexes on a table.

        Excludes indexes that back primary key or unique constraints, as these are
        automatically created with table constraints.

        Args:
            table_name: The name of the table to get indexes for.

        Returns:
            List of CREATE INDEX SQL statements.
        """
        table = exp.to_table(table_name)
        schema_name = table.db or self._get_current_schema()

        # CTE to find indexes backing primary key or unique constraints
        excluded_indexes_cte = (
            exp.select(exp.column("relname", "idx").as_("indexname"))
            .from_(exp.table_("pg_constraint", "pg_catalog").as_("c"))
            .join(
                exp.table_("pg_class", "pg_catalog").as_("idx"),
                on=exp.column("oid", "idx").eq(exp.column("conindid", "c")),
            )
            .join(
                exp.table_("pg_namespace", "pg_catalog").as_("n"),
                on=exp.column("oid", "n").eq(exp.column("connamespace", "c")),
            )
            .where(
                exp.column("nspname", "n").eq(exp.Literal.string(schema_name)),
                exp.column("contype", "c").isin(exp.Literal.string("p"), exp.Literal.string("u")),
            )
        )

        query = (
            exp.select(exp.column("indexdef", "i"))
            .from_(exp.table_("pg_indexes").as_("i"))
            .join(
                excluded_indexes_cte.subquery("ei"),
                on=exp.column("indexname", "ei").eq(exp.column("indexname", "i")),
                join_type="LEFT",
            )
            .where(
                exp.column("schemaname", "i").eq(exp.Literal.string(schema_name)),
                exp.column("tablename", "i").eq(exp.Literal.string(table.name)),
                exp.column("indexname", "ei").is_(exp.null()),
            )
        )

        logger.info("Fetching table indexes for %s", table_name)
        self.execute(query)
        return [row[0] for row in self.cursor.fetchall() if row[0]]

    def _recreate_indexes(
        self,
        index_definitions: t.List[str],
        target_table: exp.Table,
    ) -> None:
        """Recreates indexes on target table from stored definitions.

        Args:
            index_definitions: List of CREATE INDEX SQL statements.
            target_table: The table to create indexes on.
        """
        logger.info("Recreating indexes on %s", target_table.sql(dialect=self.dialect))

        for index_def in index_definitions:
            index_expr: exp.Expression = exp.maybe_parse(index_def, dialect=self.dialect)
            if index := index_expr.find(exp.Index):
                index.set("table", target_table.copy())
                index.this.set("this", random_id())
            try:
                self.execute(index_expr)
            except Exception as e:
                logger.error("Failed to recreate index: %s", e)

    def _get_table_grants(
        self, table_name: TableName
    ) -> t.List[t.Tuple[str, str, t.Optional[str]]]:
        """Fetches all grants on a table/view from information_schema.

        Retrieves both table-level grants (aggregated by grantee) and column-level
        grants (aggregated by grantee and privilege_type).

        Args:
            table_name: The name of the table/view to get grants for.

        Returns:
            List of tuples (grantee, privileges, columns).
            - For table-level: (grantee, "SELECT, INSERT, ...", None)
            - For column-level: (grantee, "SELECT", "col1, col2, ...")
        """
        table = exp.to_table(table_name)
        schema_name = table.db or self._get_current_schema()

        # Table-level grants: aggregate privileges per grantee
        table_grants_query = (
            exp.select(
                exp.column("grantee"),
                exp.func("string_agg", exp.column("privilege_type"), exp.Literal.string(", ")).as_(
                    "privileges"
                ),
                exp.cast(exp.null(), "text").as_("columns"),
            )
            .from_(exp.table_("role_table_grants", "information_schema"))
            .where(
                exp.column("table_schema").eq(exp.Literal.string(schema_name)),
                exp.column("table_name").eq(exp.Literal.string(table.name)),
                exp.column("grantee").neq(exp.var("current_user")),
            )
            .group_by(exp.column("grantee"))
        )

        # Column-level grants: aggregate columns per (grantee, privilege_type)
        column_grants_query = (
            exp.select(
                exp.column("grantee"),
                exp.column("privilege_type").as_("privileges"),
                exp.func("string_agg", exp.column("column_name"), exp.Literal.string(", ")).as_(
                    "columns"
                ),
            )
            .from_(exp.table_("column_privileges", "information_schema"))
            .where(
                exp.column("table_schema").eq(exp.Literal.string(schema_name)),
                exp.column("table_name").eq(exp.Literal.string(table.name)),
                exp.column("grantee").neq(exp.var("current_user")),
            )
            .group_by(exp.column("grantee"), exp.column("privilege_type"))
        )

        logger.info("Fetching table grants for %s", table_name)
        self.execute(exp.union(table_grants_query, column_grants_query, distinct=False))
        return self.cursor.fetchall()

    def _apply_table_grants(
        self, table: exp.Table, grants: t.List[t.Tuple[str, str, t.Optional[str]]]
    ) -> None:
        """Applies grants to a table including column-level grants.

        Args:
            table: The table expression to apply grants to.
            grants: List of tuples (grantee, privileges, columns).
                   - For table-level: (grantee, "SELECT, INSERT, ...", None)
                   - For column-level: (grantee, "SELECT", "col1, col2, ...")
        """
        for grantee, privileges, columns in grants:
            if columns:
                # Column-level grant: GRANT privilege (col1, col2) ON TABLE table TO grantee
                privilege_str = f"{privileges} ({columns})"
            else:
                # Table-level grant: GRANT priv1, priv2 ON TABLE table TO grantee
                privilege_str = privileges

            grant_expr = exp.Grant(
                privileges=[exp.Var(this=privilege_str)],
                kind="TABLE",
                securable=table.copy(),
                principals=[exp.Var(this=grantee)],
            )

            logger.info("Recreating grants on %s", table.sql(dialect=self.dialect))

            try:
                self.execute(grant_expr)
            except Exception as e:
                logger.warning("Failed to apply grant: %s", e)

    def _get_hypertable_config(self, table_name: TableName) -> t.Optional[HypertableConfig]:
        """Gets TimescaleDB hypertable configuration if the table is a hypertable.

        Queries TimescaleDB information schema to retrieve hypertable dimensions.
        Returns None if the table is not a hypertable or TimescaleDB is not installed.

        Args:
            table_name: The name of the table to check.

        Returns:
            HypertableConfig with time column and optional space partitioning info,
            or None if not a hypertable.
        """
        table = exp.to_table(table_name)
        schema_name = table.db or self._get_current_schema()

        # Query TimescaleDB dimensions view for hypertable info
        # This view contains both time and space dimensions
        query = (
            exp.select(
                exp.column("column_name", "d"),
                exp.column("dimension_type", "d"),
                exp.column("time_interval", "d"),
                exp.column("num_partitions", "d"),
            )
            .from_(exp.table_("dimensions", "timescaledb_information").as_("d"))
            .join(
                exp.table_("hypertables", "timescaledb_information").as_("h"),
                on=exp.and_(
                    exp.column("hypertable_schema", "d").eq(exp.column("hypertable_schema", "h")),
                    exp.column("hypertable_name", "d").eq(exp.column("hypertable_name", "h")),
                ),
            )
            .where(
                exp.column("hypertable_schema", "h").eq(exp.Literal.string(schema_name)),
                exp.column("hypertable_name", "h").eq(exp.Literal.string(table.name)),
            )
            .order_by(exp.column("dimension_number", "d"))
        )

        try:
            logger.info("Fetching TimescaleDB info for %s", table_name)
            self.execute(query)
            rows = self.cursor.fetchall()
        except Exception as e:
            # TimescaleDB not installed or other error
            logger.warning("Could not query TimescaleDB info: %s", e)
            # Rollback to recover from the failed transaction state in PostgreSQL
            # Without this, all subsequent queries would fail
            self._connection_pool.rollback()
            return None

        if not rows:
            return None

        time_column = None
        chunk_time_interval = None
        partitioning_column = None
        number_partitions = None

        for column_name, dimension_type, time_interval, num_partitions in rows:
            if dimension_type == "Time":
                time_column = column_name
                if time_interval:
                    chunk_time_interval = str(time_interval)
            elif dimension_type == "Space":
                partitioning_column = column_name
                number_partitions = num_partitions

        if not time_column:
            return None

        return HypertableConfig(
            time_column=time_column,
            chunk_time_interval=chunk_time_interval,
            partitioning_column=partitioning_column,
            number_partitions=number_partitions,
        )

    def _create_hypertable(
        self, table: exp.Table, config: HypertableConfig, create_default_indexes: bool = True
    ) -> None:
        """Converts a regular table to a TimescaleDB hypertable.

        Args:
            table: The table to convert.
            config: HypertableConfig with the hypertable configuration.
            create_default_indexes: Whether to create default indexes on the time column.
                Set to False when recreating indexes manually to avoid conflicts.
        """
        table_sql = table.sql(dialect=self.dialect)

        # Build create_hypertable call - arguments must be string literals (single quotes)
        # The table reference is passed as a string that PostgreSQL casts to regclass
        # Column names are passed as NAME type strings
        def escape_literal(s: str) -> str:
            return s.replace("'", "''")

        args = [f"'{table_sql}'", f"'{escape_literal(config.time_column)}'"]

        if config.chunk_time_interval:
            args.append(f"chunk_time_interval => INTERVAL '{config.chunk_time_interval}'")

        if config.partitioning_column and config.number_partitions:
            args.append(f"partitioning_column => '{escape_literal(config.partitioning_column)}'")
            args.append(f"number_partitions => {config.number_partitions}")

        if not create_default_indexes:
            args.append("create_default_indexes => FALSE")

        create_hypertable_sql = f"SELECT create_hypertable({', '.join(args)})"
        logger.info("Converting table to hypertable: %s", table_sql)
        self.execute(create_hypertable_sql)

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        supports_replace_table_override: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> None:
        """Replaces table contents using atomic swap.

        Uses RENAME to swap tables atomically, then recreates dependent views (bound by OID),
        restores indexes and grants. Falls back to _insert_overwrite_by_condition for
        self-referencing queries.
        """
        target_data_object = self.get_data_object(table_name)
        table_exists = target_data_object is not None
        if self.drop_data_object_on_type_mismatch(target_data_object, DataObjectType.TABLE):
            table_exists = False

        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            target_table=table_name,
            source_columns=source_columns,
        )

        if not table_exists:
            return self._create_table_from_source_queries(
                table_name,
                source_queries,
                target_columns_to_types,
                replace=False,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )

        target_table = exp.to_table(table_name)
        query = source_queries[0].query_factory()

        from sqlmesh.core.engine_adapter.base import quote_identifiers

        self_referencing = any(
            quote_identifiers(tbl) == quote_identifiers(target_table)
            for tbl in query.find_all(exp.Table)
        )

        if self_referencing:
            if not target_columns_to_types:
                target_columns_to_types = self.columns(target_table)
            return self._insert_overwrite_by_condition(
                target_table,
                source_queries,
                target_columns_to_types,
                **kwargs,
            )

        if not target_columns_to_types:
            target_columns_to_types = self.columns(target_table)

        dependent_views = self._get_dependent_views(table_name)
        indexes = self._get_table_indexes(table_name)
        grants = self._get_table_grants(table_name)
        hypertable_config = self._get_hypertable_config(table_name)

        temp_table = self._get_temp_table(target_table)
        old_table = self._get_temp_table(target_table)

        try:
            self.create_table(
                temp_table,
                target_columns_to_types,
                exists=False,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )

            # Convert to hypertable before inserting data (required by TimescaleDB)
            # When we have indexes to recreate, disable default index creation to avoid conflicts
            # (TimescaleDB creates indexes on time column by default, which would conflict with
            # the indexes we're about to recreate)
            if hypertable_config:
                self._create_hypertable(
                    temp_table, hypertable_config, create_default_indexes=not indexes
                )

            self._insert_append_source_queries(temp_table, source_queries, target_columns_to_types)

            if indexes:
                self._recreate_indexes(indexes, temp_table)
            if grants:
                self._apply_table_grants(temp_table, grants)

            self.execute(
                exp.Command(this="ANALYZE", expression=temp_table.sql(dialect=self.dialect))
            )
        except Exception:
            self.drop_table(temp_table, exists=True)
            raise

        try:
            with self.transaction():
                self.rename_table(target_table, old_table)
                self.rename_table(temp_table, target_table)

            try:
                with self.transaction():
                    if dependent_views:
                        self._recreate_dependent_views(dependent_views)
                    self.drop_table(old_table)

            except Exception:
                self.rename_table(old_table, target_table)
                raise

        except Exception:
            # Transaction rolled back, temp_table still exists
            self.drop_table(temp_table, exists=True)
            raise
