from __future__ import annotations

import logging
import re
import typing as t
from functools import partial
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
    logical_merge,
)
from sqlmesh.core.engine_adapter.shared import set_catalog
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF

logger = logging.getLogger(__name__)


@set_catalog()
class PostgresEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
):
    DIALECT = "postgres"
    SUPPORTS_INDEXES = True
    HAS_VIEW_BINDING = True
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    SUPPORTS_REPLACE_TABLE = False
    MAX_IDENTIFIER_LENGTH = 63
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # DECIMAL without precision is "up to 131072 digits before the decimal point; up to 16383 digits after the decimal point"
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(131072 + 16383, 16383), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIME", dialect=DIALECT).this: [(6,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(6,)],
        },
        types_with_unlimited_length={
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
        drop_cascade=True,
    )

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

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
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
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
        **kwargs: t.Any,
    ) -> None:
        # Merge isn't supported until Postgres 15
        major, minor = self.get_server_version()
        merge_impl = super().merge if major >= 15 else partial(logical_merge, self)
        merge_impl(  # type: ignore
            target_table,
            source_table,
            columns_to_types,
            unique_key,
            when_matched=when_matched,
            merge_filter=merge_filter,
        )

    def get_server_version(self) -> t.Tuple[int, int]:
        """Return major and minor server versions of the connection"""
        connection = self._connection_pool.get()
        connection_module = connection.__class__.__module__
        if connection_module.startswith("pg8000"):
            server_version = connection.parameter_statuses.get("server_version")
            # pg8000 server version contains version as well as packaging and distribution information
            # e.g. 15.13 (Debian 15.13-1.pgdg120+1)
            match = re.search(r"(\d+)\.(\d+)", server_version)
            if match:
                return int(match.group(1)), int(match.group(2))
        elif connection_module.startswith("psycopg"):
            # This handles both psycopg and psycopg2 connection objects
            server_version = connection.info.server_version
            # Since major version 10, PostgreSQL represents the server version with an integer by
            # multiplying the server's major version number by 10000 and adding the minor version number
            # See https://www.postgresql.org/docs/current/libpq-status.html#LIBPQ-PQSERVERVERSION
            return server_version // 10000, server_version % 100
        return 0, 0
