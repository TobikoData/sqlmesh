from __future__ import annotations

import logging
import typing as t
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF

logger = logging.getLogger(__name__)


@set_catalog()
class PostgresEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "postgres"
    SUPPORTS_INDEXES = True
    HAS_VIEW_BINDING = True
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    SUPPORTS_REPLACE_TABLE = False
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
    )
    REQUIRE_APPLICATION_LOCK = True

    _default_advisory_lock_id = 440077
    """Default sqlmesh advisory lock id for `pg_advisory_lock`, can be manipulated by the user if needed"""
    _active_advisory_lock_ids: t.Set[int] = set()
    """A set of active advisory lock ids used to ensure idempotency within a single engine adapter instance"""

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

    def _acquire_application_lock(self, lock_id: t.Optional[int] = None) -> None:
        """Use `pg_advisory_lock` to acquire a lock for the application, idempotent for an engine adapter instance

        Reference: https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
        """
        lock_id = lock_id or self._default_advisory_lock_id
        if lock_id in self._active_advisory_lock_ids:
            return
        self._execute(f"SELECT pg_advisory_lock({lock_id});")
        self._active_advisory_lock_ids.add(lock_id)

    def _release_application_lock(self, lock_id: t.Optional[int] = None) -> None:
        """Release the application lock, idempotent for an engine adapter instance

        Note the lock is released when the connection is closed so we don't need to worry about stale locks in
        the event of application crashes or unexpected terminations
        """
        lock_id = lock_id or self._default_advisory_lock_id
        if lock_id not in self._active_advisory_lock_ids:
            return
        self._execute(f"SELECT pg_advisory_unlock({lock_id});")
        self._active_advisory_lock_ids.remove(lock_id)
