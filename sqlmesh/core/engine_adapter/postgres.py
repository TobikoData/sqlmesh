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
        # user may pass a parameterize-able type with 0, 1, or 2 parameters present (depending on the type)
        #  - this dictionary's keys are the underlying exp.DataType.Type data types returned by sqlglot
        #  - each key's value is a dictionary whose keys are the number of parameters provided by the user
        #      and whose values are a tuple containing the default values for the omitted parameters
        #  - the default values are in the order they are specified in the data type string
        #  - parameters are appended to existing parameters (e.g., "DECIMAL(10)" -> `(0,)` -> `DECIMAL(10,0)`)
        parameterized_type_defaults={
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: {1: (0,)},
            exp.DataType.build("CHAR", dialect=DIALECT).this: {0: (1,)},
            exp.DataType.build("TIME", dialect=DIALECT).this: {0: (6,)},
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: {0: (6,)},
            exp.DataType.build("INTERVAL", dialect=DIALECT).this: {0: (6,)},
        },
        # keys are an unlimited length type, values are the types that can always ALTER to the key type
        types_with_unlimited_length={
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
            # parameterized `DECIMAL(n)` can ALTER to unparameterized `DECIMAL`
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: {
                exp.DataType.build("DECIMAL", dialect=DIALECT).this
            },
        },
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
