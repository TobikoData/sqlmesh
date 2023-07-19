from __future__ import annotations

import logging
import typing as t

from pandas.io.sql import read_sql_query
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.shared import TransactionType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF

logger = logging.getLogger(__name__)


class PostgresEngineAdapter(BasePostgresEngineAdapter):
    DIALECT = "postgres"
    SUPPORTS_INDEXES = True

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Postgres does not support replace table and also enforce binding views. Therefore we can't swap out tables
        under views and we also can't drop them. Therefore we need to truncate and insert within a transaction.
        I believe the issue with this approach is that even in a transaction we do an exclusive lock on the table which will
        block reads until the insert is done. I'm not certain about this though.
        """
        if not self.table_exists(table_name):
            return self.ctas(table_name, query_or_df, columns_to_types, exists=False, **kwargs)
        with self.transaction(TransactionType.DDL):
            # TODO: remove quote_identifiers when sqlglot has an expression to represent TRUNCATE
            table = quote_identifiers(exp.to_table(table_name))
            sql = f"TRUNCATE {table.sql(dialect=self.dialect)}"
            self.execute(sql)
            return self.insert_append(table_name, query_or_df, columns_to_types)

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a Pandas DataFrame from a SQL query."""
        sql = (
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )
        logger.debug(f"Executing SQL:\n{sql}")
        return read_sql_query(sql, self.cursor.connection)
