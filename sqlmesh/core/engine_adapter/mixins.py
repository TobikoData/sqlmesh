from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.engine_adapter.base import EngineAdapter, TransactionType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF

logger = logging.getLogger(__name__)


class LogicalMergeMixin(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[str],
    ) -> None:
        """
        Merge implementation for engine adapters that do not support merge natively.

        The merge is executed as follows:
        1. Create a temporary table containing the new data to merge.
        2. Delete rows from target table where unique_key cols match a row in the temporary table.
        3. Insert the temporary table contents into the target table. Any duplicate, non-unique rows
           within the temporary table are ommitted.
        4. Drop the temporary table.
        """
        if columns_to_types is None:
            columns_to_types = self.columns(target_table)

        temp_table = self._get_temp_table(target_table)
        unique_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", *unique_key)
        column_names = list(columns_to_types or [])

        with self.transaction(TransactionType.DML):
            self.ctas(temp_table, source_table, columns_to_types=columns_to_types, exists=False)
            self.execute(
                exp.delete(target_table).where(
                    unique_exp.isin(query=exp.select(unique_exp).from_(temp_table))
                )
            )
            self.execute(
                exp.insert(
                    exp.select(*columns_to_types)
                    .distinct(*unique_key)
                    .from_(temp_table)
                    .subquery(),
                    target_table,
                    columns=column_names,
                )
            )
            self.drop_table(temp_table)


class LogicalReplaceQueryMixin(EngineAdapter):
    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Some engines do not support replace table and also enforce binding views. Therefore we can't swap out tables
        under views and we also can't drop them. Therefore we need to truncate and insert within a transaction.
        """
        if not self.table_exists(table_name):
            return self.ctas(table_name, query_or_df, columns_to_types, exists=False, **kwargs)
        with self.transaction(TransactionType.DDL):
            # TODO: remove quote_identifiers when sqlglot has an expression to represent TRUNCATE
            table = quote_identifiers(exp.to_table(table_name))
            sql = f"TRUNCATE {table.sql(dialect=self.dialect)}"
            self.execute(sql)
            return self.insert_append(table_name, query_or_df, columns_to_types)


class PandasNativeFetchDFSupportMixin(EngineAdapter):
    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a Pandas DataFrame from a SQL query."""
        from pandas.io.sql import read_sql_query

        sql = (
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )
        logger.debug(f"Executing SQL:\n{sql}")
        return read_sql_query(sql, self._connection_pool.get())


class CommitOnExecuteMixin(EngineAdapter):
    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """
        To make sure that inserts and updates take effect we need to commit explicitly unless the
        statement is executed as part of an active transaction.

        Reference: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
        """
        super().execute(
            expressions,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
            **kwargs,
        )
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()
