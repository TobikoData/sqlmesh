from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.engine_adapter.base import EngineAdapter, SourceQuery
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF
    from sqlmesh.core.engine_adapter.base import QueryOrDF

logger = logging.getLogger(__name__)


class LogicalMergeMixin(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
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

        with self.transaction():
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
        with self.transaction():
            # TODO: remove quote_identifiers when sqlglot has an expression to represent TRUNCATE
            source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
                query_or_df, columns_to_types, table_name
            )
            columns_to_types = columns_to_types or self.columns(table_name)
            if len(source_queries) != 1:
                raise SQLMeshError(
                    f"Replace query for {table_name} must have exactly one source query."
                )
            with source_queries[0] as query:
                target_table = exp.to_table(table_name)
                # Check if self-referencing
                matching_tables = [
                    table
                    for table in query.find_all(exp.Table)
                    if quote_identifiers(table) == quote_identifiers(target_table)
                ]
                if matching_tables:
                    with self.temp_table(
                        exp.select(*columns_to_types).from_(target_table),
                        target_table,
                        columns_to_types,
                    ) as temp_table:
                        for table in matching_tables:
                            table.replace(temp_table.copy())
                        self.execute(self._truncate_table(table_name))
                        return self._insert_append_query(table_name, query, columns_to_types)
                self.execute(self._truncate_table(table_name))
                return self._insert_append_query(table_name, query, columns_to_types)


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


class InsertOverwriteWithMergeMixin(EngineAdapter):
    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
    ) -> None:
        """
        Some engines do not support `INSERT OVERWRITE` but instead support
        doing an "INSERT OVERWRITE" using a Merge expression but with the
        predicate being `False`.
        """
        columns_to_types = columns_to_types or self.columns(table_name)
        for source_query in source_queries:
            with source_query as query:
                query = self._add_where_to_query(query, where, columns_to_types)
                columns = [exp.to_column(col) for col in columns_to_types]
                when_not_matched_by_source = exp.When(
                    matched=False,
                    source=True,
                    condition=where,
                    then=exp.Delete(),
                )
                when_not_matched_by_target = exp.When(
                    matched=False,
                    source=False,
                    then=exp.Insert(
                        this=exp.Tuple(expressions=columns),
                        expression=exp.Tuple(expressions=columns),
                    ),
                )
                self._merge(
                    target_table=table_name,
                    query=query,
                    on=exp.false(),
                    match_expressions=[when_not_matched_by_source, when_not_matched_by_target],
                )
