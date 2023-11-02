from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.engine_adapter.base import EngineAdapter, SourceQuery
from sqlmesh.core.node import IntervalUnit
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query
    from sqlmesh.core.engine_adapter.base import QueryOrDF

logger = logging.getLogger(__name__)


class LogicalMergeMixin(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.When] = None,
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
        if when_matched:
            raise SQLMeshError(
                "This engine does not support MERGE expressions and therefore `when_matched` is not supported."
            )
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
    @classmethod
    def overwrite_target_from_temp(
        cls,
        engine_adapter: EngineAdapter,
        query: Query,
        columns_to_types: t.Dict[str, exp.DataType],
        target_table: TableName,
    ) -> None:
        """
        Overwrites the target table from the temp table. This is used when the target table is self-referencing.
        """
        with engine_adapter.temp_table(
            exp.select(*columns_to_types).from_(target_table),
            target_table,
            columns_to_types,
        ) as temp_table:

            def replace_table(
                node: exp.Expression, curr_table: exp.Table, new_table: exp.Table
            ) -> exp.Expression:
                if isinstance(node, exp.Table) and quote_identifiers(node) == quote_identifiers(
                    curr_table
                ):
                    return new_table
                return node

            temp_query = query.transform(
                replace_table, curr_table=target_table, new_table=temp_table
            )
            engine_adapter.execute(engine_adapter._truncate_table(target_table))
            return engine_adapter._insert_append_query(target_table, temp_query, columns_to_types)

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
                self_referencing = any(
                    quote_identifiers(table) == quote_identifiers(target_table)
                    for table in query.find_all(exp.Table)
                )
                if self_referencing:
                    return self.overwrite_target_from_temp(
                        self, query, columns_to_types, target_table
                    )
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


class HiveMetastoreTablePropertiesMixin(EngineAdapter):
    @classmethod
    def __table_properties_to_expressions(
        cls, table_properties: t.Optional[t.Dict[str, exp.Expression]] = None
    ) -> t.List[exp.Property]:
        if not table_properties:
            return []
        return [
            exp.Property(this=key, value=value.copy()) for key, value in table_properties.items()
        ]

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if storage_format:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=storage_format)))

        if partitioned_by:
            for expr in partitioned_by:
                if not isinstance(expr, exp.Column):
                    raise SQLMeshError(
                        f"PARTITIONED BY contains non-column value '{expr.sql(dialect='spark')}'."
                    )
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Schema(expressions=partitioned_by),
                )
            )

        properties.extend(self.__table_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _create_view_properties(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        if not table_properties:
            return None
        return exp.Properties(expressions=self.__table_properties_to_expressions(table_properties))


class GetCurrentCatalogFromFunctionMixin(EngineAdapter):
    CURRENT_CATALOG_FUNCTION = "current_catalog"

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        result = self.fetchone(f"SELECT {self.CURRENT_CATALOG_FUNCTION}")
        if result:
            return result[0]
        return None
