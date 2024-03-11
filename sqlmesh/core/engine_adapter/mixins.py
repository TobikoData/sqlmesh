from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.node import IntervalUnit
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
                    self._select_columns(columns_to_types)
                    .distinct(*unique_key)
                    .from_(temp_table)
                    .subquery(),
                    target_table,
                    columns=column_names,
                )
            )
            self.drop_table(temp_table)


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
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
    ) -> None:
        """
        Some engines do not support `INSERT OVERWRITE` but instead support
        doing an "INSERT OVERWRITE" using a Merge expression but with the
        predicate being `False`.
        """
        columns_to_types = columns_to_types or self.columns(table_name)
        for source_query in source_queries:
            with source_query as query:
                query = self._order_projections_and_filter(query, columns_to_types, where=where)
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
    MAX_TABLE_COMMENT_LENGTH = 4000
    MAX_COLUMN_COMMENT_LENGTH = 4000

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
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

            property: exp.Property = exp.PartitionedByProperty(
                this=exp.Schema(expressions=partitioned_by),
            )

            if (
                self.dialect == "trino"
                and self.get_catalog_type(catalog_name or self.get_current_catalog()) == "iceberg"
            ):
                # On the Trino Iceberg catalog, the table property is called "partitioning" - not "partitioned_by"
                # In addition, partition column transform expressions like `day(col)` or `bucket(col, 5)` are allowed
                # ref: https://trino.io/docs/current/connector/iceberg.html#table-properties
                property = exp.Property(
                    this=exp.var("PARTITIONING"), value=exp.Schema(expressions=partitioned_by)
                )

            properties.append(property)

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        properties.extend(self._table_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _build_view_properties_exp(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        properties.extend(self._table_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _truncate_comment(self, comment: str, length: t.Optional[int]) -> str:
        # iceberg does not have a comment length limit
        if self.current_catalog_type == "iceberg":
            return comment
        return super()._truncate_comment(comment, length)


class GetCurrentCatalogFromFunctionMixin(EngineAdapter):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_catalog")

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        result = self.fetchone(exp.select(self.CURRENT_CATALOG_EXPRESSION))
        if result:
            return result[0]
        return None


class NonTransactionalTruncateMixin(EngineAdapter):
    def _truncate_table(self, table_name: TableName) -> None:
        # Truncate forces a commit of the current transaction so we want to do an unconditional delete to
        # preserve the transaction if one exists otherwise we can truncate
        if self._connection_pool.is_transaction_active:
            return self.execute(exp.Delete(this=exp.to_table(table_name)))
        super()._truncate_table(table_name)
