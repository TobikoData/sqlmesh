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
        when_matched: t.Optional[t.Union[exp.When, t.List[exp.When]]] = None,
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
        self._replace_by_key(
            target_table, source_table, columns_to_types, unique_key, is_unique_key=True
        )


class PandasNativeFetchDFSupportMixin(EngineAdapter):
    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a Pandas DataFrame from a SQL query."""
        from warnings import catch_warnings, filterwarnings

        from pandas.io.sql import read_sql_query

        sql = (
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )
        logger.debug(f"Executing SQL:\n{sql}")
        with catch_warnings(), self.transaction():
            filterwarnings(
                "ignore",
                category=UserWarning,
                message=".*pandas only supports SQLAlchemy connectable.*",
            )
            df = read_sql_query(sql, self._connection_pool.get())
        return df


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

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        *,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Union[exp.PartitionedByProperty, exp.Property]:
        if (
            self.dialect == "trino"
            and self.get_catalog_type(catalog_name or self.get_current_catalog()) == "iceberg"
        ):
            # On the Trino Iceberg catalog, the table property is called "partitioning" - not "partitioned_by"
            # In addition, partition column transform expressions like `day(col)` or `bucket(col, 5)` are allowed
            # Also, column names and transforms need to be strings and supplied as an ARRAY[varchar]
            # ref: https://trino.io/docs/current/connector/iceberg.html#table-properties
            return exp.Property(
                this=exp.var("PARTITIONING"),
                value=exp.array(
                    *(exp.Literal.string(e.sql(dialect=self.dialect)) for e in partitioned_by)
                ),
            )
        for expr in partitioned_by:
            if not isinstance(expr, exp.Column):
                raise SQLMeshError(
                    f"PARTITIONED BY contains non-column value '{expr.sql(dialect=self.dialect)}'."
                )
        return exp.PartitionedByProperty(
            this=exp.Schema(expressions=partitioned_by),
        )

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
        table_kind: t.Optional[str] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if storage_format:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=storage_format)))

        if partitioned_by:
            properties.append(
                self._build_partitioned_by_exp(
                    partitioned_by,
                    partition_interval_unit=partition_interval_unit,
                    catalog_name=catalog_name,
                )
            )

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        properties.extend(self._table_or_view_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _build_view_properties_exp(
        self,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        properties.extend(self._table_or_view_properties_to_expressions(view_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _truncate_comment(self, comment: str, length: t.Optional[int]) -> str:
        # iceberg and delta do not have a comment length limit
        if self.current_catalog_type in ("iceberg", "delta"):
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


class VarcharSizeWorkaroundMixin(EngineAdapter):
    def _default_precision_to_max(
        self, columns_to_types: t.Dict[str, exp.DataType]
    ) -> t.Dict[str, exp.DataType]:
        # get default lengths for types that support "max" length
        types_with_max_default_param = {
            k: [self.SCHEMA_DIFFER.parameterized_type_defaults[k][0][0]]
            for k in self.SCHEMA_DIFFER.max_parameter_length
            if k in self.SCHEMA_DIFFER.parameterized_type_defaults
        }

        # Redshift and MSSQL have a bug where CTAS statements have non-deterministic types. If a LIMIT
        # is applied to a CTAS statement, VARCHAR (and possibly other) types sometimes revert to their
        # default length of 256 (Redshift) or 1 (MSSQL). If we detect that a type has its default length
        # and supports "max" length, we convert it to "max" length to prevent inadvertent data truncation.
        for col_name, col_type in columns_to_types.items():
            if col_type.this in types_with_max_default_param and col_type.expressions:
                parameter = self.SCHEMA_DIFFER.get_type_parameters(col_type)
                type_default = types_with_max_default_param[col_type.this]
                if parameter == type_default:
                    col_type.set("expressions", [exp.DataTypeParam(this=exp.var("max"))])

        return columns_to_types

    def _build_create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        statement = super()._build_create_table_exp(
            table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            table_description=table_description,
            table_kind=table_kind,
            **kwargs,
        )

        if (
            statement.expression
            and statement.expression.args.get("limit") is not None
            and statement.expression.args["limit"].expression.this == "0"
        ):
            assert not isinstance(table_name_or_schema, exp.Schema)
            # redshift and mssql have a bug where CTAS statements have non determistic types. if a limit
            # is applied to a ctas statement, VARCHAR types default to 1 in some instances.
            select_statement = statement.expression.copy()
            for select_or_union in select_statement.find_all(exp.Select, exp.SetOperation):
                select_or_union.set("limit", None)
                select_or_union.set("where", None)

            temp_view_name = self._get_temp_table("ctas")
            self.create_view(
                temp_view_name, select_statement, replace=False, no_schema_binding=False
            )
            try:
                columns_to_types_from_view = self._default_precision_to_max(
                    self.columns(temp_view_name)
                )

                schema = self._build_schema_exp(
                    exp.to_table(table_name_or_schema),
                    columns_to_types_from_view,
                )
                statement = super()._build_create_table_exp(
                    schema,
                    None,
                    exists=exists,
                    replace=replace,
                    columns_to_types=columns_to_types_from_view,
                    table_description=table_description,
                    **kwargs,
                )
            finally:
                self.drop_view(temp_view_name)

        return statement


class ClusteredByMixin(EngineAdapter):
    def _build_clustered_by_exp(
        self,
        clustered_by: t.List[str],
        **kwargs: t.Any,
    ) -> t.Optional[exp.Cluster]:
        return exp.Cluster(expressions=[exp.column(col) for col in clustered_by])
