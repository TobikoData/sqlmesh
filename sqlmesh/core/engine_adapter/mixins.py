from __future__ import annotations

import logging
import typing as t

from sqlglot import exp, parse_one
from sqlglot.helper import seq_get

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.dialect import schema_
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF
    from sqlmesh.core.engine_adapter.base import QueryOrDF

logger = logging.getLogger(__name__)

NORMALIZED_DATE_FORMAT = "%Y-%m-%d"
NORMALIZED_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class LogicalMergeMixin(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[exp.Whens] = None,
        merge_filter: t.Optional[exp.Expression] = None,
    ) -> None:
        logical_merge(
            self,
            target_table,
            source_table,
            columns_to_types,
            unique_key,
            when_matched=when_matched,
            merge_filter=merge_filter,
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
        **kwargs: t.Any,
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
                columns = [exp.column(col) for col in columns_to_types]
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
                    whens=exp.Whens(
                        expressions=[when_not_matched_by_source, when_not_matched_by_target]
                    ),
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
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if table_format and self.dialect == "spark":
            properties.append(exp.FileFormatProperty(this=exp.Var(this=table_format)))
            if storage_format:
                properties.append(
                    exp.Property(
                        this="write.format.default", value=exp.Literal.string(storage_format)
                    )
                )
        elif storage_format:
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

            self.create_view(temp_view_name, select_statement, replace=False)
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
        clustered_by: t.List[exp.Expression],
        **kwargs: t.Any,
    ) -> t.Optional[exp.Cluster]:
        return exp.Cluster(expressions=[c.copy() for c in clustered_by])

    def _parse_clustering_key(self, clustering_key: t.Optional[str]) -> t.List[exp.Expression]:
        if not clustering_key:
            return []

        # Note: Assumes `clustering_key` as a string like:
        # - "(col_a)"
        # - "(col_a, col_b)"
        # - "func(col_a, transform(col_b))"
        parsed_cluster_key = parse_one(clustering_key, dialect=self.dialect)

        return parsed_cluster_key.expressions or [parsed_cluster_key.this]

    def get_alter_expressions(
        self, current_table_name: TableName, target_table_name: TableName
    ) -> t.List[exp.Alter]:
        expressions = super().get_alter_expressions(current_table_name, target_table_name)

        # check for a change in clustering
        current_table = exp.to_table(current_table_name)
        target_table = exp.to_table(target_table_name)

        current_table_schema = schema_(current_table.db, catalog=current_table.catalog)
        target_table_schema = schema_(target_table.db, catalog=target_table.catalog)

        current_table_info = seq_get(
            self.get_data_objects(current_table_schema, {current_table.name}), 0
        )
        target_table_info = seq_get(
            self.get_data_objects(target_table_schema, {target_table.name}), 0
        )

        if current_table_info and target_table_info:
            if target_table_info.is_clustered:
                if target_table_info.clustering_key and (
                    current_table_info.clustering_key != target_table_info.clustering_key
                ):
                    expressions.append(
                        self._change_clustering_key_expr(
                            current_table,
                            self._parse_clustering_key(target_table_info.clustering_key),
                        )
                    )
            elif current_table_info.is_clustered:
                expressions.append(self._drop_clustering_key_expr(current_table))

        return expressions

    def _change_clustering_key_expr(
        self, table: exp.Table, cluster_by: t.List[exp.Expression]
    ) -> exp.Alter:
        return exp.Alter(
            this=table,
            kind="TABLE",
            actions=[exp.Cluster(expressions=cluster_by)],
        )

    def _drop_clustering_key_expr(self, table: exp.Table) -> exp.Alter:
        return exp.Alter(
            this=table,
            kind="TABLE",
            actions=[exp.Command(this="DROP", expression="CLUSTERING KEY")],
        )


def logical_merge(
    engine_adapter: EngineAdapter,
    target_table: TableName,
    source_table: QueryOrDF,
    columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
    unique_key: t.Sequence[exp.Expression],
    when_matched: t.Optional[exp.Whens] = None,
    merge_filter: t.Optional[exp.Expression] = None,
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
    if when_matched or merge_filter:
        prop = "when_matched" if when_matched else "merge_filter"
        raise SQLMeshError(
            f"This engine does not support MERGE expressions and therefore `{prop}` is not supported."
        )

    engine_adapter._replace_by_key(
        target_table, source_table, columns_to_types, unique_key, is_unique_key=True
    )


class RowDiffMixin(EngineAdapter):
    # The maximum supported value for n in timestamp(n).
    # Most databases are microsecond (6) but some can only handle millisecond (3) while others go to nanosecond (9)
    MAX_TIMESTAMP_PRECISION = 6

    def concat_columns(
        self,
        columns_to_types: t.Dict[str, exp.DataType],
        decimal_precision: int = 3,
        timestamp_precision: int = MAX_TIMESTAMP_PRECISION,
        delimiter: str = ",",
    ) -> exp.Expression:
        """
        Produce an expression that generates a string version of a record, that is:
            - Every column converted to a string representation, joined together into a single string using the specified :delimiter
        """
        expressions_to_concat: t.List[exp.Expression] = []
        for idx, (column, type) in enumerate(columns_to_types.items()):
            expressions_to_concat.append(
                exp.func(
                    "COALESCE",
                    self.normalize_value(
                        exp.to_column(column), type, decimal_precision, timestamp_precision
                    ),
                    exp.Literal.string(""),
                )
            )
            if idx < len(columns_to_types) - 1:
                expressions_to_concat.append(exp.Literal.string(delimiter))

        return exp.func("CONCAT", *expressions_to_concat)

    def normalize_value(
        self,
        expr: exp.Expression,
        type: exp.DataType,
        decimal_precision: int = 3,
        timestamp_precision: int = MAX_TIMESTAMP_PRECISION,
    ) -> exp.Expression:
        """
        Return an expression that converts the values inside the column `col` to a normalized string

        This string should be comparable across database engines, eg:
            - `date` columns -> YYYY-MM-DD string
            - `datetime`/`timestamp`/`timestamptz` columns -> ISO-8601 string to :timestamp_precision digits of subsecond precision
            - `float` / `double` / `decimal` -> Value formatted to :decimal_precision decimal places
            - `boolean` columns -> '1' or '0'
            - NULLS -> "" (empty string)
        """
        if type.is_type(exp.DataType.Type.BOOLEAN):
            value = self._normalize_boolean_value(expr)
        elif type.is_type(*exp.DataType.INTEGER_TYPES):
            value = self._normalize_integer_value(expr)
        elif type.is_type(*exp.DataType.REAL_TYPES):
            # If there is no scale on the decimal type, treat it like an integer when comparing
            # Some databases like Snowflake deliberately create all integer types as NUMERIC(<size>, 0)
            # and they should be treated as integers and not decimals
            type_params = list(type.find_all(exp.DataTypeParam))
            if len(type_params) == 2 and type_params[-1].this.to_py() == 0:
                value = self._normalize_integer_value(expr)
            else:
                value = self._normalize_decimal_value(expr, decimal_precision)
        elif type.is_type(*exp.DataType.TEMPORAL_TYPES):
            value = self._normalize_timestamp_value(expr, type, timestamp_precision)
        elif type.is_type(*exp.DataType.NESTED_TYPES):
            value = self._normalize_nested_value(expr)
        else:
            value = expr

        return exp.cast(value, to=exp.DataType.build("VARCHAR"))

    def _normalize_nested_value(self, expr: exp.Expression) -> exp.Expression:
        return expr

    def _normalize_timestamp_value(
        self, expr: exp.Expression, type: exp.DataType, precision: int
    ) -> exp.Expression:
        if precision > self.MAX_TIMESTAMP_PRECISION:
            raise ValueError(
                f"Requested timestamp precision '{precision}' exceeds maximum supported precision: {self.MAX_TIMESTAMP_PRECISION}"
            )

        is_date = type.is_type(exp.DataType.Type.DATE, exp.DataType.Type.DATE32)

        format = NORMALIZED_DATE_FORMAT if is_date else NORMALIZED_TIMESTAMP_FORMAT

        if type.is_type(
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMPNTZ,
        ):
            # Convert all timezone-aware values to UTC for comparison
            expr = exp.AtTimeZone(this=expr, zone=exp.Literal.string("UTC"))

        digits_to_chop_off = (
            6 - precision
        )  # 6 = max precision across all adapters and also the max amount of digits TimeToStr will render since its based on `strftime` and `%f` only renders to microseconds

        expr = exp.TimeToStr(this=expr, format=exp.Literal.string(format))
        if digits_to_chop_off > 0:
            expr = exp.func(
                "SUBSTRING", expr, 1, len("2023-01-01 12:13:14.000000") - digits_to_chop_off
            )

        return expr

    def _normalize_integer_value(self, expr: exp.Expression) -> exp.Expression:
        return exp.cast(expr, "BIGINT")

    def _normalize_decimal_value(self, expr: exp.Expression, precision: int) -> exp.Expression:
        return exp.cast(expr, f"DECIMAL(38,{precision})")

    def _normalize_boolean_value(self, expr: exp.Expression) -> exp.Expression:
        return exp.cast(expr, "INT")
