from __future__ import annotations

import contextlib
import logging
import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp
from sqlglot.helper import ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    ClusteredByMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import optional_import
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)
snowpark = optional_import("snowflake.snowpark")

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, SessionProperties, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF, SnowparkSession
    from sqlmesh.core.node import IntervalUnit


@set_catalog(
    override_mapping={
        "_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG,
        "create_schema": CatalogSupport.REQUIRES_SET_CATALOG,
        "drop_schema": CatalogSupport.REQUIRES_SET_CATALOG,
    }
)
class SnowflakeEngineAdapter(GetCurrentCatalogFromFunctionMixin, ClusteredByMixin, RowDiffMixin):
    DIALECT = "snowflake"
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CLONING = True
    SUPPORTS_MANAGED_MODELS = True
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            exp.DataType.build("BINARY", dialect=DIALECT).this: [(8388608,)],
            exp.DataType.build("VARBINARY", dialect=DIALECT).this: [(8388608,)],
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(38, 0), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NCHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: [(16777216,)],
            exp.DataType.build("TIME", dialect=DIALECT).this: [(9,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(9,)],
            exp.DataType.build("TIMESTAMP_LTZ", dialect=DIALECT).this: [(9,)],
            exp.DataType.build("TIMESTAMP_NTZ", dialect=DIALECT).this: [(9,)],
            exp.DataType.build("TIMESTAMP_TZ", dialect=DIALECT).this: [(9,)],
        },
    )
    MANAGED_TABLE_KIND = "DYNAMIC TABLE"

    @contextlib.contextmanager
    def session(self, properties: SessionProperties) -> t.Iterator[None]:
        warehouse = properties.get("warehouse")
        if not warehouse:
            yield
            return

        if isinstance(warehouse, str):
            warehouse = exp.to_identifier(warehouse)
        if not isinstance(warehouse, exp.Expression):
            raise SQLMeshError(f"Invalid warehouse: '{warehouse}'")

        warehouse_exp = quote_identifiers(
            normalize_identifiers(warehouse, dialect=self.dialect), dialect=self.dialect
        )
        warehouse_sql = warehouse_exp.sql(dialect=self.dialect)
        current_warehouse_sql = self._current_warehouse.sql(dialect=self.dialect)

        if warehouse_sql == current_warehouse_sql:
            yield
            return

        self.execute(f"USE WAREHOUSE {warehouse_sql}")
        yield
        self.execute(f"USE WAREHOUSE {current_warehouse_sql}")

    @property
    def _current_warehouse(self) -> exp.Identifier:
        current_warehouse_str = self.fetchone("SELECT CURRENT_WAREHOUSE()")[0]  # type: ignore
        # The warehouse value returned by Snowflake is already normalized, so only quoting is needed.
        return quote_identifiers(exp.to_identifier(current_warehouse_str), dialect=self.dialect)

    @property
    def snowpark(self) -> t.Optional[SnowparkSession]:
        if snowpark:
            return snowpark.Session.builder.configs(
                {"connection": self._connection_pool.get()}
            ).getOrCreate()
        return None

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.FULL_SUPPORT

    def create_managed_table(
        self,
        table_name: TableName,
        query: Query,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        target_table = exp.to_table(table_name)

        # Snowflake defaults to uppercase and it also makes the property presence checks
        # easier
        table_properties = {k.upper(): v for k, v in (table_properties or {}).items()}

        # the WAREHOUSE property is required for a Dynamic Table
        if "WAREHOUSE" not in table_properties:
            table_properties["WAREHOUSE"] = self._current_warehouse

        # so is TARGET_LAG
        # ref: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table
        if "TARGET_LAG" not in table_properties:
            raise SQLMeshError(
                "`target_lag` must be specified in the model physical_properties for a Snowflake Dynamic Table"
            )

        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query, columns_to_types, target_table=target_table
        )

        self._create_table_from_source_queries(
            target_table,
            source_queries,
            columns_to_types,
            replace=self.SUPPORTS_REPLACE_TABLE,
            partitioned_by=partitioned_by,
            clustered_by=clustered_by,
            table_properties=table_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            table_kind=self.MANAGED_TABLE_KIND,
            **kwargs,
        )

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        properties = create_kwargs.pop("properties", None)
        if not properties:
            properties = exp.Properties(expressions=[])
        if replace:
            properties.append("expressions", exp.CopyGrantsProperty())

        super().create_view(
            view_name=view_name,
            query_or_df=query_or_df,
            columns_to_types=columns_to_types,
            replace=replace,
            materialized=materialized,
            materialized_properties=materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            properties=properties,
            **create_kwargs,
        )

    def drop_managed_table(self, table_name: TableName, exists: bool = True) -> None:
        self._drop_object(table_name, exists, kind=self.MANAGED_TABLE_KIND)

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

        # TODO: there is some overlap with the base class and other engine adapters
        # we need a better way of filtering table properties relevent to the current engine
        # and using those to build the expression
        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if clustered_by and (clustered_by_prop := self._build_clustered_by_exp(clustered_by)):
            properties.append(clustered_by_prop)

        if table_properties:
            table_properties = {k.upper(): v for k, v in table_properties.items()}
            # if we are creating a non-dynamic table; remove any properties that are only valid for dynamic tables
            if table_kind != self.MANAGED_TABLE_KIND:
                for prop in {"WAREHOUSE", "TARGET_LAG", "REFRESH_MODE", "INITIALIZE"}:
                    table_properties.pop(prop, None)

            table_type = self._pop_creatable_type_from_properties(table_properties)
            properties.extend(ensure_list(table_type))

            properties.extend(self._table_or_view_properties_to_expressions(table_properties))

        return exp.Properties(expressions=properties) if properties else None

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(
            target_table or "pandas", quoted=False
        )  # write_pandas() re-quotes everything without checking if its already quoted

        is_snowpark_dataframe = snowpark and isinstance(df, snowpark.dataframe.DataFrame)

        def query_factory() -> Query:
            if is_snowpark_dataframe:
                df.createOrReplaceTempView(temp_table.sql(dialect=self.dialect, identify=True))  # type: ignore
            elif isinstance(df, pd.DataFrame):
                from snowflake.connector.pandas_tools import write_pandas

                # Workaround for https://github.com/snowflakedb/snowflake-connector-python/issues/1034
                # The above issue has already been fixed upstream, but we keep the following
                # line anyway in order to support a wider range of Snowflake versions.
                schema = temp_table.db
                if temp_table.catalog:
                    schema = f"{temp_table.catalog}.{schema}"
                self.set_current_schema(schema)

                # See: https://stackoverflow.com/a/75627721
                for column, kind in columns_to_types.items():
                    if is_datetime64_any_dtype(df.dtypes[column]):
                        if kind.is_type("date"):  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.date  # type: ignore
                        elif getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.strftime(
                                "%Y-%m-%d %H:%M:%S.%f%z"
                            )  # type: ignore
                        # https://github.com/snowflakedb/snowflake-connector-python/issues/1677
                        else:  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.strftime(
                                "%Y-%m-%d %H:%M:%S.%f"
                            )  # type: ignore

                # create the table first using our usual method ensure the column datatypes match what we parsed with sqlglot
                # otherwise we would be trusting `write_pandas()` from the snowflake lib to do this correctly
                self.create_table(temp_table, columns_to_types, table_kind="TEMPORARY TABLE")

                write_pandas(
                    self._connection_pool.get(),
                    df,
                    temp_table.name,
                    schema=temp_table.db or None,
                    database=normalize_identifiers(temp_table.catalog, dialect=self.dialect).sql(
                        dialect=self.dialect
                    )
                    if temp_table.catalog
                    else None,
                    chunk_size=self.DEFAULT_BATCH_SIZE,
                    overwrite=True,
                    table_type="temp",
                )
            else:
                raise SQLMeshError(
                    f"Unknown dataframe type: {type(df)} for {target_table}. Expecting pandas or snowpark."
                )

            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        def cleanup() -> None:
            if is_snowpark_dataframe:
                self.drop_view(temp_table)
            else:
                self.drop_table(temp_table)

        # the cleanup_func technically isnt needed because the temp table gets dropped when the session ends
        # but boy does it make our multi-adapter integration tests easier to write
        return [SourceQuery(query_factory=query_factory, cleanup_func=cleanup)]

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        from snowflake.connector.errors import NotSupportedError

        self.execute(query, quote_identifiers=quote_identifiers)

        try:
            return self.cursor.fetch_pandas_all()
        except NotSupportedError:
            # Sometimes Snowflake will not return results as an Arrow result and the fetch from
            # pandas will fail (Ex: `SHOW TERSE OBJECTS IN SCHEMA`). Therefore we manually convert
            # the result into a DataFrame when this happens.
            rows = self.cursor.fetchall()
            columns = self.cursor._result_set.batches[0].column_names
            return pd.DataFrame([dict(zip(columns, row)) for row in rows])

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """

        schema = to_schema(schema_name)
        catalog_name = schema.catalog or self.get_current_catalog()

        query = (
            exp.select(
                exp.column("TABLE_CATALOG").as_("catalog"),
                exp.column("TABLE_NAME").as_("name"),
                exp.column("TABLE_SCHEMA").as_("schema_name"),
                exp.case()
                .when(
                    exp.And(
                        this=exp.column("TABLE_TYPE").eq("BASE TABLE"),
                        expression=exp.column("IS_DYNAMIC").eq("YES"),
                    ),
                    exp.Literal.string("MANAGED_TABLE"),
                )
                .when(exp.column("TABLE_TYPE").eq("BASE TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("TEMPORARY TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("LOCAL TEMPORARY"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("EXTERNAL TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("EVENT TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("VIEW"), exp.Literal.string("VIEW"))
                .when(
                    exp.column("TABLE_TYPE").eq("MATERIALIZED VIEW"),
                    exp.Literal.string("MATERIALIZED_VIEW"),
                )
                .else_(exp.column("TABLE_TYPE"))
                .as_("type"),
                exp.column("CLUSTERING_KEY").as_("clustering_key"),
            )
            .from_(exp.table_("TABLES", db="INFORMATION_SCHEMA", catalog=catalog_name))
            .where(exp.column("TABLE_SCHEMA").eq(schema.db))
            # Snowflake seems to have delayed internal metadata updates and will sometimes return duplicates
            .distinct()
        )
        if object_names:
            query = query.where(exp.column("TABLE_NAME").isin(*object_names))

        df = self.fetchdf(query, quote_identifiers=True)
        if df.empty:
            return []
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
                clustering_key=row.clustering_key,  # type: ignore
            )
            for row in df.itertuples()
        ]

    def set_current_catalog(self, catalog: str) -> None:
        self.execute(exp.Use(this=exp.to_identifier(catalog)))

    def set_current_schema(self, schema: str) -> None:
        self.execute(exp.Use(kind="SCHEMA", this=to_schema(schema)))

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        # note: important to use self._default_catalog instead of the self.default_catalog property
        # otherwise we get RecursionError: maximum recursion depth exceeded
        # because it calls get_current_catalog(), which executes a query, which needs the default catalog, which calls get_current_catalog()... etc
        if self._default_catalog:
            # the purpose of this function is to identify instances where the default catalog is being used
            # (so that we can replace it with the actual catalog as specified in the gateway)
            #
            # we can't do a direct string comparison because the catalog value on the model
            # gets changed when it's normalized as part of generating `model.fqn`
            def unquote_and_lower(identifier: str) -> str:
                return exp.parse_identifier(identifier).name.lower()

            default_catalog_unquoted = unquote_and_lower(self._default_catalog)
            default_catalog_normalized = normalize_identifiers(
                self._default_catalog, dialect=self.dialect
            )

            def catalog_rewriter(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table):
                    if node.catalog:
                        # only replace the catalog on the model with the target catalog if the two are functionally equivalent
                        if unquote_and_lower(node.catalog) == default_catalog_unquoted:
                            node.set("catalog", default_catalog_normalized)
                elif isinstance(node, exp.Use) and isinstance(node.this, exp.Identifier):
                    if unquote_and_lower(node.this.output_name) == default_catalog_unquoted:
                        node.set("this", default_catalog_normalized)
                return node

            # Rewrite whatever default catalog is present on the query to be compatible with what the user supplied in the
            # Snowflake connection config. This is because the catalog present on the model gets normalized and quoted to match
            # the source dialect, which isnt always compatible with Snowflake
            expression = expression.transform(catalog_rewriter)

        return super()._to_sql(expression=expression, quote=quote, **kwargs)

    def _create_column_comments(
        self,
        table_name: TableName,
        column_comments: t.Dict[str, str],
        table_kind: str = "TABLE",
        materialized_view: bool = False,
    ) -> None:
        """
        Reference: https://docs.snowflake.com/en/sql-reference/sql/alter-table-column#syntax
        """
        if not column_comments:
            return

        table = exp.to_table(table_name)
        table_sql = self._to_sql(table)

        list_comment_sql = []
        for column_name, column_comment in column_comments.items():
            column_sql = exp.column(column_name).sql(dialect=self.dialect, identify=True)

            truncated_comment = self._truncate_column_comment(column_comment)
            comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

            list_comment_sql.append(f"COLUMN {column_sql} COMMENT {comment_sql}")

        combined_sql = f"ALTER {table_kind} {table_sql} ALTER {', '.join(list_comment_sql)}"
        try:
            self.execute(combined_sql)
        except Exception:
            logger.warning(
                f"Column comments for table '{table.alias_or_name}' not registered - this may be due to limited permissions.",
                exc_info=True,
            )
