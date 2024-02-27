from __future__ import annotations

import contextlib
import typing as t

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import GetCurrentCatalogFromFunctionMixin
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, SessionProperties, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query


@set_catalog(
    override_mapping={
        "_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG,
        "create_schema": CatalogSupport.REQUIRES_SET_CATALOG,
        "drop_schema": CatalogSupport.REQUIRES_SET_CATALOG,
    }
)
class SnowflakeEngineAdapter(GetCurrentCatalogFromFunctionMixin):
    DIALECT = "snowflake"
    ESCAPE_JSON = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SUPPORTS_CLONING = True
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COLUMN_COMMAND

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

        current_warehouse_str = self.fetchone("SELECT CURRENT_WAREHOUSE()")[0]
        # The warehouse value returned by Snowflake is already normalized, so only quoting is needed.
        current_warehouse_exp = quote_identifiers(
            exp.to_identifier(current_warehouse_str), dialect=self.dialect
        )
        current_warehouse_sql = current_warehouse_exp.sql(dialect=self.dialect)

        if warehouse_sql == current_warehouse_sql:
            yield
            return

        self.execute(f"USE WAREHOUSE {warehouse_sql}")
        yield
        self.execute(f"USE WAREHOUSE {current_warehouse_sql}")

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        assert isinstance(df, pd.DataFrame)
        temp_table = self._get_temp_table(target_table or "pandas")

        def query_factory() -> Query:
            from snowflake.connector.pandas_tools import write_pandas

            if not self.table_exists(temp_table):
                # Workaround for https://github.com/snowflakedb/snowflake-connector-python/issues/1034
                #
                # The above issue has already been fixed upstream, but we keep the following
                # line anyway in order to support a wider range of Snowflake versions.
                self.cursor.execute(f'USE SCHEMA "{temp_table.db}"')

                # See: https://stackoverflow.com/a/75627721
                for column, kind in columns_to_types.items():
                    if is_datetime64_any_dtype(df.dtypes[column]):
                        if kind.is_type("date"):  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.date  # type: ignore
                        elif getattr(df.dtypes[column], "tz", None) is not None:  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")  # type: ignore
                        # https://github.com/snowflakedb/snowflake-connector-python/issues/1677
                        else:  # type: ignore
                            df[column] = pd.to_datetime(df[column]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # type: ignore
                self.create_table(temp_table, columns_to_types, exists=False)
                write_pandas(
                    self._connection_pool.get(),
                    df,
                    temp_table.name,
                    schema=temp_table.db or None,
                    database=temp_table.catalog or None,
                    chunk_size=self.DEFAULT_BATCH_SIZE,
                )
            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

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
        schema_sql = schema.sql(dialect=self.dialect)
        query = (
            exp.select(
                exp.column("TABLE_CATALOG").as_("catalog"),
                exp.column("TABLE_NAME").as_("name"),
                exp.column("TABLE_SCHEMA").as_("schema_name"),
                exp.case()
                .when(exp.column("TABLE_TYPE").eq("BASE TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("TEMPORARY TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("EXTERNAL TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("EVENT TABLE"), exp.Literal.string("TABLE"))
                .when(exp.column("TABLE_TYPE").eq("VIEW"), exp.Literal.string("VIEW"))
                .when(
                    exp.column("TABLE_TYPE").eq("MATERIALIZED VIEW"),
                    exp.Literal.string("MATERIALIZED_VIEW"),
                )
                .else_(exp.column("TABLE_TYPE"))
                .as_("type"),
            )
            .from_(exp.table_("TABLES", db="INFORMATION_SCHEMA", catalog=catalog_name))
            .where(exp.column("TABLE_SCHEMA").eq(schema.db))
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
            )
            for row in df.itertuples()
        ]

    def set_current_catalog(self, catalog: str) -> None:
        self.execute(exp.Use(this=exp.to_identifier(catalog)))
