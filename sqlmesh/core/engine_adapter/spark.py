from __future__ import annotations

import logging
import typing as t
from functools import partial

import pandas as pd
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import (
    CatalogSupport,
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    HiveMetastoreTablePropertiesMixin,
    LogicalReplaceQueryMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType, set_catalog
from sqlmesh.utils import classproperty
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from pyspark.sql import types as spark_types

    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        Query,
    )
    from sqlmesh.core.engine_adapter.base import QueryOrDF


logger = logging.getLogger(__name__)


class SparkEngineAdapter(GetCurrentCatalogFromFunctionMixin, HiveMetastoreTablePropertiesMixin):
    DIALECT = "spark"
    ESCAPE_JSON = True
    SUPPORTS_TRANSACTIONS = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    SUPPORTS_ROW_LEVEL_OP = False

    @property
    def spark(self) -> PySparkSession:
        return self._connection_pool.get().spark

    @property
    def _use_spark_session(self) -> bool:
        return True

    @classproperty
    def _sqlglot_to_spark_primitive_mapping(self) -> t.Dict[t.Any, t.Any]:
        from pyspark.sql import types as spark_types

        return {
            exp.DataType.Type.TINYINT: spark_types.ByteType,
            exp.DataType.Type.SMALLINT: spark_types.ShortType,
            exp.DataType.Type.INT: spark_types.IntegerType,
            exp.DataType.Type.BIGINT: spark_types.LongType,
            exp.DataType.Type.FLOAT: spark_types.FloatType,
            exp.DataType.Type.DOUBLE: spark_types.DoubleType,
            exp.DataType.Type.DECIMAL: spark_types.DecimalType,
            # SQLGlot currently converts VARCHAR and CHAR to Strings
            exp.DataType.Type.VARCHAR: spark_types.StringType,
            exp.DataType.Type.CHAR: spark_types.StringType,
            exp.DataType.Type.TEXT: spark_types.StringType,
            exp.DataType.Type.BINARY: spark_types.BinaryType,
            exp.DataType.Type.BOOLEAN: spark_types.BooleanType,
            exp.DataType.Type.DATE: spark_types.DateType,
            exp.DataType.Type.DATETIME: spark_types.TimestampNTZType,
            exp.DataType.Type.TIMESTAMPLTZ: spark_types.TimestampType,
            exp.DataType.Type.TIMESTAMPTZ: spark_types.TimestampType,
            exp.DataType.Type.TIMESTAMP: spark_types.TimestampType,
        }

    @classproperty
    def _sqlglot_to_spark_complex_mapping(self) -> t.Dict[t.Any, t.Any]:
        from pyspark.sql import types as spark_types

        return {
            exp.DataType.Type.ARRAY: spark_types.ArrayType,
            exp.DataType.Type.MAP: spark_types.MapType,
            exp.DataType.Type.STRUCT: spark_types.StructType,
        }

    @classproperty
    def _spark_to_sqlglot_primitive_mapping(self) -> t.Dict[t.Any, t.Any]:
        return {v: k for k, v in self._sqlglot_to_spark_primitive_mapping.items()}

    @classproperty
    def _spark_to_sqlglot_complex_mapping(self) -> t.Dict[t.Any, t.Any]:
        return {v: k for k, v in self._sqlglot_to_spark_complex_mapping.items()}

    @classmethod
    def spark_to_sqlglot_types(cls, input: spark_types.StructType) -> t.Dict[str, exp.DataType]:
        from pyspark.sql import types as spark_types

        def spark_complex_to_sqlglot_complex(
            complex_type: t.Union[
                spark_types.StructType, spark_types.ArrayType, spark_types.MapType
            ]
        ) -> exp.DataType:
            def get_fields(
                complex_type: t.Union[
                    spark_types.StructType, spark_types.ArrayType, spark_types.MapType
                ]
            ) -> t.Sequence[spark_types.DataType]:
                if isinstance(complex_type, spark_types.StructType):
                    return complex_type.fields
                if isinstance(complex_type, spark_types.ArrayType):
                    return [complex_type.elementType]
                if isinstance(complex_type, spark_types.MapType):
                    return [complex_type.keyType, complex_type.valueType]
                raise SQLMeshError(f"Unsupported complex type: {complex_type}")

            expressions: t.List[t.Union[exp.ColumnDef, exp.DataType]] = []
            fields = get_fields(complex_type)
            for field in fields:
                if isinstance(field, (spark_types.StructType, spark_types.MapType)):
                    expressions.append(spark_complex_to_sqlglot_complex(field))
                elif isinstance(field, spark_types.StructField):
                    sqlglot_data_type = cls._spark_to_sqlglot_primitive_mapping.get(
                        type(field.dataType)
                    ) or spark_complex_to_sqlglot_complex(
                        field.dataType  # type: ignore
                    )
                    kind = (
                        sqlglot_data_type
                        if isinstance(sqlglot_data_type, exp.DataType)
                        else exp.DataType(this=sqlglot_data_type)
                    )
                    expressions.append(exp.ColumnDef(this=exp.to_identifier(field.name), kind=kind))
                else:
                    kind = exp.DataType(this=cls._spark_to_sqlglot_primitive_mapping[type(field)])
                    expressions.append(kind)
            dtype = cls._spark_to_sqlglot_complex_mapping[type(complex_type)]
            return exp.DataType(
                this=dtype,
                expressions=expressions,
                nested=True,
            )

        resp = spark_complex_to_sqlglot_complex(input)
        return {column_def.this.name: column_def.args["kind"] for column_def in resp.expressions}

    @classmethod
    def sqlglot_to_spark_types(cls, input: t.Dict[str, exp.DataType]) -> spark_types.StructType:
        from pyspark.sql import types as spark_types

        def sqlglot_complex_to_spark_complex(complex_type: exp.DataType) -> spark_types.DataType:
            is_struct = complex_type.is_type(exp.DataType.Type.STRUCT)
            expressions = []
            for column_def in complex_type.expressions:
                col_name = column_def.this.name if is_struct else None
                data_type = column_def.args["kind"] if is_struct else column_def
                primitive_func = cls._sqlglot_to_spark_primitive_mapping.get(data_type.this)
                type_func = (
                    primitive_func
                    if primitive_func
                    else partial(sqlglot_complex_to_spark_complex, data_type)
                )
                if is_struct:
                    expressions.append(spark_types.StructField(col_name, type_func()))
                else:
                    expressions.append(type_func())
            klass = cls._sqlglot_to_spark_complex_mapping[complex_type.this]
            if is_struct:
                return klass(expressions)
            return klass(*expressions)

        return t.cast(
            spark_types.StructType,
            sqlglot_complex_to_spark_complex(
                exp.DataType(
                    this=exp.DataType.Type.STRUCT,
                    expressions=[
                        exp.ColumnDef(this=exp.to_identifier(column), kind=data_type)
                        for column, data_type in input.items()
                    ],
                )
            ),
        )

    @classmethod
    def is_pyspark_df(cls, value: t.Any) -> bool:
        return hasattr(value, "sparkSession")

    @classmethod
    def try_get_pyspark_df(cls, value: t.Any) -> t.Optional[PySparkDataFrame]:
        if cls.is_pyspark_df(value):
            return value
        return None

    @classmethod
    def try_get_pandas_df(cls, value: t.Any) -> t.Optional[pd.DataFrame]:
        if cls.is_pandas_df(value):
            return value
        return None

    @t.overload
    def _columns_to_types(
        self, query_or_df: DF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Dict[str, exp.DataType]:
        ...

    @t.overload
    def _columns_to_types(
        self, query_or_df: Query, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        ...

    def _columns_to_types(
        self, query_or_df: QueryOrDF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        if columns_to_types:
            return columns_to_types
        if self.is_pyspark_df(query_or_df):
            from pyspark.sql import DataFrame

            return self.spark_to_sqlglot_types(t.cast(DataFrame, query_or_df).schema)
        return super()._columns_to_types(query_or_df, columns_to_types)

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        if not self._use_spark_session:
            return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)
        df = self._ensure_pyspark_df(df, columns_to_types)

        def query_factory() -> Query:
            temp_table = self._get_temp_table(target_table or "spark", table_only=True)
            df.createOrReplaceGlobalTempView(temp_table.sql(dialect=self.dialect))  # type: ignore
            temp_table.set("db", "global_temp")
            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        if self._use_spark_session:
            return [SourceQuery(query_factory=query_factory)]
        return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)

    def _ensure_pyspark_df(
        self, generic_df: DF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> PySparkDataFrame:
        pyspark_df = self.try_get_pyspark_df(generic_df)
        if pyspark_df:
            return pyspark_df
        df = self.try_get_pandas_df(generic_df)
        if df is None:
            raise SQLMeshError("Ensure PySpark DF can only be run on a PySpark or Pandas DataFrame")
        kwargs = (
            dict(schema=self.sqlglot_to_spark_types(columns_to_types)) if columns_to_types else {}
        )
        return self.spark.createDataFrame(df, **kwargs)  # type: ignore

    def _get_temp_table(
        self,
        table: TableName,
        table_only: bool = False,
    ) -> exp.Table:
        """
        Returns the name of the temp table that should be used for the given table name.
        """
        table = super()._get_temp_table(table, table_only=table_only)
        table_name_id = table.args["this"]
        # Spark with local filesystem has an issue with temp tables that start with __temp so
        # we update here to remove the leading double underscore
        table_name_id.set("this", table_name_id.this.replace("__temp_", "temp_"))
        return table

    def fetchdf(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        return self.fetch_pyspark_df(query, quote_identifiers=quote_identifiers).toPandas()

    def fetch_pyspark_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> PySparkDataFrame:
        return self._ensure_pyspark_df(
            self._fetch_native_df(query, quote_identifiers=quote_identifiers)
        )

    @set_catalog(override=CatalogSupport.REQUIRES_SET_CATALOG)
    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        schema_name = to_schema(schema_name).sql(dialect=self.dialect)
        sql = f"SHOW TABLE EXTENDED IN {schema_name} LIKE '*'"
        try:
            results = (
                self.fetch_pyspark_df(sql).collect()
                if self._use_spark_session
                else self.fetchdf(sql).to_dict("records")
            )
        # Improvement: Figure out all the different exceptions we could get from executing a query either with or
        # without a Spark Session. In addition Databricks would need to be updated to handle it's own exceptions.
        # Therefore just doing except Exception for now.
        except Exception:
            return []
        return [
            DataObject(
                catalog=self.get_current_catalog(),
                schema=schema_name,
                name=row["tableName"],
                type=DataObjectType.VIEW
                if "Type: VIEW" in row["information"]
                else DataObjectType.TABLE,
            )
            for row in results  # type: ignore
        ]

    def get_current_catalog(self) -> t.Optional[str]:
        # Spark 3.4+ API
        if self._use_spark_session:
            return self.spark.catalog.currentCatalog()
        return super().get_current_catalog()

    def set_current_catalog(self, catalog_name: str) -> None:
        # Spark 3.4+ API
        self.spark.catalog.setCurrentCatalog(catalog_name)

    def get_current_database(self) -> str:
        if self._use_spark_session:
            return self.spark.catalog.currentDatabase()
        return self.fetchone(exp.select(exp.func("current_database")))[0]

    @set_catalog(override=CatalogSupport.REQUIRES_SET_CATALOG)
    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
    ) -> None:
        super().create_schema(
            schema_name, ignore_if_exists=ignore_if_exists, warn_on_error=warn_on_error
        )

    @set_catalog(override=CatalogSupport.REQUIRES_SET_CATALOG)
    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=cascade)

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        # Note: Some storage formats (like Delta and Iceberg) support REPLACE TABLE but since we don't
        # currently check for storage formats we will just do an insert/overwrite.
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        columns_to_types = columns_to_types or self.columns(table_name)
        if not columns_to_types:
            raise SQLMeshError("Cannot replace table without columns to types")

        # Self-referential queries: cannot insert overwrite a SELECT from itself, so
        # use LogicalReplaceQuery (which creates a temp table and SELECTs from it)
        if len(source_queries) > 1:
            raise SQLMeshError("Cannot replace table with a batched dataframe")
        with source_queries[0] as query:
            target_table = exp.to_table(table_name)
            self_referencing = any(
                quote_identifiers(table) == quote_identifiers(target_table)
                for table in query.find_all(exp.Table)
            )

            if self_referencing:
                return LogicalReplaceQueryMixin.overwrite_target_from_temp(
                    self, query, columns_to_types, target_table, **kwargs
                )

        return self._insert_overwrite_by_condition(
            table_name, source_queries, columns_to_types, where=exp.true()
        )

    def create_state_table(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        self.create_table(
            table_name,
            columns_to_types,
            partitioned_by=[exp.column(x) for x in primary_key] if primary_key else None,
        )

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        """Create a view with a query or dataframe.

        If a dataframe is passed in, it will be converted into a literal values statement.
        This should only be done if the dataframe is very small!

        Args:
            view_name: The view name.
            query_or_df: A query or dataframe.
            columns_to_types: Columns to use in the view statement.
            replace: Whether or not to replace an existing view defaults to True.
            create_kwargs: Additional kwargs to pass into the Create expression
        """
        pyspark_df = self.try_get_pyspark_df(query_or_df)
        if pyspark_df:
            query_or_df = pyspark_df.toPandas()
        super().create_view(
            view_name, query_or_df, columns_to_types, replace, materialized, **create_kwargs
        )

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        super()._create_table(
            table_name_or_schema,
            expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            **kwargs,
        )
        table_name = (
            table_name_or_schema.this
            if isinstance(table_name_or_schema, exp.Schema)
            else exp.to_table(table_name_or_schema)
        )
        if (kwargs.get("storage_format") or "").lower() == "iceberg" or self.wap_supported(
            table_name
        ):
            # Performing a dummy insert to create a dummy snapshot for Iceberg tables
            # to workaround https://github.com/apache/iceberg/issues/8849.
            dummy_insert = exp.insert(exp.select("*").from_(table_name), table_name)
            self.execute(dummy_insert)

    def wap_supported(self, table_name: TableName) -> bool:
        fqn = self._ensure_fqn(table_name)
        return (
            self.spark.conf.get(f"spark.sql.catalog.{fqn.catalog}")
            == "org.apache.iceberg.spark.SparkCatalog"
        )

    def wap_table_name(self, table_name: TableName, wap_id: str) -> str:
        branch_name = _wap_branch_name(wap_id)
        fqn = self._ensure_fqn(table_name)
        return exp.Dot.build([fqn, exp.to_identifier(f"branch_{branch_name}")]).sql(
            dialect=self.dialect
        )

    def wap_prepare(self, table_name: TableName, wap_id: str) -> str:
        branch_name = _wap_branch_name(wap_id)
        fqn = self._ensure_fqn(table_name)
        self.execute(f"ALTER TABLE {fqn.sql(dialect=self.dialect)} CREATE BRANCH {branch_name}")
        return self.wap_table_name(table_name, wap_id)

    def wap_publish(self, table_name: TableName, wap_id: str) -> None:
        branch_name = _wap_branch_name(wap_id)
        fqn = self._ensure_fqn(table_name)

        get_snapshot_id_query = (
            exp.select("snapshot_id")
            .from_(exp.Dot.build([fqn, exp.to_identifier("refs")]))
            .where(exp.column("name").eq(branch_name))
        )
        iceberg_snapshot_ids = self.fetchall(get_snapshot_id_query)
        if not iceberg_snapshot_ids:
            raise SQLMeshError(f"Could not find Iceberg branch '{branch_name}'.")
        iceberg_snapshot_id = iceberg_snapshot_ids[0][0]

        logger.info(
            "Cherry-picking Iceberg snapshot %s into table '%s'...", iceberg_snapshot_id, fqn
        )

        self.execute(
            f"CALL {fqn.catalog}.system.cherrypick_snapshot('{fqn.db}.{fqn.name}', {iceberg_snapshot_id})"
        )
        self.execute(f"ALTER TABLE {fqn.sql(dialect=self.dialect)} DROP BRANCH {branch_name}")

    def _truncate_table(self, table_name: TableName) -> str:
        table = quote_identifiers(exp.to_table(table_name))
        return f"TRUNCATE TABLE {table.sql(dialect=self.dialect)}"

    def _ensure_fqn(self, table_name: TableName) -> exp.Table:
        if isinstance(table_name, exp.Table):
            table_name = table_name.copy()
        table = exp.to_table(table_name, dialect=self.dialect)
        if not table.catalog:
            table.set("catalog", self.get_current_catalog())
        if not table.db:
            table.set("db", self.get_current_database())
        return table


def _wap_branch_name(wap_id: str) -> str:
    return f"wap_{wap_id}"
