from __future__ import annotations

import logging
import os
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    DataObject,
    InsertOverwriteStrategy,
    set_catalog,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, PySparkSession, Query

logger = logging.getLogger(__name__)


@set_catalog(
    {
        "_get_data_objects": CatalogSupport.REQUIRES_SET_CATALOG,
    }
)
class DatabricksEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE
    SUPPORTS_CLONING = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SCHEMA_DIFFER = SchemaDiffer(
        support_positional_add=True,
        support_nested_operations=True,
        support_nested_drop=True,
        array_element_selector="element",
        parameterized_type_defaults={
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(10, 0), (0,)],
        },
    )
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        super().__init__(*args, **kwargs)
        self._spark: t.Optional[PySparkSession] = None

    @classmethod
    def can_access_spark_session(cls, disable_spark_session: bool) -> bool:
        from sqlmesh import RuntimeEnv

        if disable_spark_session:
            return False

        return RuntimeEnv.get().is_databricks

    @classmethod
    def can_access_databricks_connect(cls, disable_databricks_connect: bool) -> bool:
        if disable_databricks_connect:
            return False

        try:
            from databricks.connect import DatabricksSession  # noqa

            return True
        except ImportError:
            return False

    @property
    def _use_spark_session(self) -> bool:
        if self.can_access_spark_session(bool(self._extra_config.get("disable_spark_session"))):
            return True
        return (
            self.can_access_databricks_connect(
                bool(self._extra_config.get("disable_databricks_connect"))
            )
            and (
                {
                    "databricks_connect_server_hostname",
                    "databricks_connect_access_token",
                }.issubset(self._extra_config)
            )
            and (
                "databricks_connect_cluster_id" in self._extra_config
                or "databricks_connect_use_serverless" in self._extra_config
            )
        )

    @property
    def use_serverless(self) -> bool:
        from sqlmesh import RuntimeEnv
        from sqlmesh.utils import str_to_bool

        if not self._use_spark_session:
            return False
        return (
            RuntimeEnv.get().is_databricks and str_to_bool(os.environ.get("IS_SERVERLESS", "False"))
        ) or bool(self._extra_config["databricks_connect_use_serverless"])

    @property
    def is_spark_session_cursor(self) -> bool:
        from sqlmesh.engines.spark.db_api.spark_session import SparkSessionCursor

        return isinstance(self.cursor, SparkSessionCursor)

    @property
    def spark(self) -> PySparkSession:
        if not self._use_spark_session:
            raise SQLMeshError(
                "SparkSession is not available. "
                "Either run from a Databricks Notebook or "
                "install `databricks-connect` and configure it to connect to your Databricks cluster."
            )

        if self.is_spark_session_cursor:
            return self._connection_pool.get().spark

        from databricks.connect import DatabricksSession

        if self._spark is None:
            self._spark = (
                DatabricksSession.builder.remote(
                    host=self._extra_config["databricks_connect_server_hostname"],
                    token=self._extra_config["databricks_connect_access_token"],
                    cluster_id=self._extra_config["databricks_connect_cluster_id"],
                )
                .userAgent("sqlmesh")
                .getOrCreate()
            )
            catalog = self._extra_config.get("catalog")
            if catalog:
                self.set_current_catalog(catalog)
        return self._spark

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        if not self._use_spark_session:
            return super(SparkEngineAdapter, self)._df_to_source_queries(
                df, columns_to_types, batch_size, target_table
            )
        df = self._ensure_pyspark_df(df, columns_to_types)

        def query_factory() -> Query:
            temp_table = self._get_temp_table(target_table or "spark", table_only=True)
            if self.use_serverless:
                # Global temp views are not supported on Databricks Serverless
                # This also means we can't mix Python SQL Connection and DB Connect since they wouldn't
                # share the same temp objects.
                df.createOrReplaceTempView(temp_table.sql(dialect=self.dialect))  # type: ignore
            else:
                df.createOrReplaceGlobalTempView(temp_table.sql(dialect=self.dialect))  # type: ignore
                temp_table.set("db", "global_temp")
            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        if self._use_spark_session:
            return [SourceQuery(query_factory=query_factory)]
        return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a DataFrame that can be either Pandas or PySpark from the cursor"""
        if self.is_spark_session_cursor:
            return super()._fetch_native_df(query, quote_identifiers=quote_identifiers)
        if self._use_spark_session:
            sql = (
                self._to_sql(query, quote=quote_identifiers)
                if isinstance(query, exp.Expression)
                else query
            )
            self._log_sql(sql)
            return self.spark.sql(sql)
        self.execute(query)
        return self.cursor.fetchall_arrow().to_pandas()

    def fetchdf(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """
        Returns a Pandas DataFrame from a query or expression.
        """
        df = self._fetch_native_df(query, quote_identifiers=quote_identifiers)
        if not isinstance(df, pd.DataFrame):
            return df.toPandas()
        return df

    def get_current_catalog(self) -> t.Optional[str]:
        # Update the Dataframe API if we have a spark session
        if self._use_spark_session:
            from py4j.protocol import Py4JError
            from pyspark.errors.exceptions.connect import SparkConnectGrpcException

            try:
                # Note: Spark 3.4+ Only API
                return self.spark.catalog.currentCatalog()
            except (Py4JError, SparkConnectGrpcException):
                pass
        result = self.fetchone(exp.select(self.CURRENT_CATALOG_EXPRESSION))
        if result:
            return result[0]
        return None

    def set_current_catalog(self, catalog_name: str) -> None:
        # Since Databricks splits commands across the Dataframe API and the SQL Connector
        # (depending if databricks-connect is installed and a Dataframe is used) we need to ensure both
        # are set to the same catalog since they maintain their default catalog seperately
        self.execute(exp.Use(this=exp.to_identifier(catalog_name), kind="CATALOG"))
        # Update the Dataframe API is we have a spark session
        if self._use_spark_session:
            from py4j.protocol import Py4JError
            from pyspark.errors.exceptions.connect import SparkConnectGrpcException

            try:
                # Note: Spark 3.4+ Only API
                self.spark.catalog.setCurrentCatalog(catalog_name)
            except (Py4JError, SparkConnectGrpcException):
                pass

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        return super()._get_data_objects(schema_name, object_names=object_names)

    def clone_table(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        replace: bool = False,
        clone_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ) -> None:
        clone_kwargs = clone_kwargs or {}
        clone_kwargs["shallow"] = True
        super().clone_table(
            target_table_name,
            source_table_name,
            replace=replace,
            clone_kwargs=clone_kwargs,
            **kwargs,
        )

    def wap_supported(self, table_name: TableName) -> bool:
        return False

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
    ) -> t.Optional[exp.Properties]:
        properties = super()._build_table_properties_exp(
            catalog_name=catalog_name,
            table_format=table_format,
            storage_format=storage_format,
            partitioned_by=partitioned_by,
            partition_interval_unit=partition_interval_unit,
            clustered_by=clustered_by,
            table_properties=table_properties,
            columns_to_types=columns_to_types,
            table_description=table_description,
            table_kind=table_kind,
        )
        if clustered_by:
            cluster_key = exp.maybe_parse(
                f"({','.join(clustered_by)})", into=exp.Ordered, dialect=self.dialect
            )
            clustered_by_exp = exp.Cluster(expressions=[cluster_key])
            expressions = properties.expressions if properties else []
            expressions.append(clustered_by_exp)
            properties = exp.Properties(expressions=expressions)
        return properties
