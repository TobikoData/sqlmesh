from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    InsertOverwriteStrategy,
    set_catalog,
)
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import classproperty
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, PySparkSession

logger = logging.getLogger(__name__)


@set_catalog()
class DatabricksEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    SUPPORTS_CLONING = True
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    SCHEMA_DIFFER = SchemaDiffer(
        support_positional_add=True,
        support_nested_operations=True,
        array_element_selector="element",
    )
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    SUPPORTS_ROW_LEVEL_OP = True

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        super().__init__(*args, **kwargs)
        self._spark: t.Optional[PySparkSession] = None

    @classproperty
    def can_access_spark_session(cls) -> bool:
        from sqlmesh import RuntimeEnv

        if RuntimeEnv.get().is_databricks:
            return True
        try:
            from databricks.connect import DatabricksSession  # type: ignore

            return True
        except ImportError:
            return False

    @property
    def _use_spark_session(self) -> bool:
        from sqlmesh import RuntimeEnv

        if RuntimeEnv.get().is_databricks:
            return True
        return (
            self.can_access_spark_session
            and {
                "databricks_connect_server_hostname",
                "databricks_connect_access_token",
                "databricks_connect_cluster_id",
            }.issubset(self._extra_config)
            and not self._extra_config.get("disable_databricks_connect")
        )

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
            self._spark = DatabricksSession.builder.remote(
                host=self._extra_config["databricks_connect_server_hostname"],
                token=self._extra_config["databricks_connect_access_token"],
                cluster_id=self._extra_config["databricks_connect_cluster_id"],
            ).getOrCreate()
            catalog = self._extra_config.get("catalog")
            if catalog:
                self.set_current_catalog(catalog)
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return self._spark

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
        from py4j.protocol import Py4JError
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException

        # Update the Dataframe API is we have a spark session
        if self._use_spark_session:
            try:
                # Note: Spark 3.4+ Only API
                return super().get_current_catalog()
            except (Py4JError, SparkConnectGrpcException):
                pass
        result = self.fetchone(exp.select(self.CURRENT_CATALOG_EXPRESSION))
        if result:
            return result[0]
        return None

    def set_current_catalog(self, catalog_name: str) -> None:
        from py4j.protocol import Py4JError
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException

        # Since Databricks splits commands across the Dataframe API and the SQL Connector
        # (depending if databricks-connect is installed and a Dataframe is used) we need to ensure both
        # are set to the same catalog since they maintain their default catalog seperately
        self.execute(exp.Use(this=exp.to_identifier(catalog_name), kind="CATALOG"))
        # Update the Dataframe API is we have a spark session
        if self._use_spark_session:
            try:
                # Note: Spark 3.4+ Only API
                super().set_current_catalog(catalog_name)
            except (Py4JError, SparkConnectGrpcException):
                pass

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
