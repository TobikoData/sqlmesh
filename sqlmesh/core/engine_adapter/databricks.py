from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import Dialect, exp

from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import classproperty
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, PySparkSession

logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        cursor_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ):
        super().__init__(
            connection_factory,
            dialect,
            sql_gen_kwargs,
            multithreaded,
            cursor_kwargs,
            **kwargs,
        )
        self._spark: t.Optional[PySparkSession] = None

    @classproperty
    def can_access_spark_session(cls) -> bool:
        from sqlmesh import runtime_env

        if runtime_env.is_databricks:
            return True
        try:
            from databricks.connect import DatabricksSession  # type: ignore

            return True
        except ImportError:
            return False

    @property
    def _use_spark_session(self) -> bool:
        from sqlmesh import runtime_env

        if runtime_env.is_databricks:
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
                from py4j.protocol import Py4JError

                # Note: Spark 3.4+ Only API
                try:
                    self.spark.catalog.setCurrentCatalog(catalog)
                # If `setCurrentCatalog` should work for both non-unity and Unity single user
                # clusters. If it fails then we try `USE CATALOG` which is Unity only but works
                # across all clusters
                except Py4JError:
                    self.spark.sql(f"USE CATALOG {catalog}")
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return self._spark

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a DataFrame that can be either Pandas or PySpark from the cursor"""
        if self.is_spark_session_cursor:
            return super()._fetch_native_df(query, quote_identifiers=quote_identifiers)
        if self._use_spark_session:
            logger.debug(f"Executing SQL:\n{query}")
            return self.spark.sql(
                self._to_sql(query, quote=quote_identifiers)
                if isinstance(query, exp.Expression)
                else query
            )
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
