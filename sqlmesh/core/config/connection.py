from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field

from sqlmesh.core import engine_adapter
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.engine_adapter import EngineAdapter

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class _ConnectionConfig(abc.ABC, BaseConfig):
    concurrent_tasks: int

    @property
    @abc.abstractmethod
    def connection_kwargs(self) -> t.Set[str]:
        """keywords that should be passed into the connection"""

    @property
    @abc.abstractmethod
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        """The engine adapter for this connection"""

    @property
    @abc.abstractmethod
    def connector(self) -> t.Callable:
        """The connector for this connection"""

    @property
    def static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        """The static connection kwargs for this connection"""
        return {}

    def create_engine_adapter(self) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter."""
        return self.engine_adapter(
            lambda: self.connector(
                **{
                    **self.static_connection_kwargs,
                    **{
                        k: v
                        for k, v in self.dict().items()
                        if k in self.connection_kwargs
                    },
                }
            ),
            multithreaded=self.concurrent_tasks > 1,
        )


class DuckDBConnectionConfig(_ConnectionConfig):
    """Configuration for the DuckDB connection.

    Args:
        database: The optional database name. If not specified the in-memory database will be used.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
    """

    database: t.Optional[str]

    type_: Literal["duckdb"] = Field(alias="type", default="duckdb")
    concurrent_tasks: Literal[1] = 1

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return {"database"}

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.DuckDBEngineAdapter

    @property
    def connector(self) -> t.Callable:
        import duckdb

        return duckdb.connect


class SnowflakeConnectionConfig(_ConnectionConfig):
    """Configuration for the Snowflake connection.

    Args:
        user: The Snowflake username.
        password: The Snowflake password.
        account: The Snowflake account name.
        warehouse: The optional warehouse name.
        database: The optional database name.
        role: The optional role name.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
    """

    user: str
    password: str
    account: str
    warehouse: t.Optional[str]
    database: t.Optional[str]
    role: t.Optional[str]

    type_: Literal["snowflake"] = Field(alias="type", default="snowflake")
    concurrent_tasks: int = 4
    _concurrent_tasks_validator = concurrent_tasks_validator

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return {"user", "password", "account", "warehouse", "database", "role"}

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.SnowflakeEngineAdapter

    @property
    def connector(self) -> t.Callable:
        from snowflake import connector

        return connector.connect


class DatabricksAPIConnectionConfig(_ConnectionConfig):
    """
    Configuration for the Databricks API connection. This connection is used to access the Databricks
    when you don't have access to a SparkSession. Ex: Running Jupyter locally on your laptop to connect to a
    Databricks cluster

    Arg Source: https://github.com/databricks/databricks-sql-python/blob/main/src/databricks/sql/client.py#L39
    Args:
        server_hostname: Databricks instance host name.
        http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
                   or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        http_headers: An optional list of (k, v) pairs that will be set as Http headers on every request
        session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
               Execute the SQL command `SET -v` to get a full list of available commands.
    """

    server_hostname: str
    http_path: str
    access_token: str
    http_headers: t.Optional[t.List[t.Tuple[str, str]]]
    session_configuration: t.Optional[t.Dict[str, t.Any]]

    concurrent_tasks: int = 4
    type_: Literal["databricks_api"] = Field(alias="type", default="databricks_api")
    _concurrent_tasks_validator = concurrent_tasks_validator

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return {
            "server_hostname",
            "http_path",
            "access_token",
            "http_headers",
            "session_configuration",
        }

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.DatabricksAPIEngineAdapter

    @property
    def connector(self) -> t.Callable:
        from databricks import sql

        return sql.connect


class DatabricksConnectionConfig(_ConnectionConfig):
    """
    Configuration for the Databricks connection. This connection is used to access the Databricks
    when you have access to a SparkSession. Ex: Running in a Databricks notebook or cluster
    """

    type_: Literal["databricks"] = Field(alias="type", default="databricks")
    concurrent_tasks: Literal[1] = 1

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return set()

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.DatabricksEngineAdapter

    @property
    def connector(self) -> t.Callable:
        from sqlmesh.engines.spark.db_api.spark_session import connection

        return connection

    @property
    def static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        from pyspark.sql import SparkSession

        return dict(spark=SparkSession.builder.enableHiveSupport().getOrCreate())


class DatabricksAutoConnectionConfig(_ConnectionConfig):
    """
    Databricks connection that prefers to use SparkSession if available, otherwise it will use the Databricks API.

    Arg Source: https://github.com/databricks/databricks-sql-python/blob/main/src/databricks/sql/client.py#L39
    Args:
        server_hostname: Databricks instance host name.
        http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
                   or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        http_headers: An optional list of (k, v) pairs that will be set as Http headers on every request
        session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
               Execute the SQL command `SET -v` to get a full list of available commands.
    """

    server_hostname: str
    http_path: str
    access_token: str
    http_headers: t.Optional[t.List[t.Tuple[str, str]]]
    session_configuration: t.Optional[t.Dict[str, t.Any]]

    concurrent_tasks: int = 4
    type_: Literal["databricks_auto"] = Field(alias="type", default="databricks_auto")
    _has_spark_session_access: bool

    class Config:
        allow_mutation = True

    @property
    def has_spark_session_access(self) -> bool:
        if self._has_spark_session_access is None:
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark:
                    self._has_spark_session_access = True
                    self.concurrent_tasks = 1
                else:
                    self._has_spark_session_access = False
            except ImportError:
                self._has_spark_session_access = False
        return self._has_spark_session_access

    @property
    def connection_kwargs(self) -> t.Set[str]:
        if self.has_spark_session_access:
            return set()
        return {
            "server_hostname",
            "http_path",
            "access_token",
            "http_headers",
            "session_configuration",
        }

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        if self.has_spark_session_access:
            return engine_adapter.DatabricksEngineAdapter
        return engine_adapter.DatabricksAPIEngineAdapter

    @property
    def connector(self) -> t.Callable:
        if self.has_spark_session_access:
            from sqlmesh.engines.spark.db_api.spark_session import connection

            return connection
        from databricks import sql

        return sql.connect

    @property
    def static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        if self.has_spark_session_access:
            from pyspark.sql import SparkSession

            return dict(spark=SparkSession.builder.enableHiveSupport().getOrCreate())
        return {}


class BigQueryConnectionConfig(_ConnectionConfig):
    """
    BigQuery Connection Configuration.

    TODO: Need to update to support all the different authentication options
    """

    type_: Literal["bigquery"] = Field(alias="type", default="bigquery")
    concurrent_tasks: int = 4

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return set()

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.BigQueryEngineAdapter

    @property
    def connector(self) -> t.Callable:
        from google.cloud.bigquery.dbapi import connect

        return connect


class RedshiftConnectionConfig(_ConnectionConfig):
    """
    Redshift Connection Configuration.

    Arg Source: https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/__init__.py#L146
    Note: A subset of properties were selected. Please open an issue/PR if you want to see more supported.
    Args:
        user: The username to use for authentication with the Amazon Redshift cluster.
        password: The password to use for authentication with the Amazon Redshift cluster.
        database: The name of the database instance to connect to.
        host: The hostname of the Amazon Redshift cluster.
        port: The port number of the Amazon Redshift cluster. Default value is 5439.
        source_address: No description provided
        unix_sock: No description provided
        ssl: Is SSL enabled. Default value is ``True``. SSL must be enabled when authenticating using IAM.
        sslmode: The security of the connection to the Amazon Redshift cluster. 'verify-ca' and 'verify-full' are supported.
        timeout: The number of seconds before the connection to the server will timeout. By default there is no timeout.
        tcp_keepalive: Is `TCP keepalive <https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive>`_ used. The default value is ``True``.
        application_name: Sets the application name. The default value is None.
        preferred_role: The IAM role preferred for the current connection.
        principal_arn: The ARN of the IAM entity (user or role) for which you are generating a policy.
        credentials_provider: The class name of the IdP that will be used for authenticating with the Amazon Redshift cluster.
        region: The AWS region where the Amazon Redshift cluster is located.
        cluster_identifier: The cluster identifier of the Amazon Redshift cluster.
        iam: If IAM authentication is enabled. Default value is False. IAM must be True when authenticating using an IdP.
        is_serverless: Redshift end-point is serverless or provisional. Default value false.
        serverless_acct_id: The account ID of the serverless. Default value None
        serverless_work_group: The name of work group for serverless end point. Default value None.
    """

    user: t.Optional[str]
    password: t.Optional[str]
    database: t.Optional[str]
    host: t.Optional[str]
    port: t.Optional[int]
    source_address: t.Optional[str]
    unix_sock: t.Optional[str]
    ssl: t.Optional[bool]
    sslmode: t.Optional[str]
    timeout: t.Optional[int]
    tcp_keepalive: t.Optional[bool]
    application_name: t.Optional[str]
    preferred_role: t.Optional[str]
    principal_arn: t.Optional[str]
    credentials_provider: t.Optional[str]
    region: t.Optional[str]
    cluster_identifier: t.Optional[str]
    iam: t.Optional[bool]
    is_serverless: t.Optional[bool]
    serverless_acct_id: t.Optional[str]
    serverless_work_group: t.Optional[str]

    type_: Literal["redshift"] = Field(alias="type", default="redshift")
    concurrent_tasks: int = 4

    @property
    def connection_kwargs(self) -> t.Set[str]:
        return {
            "user",
            "password",
            "database",
            "host",
            "port",
            "source_address",
            "unix_sock",
            "ssl",
            "sslmode",
            "timeout",
            "tcp_keepalive",
            "application_name",
            "preferred_role",
            "principal_arn",
            "credentials_provider",
            "region",
            "cluster_identifier",
            "iam",
            "is_serverless",
            "serverless_acct_id",
            "serverless_work_group",
        }

    @property
    def engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.RedshiftEngineAdapter

    @property
    def connector(self) -> t.Callable:
        from redshift_connector import connect

        return connect


ConnectionConfig = Annotated[
    t.Union[
        DuckDBConnectionConfig,
        SnowflakeConnectionConfig,
        DatabricksAPIConnectionConfig,
        DatabricksConnectionConfig,
        DatabricksAutoConnectionConfig,
        BigQueryConnectionConfig,
        RedshiftConnectionConfig,
    ],
    Field(discriminator="type_"),
]
