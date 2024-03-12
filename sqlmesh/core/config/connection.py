from __future__ import annotations

import abc
import base64
import logging
import os
import pathlib
import sys
import typing as t
from enum import Enum

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import Field
from sqlglot import exp
from sqlglot.helper import subclasses

from sqlmesh.core import engine_adapter
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.common import (
    concurrent_tasks_validator,
    http_headers_validator,
)
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import (
    PYDANTIC_MAJOR_VERSION,
    field_validator,
    model_validator,
    model_validator_v1_args,
)

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


logger = logging.getLogger(__name__)


class ConnectionConfig(abc.ABC, BaseConfig):
    concurrent_tasks: int
    register_comments: bool

    @property
    @abc.abstractmethod
    def _connection_kwargs_keys(self) -> t.Set[str]:
        """keywords that should be passed into the connection"""

    @property
    @abc.abstractmethod
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        """The engine adapter for this connection"""

    @property
    @abc.abstractmethod
    def _connection_factory(self) -> t.Callable:
        """A function that is called to return a connection object for the given Engine Adapter"""

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        """The static connection kwargs for this connection"""
        return {}

    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        """kwargs that are for execution config only"""
        return {}

    @property
    def _cursor_kwargs(self) -> t.Optional[t.Dict[str, t.Any]]:
        """Key-value arguments that will be passed during cursor construction."""
        return None

    @property
    def _cursor_init(self) -> t.Optional[t.Callable[[t.Any], None]]:
        """A function that is called to initialize the cursor"""
        return None

    def create_engine_adapter(self, register_comments_override: bool = False) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter."""
        return self._engine_adapter(
            lambda: self._connection_factory(
                **{
                    **self._static_connection_kwargs,
                    **{k: v for k, v in self.dict().items() if k in self._connection_kwargs_keys},
                }
            ),
            multithreaded=self.concurrent_tasks > 1,
            cursor_kwargs=self._cursor_kwargs,
            default_catalog=self.get_catalog(),
            cursor_init=self._cursor_init,
            register_comments=register_comments_override or self.register_comments,
            **self._extra_engine_config,
        )

    def get_catalog(self) -> t.Optional[str]:
        """The catalog for this connection"""
        if hasattr(self, "catalog"):
            return self.catalog
        if hasattr(self, "database"):
            return self.database
        if hasattr(self, "db"):
            return self.db
        return None


class BaseDuckDBConnectionConfig(ConnectionConfig):
    """Common configuration for the DuckDB-based connections.

    Args:
        extensions: A list of autoloadable extensions to load.
        connector_config: A dictionary of configuration to pass into the duckdb connector.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
        register_comments: Whether or not to register model comments with the SQL engine.
    """

    extensions: t.List[str] = []
    connector_config: t.Dict[str, t.Any] = {}

    concurrent_tasks: Literal[1] = 1
    register_comments: bool = True

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.DuckDBEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        import duckdb

        return duckdb.connect

    @property
    def _cursor_init(self) -> t.Optional[t.Callable[[t.Any], None]]:
        """A function that is called to initialize the cursor"""
        import duckdb
        from duckdb import BinderException

        def init(cursor: duckdb.DuckDBPyConnection) -> None:
            for extension in self.extensions:
                try:
                    cursor.execute(f"INSTALL {extension}")
                    cursor.execute(f"LOAD {extension}")
                except Exception as e:
                    raise ConfigError(f"Failed to load extension {extension}: {e}")

            for field, setting in self.connector_config.items():
                try:
                    cursor.execute(f"SET {field} = '{setting}'")
                except Exception as e:
                    raise ConfigError(f"Failed to set connector config {field} to {setting}: {e}")

            for i, (alias, path) in enumerate((getattr(self, "catalogs", None) or {}).items()):
                # we parse_identifier and generate to ensure that `alias` has exactly one set of quotes
                # regardless of whether it comes in quoted or not
                alias = exp.parse_identifier(alias, dialect="duckdb").sql(
                    identify=True, dialect="duckdb"
                )
                try:
                    cursor.execute(f"""ATTACH '{path}' AS {alias}""")
                except BinderException as e:
                    # If a user tries to create a catalog pointing at `:memory:` and with the name `memory`
                    # then we don't want to raise since this happens by default. They are just doing this to
                    # set it as the default catalog.
                    if not (
                        'database with name "memory" already exists' in str(e)
                        and path == ":memory:"
                    ):
                        raise e
                if i == 0 and not getattr(self, "database", None):
                    cursor.execute(f"USE {alias}")

        return init


class MotherDuckConnectionConfig(BaseDuckDBConnectionConfig):
    """Configuration for the MotherDuck connection.

    Args:
        database: The database name.
        token: The optional MotherDuck token. If not specified, the user will be prompted to login with their web browser.
    """

    database: str
    token: t.Optional[str] = None

    type_: Literal["motherduck"] = Field(alias="type", default="motherduck")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return set()

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        """kwargs that are for execution config only"""
        connection_str = f"md:{self.database}"
        if self.token:
            connection_str += f"?motherduck_token={self.token}"
        return {"database": connection_str}


class DuckDBConnectionConfig(BaseDuckDBConnectionConfig):
    """Configuration for the DuckDB connection.

    Args:
        database: The optional database name. If not specified, the in-memory database will be used.
        catalogs: Key is the name of the catalog and value is the path.
    """

    database: t.Optional[str] = None
    catalogs: t.Optional[t.Dict[str, str]] = None

    type_: Literal["duckdb"] = Field(alias="type", default="duckdb")

    _data_file_to_adapter: t.ClassVar[t.Dict[str, EngineAdapter]] = {}

    @model_validator(mode="before")
    @model_validator_v1_args
    def _validate_database_catalogs(
        cls, values: t.Dict[str, t.Optional[str]]
    ) -> t.Dict[str, t.Optional[str]]:
        if values.get("database") and values.get("catalogs"):
            raise ConfigError(
                "Cannot specify both `database` and `catalogs`. Define all your catalogs in `catalogs` and have the first entry be the default catalog"
            )
        return values

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {"database"}

    def create_engine_adapter(self, register_comments_override: bool = False) -> EngineAdapter:
        """Checks if another engine adapter has already been created that shares a catalog that points to the same data
        file. If so, it uses that same adapter instead of creating a new one. As a result, any additional configuration
        associated with the new adapter will be ignored."""
        data_files = set((self.catalogs or {}).values())
        if self.database:
            data_files.add(self.database)
        data_files.discard(":memory:")
        for data_file in data_files:
            if adapter := DuckDBConnectionConfig._data_file_to_adapter.get(data_file):
                logger.info(
                    f"Using existing DuckDB adapter due to overlapping data file: {data_file}"
                )
                return adapter
        if data_files:
            logger.info(f"Creating new DuckDB adapter for data files: {data_files}")
        else:
            logger.info("Creating new DuckDB adapter for in-memory database")
        adapter = super().create_engine_adapter(register_comments_override)
        for data_file in data_files:
            DuckDBConnectionConfig._data_file_to_adapter[data_file] = adapter
        return adapter

    def get_catalog(self) -> t.Optional[str]:
        if self.database:
            # Remove `:` from the database name in order to handle if `:memory:` is passed in
            return pathlib.Path(self.database.replace(":", "")).stem
        if self.catalogs:
            return list(self.catalogs)[0]
        return None


class SnowflakeConnectionConfig(ConnectionConfig):
    """Configuration for the Snowflake connection.

    Args:
        account: The Snowflake account name.
        user: The Snowflake username.
        password: The Snowflake password.
        warehouse: The optional warehouse name.
        database: The optional database name.
        role: The optional role name.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
        authenticator: The optional authenticator name. Defaults to username/password authentication ("snowflake").
                       Options: https://github.com/snowflakedb/snowflake-connector-python/blob/e937591356c067a77f34a0a42328907fda792c23/src/snowflake/connector/network.py#L178-L183
        token: The optional oauth access token to use for authentication when authenticator is set to "oauth".
        private_key: The optional private key to use for authentication. Key can be Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or bytes (Python config only). https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#using-key-pair-authentication-key-pair-rotation
        private_key_path: The optional path to the private key to use for authentication. This would be used instead of `private_key`.
        private_key_passphrase: The optional passphrase to use to decrypt `private_key` or `private_key_path`. Keys can be created without encryption so only provide this if needed.
        register_comments: Whether or not to register model comments with the SQL engine.
    """

    account: str
    user: t.Optional[str] = None
    password: t.Optional[str] = None
    warehouse: t.Optional[str] = None
    database: t.Optional[str] = None
    role: t.Optional[str] = None
    authenticator: t.Optional[str] = None
    token: t.Optional[str] = None

    # Private Key Auth
    private_key: t.Optional[t.Union[str, bytes]] = None
    private_key_path: t.Optional[str] = None
    private_key_passphrase: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = False

    type_: Literal["snowflake"] = Field(alias="type", default="snowflake")

    _concurrent_tasks_validator = concurrent_tasks_validator

    @model_validator(mode="before")
    @model_validator_v1_args
    def _validate_authenticator(
        cls, values: t.Dict[str, t.Optional[str]]
    ) -> t.Dict[str, t.Optional[str]]:
        from snowflake.connector.network import (
            DEFAULT_AUTHENTICATOR,
            OAUTH_AUTHENTICATOR,
        )

        auth = values.get("authenticator")
        auth = auth.upper() if auth else DEFAULT_AUTHENTICATOR
        user = values.get("user")
        password = values.get("password")
        values["private_key"] = cls._get_private_key(values, auth)  # type: ignore
        if (
            auth == DEFAULT_AUTHENTICATOR
            and not values.get("private_key")
            and (not user or not password)
        ):
            raise ConfigError("User and password must be provided if using default authentication")
        if auth == OAUTH_AUTHENTICATOR and not values.get("token"):
            raise ConfigError("Token must be provided if using oauth authentication")
        return values

    @classmethod
    def _get_private_key(cls, values: t.Dict[str, t.Optional[str]], auth: str) -> t.Optional[bytes]:
        """
        source: https://github.com/dbt-labs/dbt-snowflake/blob/0374b4ec948982f2ac8ec0c95d53d672ad19e09c/dbt/adapters/snowflake/connections.py#L247C5-L285C1

        Overall code change: Use local variables instead of class attributes + Validation
        """
        # Start custom code
        from snowflake.connector.network import (
            DEFAULT_AUTHENTICATOR,
            KEY_PAIR_AUTHENTICATOR,
        )

        private_key = values.get("private_key")
        private_key_path = values.get("private_key_path")
        private_key_passphrase = values.get("private_key_passphrase")
        user = values.get("user")
        password = values.get("password")
        auth = auth if auth and auth != DEFAULT_AUTHENTICATOR else KEY_PAIR_AUTHENTICATOR

        if not private_key and not private_key_path:
            return None
        if private_key and private_key_path:
            raise ConfigError("Cannot specify both `private_key` and `private_key_path`")
        if auth != KEY_PAIR_AUTHENTICATOR:
            raise ConfigError(
                f"Private key or private key path can only be provided when using {KEY_PAIR_AUTHENTICATOR} authentication"
            )
        if not user:
            raise ConfigError(
                f"User must be provided when using {KEY_PAIR_AUTHENTICATOR} authentication"
            )
        if password:
            raise ConfigError(
                f"Password cannot be provided when using {KEY_PAIR_AUTHENTICATOR} authentication"
            )

        if isinstance(private_key, bytes):
            return private_key
        # End Custom Code

        if private_key_passphrase:
            encoded_passphrase = private_key_passphrase.encode()
        else:
            encoded_passphrase = None

        if private_key:
            if private_key.startswith("-"):
                p_key = serialization.load_pem_private_key(
                    data=bytes(private_key, "utf-8"),
                    password=encoded_passphrase,
                    backend=default_backend(),
                )

            else:
                p_key = serialization.load_der_private_key(
                    data=base64.b64decode(private_key),
                    password=encoded_passphrase,
                    backend=default_backend(),
                )

        elif private_key_path:
            with open(private_key_path, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(), password=encoded_passphrase, backend=default_backend()
                )
        else:
            return None

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "user",
            "password",
            "account",
            "warehouse",
            "database",
            "role",
            "authenticator",
            "token",
            "private_key",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.SnowflakeEngineAdapter

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        return {"autocommit": False}

    @property
    def _connection_factory(self) -> t.Callable:
        from snowflake import connector

        return connector.connect


class DatabricksConnectionConfig(ConnectionConfig):
    """
    Databricks connection that uses the SQL connector for SQL models and then Databricks Connect for Dataframe operations

    Arg Source: https://github.com/databricks/databricks-sql-python/blob/main/src/databricks/sql/client.py#L39
    Args:
        server_hostname: Databricks instance host name.
        http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
            or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        catalog: Default catalog to use for SQL models. Defaults to None which means it will use the default set in
            the Databricks cluster (most likely `hive_metastore`).
        http_headers: An optional list of (k, v) pairs that will be set as Http headers on every request
        session_configuration: An optional dictionary of Spark session parameters.
            Execute the SQL command `SET -v` to get a full list of available commands.
        databricks_connect_server_hostname: The hostname to use when establishing a connecting using Databricks Connect.
            Defaults to the `server_hostname` value.
        databricks_connect_access_token: The access token to use when establishing a connecting using Databricks Connect.
            Defaults to the `access_token` value.
        databricks_connect_cluster_id: The cluster id to use when establishing a connecting using Databricks Connect.
            Defaults to deriving the cluster id from the `http_path` value.
        force_databricks_connect: Force all queries to run using Databricks Connect instead of the SQL connector.
        disable_databricks_connect: Even if databricks connect is installed, do not use it.
    """

    server_hostname: t.Optional[str] = None
    http_path: t.Optional[str] = None
    access_token: t.Optional[str] = None
    catalog: t.Optional[str] = None
    http_headers: t.Optional[t.List[t.Tuple[str, str]]] = None
    session_configuration: t.Optional[t.Dict[str, t.Any]] = None
    databricks_connect_server_hostname: t.Optional[str] = None
    databricks_connect_access_token: t.Optional[str] = None
    databricks_connect_cluster_id: t.Optional[str] = None
    force_databricks_connect: bool = False
    disable_databricks_connect: bool = False

    concurrent_tasks: int = 1
    register_comments: bool = True

    type_: Literal["databricks"] = Field(alias="type", default="databricks")

    _concurrent_tasks_validator = concurrent_tasks_validator
    _http_headers_validator = http_headers_validator

    @model_validator(mode="before")
    @model_validator_v1_args
    def _databricks_connect_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        from sqlmesh import RuntimeEnv
        from sqlmesh.core.engine_adapter.databricks import DatabricksEngineAdapter

        runtime_env = RuntimeEnv.get()

        if runtime_env.is_databricks:
            return values
        server_hostname, http_path, access_token = (
            values.get("server_hostname"),
            values.get("http_path"),
            values.get("access_token"),
        )
        if not server_hostname or not http_path or not access_token:
            raise ValueError(
                "`server_hostname`, `http_path`, and `access_token` are required for Databricks connections when not running in a notebook"
            )
        if DatabricksEngineAdapter.can_access_spark_session:
            if not values.get("databricks_connect_server_hostname"):
                values["databricks_connect_server_hostname"] = f"https://{server_hostname}"
            if not values.get("databricks_connect_access_token"):
                values["databricks_connect_access_token"] = access_token
            if not values.get("databricks_connect_cluster_id"):
                values["databricks_connect_cluster_id"] = http_path.split("/")[-1]
        if not values.get("session_configuration"):
            values["session_configuration"] = {}
        values["session_configuration"]["spark.sql.sources.partitionOverwriteMode"] = "dynamic"
        return values

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        if self.use_spark_session_only:
            return set()
        return {
            "server_hostname",
            "http_path",
            "access_token",
            "http_headers",
            "session_configuration",
            "catalog",
        }

    @property
    def _engine_adapter(self) -> t.Type[engine_adapter.DatabricksEngineAdapter]:
        return engine_adapter.DatabricksEngineAdapter

    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        return {
            k: v
            for k, v in self.dict().items()
            if k.startswith("databricks_connect_") or k in ("catalog", "disable_databricks_connect")
        }

    @property
    def use_spark_session_only(self) -> bool:
        from sqlmesh import RuntimeEnv

        return RuntimeEnv.get().is_databricks or self.force_databricks_connect

    @property
    def _connection_factory(self) -> t.Callable:
        if self.use_spark_session_only:
            from sqlmesh.engines.spark.db_api.spark_session import connection

            return connection

        from databricks import sql

        return sql.connect

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        from sqlmesh import RuntimeEnv

        if not self.use_spark_session_only:
            return {}

        if RuntimeEnv.get().is_databricks:
            from pyspark.sql import SparkSession

            return dict(
                spark=SparkSession.getActiveSession(),
                catalog=self.catalog,
            )

        from databricks.connect import DatabricksSession

        return dict(
            spark=DatabricksSession.builder.remote(
                host=self.databricks_connect_server_hostname,
                token=self.databricks_connect_access_token,
                cluster_id=self.databricks_connect_cluster_id,
            ).getOrCreate(),
            catalog=self.catalog,
        )


class BigQueryConnectionMethod(str, Enum):
    OAUTH = "oauth"
    OAUTH_SECRETS = "oauth-secrets"
    SERVICE_ACCOUNT = "service-account"
    SERVICE_ACCOUNT_JSON = "service-account-json"


class BigQueryPriority(str, Enum):
    BATCH = "batch"
    INTERACTIVE = "interactive"

    @property
    def is_batch(self) -> bool:
        return self == self.BATCH

    @property
    def is_interactive(self) -> bool:
        return self == self.INTERACTIVE

    @property
    def bigquery_constant(self) -> str:
        from google.cloud.bigquery import QueryPriority

        if self.is_batch:
            return QueryPriority.BATCH
        return QueryPriority.INTERACTIVE


class BigQueryConnectionConfig(ConnectionConfig):
    """
    BigQuery Connection Configuration.
    """

    method: BigQueryConnectionMethod = BigQueryConnectionMethod.OAUTH

    project: t.Optional[str] = None
    execution_project: t.Optional[str] = None
    location: t.Optional[str] = None
    # Keyfile Auth
    keyfile: t.Optional[str] = None
    keyfile_json: t.Optional[t.Dict[str, t.Any]] = None
    # Oath Secret Auth
    token: t.Optional[str] = None
    refresh_token: t.Optional[str] = None
    client_id: t.Optional[str] = None
    client_secret: t.Optional[str] = None
    token_uri: t.Optional[str] = None
    scopes: t.Tuple[str, ...] = ("https://www.googleapis.com/auth/bigquery",)
    job_creation_timeout_seconds: t.Optional[int] = None
    # Extra Engine Config
    job_execution_timeout_seconds: t.Optional[int] = None
    job_retries: t.Optional[int] = 1
    job_retry_deadline_seconds: t.Optional[int] = None
    priority: t.Optional[BigQueryPriority] = None
    maximum_bytes_billed: t.Optional[int] = None

    concurrent_tasks: int = 1
    register_comments: bool = True

    type_: Literal["bigquery"] = Field(alias="type", default="bigquery")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return set()

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.BigQueryEngineAdapter

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        """The static connection kwargs for this connection"""
        import google.auth
        from google.api_core import client_info
        from google.oauth2 import credentials, service_account

        if self.method == BigQueryConnectionMethod.OAUTH:
            creds, _ = google.auth.default(scopes=self.scopes)
        elif self.method == BigQueryConnectionMethod.SERVICE_ACCOUNT:
            creds = service_account.Credentials.from_service_account_file(
                self.keyfile, scopes=self.scopes
            )
        elif self.method == BigQueryConnectionMethod.SERVICE_ACCOUNT_JSON:
            creds = service_account.Credentials.from_service_account_info(
                self.keyfile_json, scopes=self.scopes
            )
        elif self.method == BigQueryConnectionMethod.OAUTH_SECRETS:
            creds = credentials.Credentials(
                token=self.token,
                refresh_token=self.refresh_token,
                client_id=self.client_id,
                client_secret=self.client_secret,
                token_uri=self.token_uri,
                scopes=self.scopes,
            )
        else:
            raise ConfigError("Invalid BigQuery Connection Method")
        client = google.cloud.bigquery.Client(
            project=self.execution_project or self.project,
            credentials=creds,
            location=self.location,
            client_info=client_info.ClientInfo(user_agent="sqlmesh"),
        )

        return {
            "client": client,
        }

    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        return {
            k: v
            for k, v in self.dict().items()
            if k
            in {
                "job_creation_timeout_seconds",
                "job_execution_timeout_seconds",
                "job_retries",
                "job_retry_deadline_seconds",
                "priority",
                "maximum_bytes_billed",
            }
        }

    @property
    def _connection_factory(self) -> t.Callable:
        from google.cloud.bigquery.dbapi import connect

        return connect

    def get_catalog(self) -> t.Optional[str]:
        return self.project


class GCPPostgresConnectionConfig(ConnectionConfig):
    """
    Postgres Connection Configuration for GCP.

    Args:
        instance_connection_string: Connection name for the postgres instance.
        user: Postgres or IAM user's name
        password: The postgres user's password. Only needed when the user is a postgres user.
        enable_iam_auth: Set to True when user is an IAM user.
        db: Name of the db to connect to.
    """

    instance_connection_string: str
    user: str
    password: t.Optional[str] = None
    enable_iam_auth: t.Optional[bool] = None
    db: str
    timeout: t.Optional[int] = None

    driver: str = "pg8000"
    type_: Literal["gcp_postgres"] = Field(alias="type", default="gcp_postgres")
    concurrent_tasks: int = 4
    register_comments: bool = True

    @model_validator(mode="before")
    @model_validator_v1_args
    def _validate_auth_method(
        cls, values: t.Dict[str, t.Optional[str]]
    ) -> t.Dict[str, t.Optional[str]]:
        password = values.get("password")
        enable_iam_auth = values.get("enable_iam_auth")
        if password and enable_iam_auth:
            raise ConfigError(
                "Invalid GCP Postgres connection configuration - both password and"
                " enable_iam_auth set. Use password when connecting to a postgres"
                " user and enable_iam_auth 'True' when connecting to an IAM user."
            )
        if not password and not enable_iam_auth:
            raise ConfigError(
                "GCP Postgres connection configuration requires either password set"
                " for a postgres user account or enable_iam_auth set to 'True'"
                " for an IAM user account."
            )
        return values

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "instance_connection_string",
            "driver",
            "user",
            "password",
            "db",
            "enable_iam_auth",
            "timeout",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.PostgresEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from google.cloud.sql.connector import Connector

        return Connector().connect


class RedshiftConnectionConfig(ConnectionConfig):
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

    user: t.Optional[str] = None
    password: t.Optional[str] = None
    database: t.Optional[str] = None
    host: t.Optional[str] = None
    port: t.Optional[int] = None
    source_address: t.Optional[str] = None
    unix_sock: t.Optional[str] = None
    ssl: t.Optional[bool] = None
    sslmode: t.Optional[str] = None
    timeout: t.Optional[int] = None
    tcp_keepalive: t.Optional[bool] = None
    application_name: t.Optional[str] = None
    preferred_role: t.Optional[str] = None
    principal_arn: t.Optional[str] = None
    credentials_provider: t.Optional[str] = None
    region: t.Optional[str] = None
    cluster_identifier: t.Optional[str] = None
    iam: t.Optional[bool] = None
    is_serverless: t.Optional[bool] = None
    serverless_acct_id: t.Optional[str] = None
    serverless_work_group: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["redshift"] = Field(alias="type", default="redshift")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
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
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.RedshiftEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from redshift_connector import connect

        return connect


class PostgresConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    port: int
    database: str
    keepalives_idle: t.Optional[int] = None
    connect_timeout: int = 10
    role: t.Optional[str] = None
    sslmode: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["postgres"] = Field(alias="type", default="postgres")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "host",
            "user",
            "password",
            "port",
            "database",
            "keepalives_idle",
            "connect_timeout",
            "role",
            "sslmode",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.PostgresEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from psycopg2 import connect

        return connect


class MySQLConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    port: t.Optional[int] = None
    charset: t.Optional[str] = None
    ssl_disabled: t.Optional[bool] = None

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["mysql"] = Field(alias="type", default="mysql")

    @property
    def _cursor_kwargs(self) -> t.Optional[t.Dict[str, t.Any]]:
        """Key-value arguments that will be passed during cursor construction."""
        return {"buffered": True}

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        connection_keys = {
            "host",
            "user",
            "password",
            "port",
            "database",
        }
        if self.port is not None:
            connection_keys.add("port")
        if self.charset is not None:
            connection_keys.add("charset")
        if self.ssl_disabled is not None:
            connection_keys.add("ssl_disabled")
        return connection_keys

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MySQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from mysql.connector import connect

        return connect


class MSSQLConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    database: t.Optional[str] = ""
    timeout: t.Optional[int] = 0
    login_timeout: t.Optional[int] = 60
    charset: t.Optional[str] = "UTF-8"
    appname: t.Optional[str] = None
    port: t.Optional[int] = 1433
    conn_properties: t.Optional[t.Union[t.Iterable[str], str]] = None
    autocommit: t.Optional[bool] = False
    tds_version: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["mssql"] = Field(alias="type", default="mssql")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "host",
            "user",
            "password",
            "database",
            "timeout",
            "login_timeout",
            "charset",
            "appname",
            "port",
            "conn_properties",
            "autocommit",
            "tds_version",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MSSQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        import pymssql

        return pymssql.connect


class SparkConnectionConfig(ConnectionConfig):
    """
    Vanilla Spark Connection Configuration. Use `DatabricksConnectionConfig` for Databricks.
    """

    config_dir: t.Optional[str] = None
    catalog: t.Optional[str] = None
    config: t.Dict[str, t.Any] = {}

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["spark"] = Field(alias="type", default="spark")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "catalog",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.SparkEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from sqlmesh.engines.spark.db_api.spark_session import connection

        return connection

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession

        spark_config = SparkConf()
        if self.config:
            for k, v in self.config.items():
                spark_config.set(k, v)

        if self.config_dir:
            os.environ["SPARK_CONF_DIR"] = self.config_dir
        return {
            "spark": SparkSession.builder.config(conf=spark_config)
            .enableHiveSupport()
            .getOrCreate(),
        }


class TrinoAuthenticationMethod(str, Enum):
    NO_AUTH = "no-auth"
    BASIC = "basic"
    LDAP = "ldap"
    KERBEROS = "kerberos"
    JWT = "jwt"
    CERTIFICATE = "certificate"
    OAUTH = "oauth"

    @property
    def is_no_auth(self) -> bool:
        return self == self.NO_AUTH

    @property
    def is_basic(self) -> bool:
        return self == self.BASIC

    @property
    def is_ldap(self) -> bool:
        return self == self.LDAP

    @property
    def is_kerberos(self) -> bool:
        return self == self.KERBEROS

    @property
    def is_jwt(self) -> bool:
        return self == self.JWT

    @property
    def is_certificate(self) -> bool:
        return self == self.CERTIFICATE

    @property
    def is_oauth(self) -> bool:
        return self == self.OAUTH


class TrinoConnectionConfig(ConnectionConfig):
    method: TrinoAuthenticationMethod = TrinoAuthenticationMethod.NO_AUTH
    host: str
    user: str
    catalog: str
    port: t.Optional[int] = None
    http_scheme: Literal["http", "https"] = "https"
    # General Optional
    roles: t.Optional[t.Dict[str, str]] = None
    http_headers: t.Optional[t.Dict[str, str]] = None
    session_properties: t.Optional[t.Dict[str, str]] = None
    retries: int = 3
    timezone: t.Optional[str] = None
    # Basic/LDAP
    password: t.Optional[str] = None
    # LDAP
    impersonation_user: t.Optional[str] = None
    # Kerberos
    keytab: t.Optional[str] = None
    krb5_config: t.Optional[str] = None
    principal: t.Optional[str] = None
    service_name: str = "trino"
    hostname_override: t.Optional[str] = None
    mutual_authentication: bool = False
    force_preemptive: bool = False
    sanitize_mutual_error_response: bool = True
    delegate: bool = False
    # JWT
    jwt_token: t.Optional[str] = None
    # Certificate
    client_certificate: t.Optional[str] = None
    client_private_key: t.Optional[str] = None
    cert: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = True

    type_: Literal["trino"] = Field(alias="type", default="trino")

    @model_validator(mode="after")
    @model_validator_v1_args
    def _root_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        port = values.get("port")
        if (
            values["http_scheme"] == "http"
            and not values["method"].is_no_auth
            and not values["method"].is_basic
        ):
            raise ConfigError("HTTP scheme can only be used with no-auth or basic method")
        if port is None:
            values["port"] = 80 if values["http_scheme"] == "http" else 443
        if (values["method"].is_ldap or values["method"].is_basic) and (
            not values["password"] or not values["user"]
        ):
            raise ConfigError(
                f"Username and Password must be provided if using {values['method'].value} authentication"
            )
        if values["method"].is_kerberos and (
            not values["principal"] or not values["keytab"] or not values["krb5_config"]
        ):
            raise ConfigError(
                "Kerberos requires the following fields: principal, keytab, and krb5_config"
            )
        if values["method"].is_jwt and not values["jwt_token"]:
            raise ConfigError("JWT requires `jwt_token` to be set")
        if values["method"].is_certificate and (
            not values["cert"]
            or not values["client_certificate"]
            or not values["client_private_key"]
        ):
            raise ConfigError(
                "Certificate requires the following fields: cert, client_certificate, and client_private_key"
            )
        return values

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        kwargs = {
            "host",
            "port",
            "catalog",
            "roles",
            "http_scheme",
            "http_headers",
            "session_properties",
            "timezone",
        }
        return kwargs

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.TrinoEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from trino.dbapi import connect

        return connect

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        from trino.auth import (
            BasicAuthentication,
            CertificateAuthentication,
            JWTAuthentication,
            KerberosAuthentication,
            OAuth2Authentication,
        )

        if self.method.is_basic or self.method.is_ldap:
            auth = BasicAuthentication(self.user, self.password)
        elif self.method.is_kerberos:
            if self.keytab:
                os.environ["KRB5_CLIENT_KTNAME"] = self.keytab
            auth = KerberosAuthentication(
                config=self.krb5_config,
                service_name=self.service_name,
                principal=self.principal,
                mutual_authentication=self.mutual_authentication,
                ca_bundle=self.cert,
                force_preemptive=self.force_preemptive,
                hostname_override=self.hostname_override,
                sanitize_mutual_error_response=self.sanitize_mutual_error_response,
                delegate=self.delegate,
            )
        elif self.method.is_oauth:
            auth = OAuth2Authentication()
        elif self.method.is_jwt:
            auth = JWTAuthentication(self.jwt_token)
        elif self.method.is_certificate:
            auth = CertificateAuthentication(self.client_certificate, self.client_private_key)
        else:
            auth = None

        return {
            "auth": auth,
            "user": self.impersonation_user or self.user,
            "max_attempts": self.retries,
            "verify": self.cert,
            "source": "sqlmesh",
        }


CONNECTION_CONFIG_TO_TYPE = {
    # Map all subclasses of ConnectionConfig to the value of their `type_` field.
    tpe.all_field_infos()["type_"].default: tpe
    for tpe in subclasses(
        __name__, ConnectionConfig, exclude=(ConnectionConfig, BaseDuckDBConnectionConfig)
    )
}


def parse_connection_config(v: t.Dict[str, t.Any]) -> ConnectionConfig:
    if "type" not in v:
        raise ConfigError("Missing connection type.")

    connection_type = v["type"]
    if connection_type not in CONNECTION_CONFIG_TO_TYPE:
        raise ConfigError(f"Unknown connection type '{connection_type}'.")

    return CONNECTION_CONFIG_TO_TYPE[connection_type](**v)


def _connection_config_validator(
    cls: t.Type, v: ConnectionConfig | t.Dict[str, t.Any] | None
) -> ConnectionConfig | None:
    if v is None or isinstance(v, ConnectionConfig):
        return v
    return parse_connection_config(v)


connection_config_validator = field_validator(
    "connection",
    "state_connection",
    "test_connection",
    "default_connection",
    "default_test_connection",
    mode="before",
    check_fields=False,
)(_connection_config_validator)


if t.TYPE_CHECKING:
    # TypeAlias hasn't been introduced until Python 3.10 which means that we can't use it
    # outside the TYPE_CHECKING guard.
    SerializableConnectionConfig: t.TypeAlias = ConnectionConfig  # type: ignore
elif PYDANTIC_MAJOR_VERSION >= 2:
    import pydantic

    # Workaround for https://docs.pydantic.dev/latest/concepts/serialization/#serializing-with-duck-typing
    SerializableConnectionConfig = pydantic.SerializeAsAny[ConnectionConfig]  # type: ignore
else:
    SerializableConnectionConfig = ConnectionConfig  # type: ignore
