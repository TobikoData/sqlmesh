import os
import pathlib
import re
from pathlib import Path
from unittest import mock

import pytest
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
    BigQueryConnectionConfig,
    MotherDuckConnectionConfig,
    BuiltInSchedulerConfig,
    EnvironmentSuffixTarget,
)
from sqlmesh.core.config.connection import DuckDBAttachOptions, RedshiftConnectionConfig
from sqlmesh.core.config.feature_flag import DbtFeatureFlag, FeatureFlag
from sqlmesh.core.config.loader import (
    load_config_from_env,
    load_config_from_paths,
    load_config_from_python_module,
)
from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter.athena import AthenaEngineAdapter
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.engine_adapter.redshift import RedshiftEngineAdapter
from sqlmesh.core.loader import MigratedDbtProjectLoader
from sqlmesh.core.notification_target import ConsoleNotificationTarget
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError
from tests.utils.test_filesystem import create_temp_file


@pytest.fixture(scope="session")
def yaml_config_path(tmp_path_factory) -> Path:
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    another_gateway:
        connection:
            type: duckdb
            database: test_db

model_defaults:
    dialect: ''
        """
        )
    return config_path


@pytest.fixture(scope="session")
def python_config_path(tmp_path_factory) -> Path:
    config_path = tmp_path_factory.mktemp("python_config") / "config.py"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig, ModelDefaultsConfig
config = Config(gateways=GatewayConfig(connection=DuckDBConnectionConfig()), model_defaults=ModelDefaultsConfig(dialect=''))
        """
        )
    return config_path


def test_update_with_gateways():
    gateway0_config = GatewayConfig(connection=DuckDBConnectionConfig())
    gateway1_config = GatewayConfig(connection=DuckDBConnectionConfig(database="test"))

    assert Config(gateways=gateway0_config).update_with(
        Config(gateways={"gateway1": gateway1_config})
    ) == Config(gateways={"": gateway0_config, "gateway1": gateway1_config})

    assert Config(gateways={"gateway1": gateway1_config}).update_with(
        Config(gateways=gateway0_config)
    ) == Config(gateways={"gateway1": gateway1_config, "": gateway0_config})

    assert Config(gateways=gateway0_config).update_with(Config(gateways=gateway1_config)) == Config(
        gateways=gateway1_config
    )

    assert Config(gateways={"gateway0": gateway0_config}).update_with(
        Config(gateways={"gateway1": gateway1_config})
    ) == Config(gateways={"gateway0": gateway0_config, "gateway1": gateway1_config})


def test_update_with_users():
    user_a = User(username="a")
    user_b = User(username="b")

    assert Config(users=[user_a]).update_with(Config(users=[user_b])) == Config(
        users=[user_a, user_b]
    )


def test_update_with_ignore_patterns():
    pattern_a = "pattern_a"
    pattern_b = "pattern_b"

    assert Config(ignore_patterns=[pattern_a]).update_with(
        Config(ignore_patterns=[pattern_b])
    ) == Config(ignore_patterns=[pattern_a, pattern_b])


def test_update_with_notification_targets():
    target = ConsoleNotificationTarget()

    assert Config(notification_targets=[target]).update_with(
        Config(notification_targets=[target])
    ) == Config(notification_targets=[target] * 2)


def test_update_with_model_defaults():
    config_a = Config(model_defaults=ModelDefaultsConfig(start="2022-01-01", dialect="duckdb"))
    config_b = Config(model_defaults=ModelDefaultsConfig(dialect="spark"))

    assert config_a.update_with(config_b) == Config(
        model_defaults=ModelDefaultsConfig(start="2022-01-01", dialect="spark")
    )


def test_default_gateway():
    gateway_a = GatewayConfig(connection=DuckDBConnectionConfig(database="test_db_a"))
    gateway_b = GatewayConfig(connection=DuckDBConnectionConfig(database="test_db_b"))
    gateway_c = GatewayConfig(connection=DuckDBConnectionConfig(database="test_db_c"))

    config = Config(
        gateways={
            "gateway1": gateway_b,
            "gateway2": gateway_c,
            "": gateway_a,
        },
    )

    assert config.get_gateway() == gateway_a

    assert config.copy(update={"default_gateway": "gateway2"}).get_gateway() == gateway_c

    assert (
        Config(
            gateways={
                "gateway1": gateway_b,
                "gateway2": gateway_c,
            },
        ).get_gateway()
        == gateway_b
    )

    with pytest.raises(
        ConfigError,
        match="Missing gateway with name 'missing'",
    ):
        config.copy(update={"default_gateway": "missing"}).get_gateway()


def test_load_config_from_paths(yaml_config_path: Path, python_config_path: Path):
    config: Config = load_config_from_paths(
        Config,
        project_paths=[yaml_config_path, python_config_path],
    )

    assert config == Config(
        gateways={  # type: ignore
            "another_gateway": GatewayConfig(connection=DuckDBConnectionConfig(database="test_db")),
            "": GatewayConfig(connection=DuckDBConnectionConfig()),
        },
        model_defaults=ModelDefaultsConfig(dialect=""),
    )


def test_load_config_multiple_config_files_in_folder(tmp_path):
    config_a_path = tmp_path / "config.yaml"
    with open(config_a_path, "w", encoding="utf-8") as fd:
        fd.write("project: project_a")

    config_b_path = tmp_path / "config.yml"
    with open(config_b_path, "w", encoding="utf-8") as fd:
        fd.write("project: project_b")

    with pytest.raises(ConfigError, match=r"^Multiple configuration files found in folder.*"):
        load_config_from_paths(Config, project_paths=[config_a_path, config_b_path])


def test_load_config_no_config():
    with pytest.raises(ConfigError, match=r"^SQLMesh project config could not be found.*"):
        load_config_from_paths(Config, load_from_env=False)


def test_load_config_no_dialect(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
gateways:
    local:
        connection:
            type: duckdb
            database: db.db
""",
    )

    create_temp_file(
        tmp_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.core.config import Config, DuckDBConnectionConfig

config = Config(default_connection=DuckDBConnectionConfig())
""",
    )

    with pytest.raises(
        ConfigError, match=r"^Default model SQL dialect is a required configuration parameter.*"
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

    with pytest.raises(
        ConfigError, match=r"^Default model SQL dialect is a required configuration parameter.*"
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.py"])


def test_load_config_bad_model_default_key(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
gateways:
    local:
        connection:
            type: duckdb
            database: db.db

model_defaults:
    dialect: ''
    test: 1
""",
    )

    with pytest.raises(
        ConfigError, match=r"^'test' is not a valid model default configuration key.*"
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])


def test_load_config_unsupported_extension(tmp_path):
    config_path = tmp_path / "config.txt"
    config_path.touch()

    with pytest.raises(ConfigError, match=r"^Unsupported config file extension 'txt'.*"):
        load_config_from_paths(Config, project_paths=[config_path])


def test_load_python_config_with_personal_config(tmp_path):
    create_temp_file(
        tmp_path / "personal",
        pathlib.Path("config.yaml"),
        """
gateways:
    local:
        connection:
            type: duckdb
            database: db.db
""",
    )

    create_temp_file(
        tmp_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.core.config import Config, DuckDBConnectionConfig, ModelDefaultsConfig

custom_config = Config(default_connection=DuckDBConnectionConfig(), model_defaults=ModelDefaultsConfig(dialect="duckdb"))
""",
    )
    config = load_config_from_paths(
        Config,
        project_paths=[tmp_path / "config.py"],
        personal_paths=[tmp_path / "personal" / "config.yaml"],
        config_name="custom_config",
    )
    assert config.gateways["local"].connection.database == "db.db"
    assert config.default_connection.database is None
    assert config.model_defaults.dialect == "duckdb"


def test_load_config_from_env():
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__GATEWAY__CONNECTION__TYPE": "duckdb",
            "SQLMESH__GATEWAY__CONNECTION__DATABASE": "test_db",
            "SQLMESH__FEATURE_FLAGS__DBT__SCD_TYPE_2_SUPPORT": "false",
        },
    ):
        assert Config.parse_obj(load_config_from_env()) == Config(
            gateways=GatewayConfig(connection=DuckDBConnectionConfig(database="test_db")),
            feature_flags=FeatureFlag(dbt=DbtFeatureFlag(scd_type_2_support=False)),
        )


def test_load_config_from_env_fails():
    with mock.patch.dict(os.environ, {"SQLMESH__GATEWAYS__ABCDEF__CONNECTION__PASSWORD": "..."}):
        with pytest.raises(
            ConfigError,
            match="Missing connection type.\n\nVerify your config.yaml and environment variables.",
        ):
            Config.parse_obj(load_config_from_env())


def test_load_config_from_env_no_config_vars():
    with mock.patch.dict(
        os.environ,
        {
            "DUMMY_ENV_VAR": "dummy",
        },
    ):
        assert load_config_from_env() == {}


def test_load_config_from_env_invalid_variable_name():
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__": "",
        },
    ):
        with pytest.raises(
            ConfigError,
            match="Invalid SQLMesh configuration variable 'sqlmesh__'.",
        ):
            load_config_from_env()


def test_load_yaml_config_env_var_gateway_override(tmp_path_factory):
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    testing:
        connection:
            type: motherduck
            database: blah
model_defaults:
    dialect: bigquery
        """
        )
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__GATEWAYS__TESTING__STATE_CONNECTION__TYPE": "bigquery",
            "SQLMESH__GATEWAYS__TESTING__STATE_CONNECTION__CHECK_IMPORT": "false",
            "SQLMESH__DEFAULT_GATEWAY": "testing",
        },
    ):
        assert load_config_from_paths(
            Config,
            project_paths=[config_path],
        ) == Config(
            gateways={
                "testing": GatewayConfig(
                    connection=MotherDuckConnectionConfig(database="blah"),
                    state_connection=BigQueryConnectionConfig(check_import=False),
                ),
            },
            model_defaults=ModelDefaultsConfig(dialect="bigquery"),
            default_gateway="testing",
        )


def test_load_py_config_env_var_gateway_override(tmp_path_factory):
    config_path = tmp_path_factory.mktemp("python_config") / "config.py"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig, ModelDefaultsConfig
config = Config(gateways={"duckdb_gateway": GatewayConfig(connection=DuckDBConnectionConfig())}, model_defaults=ModelDefaultsConfig(dialect=''))
        """
        )
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__GATEWAYS__DUCKDB_GATEWAY__STATE_CONNECTION__TYPE": "bigquery",
            "SQLMESH__GATEWAYS__DUCKDB_GATEWAY__STATE_CONNECTION__CHECK_IMPORT": "false",
            "SQLMESH__DEFAULT_GATEWAY": "duckdb_gateway",
        },
    ):
        config = load_config_from_paths(
            Config,
            project_paths=[config_path],
        )
        assert config == Config(
            gateways={  # type: ignore
                "duckdb_gateway": GatewayConfig(
                    connection=DuckDBConnectionConfig(),
                    state_connection=BigQueryConnectionConfig(check_import=False),
                ),
            },
            model_defaults=ModelDefaultsConfig(dialect=""),
            default_gateway="duckdb_gateway",
        )


def test_load_config_from_python_module_missing_config(tmp_path):
    config_path = tmp_path / "missing_config.py"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write("from sqlmesh.core.config import Config")

    with pytest.raises(ConfigError, match="Config 'config' was not found."):
        load_config_from_python_module(Config, config_path)


def test_load_config_from_python_module_invalid_config_object(tmp_path):
    config_path = tmp_path / "invalid_config.py"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write("config = None")

    with pytest.raises(
        ConfigError,
        match=r"^Config needs to be a valid object.*",
    ):
        load_config_from_python_module(Config, config_path)


@pytest.mark.parametrize(
    [
        "mapping",
        "expected",
        "dialect",
        "raise_error",
    ],
    [
        (
            "'^dev$': dev_catalog\n    '^other$': other_catalog",
            {re.compile("^dev$"): "dev_catalog", re.compile("^other$"): "other_catalog"},
            "duckdb",
            "",
        ),
        (
            "'^(?!prod$)': dev",
            {re.compile("^(?!prod$)"): "dev"},
            "duckdb",
            "",
        ),
        (
            "'^dev$': dev_catalog\n    '[': other_catalog",
            {},
            "duckdb",
            "`\\[` is not a valid regular expression.",
        ),
        (
            "'^prod$': prod_catalog",
            {re.compile("^prod$"): "PROD_CATALOG"},
            "snowflake",
            "",
        ),
    ],
)
def test_environment_catalog_mapping(tmp_path_factory, mapping, expected, dialect, raise_error):
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            f"""
gateways:
    local:
        connection:
            type: duckdb

model_defaults:
    dialect: {dialect}

environment_catalog_mapping:
    {mapping}
        """
        )
    if raise_error:
        with pytest.raises(ConfigError, match=raise_error):
            load_config_from_paths(
                Config,
                project_paths=[config_path],
            )
    else:
        assert (
            load_config_from_paths(Config, project_paths=[config_path]).environment_catalog_mapping
            == expected
        )


def test_physical_schema_mapping_mutually_exclusive_with_physical_schema_override() -> None:
    Config(physical_schema_override={"foo": "bar"})  # type: ignore
    Config(physical_schema_mapping={"^foo$": "bar"})

    with pytest.raises(
        ConfigError, match=r"Only one.*physical_schema_override.*physical_schema_mapping"
    ):
        Config(physical_schema_override={"foo": "bar"}, physical_schema_mapping={"^foo$": "bar"})  # type: ignore


def test_load_feature_flag(tmp_path_factory):
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    duckdb_gateway:
        connection:
            type: duckdb
model_defaults:
    dialect: bigquery
feature_flags:
    dbt:
        scd_type_2_support: false
        """
        )

    assert load_config_from_paths(
        Config,
        project_paths=[config_path],
    ) == Config(
        gateways={
            "duckdb_gateway": GatewayConfig(connection=DuckDBConnectionConfig()),
        },
        model_defaults=ModelDefaultsConfig(dialect="bigquery"),
        feature_flags=FeatureFlag(dbt=DbtFeatureFlag(scd_type_2_support=False)),
    )


def test_load_alternative_config_type(yaml_config_path: Path, python_config_path: Path):
    class DerivedConfig(Config):
        pass

    config = load_config_from_paths(
        DerivedConfig,
        project_paths=[yaml_config_path, python_config_path],
    )

    assert config == DerivedConfig(
        gateways={  # type: ignore
            "another_gateway": GatewayConfig(connection=DuckDBConnectionConfig(database="test_db")),
            "": GatewayConfig(connection=DuckDBConnectionConfig()),
        },
        model_defaults=ModelDefaultsConfig(dialect=""),
    )


def test_connection_config_serialization():
    config = Config(
        default_connection=DuckDBConnectionConfig(database="my_db"),
        default_test_connection=DuckDBConnectionConfig(database="my_test_db"),
    )
    serialized = config.dict()
    assert serialized["default_connection"] == {
        "concurrent_tasks": 1,
        "register_comments": True,
        "type": "duckdb",
        "extensions": [],
        "pre_ping": False,
        "pretty_sql": False,
        "connector_config": {},
        "secrets": [],
        "database": "my_db",
    }
    assert serialized["default_test_connection"] == {
        "concurrent_tasks": 1,
        "register_comments": True,
        "type": "duckdb",
        "extensions": [],
        "pre_ping": False,
        "pretty_sql": False,
        "connector_config": {},
        "secrets": [],
        "database": "my_test_db",
    }


def test_variables():
    variables = {
        "int_var": 1,
        "str_var": "test_value",
        "bool_var": True,
        "float_var": 1.0,
        "list_var": [1, 2, 3],
        "dict_var": {"a": "test_value", "b": 2},
    }
    gateway_variables = {
        "UPPERCASE_VAR": 2,
    }
    config = Config(
        variables=variables, gateways={"local": GatewayConfig(variables=gateway_variables)}
    )
    assert config.variables == variables
    assert config.get_gateway("local").variables == {"uppercase_var": 2}

    with pytest.raises(
        ConfigError, match="Unsupported variable value type: <class 'sqlglot.expressions.Column'>"
    ):
        Config(variables={"invalid_var": exp.column("sqlglot_expr")})


def test_load_duckdb_attach_config(tmp_path):
    config_path = tmp_path / "config_duckdb_attach.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    another_gateway:
        connection:
            type: duckdb
            catalogs:
              memory: ':memory:'
              sqlite:
                type: 'sqlite'
                path: 'test.db'
              postgres:
                type: 'postgres'
                path: 'dbname=postgres user=postgres host=127.0.0.1'
                read_only: true
model_defaults:
    dialect: ''
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    assert config.gateways["another_gateway"].connection.catalogs.get("memory") == ":memory:"

    attach_config_1 = config.gateways["another_gateway"].connection.catalogs.get("sqlite")

    assert isinstance(attach_config_1, DuckDBAttachOptions)
    assert attach_config_1.type == "sqlite"
    assert attach_config_1.path == "test.db"
    assert attach_config_1.read_only is False

    attach_config_2 = config.gateways["another_gateway"].connection.catalogs.get("postgres")

    assert isinstance(attach_config_2, DuckDBAttachOptions)
    assert attach_config_2.type == "postgres"
    assert attach_config_2.path == "dbname=postgres user=postgres host=127.0.0.1"
    assert attach_config_2.read_only is True


def test_load_model_defaults_audits(tmp_path):
    config_path = tmp_path / "config_model_defaults_audits.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
model_defaults:
    dialect: ''
    audits:
        - assert_positive_order_ids
        - does_not_exceed_threshold(column := id, threshold := 1000)
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    assert len(config.model_defaults.audits) == 2
    assert config.model_defaults.audits[0] == ("assert_positive_order_ids", {})
    assert config.model_defaults.audits[1][0] == "does_not_exceed_threshold"
    assert type(config.model_defaults.audits[1][1]["column"]) == exp.Column
    assert config.model_defaults.audits[1][1]["column"].this.this == "id"
    assert type(config.model_defaults.audits[1][1]["threshold"]) == exp.Literal
    assert config.model_defaults.audits[1][1]["threshold"].this == "1000"


def test_scheduler_config(tmp_path_factory):
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    builtin_gateway:
        scheduler:
            type: builtin

default_scheduler:
    type: builtin

model_defaults:
    dialect: bigquery
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )
    assert isinstance(config.default_scheduler, BuiltInSchedulerConfig)
    assert isinstance(config.get_gateway("builtin_gateway").scheduler, BuiltInSchedulerConfig)


def test_multi_gateway_config(tmp_path, mocker: MockerFixture):
    config_path = tmp_path / "config_athena_redshift.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    redshift:
        connection:
            type: redshift
            user: user
            password: '1234'
            host: host
            database: db
        test_connection:
            type: redshift
            database: test_db
        state_connection:
            type: duckdb
            database: state.db
    athena:
        connection:
            type: athena
            aws_access_key_id: '1234'
            aws_secret_access_key: accesskey
            work_group: group
            s3_warehouse_location: s3://location
    duckdb:
        connection:
            type: duckdb
            database: db.db

default_gateway: redshift

model_defaults:
    dialect: redshift
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    ctx = Context(paths=tmp_path, config=config)

    assert isinstance(ctx.connection_config, RedshiftConnectionConfig)
    assert len(ctx.engine_adapters) == 3
    assert isinstance(ctx.engine_adapters["athena"], AthenaEngineAdapter)
    assert isinstance(ctx.engine_adapters["redshift"], RedshiftEngineAdapter)
    assert isinstance(ctx.engine_adapters["duckdb"], DuckDBEngineAdapter)
    assert ctx.engine_adapter == ctx._get_engine_adapter("redshift")

    # The duckdb engine adapter should be have been set as multithreaded as well
    assert ctx.engine_adapters["duckdb"]._multithreaded


def test_multi_gateway_single_threaded_config(tmp_path):
    config_path = tmp_path / "config_duck_athena.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    duckdb:
        connection:
            type: duckdb
            database: db.db
    athena:
        connection:
            type: athena
            aws_access_key_id: '1234'
            aws_secret_access_key: accesskey
            work_group: group
            s3_warehouse_location: s3://location
default_gateway: duckdb
model_defaults:
    dialect: duckdb
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    ctx = Context(paths=tmp_path, config=config)
    assert isinstance(ctx.connection_config, DuckDBConnectionConfig)
    assert len(ctx.engine_adapters) == 2
    assert ctx.engine_adapter == ctx._get_engine_adapter("duckdb")
    assert isinstance(ctx.engine_adapters["athena"], AthenaEngineAdapter)

    # In this case athena should use 1 concurrent task as the default gateway is duckdb
    assert not ctx.engine_adapters["athena"]._multithreaded


def test_trino_schema_location_mapping_syntax(tmp_path):
    config_path = tmp_path / "config_trino.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
    gateways:
      trino:
        connection:
          type: trino
          user: trino
          host: trino
          catalog: trino
          schema_location_mapping:
            '^utils$': 's3://utils-bucket/@{schema_name}'
            '^landing\\..*$': 's3://raw-data/@{catalog_name}/@{schema_name}'

    default_gateway: trino

    model_defaults:
      dialect: trino
    """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    from sqlmesh.core.config.connection import TrinoConnectionConfig

    conn = config.gateways["trino"].connection
    assert isinstance(conn, TrinoConnectionConfig)

    assert len(conn.schema_location_mapping) == 2


def test_gcp_postgres_ip_and_scopes(tmp_path):
    config_path = tmp_path / "config_gcp_postgres.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
    gateways:
      gcp_postgres:
        connection:
          type: gcp_postgres
          check_import: false
          instance_connection_string: something
          user: user
          password: password
          db: db
          ip_type: private
          scopes:
          - https://www.googleapis.com/auth/cloud-platform
          - https://www.googleapis.com/auth/sqlservice.admin

    default_gateway: gcp_postgres

    model_defaults:
      dialect: postgres
    """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    from sqlmesh.core.config.connection import GCPPostgresConnectionConfig

    conn = config.gateways["gcp_postgres"].connection
    assert isinstance(conn, GCPPostgresConnectionConfig)

    assert len(conn.scopes) == 2
    assert conn.scopes[0] == "https://www.googleapis.com/auth/cloud-platform"
    assert conn.scopes[1] == "https://www.googleapis.com/auth/sqlservice.admin"
    assert conn.ip_type == "private"


def test_gateway_model_defaults(tmp_path):
    global_defaults = ModelDefaultsConfig(
        dialect="snowflake", owner="foo", optimize_query=True, enabled=True, cron="@daily"
    )
    gateway_defaults = ModelDefaultsConfig(dialect="duckdb", owner="baz", optimize_query=False)

    config = Config(
        gateways={
            "duckdb": GatewayConfig(
                connection=DuckDBConnectionConfig(database="db.db"),
                model_defaults=gateway_defaults,
            )
        },
        model_defaults=global_defaults,
        default_gateway="duckdb",
    )

    ctx = Context(paths=tmp_path, config=config, gateway="duckdb")

    expected = ModelDefaultsConfig(
        dialect="duckdb", owner="baz", optimize_query=False, enabled=True, cron="@daily"
    )

    assert ctx.config.model_defaults == expected


def test_redshift_merge_flag(tmp_path, mocker: MockerFixture):
    config_path = tmp_path / "config_redshift_merge.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    redshift:
        connection:
            type: redshift
            user: user
            password: '1234'
            host: host
            database: db
            enable_merge: true
    default:
        connection:
            type: redshift
            user: user
            password: '1234'
            host: host
            database: db

default_gateway: redshift

model_defaults:
    dialect: redshift
        """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )
    redshift_connection = config.get_connection("redshift")
    assert isinstance(redshift_connection, RedshiftConnectionConfig)
    assert redshift_connection.enable_merge
    adapter = redshift_connection.create_engine_adapter()
    assert isinstance(adapter, RedshiftEngineAdapter)
    assert adapter.enable_merge

    adapter_2 = config.get_connection("default").create_engine_adapter()
    assert isinstance(adapter_2, RedshiftEngineAdapter)
    assert not adapter_2.enable_merge


def test_environment_statements_config(tmp_path):
    config_path = tmp_path / "config_before_after_all.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
    gateways:
      postgres:
        connection:
          type: postgres
          database: db
          user: postgres
          password: postgres
          host: localhost
          port: 5432

    default_gateway: postgres

    before_all:
    - CREATE TABLE IF NOT EXISTS custom_analytics (physical_table VARCHAR, evaluation_time VARCHAR);
    after_all:
    - "@grant_schema_privileges()"
    - "GRANT REFERENCES ON FUTURE VIEWS IN DATABASE db TO ROLE admin_role;"

    model_defaults:
      dialect: postgres
    """
        )

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    assert config.before_all == [
        "CREATE TABLE IF NOT EXISTS custom_analytics (physical_table VARCHAR, evaluation_time VARCHAR);"
    ]
    assert config.after_all == [
        "@grant_schema_privileges()",
        "GRANT REFERENCES ON FUTURE VIEWS IN DATABASE db TO ROLE admin_role;",
    ]


# https://github.com/TobikoData/sqlmesh/pull/4049
def test_pydantic_import_error() -> None:
    class TestConfig(DuckDBConnectionConfig):
        pass

    TestConfig()


def test_config_subclassing() -> None:
    class ConfigSubclass(Config): ...

    ConfigSubclass()


def test_config_complex_types_supplied_as_json_strings_from_env(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("""
    gateways:
      bigquery:
        connection:
          type: bigquery
          project: unit-test

    default_gateway: bigquery

    model_defaults:
      dialect: bigquery
""")
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__GATEWAYS__BIGQUERY__CONNECTION__SCOPES": '     ["a","b","c"]',  # note: leading whitespace is deliberate
            "SQLMESH__GATEWAYS__BIGQUERY__CONNECTION__KEYFILE_JSON": '{ "foo": "bar" }',
        },
    ):
        config = load_config_from_paths(
            Config,
            project_paths=[config_path],
        )

        conn = config.gateways["bigquery"].connection
        assert isinstance(conn, BigQueryConnectionConfig)

        assert conn.project == "unit-test"
        assert conn.scopes == ("a", "b", "c")
        assert conn.keyfile_json == {"foo": "bar"}


def test_loader_for_migrated_dbt_project(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("""
    gateways:
      bigquery:
        connection:
          type: bigquery
          project: unit-test

    default_gateway: bigquery

    model_defaults:
      dialect: bigquery
                           
    variables:    
      __dbt_project_name__: sushi                           
""")

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    assert config.loader == MigratedDbtProjectLoader


def test_environment_suffix_target_catalog(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("""
    gateways:
      warehouse:
        connection:
          type: duckdb
                           
    default_gateway: warehouse

    model_defaults:
      dialect: duckdb
                           
    environment_suffix_target: catalog                          
""")

    config = load_config_from_paths(
        Config,
        project_paths=[config_path],
    )

    assert config.environment_suffix_target == EnvironmentSuffixTarget.CATALOG
    assert not config.environment_catalog_mapping

    config_path.write_text("""
    gateways:
      warehouse:
        connection:
          type: duckdb
                           
    default_gateway: warehouse

    model_defaults:
      dialect: duckdb
                           
    environment_suffix_target: catalog             

    environment_catalog_mapping:
      '.*': "foo"
""")

    with pytest.raises(ConfigError, match=r"mutually exclusive"):
        config = load_config_from_paths(
            Config,
            project_paths=[config_path],
        )
