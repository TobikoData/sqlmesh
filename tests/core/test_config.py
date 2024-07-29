import os
import pathlib
import re
from pathlib import Path
from unittest import mock

import pytest
from sqlglot import exp

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.config.connection import DuckDBAttachOptions
from sqlmesh.core.config.feature_flag import DbtFeatureFlag, FeatureFlag
from sqlmesh.core.config.loader import (
    load_config_from_env,
    load_config_from_paths,
    load_config_from_python_module,
)
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


def test_cloud_composer_scheduler_config(tmp_path_factory):
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
gateways:
    another_gateway:
        connection:
            type: duckdb
            database: test_db
        scheduler:
            type: cloud_composer
            airflow_url: https://airflow.url

model_defaults:
    dialect: bigquery
        """
        )

    assert load_config_from_paths(
        Config,
        project_paths=[config_path],
    )


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
        "connector_config": {},
        "database": "my_db",
    }
    assert serialized["default_test_connection"] == {
        "concurrent_tasks": 1,
        "register_comments": True,
        "type": "duckdb",
        "extensions": [],
        "pre_ping": False,
        "connector_config": {},
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
