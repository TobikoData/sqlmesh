import os
from pathlib import Path
from unittest import mock

import pytest

from sqlmesh.core.config import Config, DuckDBConnectionConfig, ModelDefaultsConfig
from sqlmesh.core.config.loader import (
    load_config_from_env,
    load_config_from_paths,
    load_config_from_python_module,
)
from sqlmesh.core.notification_target import ConsoleNotificationTarget
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError


@pytest.fixture(scope="session")
def yaml_config_path(tmp_path_factory) -> Path:
    config_path = tmp_path_factory.mktemp("yaml_config") / "config.yaml"
    with open(config_path, "w") as fd:
        fd.write(
            """
connections:
    another_connection:
        type: duckdb
        database: test_db
        """
        )
    return config_path


@pytest.fixture(scope="session")
def python_config_path(tmp_path_factory) -> Path:
    config_path = tmp_path_factory.mktemp("python_config") / "config.py"
    with open(config_path, "w") as fd:
        fd.write(
            """from sqlmesh.core.config import Config, DuckDBConnectionConfig
config = Config(connection=DuckDBConnectionConfig())
        """
        )
    return config_path


def test_update_with_connections():
    conn0_config = DuckDBConnectionConfig()
    conn1_config = DuckDBConnectionConfig(database="test")

    assert Config(connections=conn0_config).update_with(
        Config(connections={"conn1": conn1_config})
    ) == Config(connections={"": conn0_config, "conn1": conn1_config})

    assert Config(connections={"conn1": conn1_config}).update_with(
        Config(connections=conn0_config)
    ) == Config(connections={"conn1": conn1_config, "": conn0_config})

    assert Config(connections=conn0_config).update_with(Config(connections=conn1_config)) == Config(
        connections=conn1_config
    )

    assert Config(connections={"conn0": conn0_config}).update_with(
        Config(connections={"conn1": conn1_config})
    ) == Config(connections={"conn0": conn0_config, "conn1": conn1_config})


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


def test_default_connection():
    conn_a = DuckDBConnectionConfig(database="test_db_a")
    conn_b = DuckDBConnectionConfig(database="test_db_b")
    conn_c = DuckDBConnectionConfig(database="test_db_c")

    config = Config(
        connections={
            "conn1": conn_b,
            "conn2": conn_c,
            "": conn_a,
        },
    )

    assert config.get_connection() == conn_a

    assert config.copy(update={"default_connection": "conn2"}).get_connection() == conn_c

    assert (
        Config(
            connections={
                "conn1": conn_b,
                "conn2": conn_c,
            },
        ).get_connection()
        == conn_b
    )

    with pytest.raises(
        ConfigError,
        match="Missing connection with name 'missing'",
    ):
        config.copy(update={"default_connection": "missing"}).get_connection()


def test_load_config_from_paths(yaml_config_path: Path, python_config_path: Path):
    config = load_config_from_paths(yaml_config_path, python_config_path)

    assert config == Config(
        connections={
            "another_connection": DuckDBConnectionConfig(database="test_db"),
            "": DuckDBConnectionConfig(),
        }
    )


def test_load_config_multiple_config_files_in_folder(tmp_path):
    config_a_path = tmp_path / "config.yaml"
    with open(config_a_path, "w") as fd:
        fd.write("physical_schema: schema_a")

    config_b_path = tmp_path / "config.yml"
    with open(config_b_path, "w") as fd:
        fd.write("physical_schema: schema_b")

    with pytest.raises(ConfigError, match=r"^Multiple configuration files found in folder.*"):
        load_config_from_paths(config_a_path, config_b_path)


def test_load_config_no_config():
    with pytest.raises(ConfigError, match=r"^SQLMesh config could not be found.*"):
        load_config_from_paths(load_from_env=False)


def test_load_config_unsupported_extension(tmp_path):
    config_path = tmp_path / "config.txt"
    config_path.touch()

    with pytest.raises(ConfigError, match=r"^Unsupported config file extension 'txt'.*"):
        load_config_from_paths(config_path)


def test_load_config_from_env():
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__CONNECTION__TYPE": "duckdb",
            "SQLMESH__CONNECTION__DATABASE": "test_db",
        },
    ):
        assert load_config_from_env() == Config(
            connections=DuckDBConnectionConfig(database="test_db")
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
    with open(config_path, "w") as fd:
        fd.write("from sqlmesh.core.config import Config")

    with pytest.raises(ConfigError, match="Config 'config' was not found."):
        load_config_from_python_module(config_path)


def test_load_config_from_python_module_invalid_config_object(tmp_path):
    config_path = tmp_path / "invalid_config.py"
    with open(config_path, "w") as fd:
        fd.write("config = None")

    with pytest.raises(
        ConfigError,
        match=r"^Config needs to be a valid object.*",
    ):
        load_config_from_python_module(config_path)
