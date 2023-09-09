import pytest

from sqlmesh.core.config.connection import (
    SnowflakeConnectionConfig,
    _connection_config_validator,
)
from sqlmesh.utils.errors import ConfigError


def test_snowflake_auth():
    # Authenticator and user/password is fine
    config = _connection_config_validator(
        None,
        dict(
            type="snowflake",
            account="test",
            user="test",
            password="test",
            authenticator="externalbrowser",
        ),
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # Auth with no user/password is fine
    config = _connection_config_validator(
        None, dict(type="snowflake", account="test", authenticator="externalbrowser")
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # No auth and no user raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        _connection_config_validator(None, dict(type="snowflake", account="test", password="test"))
    # No auth and no password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        _connection_config_validator(None, dict(type="snowflake", account="test", user="test"))
    # No auth and no user/password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        _connection_config_validator(None, dict(type="snowflake", account="test"))


def test_validator():
    assert _connection_config_validator(None, None) is None

    snowflake_config = SnowflakeConnectionConfig(account="test", authenticator="externalbrowser")
    assert _connection_config_validator(None, snowflake_config) == snowflake_config

    assert (
        _connection_config_validator(
            None, dict(type="snowflake", account="test", authenticator="externalbrowser")
        )
        == snowflake_config
    )

    with pytest.raises(ConfigError, match="Missing connection type."):
        _connection_config_validator(None, dict(account="test", authenticator="externalbrowser"))

    with pytest.raises(ConfigError, match="Unknown connection type 'invalid'."):
        _connection_config_validator(
            None, dict(type="invalid", account="test", authenticator="externalbrowser")
        )
