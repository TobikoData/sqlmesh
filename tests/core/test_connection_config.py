import pydantic
import pytest

from sqlmesh.core.config.connection import ConnectionConfig, SnowflakeConnectionConfig
from sqlmesh.utils.errors import ConfigError


def test_snowflake_auth():
    # Authenticator and user/password is fine
    config = pydantic.parse_obj_as(
        ConnectionConfig,
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
    config = pydantic.parse_obj_as(
        ConnectionConfig, dict(type="snowflake", account="test", authenticator="externalbrowser")
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # No auth and no user raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        pydantic.parse_obj_as(
            ConnectionConfig, dict(type="snowflake", account="test", password="test")
        )
    # No auth and no password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        pydantic.parse_obj_as(ConnectionConfig, dict(type="snowflake", account="test", user="test"))
    # No auth and no user/password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        pydantic.parse_obj_as(ConnectionConfig, dict(type="snowflake", account="test"))
