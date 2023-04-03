import pytest

from sqlmesh.core.config.connection import SnowflakeConnectionConfig
from sqlmesh.utils.errors import ConfigError


def test_snowflake_auth():
    # Authenticator and user/password is fine
    SnowflakeConnectionConfig(
        account="test", user="test", password="test", authenticator="externalbrowser"
    )
    # Auth with no user/password is fine
    SnowflakeConnectionConfig(account="test", authenticator="externalbrowser")
    # No auth and no user raises
    with pytest.raises(ConfigError):
        SnowflakeConnectionConfig(account="test", password="test")
    # No auth and no password raises
    with pytest.raises(ConfigError):
        SnowflakeConnectionConfig(account="test", user="test")
    # No auth and no user/password raises
    with pytest.raises(ConfigError):
        SnowflakeConnectionConfig(account="test")
