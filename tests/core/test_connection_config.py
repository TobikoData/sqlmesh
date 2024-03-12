import base64
import typing as t

import pytest
from _pytest.fixtures import FixtureRequest

from sqlmesh.core.config.connection import (
    BigQueryConnectionConfig,
    ConnectionConfig,
    DuckDBConnectionConfig,
    SnowflakeConnectionConfig,
    TrinoAuthenticationMethod,
    _connection_config_validator,
)
from sqlmesh.utils.errors import ConfigError


@pytest.fixture
def make_config() -> t.Callable:
    def _make_function(**kwargs) -> ConnectionConfig:
        return _connection_config_validator(  # type: ignore
            None,  # type: ignore
            kwargs,
        )

    return _make_function


@pytest.fixture
def snowflake_key_no_passphrase_path() -> str:
    # openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
    return "tests/fixtures/snowflake/rsa_key_no_pass.p8"


@pytest.fixture
def snowflake_key_no_passphrase_pem(snowflake_key_no_passphrase_path) -> str:
    with open(snowflake_key_no_passphrase_path, "r") as key:
        return key.read()


@pytest.fixture
def snowflake_key_no_passphrase_b64() -> str:
    return "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCvMKgsYzoDMnl7QW9nWTzAMMQToyUTslgKlH9MezcEYUvvCv+hYEsY9YGQ5dhI5MSY1vkQ+Wtqc6KsvJQzMaHDA1W+Z5R/yA/IY+Mp2KqJijQxnp8XjZs1t6Unr0ssL2yBjlk2pNOZX3w4A6B6iwpkqUi/HtqI5t2M15FrUMF3rNcH68XMcDa1gAasGuBpzJtBM0bp4/cHa18xWZZfu3d2d+4CCfYUvE3OYXQXMjJunidnU56NZtYlJcKT8Fmlw16fSFsPAG01JOIWBLJmSMi5qhhB2w90AAq5URuupCbwBKB6KvwzPRWn+fZKGAvvlR7P3CGebwBJEJxnq85MljzRAgMBAAECggEAKXaTpwXJGi6dD+35xvUY6sff8GHhiZrhOYfR5TEYYWIBzc7Fl9UpkPuyMbAkk4QJf78JbdoKcURzEP0E+mTZy0UDyy/Ktr+L9LqnbiUIn8rk9YV8U9/BB2KypQTY/tkuji85sDQsnJU72ioJlldIG3DxdcKAqHwznXz7vvF7CK6rcsz37hC5w7MTtguvtzNyHGkvJ1ZBTHI1vvGR/VQJoSSFkv6nLFs2xl197kuM2x+Ss539Xbg7GGXX90/sgJP+QLyNk6kYezekRt5iCK6n3UxNfEqd0GX03AJ1oVtFM9SLx0RMHiLuXVCKlQLJ1LYf8zOT31yOun6hhowNmHvpLQKBgQDzXGQqBLvVNi9gQzQhG6oWXxdtoBILnGnd8DFsb0YZIe4PbiyoFb8b4tJuGz4GVfugeZYL07I8TsQbPKFH3tqFbx69hENMUOo06PZ4H7phucKk8Er/JHW8dhkVQVg1ttTK8J5kOm+uKjirqN5OkLlUNSSJMblaEr9AHGPmTu21MwKBgQC4SeYzJDvq/RTQk5d7AwVEokgFk95aeyv77edFAhnrD3cPIAQnPlfVyG7RgPA94HrSAQ5Hr0PL2hiQ7OxX1HfP+66FMcTVbZwktYULZuj4NMxJqwxKbCmmzzACiPF0sibg8efGMY9sAmcQRw5JRS2s6FQns1MqeksnjzyMf3196wKBgFf8zJ5AjeT9rU1hnuRliy6BfQf+uueFyuUaZdQtuyt1EAx2KiEvk6QycyCqKtfBmLOhojVued/CHrc2SZ2hnmJmFbgxrN9X1gYBQLOXzRxuPEjENGlhNkxIarM7p/frva4OJ0ZXtm9DBrBR4uaG/urKOAZ+euRtKMa2PQxU9y7vAoGAeZWX4MnZFjIe13VojWnywdNnPPbPzlZRMIdG+8plGyY64Km408NX492271XoKoq9vWug5j6FtiqP5p3JWDD/UyKzg4DQYhdM2xM/UcR1k7wRw9Cr7TXrTPiIrkN3OgyHhgVTavkrrJDxOlYG4ORZPCiTzRWMmwvQJatkwTUjsD0CgYEA8nAWBSis9H8n9aCEW30pGHT8LwqlH0XfXwOTPmkxHXOIIkhNFiZRAzc4NKaefyhzdNlc7diSMFVXpyLZ4K0l5dY1Ou2xRh0W+xkRjjKsMib/s9g/crtam+tXddADJDokLELn5PAMhaHBpti+PpOMGqdI3Wub+5yT1XCXT9aj6yU="


@pytest.fixture
def snowflake_key_no_passphrase_bytes(snowflake_key_no_passphrase_b64) -> bytes:
    return base64.b64decode(snowflake_key_no_passphrase_b64)


@pytest.fixture
def snowflake_key_passphrase_path() -> str:
    return "tests/fixtures/snowflake/rsa_key_pass.p8"


@pytest.fixture
def snowflake_key_passphrase_pem(snowflake_key_passphrase_path) -> str:
    with open(snowflake_key_passphrase_path, "r") as key:
        return key.read()


@pytest.fixture
def snowflake_key_passphrase_b64() -> str:
    return "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCIxN7HxKFBa9e4CXdm6/2iDBgKt0QWFqKXnRH1oY3m3LgLHuhgZWb6itQGpEoRSoW3xizw9GeLdM8Jwq9OvZAPLxipC/MWrK3/ksnulBVyntPumeSKvumpUcbpVocyTGyvtiw+pwHNOySZDfgCHY0qcp3FiIk5iKN1qbOxEEeeKjwzCmv5Yd4A32TiNjmMkFso3EGxdR7CRhzNJFE+iJZ5R5drF0CHVB9hZEomf73gdWgAbhDr2KzJvsDvEUwcIZSHj+XKAtOtGSXiqwVp9reX5wBlrgQlvPrm8gPi9fkm+lJahTpprrYLvQlj+ccmgXE43ANY3WKagsJUNxi9EFuvAgMBAAECggEACEBoeIECgaHyB+Z6T7lZOheks7DO6M5AzQjq9njiyNT0PaeFuZsklWUe2a+70ENAwg+w0nDMdnt7qkkWrpd9Q41B3aEc73dHoC3JBR3mFV5Dxxd91GkkS9TlPVq9GWnG/OruzHDjCPDSinFvTyFdTPxRTIOqU9BMnGK6tqoWyBIJrG62EMXiBFxuFW8GPqsDHyTaVtO7bX47UoRbVhgh442PdV3GyKyKZDYoiPujnkVxE780zq8H2YL1xGfacDnTwoqpQfaA45fAaZc9guPI4C/8SWGfM+SBeA66ZqOUxgl62GYsybpYSTZAZeFRYZO+AlPgmhK2iNCvWFQgI6zRkQKBgQDBaAx5VlActbc6BrTLQuPhs4j3WvbayjCAdTyBeEJAboEr6Az2X/bcmI3krYin/XlIZkNMkktktS/M0Vxj6zwK7zQ7bULitIXQ3JrFDNt3pYdKY4eFSd7ALxc6DB7P1VetqlotOSUX4lZYEcsQ0eQsuj+ELtKPAS+LoNQVYnATxQKBgQC1CFhMiSzEYrHqQE5NrStjLRJzJcz6eCvUfkFMUFg06cp/hc8aePx2qTTgBlskRAh3k+7oObVCWQF3+h8ptRZ8O+AlVilKT/BvORrMVUDRGxtwg4QUD8/lvKIK4jyW5DNa4x7uou6/eBiBqRaLq7AR/UJP52ZWBmfKxUphPxzE4wKBgGrXe+ybze3ORMX9ZmrTLOhGMefTjIMZJuoP2bj8Ij1NznXe3ypLoSgD7n7hjpie4h0owQzP1G5x2VIgZhWcobK4qfYaSdTLPRFAjQ9GJwdVngNuMDNlt3Qbj401nN/bT3BUpzRMWT10f5ZvXeqQyKgcy3HOG+t8EDPmSML3ekqxAoGARyJ4T9q3FJQThRCvtCYPnnDfhw+bc/A0iNLzpaEMh/4169YQgz53NclXVZAp0B5LlXEzt1y1tNR0l0hZZnIZ28dLVGB+6QxwVcQCm7gEOCaGqbeD9r4f2w48PjqXxFL3Owdz6CFt3x65wnlGuqtEDE2P+QXcWIE715memIfMLjECgYB4SA/YOopbGeYPzU9jj0RpykyRjGMNdUyHypjDswwvZc7MMB3dYjynwoTSuGBTVm7R3NWS/Uj54MdZhGvfyb+xmEHaorPJv9YT5/b48mr3rhzPkFoJdy85epVzLvuZzSTPg92tDIcdF/Uabc7eLEbB3H5mVYqk6qRecL4h1fNIqg=="


@pytest.fixture
def snowflake_key_passphrase_bytes(snowflake_key_passphrase_b64) -> bytes:
    return base64.b64decode(snowflake_key_passphrase_b64)


@pytest.fixture
def snowflake_oauth_access_token() -> str:
    return "eyJhbGciOiJSUzI1NiIsImtpZCI6ImFmZmM2MjkwN2E0NDYxODJhZGMxZmE0ZTgxZmRiYTYzMTBkY2U2M2YifQ.eyJhenAiOiIyNzIxOTYwNjkxNzMtZm81ZWI0MXQzbmR1cTZ1ZXRkc2pkdWdzZXV0ZnBtc3QuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiIyNzIxOTYwNjkxNzMtZm81ZWI0MXQzbmR1cTZ1ZXRkc2pkdWdzZXV0ZnBtc3QuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTc4NDc5MTI4NzU5MTM5MDU0OTMiLCJlbWFpbCI6ImFhcm9uLnBhcmVja2lAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImF0X2hhc2giOiJpRVljNDBUR0luUkhoVEJidWRncEpRIiwiZXhwIjoxNTI0NTk5MDU2LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE1MjQ1OTU0NTZ9.ho2czp_1JWsglJ9jN8gCgWfxDi2gY4X5-QcT56RUGkgh5BJaaWdlrRhhN_eNuJyN3HRPhvVA_KJVy1tMltTVd2OQ6VkxgBNfBsThG_zLPZriw7a1lANblarwxLZID4fXDYG-O8U-gw4xb-NIsOzx6xsxRBdfKKniavuEg56Sd3eKYyqrMA0DWnIagqLiKE6kpZkaGImIpLcIxJPF0-yeJTMt_p1NoJF7uguHHLYr6752hqppnBpMjFL2YMDVeg3jl1y5DeSKNPh6cZ8H2p4Xb2UIrJguGbQHVIJvtm_AspRjrmaTUQKrzXDRCfDROSUU-h7XKIWRrEd2-W9UkV5oCg"


def test_snowflake(make_config, snowflake_key_passphrase_bytes, snowflake_oauth_access_token):
    # Authenticator and user/password is fine
    config = make_config(
        type="snowflake",
        account="test",
        user="test",
        password="test",
        authenticator="externalbrowser",
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # Auth with no user/password is fine
    config = make_config(type="snowflake", account="test", authenticator="externalbrowser")
    assert isinstance(config, SnowflakeConnectionConfig)
    # No auth and no user raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        make_config(type="snowflake", account="test", password="test")
    # No auth and no password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        make_config(type="snowflake", account="test", user="test")
    # No auth and no user/password raises
    with pytest.raises(
        ConfigError, match="User and password must be provided if using default authentication"
    ):
        make_config(type="snowflake", account="test")
    # Private key and username with no authenticator is fine
    config = make_config(
        type="snowflake", account="test", private_key=snowflake_key_passphrase_bytes, user="test"
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # Private key with jwt auth is fine
    config = make_config(
        type="snowflake",
        account="test",
        private_key=snowflake_key_passphrase_bytes,
        authenticator="snowflake_jwt",
        user="test",
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    # Private key without username raises
    with pytest.raises(
        ConfigError, match=r"User must be provided when using SNOWFLAKE_JWT authentication"
    ):
        make_config(
            type="snowflake",
            account="test",
            private_key=snowflake_key_passphrase_bytes,
            authenticator="snowflake_jwt",
        )
    # Private key with password raises
    with pytest.raises(
        ConfigError, match=r"Password cannot be provided when using SNOWFLAKE_JWT authentication"
    ):
        make_config(
            type="snowflake",
            account="test",
            private_key=snowflake_key_passphrase_bytes,
            user="test",
            password="test",
        )
    # Private key with different authenticator raise
    with pytest.raises(
        ConfigError,
        match=r"Private key or private key path can only be provided when using SNOWFLAKE_JWT authentication",
    ):
        make_config(
            type="snowflake",
            account="test",
            private_key=snowflake_key_passphrase_bytes,
            user="test",
            authenticator="externalbrowser",
        )
    # Private key and private key both combined raises
    with pytest.raises(
        ConfigError, match=r"Cannot specify both `private_key` and `private_key_path`"
    ):
        make_config(
            type="snowflake",
            account="test",
            private_key=snowflake_key_passphrase_bytes,
            private_key_path="test",
            user="test",
            authenticator="snowflake_jwt",
        )
    config = make_config(
        type="snowflake",
        account="test",
        user="test",
        password="test",
        authenticator="externalbrowser",
        database="test_catalog",
    )
    assert isinstance(config, SnowflakeConnectionConfig)
    assert config.get_catalog() == "test_catalog"
    with pytest.raises(ConfigError, match=r"Token must be provided if using oauth authentication"):
        make_config(
            type="snowflake",
            account="test",
            user="test",
            authenticator="oauth",
        )


@pytest.mark.parametrize(
    "key_path_fixture, key_pem_fixture, key_b64_fixture, key_bytes_fixture, key_passphrase",
    [
        (
            "snowflake_key_no_passphrase_path",
            "snowflake_key_no_passphrase_pem",
            "snowflake_key_no_passphrase_b64",
            "snowflake_key_no_passphrase_bytes",
            None,
        ),
        (
            "snowflake_key_passphrase_path",
            "snowflake_key_passphrase_pem",
            "snowflake_key_passphrase_b64",
            "snowflake_key_passphrase_bytes",
            "insecure",
        ),
    ],
)
def test_snowflake_private_key_pass(
    make_config,
    key_path_fixture,
    key_pem_fixture,
    key_b64_fixture,
    key_bytes_fixture,
    key_passphrase,
    request: FixtureRequest,
):
    key_path = request.getfixturevalue(key_path_fixture)
    key_pem = request.getfixturevalue(key_pem_fixture)
    key_b64 = request.getfixturevalue(key_b64_fixture)
    key_bytes = request.getfixturevalue(key_bytes_fixture)
    common_kwargs = dict(
        type="snowflake",
        account="test",
        user="test",
        authenticator="snowflake_jwt",
        database="test_catalog",
    )

    config = make_config(
        private_key_path=key_path,
        private_key_passphrase=key_passphrase,
        **common_kwargs,
    )
    assert config.private_key == key_bytes

    config = make_config(
        private_key=key_pem,
        private_key_passphrase=key_passphrase,
        **common_kwargs,
    )
    assert config.private_key == key_bytes

    config = make_config(
        private_key=key_b64,
        **common_kwargs,
    )
    assert config.private_key == key_bytes

    config = make_config(
        private_key=key_bytes,
        **common_kwargs,
    )
    assert config.private_key == key_bytes


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


def test_trino(make_config):
    required_kwargs = dict(
        type="trino",
        user="user",
        host="host",
        catalog="catalog",
    )
    # Validate default behavior
    config = make_config(**required_kwargs)
    assert config.method == TrinoAuthenticationMethod.NO_AUTH
    assert config.user == "user"
    assert config.host == "host"
    assert config.catalog == "catalog"
    assert config.http_scheme == "https"
    assert config.port == 443
    assert config.get_catalog() == "catalog"

    # Validate Basic Auth
    config = make_config(method="basic", password="password", **required_kwargs)
    assert config.method == TrinoAuthenticationMethod.BASIC
    assert config.user == "user"
    assert config.password == "password"
    assert config.host == "host"
    assert config.catalog == "catalog"

    with pytest.raises(
        ConfigError, match="Username and Password must be provided if using basic authentication"
    ):
        make_config(method="basic", **required_kwargs)

    # Validate LDAP
    config = make_config(method="ldap", password="password", **required_kwargs)
    assert config.method == TrinoAuthenticationMethod.LDAP

    with pytest.raises(
        ConfigError, match="Username and Password must be provided if using ldap authentication"
    ):
        make_config(method="ldap", **required_kwargs)

    # Validate Kerberos
    config = make_config(
        method="kerberos",
        keytab="path/to/keytab.keytab",
        krb5_config="krb5.conf",
        principal="user@company.com",
        **required_kwargs,
    )
    assert config.method == TrinoAuthenticationMethod.KERBEROS

    with pytest.raises(
        ConfigError,
        match="Kerberos requires the following fields: principal, keytab, and krb5_config",
    ):
        make_config(method="kerberos", **required_kwargs)

    # Validate JWT
    config = make_config(method="jwt", jwt_token="blah", **required_kwargs)
    assert config.method == TrinoAuthenticationMethod.JWT

    with pytest.raises(ConfigError, match="JWT requires `jwt_token` to be set"):
        make_config(method="jwt", **required_kwargs)

    # Validate Certificate
    config = make_config(
        method="certificate",
        cert="blah",
        client_certificate="/tmp/client.crt",
        client_private_key="/tmp/client.key",
        **required_kwargs,
    )

    assert config.method == TrinoAuthenticationMethod.CERTIFICATE

    with pytest.raises(
        ConfigError,
        match="Certificate requires the following fields: cert, client_certificate, and client_private_key",
    ):
        make_config(method="certificate", **required_kwargs)

    # Validate OAuth
    config = make_config(method="oauth", **required_kwargs)
    assert config.method == TrinoAuthenticationMethod.OAUTH

    # Validate Port Behavior
    config = make_config(http_scheme="http", **required_kwargs)
    assert config.port == 80
    config = make_config(http_scheme="https", **required_kwargs)
    assert config.port == 443

    # Validate http is only for basic and no auth
    config = make_config(method="basic", password="password", http_scheme="http", **required_kwargs)
    assert config.method == TrinoAuthenticationMethod.BASIC
    assert config.http_scheme == "http"

    with pytest.raises(
        ConfigError, match="HTTP scheme can only be used with no-auth or basic method"
    ):
        make_config(method="ldap", http_scheme="http", **required_kwargs)


def test_duckdb(make_config):
    config = make_config(
        type="duckdb",
        database="test",
        connector_config={"foo": "bar"},
    )
    assert isinstance(config, DuckDBConnectionConfig)


@pytest.mark.parametrize(
    "kwargs1, kwargs2, shared_adapter",
    [
        (
            {
                "database": "test.duckdb",
            },
            {
                "database": "test.duckdb",
            },
            True,
        ),
        (
            {},
            {},
            False,
        ),
        (
            {
                "database": "test1.duckdb",
            },
            {
                "database": "test2.duckdb",
            },
            False,
        ),
        (
            {
                "database": ":memory:",
            },
            {
                "database": ":memory:",
            },
            False,
        ),
        (
            {
                "database": ":memory:",
            },
            {
                "database": "test.duckdb",
            },
            False,
        ),
        (
            {
                "catalogs": {
                    "test": "test.duckdb",
                }
            },
            {
                "catalogs": {
                    "test": "test.duckdb",
                }
            },
            True,
        ),
        (
            {
                "catalogs": {
                    "test": ":memory:",
                }
            },
            {
                "catalogs": {
                    "test": ":memory:",
                }
            },
            False,
        ),
        (
            {
                "catalogs": {
                    "test1": ":memory:",
                    "test2": "test2.duckdb",
                }
            },
            {
                "catalogs": {
                    "test1": ":memory:",
                    "test2": "test2.duckdb",
                }
            },
            True,
        ),
        (
            {
                "catalogs": {
                    "test1": ":memory:",
                    "test2": "test2.duckdb",
                }
            },
            {
                "catalogs": {
                    "test1": "test2.duckdb",
                    "test2": ":memory:",
                }
            },
            True,
        ),
        (
            {
                "catalogs": {
                    "test1": "test1.duckdb",
                    "test2": "test2.duckdb",
                    "test3": "test3.duckdb",
                }
            },
            {
                "catalogs": {
                    "test1": "test1_miss.duckdb",
                    "test2": "test2_miss.duckdb",
                    "test3": "test3.duckdb",
                }
            },
            True,
        ),
    ],
)
def test_duckdb_shared(make_config, caplog, kwargs1, kwargs2, shared_adapter):
    config1 = make_config(
        type="duckdb",
        **kwargs1,
    )
    config2 = make_config(
        type="duckdb",
        **kwargs2,
    )
    assert isinstance(config1, DuckDBConnectionConfig)
    assert isinstance(config2, DuckDBConnectionConfig)
    adapter1 = config1.create_engine_adapter()
    adapter2 = config2.create_engine_adapter()
    if shared_adapter:
        assert id(adapter1) == id(adapter2)
        assert "Creating new DuckDB adapter" in caplog.messages[0]
        assert "Using existing DuckDB adapter" in caplog.messages[1]
    else:
        assert id(adapter1) != id(adapter2)
        assert "Creating new DuckDB adapter" in caplog.messages[0]
        assert "Creating new DuckDB adapter" in caplog.messages[1]


def test_bigquery(make_config):
    config = make_config(
        type="bigquery",
        project="project",
        execution_project="execution_project",
    )

    assert isinstance(config, BigQueryConnectionConfig)
    assert config.project == "project"
    assert config.execution_project == "execution_project"
    assert config.get_catalog() == "project"
