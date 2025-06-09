import base64
import re
import typing as t

import pytest
from _pytest.fixtures import FixtureRequest
from unittest.mock import patch

from sqlmesh.core.config.connection import (
    BigQueryConnectionConfig,
    ClickhouseConnectionConfig,
    ConnectionConfig,
    DatabricksConnectionConfig,
    DuckDBAttachOptions,
    DuckDBConnectionConfig,
    GCPPostgresConnectionConfig,
    MotherDuckConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
    SnowflakeConnectionConfig,
    TrinoAuthenticationMethod,
    AthenaConnectionConfig,
    MSSQLConnectionConfig,
    _connection_config_validator,
    _get_engine_import_validator,
)
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


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
    with open(snowflake_key_no_passphrase_path, "r", encoding="utf-8") as key:
        return key.read()


@pytest.fixture
def snowflake_key_no_passphrase_b64(snowflake_key_no_passphrase_pem) -> str:
    return "\n".join(snowflake_key_no_passphrase_pem.split("\n")[1:-2])


@pytest.fixture
def snowflake_key_no_passphrase_bytes(snowflake_key_no_passphrase_b64) -> bytes:
    return base64.b64decode(snowflake_key_no_passphrase_b64)


@pytest.fixture
def snowflake_key_passphrase_path() -> str:
    return "tests/fixtures/snowflake/rsa_key_pass.p8"


@pytest.fixture
def snowflake_key_passphrase_pem(snowflake_key_passphrase_path) -> str:
    with open(snowflake_key_passphrase_path, "r", encoding="utf-8") as key:
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

    # Can pass in session parameters
    config = make_config(
        type="snowflake",
        account="test",
        user="test",
        password="test",
        authenticator="externalbrowser",
        session_parameters={"query_tag": "test", "timezone": "UTC"},
    )
    assert isinstance(config, SnowflakeConnectionConfig)


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
    assert config.is_recommended_for_state_sync is False

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


def test_trino_schema_location_mapping(make_config):
    required_kwargs = dict(
        type="trino",
        user="user",
        host="host",
        catalog="catalog",
    )

    with pytest.raises(
        ConfigError, match=r".*needs to include the '@\{schema_name\}' placeholder.*"
    ):
        make_config(**required_kwargs, schema_location_mapping={".*": "s3://foo"})

    config = make_config(
        **required_kwargs,
        schema_location_mapping={
            "^utils$": "s3://utils-bucket/@{schema_name}",
            "^staging.*$": "s3://bucket/@{schema_name}_dev",
            "^sqlmesh.*$": "s3://sqlmesh-internal/dev/@{schema_name}",
        },
    )

    assert config.schema_location_mapping is not None
    assert len(config.schema_location_mapping) == 3

    assert all((isinstance(k, re.Pattern) for k in config.schema_location_mapping))
    assert all((isinstance(v, str) for v in config.schema_location_mapping.values()))


def test_duckdb(make_config):
    config = make_config(
        type="duckdb",
        database="test",
        connector_config={"foo": "bar"},
        secrets=[
            {
                "type": "s3",
                "region": "aws_region",
                "key_id": "aws_access_key",
                "secret": "aws_secret",
            }
        ],
    )
    assert config.connector_config
    assert config.secrets
    assert isinstance(config, DuckDBConnectionConfig)
    assert not config.is_recommended_for_state_sync


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


def test_duckdb_attach_catalog(make_config):
    config = make_config(
        type="duckdb",
        catalogs={
            "test1": "test1.duckdb",
            "test2": DuckDBAttachOptions(
                type="duckdb",
                path="test2.duckdb",
            ),
        },
    )
    assert isinstance(config, DuckDBConnectionConfig)
    assert config.get_catalog() == "test1"

    assert config.catalogs.get("test2").read_only is False
    assert config.catalogs.get("test2").path == "test2.duckdb"
    assert not config.is_recommended_for_state_sync


def test_duckdb_attach_ducklake_catalog(make_config):
    config = make_config(
        type="duckdb",
        catalogs={
            "ducklake": DuckDBAttachOptions(
                type="ducklake",
                path="catalog.ducklake",
                data_path="/tmp/ducklake_data",
                encrypted=True,
                data_inlining_row_limit=10,
            ),
        },
    )
    assert isinstance(config, DuckDBConnectionConfig)
    ducklake_catalog = config.catalogs.get("ducklake")
    assert ducklake_catalog is not None
    assert ducklake_catalog.type == "ducklake"
    assert ducklake_catalog.path == "catalog.ducklake"
    assert ducklake_catalog.data_path == "/tmp/ducklake_data"
    assert ducklake_catalog.encrypted is True
    assert ducklake_catalog.data_inlining_row_limit == 10
    # Check that the generated SQL includes DATA_PATH
    assert "DATA_PATH '/tmp/ducklake_data'" in ducklake_catalog.to_sql("ducklake")
    assert "ENCRYPTED" in ducklake_catalog.to_sql("ducklake")
    assert "DATA_INLINING_ROW_LIMIT 10" in ducklake_catalog.to_sql("ducklake")


def test_duckdb_attach_options():
    options = DuckDBAttachOptions(
        type="postgres", path="dbname=postgres user=postgres host=127.0.0.1", read_only=True
    )

    assert (
        options.to_sql(alias="db")
        == "ATTACH IF NOT EXISTS 'dbname=postgres user=postgres host=127.0.0.1' AS db (TYPE POSTGRES, READ_ONLY)"
    )

    options = DuckDBAttachOptions(type="duckdb", path="test.db", read_only=False)

    assert options.to_sql(alias="db") == "ATTACH IF NOT EXISTS 'test.db' AS db"


def test_duckdb_config_json_strings(make_config):
    config = make_config(
        type="duckdb",
        extensions='["foo","bar"]',
        catalogs="""{
            "test1": "test1.duckdb",
            "test2": {
                "type": "duckdb",
                "path": "test2.duckdb"
            }
        }""",
    )
    assert isinstance(config, DuckDBConnectionConfig)

    assert config.extensions == ["foo", "bar"]

    assert config.get_catalog() == "test1"
    assert config.catalogs.get("test1") == "test1.duckdb"
    assert config.catalogs.get("test2").path == "test2.duckdb"


def test_motherduck_attach_catalog(make_config):
    config = make_config(
        type="motherduck",
        catalogs={
            "test1": "md:test1",
            "test2": DuckDBAttachOptions(
                type="motherduck",
                path="md:test2",
            ),
        },
    )
    assert isinstance(config, MotherDuckConnectionConfig)
    assert config.get_catalog() == "test1"

    assert config.catalogs.get("test2").read_only is False
    assert config.catalogs.get("test2").path == "md:test2"
    assert not config.is_recommended_for_state_sync


def test_motherduck_attach_options():
    options = DuckDBAttachOptions(
        type="postgres", path="dbname=postgres user=postgres host=127.0.0.1", read_only=True
    )

    assert (
        options.to_sql(alias="db")
        == "ATTACH IF NOT EXISTS 'dbname=postgres user=postgres host=127.0.0.1' AS db (TYPE POSTGRES, READ_ONLY)"
    )

    options = DuckDBAttachOptions(type="motherduck", path="md:test.db", read_only=False)

    # Here the alias should be ignored compared to duckdb
    assert options.to_sql(alias="db") == "ATTACH IF NOT EXISTS 'md:test.db'"


def test_duckdb_multithreaded_connection_factory(make_config):
    from sqlmesh.core.engine_adapter import DuckDBEngineAdapter
    from sqlmesh.utils.connection_pool import ThreadLocalSharedConnectionPool
    from threading import Thread

    config = make_config(type="duckdb")

    # defaults to 1, no issue
    assert config.concurrent_tasks == 1

    # check that the connection factory always returns the same connection in multithreaded mode
    # this sounds counter-intuitive but that's what DuckDB recommends here: https://duckdb.org/docs/guides/python/multiple_threads.html
    config = make_config(type="duckdb", concurrent_tasks=8)
    adapter = config.create_engine_adapter()
    assert isinstance(adapter, DuckDBEngineAdapter)
    assert isinstance(adapter._connection_pool, ThreadLocalSharedConnectionPool)

    threads = []
    connection_objects = []

    def _thread_connection():
        connection_objects.append(adapter.connection)

    for _ in range(8):
        threads.append(Thread(target=_thread_connection))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(connection_objects) == 8
    assert len(set(connection_objects)) == 1  # they should all be the same object

    # test that recycling the pool means we dont end up with unusable connections (eg check we havent cached a closed connection)
    assert adapter.fetchone("select 1") == (1,)
    adapter.recycle()
    assert adapter.fetchone("select 1") == (1,)


def test_motherduck_token_mask(make_config):
    config_1 = make_config(
        type="motherduck",
        token="short",
        database="whodunnit",
    )
    config_2 = make_config(
        type="motherduck",
        token="longtoken123456789",
        database="whodunnit",
    )
    config_3 = make_config(
        type="motherduck",
        token="secret1235",
        catalogs={
            "test1": DuckDBAttachOptions(
                type="motherduck",
                path="md:whodunnit",
            ),
        },
    )

    assert isinstance(config_1, MotherDuckConnectionConfig)
    assert isinstance(config_2, MotherDuckConnectionConfig)
    assert isinstance(config_3, MotherDuckConnectionConfig)
    assert config_1._mask_motherduck_token(config_1.database) == "whodunnit"
    assert (
        config_1._mask_motherduck_token(f"md:{config_1.database}?motherduck_token={config_1.token}")
        == "md:whodunnit?motherduck_token=*****"
    )
    assert (
        config_1._mask_motherduck_token(
            f"md:{config_1.database}?attach_mode=single&motherduck_token={config_1.token}"
        )
        == "md:whodunnit?attach_mode=single&motherduck_token=*****"
    )
    assert (
        config_2._mask_motherduck_token(f"md:{config_2.database}?motherduck_token={config_2.token}")
        == "md:whodunnit?motherduck_token=******************"
    )
    assert (
        config_3._mask_motherduck_token(f"md:?motherduck_token={config_3.token}")
        == "md:?motherduck_token=**********"
    )
    assert (
        config_1._mask_motherduck_token("?motherduck_token=secret1235")
        == "?motherduck_token=**********"
    )
    assert (
        config_1._mask_motherduck_token("md:whodunnit?motherduck_token=short")
        == "md:whodunnit?motherduck_token=*****"
    )
    assert (
        config_1._mask_motherduck_token("md:whodunnit?motherduck_token=longtoken123456789")
        == "md:whodunnit?motherduck_token=******************"
    )
    assert (
        config_1._mask_motherduck_token("md:whodunnit?motherduck_token=")
        == "md:whodunnit?motherduck_token="
    )
    assert config_1._mask_motherduck_token(":memory:") == ":memory:"


def test_bigquery(make_config):
    config = make_config(
        type="bigquery",
        project="project",
        execution_project="execution_project",
        quota_project="quota_project",
        check_import=False,
    )

    assert isinstance(config, BigQueryConnectionConfig)
    assert config.project == "project"
    assert config.execution_project == "execution_project"
    assert config.quota_project == "quota_project"
    assert config.get_catalog() == "project"
    assert config.is_recommended_for_state_sync is False

    with pytest.raises(ConfigError, match="you must also specify the `project` field"):
        make_config(type="bigquery", execution_project="execution_project", check_import=False)

    with pytest.raises(ConfigError, match="you must also specify the `project` field"):
        make_config(type="bigquery", quota_project="quota_project", check_import=False)


def test_bigquery_config_json_string(make_config):
    config = make_config(
        type="bigquery",
        project="project",
        # these can be present as strings if they came from env vars
        scopes='["a","b","c"]',
        keyfile_json='{"foo":"bar"}',
    )

    assert isinstance(config, BigQueryConnectionConfig)

    assert config.scopes == ("a", "b", "c")
    assert config.keyfile_json == {"foo": "bar"}


def test_postgres(make_config):
    config = make_config(
        type="postgres",
        host="host",
        user="user",
        password="password",
        port=5432,
        database="database",
    )
    assert isinstance(config, PostgresConnectionConfig)
    assert config.is_recommended_for_state_sync is True


def test_gcp_postgres(make_config):
    config = make_config(
        type="gcp_postgres",
        instance_connection_string="something",
        user="user",
        password="password",
        db="database",
        check_import=False,
    )
    assert isinstance(config, GCPPostgresConnectionConfig)
    assert config.is_recommended_for_state_sync is True
    assert config.ip_type == "public"
    config = make_config(
        type="gcp_postgres",
        instance_connection_string="something",
        user="user",
        password="password",
        db="database",
        ip_type="private",
        check_import=False,
    )
    assert config.ip_type == "private"


def test_mysql(make_config):
    config = make_config(
        type="mysql",
        host="host",
        user="user",
        password="password",
        check_import=False,
    )
    assert isinstance(config, MySQLConnectionConfig)
    assert config.is_recommended_for_state_sync is True


def test_clickhouse(make_config):
    from sqlmesh import __version__

    config = make_config(
        type="clickhouse",
        host="localhost",
        username="default",
        password="default",
        cluster="default",
        use_compression=True,
        connection_settings={"this_setting": "1"},
        server_host_name="server_host_name",
        verify=True,
        ca_cert="ca_cert",
        client_cert="client_cert",
        client_cert_key="client_cert_key",
        https_proxy="https://proxy",
        connection_pool_options={"pool_option": "value"},
    )
    assert isinstance(config, ClickhouseConnectionConfig)
    assert config.cluster == "default"
    assert config.use_compression
    assert config._static_connection_kwargs["compress"]
    assert config._static_connection_kwargs["client_name"] == f"SQLMesh/{__version__}"
    assert config._static_connection_kwargs["this_setting"] == "1"
    assert config.is_recommended_for_state_sync is False
    assert config.is_forbidden_for_state_sync

    pool = config._connection_factory.keywords["pool_mgr"]
    assert pool.connection_pool_kw["server_hostname"] == "server_host_name"
    assert pool.connection_pool_kw["assert_hostname"] == "server_host_name"  # because verify=True
    assert pool.connection_pool_kw["ca_certs"] == "ca_cert"
    assert pool.connection_pool_kw["cert_file"] == "client_cert"
    assert pool.connection_pool_kw["key_file"] == "client_cert_key"
    assert pool.connection_pool_kw["pool_option"] == "value"

    config2 = make_config(
        type="clickhouse",
        host="localhost",
        username="default",
        password="default",
        compression_method="lz4",
    )

    assert config2.use_compression
    assert config2._static_connection_kwargs["compress"] == "lz4"

    config3 = make_config(
        type="clickhouse",
        host="localhost",
        username="default",
        password="default",
        use_compression=False,
        compression_method="lz4",
    )

    assert not config3.use_compression
    assert not config3._static_connection_kwargs["compress"]


def test_athena(make_config):
    config = make_config(type="athena", work_group="primary")
    assert isinstance(config, AthenaConnectionConfig)


def test_athena_catalog(make_config):
    config = make_config(type="athena", work_group="primary", catalog_name="foo")
    assert isinstance(config, AthenaConnectionConfig)

    assert config.catalog_name == "foo"
    adapter = config.create_engine_adapter()
    assert adapter.default_catalog == "foo"

    config = make_config(type="athena", work_group="primary")
    assert isinstance(config, AthenaConnectionConfig)
    assert config.catalog_name is None
    adapter = config.create_engine_adapter()
    assert adapter.default_catalog == "awsdatacatalog"


def test_athena_s3_staging_dir_or_workgroup(make_config):
    with pytest.raises(
        ConfigError, match=r"At least one of work_group or s3_staging_dir must be set"
    ):
        config = make_config(type="athena")

    config = make_config(type="athena", s3_staging_dir="s3://foo")

    assert isinstance(config, AthenaConnectionConfig)
    assert config.work_group is None
    assert config.s3_staging_dir == "s3://foo/"  # validator appends trailing /

    config = make_config(type="athena", work_group="test")

    assert isinstance(config, AthenaConnectionConfig)
    assert config.work_group == "test"
    assert config.s3_staging_dir is None


def test_athena_s3_locations_valid(make_config):
    with pytest.raises(ConfigError, match=r".*must be a s3:// URI.*"):
        make_config(
            type="athena", work_group="primary", s3_warehouse_location="hdfs://legacy/location"
        )

    with pytest.raises(ConfigError, match=r".*must be a s3:// URI.*"):
        make_config(type="athena", s3_staging_dir="alskdjlskadgj")

    config = make_config(
        type="athena",
        s3_staging_dir="s3://bucket/query-results",
        s3_warehouse_location="s3://bucket/prod/warehouse/",
    )

    assert isinstance(config, AthenaConnectionConfig)
    assert config.s3_staging_dir == "s3://bucket/query-results/"
    assert config.s3_warehouse_location == "s3://bucket/prod/warehouse/"

    config = make_config(
        type="athena", work_group="primary", s3_staging_dir=None, s3_warehouse_location=None
    )

    assert isinstance(config, AthenaConnectionConfig)
    assert config.s3_staging_dir is None
    assert config.s3_warehouse_location is None


def test_databricks(make_config):
    # Personal Access Token
    oauth_pat_config = make_config(
        type="databricks",
        server_hostname="dbc-test.cloud.databricks.com",
        http_path="sql/test/foo",
        access_token="foo",
    )
    assert isinstance(oauth_pat_config, DatabricksConnectionConfig)
    assert oauth_pat_config.server_hostname == "dbc-test.cloud.databricks.com"
    assert oauth_pat_config.http_path == "sql/test/foo"
    assert oauth_pat_config.access_token == "foo"
    assert oauth_pat_config.auth_type is None
    assert oauth_pat_config.oauth_client_id is None
    assert oauth_pat_config.oauth_client_secret is None

    # OAuth (M2M)
    oauth_m2m_config = make_config(
        type="databricks",
        server_hostname="dbc-test.cloud.databricks.com",
        http_path="sql/test/foo",
        auth_type="databricks-oauth",
        oauth_client_id="client-id",
        oauth_client_secret="client-secret",
    )
    assert isinstance(oauth_m2m_config, DatabricksConnectionConfig)
    assert oauth_m2m_config.server_hostname == "dbc-test.cloud.databricks.com"
    assert oauth_pat_config.http_path == "sql/test/foo"
    assert oauth_m2m_config.access_token is None
    assert oauth_m2m_config.auth_type == "databricks-oauth"
    assert oauth_m2m_config.oauth_client_id == "client-id"
    assert oauth_m2m_config.oauth_client_secret == "client-secret"

    # OAuth (U2M)
    oauth_u2m_config = make_config(
        type="databricks",
        server_hostname="dbc-test.cloud.databricks.com",
        http_path="sql/test/foo",
        auth_type="databricks-oauth",
    )
    assert isinstance(oauth_u2m_config, DatabricksConnectionConfig)
    assert oauth_u2m_config.server_hostname == "dbc-test.cloud.databricks.com"
    assert oauth_pat_config.http_path == "sql/test/foo"
    assert oauth_u2m_config.access_token is None
    assert oauth_u2m_config.auth_type == "databricks-oauth"
    assert oauth_u2m_config.oauth_client_id is None
    assert oauth_u2m_config.oauth_client_secret is None

    # auth_type must match the AuthType enum if specified
    with pytest.raises(ConfigError, match=r".*nonexist does not match a valid option.*"):
        make_config(
            type="databricks", server_hostname="dbc-test.cloud.databricks.com", auth_type="nonexist"
        )

    # if client_secret is specified, client_id must also be specified
    with pytest.raises(ConfigError, match=r"`oauth_client_id` is required.*"):
        make_config(
            type="databricks",
            server_hostname="dbc-test.cloud.databricks.com",
            auth_type="databricks-oauth",
            oauth_client_secret="client-secret",
        )

    # http_path is still required when auth_type is specified
    with pytest.raises(ConfigError, match=r"`http_path` is still required.*"):
        make_config(
            type="databricks",
            server_hostname="dbc-test.cloud.databricks.com",
            auth_type="databricks-oauth",
        )


def test_engine_import_validator():
    with pytest.raises(
        ConfigError,
        match=re.escape(
            "Failed to import the 'bigquery' engine library. This may be due to a missing "
            "or incompatible installation. Please ensure the required dependency is installed by "
            'running: `pip install "sqlmesh[bigquery]"`. For more details, check the logs '
            "in the 'logs/' folder, or rerun the command with the '--debug' flag."
        ),
    ):

        class TestConfigA(PydanticModel):
            _engine_import_validator = _get_engine_import_validator("missing", "bigquery")

        TestConfigA()

    with pytest.raises(
        ConfigError,
        match=re.escape(
            "Failed to import the 'bigquery' engine library. This may be due to a missing "
            "or incompatible installation. Please ensure the required dependency is installed by "
            'running: `pip install "sqlmesh[bigquery_extra]"`. For more details, check the logs '
            "in the 'logs/' folder, or rerun the command with the '--debug' flag."
        ),
    ):

        class TestConfigB(PydanticModel):
            _engine_import_validator = _get_engine_import_validator(
                "missing", "bigquery", "bigquery_extra"
            )

        TestConfigB()

    class TestConfigC(PydanticModel):
        _engine_import_validator = _get_engine_import_validator("sqlmesh", "bigquery")

    TestConfigC()


def test_mssql_engine_import_validator():
    """Test that MSSQL import validator respects driver configuration."""

    # Test PyODBC driver suggests mssql-odbc extra when import fails
    with pytest.raises(ConfigError, match=r"pip install \"sqlmesh\[mssql-odbc\]\""):
        with patch("importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'pyodbc'")
            MSSQLConnectionConfig(host="localhost", driver="pyodbc")

    # Test PyMSSQL driver suggests mssql extra when import fails
    with pytest.raises(ConfigError, match=r"pip install \"sqlmesh\[mssql\]\""):
        with patch("importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'pymssql'")
            MSSQLConnectionConfig(host="localhost", driver="pymssql")

    # Test default driver (pymssql) suggests mssql extra when import fails
    with pytest.raises(ConfigError, match=r"pip install \"sqlmesh\[mssql\]\""):
        with patch("importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'pymssql'")
            MSSQLConnectionConfig(host="localhost")  # No driver specified

    # Test successful import works without error
    with patch("importlib.import_module") as mock_import:
        mock_import.return_value = None
        config = MSSQLConnectionConfig(host="localhost", driver="pyodbc")
        assert config.driver == "pyodbc"


def test_mssql_connection_config_parameter_validation(make_config):
    """Test MSSQL connection config parameter validation."""
    # Test default driver is pymssql
    config = make_config(type="mssql", host="localhost", check_import=False)
    assert isinstance(config, MSSQLConnectionConfig)
    assert config.driver == "pymssql"

    # Test explicit pyodbc driver
    config = make_config(type="mssql", host="localhost", driver="pyodbc", check_import=False)
    assert isinstance(config, MSSQLConnectionConfig)
    assert config.driver == "pyodbc"

    # Test explicit pymssql driver
    config = make_config(type="mssql", host="localhost", driver="pymssql", check_import=False)
    assert isinstance(config, MSSQLConnectionConfig)
    assert config.driver == "pymssql"

    # Test pyodbc specific parameters
    config = make_config(
        type="mssql",
        host="localhost",
        driver="pyodbc",
        driver_name="ODBC Driver 18 for SQL Server",
        trust_server_certificate=True,
        encrypt=False,
        odbc_properties={"Authentication": "ActiveDirectoryServicePrincipal"},
        check_import=False,
    )
    assert isinstance(config, MSSQLConnectionConfig)
    assert config.driver_name == "ODBC Driver 18 for SQL Server"
    assert config.trust_server_certificate is True
    assert config.encrypt is False
    assert config.odbc_properties == {"Authentication": "ActiveDirectoryServicePrincipal"}

    # Test pymssql specific parameters
    config = make_config(
        type="mssql",
        host="localhost",
        driver="pymssql",
        tds_version="7.4",
        conn_properties=["SET ANSI_NULLS ON"],
        check_import=False,
    )
    assert isinstance(config, MSSQLConnectionConfig)
    assert config.tds_version == "7.4"
    assert config.conn_properties == ["SET ANSI_NULLS ON"]


def test_mssql_connection_kwargs_keys():
    """Test _connection_kwargs_keys returns correct keys for each driver variant."""
    # Test pymssql driver keys
    config = MSSQLConnectionConfig(host="localhost", driver="pymssql", check_import=False)
    pymssql_keys = config._connection_kwargs_keys
    expected_pymssql_keys = {
        "password",
        "user",
        "database",
        "host",
        "timeout",
        "login_timeout",
        "charset",
        "appname",
        "port",
        "tds_version",
        "conn_properties",
        "autocommit",
    }
    assert pymssql_keys == expected_pymssql_keys

    # Test pyodbc driver keys
    config = MSSQLConnectionConfig(host="localhost", driver="pyodbc", check_import=False)
    pyodbc_keys = config._connection_kwargs_keys
    expected_pyodbc_keys = {
        "password",
        "user",
        "database",
        "host",
        "timeout",
        "login_timeout",
        "charset",
        "appname",
        "port",
        "autocommit",
        "driver_name",
        "trust_server_certificate",
        "encrypt",
        "odbc_properties",
    }
    assert pyodbc_keys == expected_pyodbc_keys

    # Verify pyodbc keys don't include pymssql-specific parameters
    assert "tds_version" not in pyodbc_keys
    assert "conn_properties" not in pyodbc_keys


def test_mssql_pyodbc_connection_string_generation():
    """Test pyodbc.connect gets invoked with the correct ODBC connection string."""
    with patch("pyodbc.connect") as mock_pyodbc_connect:
        # Mock the return value to have the methods we need
        mock_connection = mock_pyodbc_connect.return_value

        # Create a pyodbc config
        config = MSSQLConnectionConfig(
            host="testserver.database.windows.net",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            driver="pyodbc",
            driver_name="ODBC Driver 18 for SQL Server",
            trust_server_certificate=True,
            encrypt=True,
            login_timeout=30,
            check_import=False,
        )

        # Get the connection factory with kwargs and call it
        factory_with_kwargs = config._connection_factory_with_kwargs
        connection = factory_with_kwargs()

        # Verify pyodbc.connect was called with the correct connection string
        mock_pyodbc_connect.assert_called_once()
        call_args = mock_pyodbc_connect.call_args

        # Check the connection string (first argument)
        conn_str = call_args[0][0]
        expected_parts = [
            "DRIVER={ODBC Driver 18 for SQL Server}",
            "SERVER=testserver.database.windows.net,1433",
            "DATABASE=testdb",
            "Encrypt=YES",
            "TrustServerCertificate=YES",
            "Connection Timeout=30",
            "UID=testuser",
            "PWD=testpass",
        ]

        for part in expected_parts:
            assert part in conn_str

        # Check autocommit parameter
        assert call_args[1]["autocommit"] is False


def test_mssql_pyodbc_connection_string_with_odbc_properties():
    """Test pyodbc connection string includes custom ODBC properties."""
    with patch("pyodbc.connect") as mock_pyodbc_connect:
        # Create a pyodbc config with custom ODBC properties
        config = MSSQLConnectionConfig(
            host="testserver.database.windows.net",
            database="testdb",
            user="client-id",
            password="client-secret",
            driver="pyodbc",
            odbc_properties={
                "Authentication": "ActiveDirectoryServicePrincipal",
                "ClientCertificate": "/path/to/cert.pem",
                "TrustServerCertificate": "NO",  # This should be ignored since we set it explicitly
            },
            trust_server_certificate=True,  # This should take precedence
            check_import=False,
        )

        # Get the connection factory with kwargs and call it
        factory_with_kwargs = config._connection_factory_with_kwargs
        connection = factory_with_kwargs()

        # Verify pyodbc.connect was called
        mock_pyodbc_connect.assert_called_once()
        conn_str = mock_pyodbc_connect.call_args[0][0]

        # Check that custom ODBC properties are included
        assert "Authentication=ActiveDirectoryServicePrincipal" in conn_str
        assert "ClientCertificate=/path/to/cert.pem" in conn_str

        # Verify that explicit trust_server_certificate takes precedence
        assert "TrustServerCertificate=YES" in conn_str

        # Should not have the conflicting property from odbc_properties
        assert conn_str.count("TrustServerCertificate") == 1


def test_mssql_pyodbc_connection_string_minimal():
    """Test pyodbc connection string with minimal configuration."""
    with patch("pyodbc.connect") as mock_pyodbc_connect:
        config = MSSQLConnectionConfig(
            host="localhost",
            driver="pyodbc",
            autocommit=True,
            check_import=False,
        )

        factory_with_kwargs = config._connection_factory_with_kwargs
        connection = factory_with_kwargs()

        mock_pyodbc_connect.assert_called_once()
        conn_str = mock_pyodbc_connect.call_args[0][0]

        # Check basic required parts
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
        assert "SERVER=localhost,1433" in conn_str
        assert "Encrypt=YES" in conn_str  # Default encrypt=True
        assert "Connection Timeout=60" in conn_str  # Default timeout

        # Check autocommit parameter
        assert mock_pyodbc_connect.call_args[1]["autocommit"] is True


def test_mssql_pymssql_connection_factory():
    """Test pymssql connection factory returns correct function."""
    # Mock the import of pymssql at the module level
    import sys
    from unittest.mock import MagicMock

    # Create a mock pymssql module
    mock_pymssql = MagicMock()
    sys.modules["pymssql"] = mock_pymssql

    try:
        config = MSSQLConnectionConfig(
            host="localhost",
            driver="pymssql",
            check_import=False,
        )

        factory = config._connection_factory

        # Verify the factory returns pymssql.connect
        assert factory is mock_pymssql.connect
    finally:
        # Clean up the mock module
        if "pymssql" in sys.modules:
            del sys.modules["pymssql"]
