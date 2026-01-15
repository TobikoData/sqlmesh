"""Unit tests for GizmoSQL engine adapter and connection config.

These tests don't require a running GizmoSQL server - they test
configuration validation and adapter properties.
"""

import typing as t
from unittest.mock import MagicMock, patch

import pytest
from sqlglot import exp

from sqlmesh.core.config.connection import GizmoSQLConnectionConfig
from sqlmesh.core.engine_adapter.gizmosql import GizmoSQLEngineAdapter
from sqlmesh.utils.errors import ConfigError

pytestmark = [pytest.mark.gizmosql]


class TestGizmoSQLConnectionConfig:
    """Tests for GizmoSQLConnectionConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = GizmoSQLConnectionConfig(
            username="user",
            password="pass",
        )

        assert config.host == "localhost"
        assert config.port == 31337
        assert config.use_encryption is True
        assert config.disable_certificate_verification is False
        assert config.database is None
        assert config.concurrent_tasks == 4

    def test_custom_values(self):
        """Test custom configuration values."""
        config = GizmoSQLConnectionConfig(
            host="gizmosql.example.com",
            port=12345,
            username="testuser",
            password="testpass",
            use_encryption=False,
            disable_certificate_verification=True,
            database="mydb",
            concurrent_tasks=8,
        )

        assert config.host == "gizmosql.example.com"
        assert config.port == 12345
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.use_encryption is False
        assert config.disable_certificate_verification is True
        assert config.database == "mydb"
        assert config.concurrent_tasks == 8

    def test_dialect_is_duckdb(self):
        """Test that the dialect is set to duckdb."""
        config = GizmoSQLConnectionConfig(
            username="user",
            password="pass",
        )
        assert config.DIALECT == "duckdb"

    def test_type_is_gizmosql(self):
        """Test that the type is set to gizmosql."""
        config = GizmoSQLConnectionConfig(
            username="user",
            password="pass",
        )
        assert config.type_ == "gizmosql"

    def test_get_catalog(self):
        """Test get_catalog returns database."""
        config = GizmoSQLConnectionConfig(
            username="user",
            password="pass",
            database="mydb",
        )
        assert config.get_catalog() == "mydb"

    def test_get_catalog_none(self):
        """Test get_catalog returns None when database not set."""
        config = GizmoSQLConnectionConfig(
            username="user",
            password="pass",
        )
        assert config.get_catalog() is None

    def test_connection_factory_builds_correct_uri_with_tls(self):
        """Test connection factory builds correct URI with TLS."""
        mock_flightsql = MagicMock()
        mock_conn = MagicMock()
        mock_conn.adbc_get_info.return_value = {"vendor_version": "duckdb v1.0.0"}
        mock_flightsql.connect.return_value = mock_conn

        # Mock DatabaseOptions enum
        mock_db_options = MagicMock()
        mock_db_options.TLS_SKIP_VERIFY.value = "adbc.flight.sql.client_option.tls_skip_verify"

        config = GizmoSQLConnectionConfig(
            host="example.com",
            port=31337,
            username="user",
            password="pass",
            use_encryption=True,
            disable_certificate_verification=False,
        )

        mock_module = MagicMock()
        mock_module.dbapi = mock_flightsql
        mock_module.DatabaseOptions = mock_db_options

        with patch.dict("sys.modules", {"adbc_driver_flightsql": mock_module}):
            factory = config._connection_factory
            factory()

        # Verify connect was called with correct URI (first positional arg)
        call_args = mock_flightsql.connect.call_args
        assert call_args[0][0] == "grpc+tls://example.com:31337"
        # Verify db_kwargs contains credentials
        db_kwargs = call_args[1]["db_kwargs"]
        assert db_kwargs["username"] == "user"
        assert db_kwargs["password"] == "pass"

    def test_connection_factory_builds_correct_uri_without_tls(self):
        """Test connection factory builds correct URI without TLS."""
        mock_flightsql = MagicMock()
        mock_conn = MagicMock()
        mock_conn.adbc_get_info.return_value = {"vendor_version": "duckdb v1.0.0"}
        mock_flightsql.connect.return_value = mock_conn

        # Mock DatabaseOptions enum
        mock_db_options = MagicMock()
        mock_db_options.TLS_SKIP_VERIFY.value = "adbc.flight.sql.client_option.tls_skip_verify"

        config = GizmoSQLConnectionConfig(
            host="example.com",
            port=31337,
            username="user",
            password="pass",
            use_encryption=False,
        )

        mock_module = MagicMock()
        mock_module.dbapi = mock_flightsql
        mock_module.DatabaseOptions = mock_db_options

        with patch.dict("sys.modules", {"adbc_driver_flightsql": mock_module}):
            factory = config._connection_factory
            factory()

        call_args = mock_flightsql.connect.call_args
        assert call_args[0][0] == "grpc://example.com:31337"

    def test_connection_factory_rejects_non_duckdb_backend(self):
        """Test connection factory raises error for non-DuckDB backend."""
        mock_flightsql = MagicMock()
        mock_conn = MagicMock()
        mock_conn.adbc_get_info.return_value = {"vendor_version": "sqlite v3.40.0"}
        mock_flightsql.connect.return_value = mock_conn

        # Mock DatabaseOptions enum
        mock_db_options = MagicMock()
        mock_db_options.TLS_SKIP_VERIFY.value = "adbc.flight.sql.client_option.tls_skip_verify"

        config = GizmoSQLConnectionConfig(
            host="example.com",
            port=31337,
            username="user",
            password="pass",
        )

        mock_module = MagicMock()
        mock_module.dbapi = mock_flightsql
        mock_module.DatabaseOptions = mock_db_options

        with patch.dict("sys.modules", {"adbc_driver_flightsql": mock_module}):
            factory = config._connection_factory
            with pytest.raises(ConfigError, match="Unsupported GizmoSQL server backend"):
                factory()

        # Verify connection was closed after rejection
        mock_conn.close.assert_called_once()


class TestGizmoSQLEngineAdapter:
    """Tests for GizmoSQLEngineAdapter properties."""

    def test_dialect(self):
        """Test that dialect is duckdb."""
        assert GizmoSQLEngineAdapter.DIALECT == "duckdb"

    def test_supports_transactions(self):
        """Test transactions are supported via SQL statements."""
        assert GizmoSQLEngineAdapter.SUPPORTS_TRANSACTIONS is True

    def test_supports_create_drop_catalog(self):
        """Test catalog operations are supported."""
        assert GizmoSQLEngineAdapter.SUPPORTS_CREATE_DROP_CATALOG is True

    def test_supported_drop_cascade_object_kinds(self):
        """Test cascade drop is supported for schemas, tables, and views."""
        assert "SCHEMA" in GizmoSQLEngineAdapter.SUPPORTED_DROP_CASCADE_OBJECT_KINDS
        assert "TABLE" in GizmoSQLEngineAdapter.SUPPORTED_DROP_CASCADE_OBJECT_KINDS
        assert "VIEW" in GizmoSQLEngineAdapter.SUPPORTED_DROP_CASCADE_OBJECT_KINDS
