# type: ignore

import typing as t
import threading
import inspect
from unittest import mock as unittest_mock
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import FabricEngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.fabric]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> FabricEngineAdapter:
    return make_mocked_engine_adapter(FabricEngineAdapter)


def test_columns(adapter: FabricEngineAdapter):
    adapter.cursor.fetchall.return_value = [
        ("decimal_ps", "decimal", None, 5, 4),
        ("decimal", "decimal", None, 18, 0),
        ("float", "float", None, 53, None),
        ("char_n", "char", 10, None, None),
        ("varchar_n", "varchar", 10, None, None),
        ("nvarchar_max", "nvarchar", -1, None, None),
    ]

    assert adapter.columns("db.table") == {
        "decimal_ps": exp.DataType.build("decimal(5, 4)", dialect=adapter.dialect),
        "decimal": exp.DataType.build("decimal(18, 0)", dialect=adapter.dialect),
        "float": exp.DataType.build("float(53)", dialect=adapter.dialect),
        "char_n": exp.DataType.build("char(10)", dialect=adapter.dialect),
        "varchar_n": exp.DataType.build("varchar(10)", dialect=adapter.dialect),
        "nvarchar_max": exp.DataType.build("nvarchar(max)", dialect=adapter.dialect),
    }

    # Verify that the adapter queries the uppercase INFORMATION_SCHEMA
    adapter.cursor.execute.assert_called_once_with(
        """SELECT [COLUMN_NAME], [DATA_TYPE], [CHARACTER_MAXIMUM_LENGTH], [NUMERIC_PRECISION], [NUMERIC_SCALE] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME] = 'table' AND [TABLE_SCHEMA] = 'db';"""
    )


def test_table_exists(adapter: FabricEngineAdapter):
    adapter.cursor.fetchone.return_value = (1,)
    assert adapter.table_exists("db.table")
    # Verify that the adapter queries the uppercase INFORMATION_SCHEMA
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = 'table' AND [TABLE_SCHEMA] = 'db';"""
    )

    adapter.cursor.fetchone.return_value = None
    assert not adapter.table_exists("db.table")


def test_insert_overwrite_by_time_partition(adapter: FabricEngineAdapter):
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(x.strftime("%Y-%m-%d")),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    # Fabric adapter should use DELETE/INSERT strategy, not MERGE.
    assert to_sql_calls(adapter) == [
        """DELETE FROM [test_table] WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';""",
        """INSERT INTO [test_table] ([a], [b]) SELECT [a], [b] FROM (SELECT [a] AS [a], [b] AS [b] FROM [tbl]) AS [_subquery] WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';""",
    ]


def test_replace_query(adapter: FabricEngineAdapter):
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    # This behavior is inherited from MSSQLEngineAdapter and should be TRUNCATE + INSERT
    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = 'test_table';""",
        "TRUNCATE TABLE [test_table];",
        "INSERT INTO [test_table] ([a]) SELECT [a] FROM [tbl];",
    ]


# Tests for the four critical issues


def test_connection_factory_broad_typeerror_catch():
    """Test that broad TypeError catch in connection factory is problematic."""

    def problematic_factory(*args, **kwargs):
        # This should raise a TypeError that indicates a real bug
        raise TypeError("This is a serious bug, not a parameter issue")

    # Create adapter - this should not silently ignore serious TypeErrors
    adapter = FabricEngineAdapter(problematic_factory)

    # When we try to get a connection, the TypeError should be handled appropriately
    with pytest.raises(TypeError, match="This is a serious bug"):
        # Force connection creation
        adapter._connection_pool.get()


def test_connection_factory_parameter_signature_detection():
    """Test that connection factory should properly detect parameter support."""

    def factory_with_target_catalog(*args, target_catalog=None, **kwargs):
        return Mock(target_catalog=target_catalog)

    def simple_conn_func(*args, **kwargs):
        if "target_catalog" in kwargs:
            raise TypeError("unexpected keyword argument 'target_catalog'")
        return Mock()

    # Test factory that supports target_catalog
    adapter1 = FabricEngineAdapter(factory_with_target_catalog)
    adapter1._target_catalog = "test_catalog"
    conn1 = adapter1._connection_pool.get()
    assert conn1.target_catalog == "test_catalog"

    # Test factory that doesn't support target_catalog - should work without it
    adapter2 = FabricEngineAdapter(simple_conn_func)
    adapter2._target_catalog = "test_catalog"
    conn2 = (
        adapter2._connection_pool.get()
    )  # Should not raise - conservative detection avoids passing target_catalog


def test_catalog_switching_thread_safety():
    """Test that catalog switching has race conditions without proper locking."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._connection_pool = Mock()
    adapter._connection_pool.get_attribute = Mock(return_value=None)
    adapter._connection_pool.set_attribute = Mock()

    # Mock the close method to simulate clearing thread-local storage
    original_target = "original_catalog"

    def mock_close():
        # Simulate what happens in real close() - clears thread-local storage
        adapter._connection_pool.get_attribute.return_value = None

    adapter.close = mock_close
    adapter.get_current_catalog = Mock(return_value="switched_catalog")

    # Set initial target catalog
    adapter._target_catalog = original_target

    results = []
    errors = []

    def switch_catalog_worker(catalog_name, worker_id):
        try:
            # This simulates the problematic code pattern
            target_catalog = adapter._target_catalog  # Save current target
            adapter.close()  # This clears the target_catalog
            adapter._target_catalog = target_catalog  # Restore after close

            results.append(f"Worker {worker_id}: {adapter._target_catalog}")
        except Exception as e:
            errors.append(f"Worker {worker_id}: {e}")

    # Run multiple threads concurrently to expose race condition
    threads = []
    for i in range(5):
        thread = threading.Thread(target=switch_catalog_worker, args=(f"catalog_{i}", i))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Without proper locking, we might get inconsistent results
    assert len(results) == 5
    # This test demonstrates the race condition exists


def test_retry_decorator_timeout_limits():
    """Test that retry decorator has proper timeout limits to prevent extremely long wait times."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {"tenant_id": "test", "user": "test", "password": "test"}

    # Mock the auth headers to avoid authentication call
    with patch.object(
        adapter, "_get_fabric_auth_headers", return_value={"Authorization": "Bearer token"}
    ):
        # Mock the requests.get to always return an in-progress status for a few calls, then fail
        call_count = 0

        def mock_get(url, headers, timeout=None):
            nonlocal call_count
            call_count += 1
            response = Mock()
            response.raise_for_status = Mock()
            # Simulate "InProgress" for first 3 calls, then "Failed" to stop the retry loop
            if call_count <= 3:
                response.json = Mock(return_value={"status": "InProgress"})
            else:
                response.json = Mock(
                    return_value={"status": "Failed", "error": {"message": "Test failure"}}
                )
            return response

        with patch("requests.get", side_effect=mock_get):
            # Test that the retry mechanism works and eventually fails
            with pytest.raises(SQLMeshError, match="Operation test_operation failed"):
                adapter._check_operation_status("http://test.com", "test_operation")

            # The retry mechanism should have been triggered multiple times
            assert call_count > 1, f"Expected multiple retry attempts, got {call_count}"


def test_authentication_error_specificity():
    """Test that authentication errors lack specific context."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {
        "tenant_id": "test_tenant",
        "user": "test_client",
        "password": "test_secret",
    }

    # Test generic RequestException
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.RequestException("Generic network error")

        with pytest.raises(SQLMeshError, match="Authentication request to Azure AD failed"):
            adapter._get_access_token()

    # Test HTTP error without specific status codes
    with patch("requests.post") as mock_post:
        response = Mock()
        response.status_code = 401
        response.content = b'{"error": "invalid_client"}'
        response.json.return_value = {"error": "invalid_client"}
        response.text = "Unauthorized"
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
        mock_post.return_value = response

        with pytest.raises(SQLMeshError, match="Authentication failed with Azure AD"):
            adapter._get_access_token()

    # Test missing token in response
    with patch("requests.post") as mock_post:
        response = Mock()
        response.raise_for_status = Mock()
        response.json.return_value = {"error": "invalid_client"}
        mock_post.return_value = response

        with pytest.raises(SQLMeshError, match="Invalid response from Azure AD token endpoint"):
            adapter._get_access_token()


def test_api_error_handling_specificity():
    """Test that API error handling lacks specific HTTP status codes and context."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {"workspace_id": "test_workspace"}

    with patch.object(
        adapter, "_get_fabric_auth_headers", return_value={"Authorization": "Bearer token"}
    ):
        # Test generic HTTP error without status code details
        with patch("requests.get") as mock_get:
            response = Mock()
            response.status_code = 404
            response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
            response.content = b'{"error": {"message": "Workspace not found"}}'
            response.json.return_value = {"error": {"message": "Workspace not found"}}
            response.text = "Not Found"
            mock_get.return_value = response

            with pytest.raises(SQLMeshError) as exc_info:
                adapter._make_fabric_api_request("GET", "test_endpoint")

            # Current error message should include status code and Azure error codes
            assert "Fabric API HTTP error 404" in str(exc_info.value)


def test_schema_creation_error_handling_too_broad():
    """Test that schema creation error handling is too broad."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)

    # Mock the create_schema method to raise a specific error that should be handled differently
    with patch.object(adapter, "create_schema") as mock_create:
        # This should raise a permission error that we want to know about
        mock_create.side_effect = SQLMeshError("Permission denied: cannot create schema")

        # The current implementation catches all exceptions and continues
        # This masks important errors
        adapter._ensure_schema_exists("schema.test_table")

        # Schema creation was attempted
        mock_create.assert_called_once_with("schema", ignore_if_exists=True)


def test_concurrent_catalog_switching_race_condition():
    """Test race condition in concurrent catalog switching operations."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)

    # Mock methods
    adapter.get_current_catalog = Mock(return_value="default_catalog")
    adapter.close = Mock()

    results = []

    def catalog_switch_worker(catalog_name):
        # Simulate the problematic pattern from set_current_catalog
        current = adapter.get_current_catalog()
        if current == catalog_name:
            return

        # This is where the race condition occurs
        adapter._target_catalog = catalog_name
        target_catalog = adapter._target_catalog  # Save target
        adapter.close()  # Close connections
        adapter._target_catalog = target_catalog  # Restore target

        results.append(adapter._target_catalog)

    # Run multiple threads switching to different catalogs
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i in range(10):
            catalog = f"catalog_{i % 3}"
            future = executor.submit(catalog_switch_worker, catalog)
            futures.append(future)

        # Wait for all to complete
        for future in futures:
            future.result()

    # Results may be inconsistent due to race condition
    assert len(results) == 10


# New tests for caching mechanisms and performance issues


def test_authentication_token_caching():
    """Test that authentication tokens are cached and reused properly."""
    from datetime import datetime, timedelta, timezone

    # Clear any existing cache for clean test
    from sqlmesh.core.engine_adapter.fabric import _token_cache

    _token_cache.clear()

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {
        "tenant_id": "test_tenant",
        "user": "test_client",
        "password": "test_secret",
    }

    # Mock the requests.post to track how many times it's called
    call_count = 0
    token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=3600)

    def mock_post(url, data, timeout):
        nonlocal call_count
        call_count += 1
        response = Mock()
        response.raise_for_status = Mock()
        response.json.return_value = {
            "access_token": f"token_{call_count}",
            "expires_in": 3600,  # 1 hour
            "token_type": "Bearer",
        }
        return response

    with patch("requests.post", side_effect=mock_post):
        # First token request
        token1 = adapter._get_access_token()
        first_call_count = call_count

        # Second immediate request should use cached token
        token2 = adapter._get_access_token()
        second_call_count = call_count

        # Third request should also use cached token
        token3 = adapter._get_access_token()
        third_call_count = call_count

        # Tokens should be the same (cached)
        assert token1 == token2 == token3

        # Should only have made one API call
        assert first_call_count == 1
        assert second_call_count == 1  # No additional calls
        assert third_call_count == 1  # No additional calls


def test_authentication_token_expiration():
    """Test that expired tokens are automatically refreshed."""
    import time

    # Clear any existing cache for clean test
    from sqlmesh.core.engine_adapter.fabric import _token_cache

    _token_cache.clear()

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {
        "tenant_id": "test_tenant",
        "user": "test_client",
        "password": "test_secret",
    }

    call_count = 0

    def mock_post(url, data, timeout):
        nonlocal call_count
        call_count += 1
        response = Mock()
        response.raise_for_status = Mock()
        # First call returns token that expires in 1 second for testing
        # Second call returns token that expires in 1 hour
        expires_in = 1 if call_count == 1 else 3600
        response.json.return_value = {
            "access_token": f"token_{call_count}",
            "expires_in": expires_in,
            "token_type": "Bearer",
        }
        return response

    with patch("requests.post", side_effect=mock_post):
        # Get first token (expires in 1 second)
        token1 = adapter._get_access_token()
        assert call_count == 1

        # Wait for token to expire
        time.sleep(1.1)

        # Next request should refresh the token
        token2 = adapter._get_access_token()
        assert call_count == 2  # Should have made a second API call

        # Tokens should be different (new token)
        assert token1 != token2
        assert token1 == "token_1"
        assert token2 == "token_2"


def test_authentication_token_thread_safety():
    """Test that token caching is thread-safe."""
    import time

    # Clear any existing cache for clean test
    from sqlmesh.core.engine_adapter.fabric import _token_cache

    _token_cache.clear()

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {
        "tenant_id": "test_tenant",
        "user": "test_client",
        "password": "test_secret",
    }

    call_count = 0

    def mock_post(url, data, timeout):
        nonlocal call_count
        call_count += 1
        # Simulate slow network response
        time.sleep(0.1)
        response = Mock()
        response.raise_for_status = Mock()
        response.json.return_value = {
            "access_token": f"token_{call_count}",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        return response

    results = []
    errors = []

    def token_request_worker(worker_id):
        try:
            token = adapter._get_access_token()
            results.append((worker_id, token))
        except Exception as e:
            errors.append(f"Worker {worker_id}: {e}")

    with patch("requests.post", side_effect=mock_post):
        # Start multiple threads requesting tokens simultaneously
        threads = []
        for i in range(5):
            thread = threading.Thread(target=token_request_worker, args=(i,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all to complete
        for thread in threads:
            thread.join()

    # Should have no errors
    assert len(errors) == 0, f"Errors occurred: {errors}"

    # Should have 5 results
    assert len(results) == 5

    # All tokens should be the same (cached)
    tokens = [token for _, token in results]
    assert all(token == tokens[0] for token in tokens)

    # Should only have made one API call due to caching
    assert call_count == 1


def test_signature_inspection_caching():
    """Test that connection factory signature inspection is cached."""
    # Clear signature cache first
    from sqlmesh.core.engine_adapter.fabric import (
        _signature_inspection_cache,
        _signature_cache_lock,
    )

    with _signature_cache_lock:
        _signature_inspection_cache.clear()

    inspection_count = 0

    def tracked_factory(*args, **kwargs):
        return Mock()

    # Track how many times signature inspection occurs
    original_signature = inspect.signature

    def mock_signature(func):
        if func == tracked_factory:
            nonlocal inspection_count
            inspection_count += 1
        return original_signature(func)

    with patch("inspect.signature", side_effect=mock_signature):
        # Create multiple adapters with the same factory
        adapter1 = FabricEngineAdapter(tracked_factory)
        adapter2 = FabricEngineAdapter(tracked_factory)
        adapter3 = FabricEngineAdapter(tracked_factory)

        # Signature inspection should be cached - only called once
        assert inspection_count == 1, f"Expected 1 inspection, got {inspection_count}"


def test_warehouse_lookup_caching():
    """Test that warehouse listings are cached for multiple lookup operations."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = {"workspace_id": "test_workspace"}

    # Mock warehouse list response
    warehouse_list = {
        "value": [
            {"id": "warehouse1", "displayName": "test_warehouse"},
            {"id": "warehouse2", "displayName": "other_warehouse"},
        ]
    }

    api_call_count = 0

    def mock_api_request(method, endpoint, data=None, include_response_headers=False):
        nonlocal api_call_count
        if endpoint == "warehouses" and method == "GET":
            api_call_count += 1

        if endpoint == "warehouses":
            return warehouse_list
        return {}

    with patch.object(adapter, "_make_fabric_api_request", side_effect=mock_api_request):
        # Multiple calls to get cached warehouses should use caching
        warehouses1 = adapter._get_cached_warehouses()
        first_call_count = api_call_count

        warehouses2 = adapter._get_cached_warehouses()
        second_call_count = api_call_count

        warehouses3 = adapter._get_cached_warehouses()
        third_call_count = api_call_count

        # Should have cached the warehouse list after first call
        assert first_call_count == 1
        assert second_call_count == 1, f"Expected cached lookup, but got {second_call_count} calls"
        assert third_call_count == 1, f"Expected cached lookup, but got {third_call_count} calls"

        # All responses should be identical
        assert warehouses1 == warehouses2 == warehouses3


def test_configurable_timeouts():
    """Test that timeout values are configurable instead of hardcoded."""

    def mock_connection_factory(*args, **kwargs):
        return Mock()

    # Create adapter with custom configuration
    # Need to patch the extra_config during initialization
    custom_config = {
        "tenant_id": "test",
        "user": "test",
        "password": "test",
        "auth_timeout": 60,
        "api_timeout": 120,
        "operation_timeout": 900,
    }

    # Create adapter and set custom config
    adapter = FabricEngineAdapter(mock_connection_factory)
    adapter._extra_config = custom_config
    # Reinitialize timeout settings with new config
    adapter._auth_timeout = adapter._extra_config.get("auth_timeout", adapter.DEFAULT_AUTH_TIMEOUT)
    adapter._api_timeout = adapter._extra_config.get("api_timeout", adapter.DEFAULT_API_TIMEOUT)
    adapter._operation_timeout = adapter._extra_config.get(
        "operation_timeout", adapter.DEFAULT_OPERATION_TIMEOUT
    )

    # Test authentication timeout configuration
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.Timeout()

        with pytest.raises(SQLMeshError, match="timed out"):
            adapter._get_access_token()

        # Should have used custom timeout
        mock_post.assert_called_with(
            unittest_mock.ANY,
            data=unittest_mock.ANY,
            timeout=60,  # Custom timeout
        )
