from __future__ import annotations

import typing as t
import logging
import inspect
import threading
import time
from datetime import datetime, timedelta, timezone
import requests
from sqlglot import exp
from tenacity import retry, wait_exponential, retry_if_result, stop_after_delay
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.connection_pool import ConnectionPool

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName, SchemaName

from typing_extensions import NoReturn


from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin

logger = logging.getLogger(__name__)


# Cache for warehouse listings
_warehouse_list_cache: t.Dict[str, t.Tuple[t.Dict[str, t.Any], float]] = {}
_warehouse_cache_lock = threading.RLock()


class TokenCache:
    """Thread-safe cache for authentication tokens with expiration handling."""

    def __init__(self) -> None:
        self._cache: t.Dict[str, t.Tuple[str, datetime]] = {}  # key -> (token, expires_at)
        self._lock = threading.RLock()

    def get(self, cache_key: str) -> t.Optional[str]:
        with self._lock:
            if cache_key in self._cache:
                token, expires_at = self._cache[cache_key]
                if datetime.now(timezone.utc) < expires_at:
                    logger.debug(f"Using cached authentication token (expires at {expires_at})")
                    return token
                logger.debug(f"Cached token expired at {expires_at}, will refresh")
                del self._cache[cache_key]
            return None

    def set(self, cache_key: str, token: str, expires_in: int) -> None:
        with self._lock:
            # Add 5 minute buffer to prevent edge cases around expiration
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 300)
            self._cache[cache_key] = (token, expires_at)
            logger.debug(f"Cached authentication token (expires at {expires_at})")

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            logger.debug("Cleared authentication token cache")


# Global token cache shared across all Fabric adapter instances
_token_cache = TokenCache()


def catalog_aware(func: t.Callable) -> t.Callable:
    """Decorator to handle catalog switching automatically for schema operations."""

    def wrapper(
        self: "FabricEngineAdapter", schema_name: t.Any, *args: t.Any, **kwargs: t.Any
    ) -> t.Any:
        # Handle catalog-qualified schema names
        catalog_name, schema_only = self._handle_schema_with_catalog(schema_name)

        # Switch to target catalog if needed
        if catalog_name:
            self.set_current_catalog(catalog_name)

        return func(self, schema_only, *args, **kwargs)

    return wrapper


class FabricEngineAdapter(LogicalMergeMixin, MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric.
    """

    DIALECT = "fabric"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_CREATE_DROP_CATALOG = True
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    # Timeout constants
    AUTH_TIMEOUT = 30
    API_TIMEOUT = 60
    OPERATION_TIMEOUT = 600
    OPERATION_RETRY_MAX_WAIT = 30
    WAREHOUSE_CACHE_TTL = 300

    def __init__(
        self, connection_factory_or_pool: t.Union[t.Callable, t.Any], *args: t.Any, **kwargs: t.Any
    ) -> None:
        # Thread lock for catalog switching operations
        self._catalog_switch_lock = threading.RLock()
        # Store target catalog in instance rather than connection pool to survive connection closures
        self._fabric_target_catalog: t.Optional[str] = None

        # Wrap connection factory to support catalog switching
        if not isinstance(connection_factory_or_pool, ConnectionPool):
            original_connection_factory = connection_factory_or_pool
            supports_target_catalog = self._supports_target_catalog(original_connection_factory)

            def catalog_aware_factory(*args: t.Any, **kwargs: t.Any) -> t.Any:
                if supports_target_catalog:
                    logger.debug(
                        f"Creating connection with target_catalog={self._fabric_target_catalog}"
                    )
                    return original_connection_factory(
                        target_catalog=self._fabric_target_catalog, *args, **kwargs
                    )
                logger.debug("Connection factory does not support target_catalog")
                return original_connection_factory(*args, **kwargs)

            connection_factory_or_pool = catalog_aware_factory

        super().__init__(connection_factory_or_pool, *args, **kwargs)

    def _supports_target_catalog(self, factory: t.Callable) -> bool:
        """Check if the connection factory accepts the target_catalog parameter."""
        try:
            sig = inspect.signature(factory)
            return "target_catalog" in sig.parameters
        except (ValueError, TypeError):
            return False

    @property
    def _target_catalog(self) -> t.Optional[str]:
        return self._fabric_target_catalog

    @_target_catalog.setter
    def _target_catalog(self, value: t.Optional[str]) -> None:
        self._fabric_target_catalog = value

    def _switch_to_catalog_if_needed(
        self, table_or_name: t.Union[exp.Table, TableName, SchemaName]
    ) -> exp.Table:
        # Switch catalog context if needed for cross-catalog operations
        table = exp.to_table(table_or_name)

        if table.catalog:
            catalog_name = table.catalog
            logger.debug(f"Switching to catalog '{catalog_name}' for operation")
            self.set_current_catalog(catalog_name)

            # Return table without catalog for SQL generation
            return exp.Table(this=table.name, db=table.db)

        return table

    def _handle_schema_with_catalog(self, schema_name: SchemaName) -> t.Tuple[t.Optional[str], str]:
        # Parse and handle catalog-qualified schema names for cross-catalog operations
        # Handle Table objects created by schema_() function
        if isinstance(schema_name, exp.Table) and not schema_name.name:
            # This is a schema Table object - check for catalog qualification
            if schema_name.catalog:
                # Catalog-qualified schema: catalog.schema
                catalog_name = schema_name.catalog
                schema_only = schema_name.db
                logger.debug(
                    f"Detected catalog-qualified schema: catalog='{catalog_name}', schema='{schema_only}'"
                )
                # Switch to the catalog first
                self.set_current_catalog(catalog_name)
                return catalog_name, schema_only
            # Schema only, no catalog
            schema_only = schema_name.db
            logger.debug(f"Detected schema-only: schema='{schema_only}'")
            return None, schema_only
        # Handle string or table name inputs by parsing as table
        table = exp.to_table(schema_name)

        if table.catalog:
            # 3-part name detected (catalog.db.table) - this shouldn't happen for schema operations
            raise SQLMeshError(
                f"Invalid schema name format: {schema_name}. Expected 'schema' or 'catalog.schema', got 3-part name"
            )
        elif table.db:
            # Catalog-qualified schema: catalog.schema
            catalog_name = table.db
            schema_only = table.name
            logger.debug(
                f"Detected catalog.schema format: catalog='{catalog_name}', schema='{schema_only}'"
            )
            # Switch to the catalog first
            self.set_current_catalog(catalog_name)
            return catalog_name, schema_only
        else:
            # No catalog qualification, use as-is
            logger.debug(f"No catalog detected, using original: {schema_name}")
            return None, str(schema_name)

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        # Force DELETE_INSERT strategy for Fabric since MERGE isn't fully supported
        return EngineAdapter._insert_overwrite_by_condition(
            self,
            table_name,
            source_queries,
            columns_to_types,
            where,
            InsertOverwriteStrategy.DELETE_INSERT,
            **kwargs,
        )

    def _get_access_token(self) -> str:
        tenant_id = self._extra_config.get("tenant_id")
        client_id = self._extra_config.get("user")
        client_secret = self._extra_config.get("password")

        if not all([tenant_id, client_id, client_secret]):
            raise SQLMeshError(
                "Service Principal authentication requires tenant_id, client_id, and client_secret "
                "in the Fabric connection configuration"
            )

        # Create cache key from the credentials (without exposing secrets in logs)
        cache_key = f"{tenant_id}:{client_id}:{hash(client_secret)}"

        # Try to get cached token first
        cached_token = _token_cache.get(cache_key)
        if cached_token:
            return cached_token

        # Use double-checked locking to prevent multiple concurrent token requests
        with _token_cache._lock:
            # Check again inside the lock in case another thread got the token
            cached_token = _token_cache.get(cache_key)
            if cached_token:
                return cached_token

            # Use Azure AD OAuth2 token endpoint
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://api.fabric.microsoft.com/.default",
            }

            try:
                response = requests.post(token_url, data=data, timeout=self.AUTH_TIMEOUT)
                response.raise_for_status()
                token_data = response.json()

                access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour if not provided

                # Cache the token (this method is already thread-safe)
                _token_cache.set(cache_key, access_token, expires_in)

                return access_token

            except requests.exceptions.HTTPError as e:
                raise SQLMeshError(f"Authentication failed with Azure AD: {e}")
            except requests.exceptions.Timeout:
                raise SQLMeshError(f"Authentication request timed out after {self.AUTH_TIMEOUT}s")
            except requests.exceptions.RequestException as e:
                raise SQLMeshError(f"Authentication request to Azure AD failed: {e}")
            except KeyError:
                raise SQLMeshError(
                    "Invalid response from Azure AD token endpoint - missing access_token"
                )

    def _get_fabric_auth_headers(self) -> t.Dict[str, str]:
        access_token = self._get_access_token()
        return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    def _make_fabric_api_request(
        self,
        method: str,
        endpoint: str,
        data: t.Optional[t.Dict[str, t.Any]] = None,
        include_response_headers: bool = False,
    ) -> t.Dict[str, t.Any]:
        workspace_id = self._extra_config.get("workspace_id")
        if not workspace_id:
            raise SQLMeshError(
                "workspace_id parameter is required in connection config for Fabric catalog operations"
            )

        base_url = "https://api.fabric.microsoft.com/v1"
        url = f"{base_url}/workspaces/{workspace_id}/{endpoint}"

        headers = self._get_fabric_auth_headers()

        # Use configurable timeout
        timeout = self.API_TIMEOUT

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=timeout)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=timeout)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=headers, timeout=timeout)
            else:
                raise SQLMeshError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            if include_response_headers:
                result: t.Dict[str, t.Any] = {"status_code": response.status_code}

                # Extract location header for polling
                if "location" in response.headers:
                    result["location"] = response.headers["location"]

                # Include response body if present
                if response.content:
                    json_data = response.json()
                    if json_data:
                        result.update(json_data)

                return result

            if response.status_code == 204:  # No content
                return {}

            return response.json() if response.content else {}

        except requests.exceptions.HTTPError as e:
            self._raise_fabric_api_error(response, e)
        except requests.exceptions.Timeout:
            raise SQLMeshError(
                f"Fabric API request timed out after {timeout}s. The operation may still be in progress."
            )
        except requests.exceptions.ConnectionError as e:
            raise SQLMeshError(f"Failed to connect to Fabric API: {e}")
        except requests.exceptions.RequestException as e:
            raise SQLMeshError(f"Fabric API request failed: {e}")

    def _raise_fabric_api_error(self, response: t.Any, original_error: t.Any) -> NoReturn:
        """Helper to raise consistent API errors."""
        error_details = ""
        azure_error_code = ""
        try:
            if response.content:
                error_response = response.json()
                error_info = error_response.get("error", {})
                if isinstance(error_info, dict):
                    error_details = error_info.get("message", str(error_response))
                    azure_error_code = error_info.get("code", "")
                else:
                    error_details = str(error_response)
        except (ValueError, AttributeError):
            error_details = response.text if hasattr(response, "text") else str(original_error)

        azure_code_msg = f" (Azure Error: {azure_error_code})" if azure_error_code else ""
        raise SQLMeshError(
            f"Fabric API HTTP error {response.status_code}{azure_code_msg}: {error_details}"
        )

    def _make_fabric_api_request_with_location(
        self, method: str, endpoint: str, data: t.Optional[t.Dict[str, t.Any]] = None
    ) -> t.Dict[str, t.Any]:
        return self._make_fabric_api_request(method, endpoint, data, include_response_headers=True)

    def _check_operation_status(self, location_url: str, operation_name: str) -> str:
        # Create a retry decorator with instance-specific configuration
        retry_decorator = retry(
            wait=wait_exponential(multiplier=1, min=1, max=self.OPERATION_RETRY_MAX_WAIT),
            stop=stop_after_delay(self.OPERATION_TIMEOUT),
            retry=retry_if_result(lambda result: result not in ["Succeeded", "Failed"]),
        )

        # Apply retry to the actual status check method
        retrying_check = retry_decorator(self._check_operation_status_impl)
        return retrying_check(location_url, operation_name)

    def _check_operation_status_impl(self, location_url: str, operation_name: str) -> str:
        headers = self._get_fabric_auth_headers()

        try:
            response = requests.get(location_url, headers=headers, timeout=self.API_TIMEOUT)
            response.raise_for_status()

            result = response.json()
            status = result.get("status", "Unknown")

            logger.info(f"Operation {operation_name} status: {status}")

            if status == "Failed":
                error_msg = result.get("error", {}).get("message", "Unknown error")
                raise SQLMeshError(f"Operation {operation_name} failed: {error_msg}")
            elif status in ["InProgress", "Running"]:
                logger.info(f"Operation {operation_name} still in progress...")
            elif status not in ["Succeeded"]:
                logger.warning(f"Unknown status '{status}' for operation {operation_name}")

            return status

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to poll status: {e}")
            raise SQLMeshError(f"Failed to poll operation status: {e}")

    def _poll_operation_status(self, location_url: str, operation_name: str) -> None:
        try:
            final_status = self._check_operation_status(location_url, operation_name)
            if final_status != "Succeeded":
                raise SQLMeshError(
                    f"Operation {operation_name} completed with status: {final_status}"
                )
        except Exception as e:
            if "retry" in str(e).lower() or "timeout" in str(e).lower():
                raise SQLMeshError(
                    f"Operation {operation_name} did not complete within {self.OPERATION_TIMEOUT}s timeout. "
                    f"You can increase the operation_timeout configuration if needed."
                )
            raise

    def _create_catalog(self, catalog_name: exp.Identifier) -> None:
        warehouse_name = catalog_name.sql(dialect=self.dialect, identify=False)
        logger.info(f"Creating Fabric warehouse: {warehouse_name}")

        request_data = {
            "displayName": warehouse_name,
            "description": f"Warehouse created by SQLMesh: {warehouse_name}",
        }

        response = self._make_fabric_api_request_with_location("POST", "warehouses", request_data)

        # Handle direct success (201) or async creation (202)
        if response.get("status_code") == 201:
            logger.info(f"Successfully created Fabric warehouse: {warehouse_name}")
            return

        if response.get("status_code") == 202 and response.get("location"):
            logger.info(f"Warehouse creation initiated for: {warehouse_name}")
            self._poll_operation_status(response["location"], warehouse_name)
            logger.info(f"Successfully created Fabric warehouse: {warehouse_name}")
        else:
            raise SQLMeshError(f"Unexpected response from warehouse creation: {response}")

    def _get_cached_warehouses(self) -> t.Dict[str, t.Any]:
        workspace_id = self._extra_config.get("workspace_id")
        if not workspace_id:
            raise SQLMeshError(
                "workspace_id parameter is required in connection config for warehouse operations"
            )

        cache_key = workspace_id
        current_time = time.time()

        with _warehouse_cache_lock:
            if cache_key in _warehouse_list_cache:
                cached_data, cache_time = _warehouse_list_cache[cache_key]
                if current_time - cache_time < self.WAREHOUSE_CACHE_TTL:
                    logger.debug(
                        f"Using cached warehouse list (cached {current_time - cache_time:.1f}s ago)"
                    )
                    return cached_data
                logger.debug("Warehouse list cache expired, refreshing")
                del _warehouse_list_cache[cache_key]

        # Cache miss or expired - fetch fresh data
        logger.debug("Fetching warehouse list from Fabric API")
        warehouses = self._make_fabric_api_request("GET", "warehouses")

        # Cache the result
        with _warehouse_cache_lock:
            _warehouse_list_cache[cache_key] = (warehouses, current_time)

        return warehouses

    def _drop_catalog(self, catalog_name: exp.Identifier) -> None:
        warehouse_name = catalog_name.sql(dialect=self.dialect, identify=False)

        logger.info(f"Deleting Fabric warehouse: {warehouse_name}")

        try:
            # Get the warehouse ID by listing warehouses (with caching)
            warehouses = self._get_cached_warehouses()

            warehouse_id = next(
                (
                    warehouse.get("id")
                    for warehouse in warehouses.get("value", [])
                    if warehouse.get("displayName") == warehouse_name
                ),
                None,
            )

            if not warehouse_id:
                logger.info(f"Fabric warehouse does not exist: {warehouse_name}")
                return

            # Delete the warehouse by ID
            self._make_fabric_api_request("DELETE", f"warehouses/{warehouse_id}")

            # Clear warehouse cache after successful deletion since the list changed
            workspace_id = self._extra_config.get("workspace_id")
            if workspace_id:
                with _warehouse_cache_lock:
                    _warehouse_list_cache.pop(workspace_id, None)

            logger.info(f"Successfully deleted Fabric warehouse: {warehouse_name}")

        except SQLMeshError as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg:
                logger.info(f"Fabric warehouse does not exist: {warehouse_name}")
                return
            logger.error(f"Failed to delete Fabric warehouse {warehouse_name}: {e}")
            raise

    def get_current_catalog(self) -> t.Optional[str]:
        """
        Get the current catalog for Fabric connections.

        Override the default implementation to return our target catalog,
        since Fabric doesn't maintain session state and we manage catalog
        switching through connection recreation.
        """
        # Return our target catalog if set, otherwise query the database
        target = self._target_catalog
        if target:
            logger.debug(f"Returning target catalog: {target}")
            return target

        # Fall back to querying the database if no target catalog is set
        try:
            result = self.fetchone(exp.select(self.CURRENT_CATALOG_EXPRESSION))
            if result:
                catalog_name = result[0]
                logger.debug(f"Queried current catalog from database: {catalog_name}")
                # Set this as our target catalog for consistency
                self._target_catalog = catalog_name
                return catalog_name
        except Exception as e:
            logger.debug(f"Failed to query current catalog: {e}")

        return None

    def set_current_catalog(self, catalog_name: str) -> None:
        """
        Set the current catalog for Microsoft Fabric connections.

        Override to handle Fabric's stateless session limitation where USE statements
        don't persist across queries. Instead, we close existing connections and
        recreate them with the new catalog in the connection configuration.

        Args:
            catalog_name: The name of the catalog (warehouse) to switch to

        Note:
            Fabric doesn't support catalog switching via USE statements because each
            statement runs as an independent session. This method works around this
            limitation by updating the connection pool with new catalog configuration.

        See:
            https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-query-editor#limitations
        """
        # Use thread-safe locking for catalog switching operations
        with self._catalog_switch_lock:
            current_catalog = self._target_catalog
            logger.debug(f"Current target catalog before switch: {current_catalog}")

            # If already using the requested catalog, do nothing
            if current_catalog and current_catalog == catalog_name:
                logger.debug(f"Already using catalog '{catalog_name}', no action needed")
                return

            logger.info(f"Switching from catalog '{current_catalog}' to '{catalog_name}'")

            # Set the target catalog for our custom connection factory
            old_target = self._target_catalog
            self._target_catalog = catalog_name
            new_target = self._target_catalog
            logger.debug(f"Updated target catalog from '{old_target}' to '{new_target}'")

            # Close all existing connections since Fabric requires reconnection for catalog changes
            self.close()
            logger.debug("Closed all existing connections")

            # Verify the target catalog was set correctly
            final_target = self._target_catalog
            logger.debug(f"Final target catalog after switch: {final_target}")
            logger.debug(f"Successfully switched to catalog '{catalog_name}'")

    @catalog_aware
    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        super().drop_schema(schema_name, ignore_if_not_exists, cascade, **drop_args)

    @catalog_aware
    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().create_schema(schema_name, ignore_if_exists, **kwargs)

    def _ensure_schema_exists(self, table_name: TableName) -> None:
        """
        Ensure that the schema for a table exists before creating the table.
        This is necessary for Fabric because schemas must exist before tables can be created in them.
        """
        table = exp.to_table(table_name)
        if table.db:
            schema_name = table.db
            catalog_name = table.catalog

            logger.debug(f"Ensuring schema exists for table: {table}")
            logger.debug(f"Schema: {schema_name}, Catalog: {catalog_name}")

            try:
                # If there's a catalog specified, switch to it first
                if catalog_name:
                    current_catalog = self.get_current_catalog()
                    if current_catalog != catalog_name:
                        logger.debug(f"Switching to catalog {catalog_name} for schema creation")
                        self.set_current_catalog(catalog_name)

                    # Create schema without catalog prefix since we're in the right catalog
                    logger.debug(f"Creating schema: {schema_name}")
                    self.create_schema(schema_name, ignore_if_exists=True)
                else:
                    # No catalog specified, create in current catalog
                    logger.debug(f"Creating schema in current catalog: {schema_name}")
                    self.create_schema(schema_name, ignore_if_exists=True)

            except SQLMeshError as e:
                error_msg = str(e).lower()
                if any(
                    keyword in error_msg for keyword in ["already exists", "duplicate", "exists"]
                ):
                    logger.debug(f"Schema {schema_name} already exists")
                elif any(
                    keyword in error_msg
                    for keyword in ["permission", "access", "denied", "forbidden"]
                ):
                    logger.warning(f"Insufficient permissions to create schema {schema_name}: {e}")
                else:
                    logger.warning(f"Failed to create schema {schema_name}: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error creating schema {schema_name}: {e}")
                # Continue anyway for backward compatibility, but log as warning instead of debug

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Override _create_table to ensure schema exists before creating tables.
        """
        # Extract table name for schema creation
        if isinstance(table_name_or_schema, exp.Schema):
            table_name = table_name_or_schema.this
        else:
            table_name = table_name_or_schema

        # Ensure the schema exists before creating the table
        self._ensure_schema_exists(table_name)

        # Call the parent implementation
        super()._create_table(
            table_name_or_schema=table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            table_description=table_description,
            column_descriptions=column_descriptions,
            table_kind=table_kind,
            **kwargs,
        )

    def create_view(
        self,
        view_name: SchemaName,
        query_or_df: t.Any,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        unqualified_view = self._switch_to_catalog_if_needed(view_name)
        self._ensure_schema_exists(unqualified_view)

        super().create_view(
            unqualified_view,
            query_or_df,
            columns_to_types,
            replace,
            materialized,
            materialized_properties,
            table_description,
            column_descriptions,
            view_properties,
            **create_kwargs,
        )
