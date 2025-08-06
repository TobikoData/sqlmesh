from __future__ import annotations

import typing as t
import logging
import requests
from sqlglot import exp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName, SchemaName


from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin

logger = logging.getLogger(__name__)


class FabricEngineAdapter(LogicalMergeMixin, MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric.
    """

    DIALECT = "fabric"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_CREATE_DROP_CATALOG = True
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        # Store the original connection factory for wrapping
        self._original_connection_factory = self._connection_pool._connection_factory  # type: ignore
        # Replace the connection factory with our custom one
        self._connection_pool._connection_factory = self._create_fabric_connection  # type: ignore

    @property
    def _target_catalog(self) -> t.Optional[str]:
        """Thread-local target catalog storage."""
        return self._connection_pool.get_attribute("target_catalog")

    @_target_catalog.setter
    def _target_catalog(self, value: t.Optional[str]) -> None:
        """Thread-local target catalog storage."""
        self._connection_pool.set_attribute("target_catalog", value)

    def _switch_to_catalog_if_needed(
        self, table_or_name: t.Union[exp.Table, TableName, SchemaName]
    ) -> exp.Table:
        """
        Switch to catalog if the table/name is catalog-qualified.

        Returns the table object with catalog information parsed.
        If catalog switching occurs, the returned table will have catalog removed.
        """
        table = exp.to_table(table_or_name)

        if table.catalog:
            catalog_name = table.catalog
            logger.debug(f"Switching to catalog '{catalog_name}' for operation")
            self.set_current_catalog(catalog_name)

            # Return table without catalog for SQL generation
            return exp.Table(this=table.name, db=table.db)

        return table

    def _handle_schema_with_catalog(self, schema_name: SchemaName) -> t.Tuple[t.Optional[str], str]:
        """
        Handle schema operations with catalog qualification.

        Returns tuple of (catalog_name, schema_only_name).
        If catalog switching occurs, it will be performed.
        """
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

    def _create_fabric_connection(self) -> t.Any:
        """Custom connection factory that uses the target catalog if set."""
        # If we have a target catalog, we need to modify the connection parameters
        if self._target_catalog:
            # The original factory was created with partial(), so we need to extract and modify the kwargs
            if hasattr(self._original_connection_factory, "keywords"):
                # It's a partial function, get the original keywords
                original_kwargs = self._original_connection_factory.keywords.copy()
                original_kwargs["database"] = self._target_catalog
                # Call the underlying function with modified kwargs
                return self._original_connection_factory.func(**original_kwargs)

        # Use the original factory if no target catalog is set
        return self._original_connection_factory()

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Implements the insert overwrite strategy for Fabric using DELETE and INSERT.

        This method is overridden to avoid the MERGE statement from the parent
        MSSQLEngineAdapter, which is not fully supported in Fabric.
        """
        return EngineAdapter._insert_overwrite_by_condition(
            self,
            table_name=table_name,
            source_queries=source_queries,
            columns_to_types=columns_to_types,
            where=where,
            insert_overwrite_strategy_override=InsertOverwriteStrategy.DELETE_INSERT,
            **kwargs,
        )

    def _get_access_token(self) -> str:
        """Get access token using Service Principal authentication."""
        tenant_id = self._extra_config.get("tenant_id")
        client_id = self._extra_config.get("user")
        client_secret = self._extra_config.get("password")

        if not all([tenant_id, client_id, client_secret]):
            raise SQLMeshError(
                "Service Principal authentication requires tenant_id, client_id, and client_secret "
                "in the Fabric connection configuration"
            )

        if not requests:
            raise SQLMeshError("requests library is required for Fabric authentication")

        # Use Azure AD OAuth2 token endpoint
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default",
        }

        try:
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            return token_data["access_token"]
        except requests.exceptions.RequestException as e:
            raise SQLMeshError(f"Failed to authenticate with Azure AD: {e}")
        except KeyError:
            raise SQLMeshError("Invalid response from Azure AD token endpoint")

    def _get_fabric_auth_headers(self) -> t.Dict[str, str]:
        """Get authentication headers for Fabric REST API calls."""
        access_token = self._get_access_token()
        return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    def _make_fabric_api_request(
        self, method: str, endpoint: str, data: t.Optional[t.Dict[str, t.Any]] = None
    ) -> t.Dict[str, t.Any]:
        """Make a request to the Fabric REST API."""
        if not requests:
            raise SQLMeshError("requests library is required for Fabric catalog operations")

        workspace_id = self._extra_config.get("workspace_id")
        if not workspace_id:
            raise SQLMeshError(
                "workspace_id parameter is required in connection config for Fabric catalog operations"
            )

        base_url = "https://api.fabric.microsoft.com/v1"
        url = f"{base_url}/workspaces/{workspace_id}/{endpoint}"

        headers = self._get_fabric_auth_headers()

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise SQLMeshError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            if response.status_code == 204:  # No content
                return {}

            return response.json() if response.content else {}

        except requests.exceptions.HTTPError as e:
            error_details = ""
            try:
                if response.content:
                    error_response = response.json()
                    error_details = error_response.get("error", {}).get(
                        "message", str(error_response)
                    )
            except (ValueError, AttributeError):
                error_details = response.text if hasattr(response, "text") else str(e)

            raise SQLMeshError(f"Fabric API HTTP error ({response.status_code}): {error_details}")
        except requests.exceptions.RequestException as e:
            raise SQLMeshError(f"Fabric API request failed: {e}")

    def _make_fabric_api_request_with_location(
        self, method: str, endpoint: str, data: t.Optional[t.Dict[str, t.Any]] = None
    ) -> t.Dict[str, t.Any]:
        """Make a request to the Fabric REST API and return response with status code and location."""
        if not requests:
            raise SQLMeshError("requests library is required for Fabric catalog operations")

        workspace_id = self._extra_config.get("workspace_id")
        if not workspace_id:
            raise SQLMeshError(
                "workspace_id parameter is required in connection config for Fabric catalog operations"
            )

        base_url = "https://api.fabric.microsoft.com/v1"
        url = f"{base_url}/workspaces/{workspace_id}/{endpoint}"
        headers = self._get_fabric_auth_headers()

        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data)
            else:
                raise SQLMeshError(f"Unsupported HTTP method for location tracking: {method}")

            # Check for errors first
            response.raise_for_status()

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

        except requests.exceptions.HTTPError as e:
            error_details = ""
            try:
                if response.content:
                    error_response = response.json()
                    error_details = error_response.get("error", {}).get(
                        "message", str(error_response)
                    )
            except (ValueError, AttributeError):
                error_details = response.text if hasattr(response, "text") else str(e)

            raise SQLMeshError(f"Fabric API HTTP error ({response.status_code}): {error_details}")
        except requests.exceptions.RequestException as e:
            raise SQLMeshError(f"Fabric API request failed: {e}")

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(60),
        retry=retry_if_result(lambda result: result not in ["Succeeded", "Failed"]),
    )
    def _check_operation_status(self, location_url: str, operation_name: str) -> str:
        """Check the operation status and return the status string."""
        if not requests:
            raise SQLMeshError("requests library is required for Fabric catalog operations")

        headers = self._get_fabric_auth_headers()

        try:
            response = requests.get(location_url, headers=headers)
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
        """Poll the operation status until completion."""
        try:
            final_status = self._check_operation_status(location_url, operation_name)
            if final_status != "Succeeded":
                raise SQLMeshError(
                    f"Operation {operation_name} completed with status: {final_status}"
                )
        except Exception as e:
            if "retry" in str(e).lower():
                raise SQLMeshError(f"Operation {operation_name} did not complete within timeout")
            raise

    def _create_catalog(self, catalog_name: exp.Identifier) -> None:
        """Create a catalog (warehouse) in Microsoft Fabric via REST API."""
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

    def _drop_catalog(self, catalog_name: exp.Identifier) -> None:
        """Drop a catalog (warehouse) in Microsoft Fabric via REST API."""
        warehouse_name = catalog_name.sql(dialect=self.dialect, identify=False)

        logger.info(f"Deleting Fabric warehouse: {warehouse_name}")

        try:
            # Get the warehouse ID by listing warehouses
            warehouses = self._make_fabric_api_request("GET", "warehouses")
            warehouse_id = None

            for warehouse in warehouses.get("value", []):
                if warehouse.get("displayName") == warehouse_name:
                    warehouse_id = warehouse.get("id")
                    break

            if not warehouse_id:
                logger.info(f"Fabric warehouse does not exist: {warehouse_name}")
                return

            # Delete the warehouse by ID
            self._make_fabric_api_request("DELETE", f"warehouses/{warehouse_id}")
            logger.info(f"Successfully deleted Fabric warehouse: {warehouse_name}")

        except SQLMeshError as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg:
                logger.info(f"Fabric warehouse does not exist: {warehouse_name}")
                return
            logger.error(f"Failed to delete Fabric warehouse {warehouse_name}: {e}")
            raise

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
        current_catalog = self.get_current_catalog()

        # If already using the requested catalog, do nothing
        if current_catalog and current_catalog == catalog_name:
            logger.debug(f"Already using catalog '{catalog_name}', no action needed")
            return

        logger.info(f"Switching from catalog '{current_catalog}' to '{catalog_name}'")

        # Set the target catalog for our custom connection factory
        self._target_catalog = catalog_name

        # Save the target catalog before closing (close() clears thread-local storage)
        target_catalog = self._target_catalog

        # Close all existing connections since Fabric requires reconnection for catalog changes
        self.close()

        # Restore the target catalog after closing
        self._target_catalog = target_catalog

        # Verify the catalog switch worked by getting a new connection
        try:
            actual_catalog = self.get_current_catalog()
            if actual_catalog and actual_catalog == catalog_name:
                logger.debug(f"Successfully switched to catalog '{catalog_name}'")
            else:
                logger.warning(
                    f"Catalog switch may have failed. Expected '{catalog_name}', got '{actual_catalog}'"
                )
        except Exception as e:
            logger.debug(f"Could not verify catalog switch: {e}")

        logger.debug(f"Updated target catalog to '{catalog_name}' and closed connections")

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        """
        Override drop_schema to handle catalog-qualified schema names.
        Fabric doesn't support 'DROP SCHEMA [catalog].[schema]' syntax.
        """
        logger.debug(f"drop_schema called with: {schema_name} (type: {type(schema_name)})")

        # Use helper to handle catalog switching and get schema name
        catalog_name, schema_only = self._handle_schema_with_catalog(schema_name)

        # Use just the schema name for the operation
        super().drop_schema(schema_only, ignore_if_not_exists, cascade, **drop_args)

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """
        Override create_schema to handle catalog-qualified schema names.
        Fabric doesn't support 'CREATE SCHEMA [catalog].[schema]' syntax.
        """
        # Use helper to handle catalog switching and get schema name
        catalog_name, schema_only = self._handle_schema_with_catalog(schema_name)

        # Use just the schema name for the operation
        super().create_schema(schema_only, ignore_if_exists, **kwargs)

    def _ensure_schema_exists(self, table_name: TableName) -> None:
        """
        Ensure that the schema for a table exists before creating the table.
        This is necessary for Fabric because schemas must exist before tables can be created in them.
        """
        table = exp.to_table(table_name)
        if table.db:
            schema_name = table.db
            catalog_name = table.catalog

            # Build the full schema name
            full_schema_name = f"{catalog_name}.{schema_name}" if catalog_name else schema_name

            logger.debug(f"Ensuring schema exists: {full_schema_name}")

            try:
                # Create the schema if it doesn't exist
                self.create_schema(full_schema_name, ignore_if_exists=True)
            except Exception as e:
                logger.debug(f"Error creating schema {full_schema_name}: {e}")
                # Continue anyway - the schema might already exist or we might not have permissions

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

    def create_table(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Override create_table to ensure schema exists before creating tables.
        """
        # Ensure the schema exists before creating the table
        self._ensure_schema_exists(table_name)

        # Call the parent implementation
        super().create_table(
            table_name=table_name,
            columns_to_types=columns_to_types,
            primary_key=primary_key,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def ctas(
        self,
        table_name: TableName,
        query_or_df: t.Any,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Override ctas to ensure schema exists before creating tables.
        """
        # Ensure the schema exists before creating the table
        self._ensure_schema_exists(table_name)

        # Call the parent implementation
        super().ctas(
            table_name=table_name,
            query_or_df=query_or_df,
            columns_to_types=columns_to_types,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
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
        """
        Override create_view to handle catalog-qualified view names and ensure schema exists.
        Fabric doesn't support 'CREATE VIEW [catalog].[schema].[view]' syntax.
        """
        # Switch to catalog if needed and get unqualified table
        unqualified_view = self._switch_to_catalog_if_needed(view_name)

        # Ensure schema exists for the view
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
