from __future__ import annotations

import typing as t
import logging
import time
from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.utils import optional_import
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin

logger = logging.getLogger(__name__)
requests = optional_import("requests")


class FabricAdapter(LogicalMergeMixin, MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric.
    """

    DIALECT = "fabric"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_CREATE_DROP_CATALOG = True
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

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
        tenant_id = self._extra_config.get("tenant")
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

        workspace = self._extra_config.get("workspace")
        if not workspace:
            raise SQLMeshError(
                "workspace parameter is required in connection config for Fabric catalog operations"
            )

        base_url = "https://api.fabric.microsoft.com/v1"
        url = f"{base_url}/workspaces/{workspace}/{endpoint}"

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

        workspace = self._extra_config.get("workspace")
        if not workspace:
            raise SQLMeshError(
                "workspace parameter is required in connection config for Fabric catalog operations"
            )

        base_url = "https://api.fabric.microsoft.com/v1"
        url = f"{base_url}/workspaces/{workspace}/{endpoint}"
        headers = self._get_fabric_auth_headers()

        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data)
            else:
                raise SQLMeshError(f"Unsupported HTTP method for location tracking: {method}")

            # Check for errors first
            response.raise_for_status()

            result = {"status_code": response.status_code}

            # Extract location header for polling
            if "location" in response.headers:
                result["location"] = response.headers["location"]

            # Include response body if present
            if response.content:
                result.update(response.json())

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

    def _poll_operation_status(self, location_url: str, operation_name: str) -> None:
        """Poll the operation status until completion."""
        if not requests:
            raise SQLMeshError("requests library is required for Fabric catalog operations")

        headers = self._get_fabric_auth_headers()
        max_attempts = 60  # Poll for up to 10 minutes
        initial_delay = 1  # Start with 1 second

        for attempt in range(max_attempts):
            try:
                response = requests.get(location_url, headers=headers)
                response.raise_for_status()

                result = response.json()
                status = result.get("status", "Unknown")

                logger.info(f"Operation {operation_name} status: {status}")

                if status == "Succeeded":
                    return
                if status == "Failed":
                    error_msg = result.get("error", {}).get("message", "Unknown error")
                    raise SQLMeshError(f"Operation {operation_name} failed: {error_msg}")
                elif status in ["InProgress", "Running"]:
                    # Use exponential backoff with max of 30 seconds
                    delay = min(initial_delay * (2 ** min(attempt // 3, 4)), 30)
                    logger.info(f"Waiting {delay} seconds before next status check...")
                    time.sleep(delay)
                else:
                    logger.warning(f"Unknown status '{status}' for operation {operation_name}")
                    time.sleep(5)

            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"Failed to poll status (attempt {attempt + 1}): {e}")
                    time.sleep(5)
                else:
                    raise SQLMeshError(f"Failed to poll operation status: {e}")

        raise SQLMeshError(f"Operation {operation_name} did not complete within timeout")

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
