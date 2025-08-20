from __future__ import annotations

import typing as t
import logging
import requests
from functools import cached_property
from sqlglot import exp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    InsertOverwriteStrategy,
    SourceQuery,
)
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.connection_pool import ConnectionPool

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


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

    def __init__(
        self, connection_factory_or_pool: t.Union[t.Callable, t.Any], *args: t.Any, **kwargs: t.Any
    ) -> None:
        # Wrap connection factory to support changing the catalog dynamically at runtime
        if not isinstance(connection_factory_or_pool, ConnectionPool):
            original_connection_factory = connection_factory_or_pool

            connection_factory_or_pool = lambda *args, **kwargs: original_connection_factory(
                target_catalog=self._target_catalog, *args, **kwargs
            )

        super().__init__(connection_factory_or_pool, *args, **kwargs)

    @property
    def _target_catalog(self) -> t.Optional[str]:
        return self._connection_pool.get_attribute("target_catalog")

    @_target_catalog.setter
    def _target_catalog(self, value: t.Optional[str]) -> None:
        self._connection_pool.set_attribute("target_catalog", value)

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        # Override to avoid MERGE statement which isn't fully supported in Fabric
        return EngineAdapter._insert_overwrite_by_condition(
            self,
            table_name=table_name,
            source_queries=source_queries,
            target_columns_to_types=target_columns_to_types,
            where=where,
            insert_overwrite_strategy_override=InsertOverwriteStrategy.DELETE_INSERT,
            **kwargs,
        )

    @property
    def api_client(self) -> FabricHttpClient:
        # the requests Session is not guaranteed to be threadsafe
        # so we create a http client per thread on demand
        if existing_client := self._connection_pool.get_attribute("api_client"):
            return existing_client

        tenant_id: t.Optional[str] = self._extra_config.get("tenant_id")
        workspace_id: t.Optional[str] = self._extra_config.get("workspace_id")
        client_id: t.Optional[str] = self._extra_config.get("user")
        client_secret: t.Optional[str] = self._extra_config.get("password")

        if not tenant_id or not client_id or not client_secret:
            raise SQLMeshError(
                "Service Principal authentication requires tenant_id, client_id, and client_secret "
                "in the Fabric connection configuration"
            )

        if not workspace_id:
            raise SQLMeshError(
                "Fabric requires the workspace_id to be configured in the connection configuration to create / drop catalogs"
            )

        client = FabricHttpClient(
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        self._connection_pool.set_attribute("api_client", client)
        return client

    def _create_catalog(self, catalog_name: exp.Identifier) -> None:
        """Create a catalog (warehouse) in Microsoft Fabric via REST API."""
        warehouse_name = catalog_name.sql(dialect=self.dialect, identify=False)
        logger.info(f"Creating Fabric warehouse: {warehouse_name}")

        self.api_client.create_warehouse(warehouse_name)

    def _drop_catalog(self, catalog_name: exp.Identifier) -> None:
        """Drop a catalog (warehouse) in Microsoft Fabric via REST API."""
        warehouse_name = catalog_name.sql(dialect=self.dialect, identify=False)

        logger.info(f"Deleting Fabric warehouse: {warehouse_name}")
        self.api_client.delete_warehouse(warehouse_name)

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

        # note: we call close() on the connection pool instead of self.close() because self.close() calls close_all()
        # on the connection pool but we just want to close the connection for this thread
        self._connection_pool.close()
        self._target_catalog = catalog_name  # new connections will use this catalog

        catalog_after_switch = self.get_current_catalog()

        if catalog_after_switch != catalog_name:
            # We need to raise an error if the catalog switch failed to prevent the operation that needed the catalog switch from being run against the wrong catalog
            raise SQLMeshError(
                f"Unable to switch catalog to {catalog_name}, catalog ended up as {catalog_after_switch}"
            )


class FabricHttpClient:
    def __init__(self, tenant_id: str, workspace_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_id = workspace_id

    def create_warehouse(self, warehouse_name: str) -> None:
        """Create a catalog (warehouse) in Microsoft Fabric via REST API."""
        logger.info(f"Creating Fabric warehouse: {warehouse_name}")

        request_data = {
            "displayName": warehouse_name,
            "description": f"Warehouse created by SQLMesh: {warehouse_name}",
        }

        response = self.session.post(self._endpoint_url("warehouses"), json=request_data)
        response.raise_for_status()

        # Handle direct success (201) or async creation (202)
        if response.status_code == 201:
            logger.info(f"Successfully created Fabric warehouse: {warehouse_name}")
            return

        if response.status_code == 202 and (location_header := response.headers.get("location")):
            logger.info(f"Warehouse creation initiated for: {warehouse_name}")
            self._wait_for_completion(location_header, warehouse_name)
            logger.info(f"Successfully created Fabric warehouse: {warehouse_name}")
        else:
            logger.error(f"Unexpected response from Fabric API: {response}\n{response.text}")
            raise SQLMeshError(f"Unable to create warehouse: {response}")

    def delete_warehouse(self, warehouse_name: str) -> None:
        """Drop a catalog (warehouse) in Microsoft Fabric via REST API."""
        logger.info(f"Deleting Fabric warehouse: {warehouse_name}")

        # Get the warehouse ID by listing warehouses
        response = self.session.get(self._endpoint_url("warehouses"))
        response.raise_for_status()

        warehouse_name_to_id = {
            warehouse.get("displayName"): warehouse.get("id")
            for warehouse in response.json().get("value", [])
        }

        warehouse_id = warehouse_name_to_id.get(warehouse_name, None)

        if not warehouse_id:
            logger.error(
                f"Fabric warehouse does not exist: {warehouse_name}\n(available warehouses: {', '.join(warehouse_name_to_id)})"
            )
            raise SQLMeshError(
                f"Unable to delete Fabric warehouse {warehouse_name} as it doesnt exist"
            )

        # Delete the warehouse by ID
        response = self.session.delete(self._endpoint_url(f"warehouses/{warehouse_id}"))
        response.raise_for_status()

        logger.info(f"Successfully deleted Fabric warehouse: {warehouse_name}")

    @cached_property
    def session(self) -> requests.Session:
        s = requests.Session()

        access_token = self._get_access_token()
        s.headers.update({"Authorization": f"Bearer {access_token}"})

        return s

    def _endpoint_url(self, endpoint: str) -> str:
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]

        return f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/{endpoint}"

    def _get_access_token(self) -> str:
        """Get access token using Service Principal authentication."""

        # Use Azure AD OAuth2 token endpoint
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://api.fabric.microsoft.com/.default",
        }

        response = requests.post(token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        return token_data["access_token"]

    def _wait_for_completion(self, location_url: str, operation_name: str) -> None:
        """Poll the operation status until completion."""

        @retry(
            wait=wait_exponential(multiplier=1, min=1, max=30),
            stop=stop_after_attempt(20),
            retry=retry_if_result(lambda result: result not in ["Succeeded", "Failed"]),
        )
        def _poll() -> str:
            response = self.session.get(location_url)
            response.raise_for_status()

            result = response.json()
            status = result.get("status", "Unknown")

            logger.debug(f"Operation {operation_name} status: {status}")

            if status == "Failed":
                error_msg = result.get("error", {}).get("message", "Unknown error")
                raise SQLMeshError(f"Operation {operation_name} failed: {error_msg}")
            elif status in ["InProgress", "Running"]:
                logger.debug(f"Operation {operation_name} still in progress...")
            elif status not in ["Succeeded"]:
                logger.warning(f"Unknown status '{status}' for operation {operation_name}")

            return status

        final_status = _poll()
        if final_status != "Succeeded":
            raise SQLMeshError(f"Operation {operation_name} completed with status: {final_status}")
