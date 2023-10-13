from __future__ import annotations

import typing as t

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if t.TYPE_CHECKING:
    from google.cloud.bigquery.dbapi import Connection


class SQLMeshBigQueryHook(GoogleBaseHook, DbApiHook):
    """
    Interact with BigQuery. This hook uses the Google Cloud connection. We didn't use the Airflow BigQueryHook
    because it implements an Airflow specific version of the BigQuery DB API that is different then the DB API
    provided from Google's python package.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param delegate_to: This performs a task on one host with reference to other hosts.
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    """

    conn_name_attr = "sqlmesh_gcp_conn_id"
    default_conn_name = "sqlmesh_google_cloud_bigquery_default"
    conn_type = "sqlmeshgcpbigquery"
    hook_name = "SQLMesh Google Bigquery"

    def __init__(
        self,
        gcp_conn_id: str = default_conn_name,
        delegate_to: t.Optional[str] = None,
        impersonation_chain: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        location: t.Optional[str] = None,
    ) -> None:
        GoogleBaseHook.__init__(
            self,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.location = location

    def get_conn(self) -> Connection:
        """Returns a BigQuery DBAPI connection object."""
        from google.api_core.client_info import ClientInfo
        from google.cloud.bigquery import Client
        from google.cloud.bigquery.dbapi import Connection

        # This method is private in older versions of the BigQuery library and public later. So we check for both
        try:
            creds, project_id = self._get_credentials_and_project_id()  # type: ignore
        except AttributeError:
            creds, project_id = self.get_credentials_and_project_id()  # type: ignore
        client = Client(
            project=project_id,
            credentials=creds,
            location=self.location,
            client_info=ClientInfo(user_agent="sqlmesh"),
        )
        return Connection(client=client)
