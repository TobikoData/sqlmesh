from __future__ import annotations

import typing as t

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if t.TYPE_CHECKING:
    from google.cloud.bigquery.dbapi import Connection


class SQLMeshBigQueryHook(GoogleBaseHook, DbApiHook):
    """
    Interact with BigQuery. This hook uses the Google Cloud connection.
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
        delegate_to: str | None = None,
        impersonation_chain: str | t.Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        GoogleBaseHook.__init__(
            self,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> Connection:
        """Returns a BigQuery DBAPI connection object."""
        from google.cloud.bigquery import Client
        from google.cloud.bigquery.dbapi import Connection

        try:
            creds, project_id = self._get_credentials_and_project_id()
        except AttributeError:
            creds, project_id = self.get_credentials_and_project_id()
        client = Client(project=project_id, credentials=creds)
        return Connection(client=client)
