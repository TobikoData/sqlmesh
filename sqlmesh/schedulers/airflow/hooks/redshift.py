from __future__ import annotations

import typing as t

import redshift_connector
from airflow.providers.common.sql.hooks.sql import DbApiHook


class SQLMeshRedshiftHook(DbApiHook):
    """
    Uses the Redshift Python DB API connector.
    """

    conn_name_attr = "sqlmesh_redshift_conn_id"
    default_conn_name = "sqlmesh_redshift_default"
    conn_type = "sqlmesh_redshift"
    hook_name = "SQLMesh Redshift"
    connector = redshift_connector

    def get_conn(self) -> redshift_connector.Connection:
        """Returns a Redshift connection object"""
        db = self.get_connection(getattr(self, t.cast(str, self.conn_name_attr)))

        return self.connector.connect(
            host=db.host,
            port=db.port,
            user=db.login,
            password=db.password,
            database=db.schema,
            **db.extra_dejson,
        )
