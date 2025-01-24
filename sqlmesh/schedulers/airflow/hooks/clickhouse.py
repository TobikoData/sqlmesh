from __future__ import annotations

import typing as t

import clickhouse_connect
from airflow.providers.common.sql.hooks.sql import DbApiHook
import clickhouse_connect.dbapi as clickhouse_dbapi
from clickhouse_connect.dbapi import connect  # type: ignore
from clickhouse_connect.dbapi.connection import Connection
from clickhouse_connect.driver import httputil  # type: ignore
import clickhouse_connect.driver
from clickhouse_connect.driver import client


class SQLMeshClickHouseHook(DbApiHook):
    """
    Uses the ClickHouse Python DB API connector.
    """

    conn_name_attr = "sqlmesh_clickhouse_conn_id"
    default_conn_name = "sqlmesh_clickhouse_default"
    conn_type = "sqlmesh_clickhouse"
    hook_name = "SQLMesh ClickHouse"
    connector = clickhouse_connect

    def get_conn(self) -> Connection:
        """Returns a Redshift connection object"""
        db = self.get_connection(getattr(self, t.cast(str, self.conn_name_attr)))

        return connect(
            host=db.host,
            port=db.port,
            user=db.login,
            password=db.password,
            database=db.schema,
            **db.extra_dejson,
        )
