from __future__ import annotations

import typing as t

from airflow.providers.common.sql.hooks.sql import DbApiHook

if t.TYPE_CHECKING:
    from clickhouse_connect.dbapi.connection import Connection


class SQLMeshClickHouseHook(DbApiHook):
    """
    Uses the ClickHouse Python DB API connector.
    """

    conn_name_attr = "sqlmesh_clickhouse_conn_id"
    default_conn_name = "sqlmesh_clickhouse_default"
    conn_type = "sqlmesh_clickhouse"
    hook_name = "SQLMesh ClickHouse"

    def get_conn(self) -> Connection:
        """Returns a ClickHouse connection object"""
        from clickhouse_connect.dbapi import connect

        db = self.get_connection(getattr(self, t.cast(str, self.conn_name_attr)))

        return connect(
            host=db.host,
            port=db.port,
            username=db.login,
            password=db.password,
            database=db.schema,
            **db.extra_dejson,
        )
