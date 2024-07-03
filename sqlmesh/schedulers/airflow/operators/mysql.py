from __future__ import annotations

import typing as t

from airflow.providers.mysql.hooks.mysql import MySqlHook

from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshMySqlOperator(BaseDbApiOperator):
    """The operator that evaluates a SQLMesh model snapshot on a mysql target

    Args:
        target: The target that will be executed by this operator instance.
        mysql_conn_id: The Airflow connection id for the mysql target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        mysql_conn_id: str = MySqlHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            target=target, conn_id=mysql_conn_id, dialect="mysql", hook_type=MySqlHook, **kwargs
        )
