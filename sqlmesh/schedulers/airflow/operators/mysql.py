from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshMysqlOperator(BaseOperator):
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
        super().__init__(**kwargs)
        self._target = target
        self._mysql_conn_id = mysql_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> MySqlHook:
        """Gets the mysql Hook which contains the DB API connection object"""
        return MySqlHook(self._mysql_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured mysql connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "mysql")
