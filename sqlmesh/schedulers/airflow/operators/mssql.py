from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshMsSqlOperator(BaseOperator):
    """The operator that evaluates a SQLMesh model snapshot on a mssql target

    Args:
        target: The target that will be executed by this operator instance.
        mssql_conn_id: The Airflow connection id for the mssql target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        mssql_conn_id: str = MsSqlHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._mssql_conn_id = mssql_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> MsSqlHook:
        """Gets the mssql Hook which contains the DB API connection object"""
        return MsSqlHook(self._mssql_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured mssql connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "mssql")
