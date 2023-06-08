from __future__ import annotations

import typing as t

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.hooks.redshift import SQLMeshRedshiftHook
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshRedshiftOperator(BaseSQLOperator):
    """The operator that evaluates a SQLMesh model snapshot on Redshift cluster

    Args:
        target: The target that will be executed by this operator instance.
        redshift_conn_id: The Airflow connection id for the Redshift target.
    """

    def __init__(
        self,
        target: BaseTarget,
        redshift_conn_id: str = SQLMeshRedshiftHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(conn_id=redshift_conn_id, **kwargs)
        self._target = target

    def get_db_hook(self) -> SQLMeshRedshiftHook:
        """Gets the Redshift Hook which contains the DB API connection object"""
        return SQLMeshRedshiftHook()

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured Redshift connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "redshift")
