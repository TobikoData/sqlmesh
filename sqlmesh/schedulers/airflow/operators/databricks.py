from __future__ import annotations

from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshDatabricksSQLOperator(BaseOperator):
    """The operator which evaluates a SQLMesh model snapshot on a Databricks cluster

    Args:
        target: The target that will be executed by this operator instance.
        databricks_conn_id: The Airflow connection id for the databricks target
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._target = target
        self._databricks_conn_id = databricks_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> DatabricksSqlHook:
        """Gets the Databricks SQL Hook which contains the DB API connection object"""
        return DatabricksSqlHook(self._databricks_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured Databricks connection"""
        connection = self.get_db_hook().get_conn()
        self._target.execute(context, connection, "spark")
