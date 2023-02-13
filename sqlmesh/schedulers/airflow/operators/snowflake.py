from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshSnowflakeOperator(BaseOperator):
    """The operator that evaluates a SQLMesh model snapshot on a Snowflake target

    Args:
        target: The target that will be executed by this operator instance.
        databricks_conn_id: The Airflow connection id for the snowflake target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        snowflake_conn_id: str = SnowflakeHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._snowflake_conn_id = snowflake_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> SnowflakeHook:
        """Gets the Snowflake Hook which contains the DB API connection object"""
        return SnowflakeHook(self._snowflake_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured Snowflake connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "snowflake")
