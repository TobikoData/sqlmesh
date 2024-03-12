from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshTrinoOperator(BaseOperator):
    """The operator that evaluates a SQLMesh model snapshot on a Trino target

    Args:
        target: The target that will be executed by this operator instance.
        trino_conn_id: The Airflow connection id for the trino target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        trino_conn_id: str = TrinoHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._trino_conn_id = trino_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> TrinoHook:
        """Gets the Postgres Hook which contains the DB API connection object"""
        return TrinoHook(self._trino_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured Trino connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "trino")
