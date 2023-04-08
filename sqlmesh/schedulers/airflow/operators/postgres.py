from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshPostgresOperator(BaseOperator):
    """The operator that evaluates a SQLMesh model snapshot on a Postgres target

    Args:
        target: The target that will be executed by this operator instance.
        postgres_conn_id: The Airflow connection id for the postgres target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        postgres_conn_id: str = PostgresHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._postgres_conn_id = postgres_conn_id
        self._hook_params = kwargs

    def get_db_hook(self) -> PostgresHook:
        """Gets the Postgres Hook which contains the DB API connection object"""
        return PostgresHook(self._postgres_conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured Postgres connection"""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), "postgres")
