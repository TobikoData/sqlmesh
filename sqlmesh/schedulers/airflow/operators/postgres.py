from __future__ import annotations

import typing as t

from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshPostgresOperator(BaseDbApiOperator):
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
        super().__init__(
            target=target,
            conn_id=postgres_conn_id,
            dialect="postgres",
            hook_type=PostgresHook,
            **kwargs,
        )
