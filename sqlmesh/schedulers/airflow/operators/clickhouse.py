from __future__ import annotations

import typing as t


from sqlmesh.schedulers.airflow.hooks.clickhouse import SQLMeshClickHouseHook
from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshClickHouseOperator(BaseDbApiOperator):
    """The operator that evaluates a SQLMesh model snapshot on a ClickHouse target

    Args:
        target: The target that will be executed by this operator instance.
        postgres_conn_id: The Airflow connection id for the postgres target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        clickhouse_conn_id: str = SQLMeshClickHouseHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            target=target,
            conn_id=clickhouse_conn_id,
            dialect="clickhouse",
            hook_type=SQLMeshClickHouseHook,
            **kwargs,
        )
