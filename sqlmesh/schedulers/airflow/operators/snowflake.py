from __future__ import annotations

import typing as t

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshSnowflakeOperator(BaseDbApiOperator):
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
        super().__init__(
            target=target,
            conn_id=snowflake_conn_id,
            dialect="snowflake",
            hook_type=SnowflakeHook,
            **kwargs,
        )
