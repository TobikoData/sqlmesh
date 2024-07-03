from __future__ import annotations

import typing as t


from sqlmesh.schedulers.airflow.hooks.redshift import SQLMeshRedshiftHook
from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshRedshiftOperator(BaseDbApiOperator):
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
        super().__init__(
            target=target,
            conn_id=redshift_conn_id,
            dialect="redshift",
            hook_type=SQLMeshRedshiftHook,
            **kwargs,
        )
