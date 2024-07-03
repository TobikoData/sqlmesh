from __future__ import annotations

import typing as t

from airflow.providers.trino.hooks.trino import TrinoHook

from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshTrinoOperator(BaseDbApiOperator):
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
        super().__init__(
            target=target, conn_id=trino_conn_id, dialect="trino", hook_type=TrinoHook, **kwargs
        )
