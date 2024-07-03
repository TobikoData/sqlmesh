from __future__ import annotations

import typing as t

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshMsSqlOperator(BaseDbApiOperator):
    """The operator that evaluates a SQLMesh model snapshot on a mssql target

    Args:
        target: The target that will be executed by this operator instance.
        mssql_conn_id: The Airflow connection id for the mssql target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        mssql_conn_id: str = MsSqlHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            target=target, conn_id=mssql_conn_id, dialect="mssql", hook_type=MsSqlHook, **kwargs
        )
