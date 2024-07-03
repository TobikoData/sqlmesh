from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.common.sql.hooks.sql import DbApiHook


from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class BaseDbApiOperator(BaseOperator):
    """The base class for DB API operators.

    Args:
        target: The target that will be executed by this operator instance.
        conn_id: The Airflow connection id.
        dialect: The target SQL dialect.
        hook_type: The type of the DB API hook.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        conn_id: str,
        dialect: str,
        hook_type: t.Type[DbApiHook],
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._hook_type = hook_type
        self._target = target
        self._conn_id = conn_id
        self._dialect = dialect
        self._hook_params = kwargs

    def get_db_hook(self) -> DbApiHook:
        """Gets the Hook which contains the DB API connection object."""
        return self._hook_type(self._conn_id, **self._hook_params)

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured connection."""
        self._target.execute(context, lambda: self.get_db_hook().get_conn(), self._dialect)
