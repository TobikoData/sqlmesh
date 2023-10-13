from __future__ import annotations

import typing as t

from airflow.models import BaseOperator
from airflow.utils.context import Context

from sqlmesh.schedulers.airflow.hooks.bigquery import SQLMeshBigQueryHook
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshBigQueryOperator(BaseOperator):
    """The operator that evaluates a SQLMesh model snapshot on Bigquery

    Args:
        target: The target that will be executed by this operator instance.
        bigquery_conn_id: The Airflow connection id for the bigquery target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        bigquery_conn_id: str = SQLMeshBigQueryHook.default_conn_name,
        delegate_to: str | None = None,
        impersonation_chain: str | t.Sequence[str] | None = None,
        location: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._bigquery_conn_id = bigquery_conn_id
        self._delegate_to = delegate_to
        self._impersonation_chain = impersonation_chain
        self._location = location

    def get_db_hook(self) -> SQLMeshBigQueryHook:
        """Gets the BigQuery Hook which contains the DB API connection object"""
        return SQLMeshBigQueryHook(
            self._bigquery_conn_id,
            delegate_to=self._delegate_to,
            impersonation_chain=self._impersonation_chain,
            location=self._location,
        )

    def execute(self, context: Context) -> None:
        """Executes the desired target against the configured BigQuery connection"""
        self._target.execute(
            context,
            lambda: self.get_db_hook().get_conn(),
            "bigquery",
            job_retries=self.get_db_hook().num_retries,
        )
