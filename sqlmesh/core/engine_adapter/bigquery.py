from __future__ import annotations

import contextlib
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import TransactionType
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.table import Table as BigQueryTable

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


class BigQueryEngineAdapter(EngineAdapter):

    DIALECT = "bigquery"
    DEFAULT_BATCH_SIZE = 1000

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, multithreaded=multithreaded)
        self._session_id = None

    @property
    def client(self) -> BigQueryClient:
        return self.cursor.connection._client

    @contextlib.contextmanager
    def transaction(
        self, transaction_type: TransactionType = TransactionType.DML
    ) -> t.Generator[None, None, None]:
        """A transaction context manager."""
        if self._session_id or transaction_type.is_ddl:
            yield
            return

        self.execute(exp.Transaction())
        job = self.cursor._query_job
        self._session_id = job.session_info.session_id
        try:
            yield
        except Exception as e:
            self.execute(exp.Rollback())
            self._session_id = None
            raise e
        else:
            self.execute(exp.Commit())
        finally:
            self._session_id = None

    def columns(self, table_name: TableName) -> t.Dict[str, str]:
        """Fetches column names and types for the target table."""
        table = self._get_table(table_name)
        return {field.name: field.field_type for field in table.schema}

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        """
        BigQuery does not support multiple transactions with deletes against the same table. Short term
        we are going to make this delete/insert non-transactional. Long term I want to try out writing to a staging
        table and then using API calls like copy partitions/write_truncate to see if we can implement atomic
        insert/overwrite.
        """
        if where is None:
            raise SQLMeshError(
                "Where condition is required when doing a BigQuery insert overwrite"
            )
        self.delete_from(table_name, where=where)
        self.insert_append(table_name, query_or_df, columns_to_types=columns_to_types)

    def table_exists(self, table_name: TableName) -> bool:
        from google.cloud.exceptions import NotFound

        try:
            self._get_table(table_name)
            return True
        except NotFound:
            return False

    def _get_table(self, table_name: TableName) -> BigQueryTable:
        """
        Returns a BigQueryTable object for the given table name.

        Raises: `google.cloud.exceptions.NotFound` if the table does not exist.
        """
        if isinstance(table_name, exp.Table):
            table_name = table_name.sql(dialect=self.dialect)

        return self.client.get_table(table_name)

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        self.execute(query)
        return self.cursor._query_job.to_dataframe()

    def execute(self, sql: t.Union[str, exp.Expression], **kwargs: t.Any) -> None:
        from google.cloud import bigquery  # type: ignore

        create_session = isinstance(sql, exp.Transaction) and self._session_id is None
        job_config = None
        if create_session:
            job_config = bigquery.QueryJobConfig(create_session=create_session)
        elif self._session_id:
            job_config = bigquery.QueryJobConfig(
                create_session=False,
                connection_properties=[
                    bigquery.query.ConnectionProperty(
                        key="session_id", value=self._session_id
                    )
                ],
            )
        super().execute(sql, **{**kwargs, "job_config": job_config})

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        if transaction_type.is_dml:
            return True
        elif transaction_type.is_ddl:
            return False
        raise ValueError(f"Unknown transaction type: {transaction_type}")
