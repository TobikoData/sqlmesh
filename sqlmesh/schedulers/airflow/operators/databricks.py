from __future__ import annotations

import os
import tempfile
import typing as t

from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.utils.context import Context

import sqlmesh
from sqlmesh import utils
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow.operators.base import BaseDbApiOperator
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget


class SQLMeshDatabricksSubmitOperator(DatabricksSubmitRunOperator):
    """Operator for submitting Databricks jobs to a Databricks cluster using the submit run API.

    Args:
        target: The target that will be executed by this operator instance.
        dbfs_location: The dbfs location where the app.py file and payload will be copied to.
        existing_cluster_id: The id of the cluster to run the job on. Either this or new_cluster must be specified.
        new_cluster: The specification for a new cluster to run the job on. Either this or existing_cluster_id must be specified.
    """

    def __init__(
        self,
        target: BaseTarget,
        dbfs_location: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        if not dbfs_location:
            raise ValueError(
                "dbfs_location is required for Databricks connections. See documentation for more details."
            )
        if not dbfs_location.startswith("dbfs:/"):
            raise ValueError(
                "dbfs_location must start with 'dbfs:/'. See documentation for more details."
            )
        super().__init__(**kwargs)
        self._target = target
        self._dbfs_location = os.path.join(dbfs_location, utils.random_id())

    def execute(self, context: Context) -> None:
        """Executes the target against the configured databricks cluster using the submit run API.

        SQLMesh copies the app.py file to the dbfs location specified in the operator. It also copies a file containing
        the target's payload to the dbfs location. The app.py file is then executed with the target's payload as an
        argument.

        TODO: Add support for `idempotency token`. This would allow this operator to reattach to an existing run if it
        exists instead of creating a new one. We would need to make sure this is done correctly by have the dbfs
        path use that token for the path instead of a random ID. Consider using a plan ID but also consider how
        cadence runs and restatements will also work.
        """
        from databricks_cli.dbfs.api import DbfsApi
        from databricks_cli.sdk.api_client import ApiClient

        if "new_cluster" not in self.json and "existing_cluster_id" not in self.json:
            http_path = self._hook.databricks_conn.extra_dejson.get("http_path")
            if not http_path:
                raise ValueError(
                    "Must provide a cluster to run on or new cluster specification. See documentation for more details."
                )
            cluster_id = http_path.split("/")[-1]
            if "-" not in cluster_id:
                raise ValueError(
                    "Must provide a non-DBSQL cluster to execute against. See documentation for more details."
                )
            self.json["existing_cluster_id"] = cluster_id

        api_client = ApiClient(
            host=f"https://{self._hook.host}",
            token=self._hook._get_token(raise_error=False),
            user=self._hook.databricks_conn.login,
            password=self._hook.databricks_conn.password,
        )
        dbfs_api = DbfsApi(api_client)

        local_app_path = os.path.join(
            os.path.dirname(os.path.abspath(sqlmesh.__file__)), "engines/spark/app.py"
        )
        remote_app_path = os.path.join(self._dbfs_location, "app.py")
        dbfs_api.cp(recursive=False, overwrite=True, src=local_app_path, dst=remote_app_path)

        command_payload = self._target.serialized_command_payload(context)
        with tempfile.TemporaryDirectory() as tmp:
            local_payload_path = os.path.join(tmp, commands.COMMAND_PAYLOAD_FILE_NAME)
            with open(local_payload_path, "w", encoding="utf-8") as payload_fd:
                payload_fd.write(command_payload)
            remote_payload_path = os.path.join(
                self._dbfs_location, commands.COMMAND_PAYLOAD_FILE_NAME
            )
            dbfs_api.cp(
                recursive=False, overwrite=True, src=local_payload_path, dst=remote_payload_path
            )
        task_arguments = {
            "dialect": "databricks",
            "default_catalog": self._target.default_catalog,
            "command_type": self._target.command_type.value if self._target.command_type else None,
            "ddl_concurrent_tasks": self._target.ddl_concurrent_tasks,
            "payload_path": remote_payload_path,
        }
        python_task = {
            "python_file": remote_app_path,
            "parameters": [f"--{k}={v}" for k, v in task_arguments.items() if v is not None],
        }
        self.json["spark_python_task"] = python_task
        super().execute(context)
        self._target.post_hook(context)


class SQLMeshDatabricksSQLOperator(BaseDbApiOperator):
    """Operator for running just SQL operations against Databricks.

    Args:
        target: The target that will be executed by this operator instance.
        databricks_conn_id: The Airflow connection id for the databricks target.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            target=target,
            conn_id=databricks_conn_id,
            dialect="databricks",
            hook_type=DatabricksSqlHook,
            **kwargs,
        )
