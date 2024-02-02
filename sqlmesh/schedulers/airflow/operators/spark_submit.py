import os
import tempfile
import typing as t

from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.utils.context import Context

import sqlmesh
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow.operators.targets import (
    BaseTarget,
    SnapshotEvaluationTarget,
)


class SQLMeshSparkSubmitOperator(BaseOperator):
    """The operator which evaluates a SQLMesh model snapshot using a dedicated Spark job instance.

    It requires the "spark-submit" binary to be available in the PATH or the spark_home
    attribute to be set in the connection extras.

    Args:
        target: The target that will be executed by this operator instance.
        application_name: The name of the submitted application (default sqlmesh-spark).
        spark_conf: Spark configuration properties.
        connection_id: The Airflow connection ID as described in
            https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html
            (default spark_default).
        total_executor_cores: (Srandalone & Mesos only) The total number of cores for all executors.
        executor_cores: (Standalone, YARN and Kubernetes only) The number of cores per executor.
        executor_memory: The amount of memory allocated to each executor (e.g. 1024M, 2G).
        driver_memory: The amount of memory allocated to the driver (e.g. 1024M, 2G).
        keytab: The full path to the file that contains the keytab.
        principal: The name of the Kerberos principal used for the keytab.
        proxy_user: The name of a user which should be impersonated when submitting the application.
        num_executors: The number of executors that will be allocateed to the application.
    """

    def __init__(
        self,
        *,
        target: BaseTarget,
        application_name: str = "sqlmesh-spark",
        spark_conf: t.Optional[t.Dict[str, t.Any]] = None,
        connection_id: str = "spark_default",
        total_executor_cores: t.Optional[int] = None,
        executor_cores: t.Optional[int] = None,
        executor_memory: t.Optional[str] = None,
        driver_memory: t.Optional[str] = None,
        keytab: t.Optional[str] = None,
        principal: t.Optional[str] = None,
        proxy_user: t.Optional[str] = None,
        num_executors: t.Optional[int] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._target = target
        self._application_name = application_name
        self._spark_conf = spark_conf or {}
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._num_executors = num_executors
        self._connection_id = connection_id
        self._application = os.path.join(
            os.path.dirname(os.path.abspath(sqlmesh.__file__)), "engines/spark/app.py"
        )
        self._hook: t.Optional[SparkSubmitHook] = None

    def execute(self, context: Context) -> None:
        command_payload = self._target.serialized_command_payload(context)
        with tempfile.TemporaryDirectory() as tmp:
            payload_file_path = os.path.join(tmp, commands.COMMAND_PAYLOAD_FILE_NAME)
            with open(payload_file_path, "w") as payload_fd:
                payload_fd.write(command_payload)

            if self._hook is None:
                if (
                    isinstance(self._target, SnapshotEvaluationTarget)
                    and self._target.snapshot.is_model
                ):
                    session_properties = self._target.snapshot.model.session_properties
                    executor_cores: t.Optional[int] = session_properties.pop(  # type: ignore
                        "spark.executor.cores", self._executor_cores
                    )
                    executor_memory: t.Optional[str] = session_properties.pop(  # type: ignore
                        "spark.executor.memory", self._executor_memory
                    )
                    driver_memory: t.Optional[str] = session_properties.pop(  # type: ignore
                        "spark.driver.memory", self._driver_memory
                    )
                    num_executors: t.Optional[int] = session_properties.pop(  # type: ignore
                        "spark.executor.instances", self._num_executors
                    )
                    spark_conf: t.Dict[str, t.Any] = {**self._spark_conf, **session_properties}
                else:
                    executor_cores = self._executor_cores
                    executor_memory = self._executor_memory
                    driver_memory = self._driver_memory
                    num_executors = self._num_executors
                    spark_conf = self._spark_conf

                self._hook = self._get_hook(
                    self._target.command_type,
                    payload_file_path,
                    self._target.ddl_concurrent_tasks,
                    spark_conf,
                    executor_cores,
                    executor_memory,
                    driver_memory,
                    num_executors,
                )
            self._hook.submit(self._application)
        self._target.post_hook(context)

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook(None, None, None, None, None, None, None, None)
        self._hook.on_kill()

    def _get_hook(
        self,
        command_type: t.Optional[commands.CommandType],
        command_payload_file_path: t.Optional[str],
        ddl_concurrent_tasks: t.Optional[int],
        spark_conf: t.Optional[t.Dict[str, t.Any]],
        executor_cores: t.Optional[int],
        executor_memory: t.Optional[str],
        driver_memory: t.Optional[str],
        num_executors: t.Optional[int],
    ) -> SparkSubmitHook:
        application_args = {
            "dialect": "spark",
            "default_catalog": self._target.default_catalog,
            "command_type": command_type.value if command_type else None,
            "ddl_concurrent_tasks": ddl_concurrent_tasks,
            "payload_path": (
                command_payload_file_path.split("/")[-1] if command_payload_file_path else None
            ),
        }
        return SparkSubmitHook(
            conf=spark_conf,
            conn_id=self._connection_id,
            total_executor_cores=self._total_executor_cores,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            driver_memory=driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._application_name,
            num_executors=num_executors,
            application_args=[f"--{k}={v}" for k, v in application_args.items() if v is not None],
            files=command_payload_file_path,
        )
