from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.core import constants as c
from sqlmesh.core.state_sync import StateReader
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.dag_generator import SnapshotDagGenerator
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.schedulers.airflow.plan import PlanDagState

if t.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SQLMeshAirflow:
    """The entry point for the SQLMesh integration with Airflow.

    The instance of this class should be created in a module that is part of the
    Airflow DAGs folder. Its primary purpose is to create DAG objects for the operational
    needs of the platform, as well as for model evaluation and backfills.

    Please note that the user must pass created DAGs into the
    Airflow scheduler. See the example below:

    Example:
        Create a new python module in the Airflow DAGs folder called "sqlmesh_integration.py"
        with the following content:

            from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

            for dag in SQLMeshAirflow("spark").dags:
                globals()[dag.dag_id] = dag

    Args:
        engine_operator: The type of the Airflow operator that will be used for model evaluation.
            If a string value is passed, an automatic operator discovery is attempted based
            on the engine name specified in the string.
        default_catalog: The default catalog to use when models are defined that do not contain a catalog in their name. This should match the default catalog applied by the connection.
        engine_operator_args: The dictionary of arguments that will be passed into the evaluate engine
            operator during its construction.
            This can be used to customize parameters such as connection ID.
        ddl_engine_operator: The type of the Airflow operator that will be used for environment management.
            These operations are SQL only.
            If a string value is passed, an automatic operator discovery is attempted based
            on the engine name specified in the string.
        ddl_engine_operator_args: Args to be passed into just the environment management operator.
            This can be used to customize parameters such as connection ID.
            If not specified, and the operator is the same as `engine_operator`, falls back to using `engine_operator_args`.
        janitor_interval: Defines how often the janitor DAG runs.
            The janitor DAG removes platform-managed DAG instances that are pending
            deletion from Airflow. Default: 1 hour.
        plan_application_dag_ttl: Determines the time-to-live period for finished plan application DAGs.
            Once this period is exceeded, finished plan application DAGs are deleted by the janitor. Default: 2 days.
        external_table_sensor_factory: A factory function that creates a sensor operator for a given signal payload.
        generate_cadence_dags: Whether to generate cadence DAGs for model versions that are currently deployed to production.
    """

    def __init__(
        self,
        engine_operator: t.Union[str, t.Type[BaseOperator]],
        default_catalog: str,
        engine_operator_args: t.Optional[t.Dict[str, t.Any]] = None,
        ddl_engine_operator: t.Optional[t.Union[str, t.Type[BaseOperator]]] = None,
        ddl_engine_operator_args: t.Optional[t.Dict[str, t.Any]] = None,
        janitor_interval: timedelta = timedelta(hours=1),
        plan_application_dag_ttl: timedelta = timedelta(days=2),
        external_table_sensor_factory: t.Optional[
            t.Callable[[t.Dict[str, t.Any]], BaseSensorOperator]
        ] = None,
        generate_cadence_dags: bool = True,
    ):
        if isinstance(engine_operator, str):
            if not ddl_engine_operator:
                ddl_engine_operator = util.discover_engine_operator(engine_operator, sql_only=True)
            engine_operator = util.discover_engine_operator(engine_operator, sql_only=False)

        if isinstance(ddl_engine_operator, str):
            ddl_engine_operator = util.discover_engine_operator(ddl_engine_operator, sql_only=True)

        engine_operator_args = engine_operator_args or {}
        ddl_engine_operator_args = ddl_engine_operator_args or {}

        self._engine_operator = engine_operator
        self._engine_operator_args = engine_operator_args
        self._ddl_engine_operator = ddl_engine_operator or engine_operator
        if self._engine_operator == self._ddl_engine_operator:
            self._ddl_engine_operator_args = {**engine_operator_args, **ddl_engine_operator_args}
        else:
            self._ddl_engine_operator_args = ddl_engine_operator_args or {}
        self._janitor_interval = janitor_interval
        self._plan_application_dag_ttl = plan_application_dag_ttl
        self._external_table_sensor_factory = external_table_sensor_factory
        self._generate_cadence_dags = generate_cadence_dags
        self._default_catalog = default_catalog

    @classmethod
    def set_default_catalog(cls, default_catalog: str) -> None:
        current_value = Variable.get(common.DEFAULT_CATALOG_VARIABLE_NAME, default_var=None)
        if not current_value:
            Variable.set(common.DEFAULT_CATALOG_VARIABLE_NAME, default_catalog)
        if current_value != default_catalog:
            Variable.update(common.DEFAULT_CATALOG_VARIABLE_NAME, default_catalog)

    @property
    def dags(self) -> t.List[DAG]:
        """Returns all DAG instances that must be registered with the Airflow scheduler
        for the integration to work.

        Returns:
            The list of DAG instances managed by the platform.
        """
        self.set_default_catalog(self._default_catalog)
        with util.scoped_state_sync() as state_sync:
            dag_generator = self._create_dag_generator(state_sync)

            if self._generate_cadence_dags:
                prod_env = state_sync.get_environment(c.PROD)
                cadence_dags = (
                    dag_generator.generate_cadence_dags(prod_env.snapshots) if prod_env else []
                )
                _delete_orphaned_snapshot_dags({d.dag_id for d in cadence_dags})
            else:
                cadence_dags = []

            plan_dag_specs = PlanDagState.from_state_sync(state_sync).get_dag_specs()
            plan_application_dags = [
                dag_generator.generate_plan_application_dag(s) for s in plan_dag_specs
            ]

            system_dags = [
                self._create_janitor_dag(),
            ]

            return system_dags + cadence_dags + plan_application_dags

    def _create_janitor_dag(self) -> DAG:
        dag = self._create_system_dag(common.JANITOR_DAG_ID, self._janitor_interval)
        janitor_task_op = PythonOperator(
            task_id=common.JANITOR_TASK_ID,
            python_callable=_janitor_task,
            op_kwargs={"plan_application_dag_ttl": self._plan_application_dag_ttl},
            dag=dag,
        )

        table_cleanup_task_op = self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotCleanupTarget(),
            task_id="snapshot_table_cleanup_task",
            dag=dag,
        )

        janitor_task_op >> table_cleanup_task_op

        return dag

    def _create_system_dag(self, dag_id: str, schedule_interval: t.Optional[timedelta]) -> DAG:
        return DAG(
            dag_id=dag_id,
            default_args=dict(
                execution_timeout=timedelta(minutes=10),
                retries=0,
            ),
            schedule_interval=schedule_interval,
            start_date=datetime(2023, 1, 1),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=False,
            tags=[common.SQLMESH_AIRFLOW_TAG],
        )

    def _create_dag_generator(self, state_reader: StateReader) -> SnapshotDagGenerator:
        return SnapshotDagGenerator(
            self._engine_operator,
            self._engine_operator_args,
            self._ddl_engine_operator,
            self._ddl_engine_operator_args,
            self._external_table_sensor_factory,
            state_reader,
        )


@provide_session
def _janitor_task(
    plan_application_dag_ttl: timedelta,
    ti: TaskInstance,
    session: Session = util.PROVIDED_SESSION,
) -> None:
    with util.scoped_state_sync() as state_sync:
        expired_environments = state_sync.delete_expired_environments()
        expired_snapshots = state_sync.delete_expired_snapshots()
        ti.xcom_push(
            key=common.SNAPSHOT_CLEANUP_COMMAND_XCOM_KEY,
            value=commands.CleanupCommandPayload(
                environments=expired_environments,
                tasks=expired_snapshots,
            ).json(),
            session=session,
        )

        prod_env = state_sync.get_environment(c.PROD)
        if prod_env:
            active_snapshot_dag_ids = {
                common.dag_id_for_snapshot_info(s) for s in prod_env.snapshots
            }
            _delete_orphaned_snapshot_dags(active_snapshot_dag_ids, session=session)

        plan_application_dag_ids = util.get_finished_plan_application_dag_ids(
            ttl=plan_application_dag_ttl, session=session
        )
        logger.info("Deleting expired Plan Application DAGs: %s", plan_application_dag_ids)
        PlanDagState.from_state_sync(state_sync).delete_dag_specs(plan_application_dag_ids)
        util.delete_dags(plan_application_dag_ids, session=session)

        state_sync.compact_intervals()


@provide_session
def _delete_orphaned_snapshot_dags(
    active_snapshot_dag_ids: t.Set[str], session: Session = util.PROVIDED_SESSION
) -> None:
    all_snapshot_dag_ids = set(util.get_snapshot_dag_ids(session=session))
    orphaned_snapshot_dag_ids = all_snapshot_dag_ids - active_snapshot_dag_ids
    logger.info("Deleting orphaned Snapshot DAGs: %s", orphaned_snapshot_dag_ids)
    util.delete_dags(orphaned_snapshot_dag_ids, session=session)
