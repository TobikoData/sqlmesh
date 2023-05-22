from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.dag_generator import SnapshotDagGenerator
from sqlmesh.schedulers.airflow.operators import targets

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
    """

    def __init__(
        self,
        engine_operator: t.Union[str, t.Type[BaseOperator]],
        engine_operator_args: t.Optional[t.Dict[str, t.Any]] = None,
        ddl_engine_operator: t.Optional[t.Union[str, t.Type[BaseOperator]]] = None,
        ddl_engine_operator_args: t.Optional[t.Dict[str, t.Any]] = None,
        janitor_interval: timedelta = timedelta(hours=1),
        plan_application_dag_ttl: timedelta = timedelta(days=2),
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

    @property
    def dags(self) -> t.List[DAG]:
        """Returns all DAG instances that must be registered with the Airflow scheduler
        for the integration to work.

        Returns:
            The list of DAG instances managed by the platform.
        """
        with util.scoped_state_sync() as state_sync:
            stored_snapshots = state_sync.get_snapshots(None)

        dag_generator = SnapshotDagGenerator(
            self._engine_operator,
            self._engine_operator_args,
            self._ddl_engine_operator,
            self._ddl_engine_operator_args,
            stored_snapshots,
        )

        cadence_dags = dag_generator.generate_cadence_dags()

        plan_application_dags = [
            dag_generator.generate_plan_application_dag(s) for s in _get_plan_dag_specs()
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
                snapshots=[s.table_info for s in expired_snapshots],
            ).json(),
            session=session,
        )

        all_snapshot_dag_ids = set(util.get_snapshot_dag_ids())
        active_snapshot_dag_ids = {
            common.dag_id_for_snapshot_info(s) for s in state_sync.get_snapshots(None).values()
        }
        expired_snapshot_dag_ids = all_snapshot_dag_ids - active_snapshot_dag_ids
        logger.info("Deleting expired Snapshot DAGs: %s", expired_snapshot_dag_ids)
        util.delete_dags(expired_snapshot_dag_ids, session=session)

        plan_application_dag_ids = util.get_finished_plan_application_dag_ids(
            ttl=plan_application_dag_ttl, session=session
        )
        logger.info("Deleting expired Plan Application DAGs: %s", plan_application_dag_ids)
        util.delete_variables(
            {common.plan_dag_spec_key_from_dag_id(dag_id) for dag_id in plan_application_dag_ids},
            session=session,
        )
        util.delete_dags(plan_application_dag_ids, session=session)

        state_sync.compact_intervals()


@provide_session
def _get_plan_dag_specs(
    session: Session = util.PROVIDED_SESSION,
) -> t.List[common.PlanDagSpec]:
    records = (
        session.query(Variable)
        .filter(Variable.key.like(f"{common.PLAN_DAG_SPEC_KEY_PREFIX}%"))
        .all()
    )
    return [common.PlanDagSpec.parse_raw(r.val) for r in records]
