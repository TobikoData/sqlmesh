from __future__ import annotations

import json
import logging
import typing as t
from datetime import timedelta

from airflow import DAG
from airflow.models import BaseOperator, DagRun, TaskInstance, XCom
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.core import scheduler
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import ModelKind
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotTableInfo
from sqlmesh.core.state_sync import StateSync
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.dag_generator import SnapshotDagGenerator
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.schedulers.airflow.state_sync.xcom import XComStateSync
from sqlmesh.utils.date import TimeLike, now
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


class SQLMeshAirflow:
    """The entrypoint for the SQLMesh integration with Airflow.

    The instance of this class should be created in a module that is part of the
    Airflow DAGs folder. Its primary purpose is to create DAG objects for operational
    needs of the platform as well as for model evaluation and backfills.

    Please note that it's a responsibility of a user to pass created DAGs into the
    Airflow scheduler (see the example below).

    Example:
        Create a new python module in the Airflow DAGs folder called "sqlmesh_integration.py"
        with the following content::

            from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

            for dag in SQLMeshAirflow("spark").dags:
                globals()[dag.dag_id] = dag

    Args:
        engine_operarator: The type of the Airflow operator which will be used for model evaluation.
            If a string value is passed, an automatic operator discovery will be attempted based
            on the engine name specified in the string. Supported string values are: spark.
        engine_operator_args: The dictionary of arguments which will be passed into the engine
            operator during its construction. This can be used to customize parameters like
            connection ID.
        ddl_engine_operator_args: Same as `engine_operator_args` but only used for the
            snapshot promotion process. If not specified falls back to using `engine_operator_args`.
        ddl_concurrent_task: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
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
        ddl_engine_operator_args: t.Optional[t.Dict[str, t.Any]] = None,
        ddl_concurrent_tasks: int = 1,
        janitor_interval: timedelta = timedelta(hours=1),
        plan_application_dag_ttl: timedelta = timedelta(days=2),
    ):
        if isinstance(engine_operator, str):
            engine_operator = util.discover_engine_operator(engine_operator)

        self._engine_operator = engine_operator
        self._engine_operator_args = engine_operator_args
        self._ddl_engine_operator_args = (
            ddl_engine_operator_args or engine_operator_args or {}
        )
        self._ddl_concurrent_tasks = ddl_concurrent_tasks
        self._janitor_interval = janitor_interval
        self._plan_application_dag_ttl = plan_application_dag_ttl

    @property
    def dags(self) -> t.List[DAG]:
        """Returns all DAG instances that need to be registered with the Airflow scheduler
        for the integration to work.

        Returns:
            The list of DAG instances managed by the platform.
        """
        stored_snapshots = _get_all_snapshots()

        dag_generator = SnapshotDagGenerator(
            self._engine_operator,
            self._engine_operator_args,
            self._ddl_engine_operator_args,
            stored_snapshots,
        )

        incremental_dags = dag_generator.generate_incremental()

        plan_application_dags = [
            dag_generator.generate_apply(r) for r in _get_plan_application_requests()
        ]

        system_dags = [
            self._create_plan_receiver_dag(),
            self._create_janitor_dag(),
        ]

        return system_dags + incremental_dags + plan_application_dags

    def _create_plan_receiver_dag(self) -> DAG:
        dag = self._create_system_dag(common.PLAN_RECEIVER_DAG_ID, None)

        receiver_task = PythonOperator(
            task_id=common.PLAN_RECEIVER_TASK_ID,
            python_callable=_plan_receiver_task,
            op_kwargs={"ddl_concurrent_tasks": self._ddl_concurrent_tasks},
            dag=dag,
        )

        return dag

    def _create_janitor_dag(self) -> DAG:
        dag = self._create_system_dag(common.JANITOR_DAG_ID, self._janitor_interval)
        janitor_task_op = PythonOperator(
            task_id=common.JANITOR_TASK_ID,
            python_callable=_janitor_task,
            op_kwargs={"plan_application_dag_ttl": self._plan_application_dag_ttl},
            dag=dag,
        )

        table_cleanup_task_op = self._engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotTableCleanupTarget(),
            task_id="snapshot_table_cleanup_task",
            dag=dag,
        )

        janitor_task_op >> table_cleanup_task_op

        return dag

    def _create_system_dag(
        self, dag_id: str, schedule_interval: t.Optional[timedelta]
    ) -> DAG:
        return DAG(
            dag_id=dag_id,
            default_args=dict(
                execution_timeout=timedelta(minutes=10),
                retries=0,
            ),
            schedule_interval=schedule_interval,
            start_date=now(),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=False,
            tags=[common.SQLMESH_AIRFLOW_TAG],
        )


@provide_session
def _plan_receiver_task(
    dag_run: DagRun,
    ti: TaskInstance,
    ddl_concurrent_tasks: int,
    session: Session = util.PROVIDED_SESSION,
) -> None:
    state_sync = XComStateSync(session)
    plan_conf = common.PlanReceiverDagConf.parse_obj(dag_run.conf)

    new_snapshots = {s.snapshot_id: s for s in plan_conf.new_snapshots}
    stored_snapshots = state_sync.get_all_snapshots()

    duplicated_snapshots = set(stored_snapshots).intersection(new_snapshots)
    if duplicated_snapshots:
        raise SQLMeshError(
            f"Snapshots {duplicated_snapshots} already exist. "
            "Make sure your code base is up to date and try re-creating the plan"
        )

    if plan_conf.environment.end:
        end = plan_conf.environment.end
    else:
        # Unbounded end date means we need to unpause all paused snapshots
        # that are part of the target environment.
        end = now()
        stored_environment_snapshots = [
            stored_snapshots[s.snapshot_id]
            for s in plan_conf.environment.snapshots
            if s.snapshot_id in stored_snapshots
        ]
        _unpause_snapshots(
            state_sync, new_snapshots.values(), stored_environment_snapshots, end
        )

    all_snapshots = {**new_snapshots, **stored_snapshots}

    if plan_conf.restatements:
        state_sync.remove_interval(
            [],
            start=plan_conf.environment.start,
            end=end,
            all_snapshots=(
                snapshot
                for snapshot in all_snapshots.values()
                if snapshot.name in plan_conf.restatements
            ),
        )

    backfill_batches = scheduler.compute_interval_params(
        plan_conf.environment.snapshots,
        snapshots=all_snapshots,
        start=plan_conf.environment.start,
        end=end,
        latest=end,
    )
    backfill_intervals_per_snapshot = [
        common.BackfillIntervalsPerSnapshot(
            snapshot_id=snapshot.snapshot_id,
            intervals=intervals,
        )
        for (snapshot, intervals) in backfill_batches
    ]

    new_snapshot_batches = util.create_topological_snapshot_batches(
        plan_conf.new_snapshots,
        ddl_concurrent_tasks,
        lambda s: s.model.kind == ModelKind.VIEW,
    )

    promotion_batches = util.create_batches(
        plan_conf.environment.snapshots, ddl_concurrent_tasks
    )

    demotion_batches = util.create_batches(
        _get_demoted_snapshots(plan_conf.environment, state_sync), ddl_concurrent_tasks
    )

    request = common.PlanApplicationRequest(
        request_id=plan_conf.request_id,
        environment_name=plan_conf.environment.name,
        new_snapshot_batches=new_snapshot_batches,
        backfill_intervals_per_snapshot=backfill_intervals_per_snapshot,
        promotion_batches=promotion_batches,
        demotion_batches=demotion_batches,
        start=plan_conf.environment.start,
        end=plan_conf.environment.end,
        no_gaps=plan_conf.no_gaps,
        plan_id=plan_conf.environment.plan_id,
        previous_plan_id=plan_conf.environment.previous_plan_id,
        notification_targets=plan_conf.notification_targets,
    )

    ti.xcom_push(
        common.plan_application_request_xcom_key(plan_conf.request_id), request.json()
    )


@provide_session
def _janitor_task(
    plan_application_dag_ttl: timedelta,
    ti: TaskInstance,
    session: Session = util.PROVIDED_SESSION,
) -> None:
    state_sync = XComStateSync(session)

    expired_snapshots = state_sync.remove_expired_snapshots()
    ti.xcom_push(
        key=common.SNAPSHOT_TABLE_CLEANUP_XCOM_KEY,
        value=json.dumps([s.table_info.dict() for s in expired_snapshots]),
        session=session,
    )

    all_snapshot_dag_ids = set(util.get_snapshot_dag_ids())
    active_snapshot_dag_ids = {
        common.dag_id_for_name_version(s["name"], s["version"])
        for s in state_sync.get_all_snapshots_raw()
    }
    expired_snapshot_dag_ids = all_snapshot_dag_ids - active_snapshot_dag_ids
    logger.info("Deleting expired Snapshot DAGs: %s", expired_snapshot_dag_ids)
    util.delete_dags(expired_snapshot_dag_ids, session=session)

    plan_application_dag_ids = util.get_finished_plan_application_dag_ids(
        ttl=plan_application_dag_ttl, session=session
    )
    logger.info("Deleting expired Plan Application DAGs: %s", plan_application_dag_ids)
    util.delete_xcoms(
        common.PLAN_RECEIVER_DAG_ID,
        {
            common.plan_application_request_xcom_key_from_dag_id(dag_id)
            for dag_id in plan_application_dag_ids
        },
        session=session,
    )
    util.delete_dags(plan_application_dag_ids, session=session)


@provide_session
def _get_all_snapshots(
    session: Session = util.PROVIDED_SESSION,
) -> t.Dict[SnapshotId, Snapshot]:
    return XComStateSync(session).get_all_snapshots()


@provide_session
def _get_plan_application_requests(
    session: Session = util.PROVIDED_SESSION,
) -> t.List[common.PlanApplicationRequest]:
    xcoms = (
        session.query(XCom)
        .filter(XCom.dag_id == common.PLAN_RECEIVER_DAG_ID)
        .filter(XCom.key.like(f"{common.PLAN_APPLICATION_REQUEST_KEY_PREFIX}%"))
        .all()
    )
    return [common.PlanApplicationRequest.parse_raw(xcom.value) for xcom in xcoms]


def _get_demoted_snapshots(
    new_environment: Environment, state_sync: StateSync
) -> t.List[SnapshotTableInfo]:
    current_environment = state_sync.get_environment(new_environment.name)
    if current_environment:
        preserved_snapshot_names = {s.name for s in new_environment.snapshots}
        return [
            s
            for s in current_environment.snapshots
            if s.name not in preserved_snapshot_names
        ]
    return []


def _unpause_snapshots(
    state_sync: XComStateSync,
    new_snaspots: t.Iterable[Snapshot],
    stored_snapshots: t.Iterable[Snapshot],
    unpaused_dt: TimeLike,
) -> None:
    paused_stored_snapshots = []
    for s in stored_snapshots:
        if not s.unpaused_ts:
            logger.info(f"Unpausing stored snapshot %s", s.snapshot_id)
            paused_stored_snapshots.append(s)
    state_sync.unpause_snapshots(paused_stored_snapshots, unpaused_dt)

    for s in new_snaspots:
        logger.info(f"Unpausing new snapshot %s", s.snapshot_id)
        s.set_unpaused_ts(unpaused_dt)
