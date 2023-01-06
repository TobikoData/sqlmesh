from __future__ import annotations

import json
import logging
import typing as t
from collections import defaultdict
from datetime import timedelta

from airflow import DAG
from airflow.models import BaseOperator, DagRun, TaskInstance, XCom
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.core import scheduler
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotTableInfo
from sqlmesh.core.state_sync import StateSync
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.dag_generator import SnapshotDagGenerator
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.schedulers.airflow.state_sync.xcom import XComStateSync
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError

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
            on the engine name specified in the string. Supported string values are: spark.
        engine_operator_args: The dictionary of arguments that will be passed into the engine
            operator during its construction. This can be used to customize parameters such as
            connection ID.
        ddl_engine_operator_args: Same as `engine_operator_args`, but only used for the
            snapshot promotion process. If not specified, falls back to using `engine_operator_args`.
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
        self._janitor_interval = janitor_interval
        self._plan_application_dag_ttl = plan_application_dag_ttl

    @property
    def dags(self) -> t.List[DAG]:
        """Returns all DAG instances that must be registered with the Airflow scheduler
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

        PythonOperator(
            task_id=common.PLAN_RECEIVER_TASK_ID,
            python_callable=_plan_receiver_task,
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

    if plan_conf.environment.end_at:
        end = plan_conf.environment.end_at
        unpaused_dt = None
    else:
        # Unbounded end date means we need to unpause all paused snapshots
        # that are part of the target environment.
        end = now()
        unpaused_dt = end

    snapshots_for_intervals = {**new_snapshots, **stored_snapshots}
    all_snapshots_by_version = defaultdict(set)
    for snapshot in snapshots_for_intervals.values():
        all_snapshots_by_version[(snapshot.name, snapshot.version)].add(
            snapshot.snapshot_id
        )

    if plan_conf.is_dev:
        # When in development mode exclude snapshots that match the version of each
        # paused forward-only snapshot that is a part of the plan.
        for s in plan_conf.environment.snapshots:
            if s.is_forward_only and snapshots_for_intervals[s.snapshot_id].is_paused:
                previous_snapshot_ids = all_snapshots_by_version[
                    (s.name, s.version)
                ] - {s.snapshot_id}
                for sid in previous_snapshot_ids:
                    snapshots_for_intervals.pop(sid)

    if plan_conf.restatements:
        state_sync.remove_interval(
            [],
            start=plan_conf.environment.start_at,
            end=end,
            all_snapshots=(
                snapshot
                for snapshot in snapshots_for_intervals.values()
                if snapshot.name in plan_conf.restatements
                and snapshot.snapshot_id not in new_snapshots
            ),
        )

    if not plan_conf.skip_backfill:
        backfill_batches = scheduler.compute_interval_params(
            plan_conf.environment.snapshots,
            snapshots=snapshots_for_intervals,
            start=plan_conf.environment.start_at,
            end=end,
            latest=end,
        )
    else:
        backfill_batches = []

    backfill_intervals_per_snapshot = [
        common.BackfillIntervalsPerSnapshot(
            snapshot_id=snapshot.snapshot_id,
            intervals=intervals,
        )
        for (snapshot, intervals) in backfill_batches
    ]

    request = common.PlanApplicationRequest(
        request_id=plan_conf.request_id,
        environment_name=plan_conf.environment.name,
        new_snapshots=plan_conf.new_snapshots,
        backfill_intervals_per_snapshot=backfill_intervals_per_snapshot,
        promoted_snapshots=plan_conf.environment.snapshots,
        demoted_snapshots=_get_demoted_snapshots(plan_conf.environment, state_sync),
        start=plan_conf.environment.start_at,
        end=plan_conf.environment.end_at,
        unpaused_dt=unpaused_dt,
        no_gaps=plan_conf.no_gaps,
        plan_id=plan_conf.environment.plan_id,
        previous_plan_id=plan_conf.environment.previous_plan_id,
        notification_targets=plan_conf.notification_targets,
        backfill_concurrent_tasks=plan_conf.backfill_concurrent_tasks,
        ddl_concurrent_tasks=plan_conf.ddl_concurrent_tasks,
        users=plan_conf.users,
        is_dev=plan_conf.is_dev,
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
