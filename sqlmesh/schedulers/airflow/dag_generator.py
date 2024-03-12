from __future__ import annotations

import logging
import os
import typing as t

import pendulum
from airflow import DAG
from airflow.models import BaseOperator, baseoperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.plan import PlanStatus
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotTableInfo,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.schedulers.airflow.operators.hwm_sensor import (
    HighWaterMarkExternalSensor,
    HighWaterMarkSensor,
)
from sqlmesh.schedulers.airflow.operators.notification import (
    BaseNotificationOperatorProvider,
)
from sqlmesh.utils import sanitize_name
from sqlmesh.utils.date import TimeLike, to_datetime, yesterday_timestamp
from sqlmesh.utils.errors import SQLMeshError

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

logger = logging.getLogger(__name__)


TASK_ID_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S"

NOTIFICATION_TARGET_TO_OPERATOR_PROVIDER: t.Dict[
    t.Type[NotificationTarget], BaseNotificationOperatorProvider
] = {}

DAG_DEFAULT_ARGS = {
    # `AIRFLOW__CORE__DEFAULT_TASK_RETRY_DELAY` support added in 2.4.0
    # We can't use `AIRFLOW__CORE__DEFAULT_TASK_RETRY_DELAY` because cloud composer doesn't allow you to set config
    # from an environment variable
    "retry_delay": int(
        os.getenv(
            "SQLMESH_AIRFLOW_DEFAULT_TASK_RETRY_DELAY",
            os.getenv("AIRFLOW__CORE__DEFAULT_TASK_RETRY_DELAY", "300"),
        )
    ),
}

AIRFLOW_TAG_CHARACTER_LIMIT = 100


class SnapshotDagGenerator:
    def __init__(
        self,
        engine_operator: t.Type[BaseOperator],
        engine_operator_args: t.Optional[t.Dict[str, t.Any]],
        ddl_engine_operator: t.Type[BaseOperator],
        ddl_engine_operator_args: t.Optional[t.Dict[str, t.Any]],
        external_table_sensor_factory: t.Optional[
            t.Callable[[t.Dict[str, t.Any]], BaseSensorOperator]
        ],
        state_reader: StateReader,
    ):
        self._engine_operator = engine_operator
        self._engine_operator_args = engine_operator_args or {}
        self._ddl_engine_operator = ddl_engine_operator
        self._ddl_engine_operator_args = ddl_engine_operator_args or {}
        self._external_table_sensor_factory = external_table_sensor_factory
        self._state_reader = state_reader

    def generate_cadence_dags(self, snapshots: t.Iterable[SnapshotIdLike]) -> t.List[DAG]:
        dags = []
        snapshots = self._state_reader.get_snapshots(snapshots)
        for snapshot in snapshots.values():
            if snapshot.unpaused_ts and not snapshot.is_symbolic and not snapshot.is_seed:
                dags.append(self._create_cadence_dag_for_snapshot(snapshot, snapshots))
        return dags

    def generate_plan_application_dag(self, spec: common.PlanDagSpec) -> DAG:
        return self._create_plan_application_dag(spec)

    def _create_cadence_dag_for_snapshot(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> DAG:
        dag_id = common.dag_id_for_snapshot_info(snapshot.table_info)
        logger.info(
            "Generating the cadence DAG '%s' for snapshot %s",
            dag_id,
            snapshot.snapshot_id,
        )

        if not snapshot.unpaused_ts:
            raise SQLMeshError(
                f"Can't create a cadence DAG for the paused snapshot {snapshot.snapshot_id}"
            )

        with DAG(
            dag_id=dag_id,
            schedule_interval=snapshot.node.cron,
            start_date=pendulum.instance(to_datetime(snapshot.unpaused_ts)),
            max_active_runs=1,
            catchup=True,
            is_paused_upon_creation=False,
            tags=[
                common.SQLMESH_AIRFLOW_TAG,
                common.SNAPSHOT_AIRFLOW_TAG,
                snapshot.node.name[-AIRFLOW_TAG_CHARACTER_LIMIT:],
            ],
            default_args={
                **DAG_DEFAULT_ARGS,
                "email": snapshot.node.owner,
                "email_on_failure": True,
            },
        ) as dag:
            hwm_sensor_tasks = self._create_hwm_sensors(snapshot, snapshots)

            evaluator_task = self._create_snapshot_evaluation_operator(
                snapshots=snapshots,
                snapshot=snapshot,
                task_id="snapshot_evaluator",
            )

            hwm_sensor_tasks >> evaluator_task

            return dag

    def _create_plan_application_dag(self, plan_dag_spec: common.PlanDagSpec) -> DAG:
        dag_id = common.plan_application_dag_id(
            plan_dag_spec.environment.name, plan_dag_spec.request_id
        )
        logger.info(
            "Generating the plan application DAG '%s' for environment '%s'",
            dag_id,
            plan_dag_spec.environment.name,
        )

        all_snapshots = {
            **{s.snapshot_id: s for s in plan_dag_spec.new_snapshots},
            **self._state_reader.get_snapshots(plan_dag_spec.environment.snapshots),
        }

        snapshots_to_create = [
            all_snapshots[snapshot.snapshot_id]
            for snapshot in plan_dag_spec.environment.snapshots
            if snapshot.snapshot_id in all_snapshots
            and (
                plan_dag_spec.models_to_backfill is None
                or snapshot.name in plan_dag_spec.models_to_backfill
            )
        ]

        with DAG(
            dag_id=dag_id,
            schedule_interval="@once",
            start_date=pendulum.instance(
                to_datetime(plan_dag_spec.dag_start_ts or yesterday_timestamp())
            ),
            max_active_tasks=plan_dag_spec.backfill_concurrent_tasks,
            catchup=False,
            is_paused_upon_creation=False,
            default_args=DAG_DEFAULT_ARGS,
            tags=[
                common.SQLMESH_AIRFLOW_TAG,
                common.PLAN_AIRFLOW_TAG,
                plan_dag_spec.environment.name,
            ],
        ) as dag:
            start_task = EmptyOperator(task_id="plan_application_start")
            end_task = EmptyOperator(task_id="plan_application_end")

            (create_start_task, create_end_task) = self._create_creation_tasks(
                snapshots_to_create,
                plan_dag_spec.new_snapshots,
                plan_dag_spec.ddl_concurrent_tasks,
                plan_dag_spec.deployability_index_for_creation,
            )

            (
                backfill_before_promote_start_task,
                backfill_before_promote_end_task,
            ) = self._create_backfill_tasks(
                [i for i in plan_dag_spec.backfill_intervals_per_snapshot if i.before_promote],
                all_snapshots,
                plan_dag_spec.deployability_index,
                plan_dag_spec.environment.plan_id,
                "before_promote",
            )

            (
                backfill_after_promote_start_task,
                backfill_after_promote_end_task,
            ) = self._create_backfill_tasks(
                [i for i in plan_dag_spec.backfill_intervals_per_snapshot if not i.before_promote],
                all_snapshots,
                plan_dag_spec.deployability_index,
                plan_dag_spec.environment.plan_id,
                "after_promote",
            )

            (
                promote_start_task,
                promote_end_task,
            ) = self._create_promotion_demotion_tasks(plan_dag_spec, all_snapshots)

            start_task >> create_start_task
            create_end_task >> backfill_before_promote_start_task
            backfill_before_promote_end_task >> promote_start_task

            update_views_task_pair = self._create_update_views_tasks(plan_dag_spec, all_snapshots)
            if update_views_task_pair:
                backfill_after_promote_end_task >> update_views_task_pair[0]
                before_finalize_task = update_views_task_pair[1]
            else:
                before_finalize_task = backfill_after_promote_end_task

            unpause_snapshots_task = self._create_unpause_snapshots_task(plan_dag_spec)
            if unpause_snapshots_task:
                if not plan_dag_spec.ensure_finalized_snapshots:
                    # Only unpause right after updatign the environment record if we don't
                    # have to use the finalized snapshots for subsequent plan applications.
                    promote_end_task >> unpause_snapshots_task
                    unpause_snapshots_task >> backfill_after_promote_start_task
                else:
                    # Otherwise, unpause right before finalizing the environment.
                    promote_end_task >> backfill_after_promote_start_task
                    before_finalize_task >> unpause_snapshots_task
                    before_finalize_task = unpause_snapshots_task
            else:
                promote_end_task >> backfill_after_promote_start_task

            finalize_task = self._create_finalize_task(plan_dag_spec.environment)
            before_finalize_task >> finalize_task

            self._add_notification_target_tasks(plan_dag_spec, start_task, end_task, finalize_task)
            return dag

    def _add_notification_target_tasks(
        self,
        request: common.PlanDagSpec,
        start_task: BaseOperator,
        end_task: BaseOperator,
        previous_end_task: BaseOperator,
    ) -> None:
        has_success_or_failed_notification = False
        for notification_target in request.notification_targets:
            notification_operator_provider = NOTIFICATION_TARGET_TO_OPERATOR_PROVIDER.get(
                type(notification_target)
            )
            if not notification_operator_provider:
                continue
            plan_start_notification_task = notification_operator_provider.operator(
                notification_target, PlanStatus.STARTED, request
            )
            plan_success_notification_task = notification_operator_provider.operator(
                notification_target, PlanStatus.FINISHED, request
            )
            plan_failed_notification_task = notification_operator_provider.operator(
                notification_target, PlanStatus.FAILED, request
            )
            if plan_start_notification_task:
                start_task >> plan_start_notification_task
            if plan_success_notification_task:
                has_success_or_failed_notification = True
                previous_end_task >> plan_success_notification_task
                plan_success_notification_task >> end_task
            if plan_failed_notification_task:
                has_success_or_failed_notification = True
                previous_end_task >> plan_failed_notification_task
                plan_failed_notification_task >> end_task
        if not has_success_or_failed_notification:
            previous_end_task >> end_task

    def _create_creation_tasks(
        self,
        snapshots_to_create: t.List[Snapshot],
        new_snapshots: t.List[Snapshot],
        ddl_concurrent_tasks: int,
        deployability_index: DeployabilityIndex,
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        start_task = EmptyOperator(task_id="snapshot_creation_start")
        end_task = EmptyOperator(task_id="snapshot_creation_end", trigger_rule="none_failed")

        current_task: BaseOperator = start_task

        if snapshots_to_create:
            creation_task = self._create_snapshot_create_tables_operator(
                snapshots_to_create,
                ddl_concurrent_tasks,
                deployability_index,
                "snapshot_creation__create_tables",
            )
            current_task >> creation_task
            current_task = creation_task

        if new_snapshots:
            update_state_task = PythonOperator(
                task_id="snapshot_creation__update_state",
                python_callable=creation_update_state_task,
                op_kwargs={"new_snapshots": new_snapshots},
            )
            current_task >> update_state_task
            current_task = update_state_task

        current_task >> end_task

        return (start_task, end_task)

    def _create_promotion_demotion_tasks(
        self,
        request: common.PlanDagSpec,
        snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        update_state_task = PythonOperator(
            task_id="snapshot_promotion_update_state",
            python_callable=promotion_update_state_task,
            op_kwargs={
                "environment": request.environment,
                "no_gaps_snapshot_names": (
                    request.no_gaps_snapshot_names if request.no_gaps else set()
                ),
            },
        )

        start_task = update_state_task
        end_task: BaseOperator = update_state_task

        if request.environment.promoted_snapshots and not request.is_dev and request.unpaused_dt:
            migrate_tables_task = self._create_snapshot_migrate_tables_operator(
                [
                    snapshots[s.snapshot_id]
                    for s in request.environment.promoted_snapshots
                    if snapshots[s.snapshot_id].is_paused
                ],
                request.ddl_concurrent_tasks,
                "snapshot_promotion_migrate_tables",
            )
            update_state_task >> migrate_tables_task
            end_task = migrate_tables_task

        return (start_task, end_task)

    def _create_unpause_snapshots_task(
        self, request: common.PlanDagSpec
    ) -> t.Optional[BaseOperator]:
        if request.is_dev or not request.unpaused_dt:
            return None
        return PythonOperator(
            task_id="snapshot_promotion_unpause_snapshots",
            python_callable=promotion_unpause_snapshots_task,
            op_kwargs={
                "environment": request.environment,
                "unpaused_dt": request.unpaused_dt,
            },
            trigger_rule="none_failed",
        )

    def _create_update_views_tasks(
        self, request: common.PlanDagSpec, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> t.Optional[t.Tuple[BaseOperator, BaseOperator]]:
        create_views_task = None
        delete_views_task = None

        environment_naming_info = request.environment.naming_info

        if request.environment.promoted_snapshots:
            create_views_task = self._create_snapshot_promotion_operator(
                [snapshots[x.snapshot_id] for x in request.environment.promoted_snapshots],
                environment_naming_info,
                request.ddl_concurrent_tasks,
                request.deployability_index,
                "snapshot_promotion_create_views",
            )

        if request.demoted_snapshots:
            delete_views_task = self._create_snapshot_demotion_operator(
                request.demoted_snapshots,
                environment_naming_info,
                request.ddl_concurrent_tasks,
                "snapshot_promotion_delete_views",
            )

        if create_views_task and delete_views_task:
            create_views_task >> delete_views_task
            return create_views_task, delete_views_task
        if create_views_task:
            return create_views_task, create_views_task
        if delete_views_task:
            return delete_views_task, delete_views_task
        return None

    def _create_finalize_task(self, environment: Environment) -> BaseOperator:
        return PythonOperator(
            task_id="snapshot_promotion_finalize",
            python_callable=promotion_finalize_task,
            op_kwargs={"environment": environment},
        )

    def _create_backfill_tasks(
        self,
        backfill_intervals: t.List[common.BackfillIntervalsPerSnapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: DeployabilityIndex,
        plan_id: str,
        task_id_suffix: str,
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        snapshot_to_tasks = {}
        for intervals_per_snapshot in backfill_intervals:
            sid = intervals_per_snapshot.snapshot_id

            if not intervals_per_snapshot.intervals:
                logger.info("Skipping backfill for snapshot %s", sid)
                continue

            snapshot = snapshots[sid]
            sanitized_model_name = sanitize_name(snapshot.node.name)

            snapshot_intervals_chain: t.List[t.Union[BaseOperator, t.List[BaseOperator]]] = []

            snapshot_start_task = EmptyOperator(
                task_id=f"snapshot_backfill__{sanitized_model_name}__{snapshot.identifier}__start"
            )
            snapshot_end_task = EmptyOperator(
                task_id=f"snapshot_backfill__{sanitized_model_name}__{snapshot.identifier}__end"
            )

            task_id_prefix = f"snapshot_backfill__{sanitized_model_name}__{snapshot.identifier}"
            for start, end in intervals_per_snapshot.intervals:
                evaluation_task = self._create_snapshot_evaluation_operator(
                    snapshots=snapshots,
                    snapshot=snapshot,
                    task_id=f"{task_id_prefix}__{start.strftime(TASK_ID_DATE_FORMAT)}__{end.strftime(TASK_ID_DATE_FORMAT)}",
                    start=start,
                    end=end,
                    deployability_index=deployability_index,
                    plan_id=plan_id,
                )

                external_sensor_task = self._create_hwm_external_sensor(
                    snapshot, start=start, end=end
                )
                if external_sensor_task:
                    if snapshot.depends_on_past:
                        snapshot_intervals_chain.extend([external_sensor_task, evaluation_task])
                    else:
                        (
                            snapshot_start_task
                            >> external_sensor_task
                            >> evaluation_task
                            >> snapshot_end_task
                        )
                else:
                    if snapshot.depends_on_past:
                        snapshot_intervals_chain.append(evaluation_task)
                    else:
                        snapshot_start_task >> evaluation_task >> snapshot_end_task

            if snapshot.depends_on_past:
                baseoperator.chain(
                    snapshot_start_task, *snapshot_intervals_chain, snapshot_end_task
                )
            elif not intervals_per_snapshot.intervals:
                snapshot_start_task >> snapshot_end_task

            snapshot_to_tasks[snapshot.snapshot_id] = (
                snapshot_start_task,
                snapshot_end_task,
            )

        backfill_start_task = EmptyOperator(task_id=f"snapshot_backfill_{task_id_suffix}_start")
        backfill_end_task = EmptyOperator(task_id=f"snapshot_backfill_{task_id_suffix}_end")

        if not snapshot_to_tasks:
            backfill_start_task >> backfill_end_task
            return (backfill_start_task, backfill_end_task)

        entry_tasks = []
        parent_ids_to_backfill = set()
        for sid, (start_task, _) in snapshot_to_tasks.items():
            has_parents_to_backfill = False
            for p_sid in snapshots[sid].parents:
                if p_sid in snapshot_to_tasks:
                    snapshot_to_tasks[p_sid][1] >> start_task
                    parent_ids_to_backfill.add(p_sid)
                    has_parents_to_backfill = True

            if not has_parents_to_backfill:
                entry_tasks.append(start_task)

        backfill_start_task >> entry_tasks

        exit_tasks = [
            end_task
            for sid, (_, end_task) in snapshot_to_tasks.items()
            if sid not in parent_ids_to_backfill
        ]
        for task in exit_tasks:
            task >> backfill_end_task

        return (backfill_start_task, backfill_end_task)

    def _create_snapshot_promotion_operator(
        self,
        snapshots: t.List[Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        ddl_concurrent_tasks: int,
        deployability_index: DeployabilityIndex,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotPromotionTarget(
                snapshots=snapshots,
                environment_naming_info=environment_naming_info,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
                deployability_index=deployability_index,
            ),
            task_id=task_id,
        )

    def _create_snapshot_demotion_operator(
        self,
        snapshots: t.List[SnapshotTableInfo],
        environment_naming_info: EnvironmentNamingInfo,
        ddl_concurrent_tasks: int,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotDemotionTarget(
                snapshots=snapshots,
                environment_naming_info=environment_naming_info,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
            ),
            task_id=task_id,
        )

    def _create_snapshot_create_tables_operator(
        self,
        new_snapshots: t.List[Snapshot],
        ddl_concurrent_tasks: int,
        deployability_index: DeployabilityIndex,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotCreateTablesTarget(
                new_snapshots=new_snapshots,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
                deployability_index=deployability_index,
            ),
            task_id=task_id,
        )

    def _create_snapshot_migrate_tables_operator(
        self,
        snapshots: t.List[Snapshot],
        ddl_concurrent_tasks: int,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotMigrateTablesTarget(
                snapshots=snapshots, ddl_concurrent_tasks=ddl_concurrent_tasks
            ),
            task_id=task_id,
        )

    def _create_snapshot_evaluation_operator(
        self,
        snapshots: t.Dict[SnapshotId, Snapshot],
        snapshot: Snapshot,
        task_id: str,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        plan_id: t.Optional[str] = None,
    ) -> BaseOperator:
        parent_snapshots = {snapshots[sid].name: snapshots[sid] for sid in snapshot.parents}

        return self._engine_operator(
            **self._engine_operator_args,
            target=targets.SnapshotEvaluationTarget(
                snapshot=snapshot,
                parent_snapshots=parent_snapshots,
                start=start,
                end=end,
                deployability_index=deployability_index or DeployabilityIndex.all_deployable(),
                plan_id=plan_id,
            ),
            task_id=task_id,
        )

    def _create_hwm_sensors(
        self, snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
    ) -> t.List[BaseSensorOperator]:
        output: t.List[BaseSensorOperator] = []
        for upstream_snapshot_id in snapshot.parents:
            upstream_snapshot = snapshots[upstream_snapshot_id]
            if not upstream_snapshot.is_symbolic and not upstream_snapshot.is_seed:
                output.append(
                    HighWaterMarkSensor(
                        target_snapshot_info=upstream_snapshot.table_info,
                        this_snapshot=snapshot,
                        task_id=f"{sanitize_name(upstream_snapshot.node.name)}_{upstream_snapshot.version}_high_water_mark_sensor",
                    )
                )

        external_sesnor = self._create_hwm_external_sensor(snapshot)
        if external_sesnor:
            output.append(external_sesnor)

        return output

    def _create_hwm_external_sensor(
        self,
        snapshot: Snapshot,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
    ) -> t.Optional[BaseSensorOperator]:
        if self._external_table_sensor_factory and snapshot.model.signals:
            return HighWaterMarkExternalSensor(
                snapshot=snapshot,
                external_table_sensor_factory=self._external_table_sensor_factory,
                task_id="external_high_water_mark_sensor",
                start=start,
                end=end,
            )
        return None


def creation_update_state_task(new_snapshots: t.Iterable[Snapshot]) -> None:
    with util.scoped_state_sync() as state_sync:
        state_sync.push_snapshots(new_snapshots)


def promotion_update_state_task(
    environment: Environment,
    no_gaps_snapshot_names: t.Optional[t.Set[str]],
) -> None:
    with util.scoped_state_sync() as state_sync:
        state_sync.promote(environment, no_gaps_snapshot_names=no_gaps_snapshot_names)


def promotion_unpause_snapshots_task(
    environment: Environment,
    unpaused_dt: t.Optional[TimeLike],
) -> None:
    if environment.snapshots and unpaused_dt:
        with util.scoped_state_sync() as state_sync:
            state_sync.unpause_snapshots(environment.snapshots, unpaused_dt)


def promotion_finalize_task(environment: Environment) -> None:
    with util.scoped_state_sync() as state_sync:
        state_sync.finalize(environment)
