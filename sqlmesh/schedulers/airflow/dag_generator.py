from __future__ import annotations

import logging
import os
import typing as t

from airflow import DAG
from airflow.models import BaseOperator, baseoperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import PlanStatus
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotTableInfo
from sqlmesh.integrations.github.notification_operator_provider import (
    GithubNotificationOperatorProvider,
)
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.schedulers.airflow.operators.hwm_sensor import HighWaterMarkSensor
from sqlmesh.schedulers.airflow.operators.notification import (
    BaseNotificationOperatorProvider,
)
from sqlmesh.utils.date import TimeLike, now, to_datetime
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


TASK_ID_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S"

NOTIFICATION_TARGET_TO_OPERATOR_PROVIDER: t.Dict[
    t.Type[NotificationTarget], BaseNotificationOperatorProvider
] = {
    GithubNotificationTarget: GithubNotificationOperatorProvider(),
}

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


class SnapshotDagGenerator:
    def __init__(
        self,
        engine_operator: t.Type[BaseOperator],
        engine_operator_args: t.Optional[t.Dict[str, t.Any]],
        ddl_engine_operator: t.Type[BaseOperator],
        ddl_engine_operator_args: t.Optional[t.Dict[str, t.Any]],
        snapshots: t.Dict[SnapshotId, Snapshot],
    ):
        self._engine_operator = engine_operator
        self._engine_operator_args = engine_operator_args or {}
        self._ddl_engine_operator = ddl_engine_operator
        self._ddl_engine_operator_args = ddl_engine_operator_args or {}
        self._snapshots = snapshots

    def generate_cadence_dags(self) -> t.List[DAG]:
        return [
            self._create_cadence_dag_for_snapshot(s)
            for s in self._snapshots.values()
            if s.unpaused_ts and not s.is_embedded_kind and not s.is_seed_kind
        ]

    def generate_plan_application_dag(self, spec: common.PlanDagSpec) -> DAG:
        return self._create_plan_application_dag(spec)

    def _create_cadence_dag_for_snapshot(self, snapshot: Snapshot) -> DAG:
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
            schedule_interval=snapshot.model.cron,
            start_date=to_datetime(snapshot.unpaused_ts),
            max_active_runs=1,
            catchup=True,
            is_paused_upon_creation=False,
            tags=[
                common.SQLMESH_AIRFLOW_TAG,
                common.SNAPSHOT_AIRFLOW_TAG,
                snapshot.name,
            ],
            default_args={
                **DAG_DEFAULT_ARGS,
                "email": snapshot.model.owner,
                "email_on_failure": True,
            },
        ) as dag:
            hwm_sensor_tasks = self._create_hwm_sensors(snapshot=snapshot)

            evaluator_task = self._create_snapshot_evaluator_operator(
                snapshots=self._snapshots,
                snapshot=snapshot,
                task_id="snapshot_evaluator",
            )

            hwm_sensor_tasks >> evaluator_task

            return dag

    def _create_plan_application_dag(self, plan_dag_spec: common.PlanDagSpec) -> DAG:
        dag_id = common.plan_application_dag_id(
            plan_dag_spec.environment_name, plan_dag_spec.request_id
        )
        logger.info(
            "Generating the plan application DAG '%s' for environment '%s'",
            dag_id,
            plan_dag_spec.environment_name,
        )

        all_snapshots = {
            **{s.snapshot_id: s for s in plan_dag_spec.new_snapshots},
            **self._snapshots,
        }

        with DAG(
            dag_id=dag_id,
            schedule_interval="@once",
            start_date=now(),
            max_active_tasks=plan_dag_spec.backfill_concurrent_tasks,
            catchup=False,
            is_paused_upon_creation=False,
            default_args=DAG_DEFAULT_ARGS,
            tags=[
                common.SQLMESH_AIRFLOW_TAG,
                common.PLAN_AIRFLOW_TAG,
                plan_dag_spec.environment_name,
            ],
        ) as dag:
            start_task = EmptyOperator(task_id="plan_application_start")
            end_task = EmptyOperator(task_id="plan_application_end")

            (create_start_task, create_end_task) = self._create_creation_tasks(
                plan_dag_spec.new_snapshots, plan_dag_spec.ddl_concurrent_tasks
            )

            (backfill_start_task, backfill_end_task) = self._create_backfill_tasks(
                plan_dag_spec.backfill_intervals_per_snapshot,
                all_snapshots,
                plan_dag_spec.is_dev,
            )

            (
                promote_start_task,
                promote_end_task,
            ) = self._create_promotion_demotion_tasks(plan_dag_spec)

            start_task >> create_start_task
            create_end_task >> backfill_start_task
            backfill_end_task >> promote_start_task

            self._add_notification_target_tasks(
                plan_dag_spec, start_task, end_task, promote_end_task
            )
            return dag

    def _add_notification_target_tasks(
        self,
        request: common.PlanDagSpec,
        start_task: BaseOperator,
        end_task: BaseOperator,
        promote_end_task: BaseOperator,
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
                promote_end_task >> plan_success_notification_task
                plan_success_notification_task >> end_task
            if plan_failed_notification_task:
                has_success_or_failed_notification = True
                promote_end_task >> plan_failed_notification_task
                plan_failed_notification_task >> end_task
        if not has_success_or_failed_notification:
            promote_end_task >> end_task

    def _create_creation_tasks(
        self, new_snapshots: t.List[Snapshot], ddl_concurrent_tasks: int
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        start_task = EmptyOperator(task_id="snapshot_creation_start")
        end_task = EmptyOperator(task_id="snapshot_creation_end")

        if not new_snapshots:
            start_task >> end_task
            return (start_task, end_task)

        creation_task = self._create_snapshot_create_tables_operator(
            new_snapshots, ddl_concurrent_tasks, "snapshot_creation__create_tables"
        )

        update_state_task = PythonOperator(
            task_id="snapshot_creation__update_state",
            python_callable=creation_update_state_task,
            op_kwargs={"new_snapshots": new_snapshots},
        )

        start_task >> creation_task
        creation_task >> update_state_task
        update_state_task >> end_task

        return (start_task, end_task)

    def _create_promotion_demotion_tasks(
        self, request: common.PlanDagSpec
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        start_task = EmptyOperator(task_id="snapshot_promotion_start")
        end_task = EmptyOperator(task_id="snapshot_promotion_end")

        update_state_task = PythonOperator(
            task_id="snapshot_promotion__update_state",
            python_callable=promotion_update_state_task,
            op_kwargs={
                "snapshots": request.promoted_snapshots,
                "environment_name": request.environment_name,
                "start": request.start,
                "end": request.end,
                "unpaused_dt": request.unpaused_dt,
                "no_gaps": request.no_gaps,
                "plan_id": request.plan_id,
                "previous_plan_id": request.previous_plan_id,
                "environment_expiration_ts": request.environment_expiration_ts,
            },
        )

        start_task >> update_state_task

        if request.promoted_snapshots:
            create_views_task = self._create_snapshot_promotion_operator(
                request.promoted_snapshots,
                request.environment_name,
                request.ddl_concurrent_tasks,
                request.is_dev,
                "snapshot_promotion__create_views",
            )
            create_views_task >> end_task

            if not request.is_dev and request.unpaused_dt:
                migrate_tables_task = self._create_snapshot_migrate_tables_operator(
                    request.promoted_snapshots,
                    request.ddl_concurrent_tasks,
                    "snapshot_promotion__migrate_tables",
                )
                update_state_task >> migrate_tables_task
                migrate_tables_task >> create_views_task
            else:
                update_state_task >> create_views_task

        if request.demoted_snapshots:
            delete_views_task = self._create_snapshot_demotion_operator(
                request.demoted_snapshots,
                request.environment_name,
                request.ddl_concurrent_tasks,
                "snapshot_promotion__delete_views",
            )
            update_state_task >> delete_views_task
            delete_views_task >> end_task

        if not request.promoted_snapshots and not request.demoted_snapshots:
            update_state_task >> end_task

        return (start_task, end_task)

    def _create_backfill_tasks(
        self,
        backfill_intervals: t.List[common.BackfillIntervalsPerSnapshot],
        snapshots: t.Dict[SnapshotId, Snapshot],
        is_dev: bool,
    ) -> t.Tuple[BaseOperator, BaseOperator]:
        snapshot_to_tasks = {}
        for intervals_per_snapshot in backfill_intervals:
            sid = intervals_per_snapshot.snapshot_id

            if not intervals_per_snapshot.intervals:
                logger.info(f"Skipping backfill for snapshot %s", sid)
                continue

            snapshot = snapshots[sid]

            task_id_prefix = f"snapshot_evaluator__{snapshot.name}__{snapshot.identifier}"
            tasks = [
                self._create_snapshot_evaluator_operator(
                    snapshots=snapshots,
                    snapshot=snapshot,
                    task_id=f"{task_id_prefix}__{start.strftime(TASK_ID_DATE_FORMAT)}__{end.strftime(TASK_ID_DATE_FORMAT)}",
                    start=start,
                    end=end,
                    is_dev=is_dev,
                )
                for (start, end) in intervals_per_snapshot.intervals
            ]
            snapshot_start_task = EmptyOperator(
                task_id=f"snapshot_backfill__{snapshot.name}__{snapshot.identifier}__start"
            )
            snapshot_end_task = EmptyOperator(
                task_id=f"snapshot_backfill__{snapshot.name}__{snapshot.identifier}__end"
            )
            if snapshot.is_incremental_by_unique_key_kind:
                baseoperator.chain(snapshot_start_task, *tasks, snapshot_end_task)
            else:
                snapshot_start_task >> tasks >> snapshot_end_task
            snapshot_to_tasks[snapshot.snapshot_id] = (
                snapshot_start_task,
                snapshot_end_task,
            )

        backfill_start_task = EmptyOperator(task_id="snapshot_backfill_start")
        backfill_end_task = EmptyOperator(task_id="snapshot_backfill_end")

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
        snapshots: t.List[SnapshotTableInfo],
        environment: str,
        ddl_concurrent_tasks: int,
        is_dev: bool,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotPromotionTarget(
                snapshots=snapshots,
                environment=environment,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
                is_dev=is_dev,
            ),
            task_id=task_id,
        )

    def _create_snapshot_demotion_operator(
        self,
        snapshots: t.List[SnapshotTableInfo],
        environment: str,
        ddl_concurrent_tasks: int,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotDemotionTarget(
                snapshots=snapshots,
                environment=environment,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
            ),
            task_id=task_id,
        )

    def _create_snapshot_create_tables_operator(
        self,
        new_snapshots: t.List[Snapshot],
        ddl_concurrent_tasks: int,
        task_id: str,
    ) -> BaseOperator:
        return self._ddl_engine_operator(
            **self._ddl_engine_operator_args,
            target=targets.SnapshotCreateTablesTarget(
                new_snapshots=new_snapshots, ddl_concurrent_tasks=ddl_concurrent_tasks
            ),
            task_id=task_id,
        )

    def _create_snapshot_migrate_tables_operator(
        self,
        snapshots: t.List[SnapshotTableInfo],
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

    def _create_snapshot_evaluator_operator(
        self,
        snapshots: t.Dict[SnapshotId, Snapshot],
        snapshot: Snapshot,
        task_id: str,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        is_dev: bool = False,
    ) -> BaseOperator:
        parent_snapshots = {sid.name: snapshots[sid] for sid in snapshot.parents}

        return self._engine_operator(
            **self._engine_operator_args,
            target=targets.SnapshotEvaluationTarget(
                snapshot=snapshot,
                parent_snapshots=parent_snapshots,
                start=start,
                end=end,
                is_dev=is_dev,
            ),
            task_id=task_id,
        )

    def _create_hwm_sensors(self, snapshot: Snapshot) -> t.List[HighWaterMarkSensor]:
        output = []
        for upstream_snapshot_id in snapshot.parents:
            upstream_snapshot = self._snapshots[upstream_snapshot_id]
            if not upstream_snapshot.is_embedded_kind and not upstream_snapshot.is_seed_kind:
                output.append(
                    HighWaterMarkSensor(
                        target_snapshot_info=upstream_snapshot.table_info,
                        this_snapshot=snapshot,
                        task_id=f"{upstream_snapshot.name}_{upstream_snapshot.version}_high_water_mark_sensor",
                    )
                )
        return output


def creation_update_state_task(new_snapshots: t.Iterable[Snapshot]) -> None:
    with util.scoped_state_sync() as state_sync:
        state_sync.push_snapshots(new_snapshots)


def promotion_update_state_task(
    snapshots: t.List[SnapshotTableInfo],
    environment_name: str,
    start: TimeLike,
    end: t.Optional[TimeLike],
    unpaused_dt: t.Optional[TimeLike],
    no_gaps: bool,
    plan_id: str,
    previous_plan_id: t.Optional[str],
    environment_expiration_ts: t.Optional[int],
) -> None:
    environment = Environment(
        name=environment_name,
        snapshots=snapshots,
        start_at=start,
        end_at=end,
        plan_id=plan_id,
        previous_plan_id=previous_plan_id,
        expiration_ts=environment_expiration_ts,
    )
    with util.scoped_state_sync() as state_sync:
        state_sync.promote(environment, no_gaps=no_gaps)
        if snapshots and not end and unpaused_dt:
            state_sync.unpause_snapshots(snapshots, unpaused_dt)
