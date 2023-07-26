"""
# PlanEvaluator

A plan evaluator is responsible for evaluating a plan when it is being applied.

# Evaluation steps

At a high level, when a plan is evaluated, SQLMesh will:
- Push new snapshots to the state sync.
- Create snapshot tables.
- Backfill data.
- Promote the snapshots.

Refer to `sqlmesh.core.plan`.
"""
import abc
import typing as t

from sqlmesh.core.console import Console, get_console
from sqlmesh.core.notification_target import (
    NotificationTarget,
    NotificationTargetManager,
)
from sqlmesh.core.plan.definition import Plan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotId,
    SnapshotInfoLike,
    has_paused_forward_only,
)
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common as airflow_common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils import random_id
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError


class PlanEvaluator(abc.ABC):
    @abc.abstractmethod
    def evaluate(self, plan: Plan) -> None:
        """Evaluates a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state and then kicks off
        the backfill process for all affected snapshots. Once backfill is done,
        snapshots that are part of the plan are promoted in the environment targeted
        by this plan.

        Args:
            plan: The plan to evaluate.
        """


class BuiltInPlanEvaluator(PlanEvaluator):
    def __init__(
        self,
        state_sync: StateSync,
        snapshot_evaluator: SnapshotEvaluator,
        backfill_concurrent_tasks: int = 1,
        console: t.Optional[Console] = None,
        notification_target_manager: t.Optional[NotificationTargetManager] = None,
    ):
        self.state_sync = state_sync
        self.snapshot_evaluator = snapshot_evaluator
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.console = console or get_console()
        self.notification_target_manager = notification_target_manager

        self.__all_snapshots: t.Dict[str, t.Dict[SnapshotId, Snapshot]] = {}

    def evaluate(self, plan: Plan) -> None:
        tasks = (
            [self._push, self._restate, self._backfill, self._promote]
            if not has_paused_forward_only(plan.snapshots, plan.snapshots) or plan.is_dev
            else [self._push, self._restate, self._promote, self._backfill]
        )

        for task in tasks:
            task(plan)

        if not plan.requires_backfill:
            self.console.log_success("Virtual Update executed successfully")

    def _backfill(self, plan: Plan) -> None:
        """Backfill missing intervals for snapshots that are part of the given plan.

        Args:
            plan: The plan to source snapshots from.
        """
        if not plan.requires_backfill:
            return

        snapshots = plan.snapshots
        scheduler = Scheduler(
            snapshots,
            self.snapshot_evaluator,
            self.state_sync,
            max_workers=self.backfill_concurrent_tasks,
            console=self.console,
            notification_target_manager=self.notification_target_manager,
        )
        is_run_successful = scheduler.run(
            plan.environment_name,
            plan.start,
            plan.end,
            restatements=plan.restatements,
            ignore_cron=True,
        )
        if not is_run_successful:
            raise SQLMeshError("Plan application failed.")

    def _push(self, plan: Plan) -> None:
        """Push the snapshots to the state sync.

        As a part of plan pushing, snapshot tables are created.

        Args:
            plan: The plan to source snapshots from.
        """
        snapshot_id_to_snapshot = {s.snapshot_id: s for s in plan.snapshots}

        if plan.new_snapshots:
            self.console.start_creation_progress(len(plan.new_snapshots))

        def on_complete(snapshot: SnapshotInfoLike) -> None:
            self.console.update_creation_progress(1)

        completed = False
        try:
            self.snapshot_evaluator.create(
                plan.new_snapshots,
                snapshot_id_to_snapshot,
                on_complete=on_complete,
            )
            completed = True
        finally:
            self.console.stop_creation_progress(success=completed)

        self.state_sync.push_snapshots(plan.new_snapshots)

    def _promote(self, plan: Plan) -> None:
        """Promote a plan.

        Promotion creates views with a model's name + env pointing to a physical snapshot.

        Args:
            plan: The plan to promote.
        """
        environment = plan.environment

        added, removed = self.state_sync.promote(environment, no_gaps=plan.no_gaps)

        self.console.start_promotion_progress(environment.name, len(added) + len(removed))

        if not plan.is_dev:
            self.snapshot_evaluator.migrate(
                [s for s in plan.snapshots if s.is_paused],
                {s.snapshot_id: s for s in plan.snapshots},
            )
            self.state_sync.unpause_snapshots(added, now())

        def on_complete(snapshot: SnapshotInfoLike) -> None:
            self.console.update_promotion_progress(1)

        completed = False
        try:
            self.snapshot_evaluator.promote(
                added,
                environment=environment.name,
                is_dev=plan.is_dev,
                on_complete=on_complete,
            )
            self.snapshot_evaluator.demote(
                removed,
                environment=environment.name,
                on_complete=on_complete,
            )
            self.state_sync.finalize(environment)
            completed = True
        finally:
            self.console.stop_promotion_progress(success=completed)

    def _restate(self, plan: Plan) -> None:
        if not plan.restatements:
            return

        target_snapshots = [s for s in plan.snapshots if s.name in plan.restatements]
        if plan.is_dev:
            self.state_sync.remove_interval(
                [],
                start=plan.start,
                end=plan.end,
                all_snapshots=target_snapshots,
            )
        else:
            self.state_sync.remove_interval(
                target_snapshots,
                start=plan.start,
                end=plan.end,
            )


class AirflowPlanEvaluator(PlanEvaluator):
    def __init__(
        self,
        airflow_client: AirflowClient,
        console: t.Optional[Console] = None,
        blocking: bool = True,
        dag_run_poll_interval_secs: int = 10,
        dag_creation_poll_interval_secs: int = 30,
        dag_creation_max_retry_attempts: int = 10,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        backfill_concurrent_tasks: int = 1,
        ddl_concurrent_tasks: int = 1,
        users: t.Optional[t.List[User]] = None,
    ):
        self.airflow_client = airflow_client
        self.blocking = blocking
        self.dag_run_poll_interval_secs = dag_run_poll_interval_secs
        self.dag_creation_poll_interval_secs = dag_creation_poll_interval_secs
        self.dag_creation_max_retry_attempts = dag_creation_max_retry_attempts
        self.console = console or get_console()
        self.notification_targets = notification_targets or []
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self.users = users or []

    def evaluate(self, plan: Plan) -> None:
        environment = plan.environment

        plan_request_id = random_id()

        self.airflow_client.apply_plan(
            plan.new_snapshots,
            environment,
            plan_request_id,
            no_gaps=plan.no_gaps,
            skip_backfill=plan.skip_backfill,
            restatements=plan.restatements,
            notification_targets=self.notification_targets,
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=self.users,
            is_dev=plan.is_dev,
            forward_only=plan.forward_only,
        )

        if self.blocking:
            plan_application_dag_id = airflow_common.plan_application_dag_id(
                environment.name, plan_request_id
            )

            self.console.log_status_update(
                f"Waiting for the plan application DAG '{plan_application_dag_id}' to be provisioned on Airflow"
            )

            plan_application_dag_run_id = self.airflow_client.wait_for_first_dag_run(
                plan_application_dag_id,
                self.dag_creation_poll_interval_secs,
                self.dag_creation_max_retry_attempts,
            )

            self.airflow_client.print_tracking_url(
                plan_application_dag_id,
                plan_application_dag_run_id,
                "plan application",
            )
            plan_application_succeeded = self.airflow_client.wait_for_dag_run_completion(
                plan_application_dag_id,
                plan_application_dag_run_id,
                self.dag_run_poll_interval_secs,
            )
            if not plan_application_succeeded:
                raise SQLMeshError("Plan application failed.")
