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
import logging
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
)
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common as airflow_common
from sqlmesh.schedulers.airflow.client import AirflowClient, BaseAirflowClient
from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient
from sqlmesh.utils import random_id
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


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
        snapshots = {s.snapshot_id: s for s in plan.snapshots}
        all_names = {s.name for s in plan.snapshots}
        if plan.is_dev:
            before_promote_snapshots = all_names
            after_promote_snapshots = set()
        else:
            before_promote_snapshots = {
                s.name for s in snapshots.values() if can_evaluate_before_promote(s, snapshots)
            }
            after_promote_snapshots = all_names - before_promote_snapshots

        self._push(plan)
        self._restate(plan)
        self._backfill(plan, before_promote_snapshots)
        self._promote(plan)
        self._backfill(plan, after_promote_snapshots)

        if not plan.requires_backfill:
            self.console.log_success("Virtual Update executed successfully")

    def _backfill(self, plan: Plan, selected_snapshots: t.Set[str]) -> None:
        """Backfill missing intervals for snapshots that are part of the given plan.

        Args:
            plan: The plan to source snapshots from.
            selected_snapshots: The snapshots to backfill.
        """
        if not plan.requires_backfill or not selected_snapshots:
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
            plan.environment_naming_info,
            plan.start,
            plan.end,
            restatements=plan.restatements,
            ignore_cron=True,
            selected_snapshots=selected_snapshots,
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

        promotion_result = self.state_sync.promote(environment, no_gaps=plan.no_gaps)

        self.console.start_promotion_progress(
            environment.name, len(promotion_result.added) + len(promotion_result.removed)
        )

        if not plan.is_dev:
            self.snapshot_evaluator.migrate(
                [s for s in plan.snapshots if s.is_paused],
                {s.snapshot_id: s for s in plan.snapshots},
            )
            self.state_sync.unpause_snapshots(promotion_result.added, now())

        def on_complete(snapshot: SnapshotInfoLike) -> None:
            self.console.update_promotion_progress(1)

        completed = False
        try:
            self.snapshot_evaluator.promote(
                [plan.context_diff.snapshots[s.name] for s in promotion_result.added],
                environment.naming_info,
                is_dev=plan.is_dev,
                on_complete=on_complete,
            )
            if promotion_result.removed_environment_naming_info:
                self.snapshot_evaluator.demote(
                    promotion_result.removed,
                    promotion_result.removed_environment_naming_info,
                    on_complete=on_complete,
                )
            self.state_sync.finalize(environment)
            completed = True
        finally:
            self.console.stop_promotion_progress(success=completed)

    def _restate(self, plan: Plan) -> None:
        if not plan.restatements:
            return

        self.state_sync.remove_interval(
            [
                (plan.context_diff.snapshots[s], interval)
                for s, interval in plan.restatements.items()
            ],
            plan._execution_time,
            remove_shared_versions=not plan.is_dev,
        )


class BaseAirflowPlanEvaluator(PlanEvaluator):
    def __init__(
        self,
        console: t.Optional[Console],
        blocking: bool,
        dag_run_poll_interval_secs: int,
        dag_creation_poll_interval_secs: int,
        dag_creation_max_retry_attempts: int,
    ):
        self.blocking = blocking
        self.dag_run_poll_interval_secs = dag_run_poll_interval_secs
        self.dag_creation_poll_interval_secs = dag_creation_poll_interval_secs
        self.dag_creation_max_retry_attempts = dag_creation_max_retry_attempts
        self.console = console or get_console()

    def evaluate(self, plan: Plan) -> None:
        plan_request_id = random_id()
        self._apply_plan(plan, plan_request_id)

        if self.blocking:
            plan_application_dag_id = airflow_common.plan_application_dag_id(
                plan.environment.name, plan_request_id
            )

            self.console.log_status_update(
                f"Waiting for the plan application DAG '{plan_application_dag_id}' to be provisioned on Airflow"
            )

            plan_application_dag_run_id = self.client.wait_for_first_dag_run(
                plan_application_dag_id,
                self.dag_creation_poll_interval_secs,
                self.dag_creation_max_retry_attempts,
            )

            self.client.print_tracking_url(
                plan_application_dag_id,
                plan_application_dag_run_id,
                "plan application",
            )
            plan_application_succeeded = self.client.wait_for_dag_run_completion(
                plan_application_dag_id,
                plan_application_dag_run_id,
                self.dag_run_poll_interval_secs,
            )
            if not plan_application_succeeded:
                raise SQLMeshError("Plan application failed.")

    @property
    def client(self) -> BaseAirflowClient:
        raise NotImplementedError

    def _apply_plan(self, plan: Plan, plan_request_id: str) -> None:
        raise NotImplementedError


class AirflowPlanEvaluator(BaseAirflowPlanEvaluator):
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
        super().__init__(
            console,
            blocking,
            dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts,
        )
        self._airflow_client = airflow_client
        self.notification_targets = notification_targets or []
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self.users = users or []

    @property
    def client(self) -> BaseAirflowClient:
        return self._airflow_client

    def _apply_plan(self, plan: Plan, plan_request_id: str) -> None:
        self._airflow_client.apply_plan(
            plan.new_snapshots,
            plan.environment,
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


class MWAAPlanEvaluator(BaseAirflowPlanEvaluator):
    def __init__(
        self,
        client: MWAAClient,
        state_sync: StateSync,
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
        super().__init__(
            console,
            blocking,
            dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts,
        )
        self._mwaa_client = client
        self.state_sync = state_sync
        self.notification_targets = notification_targets or []
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self.users = users or []

    @property
    def client(self) -> BaseAirflowClient:
        return self._mwaa_client

    def _apply_plan(self, plan: Plan, plan_request_id: str) -> None:
        from sqlmesh.schedulers.airflow.plan import PlanDagState, create_plan_dag_spec

        plan_application_request = airflow_common.PlanApplicationRequest(
            new_snapshots=list(plan.new_snapshots),
            environment=plan.environment,
            no_gaps=plan.no_gaps,
            skip_backfill=plan.skip_backfill,
            request_id=plan_request_id,
            restatements=plan.restatements or {},
            notification_targets=self.notification_targets or [],
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=self.users or [],
            is_dev=plan.is_dev,
            forward_only=plan.forward_only,
        )
        plan_dag_spec = create_plan_dag_spec(plan_application_request, self.state_sync)
        PlanDagState.from_state_sync(self.state_sync).add_dag_spec(plan_dag_spec)


def can_evaluate_before_promote(
    snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot]
) -> bool:
    return not snapshot.is_paused_forward_only and not any(
        snapshots[p_id].is_paused_forward_only for p_id in snapshot.parents
    )
