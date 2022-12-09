"""
# PlanEvaluator

A plan evaluator is responsible for evaluating a plan when it is being applied.

# Evaluation steps

At a high level, when a plan is evaluated, SQLMesh will
- Push new snapshots to the state sync.
- Create snapshot tables.
- Backfill data.
- Promote the snapshots.

# See also

`sqlmesh.core.plan`
"""
import abc
import typing as t

from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.plan import Plan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.core.state_sync import StateSync
from sqlmesh.schedulers.airflow import common as airflow_common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils import random_id
from sqlmesh.utils.errors import SQLMeshError


class PlanEvaluator(abc.ABC):
    @abc.abstractmethod
    def evaluate(self, plan: Plan) -> None:
        """Evaluates a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state and then kicks off
        the backfill process for all affected snapshots. Once backfill is done
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
        console: t.Optional[Console] = None,
    ):
        self.state_sync = state_sync
        self.snapshot_evaluator = snapshot_evaluator
        self.console = console or get_console()

    def evaluate(self, plan: Plan) -> None:
        self._push(plan)

        if plan.restatements:
            self.state_sync.remove_interval(
                [],
                start=plan.start,
                end=plan.end,
                all_snapshots=self.state_sync.get_snapshots_by_models(
                    *plan.restatements
                ),
            )

        if plan.missing_intervals:
            snapshots = plan.snapshots
            scheduler = Scheduler(
                {snapshot.snapshot_id: snapshot for snapshot in snapshots},
                self.snapshot_evaluator,
                self.state_sync,
                console=self.console,
            )
            scheduler.run(plan.start, plan.end)

        self._promote(plan)

        if not plan.missing_intervals:
            self.console.log_success("Logical Update executed successfully")

    def _push(self, plan: Plan) -> None:
        """
        Push the snapshots to the state sync.

        As a part of plan pushing, snapshot tables are created.

        Args:
            plan: The plan to source snapshots from.
        """
        parent_snapshot_ids = {
            p_sid for snapshot in plan.new_snapshots for p_sid in snapshot.parents
        }

        stored_snapshots_by_id = self.state_sync.get_snapshots(parent_snapshot_ids)
        new_snapshots_by_id = {
            snapshot.snapshot_id: snapshot for snapshot in plan.new_snapshots
        }
        all_snapshots_by_id = {**stored_snapshots_by_id, **new_snapshots_by_id}

        self.snapshot_evaluator.create(plan.new_snapshots, all_snapshots_by_id)
        self.state_sync.push_snapshots(plan.new_snapshots)

    def _promote(self, plan: Plan) -> None:
        """Promote a plan.

        Promotion creates views with a model's name + env pointing to a physical snapshot.

        Args:
            plan: The plan to promote.
        """
        environment = plan.environment

        added, removed = self.state_sync.promote(environment, no_gaps=plan.no_gaps)

        self.snapshot_evaluator.promote(
            added,
            environment=environment.name,
        )
        self.snapshot_evaluator.demote(
            removed,
            environment=environment.name,
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
        ddl_concurrent_tasks: int = 1,
    ):
        self.airflow_client = airflow_client
        self.blocking = blocking
        self.dag_run_poll_interval_secs = dag_run_poll_interval_secs
        self.dag_creation_poll_interval_secs = dag_creation_poll_interval_secs
        self.dag_creation_max_retry_attempts = dag_creation_max_retry_attempts
        self.console = console or get_console()
        self.notification_targets = notification_targets or []
        self.ddl_concurrent_tasks = ddl_concurrent_tasks

    def evaluate(self, plan: Plan) -> None:
        environment = plan.environment

        plan_request_id = random_id()

        plan_receiver_dag_run_id = self.airflow_client.apply_plan(
            plan.new_snapshots,
            environment,
            plan_request_id,
            no_gaps=plan.no_gaps,
            restatements=plan.restatements,
            notification_targets=self.notification_targets,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
        )

        if self.blocking:
            plan_receiver_succeeded = self.airflow_client.wait_for_dag_run_completion(
                airflow_common.PLAN_RECEIVER_DAG_ID,
                plan_receiver_dag_run_id,
                self.dag_run_poll_interval_secs,
            )
            if not plan_receiver_succeeded:
                raise SQLMeshError("Failed to launch plan application")

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
            plan_application_succeeded = (
                self.airflow_client.wait_for_dag_run_completion(
                    plan_application_dag_id,
                    plan_application_dag_run_id,
                    self.dag_run_poll_interval_secs,
                )
            )
            if not plan_application_succeeded:
                raise SQLMeshError("Plan application failed")
