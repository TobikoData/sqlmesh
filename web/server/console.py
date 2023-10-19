from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from fastapi.encoders import jsonable_encoder
from sse_starlette.sse import ServerSentEvent

from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import Plan
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.date import now_timestamp
from web.server import models
from web.server.exceptions import ApiException


class ApiConsole(TerminalConsole):
    plan_cancel_stage_tracker: t.Optional[models.PlanCancelStageTracker] = None
    plan_apply_stage_tracker: t.Optional[models.PlanApplyStageTracker] = None
    plan_overview_stage_tracker: t.Optional[models.PlanOverviewStageTracker] = None

    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, t.Any]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def start_plan_evaluation(self, plan: Plan) -> None:
        self.plan_apply_stage_tracker = (
            self.plan_apply_stage_tracker
            or models.PlanApplyStageTracker(environment=plan.environment.name)
        )
        self.log_event_plan_apply()

    def stop_plan_evaluation(self) -> None:
        if self.plan_apply_stage_tracker:
            self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=True)

    def start_creation_progress(self, total_tasks: int) -> None:
        if self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.creation,
                models.PlanStageCreation(total_tasks=total_tasks, num_tasks=0),
            )

        self.log_event_plan_apply()

    def update_creation_progress(self, num_tasks: int) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.creation:
            self.plan_apply_stage_tracker.creation.update(
                {"num_tasks": self.plan_apply_stage_tracker.creation.num_tasks + num_tasks}
            )

        self.log_event_plan_apply()

    def stop_creation_progress(self, success: bool = True) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.creation:
            self.plan_apply_stage_tracker.creation.stop(success=success)

            if not success:
                self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=True)

    def start_restate_progress(self) -> None:
        if self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.restate, models.PlanStageRestate()
            )

        self.log_event_plan_apply()

    def stop_restate_progress(self, success: bool) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.restate:
            self.plan_apply_stage_tracker.restate.stop(success=success)

            if not success:
                self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=True)

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        if self.plan_apply_stage_tracker:
            tasks = {
                snapshot.name: models.BackfillTask(
                    completed=0,
                    total=total_tasks,
                    start=now_timestamp(),
                    view_name=snapshot.qualified_view_name.for_environment(environment_naming_info),
                )
                for snapshot, total_tasks in batches.items()
            }
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.backfill,
                models.PlanStageBackfill(
                    queue=set(),
                    tasks=tasks,
                ),
            )

        self.log_event_plan_apply()

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.backfill:
            self.plan_apply_stage_tracker.backfill.queue.add(snapshot.name)

        self.log_event_plan_apply()

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]
    ) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.backfill:
            task = self.plan_apply_stage_tracker.backfill.tasks[snapshot.name]
            task.completed += 1
            if task.completed >= task.total:
                task.end = now_timestamp()

            self.plan_apply_stage_tracker.backfill.tasks[snapshot.name] = task
            self.plan_apply_stage_tracker.backfill.queue.remove(snapshot.name)

        self.log_event_plan_apply()

    def stop_evaluation_progress(self, success: bool = True) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.backfill:
            self.plan_apply_stage_tracker.backfill.stop(success=success)

            if not success:
                self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=True)

    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        if self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.promote,
                models.PlanStagePromote(
                    total_tasks=total_tasks, num_tasks=0, target_environment=environment
                ),
            )

        self.log_event_plan_apply()

    def update_promotion_progress(self, num_tasks: int) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.promote:
            self.plan_apply_stage_tracker.promote.update(
                {"num_tasks": self.plan_apply_stage_tracker.promote.num_tasks + num_tasks}
            )

        self.log_event_plan_apply()

    def stop_promotion_progress(self, success: bool = True) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.promote:
            self.plan_apply_stage_tracker.promote.stop(success=success)

            if not success:
                self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=True)

    def start_plan_tracker(
        self,
        tracker: t.Union[
            models.PlanApplyStageTracker,
            models.PlanOverviewStageTracker,
            models.PlanCancelStageTracker,
        ],
    ) -> None:
        if isinstance(tracker, models.PlanApplyStageTracker):
            self.plan_apply_stage_tracker = tracker
            self.log_event_plan_apply()
        elif isinstance(tracker, models.PlanOverviewStageTracker):
            self.plan_overview_stage_tracker = tracker
            self.log_event_plan_overview()
        elif isinstance(tracker, models.PlanCancelStageTracker):
            self.plan_cancel_stage_tracker = tracker
            self.log_event_plan_cancel()

    def stop_plan_tracker(
        self,
        tracker: t.Union[
            models.PlanApplyStageTracker,
            models.PlanOverviewStageTracker,
            models.PlanCancelStageTracker,
        ],
        success: bool = True,
    ) -> None:
        if isinstance(tracker, models.PlanApplyStageTracker) and self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.stop(success=success)
            self.log_event_plan_apply()
            self.plan_apply_stage_tracker = None
        elif (
            isinstance(tracker, models.PlanOverviewStageTracker)
            and self.plan_overview_stage_tracker
        ):
            self.plan_overview_stage_tracker.stop(success=success)
            self.log_event_plan_overview()
            self.plan_overview_stage_tracker = None
        elif isinstance(tracker, models.PlanCancelStageTracker) and self.plan_cancel_stage_tracker:
            self.plan_cancel_stage_tracker.stop(success=success)
            self.log_event_plan_cancel()
            self.plan_cancel_stage_tracker = None

    def _make_event(self, event: str, data: dict[str, t.Any]) -> ServerSentEvent:
        if isinstance(event, models.ConsoleEvent):
            event = event.value
        return ServerSentEvent(
            event=event,
            data=json.dumps(jsonable_encoder(data, exclude_none=True)),
        )

    def log_event(self, event: str, data: dict[str, t.Any]) -> None:
        self.queue.put_nowait(self._make_event(event=event, data=data))

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        if result.wasSuccessful():
            self.log_event(
                event="tests",
                data=models.ReportTestsResult(
                    message=f"Successfully ran {str(result.testsRun)} tests against {target_dialect}"
                ).dict(),
            )
            return

        messages = []
        for test, details in result.failures + result.errors:
            if isinstance(test, ModelTest):
                messages.append(
                    models.ReportTestDetails(
                        message=f"Failure test: {test.model.name} {test.test_name}",
                        details=details,
                    )
                )
        self.log_event(
            event="tests",
            data=models.ReportTestsFailure(
                message="Test Failure Summary",
                total=result.testsRun,
                failures=len(result.failures),
                errors=len(result.errors),
                successful=result.testsRun - len(result.failures) - len(result.errors),
                dialect=target_dialect,
                details=messages,
                traceback=output,
            ).dict(),
        )

    def log_event_plan_apply(self) -> None:
        self.log_event(
            event=models.ConsoleEvent.plan_apply,
            data=self.plan_apply_stage_tracker.dict() if self.plan_apply_stage_tracker else {},
        )

    def log_event_plan_overview(self) -> None:
        self.log_event(
            event=models.ConsoleEvent.plan_overview,
            data=self.plan_overview_stage_tracker.dict()
            if self.plan_overview_stage_tracker
            else {},
        )

    def log_event_plan_cancel(self) -> None:
        self.log_event(
            event=models.ConsoleEvent.plan_cancel,
            data=self.plan_cancel_stage_tracker.dict() if self.plan_cancel_stage_tracker else {},
        )

    def log_exception(self) -> None:
        self.log_event(
            event="errors",
            data=ApiException(
                message="Tasks failed to run",
                origin="API -> console -> log_exception",
            ).to_dict(),
        )


api_console = ApiConsole()
