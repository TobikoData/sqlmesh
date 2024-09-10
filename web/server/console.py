from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from fastapi.encoders import jsonable_encoder
from sse_starlette.sse import ServerSentEvent

from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike
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

    def start_plan_evaluation(self, plan: EvaluatablePlan) -> None:
        self.plan_apply_stage_tracker = (
            self.plan_apply_stage_tracker
            or models.PlanApplyStageTracker(environment=plan.environment.name)
        )
        self.log_event_plan_apply()

    def stop_plan_evaluation(self) -> None:
        if self.plan_apply_stage_tracker:
            self.stop_plan_tracker(self.plan_apply_stage_tracker, True)

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        if self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.creation,
                models.PlanStageCreation(total_tasks=total_tasks, num_tasks=0),
            )

        self.log_event_plan_apply()

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.creation:
            self.plan_apply_stage_tracker.creation.update(
                {"num_tasks": self.plan_apply_stage_tracker.creation.num_tasks + 1}
            )

        self.log_event_plan_apply()

    def stop_creation_progress(self, success: bool = True) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.creation:
            self.plan_apply_stage_tracker.creation.stop(success=success)

            if not success:
                if self.is_cancelling_plan():
                    self.finish_plan_cancellation()
                else:
                    self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=success)

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
                if self.is_cancelling_plan():
                    self.finish_plan_cancellation()
                else:
                    self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=success)

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        if self.plan_apply_stage_tracker:
            tasks = {
                snapshot.name: models.BackfillTask(
                    completed=0,
                    total=total_tasks,
                    start=now_timestamp(),
                    name=snapshot.name,
                    view_name=snapshot.display_name(environment_naming_info, default_catalog),
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
                if self.is_cancelling_plan():
                    self.finish_plan_cancellation()
                else:
                    self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=success)

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        if self.plan_apply_stage_tracker:
            self.plan_apply_stage_tracker.add_stage(
                models.PlanStage.promote,
                models.PlanStagePromote(
                    total_tasks=total_tasks,
                    num_tasks=0,
                    target_environment=environment_naming_info.name,
                ),
            )

        self.log_event_plan_apply()

    def update_promotion_progress(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.promote:
            self.plan_apply_stage_tracker.promote.update(
                {"num_tasks": self.plan_apply_stage_tracker.promote.num_tasks + 1}
            )

        self.log_event_plan_apply()

    def stop_promotion_progress(self, success: bool = True) -> None:
        if self.plan_apply_stage_tracker and self.plan_apply_stage_tracker.promote:
            self.plan_apply_stage_tracker.promote.stop(success=success)

            if not success:
                if self.is_cancelling_plan():
                    self.finish_plan_cancellation()
                else:
                    self.stop_plan_tracker(tracker=self.plan_apply_stage_tracker, success=success)

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
            self.stop_plan_tracker_stages(self.plan_apply_stage_tracker, False)
            self.plan_apply_stage_tracker.stop(success)
            self.log_event_plan_apply()
            self.plan_apply_stage_tracker = None
        elif (
            isinstance(tracker, models.PlanOverviewStageTracker)
            and self.plan_overview_stage_tracker
        ):
            self.stop_plan_tracker_stages(self.plan_overview_stage_tracker, False)
            self.plan_overview_stage_tracker.stop(success)
            self.log_event_plan_overview()
        elif isinstance(tracker, models.PlanCancelStageTracker) and self.plan_cancel_stage_tracker:
            self.stop_plan_tracker_stages(self.plan_cancel_stage_tracker, False)
            self.plan_cancel_stage_tracker.stop(success)
            self.log_event_plan_cancel()
            self.plan_cancel_stage_tracker = None

    def log_event(
        self, event: models.EventName, data: t.Union[t.Dict[str, t.Any], t.List[t.Any]]
    ) -> None:
        self.queue.put_nowait(
            ServerSentEvent(
                event=event.value,
                data=json.dumps(jsonable_encoder(data, exclude_none=True)),
            )
        )

    def log_test_results(
        self, result: unittest.result.TestResult, output: t.Optional[str], target_dialect: str
    ) -> None:
        if result.wasSuccessful():
            self.log_event(
                event=models.EventName.TESTS,
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
            event=models.EventName.TESTS,
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
            event=models.EventName.PLAN_APPLY,
            data=self.plan_apply_stage_tracker.dict() if self.plan_apply_stage_tracker else {},
        )

    def log_event_plan_overview(self) -> None:
        self.log_event(
            event=models.EventName.PLAN_OVERVIEW,
            data=(
                self.plan_overview_stage_tracker.dict() if self.plan_overview_stage_tracker else {}
            ),
        )

    def log_event_plan_cancel(self) -> None:
        self.log_event(
            event=models.EventName.PLAN_CANCEL,
            data=self.plan_cancel_stage_tracker.dict() if self.plan_cancel_stage_tracker else {},
        )

    def log_exception(self, exception: ApiException) -> None:
        self.log_event(
            event=models.EventName.ERRORS,
            data=exception.to_dict(),
        )

        if self.plan_overview_stage_tracker:
            self.stop_plan_tracker(self.plan_overview_stage_tracker, False)

        if self.plan_apply_stage_tracker:
            self.stop_plan_tracker(self.plan_apply_stage_tracker, False)

    def is_cancelling_plan(self) -> bool:
        return bool(self.plan_cancel_stage_tracker and not self.plan_cancel_stage_tracker.meta.done)

    def stop_plan_tracker_stages(
        self,
        tracker: t.Optional[
            t.Union[
                models.PlanApplyStageTracker,
                models.PlanCancelStageTracker,
                models.PlanOverviewStageTracker,
            ]
        ],
        success: bool = True,
    ) -> None:
        if not tracker:
            return

        stages = (
            [attr for attr in tracker.__fields__ if not attr.startswith("__")] if tracker else []
        )

        for key in stages:
            stage = getattr(tracker, key)
            if isinstance(stage, models.Trackable) and stage.meta and not stage.meta.done:
                stage.stop(success)

        tracker.stop(success)

    def finish_plan_cancellation(self) -> None:
        cancel_tracker = self.plan_cancel_stage_tracker

        if not cancel_tracker:
            return

        if cancel_tracker.cancel and not cancel_tracker.cancel.meta.done:
            cancel_tracker.cancel.stop(success=True)

        # We can only cancel plan apply
        # We need to stop it and clean up the state
        apply_tracker = self.plan_apply_stage_tracker
        stages = (
            [attr for attr in apply_tracker.__fields__ if not attr.startswith("__")]
            if apply_tracker
            else []
        )

        def _is_stage_done(stage: str) -> bool:
            stage = getattr(apply_tracker, stage)
            return stage.meta.done if stage and hasattr(stage, "meta") else True

        is_apply_tracker_completed = all([_is_stage_done(stage) for stage in stages])

        if apply_tracker and not apply_tracker.meta.done and is_apply_tracker_completed:
            self.stop_plan_tracker(apply_tracker, False)
            self.stop_plan_tracker(cancel_tracker, True)


api_console = ApiConsole()
