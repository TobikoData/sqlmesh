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
    task: t.Optional[asyncio.Task] = None
    report: t.Optional[models.ReportProgressPlanApply] = None

    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, t.Any]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def log_start_evaluation(self, plan: Plan) -> None:
        self.report = models.ReportProgressPlanApply(environment=plan.environment.name)
        self.log_event_apply()

    def log_stop_evaluation(self) -> None:
        if self.report:
            self.report.stop(success=True)
        self.log_event_apply()
        self.report = None

    def start_creation_progress(self, total_tasks: int) -> None:
        if self.report:
            self.report.add(
                models.ReportStagePlanApply.creation,
                models.ReportStagePlanApplyCreation(
                    total_tasks=total_tasks, num_tasks=0),
            )

        self.log_event_apply()

    def update_creation_progress(self, num_tasks: int) -> None:
        if self.report and self.report.creation:
            self.report.creation.update(
                {"num_tasks": self.report.creation.num_tasks + num_tasks})

        self.log_event_apply()

    def stop_creation_progress(self, success: bool = True) -> None:
        if self.report and self.report.creation:
            self.report.creation.stop(success=success)

            if not success:
                self.log_stop_evaluation()

    def start_restate_progress(self) -> None:
        if self.report:
            self.report.add(
                models.ReportStagePlanApply.restate, models.ReportStagePlanApplyRestate()
            )

        self.log_event_apply()

    def stop_restate_progress(self, success: bool) -> None:
        if self.report and self.report.restate:
            self.report.restate.stop(success=success)

            if not success:
                self.log_stop_evaluation()

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        if self.report:
            self.report.add(
                models.ReportStagePlanApply.backfill,
                models.ReportStagePlanApplyBackfill(
                    queue=set(),
                    tasks={
                        snapshot.name: models.BackfillTask(
                            completed=0,
                            total=total_tasks,
                            start=now_timestamp(),
                            view_name=snapshot.qualified_view_name.for_environment(
                                environment_naming_info
                            ),
                        )
                        for snapshot, total_tasks in batches.items()
                    },
                ),
            )

        self.log_event_apply()

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if self.report and self.report.backfill:
            self.report.backfill.queue.add(snapshot.name)

        self.log_event_apply()

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]) -> None:
        if self.report and self.report.backfill:
            task = self.report.backfill.tasks[snapshot.name]
            task.completed += 1
            if task.completed >= task.total:
                task.end = now_timestamp()

            self.report.backfill.tasks[snapshot.name] = task
            self.report.backfill.queue.remove(snapshot.name)

        self.log_event_apply()

    def stop_evaluation_progress(self, success: bool = True) -> None:
        if self.report and self.report.backfill:
            self.report.backfill.queue.clear()
            self.report.backfill.tasks = {}
            self.report.backfill.stop(success=success)

            if not success:
                self.log_stop_evaluation()

    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        if self.report:
            self.report.add(
                models.ReportStagePlanApply.promote,
                models.ReportStagePlanApplyPromote(
                    total_tasks=total_tasks, num_tasks=0, target_environment=environment
                ),
            )

        self.log_event_apply()

    def update_promotion_progress(self, num_tasks: int) -> None:
        if self.report and self.report.promote:
            self.report.promote.update(
                {"num_tasks": self.report.promote.num_tasks + num_tasks})

        self.log_event_apply()

    def stop_promotion_progress(self, success: bool = True) -> None:
        if self.report and self.report.promote:
            self.report.promote.stop(success=success)

            if not success:
                self.log_stop_evaluation()

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
                successful=result.testsRun -
                len(result.failures) - len(result.errors),
                dialect=target_dialect,
                details=messages,
                traceback=output,
            ).dict(),
        )

    def log_event_apply(self) -> None:
        self.log_event(
            event=models.ConsoleEvent.report_plan_apply,
            data=self.report.dict() if self.report else {},
        )

    def log_exception(self) -> None:
        self.log_event(
            event="errors",
            data=ApiException(
                message="Tasks failed to a run",
                origin="API -> console -> log_exception",
            ).to_dict(),
        )


api_console = ApiConsole()
