from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from sse_starlette.sse import ServerSentEvent

from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.date import now_timestamp
from web.server.exceptions import ApiException


class ApiConsole(TerminalConsole):
    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, t.Any]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def _make_event(
        self, data: str | dict[str, t.Any], event: str | None = None, ok: bool | None = None
    ) -> ServerSentEvent:
        payload: dict[str, t.Any] = {
            "ok": True if ok is None else ok,
            "timestamp": now_timestamp(),
        }
        if isinstance(data, str):
            payload["message"] = data
        else:
            payload.update(data)
        return ServerSentEvent(
            event=event,
            data=json.dumps(payload),
        )

    def start_evaluation_progress(self, batches: t.Dict[Snapshot, int], environment: str) -> None:
        self.current_task_status = {
            snapshot.name: {
                "completed": 0,
                "total": total_tasks,
                "start": now_timestamp(),
                "view_name": snapshot.qualified_view_name.for_environment(environment),
            }
            for snapshot, total_tasks in batches.items()
        }

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        pass

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, num_batches: int) -> None:
        """Update snapshot evaluation progress."""
        if self.current_task_status:
            self.current_task_status[snapshot.name]["completed"] += num_batches
            if (
                self.current_task_status[snapshot.name]["completed"]
                >= self.current_task_status[snapshot.name]["total"]
            ):
                self.current_task_status[snapshot.name]["end"] = now_timestamp()
            self.queue.put_nowait(
                self._make_event({"tasks": self.current_task_status}, event="tasks")
            )

    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Stop the snapshot evaluation progress."""
        self.current_task_status = {}
        if success:
            self.queue.put_nowait(
                self._make_event("All model batches have been executed successfully")
            )

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        ok = True
        data: str | t.Dict[str, t.Any]
        if result.wasSuccessful():
            data = f"Successfully ran {str(result.testsRun)} tests against {target_dialect}"
        else:
            messages = []
            for test, details in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    messages.append(
                        {
                            "message": f"Failure test: {test.model.name} {test.test_name}",
                            "details": details,
                        }
                    )
            data = {
                "title": "Test Failure Summary",
                "total": result.testsRun,
                "failures": len(result.failures),
                "errors": len(result.errors),
                "successful": result.testsRun - len(result.failures) - len(result.errors),
                "dialect": target_dialect,
                "details": messages,
                "traceback": output,
            }
            ok = False
        self.queue.put_nowait(self._make_event(data, event="tests", ok=ok))

    def log_success(self, msg: str) -> None:
        self.queue.put_nowait(self._make_event(msg))

    def stop_promotion_progress(self, success: bool = True) -> None:
        self.promotion_task = None
        if self.promotion_progress is not None:
            self.promotion_progress.stop()
            self.promotion_progress = None
            if success:
                self.queue.put_nowait(
                    self._make_event(
                        "The target environment has been updated successfully",
                        event="promote-environment",
                    )
                )

    def log_exception(self) -> None:
        self.queue.put_nowait(
            self._make_event(
                event="errors",
                data=ApiException(
                    message="Tasks failed to a run",
                    origin="API -> console -> log_exception",
                ).to_dict(),
            )
        )


api_console = ApiConsole()
