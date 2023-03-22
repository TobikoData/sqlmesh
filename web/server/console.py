from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.date import now_timestamp
from web.server.sse import Event


class ApiConsole(TerminalConsole):
    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, int]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def _make_event(
        self, data: str | dict[str, t.Any], event: str | None = None, ok: bool | None = None
    ) -> Event:
        payload: dict[str, t.Any] = {
            "ok": True if ok is None else ok,
            "timestamp": now_timestamp(),
        }
        if isinstance(data, str):
            payload["message"] = data
        else:
            payload.update(data)
        return Event(
            event=event,
            data=json.dumps(payload),
        )

    def start_snapshot_progress(self, snapshot_name: str, total_batches: int) -> None:
        """Indicates that a new load progress has begun."""
        self.current_task_status[snapshot_name] = {
            "completed": 0,
            "total": total_batches,
            "start": now_timestamp(),
        }

    def update_snapshot_progress(self, snapshot_name: str, num_batches: int) -> None:
        """Update snapshot progress."""
        if self.current_task_status:
            self.current_task_status[snapshot_name]["completed"] += num_batches
            if (
                self.current_task_status[snapshot_name]["completed"]
                >= self.current_task_status[snapshot_name]["total"]
            ):
                self.current_task_status[snapshot_name]["end"] = now_timestamp()
            self.queue.put_nowait(
                self._make_event({"tasks": self.current_task_status}, event="tasks")
            )

    def stop_snapshot_progress(self, success: bool = True) -> None:
        """Stop the load progress"""
        self.current_task_status = {}
        if success:
            self.queue.put_nowait(
                self._make_event("All model batches have been executed successfully")
            )

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        ok = True
        if result.wasSuccessful():
            data = f"Successfully ran {str(result.testsRun)} tests against {target_dialect}"
        else:
            messages = ["Test Failure Summary"]
            messages.append(
                f"Num Successful Tests: {result.testsRun - len(result.failures) - len(result.errors)}"
            )
            for test, _ in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    messages.append(f"Failure Test: {test.model_name} {test.test_name}")
            messages.append(output)
            data = "\n".join(messages)
            ok = False
        self.queue.put_nowait(self._make_event(data, event="tests", ok=ok))

    def log_success(self, msg: str) -> None:
        self.queue.put_nowait(self._make_event(msg))
        self.stop_snapshot_progress()
