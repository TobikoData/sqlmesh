from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from sqlmesh.core.console import TerminalConsole
from sqlmesh.utils.date import now_timestamp
from web.server.sse import Event


class ApiConsole(TerminalConsole):
    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, int]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def _make_event(self, data: str | dict[str, t.Any], event: str | None = None) -> Event:
        payload: dict[str, t.Any] = {
            "ok": True,
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

    def complete_snapshot_progress(self) -> None:
        """Indicates that load progress is complete"""
        self.queue.put_nowait(self._make_event("All model batches have been executed successfully"))
        self.stop_snapshot_progress()

    def stop_snapshot_progress(self) -> None:
        """Stop the load progress"""
        self.previous_task_status = self.current_task_status.copy()
        self.current_task_status = {}

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        self.queue.put_nowait(
            self._make_event(
                f"Successfully ran {str(result.testsRun)} tests against {target_dialect}",
                event="tests",
            )
        )

    def log_success(self, msg: str) -> None:
        self.queue.put_nowait(self._make_event(msg))
        self.stop_snapshot_progress()
