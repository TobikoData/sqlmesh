from __future__ import annotations

import asyncio
import json
import typing as t
import unittest

from fastapi.encoders import jsonable_encoder
from sse_starlette.sse import ServerSentEvent

from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.date import now_timestamp
from web.server.exceptions import ApiException


class ApiConsole(TerminalConsole):
    task: t.Optional[asyncio.Task] = None
    payload: t.Dict[str, t.Any] = {}

    def __init__(self) -> None:
        super().__init__()
        self.current_task_status: t.Dict[str, t.Dict[str, t.Any]] = {}
        self.queue: asyncio.Queue = asyncio.Queue()

    def start_evaluation(self, environment: str) -> None:
        self.payload = {}
        self.payload["environment"] = environment
        self.payload["evaluation"] = {
            "start_at": now_timestamp(),
        }
        self.log(event="apply", data=self.payload)

    def stop_evaluation(self) -> None:
        self.payload["evaluation"]["stop_at"] = now_timestamp()
        self.log(event="apply", data=self.payload)
        self.payload = {}

    def start_creation_progress(self, total_tasks: int) -> None:
        self.payload["creation"] = {
            "status": "init",
            "start_at": now_timestamp(),
            "total_tasks": total_tasks,
            "num_tasks": 0,
        }
        self.log(event="apply", data=self.payload)

    def update_creation_progress(self, num_tasks: int) -> None:
        if "creation" in self.payload:
            self.payload["creation"]["num_tasks"] = (
                self.payload["creation"]["num_tasks"] + num_tasks
            )
        self.log(event="apply", data=self.payload)

    def stop_creation_progress(self, success: bool = True) -> None:
        if "creation" in self.payload:
            self.payload["creation"]["stop_at"] = now_timestamp()

            if success:
                self.payload["creation"]["status"] = "success"
                self.log(event="apply", data=self.payload)
            else:
                self.payload["creation"]["status"] = "fail"
                self.log(event="apply", data=self.payload)
                self.stop_evaluation()

    def start_restate_progress(self) -> None:
        self.payload["restate"] = {"status": "init", "start_at": now_timestamp()}
        self.log(event="apply", data=self.payload)

    def stop_restate_progress(self) -> None:
        """Stop the restate progress."""
        self.payload["restate"]["stop_at"] = now_timestamp()
        self.payload["restate"]["status"] = "success"
        self.log(event="apply", data=self.payload)

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        self.payload["backfill"] = {
            "status": "init",
            "start_at": now_timestamp(),
            "queue": set(),
            "tasks": {
                snapshot.name: {
                    "completed": 0,
                    "total": total_tasks,
                    "start": now_timestamp(),
                    "view_name": snapshot.qualified_view_name.for_environment(
                        environment_naming_info
                    ),
                }
                for snapshot, total_tasks in batches.items()
            },
        }
        self.log(event="apply", data=self.payload)

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if "backfill" in self.payload:
            self.payload["backfill"]["queue"].add(snapshot.name)
            self.log(event="apply", data=self.payload)

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, num_tasks: int) -> None:
        """Update snapshot evaluation progress."""
        if "backfill" in self.payload:
            self.payload["backfill"]["tasks"][snapshot.name]["completed"] += num_tasks
            if (
                self.payload["backfill"]["tasks"][snapshot.name]["completed"]
                >= self.payload["backfill"]["tasks"][snapshot.name]["total"]
            ):
                self.payload["backfill"]["tasks"][snapshot.name]["end"] = now_timestamp()
                self.payload["backfill"]["queue"].remove(snapshot.name)
            self.log(event="apply", data=self.payload)

    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Stop the snapshot evaluation progress."""
        if "backfill" in self.payload:
            self.payload["backfill"]["queue"] == set()
            self.payload["backfill"]["stop_at"] = now_timestamp()

            if success:
                self.payload["backfill"]["status"] = "success"
                self.log(event="apply", data=self.payload)
            else:
                self.payload["backfill"]["status"] = "fail"
                self.log(event="apply", data=self.payload)
                self.stop_evaluation()

    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        self.payload["promotion"] = {
            "status": "init",
            "start_at": now_timestamp(),
            "total_tasks": total_tasks,
            "num_tasks": 0,
            "target_environment": environment,
        }
        self.log(event="apply", data=self.payload)

    def update_promotion_progress(self, num_tasks: int) -> None:
        """Update snapshot promotion progress."""
        if "promotion" in self.payload:
            self.payload["promotion"]["num_tasks"] = (
                self.payload["promotion"]["num_tasks"] + num_tasks
            )
        self.log(event="apply", data=self.payload)

    def stop_promotion_progress(self, success: bool = True) -> None:
        if "promotion" in self.payload:
            self.payload["promotion"]["stop_at"] = now_timestamp()
            if success:
                self.payload["promotion"]["status"] = "success"
                self.log(event="apply", data=self.payload)
            else:
                self.payload["promotion"]["status"] = "fail"
                self.log(event="apply", data=self.payload)
                self.stop_evaluation()

    def _make_event(self, data: dict[str, t.Any], event: str) -> ServerSentEvent:

        return ServerSentEvent(
            event=event,
            data=json.dumps(jsonable_encoder(data)),
        )

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        data: t.Dict[str, t.Any]
        if result.wasSuccessful():
            data = {
                "message": f"Successfully ran {str(result.testsRun)} tests against {target_dialect}"
            }
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
        self.queue.put_nowait(self._make_event(data, event="tests"))

    def log(self, event: str, data: dict[str, t.Any]) -> None:
        self.queue.put_nowait(self._make_event(data=data, event=event))

    def log_event_apply(self) -> None:
        self.queue.put_nowait(self._make_event(data=self.payload, event="apply"))

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
