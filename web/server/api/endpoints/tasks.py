from __future__ import annotations

import asyncio
import json
import time
import typing as t

from fastapi import APIRouter, Depends, Request

from sqlmesh.core.context import Context
from web.server.console import ApiConsole
from web.server.settings import get_loaded_context
from web.server.sse import SSEResponse

SSE_DELAY = 1  # second
router = APIRouter()


@router.get("")
async def tasks(
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> SSEResponse:
    """Stream of plan application events"""
    task = None
    environment = None

    if hasattr(request.app.state, "task"):
        task = request.app.state.task
        environment = getattr(task, "_environment", None)

    def create_response(task_status: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        return {
            "data": json.dumps(
                {
                    "ok": True,
                    "environment": environment,
                    "tasks": task_status,
                    "timestamp": int(time.time()),
                }
            )
        }

    async def running_tasks() -> t.AsyncGenerator:
        console: ApiConsole = context.console  # type: ignore
        if task:
            while not task.done():
                task_status = console.current_task_status
                if task_status:
                    yield create_response(task_status)
                await asyncio.sleep(SSE_DELAY)
            task_status = console.previous_task_status
            if task_status:
                yield create_response(task_status)
        yield create_response({})

    return SSEResponse(running_tasks())
