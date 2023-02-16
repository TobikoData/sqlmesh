from __future__ import annotations

import asyncio
import functools
import json
import time
import traceback
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server import models
from web.server.console import ApiConsole
from web.server.settings import Settings, get_loaded_context, get_settings
from web.server.sse import SSEResponse
from web.server.utils import run_in_executor

SSE_DELAY = 1  # second
router = APIRouter()


@router.get(
    "/context",
    response_model=models.Context,
    response_model_exclude_unset=True,
)
def get_api_context(
    context: Context = Depends(get_loaded_context),
    settings: Settings = Depends(get_settings),
) -> models.Context:
    """Get the context"""

    context.refresh()

    return models.Context(
        concurrent_tasks=context.concurrent_tasks,
        engine_adapter=context.engine_adapter.dialect,
        scheduler=context.config.scheduler.type_,
        time_column_format=context.config.time_column_format,
        models=list(context.models),
        config=settings.config,
    )


@router.post("/apply")
async def apply(
    environment: str,
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> t.Any:
    """Apply a plan"""
    plan = functools.partial(context.plan, environment, no_prompts=True, auto_apply=True)

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        task = asyncio.create_task(run_in_executor(plan))
        setattr(task, "_environment", environment)
        request.app.state.task = task
    else:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="An apply is already running."
        )

    return {"ok": True}


@router.get("/tasks")
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


@router.post("/evaluate")
async def evaluate(
    options: models.EvaluateInput,
    context: Context = Depends(get_loaded_context),
) -> t.Optional[str]:
    """Evaluate a model with a default limit of 1000"""
    try:
        df = context.evaluate(
            options.model,
            start=options.start,
            end=options.end,
            latest=options.latest,
            limit=options.limit,
        )
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )
    if isinstance(df, pd.DataFrame):
        return df.to_json()
    return df.toPandas().to_json()


@router.post("/fetchdf")
async def fetchdf(
    sql: str = Body(embed=True),
    context: Context = Depends(get_loaded_context),
) -> t.Optional[str]:
    """Fetches a dataframe given a sql string"""
    try:
        return context.fetchdf(sql).to_json()
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )


@router.get("/dag")
async def dag(
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Any]:
    try:
        return dict(graph=context.dag.graph, sorted=context.dag.sorted())
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )
