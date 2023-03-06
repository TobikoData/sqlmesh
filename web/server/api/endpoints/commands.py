from __future__ import annotations

import asyncio
import functools
import json
import traceback
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from sqlmesh.utils.date import now_timestamp
from web.server import models
from web.server.console import TaskReport, TaskUpdateType
from web.server.settings import get_loaded_context
from web.server.sse import Event
from web.server.utils import (
    ArrowStreamingResponse,
    df_to_pyarrow_bytes,
    run_in_executor,
)

router = APIRouter()


@router.post("/apply")
async def apply(
    environment: str,
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> t.Any:
    """Apply a plan"""

    console: t.Any = context.console

    plan = plan = context.plan(environment=environment, no_prompts=True)
    apply = functools.partial(context.apply, plan)

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        task = asyncio.create_task(run_in_executor(apply))
        setattr(task, "_environment", environment)
        request.app.state.task = task
    else:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="An apply is already running."
        )

    if not plan.requires_backfill:
        print("No backfill required")
        task_logical: t.Dict[str, int] = {"start": 0, "end": 1000, "completed": 1, "total": 1}
        console.previous = TaskReport(
            start=now_timestamp(),
            type=TaskUpdateType.logical,
            end=now_timestamp() + 1000,
            tasks={name: task_logical for name in console.previous.tasks.keys()},
            completed=1,
            total=1,
            is_completed=True,
        )
        console.queue.put_nowait(
            Event(
                event="tasks",
                data=json.dumps(dict(console.previous)),
            )
        )

    return {"ok": True}


@router.post("/evaluate")
async def evaluate(
    options: models.EvaluateInput,
    context: Context = Depends(get_loaded_context),
) -> ArrowStreamingResponse:
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
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    return ArrowStreamingResponse(df_to_pyarrow_bytes(df))


@router.post("/fetchdf")
async def fetchdf(
    sql: str = Body(embed=True),
    context: Context = Depends(get_loaded_context),
) -> ArrowStreamingResponse:
    """Fetches a dataframe given a sql string"""
    try:
        df = context.fetchdf(sql)
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )
    return ArrowStreamingResponse(df_to_pyarrow_bytes(df))


@router.get("/dag")
async def dag(
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Set[str]]:
    try:
        return context.dag.graph
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )


@router.post("/render")
async def render(
    options: models.RenderInput,
    context: Context = Depends(get_loaded_context),
) -> models.Query:
    """Renders a model's query, optionally expanding referenced models"""
    snapshot = context.snapshots.get(options.model)

    if not snapshot:
        raise HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Model not found.")

    rendered = context.render(
        snapshot,
        start=options.start,
        end=options.end,
        latest=options.latest,
        expand=options.expand,
    )
    dialect = options.dialect or context.dialect
    return models.Query(sql=rendered.sql(pretty=options.pretty, dialect=dialect))
