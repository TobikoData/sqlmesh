from __future__ import annotations

import asyncio
import functools
import traceback
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server import models
from web.server.settings import get_loaded_context
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

    plan = plan = context.plan(environment=environment, no_prompts=True)
    apply = functools.partial(context.apply, plan)

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        if plan.requires_backfill:
            task = asyncio.create_task(run_in_executor(apply))
            request.app.state.task = task
        else:
            apply()

    else:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="An apply is already running."
        )

    return {"ok": True, "type": "backfill" if plan.requires_backfill else "logical"}


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
