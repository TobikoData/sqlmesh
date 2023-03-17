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


@router.post("/apply", response_model=models.ApplyResponse)
async def apply(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: models.PlanOptions = models.PlanOptions(),
) -> models.ApplyResponse:
    """Apply a plan"""

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{request.app.state.task.get_name().capitalize()} is already running.",
        )

    plan_func = functools.partial(
        context.plan,
        environment=environment,
        no_prompts=True,
        start=plan_dates.start if plan_dates else None,
        end=plan_dates.end if plan_dates else None,
        skip_tests=plan_options.skip_tests,
        no_gaps=plan_options.no_gaps,
        restate_models=plan_options.restate_models,
        create_from=plan_options.create_from,
        skip_backfill=plan_options.skip_backfill,
        forward_only=plan_options.forward_only,
        no_auto_categorization=plan_options.no_auto_categorization,
    )
    request.app.state.task = asyncio.create_task(run_in_executor(plan_func))
    request.app.state.task.set_name("plan")
    plan = await request.app.state.task

    request.app.state.task = asyncio.create_task(run_in_executor(context.apply, plan))
    request.app.state.task.set_name("apply")
    if not plan.requires_backfill or plan_options.skip_backfill:
        await request.app.state.task

    return models.ApplyResponse(type="backfill" if plan.requires_backfill else "logical")


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


@router.post("/render", response_model=models.Query)
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
