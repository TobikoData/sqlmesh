from __future__ import annotations

import asyncio
import io
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, Request

from sqlmesh.core.context import Context
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.errors import PlanError
from web.server import models
from web.server.api.endpoints.plan import get_plan_tracker
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context
from web.server.utils import (
    ArrowStreamingResponse,
    df_to_pyarrow_bytes,
    run_in_executor,
)

router = APIRouter()


@router.post(
    "/apply", response_model=models.PlanApplyStageTracker, response_model_exclude_unset=True
)
async def apply(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> models.PlanApplyStageTracker:
    """Apply a plan"""
    plan_options = plan_options or models.PlanOptions()
    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> commands -> apply",
        )
    _, plan = get_plan_tracker(
        context=context, environment=environment, plan_options=plan_options, plan_dates=plan_dates
    )

    if categories is not None:
        for new, _ in plan.context_diff.modified_snapshots.values():
            if plan.is_new_snapshot(new) and new.name in categories:
                plan.set_choice(new, categories[new.name])

    tracker_apply = models.PlanApplyStageTracker(environment=environment, plan_options=plan_options)
    tracker_apply.start = plan.start
    tracker_apply.end = plan.end
    api_console.start_plan_tracker(tracker_apply)
    request.app.state.task = apply_task = asyncio.create_task(run_in_executor(context.apply, plan))
    if not plan.requires_backfill or plan_options.skip_backfill:
        try:
            await apply_task
            api_console.stop_plan_tracker(tracker_apply, success=True)
        except PlanError as e:
            api_console.stop_plan_tracker(tracker_apply, success=False)
            raise ApiException(
                message=str(e),
                origin="API -> commands -> apply",
            )

    return tracker_apply


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
            execution_time=options.execution_time,
            limit=options.limit,
        )
    except Exception:
        raise ApiException(
            message="Unable to evaluate a model",
            origin="API -> commands -> evaluate",
        )

    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    return ArrowStreamingResponse(df_to_pyarrow_bytes(df))


@router.post("/fetchdf")
async def fetchdf(
    options: models.FetchdfInput,
    context: Context = Depends(get_loaded_context),
) -> ArrowStreamingResponse:
    """Fetches a dataframe given a sql string"""
    try:
        df = context.fetchdf(options.sql)
    except Exception:
        raise ApiException(
            message="Unable to fetch a dataframe from the given sql string",
            origin="API -> commands -> fetchdf",
        )
    return ArrowStreamingResponse(df_to_pyarrow_bytes(df))


@router.post("/render", response_model=models.Query)
async def render(
    options: models.RenderInput,
    context: Context = Depends(get_loaded_context),
) -> models.Query:
    """Renders a model's query, optionally expanding referenced models"""
    snapshot = context.snapshots.get(options.model)

    if not snapshot:
        raise ApiException(
            message="Unable to find a model",
            origin="API -> commands -> render",
        )

    try:
        rendered = context.render(
            snapshot,
            start=options.start,
            end=options.end,
            execution_time=options.execution_time,
            expand=options.expand,
        )
    except Exception:
        raise ApiException(
            message="Unable to render a model query",
            origin="API -> commands -> render",
        )

    dialect = options.dialect or context.config.dialect

    return models.Query(sql=rendered.sql(pretty=options.pretty, dialect=dialect))


@router.get("/test")
async def test(
    test: t.Optional[str] = None,
    verbose: bool = False,
    context: Context = Depends(get_loaded_context),
) -> models.TestResult:
    """Run one or all model tests"""
    test_output = io.StringIO()
    try:
        result = context.test(
            tests=[str(context.path / test)] if test else None, verbose=verbose, stream=test_output
        )
    except Exception:
        raise ApiException(
            message="Unable to run tests",
            origin="API -> commands -> test",
        )
    context.console.log_test_results(
        result, test_output.getvalue(), context._test_engine_adapter.dialect
    )
    return models.TestResult(
        errors=[
            models.TestErrorOrFailure(
                name=test.test_name,
                path=test.path_relative_to(context.path),
                tb=tb,
            )
            for test, tb in ((t.cast(ModelTest, test), tb) for test, tb in result.errors)
        ],
        failures=[
            models.TestErrorOrFailure(
                name=test.test_name,
                path=test.path_relative_to(context.path),
                tb=tb,
            )
            for test, tb in ((t.cast(ModelTest, test), tb) for test, tb in result.failures)
        ],
        skipped=[
            models.TestSkipped(
                name=test.test_name,
                path=test.path_relative_to(context.path),
                reason=reason,
            )
            for test, reason in (
                (t.cast(ModelTest, test), reason) for test, reason in result.skipped
            )
        ],
        tests_run=result.testsRun,
    )
