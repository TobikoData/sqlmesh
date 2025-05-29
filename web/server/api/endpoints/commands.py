from __future__ import annotations

import asyncio
import io
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, Request, Response
from starlette.status import HTTP_204_NO_CONTENT
from sqlmesh.core.console import Verbosity
from sqlmesh.core.context import Context
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.errors import PlanError
from web.server import models
from web.server.api.endpoints.plan import get_plan_builder
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context
from web.server.utils import (
    ArrowStreamingResponse,
    df_to_pyarrow_bytes,
    run_in_executor,
)
from pathlib import Path

router = APIRouter()


@router.post("/apply", response_model=t.Optional[models.PlanApplyStageTracker])
async def initiate_apply(
    request: Request,
    response: Response,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> t.Optional[models.PlanApplyStageTracker]:
    """Apply a plan"""
    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        request.app.state.circuit_breaker.clear()
        request.app.state.task = asyncio.create_task(
            run_in_executor(
                run_plan_apply,
                context,
                environment,
                plan_options,
                plan_dates,
                categories,
                request.app.state.circuit_breaker.is_set,
            )
        )
    else:
        api_console.log_event_plan_overview()
        api_console.log_event_plan_apply()

    response.status_code = HTTP_204_NO_CONTENT

    return None


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
    snapshot = context.get_snapshot(options.model, raise_if_missing=False)

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
    verbosity: Verbosity = Verbosity.DEFAULT,
    context: Context = Depends(get_loaded_context),
) -> models.TestResult:
    """Run one or all model tests"""
    test_output = io.StringIO()
    try:
        result = context.test(
            tests=[str(context.path / Path(test))] if test else None,
            verbosity=verbosity,
            stream=test_output,
        )
    except Exception:
        import traceback

        traceback.print_exc()
        raise ApiException(
            message="Unable to run tests",
            origin="API -> commands -> test",
        )
    context.console.log_test_results(
        result,
        test_output.getvalue(),
        context.test_connection_config._engine_adapter.DIALECT,
    )

    def _test_path(test: ModelTest) -> t.Optional[str]:
        if path := test.path_relative_to(context.path):
            return path.as_posix()
        return None

    return models.TestResult(
        errors=[
            models.TestErrorOrFailure(
                name=test.test_name,
                path=_test_path(test),
                tb=tb,
            )
            for test, tb in ((t.cast(ModelTest, test), tb) for test, tb in result.errors)
        ],
        failures=[
            models.TestErrorOrFailure(
                name=test.test_name,
                path=_test_path(test),
                tb=tb,
            )
            for test, tb in ((t.cast(ModelTest, test), tb) for test, tb in result.failures)
        ],
        skipped=[
            models.TestSkipped(
                name=test.test_name,
                path=_test_path(test),
                reason=reason,
            )
            for test, reason in (
                (t.cast(ModelTest, test), reason) for test, reason in result.skipped
            )
        ],
        successes=[
            models.TestCase(
                name=test.test_name,
                path=_test_path(test),
            )
            for test in (t.cast(ModelTest, test) for test in result.successes)
        ],
        tests_run=result.testsRun,
    )


def run_plan_apply(
    context: Context,
    environment: t.Optional[str] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
    plan_dates: t.Optional[models.PlanDates] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
    circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
) -> None:
    try:
        _run_plan_apply(
            context,
            environment,
            plan_options,
            plan_dates,
            categories,
            circuit_breaker,
        )
    except ApiException as e:
        raise e
    except Exception as e:
        raise ApiException(
            message="Unable to apply a plan",
            origin="API -> plan -> initiate_apply",
        ) from e
    return None


def _run_plan_apply(
    context: Context,
    environment: t.Optional[str] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
    plan_dates: t.Optional[models.PlanDates] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
    circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
) -> None:
    """Run plan apply"""
    plan_options = plan_options or models.PlanOptions()
    tracker_apply = models.PlanApplyStageTracker(environment=environment, plan_options=plan_options)
    api_console.start_plan_tracker(tracker_apply)
    plan_builder = get_plan_builder(context, plan_options, environment, plan_dates, categories)
    plan = plan_builder.build()
    tracker_apply.start = plan.start
    tracker_apply.end = plan.end
    api_console.start_plan_tracker(tracker_apply)
    try:
        context.apply(plan, circuit_breaker)
        if not plan.requires_backfill or plan_options.skip_backfill:
            api_console.stop_plan_tracker(tracker_apply, success=True)
    except PlanError as e:
        api_console.stop_plan_tracker(tracker_apply, success=False)
        raise ApiException(
            message=str(e),
            origin="API -> commands -> _run_plan_apply",
        )

    return None
