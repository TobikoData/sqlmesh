from __future__ import annotations

import asyncio
import functools
import io
import typing as t

import pandas as pd
from fastapi import APIRouter, Body, Depends, Request

from sqlmesh.core.context import Context
from sqlmesh.core.plan.definition import Plan
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.core.test import ModelTest
from sqlmesh.utils.errors import PlanError
from web.server import models
from web.server.console import ApiConsole
from web.server.exceptions import ApiException
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
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: models.PlanOptions = models.PlanOptions(),
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> models.ApplyResponse:
    """Apply a plan"""

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> commands -> apply",
        )

    """
        1 Run Plan
        2 Run Tests
            2.1 Run Audits?
        3 Push state 
        4 Restate
        5 Backfill
        6 Promote
        
    """
    console: ApiConsole = context.console  # type: ignore
    plan_func = functools.partial(
        context.plan,
        skip_tests=True,
        no_prompts=True,
        include_unmodified=plan_options.include_unmodified,
        environment=environment,
        start=plan_dates.start if plan_dates else None,
        end=plan_dates.end if plan_dates else None,
        no_gaps=plan_options.no_gaps,
        restate_models=plan_options.restate_models,
        create_from=plan_options.create_from,
        skip_backfill=plan_options.skip_backfill,
        forward_only=plan_options.forward_only,
        no_auto_categorization=plan_options.no_auto_categorization,
    )
    request.app.state.task = plan_task = asyncio.create_task(
        run_in_executor(plan_func))
    try:
        console.log(event="report", data={"type": "plan", "status": "init"})
        plan = await task
        console.log(event="report", data={"type": "plan", "status": "success"})
    except PlanError as e:
        console.log(event="report", data={"type": "plan", "status": "fail"})
        raise ApiException(
            message=str(e),
            origin="API -> commands -> apply",
        )

    if not plan_options.skip_tests:
        tests_func = functools.partial(context._run_tests)
        request.app.state.task = asyncio.create_task(
            run_in_executor(tests_func))
        try:
            console.log(event="report", data={
                        "type": "tests", "status": "init"})
            result, test_output = await request.app.state.task
            console.log(event="report", data={
                        "type": "tests", "status": "success"})
            if not result.wasSuccessful():
                console.log(event="report", data={
                            "type": "tests", "status": "fail"})
                console.log(
                    event="errors",
                    data=ApiException(
                        message="Cannot generate plan due to failing test(s). Fix test(s) and run again",
                        origin="API -> commands -> apply",
                    ).to_dict(),
                )
            console.log_test_results(
                result=result,
                output=test_output,
                target_dialect=context._test_engine_adapter.dialect,
            )
        except PlanError as e:
            console.log(event="report", data={
                        "type": "tests", "status": "fail"})
            raise ApiException(
                message=str(e),
                origin="API -> commands -> apply",
            )
    else:
        console.log(event="report", data={"type": "tests", "status": "skip"})
    if categories is not None:
        _categorize(plan, categories)

    plan_evaluator = context._scheduler.create_plan_evaluator(
        context)  # type: ignore
    # tasks = (
    #     [plan_evaluator._push, plan_evaluator._restate, plan_evaluator._backfill, plan_evaluator._promote]
    #     if not has_paused_forward_only(plan.snapshots, plan.snapshots) or plan.is_dev
    #     else [plan_evaluator._push, plan_evaluator._restate, plan_evaluator._promote, plan_evaluator._backfill]
    # )
    push_func = functools.partial(
        plan_evaluator._push,  # type: ignore
        plan=plan,
    )
    request.app.state.task = asyncio.create_task(run_in_executor(push_func))
    try:
        console.log(event="report", data={"type": "push", "status": "init"})
        await request.app.state.task
        console.log(event="report", data={"type": "push", "status": "success"})
    except PlanError as e:
        console.log(event="report", data={"type": "push", "status": "fail"})
        raise ApiException(
            message=str(e),
            origin="API -> commands -> apply",
        )

    if plan_options.restate_models:
        restate_func = functools.partial(
            plan_evaluator._restate,  # type: ignore
            plan=plan,
        )
        request.app.state.task = asyncio.create_task(
            run_in_executor(restate_func))
        try:
            console.log(event="report", data={
                        "type": "restate", "status": "init"})
            await request.app.state.task
            console.log(event="report", data={
                        "type": "restate", "status": "success"})
        except PlanError as e:
            console.log(event="report", data={
                        "type": "restate", "status": "fail"})
            raise ApiException(
                message=str(e),
                origin="API -> commands -> apply",
            )
    else:
        console.log(event="report", data={"type": "restate", "status": "skip"})

    backfill_func = functools.partial(
        plan_evaluator._backfill,  # type: ignore
        plan=plan,
    )
    request.app.state.task = asyncio.create_task(
        run_in_executor(backfill_func))
    try:
        console.log(event="report", data={
                    "type": "backfill", "status": "init"})
        await request.app.state.task
        console.log(event="report", data={
                    "type": "backfill", "status": "success"})
    except PlanError as e:
        console.log(event="report", data={
                    "type": "backfill", "status": "fail"})
        raise ApiException(
            message=str(e),
            origin="API -> commands -> apply",
        )

    promote_func = functools.partial(
        plan_evaluator._promote,  # type: ignore
        plan=plan,
    )
    request.app.state.task = asyncio.create_task(run_in_executor(promote_func))
    try:
        console.log(event="report", data={"type": "promote", "status": "init"})
        await request.app.state.task
        console.log(event="report", data={
                    "type": "promote", "status": "success"})
    except PlanError as e:
        console.log(event="report", data={"type": "promote", "status": "fail"})
        raise ApiException(
            message=str(e),
            origin="API -> commands -> apply",
        )

    # request.app.state.task = asyncio.create_task(run_in_executor(context.apply, plan))
    # context.console.task = request.app.state.task
    if not plan.requires_backfill or plan_options.skip_backfill:
        try:
            await apply_task
        except PlanError as e:
            raise ApiException(
                message=str(e),
                origin="API -> commands -> apply",
            )

    return models.ApplyResponse(
        type=models.ApplyType.backfill if plan.requires_backfill else models.ApplyType.virtual
    )


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


def _categorize(plan: Plan, categories: t.Dict[str, SnapshotChangeCategory]) -> None:
    for new, _ in plan.context_diff.modified_snapshots.values():
        if plan.is_new_snapshot(new) and new.name in categories:
            plan.set_choice(new, categories[new.name])
