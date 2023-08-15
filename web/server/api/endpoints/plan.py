from __future__ import annotations

import asyncio
import functools
import typing as t

from fastapi import APIRouter, Body, Depends, Request, Response, status

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from sqlmesh.utils.errors import PlanError
from web.server import models
from web.server.exceptions import ApiException
from web.server.settings import get_context, get_loaded_context
from web.server.utils import run_in_executor

router = APIRouter()


@router.post(
    "",
    response_model=models.ContextEnvironment,
    response_model_exclude_unset=True,
)
async def run_plan(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: models.PlanOptions = models.PlanOptions(),
) -> models.ContextEnvironment:
    """Get a plan for an environment."""

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> run_plan",
        )

    plan_func = functools.partial(
        context.plan,
        no_prompts=True,
        skip_tests=True,
        environment=environment,
        start=plan_dates.start if plan_dates else None,
        end=plan_dates.end if plan_dates else None,
        create_from=plan_options.create_from,
        restate_models=plan_options.restate_models,
        no_gaps=plan_options.no_gaps,
        skip_backfill=plan_options.skip_backfill,
        forward_only=plan_options.forward_only,
        no_auto_categorization=plan_options.no_auto_categorization,
    )
    request.app.state.task = asyncio.create_task(run_in_executor(plan_func))
    try:
        plan = await request.app.state.task
    except PlanError as e:
        raise ApiException(
            message=str(e),
            origin="API -> plan -> run_plan",
        )

    payload = models.ContextEnvironment(
        environment=plan.environment.name,
        start=plan.start,
        end=plan.end,
    )

    if plan.context_diff.has_changes or plan.requires_backfill:
        batches = context.scheduler().batches()
        tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches.items()}

        payload.backfills = [
            models.ContextEnvironmentBackfill(
                model_name=interval.snapshot_name,
                view_name=plan.context_diff.snapshots[
                    interval.snapshot_name
                ].qualified_view_name.for_environment(plan.environment.naming_info)
                if interval.snapshot_name in plan.context_diff.snapshots
                else interval.snapshot_name,
                interval=[
                    tuple(to_ds(t) for t in make_inclusive(start, end))
                    for start, end in interval.merged_intervals
                ][0],
                batches=tasks.get(interval.snapshot_name, 0),
            )
            for interval in plan.missing_intervals
        ]

        payload.changes = models.ContextEnvironmentChanges(
            removed=set(plan.context_diff.removed_snapshots),
            added=plan.context_diff.added,
            modified=models.ModelsDiff.get_modified_snapshots(plan.context_diff),
        )

    return payload


@router.post("/cancel")
async def cancel_plan(
    request: Request,
    response: Response,
    context: Context = Depends(get_context),
) -> None:
    """Cancel a plan application"""
    if not hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="No running plan to cancel",
            origin="API -> plan -> cancel_plan",
        )
    else:
        request.app.state.task.cancel()

    response.status_code = status.HTTP_204_NO_CONTENT
