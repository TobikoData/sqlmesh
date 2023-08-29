from __future__ import annotations

import typing as t

from fastapi import APIRouter, Body, Depends, Request, Response, status

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context

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

    try:
        plan = context.plan(
            environment=environment,
            no_prompts=True,
            include_unmodified=True,
            start=plan_dates.start if plan_dates else None,
            end=plan_dates.end if plan_dates else None,
            create_from=plan_options.create_from,
            skip_tests=plan_options.skip_tests,
            restate_models=plan_options.restate_models,
            no_gaps=plan_options.no_gaps,
            skip_backfill=plan_options.skip_backfill,
            forward_only=plan_options.forward_only,
            no_auto_categorization=plan_options.no_auto_categorization,
        )
    except Exception:
        raise ApiException(
            message="Unable to run a plan",
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
) -> None:
    """Cancel a plan application"""
    if not hasattr(request.app.state, "task") or not request.app.state.task.cancel():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> cancel_plan",
        )
    response.status_code = status.HTTP_204_NO_CONTENT
