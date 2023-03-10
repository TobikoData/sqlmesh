from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.post(
    "",
    response_model=models.ContextEnvironment,
    response_model_exclude_unset=True,
)
def get_plan(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: str = Body(),
    plan_dates: models.PlanDates = Body(None),
    additional_options: models.AdditionalOptions = Body(models.AdditionalOptions()),
) -> models.ContextEnvironment:
    """Get a plan for an environment."""

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="An apply is already running."
        )

    context.refresh()

    plan = context.plan(
        environment=environment,
        no_prompts=True,
        start=plan_dates.start if plan_dates else None,
        end=plan_dates.end if plan_dates else None,
        create_from=additional_options.create_from,
        skip_tests=additional_options.skip_tests,
        restate_models=additional_options.restate_models,
        no_gaps=additional_options.no_gaps,
        skip_backfill=additional_options.skip_backfill,
        forward_only=additional_options.forward_only,
        no_auto_categorization=additional_options.no_auto_categorization,
    )

    payload = models.ContextEnvironment(
        environment=plan.environment.name,
        start=plan.start,
        end=plan.end,
    )

    if plan.context_diff.has_changes:
        batches = context.scheduler().batches()
        tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches.items()}

        payload.backfills = [
            models.ContextEnvironmentBackfill(
                model_name=interval.snapshot_name,
                interval=[
                    [to_ds(t) for t in make_inclusive(start, end)]
                    for start, end in interval.merged_intervals
                ][0],
                batches=tasks[interval.snapshot_name],
            )
            for interval in plan.missing_intervals
        ]

        payload.changes = models.ContextEnvironmentChanges(
            removed=plan.context_diff.removed,
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
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="No active task found."
        )
    response.status_code = status.HTTP_204_NO_CONTENT
