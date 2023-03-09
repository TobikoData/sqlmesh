from __future__ import annotations

import typing as t

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from sqlmesh.utils.date import TimeLike, make_inclusive, to_ds
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get(
    "",
    response_model=models.ContextEnvironment,
    response_model_exclude_unset=True,
)
def get_plan(
    environment: str,
    request: Request,
    context: Context = Depends(get_loaded_context),
    skip_tests: bool = False,
    no_gaps: bool = False,
    skip_backfill: bool = False,
    forward_only: bool = False,
    auto_apply: bool = False,
    no_auto_categorization: bool = False,
    start: t.Optional[TimeLike] = None,
    end: t.Optional[TimeLike] = None,
    from_: t.Optional[str] = None,
    restate_models: t.Optional[str] = None,
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
        start=start,
        end=end,
        from_=from_,
        skip_tests=skip_tests,
        restate_models=restate_models,
        no_gaps=no_gaps,
        skip_backfill=skip_backfill,
        forward_only=forward_only,
        no_auto_categorization=no_auto_categorization,
        auto_apply=auto_apply,
    )

    payload = models.ContextEnvironment(
        environment=plan.environment.name,
        start=plan.start,
        end=plan.end,
    )

    if plan.context_diff.has_differences:
        batches = context.scheduler().batches()
        tasks = {snapshot.name: len(intervals)
                 for snapshot, intervals in batches.items()}

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
            modified=models.ModelsDiff.get_modified_snapshots(
                plan.context_diff),
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
