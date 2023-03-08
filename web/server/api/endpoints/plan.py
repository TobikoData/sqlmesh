from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
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
) -> models.ContextEnvironment:
    """Get a plan for an environment."""

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        context.refresh()

        plan = context.plan(environment=environment, no_prompts=True)
        payload = models.ContextEnvironment(
            environment=plan.environment.name, start=plan.start, end=plan.end
        )

        if plan.context_diff.has_differences:
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
    else:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="An apply is already running."
        )


@router.post("/cancel")
async def cancel_plan(
    request: Request,
    response: Response,
    context: Context = Depends(get_loaded_context),
) -> None:
    """Cancel a plan application"""
    if not hasattr(request.app.state, "task") or not request.app.state.task.cancel():
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="No active task found."
        )
    response.status_code = status.HTTP_204_NO_CONTENT
