from __future__ import annotations

import typing as t

from fastapi import APIRouter, Body, Depends, Request, Response, status

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context

router = APIRouter()


@router.post(
    "",
    response_model=models.PlanApplyStageTracker,
    response_model_exclude_unset=True,
)
async def run_plan(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
) -> models.PlanApplyStageTracker:
    """Get a plan for an environment."""

    plan_options = plan_options or models.PlanOptions()

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> run_plan",
        )

    tracker = models.PlanApplyStageTracker(environment=environment, plan_options=plan_options)
    api_console.log_event(event=models.ConsoleEvent.plan_apply, data=tracker.dict())
    tracker_stage_validate = models.PlanApplyStageValidation()
    tracker.add_stage(stage=models.PlanApplyStage.validation, data=tracker_stage_validate)
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
        tracker_stage_validate.stop(success=True)
    except Exception:
        tracker_stage_validate.stop(success=False)
        tracker.stop(success=False)
        api_console.log_event(event=models.ConsoleEvent.plan_apply, data=tracker.dict())
        raise ApiException(
            message="Unable to run a plan",
            origin="API -> plan -> run_plan",
        )

    if plan.context_diff.has_changes:
        tracker_stage_changes = models.PlanApplyStageChanges()
        tracker.add_stage(stage=models.PlanApplyStage.changes, data=tracker_stage_changes)
        tracker_stage_changes.update(
            {
                "removed": set(plan.context_diff.removed_snapshots),
                "added": plan.context_diff.added,
                "modified": models.ModelsDiff.get_modified_snapshots(plan.context_diff),
            }
        )
        tracker_stage_changes.stop(success=True)

    if plan.requires_backfill:
        tracker_stage_backfills = models.PlanApplyStageBackfills()
        tracker.add_stage(stage=models.PlanApplyStage.backfills, data=tracker_stage_backfills)
        batches = context.scheduler().batches()
        tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches.items()}
        tracker_stage_backfills.update(
            {
                "models": [
                    models.BackfillDetails(
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
            }
        )
        tracker_stage_backfills.stop(success=True)

    tracker.stop(success=True)
    api_console.log_event(event=models.ConsoleEvent.plan_apply, data=tracker.dict())
    return tracker


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
