from __future__ import annotations

import typing as t

from fastapi import APIRouter, Body, Depends, Request

from sqlmesh.core.context import Context
from sqlmesh.core.plan.definition import Plan
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context

router = APIRouter()


@router.post(
    "",
    response_model=models.PlanOverviewStageTracker,
    response_model_exclude_unset=True,
)
async def run_plan(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
) -> models.PlanOverviewStageTracker:
    """Get a plan for an environment."""

    plan_options = plan_options or models.PlanOptions()

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> run_plan",
        )

    tracker, _ = get_plan_tracker(
        context=context,
        environment=environment,
        plan_dates=plan_dates,
        plan_options=plan_options,
    )

    return tracker


@router.post(
    "/cancel",
    response_model=models.PlanCancelStageTracker,
    response_model_exclude_unset=True,
)
async def cancel_plan(
    request: Request,
) -> models.PlanCancelStageTracker:
    """Cancel a plan application"""
    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> cancel_plan",
        )
    request.app.state.task.cancel()
    tracker = models.PlanCancelStageTracker()
    api_console.start_plan_tracker(tracker)
    tracker_stage_cancel = models.PlanStageCancel()
    tracker.add_stage(stage=models.PlanStage.cancel, data=tracker_stage_cancel)
    tracker_stage_cancel.stop(success=True)
    api_console.stop_plan_tracker(tracker)

    return tracker


def get_plan_tracker(
    plan_options: models.PlanOptions,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
) -> t.Tuple[models.PlanOverviewStageTracker, Plan]:
    tracker = models.PlanOverviewStageTracker(environment=environment, plan_options=plan_options)
    api_console.start_plan_tracker(tracker)
    tracker_stage_validate = models.PlanStageValidation()
    tracker.add_stage(stage=models.PlanStage.validation, data=tracker_stage_validate)
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
        tracker.start = plan.start
        tracker.end = plan.end
        tracker_stage_validate.stop(success=True)
    except Exception:
        tracker_stage_validate.stop(success=False)
        tracker.stop(success=False)
        api_console.log_event_plan_overview()
        raise ApiException(
            message="Unable to run a plan",
            origin="API -> plan -> run_plan",
        )

    tracker_stage_changes = models.PlanStageChanges()
    tracker.add_stage(stage=models.PlanStage.changes, data=tracker_stage_changes)
    if plan.context_diff.has_changes:
        tracker_stage_changes.update(
            {
                "removed": set(plan.context_diff.removed_snapshots),
                "added": plan.context_diff.added,
                "modified": models.ModelsDiff.get_modified_snapshots(plan.context_diff),
            }
        )
    tracker_stage_changes.stop(success=True)

    tracker_stage_backfills = models.PlanStageBackfills()
    tracker.add_stage(stage=models.PlanStage.backfills, data=tracker_stage_backfills)
    if plan.requires_backfill:
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

    api_console.stop_plan_tracker(tracker)

    return tracker, plan
