from __future__ import annotations

import typing as t

from fastapi import APIRouter, Body, Depends, Request, Response, status

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.console import ApiConsole
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context

router = APIRouter()


@router.post(
    "",
    response_model=models.ReportProgressPlan,
    response_model_exclude_unset=True,
)
async def run_plan(
    request: Request,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: models.PlanOptions = models.PlanOptions(),
) -> models.ReportProgressPlan:
    """Get a plan for an environment."""

    if hasattr(request.app.state, "task") and not request.app.state.task.done():
        raise ApiException(
            message="Plan/apply is already running",
            origin="API -> plan -> run_plan",
        )

    console: ApiConsole = context.console  # type: ignore
    report = models.ReportProgressPlan(
        environment=environment, meta={"skip_tests": plan_options.skip_tests}
    )
    console.log_event(event=models.ConsoleEvent.report_plan, data=report.dict())
    report_stage_validate = models.ReportPlanStageValidation()
    report.add(stage=models.ReportPlanStage.validation, data=report_stage_validate)
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
        report_stage_validate.update({"start": plan.start, "end": plan.end})
        report_stage_validate.stop(success=True)
    except Exception:
        report_stage_validate.stop(success=False)
        report.stop(success=False)
        console.log_event(event=models.ConsoleEvent.report_plan, data=report.dict())
        raise ApiException(
            message="Unable to run a plan",
            origin="API -> plan -> run_plan",
        )

    if plan.context_diff.has_changes:
        report_stage_changes = models.ReportPlanStageChanges()
        report.add(stage=models.ReportPlanStage.changes, data=report_stage_changes)
        report_stage_changes.update(
            {
                "removed": set(plan.context_diff.removed_snapshots),
                "added": plan.context_diff.added,
                "modified": models.ModelsDiff.get_modified_snapshots(plan.context_diff),
            }
        )
        report_stage_changes.stop(success=True)

    if plan.requires_backfill:
        report_stage_backfills = models.ReportPlanStageBackfills()
        report.add(stage=models.ReportPlanStage.backfills, data=report_stage_backfills)
        batches = context.scheduler().batches()
        tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches.items()}
        report_stage_backfills.update(
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
        report_stage_backfills.stop(success=True)

    report.stop(success=True)
    console.log_event(event=models.ConsoleEvent.report_plan, data=report.dict())
    return report


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
