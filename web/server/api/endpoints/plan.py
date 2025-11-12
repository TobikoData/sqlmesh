from __future__ import annotations

import asyncio
import typing as t

from fastapi import APIRouter, Body, Depends, Request, Response
from starlette.status import HTTP_204_NO_CONTENT

from sqlmesh.core.context import Context
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan import Plan, PlanBuilder
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context
from web.server.utils import run_in_executor

router = APIRouter()


@router.post("", response_model=t.Optional[models.PlanOverviewStageTracker])
async def initiate_plan(
    request: Request,
    response: Response,
    context: Context = Depends(get_loaded_context),
    environment: t.Optional[str] = Body(None),
    plan_dates: t.Optional[models.PlanDates] = None,
    plan_options: t.Optional[models.PlanOptions] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> t.Optional[models.PlanOverviewStageTracker]:
    """Get a plan for an environment."""
    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        plan_options = plan_options or models.PlanOptions()
        request.app.state.task = asyncio.create_task(
            run_in_executor(
                get_plan_builder, context, plan_options, environment, plan_dates, categories
            )
        )
    else:
        api_console.log_event_plan_overview()
        api_console.log_event_plan_apply()

    response.status_code = HTTP_204_NO_CONTENT

    return None


@router.post("/cancel", response_model=t.Optional[models.PlanCancelStageTracker])
async def cancel_plan(
    request: Request,
    response: Response,
) -> t.Optional[models.PlanCancelStageTracker]:
    """Cancel a plan application"""
    tracker = models.PlanCancelStageTracker()
    api_console.start_plan_tracker(tracker)
    tracker_stage_cancel = models.PlanStageCancel()
    tracker.add_stage(stage=models.PlanStage.cancel, data=tracker_stage_cancel)

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        tracker_stage_cancel.stop(success=False)
        api_console.stop_plan_tracker(tracker)
    elif not request.app.state.task.cancelled():
        request.app.state.circuit_breaker.set()
        request.app.state.task.cancel()

        try:
            await request.app.state.task
        except asyncio.CancelledError:
            pass

    response.status_code = HTTP_204_NO_CONTENT

    return None


def get_plan_builder(
    context: Context,
    plan_options: models.PlanOptions,
    environment: t.Optional[str] = None,
    plan_dates: t.Optional[models.PlanDates] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> PlanBuilder:
    try:
        return _get_plan_builder(context, plan_options, environment, plan_dates, categories)
    except ApiException as e:
        raise e
    except Exception as e:
        raise ApiException(
            message="Unable to run a plan",
            origin="API -> plan -> initiate_plan",
        ) from e


def _get_plan_changes(context: Context, plan: Plan) -> models.PlanChanges:
    """Get plan changes"""
    snapshots = plan.context_diff.snapshots
    default_catalog = context.default_catalog
    environment_naming_info = plan.environment_naming_info

    return models.PlanChanges(
        added=[
            models.ChangeDisplay(
                name=snapshot_id.name,
                view_name=models.ChangeDisplay.get_view_name(
                    snapshots,
                    snapshot_id,
                    environment_naming_info,
                    default_catalog,
                ),
                node_type=models.ChangeDisplay.get_node_type(snapshots, snapshot_id),
            )
            for snapshot_id in plan.context_diff.added
        ],
        removed=[
            models.ChangeDisplay(
                name=snapshot_id.name,
                view_name=models.ChangeDisplay.get_view_name(
                    snapshots,
                    snapshot_id,
                    environment_naming_info,
                    default_catalog,
                ),
                node_type=snapshot_table_info.node_type,
            )
            for snapshot_id, snapshot_table_info in plan.context_diff.removed_snapshots.items()
        ],
        modified=models.ModelsDiff.get_modified_snapshots(context, plan),
    )


def _get_plan_backfills(context: Context, plan: Plan) -> t.Dict[str, t.Any]:
    """Get plan backfills"""
    merged_intervals = context.scheduler().merged_missing_intervals()
    batches = context.scheduler().batch_intervals(merged_intervals, None, EnvironmentNamingInfo())
    tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches.items()}
    snapshots = plan.context_diff.snapshots
    default_catalog = context.default_catalog

    return {
        "models": [
            models.BackfillDetails(
                name=interval.snapshot_id.name,
                view_name=models.ChangeDisplay.get_view_name(
                    snapshots,
                    interval.snapshot_id,
                    plan.environment_naming_info,
                    default_catalog,
                ),
                interval=[
                    tuple(to_ds(t) for t in make_inclusive(start, end))
                    for start, end in interval.merged_intervals
                ][0],
                batches=tasks.get(interval.snapshot_id.name, 0),
            )
            for interval in plan.missing_intervals
        ]
    }


def _get_plan_builder(
    context: Context,
    plan_options: models.PlanOptions,
    environment: t.Optional[str] = None,
    plan_dates: t.Optional[models.PlanDates] = None,
    categories: t.Optional[t.Dict[str, SnapshotChangeCategory]] = None,
) -> PlanBuilder:
    tracker = models.PlanOverviewStageTracker(environment=environment, plan_options=plan_options)
    api_console.start_plan_tracker(tracker)
    tracker_stage_validate = models.PlanStageValidation()
    tracker.add_stage(stage=models.PlanStage.validation, data=tracker_stage_validate)
    try:
        plan_builder = context.plan_builder(
            environment=environment,
            include_unmodified=plan_options.include_unmodified,
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
        plan = plan_builder.build()
        tracker.start = plan.start
        tracker.end = plan.end
        if categories:
            for new, _ in plan.context_diff.modified_snapshots.values():
                if plan.is_new_snapshot(new) and new.name in categories:
                    plan_builder.set_choice(new, categories[new.name])
            plan = plan_builder.build()
        tracker_stage_validate.stop(success=True)
        api_console.log_event_plan_overview()
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
    try:
        if plan.context_diff.has_changes:
            changes = _get_plan_changes(context, plan)
            tracker_stage_changes.update(
                {
                    "added": changes.added,
                    "removed": changes.removed,
                    "modified": changes.modified,
                }
            )
    except Exception:
        tracker_stage_changes.stop(success=False)
        tracker.stop(success=False)
        api_console.log_event_plan_overview()
        raise ApiException(
            message="Unable to get plan changes",
            origin="API -> plan -> run_plan",
        )
    tracker_stage_changes.stop(success=True)
    api_console.log_event_plan_overview()
    tracker_stage_backfills = models.PlanStageBackfills()
    tracker.add_stage(stage=models.PlanStage.backfills, data=tracker_stage_backfills)
    try:
        if plan.requires_backfill:
            tracker_stage_backfills.update(
                _get_plan_backfills(context, plan),
            )
    except Exception:
        tracker_stage_backfills.stop(success=False)
        tracker.stop(success=False)
        api_console.log_event_plan_overview()
        raise ApiException(
            message="Unable to get plan backfills",
            origin="API -> plan -> run_plan",
        )
    tracker_stage_backfills.stop(success=True)
    api_console.log_event_plan_overview()
    api_console.stop_plan_tracker(tracker)
    return plan_builder
