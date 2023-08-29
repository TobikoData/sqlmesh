from __future__ import annotations

import typing as t

from sqlmesh.core import scheduler
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import SnapshotTableInfo
from sqlmesh.core.state_sync import StateSync
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now, now_timestamp
from sqlmesh.utils.errors import SQLMeshError


def create_plan_dag_spec(
    request: common.PlanApplicationRequest, state_sync: StateSync
) -> common.PlanDagSpec:
    new_snapshots = {s.snapshot_id: s for s in request.new_snapshots}
    stored_snapshots = state_sync.get_snapshots([*new_snapshots, *request.environment.snapshots])
    all_snapshots = {**new_snapshots, **stored_snapshots}

    duplicated_snapshots = set(stored_snapshots).intersection(new_snapshots)
    if duplicated_snapshots:
        raise SQLMeshError(
            f"Snapshots {duplicated_snapshots} already exist. "
            "Make sure your code base is up to date and try re-creating the plan"
        )

    if request.environment.end_at:
        end = request.environment.end_at
        unpaused_dt = None
    else:
        # Unbounded end date means we need to unpause all paused snapshots
        # that are part of the target environment.
        end = now()
        unpaused_dt = end

    if request.restatements:
        state_sync.remove_interval(
            [
                (s, request.restatements[s.name])
                for s in all_snapshots.values()
                if s.name in request.restatements and s.snapshot_id not in new_snapshots
            ],
            remove_shared_versions=not request.is_dev,
        )

    if not request.skip_backfill:
        backfill_batches = scheduler.compute_interval_params(
            all_snapshots.values(),
            start=request.environment.start_at,
            end=end,
            execution_time=now(),
            is_dev=request.is_dev,
            restatements=request.restatements,
            ignore_cron=True,
        )
    else:
        backfill_batches = {}

    backfill_intervals_per_snapshot = [
        common.BackfillIntervalsPerSnapshot(
            snapshot_id=snapshot.snapshot_id,
            intervals=intervals,
            before_promote=request.is_dev or not snapshot.is_paused_forward_only,
        )
        for snapshot, intervals in backfill_batches.items()
    ]

    return common.PlanDagSpec(
        request_id=request.request_id,
        environment_naming_info=request.environment.naming_info,
        new_snapshots=request.new_snapshots,
        backfill_intervals_per_snapshot=backfill_intervals_per_snapshot,
        promoted_snapshots=request.environment.snapshots,
        demoted_snapshots=_get_demoted_snapshots(request.environment, state_sync),
        start=request.environment.start_at,
        end=request.environment.end_at,
        unpaused_dt=unpaused_dt,
        no_gaps=request.no_gaps,
        plan_id=request.environment.plan_id,
        previous_plan_id=request.environment.previous_plan_id,
        notification_targets=request.notification_targets,
        backfill_concurrent_tasks=request.backfill_concurrent_tasks,
        ddl_concurrent_tasks=request.ddl_concurrent_tasks,
        users=request.users,
        is_dev=request.is_dev,
        forward_only=request.forward_only,
        environment_expiration_ts=request.environment.expiration_ts,
        dag_start_ts=now_timestamp(),
    )


def _get_demoted_snapshots(
    new_environment: Environment, state_sync: StateSync
) -> t.List[SnapshotTableInfo]:
    current_environment = state_sync.get_environment(new_environment.name)
    if current_environment:
        preserved_snapshot_names = {s.name for s in new_environment.snapshots}
        return [s for s in current_environment.snapshots if s.name not in preserved_snapshot_names]
    return []
