from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core import scheduler
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import can_evaluate_before_promote
from sqlmesh.core.snapshot import SnapshotTableInfo
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateSync
from sqlmesh.core.state_sync.base import DelegatingStateSync
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now, now_timestamp
from sqlmesh.utils.errors import SQLMeshError


class PlanDagState:
    def __init__(self, engine_adapter: EngineAdapter, plan_dags_table: str):
        self.engine_adapter = engine_adapter

        self._plan_dags_table = plan_dags_table
        self._plan_dag_columns_to_types = {
            "request_id": exp.DataType.build("text"),
            "dag_id": exp.DataType.build("text"),
            "dag_spec": exp.DataType.build("text"),
        }

    @classmethod
    def from_state_sync(cls, state_sync: StateSync) -> PlanDagState:
        while isinstance(state_sync, DelegatingStateSync):
            state_sync = state_sync.state_sync
        if not isinstance(state_sync, EngineAdapterStateSync):
            raise SQLMeshError(f"Unsupported state sync {state_sync.__class__.__name__}")
        return cls(state_sync.engine_adapter, state_sync.plan_dags_table)

    def add_dag_spec(self, spec: common.PlanDagSpec) -> None:
        """Adds a new DAG spec to the state.

        Args:
            spec: the plan DAG spec to add.
        """
        df = pd.DataFrame(
            [
                {
                    "request_id": spec.request_id,
                    "dag_id": common.plan_application_dag_id(
                        spec.environment_naming_info.name, spec.request_id
                    ),
                    "dag_spec": spec.json(),
                }
            ]
        )
        self.engine_adapter.insert_append(
            self._plan_dags_table,
            df,
            columns_to_types=self._plan_dag_columns_to_types,
            contains_json=True,
        )

    def get_dag_specs(self) -> t.List[common.PlanDagSpec]:
        """Returns all DAG specs in the state."""
        query = exp.select("dag_spec").from_(self._plan_dags_table)
        return [
            common.PlanDagSpec.parse_raw(row[0])
            for row in self.engine_adapter.fetchall(
                query, ignore_unsupported_errors=True, quote_identifiers=True
            )
        ]

    def delete_dag_specs(self, dag_ids: t.Collection[str]) -> None:
        """Deletes the DAG specs with the given DAG IDs."""
        if not dag_ids:
            return
        self.engine_adapter.delete_from(
            self._plan_dags_table,
            where=exp.column("dag_id").isin(*dag_ids),
        )


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
            snapshot_id=s.snapshot_id,
            intervals=intervals,
            before_promote=request.is_dev or can_evaluate_before_promote(s, all_snapshots),
        )
        for s, intervals in backfill_batches.items()
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
