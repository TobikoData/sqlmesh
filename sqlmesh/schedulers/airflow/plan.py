from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core import scheduler
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import update_intervals_for_new_snapshots
from sqlmesh.core.snapshot import DeployabilityIndex, SnapshotTableInfo
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateSync
from sqlmesh.core.state_sync.base import DelegatingStateSync
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now, to_timestamp
from sqlmesh.utils.errors import SQLMeshError


class PlanDagState:
    def __init__(self, engine_adapter: EngineAdapter, plan_dags_table: exp.Table):
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
                        spec.environment.name, spec.request_id
                    ),
                    "dag_spec": spec.json(),
                }
            ]
        )
        self.engine_adapter.insert_append(
            self._plan_dags_table,
            df,
            columns_to_types=self._plan_dag_columns_to_types,
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
    plan = request.plan
    new_snapshots = {s.snapshot_id: s for s in plan.new_snapshots}
    stored_snapshots = state_sync.get_snapshots([*new_snapshots, *plan.environment.snapshots])
    all_snapshots = {**new_snapshots, **stored_snapshots}

    all_snaphots_by_name = {s.name: s for s in all_snapshots.values()}
    restatements = {
        all_snaphots_by_name[n].snapshot_id: i
        for n, i in plan.restatements.items()
        if n in all_snaphots_by_name
    }

    duplicated_snapshots = set(stored_snapshots).intersection(new_snapshots)
    if duplicated_snapshots:
        raise SQLMeshError(
            f"Snapshots {duplicated_snapshots} already exist. "
            "Make sure your code base is up to date and try re-creating the plan"
        )

    update_intervals_for_new_snapshots(new_snapshots.values(), state_sync)

    now_dt = now()
    end = plan.environment.end_at or now_dt
    unpaused_dt = end if not plan.is_dev and not plan.restatements else None

    if plan.restatements:
        intervals_to_remove = [
            (s, restatements[s.snapshot_id])
            for s in all_snapshots.values()
            if s.snapshot_id in restatements and s.snapshot_id not in new_snapshots
        ]
        state_sync.remove_intervals(
            intervals_to_remove,
            remove_shared_versions=not plan.is_dev,
        )
        for s, interval in intervals_to_remove:
            all_snapshots[s.snapshot_id].remove_interval(interval)

    deployability_index_for_creation = DeployabilityIndex.create(all_snapshots, start=plan.start)
    deployability_index_for_evaluation = (
        deployability_index_for_creation if plan.is_dev else DeployabilityIndex.all_deployable()
    )

    if not plan.skip_backfill:
        backfill_batches = scheduler.compute_interval_params(
            [s for s in all_snapshots.values() if plan.is_selected_for_backfill(s.name)],
            start=plan.environment.start_at,
            end=end,
            execution_time=plan.execution_time or now(),
            deployability_index=deployability_index_for_evaluation,
            restatements=restatements,
            interval_end_per_model=plan.interval_end_per_model,
            end_bounded=plan.end_bounded,
            signal_factory=None,
        )
    else:
        backfill_batches = {}

    backfill_intervals_per_snapshot = [
        common.BackfillIntervalsPerSnapshot(
            snapshot_id=s.snapshot_id,
            intervals=intervals,
            before_promote=plan.is_dev or deployability_index_for_creation.is_representative(s),
        )
        for s, intervals in backfill_batches.items()
    ]

    no_gaps_snapshot_names = (
        {
            s.name
            for s in all_snapshots.values()
            if deployability_index_for_creation.is_representative(s)
        }
        if plan.no_gaps and not plan.is_dev
        else None
        if plan.no_gaps
        else set()
    )

    return common.PlanDagSpec(
        request_id=plan.plan_id,
        environment=plan.environment,
        new_snapshots=plan.new_snapshots,
        backfill_intervals_per_snapshot=backfill_intervals_per_snapshot,
        demoted_snapshots=_get_demoted_snapshots(plan.environment, state_sync),
        unpaused_dt=unpaused_dt,
        no_gaps=plan.no_gaps,
        notification_targets=request.notification_targets,
        backfill_concurrent_tasks=request.backfill_concurrent_tasks,
        ddl_concurrent_tasks=request.ddl_concurrent_tasks,
        users=request.users,
        is_dev=plan.is_dev,
        allow_destructive_snapshots=plan.allow_destructive_models,
        forward_only=plan.forward_only,
        dag_start_ts=to_timestamp(now_dt),
        deployability_index=deployability_index_for_evaluation,
        deployability_index_for_creation=deployability_index_for_creation,
        no_gaps_snapshot_names=no_gaps_snapshot_names,
        models_to_backfill=plan.models_to_backfill,
        ensure_finalized_snapshots=plan.ensure_finalized_snapshots,
        directly_modified_snapshots=plan.directly_modified_snapshots,
        indirectly_modified_snapshots=plan.indirectly_modified_snapshots,
        removed_snapshots=plan.removed_snapshots,
        execution_time=plan.execution_time,
    )


def _get_demoted_snapshots(
    new_environment: Environment, state_sync: StateSync
) -> t.List[SnapshotTableInfo]:
    current_environment = state_sync.get_environment(new_environment.name)
    if current_environment:
        preserved_snapshot_names = {s.name for s in new_environment.snapshots}
        return [s for s in current_environment.snapshots if s.name not in preserved_snapshot_names]
    return []
