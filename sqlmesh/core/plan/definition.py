from __future__ import annotations

import typing as t
from dataclasses import dataclass
from enum import Enum

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Intervals,
    Snapshot,
    merge_intervals,
    missing_intervals,
)
from sqlmesh.core.snapshot.definition import Interval, SnapshotId, format_intervals
from sqlmesh.utils.date import TimeLike, now, to_timestamp
from sqlmesh.utils.pydantic import PydanticModel

SnapshotMapping = t.Dict[SnapshotId, t.Set[SnapshotId]]


class Plan(PydanticModel, frozen=True):
    context_diff: ContextDiff
    plan_id: str
    start: TimeLike
    end: t.Optional[TimeLike] = None

    is_dev: bool
    skip_backfill: bool
    no_gaps: bool
    forward_only: bool
    include_unmodified: bool

    environment_ttl: t.Optional[str] = None
    environment_naming_info: EnvironmentNamingInfo

    directly_modified: t.Set[SnapshotId]
    indirectly_modified: t.Dict[SnapshotId, t.Set[SnapshotId]]
    ignored: t.Set[SnapshotId]

    deployability_index: DeployabilityIndex
    restatements: t.Dict[SnapshotId, Interval]

    models_to_backfill: t.Optional[t.Set[str]] = None
    effective_from: t.Optional[TimeLike] = None
    execution_time: t.Optional[TimeLike] = None

    _categorized: t.Optional[t.List[Snapshot]] = None
    _uncategorized: t.Optional[t.List[Snapshot]] = None
    _all_modified: t.Optional[t.Dict[SnapshotId, Snapshot]] = None
    _snapshots: t.Optional[t.Dict[SnapshotId, Snapshot]] = None
    _environment: t.Optional[Environment] = None

    @property
    def end_or_now(self) -> TimeLike:
        return self.end or now()

    @property
    def previous_plan_id(self) -> t.Optional[str]:
        return self.context_diff.previous_plan_id

    @property
    def requires_backfill(self) -> bool:
        return not self.skip_backfill and (bool(self.restatements) or bool(self.missing_intervals))

    @property
    def has_changes(self) -> bool:
        modified_snapshot_ids = {
            *self.context_diff.added,
            *self.context_diff.removed_snapshots,
            *self.context_diff.current_modified_snapshot_ids,
        } - self.ignored
        return (
            self.context_diff.is_new_environment
            or self.context_diff.is_unfinalized_environment
            or bool(modified_snapshot_ids)
        )

    @property
    def has_unmodified_unpromoted(self) -> bool:
        """Is the plan for an existing dev environment, has the include unmodified flag, and contains unmodified nodes that have not been promoted."""
        return (
            self.is_dev
            and not self.context_diff.is_new_environment
            and self.include_unmodified
            and bool(self.context_diff.unpromoted_models)
        )

    @property
    def categorized(self) -> t.List[Snapshot]:
        """Returns the already categorized snapshots."""
        if self._categorized is None:
            self._categorized = [
                self.context_diff.snapshots[s_id]
                for s_id in sorted(self.directly_modified)
                if self.context_diff.snapshots[s_id].version
            ]
        return self._categorized

    @property
    def uncategorized(self) -> t.List[Snapshot]:
        """Returns the uncategorized snapshots."""
        if self._uncategorized is None:
            self._uncategorized = [
                self.context_diff.snapshots[s_id]
                for s_id in sorted(self.directly_modified)
                if not self.context_diff.snapshots[s_id].version
            ]
        return self._uncategorized

    @property
    def snapshots(self) -> t.Dict[SnapshotId, Snapshot]:
        if self._snapshots is None:
            self._snapshots = {
                s_id: s
                for s_id, s in self.context_diff.snapshots.items()
                if s_id not in self.ignored
            }
        return self._snapshots

    @property
    def modified_snapshots(self) -> t.Dict[SnapshotId, Snapshot]:
        """Returns the modified (either directly or indirectly) snapshots."""
        if self._all_modified is None:
            self._all_modified = {
                **{
                    s_id: self.context_diff.snapshots[s_id]
                    for s_id in sorted(self.directly_modified)
                },
                **{
                    s_id: self.context_diff.snapshots[s_id]
                    for downstream_s_ids in self.indirectly_modified.values()
                    for s_id in sorted(downstream_s_ids)
                },
            }
        return self._all_modified

    @property
    def new_snapshots(self) -> t.List[Snapshot]:
        """Gets only new snapshots in the plan/environment."""
        return [
            s for s in self.context_diff.new_snapshots.values() if s.snapshot_id not in self.ignored
        ]

    @property
    def missing_intervals(self) -> t.List[SnapshotIntervals]:
        """Returns the missing intervals for this plan."""
        # NOTE: Even though the plan is immutable, snapshots that are part of it are not. Since snapshot intervals
        # may change over time, we should avoid caching missing intervals within the plan instance.
        intervals = [
            SnapshotIntervals(snapshot_id=snapshot.snapshot_id, intervals=missing)
            for snapshot, missing in missing_intervals(
                [s for s in self.snapshots.values() if self.is_selected_for_backfill(s.name)],
                start=self.start,
                end=self.end,
                execution_time=self.execution_time,
                restatements=self.restatements,
                deployability_index=self.deployability_index,
                ignore_cron=True,
            ).items()
            if snapshot.is_model and missing
        ]
        return sorted(intervals, key=lambda i: i.snapshot_id)

    @property
    def environment(self) -> Environment:
        """The environment of this plan."""
        if self._environment is None:
            expiration_ts = (
                to_timestamp(self.environment_ttl, relative_base=now())
                if self.is_dev and self.environment_ttl is not None
                else None
            )

            snapshots = [s.table_info for s in self.snapshots.values()]
            promoted_snapshot_ids = None
            if self.is_dev and not self.include_unmodified:
                promoted_snapshot_ids = [
                    s.snapshot_id
                    for s in snapshots
                    if s.snapshot_id in self.context_diff.promotable_snapshot_ids
                ]

            self._environment = Environment(
                snapshots=snapshots,
                start_at=self.start,
                end_at=self.end,
                plan_id=self.plan_id,
                previous_plan_id=self.previous_plan_id,
                expiration_ts=expiration_ts,
                promoted_snapshot_ids=promoted_snapshot_ids,
                **self.environment_naming_info.dict(),
            )
        return self._environment

    def is_new_snapshot(self, snapshot: Snapshot) -> bool:
        """Returns True if the given snapshot is a new snapshot in this plan."""
        snapshot_id = snapshot.snapshot_id
        return snapshot_id in self.context_diff.new_snapshots and snapshot_id not in self.ignored

    def is_selected_for_backfill(self, model_fqn: str) -> bool:
        """Returns True if a model with the given FQN should be backfilled as part of this plan."""
        return self.models_to_backfill is None or model_fqn in self.models_to_backfill


class PlanStatus(str, Enum):
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"

    @property
    def is_started(self) -> bool:
        return self == PlanStatus.STARTED

    @property
    def is_failed(self) -> bool:
        return self == PlanStatus.FAILED

    @property
    def is_finished(self) -> bool:
        return self == PlanStatus.FINISHED


# millions of these can be created, pydantic has significant overhead
@dataclass
class SnapshotIntervals:
    snapshot_id: SnapshotId
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_intervals(self, unit: t.Optional[IntervalUnit] = None) -> str:
        return format_intervals(self.merged_intervals, unit)
