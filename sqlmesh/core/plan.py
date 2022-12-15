"""
# Plan

Planning allows you to understand and categorize potential changes to models in a given environment.
A plan can compare your local environment with a remote environment or two remote environments. Plans show
text diffs of directly modified changes while showing you possibly affected downstream models. Finally, planning
will also allow you to control what date ranges are missing and need to be backfilled.
Based on this infromation, it's up to you to choose whether changes are [breaking](#breaking-change), [non-breaking](#non-breaking-change), or [no change](#no-change).
These categorizations determine which tables are backfilled or reused.

# Change Categories
Categorizations are only prompted for new snapshots that have been directly modified. The categorization of indirectly modified downstream models is inferred based on upstream decisions. If an indirectly modified snapshot's upstream parents have conflicting decisions, it always inherits the most severe one (breaking).

## Breaking Change
If a directly modified model is categorized as a breaking-changing, then it will be backfilled along with all children. In general, this is the safest option to choose because it guarantees all downstream dependencies pick up any logical changes. However, it is the most expensive option because backfilling takes time and resources. Choose breaking when you've changed the logic of your models which needs to propogated downstream.

## Non-Breaking Change
A directly modified model that is classified as non-breaking will backfill itself but will not backfill any of its children. This is a common option for column additions that don't affect downstream models because they aren't being used yet.

## No Change
No change means don't backfill anything but use any logic changes that occured only an ongoing basis. If other environments use this particular snapshot, they will be affected ongoing as well. It is safe to use no change for metadata changes or things that don't affect the logic of a model at all. If there are indeed logic changes that would change historical runs, they will not be reflected unless a future backfill occurs. Use no change sparingly or only when necessary for logic changes.

# Backfills
After all changes have been categorized, SQLMesh prompts you with intervals that need to be backfilled, or if there are none, then a "Logical Update" button will appear. The dates to backfill can be adjusted, this is useful in development environments where you may only need a subset of data to validate your changes. A logical update means that deploying changes to an environment is cheap because only the pointers to the tables need to be swapped out.

# Apply
Changes and backfills only take place once a plan is applied. Once applied, a plan and the snapshots associated with it are immutable.
"""
from __future__ import annotations

import typing as t
from collections import defaultdict, deque
from enum import Enum

from sqlmesh.core import scheduler
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.dag import DAG
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    merge_intervals,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import random_id
from sqlmesh.utils.date import TimeLike, make_inclusive, now, to_ds, validate_date_range
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

SnapshotMapping = t.Dict[str, t.Set[str]]
InfoCache = t.Tuple[t.List[Snapshot], t.List[Snapshot], SnapshotMapping]


class Plan:
    """Plan is the main class to represent user choices on how they want to backfill and version their models.

    Args:
        context_diff: The context diff that the plan is based on.
        dag: The dag object to determine relationships.
        state_reader: The state_reader to get metadata with.
        start: The start time to backfill data.
        end: The end time to backfill data.
        apply: The callback to apply the plan.
        restate_from: A list of dependencies to globally restate.
        no_gaps:  Whether to ensure that new snapshots for models that are already a
            part of the target environment have no data gaps when compared against previous
            snapshots for same models.
        skip_backfill: Whether to skip the backfill step.
    """

    def __init__(
        self,
        context_diff: ContextDiff,
        dag: DAG,
        state_reader: StateReader,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        apply: t.Optional[t.Callable[[Plan], None]] = None,
        restate_from: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
    ):
        self.context_diff = context_diff
        self.override_start = start is not None
        self.override_end = end is not None
        self.plan_id: str = random_id()
        self.restatements = set()
        self.no_gaps = no_gaps
        self.skip_backfill = skip_backfill
        self._start = start
        self._end = end
        self._apply = apply
        self._dag = dag
        self._state_reader = state_reader
        self._missing_intervals: t.Optional[t.Dict[str, Intervals]] = None
        self._info_cache: t.Optional[InfoCache] = None

        for table in restate_from or []:
            if table in context_diff.snapshots:
                raise SQLMeshError(
                    f"Cannot restate '{table}'. Restatement can only be done on upstream models outside of the scope of SQLMesh."
                )
            downstream = self._dag.downstream(table)

            if not downstream:
                raise SQLMeshError(f"Cannot restate '{table}'. No models reference it.")

            self.restatements.update(downstream)

    @property
    def categorized(self) -> t.List[Snapshot]:
        """Returns the already categorized snapshots."""
        return self.info_cache[0]

    @property
    def uncategorized(self) -> t.List[Snapshot]:
        """Returns the uncategorized snapshots."""
        return self.info_cache[1]

    @property
    def indirectly_modified(self) -> SnapshotMapping:
        """Returns a mapping of snapshots to their indirectly modified downstream snapshots."""
        return self.info_cache[2]

    @property
    def start(self) -> TimeLike:
        """Returns the start of the plan or the earliest date of all snapshots."""
        return self._start or scheduler.earliest_start_date(self.snapshots)

    @start.setter
    def start(self, new_start) -> None:
        self._start = new_start
        self._missing_intervals = None

    @property
    def end(self) -> TimeLike:
        """Returns the end of the plan or now."""
        return self._end or now()

    @end.setter
    def end(self, new_end: TimeLike) -> None:
        self._end = new_end
        self._missing_intervals = None

    @property
    def requires_backfill(self) -> bool:
        return not self.skip_backfill and bool(self.missing_intervals)

    @property
    def missing_intervals(self) -> t.List[MissingIntervals]:
        """Returns a list of missing intervals."""
        if self._missing_intervals is None:
            previous_ids = [
                SnapshotId(
                    name=snapshot.name,
                    fingerprint=snapshot.previous_version.fingerprint,
                )
                for snapshot in self.snapshots
                if snapshot.previous_version
            ]

            previous_snapshots = (
                list(self._state_reader.get_snapshots(previous_ids).values())
                if previous_ids
                else []
            )

            end = self.end
            self._missing_intervals = {
                snapshot.version_or_fingerprint: missing
                for snapshot, missing in self._state_reader.missing_intervals(
                    previous_snapshots + list(self.snapshots),
                    start=self.start,
                    end=end,
                    latest=end,
                    restatements=self.restatements,
                ).items()
            }
        return [
            MissingIntervals(
                snapshot_name=snapshot.name,
                intervals=self._missing_intervals[snapshot.version_or_fingerprint],
            )
            for snapshot in self.snapshots
            if snapshot.version_or_fingerprint in self._missing_intervals
        ]

    @property
    def snapshots(self) -> t.Iterable[Snapshot]:
        """Gets all the snapshots in the plan/environment."""
        return self.context_diff.snapshots.values()

    @property
    def new_snapshots(self) -> t.Iterable[Snapshot]:
        """Gets only new snapshots in the plan/environment."""
        return self.context_diff.new_snapshots

    @property
    def environment(self) -> Environment:
        """The environment of the plan."""
        return Environment(
            name=self.context_diff.environment,
            snapshots=[snapshot.table_info for snapshot in self.snapshots],
            start=self.start,
            end=self._end,
            plan_id=self.plan_id,
            previous_plan_id=self.context_diff.previous_plan_id,
        )

    def apply(self) -> None:
        """Runs apply if an apply function was passed in."""
        if not self._apply:
            raise SQLMeshError(f"Plan was not initialized with an applier.")
        validate_date_range(self.start, self.end)
        self._apply(self)

    def set_choice(self, snapshot: Snapshot, choice: SnapshotChangeCategory) -> None:
        """Sets a snapshot version based on the user choice.

        Args:
            snapshot: The snapshot to version.
            choice: The user decision on how to version the snapshot and it's children.
        """
        snapshot.change_category = choice
        if choice in (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.NON_BREAKING,
        ):
            snapshot.set_version()
        else:
            snapshot.set_version(snapshot.previous_version)

        for child in self.indirectly_modified[snapshot.name]:
            child_snapshot = self.context_diff.snapshots[child]

            if choice == SnapshotChangeCategory.BREAKING:
                child_snapshot.set_version()
            else:
                child_snapshot.set_version(child_snapshot.previous_version)
            snapshot.indirect_versions[child] = child_snapshot.all_versions

            # If any other snapshot specified breaking this child, then that child
            # needs to be backfilled as a part of the plan.
            for upstream in self.uncategorized:
                if child in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child][-1]
                    if data_version.is_new_version:
                        child_snapshot.set_version()
                        break

    def snapshot_change_category(self, snapshot: Snapshot) -> SnapshotChangeCategory:
        """Returns the SnapshotChangeCategory for the specified snapshot within this plan.

        Args:
            snapshot: The snapshot within this plan
        """
        if snapshot not in self.snapshots:
            raise SQLMeshError(
                f"Snapshot {snapshot.snapshot_id} does not exist in this plan"
            )

        if not snapshot.version:
            raise SQLMeshError(
                f"Snapshot {snapshot.snapshot_id} has not be categorized yet"
            )

        if snapshot.name not in self.context_diff.modified_snapshots:
            return SnapshotChangeCategory.NO_CHANGE

        current, previous = self.context_diff.modified_snapshots[snapshot.name]
        if current.version == previous.version:
            return SnapshotChangeCategory.NO_CHANGE

        if current.data_hash_matches(previous):
            return SnapshotChangeCategory.BREAKING

        if previous.data_version in current.all_versions:
            index = current.all_versions.index(previous.data_version)
            versions = current.all_versions[index + 1 :]
        elif current.data_version in previous.all_versions:
            # Snapshot is a revert to a previous snapshot
            index = previous.all_versions.index(current.data_version)
            versions = previous.all_versions[index:]
        else:
            # Insufficient history, so err on the side of safety
            return SnapshotChangeCategory.BREAKING

        change_categories = [
            version.change_category for version in versions if version.change_category
        ]
        return min(change_categories, key=lambda x: x.value)

    @property
    def info_cache(self) -> InfoCache:
        """Returns the info cache of categorized, uncategorized snapshots."""
        if self._info_cache is None:
            queue = deque(self._dag.sorted())
            snapshots = []
            all_indirectly_modified = set()

            while queue:
                model_name = queue.popleft()

                if model_name not in self.context_diff.snapshots:
                    continue

                snapshot = self.context_diff.snapshots[model_name]

                if model_name in self.context_diff.modified_snapshots:
                    if self.context_diff.directly_modified(model_name):
                        snapshots.append(snapshot)
                    else:
                        all_indirectly_modified.add(model_name)

                        # set to breaking if an indirect child has no directly modified parents
                        # that need a decision. this can happen when a revert to a parent causes
                        # an indirectly modified snapshot to be created because of a new parent
                        if not snapshot.version and not any(
                            self.context_diff.directly_modified(upstream)
                            and not self.context_diff.snapshots[upstream].version
                            for upstream in self._dag.upstream(model_name)
                        ):
                            snapshot.set_version()

                elif model_name in self.context_diff.added:
                    snapshot.set_version()
                    snapshots.append(snapshot)

            indirectly_modified: SnapshotMapping = defaultdict(set)

            for snapshot in snapshots:
                for downstream in self._dag.downstream(snapshot.name):
                    if downstream in all_indirectly_modified:
                        indirectly_modified[snapshot.name].add(downstream)

            self._info_cache = (
                [snapshot for snapshot in snapshots if snapshot.version],
                [snapshot for snapshot in snapshots if not snapshot.version],
                indirectly_modified,
            )
        return self._info_cache


class PlanStatus(str, Enum):
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"

    @property
    def is_started(self):
        return self == PlanStatus.STARTED

    @property
    def is_failed(self):
        return self == PlanStatus.FAILED

    @property
    def is_finished(self):
        return self == PlanStatus.FINISHED


class MissingIntervals(PydanticModel, frozen=True):
    snapshot_name: str
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_missing_range(self) -> str:
        intervals = [make_inclusive(start, end) for start, end in self.merged_intervals]
        return ", ".join(f"({to_ds(start)}, {to_ds(end)})" for start, end in intervals)
