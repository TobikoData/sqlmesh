from __future__ import annotations

import json
import typing as t
from collections import defaultdict
from datetime import datetime
from enum import IntEnum

from pydantic import Field, validator
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core import constants as c
from sqlmesh.core.audit import BUILT_IN_AUDITS, Audit
from sqlmesh.core.model import (
    Model,
    ModelKindMixin,
    ModelKindName,
    PythonModel,
    SeedModel,
    SqlModel,
    ViewKind,
    kind,
)
from sqlmesh.core.model.definition import _SqlBasedModel
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.utils.date import (
    TimeLike,
    is_date,
    make_inclusive,
    make_inclusive_end,
    now,
    now_timestamp,
    to_datetime,
    to_ds,
    to_timestamp,
)
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.hashing import crc32
from sqlmesh.utils.pydantic import PydanticModel

Interval = t.Tuple[int, int]
Intervals = t.List[Interval]


class SnapshotChangeCategory(IntEnum):
    """
    Values are ordered by decreasing severity and that ordering is required.

    BREAKING: The change requires that snapshot modified and downstream dependencies be rebuilt
    NON_BREAKING: The change requires that only the snapshot modified be rebuilt
    FORWARD_ONLY: The change requires no rebuilding
    INDIRECT_BREAKING: The change was caused indirectly and is breaking.
    INDIRECT_NON_BREAKING: The change was caused indirectly by a non-breaking change.
    """

    BREAKING = 1
    NON_BREAKING = 2
    FORWARD_ONLY = 3
    INDIRECT_BREAKING = 4
    INDIRECT_NON_BREAKING = 5

    @property
    def is_breaking(self) -> bool:
        return self == self.BREAKING

    @property
    def is_non_breaking(self) -> bool:
        return self == self.NON_BREAKING

    @property
    def is_forward_only(self) -> bool:
        return self == self.FORWARD_ONLY

    @property
    def is_indirect_breaking(self) -> bool:
        return self == self.INDIRECT_BREAKING

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self == self.INDIRECT_NON_BREAKING


class SnapshotFingerprint(PydanticModel, frozen=True):
    data_hash: str
    metadata_hash: str
    parent_data_hash: str = "0"
    parent_metadata_hash: str = "0"

    def to_version(self) -> str:
        return _hash([self.data_hash, self.parent_data_hash])

    def to_identifier(self) -> str:
        return _hash(
            [
                self.data_hash,
                self.metadata_hash,
                self.parent_data_hash,
                self.parent_metadata_hash,
            ]
        )


class SnapshotId(PydanticModel, frozen=True):
    name: str
    identifier: str

    @property
    def snapshot_id(self) -> SnapshotId:
        """Helper method to return self."""
        return self


class SnapshotNameVersion(PydanticModel, frozen=True):
    name: str
    version: str


class SnapshotIntervals(PydanticModel, frozen=True):
    name: str
    identifier: str
    version: str
    intervals: Intervals
    dev_intervals: Intervals

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)


class SnapshotDataVersion(PydanticModel, frozen=True):
    fingerprint: SnapshotFingerprint
    version: str
    temp_version: t.Optional[str]
    change_category: t.Optional[SnapshotChangeCategory]
    physical_schema_: t.Optional[str] = Field(default=None, alias="physical_schema")

    def snapshot_id(self, name: str) -> SnapshotId:
        return SnapshotId(name=name, identifier=self.fingerprint.to_identifier())

    @property
    def physical_schema(self) -> str:
        # The physical schema here is optional to maintain backwards compatibility with
        # records stored by previous versions of SQLMesh.
        return self.physical_schema_ or c.SQLMESH

    @property
    def data_version(self) -> SnapshotDataVersion:
        return self

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint.to_version() == self.version


class QualifiedViewName(PydanticModel, frozen=True):
    catalog: t.Optional[str]
    schema_name: t.Optional[str]
    table: str

    def for_environment(self, environment: str) -> str:
        return ".".join(
            p
            for p in (
                self.catalog,
                self.schema_for_environment(environment),
                self.table,
            )
            if p is not None
        )

    def schema_for_environment(self, environment: str) -> str:
        schema = self.schema_name or c.DEFAULT_SCHEMA
        if environment.lower() != c.PROD:
            schema = f"{schema}__{environment}"
        return schema


class SnapshotInfoMixin(ModelKindMixin):
    name: str
    temp_version: t.Optional[str]
    change_category: t.Optional[SnapshotChangeCategory]
    fingerprint: SnapshotFingerprint
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()

    @property
    def identifier(self) -> str:
        return self.fingerprint.to_identifier()

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

    @property
    def qualified_view_name(self) -> QualifiedViewName:
        view_name = exp.to_table(self.name)
        return QualifiedViewName(
            catalog=view_name.catalog or None,
            schema_name=view_name.db or None,
            table=view_name.name,
        )

    @property
    def previous_version(self) -> t.Optional[SnapshotDataVersion]:
        """Helper method to get the previous data version."""
        if self.previous_versions:
            return self.previous_versions[-1]
        return None

    @property
    def physical_schema(self) -> str:
        raise NotImplementedError

    @property
    def data_version(self) -> SnapshotDataVersion:
        raise NotImplementedError

    @property
    def is_new_version(self) -> bool:
        raise NotImplementedError

    @property
    def is_forward_only(self) -> bool:
        return self.change_category == SnapshotChangeCategory.FORWARD_ONLY

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self.change_category == SnapshotChangeCategory.INDIRECT_NON_BREAKING

    @property
    def all_versions(self) -> t.Tuple[SnapshotDataVersion, ...]:
        """Returns previous versions with the current version trimmed to DATA_VERSION_LIMIT."""
        return (*self.previous_versions, self.data_version)[-c.DATA_VERSION_LIMIT :]

    def data_hash_matches(self, other: t.Optional[SnapshotInfoMixin | SnapshotDataVersion]) -> bool:
        return other is not None and self.fingerprint.data_hash == other.fingerprint.data_hash

    def _table_name(self, version: str, is_dev: bool, for_read: bool) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            version: The snapshot version.
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        if is_dev and for_read:
            # If this snapshot is used for **reading**, return a temporary table
            # only if this snapshot captures a direct forward-only change applied to its model.
            is_temp = self.is_forward_only
        elif is_dev:
            # Use a temporary table in the dev environment when **writing** a forward-only snapshot
            # which was modified either directly or indirectly.
            is_temp = self.is_forward_only or self.is_indirect_non_breaking
        else:
            is_temp = False

        if is_temp:
            version = self.temp_version or self.fingerprint.to_version()

        return table_name(
            self.physical_schema,
            self.name,
            version,
            is_temp=is_temp,
        )


class SnapshotTableInfo(PydanticModel, SnapshotInfoMixin, frozen=True):
    name: str
    fingerprint: SnapshotFingerprint
    version: str
    temp_version: t.Optional[str]
    physical_schema_: str = Field(alias="physical_schema")
    parents: t.Tuple[SnapshotId, ...]
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    change_category: t.Optional[SnapshotChangeCategory]
    kind_name: ModelKindName

    def table_name(self, is_dev: bool = False, for_read: bool = False) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        return self._table_name(self.version, is_dev, for_read)

    @property
    def physical_schema(self) -> str:
        return self.physical_schema_

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to return self."""
        return self

    @property
    def data_version(self) -> SnapshotDataVersion:
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            change_category=self.change_category,
            physical_schema=self.physical_schema,
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint.to_version() == self.version

    @property
    def model_kind_name(self) -> ModelKindName:
        return self.kind_name


class Snapshot(PydanticModel, SnapshotInfoMixin):
    """A snapshot represents a model at a certain point in time.

    Snapshots are used to encapsulate everything needed to evaluate a model.
    They are standalone objects that hold all state and dynamic content necessary
    to render a model's query including things like macros. Snapshots also store intervals
    (timestamp ranges for what data we've processed).

    Models can be dynamically rendered due to macros. Rendering a model to its full extent
    requires storing variables and macro definitions. We store all of the macro definitions and
    global variable references in `python_env` in raw text to avoid pickling. The helper methods
    to achieve this are defined in utils.metaprogramming.

    Args:
        name: The snapshot name which is the same as the model name and should be unique per model.

        fingerprint: A unique hash of the model definition so that models can be reused across environments.
        physical_schema: The physical schema that the snapshot is stored in.
        model: Model object that the snapshot encapsulates.
        parents: The list of parent snapshots (upstream dependencies).
        audits: The list of audits used by the model.
        intervals: List of [start, end) intervals showing which time ranges a snapshot has data for.
        dev_intervals: List of [start, end) intervals showing development intervals (forward-only).
        project: The name of the project this snapshot is associated with.
        created_ts: Epoch millis timestamp when a snapshot was first created.
        updated_ts: Epoch millis timestamp when a snapshot was last updated.
        ttl: The time-to-live of a snapshot determines when it should be deleted after it's no longer referenced
            in any environment.
        previous: The snapshot data version that this snapshot was based on. If this snapshot is new, then previous will be None.
        version: User specified version for a snapshot that is used for physical storage.
            By default, the version is the fingerprint, but not all changes to models require a backfill.
            If a user passes a previous version, that will be used instead and no backfill will be required.
        change_category: User specified change category indicating which models require backfill from model changes made in this snapshot.
        unpaused_ts: The timestamp which indicates when this snapshot was unpaused. Unpaused means that
            this snapshot is evaluated on a recurring basis. None indicates that this snapshot is paused.
        effective_from: The timestamp which indicates when this snapshot should be considered effective.
            Applicable for forward-only snapshots only.
    """

    name: str
    fingerprint: SnapshotFingerprint
    physical_schema_: t.Optional[str] = Field(default=None, alias="physical_schema")
    model: Model
    parents: t.Tuple[SnapshotId, ...]
    audits: t.Tuple[Audit, ...]
    intervals: Intervals = []
    dev_intervals: Intervals = []
    project: str = ""
    created_ts: int
    updated_ts: int
    ttl: str
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    indirect_versions: t.Dict[str, t.Tuple[SnapshotDataVersion, ...]] = {}
    version: t.Optional[str] = None
    temp_version: t.Optional[str] = None
    change_category: t.Optional[SnapshotChangeCategory] = None
    unpaused_ts: t.Optional[int] = None
    effective_from: t.Optional[TimeLike] = None
    _start: t.Optional[datetime] = None

    @validator("ttl")
    @classmethod
    def _time_delta_must_be_positive(cls, v: str) -> str:
        current_time = now()
        if to_datetime(v, current_time) < current_time:
            raise ValueError(
                "Must be positive. Use the 'in' keyword to denote a positive time interval. For example, 'in 7 days'."
            )
        return v

    @staticmethod
    def hydrate_with_intervals_by_version(
        snapshots: t.Iterable[Snapshot],
        intervals: t.Iterable[SnapshotIntervals],
        is_dev: bool = False,
    ) -> t.List[Snapshot]:
        """Hydrates target snapshots with given intervals.

        This will match snapshots with intervals by name and version rather than identifiers.

        Args:
            snapshots: Target snapshots.
            intervals: Target snapshot intervals.
            is_dev: If in development mode ignores same version intervals for paused forward-only snapshots.

        Returns:
            List of target snapshots with hydrated intervals.
        """
        intervals_by_name_version = defaultdict(list)
        for interval in intervals:
            intervals_by_name_version[(interval.name, interval.version)].append(interval)

        result = []
        for snapshot in snapshots:
            snapshot_intervals = intervals_by_name_version.get(
                (snapshot.name, snapshot.version_get_or_generate()), []
            )
            for interval in snapshot_intervals:
                if (
                    not is_dev
                    or not snapshot.is_forward_only
                    or not snapshot.is_paused
                    or snapshot.identifier == interval.identifier
                ):
                    snapshot.merge_intervals(interval)
            result.append(snapshot)

        return result

    @staticmethod
    def hydrate_with_intervals_by_identifier(
        snapshots: t.Iterable[Snapshot],
        intervals: t.Iterable[SnapshotIntervals],
    ) -> t.List[Snapshot]:
        """Hydrates target snapshots with given intervals.

        This will match snapshots with intervals by name and identifier rather than versions.

        Args:
            snapshots: Target snapshots.
            intervals: Target snapshot intervals.

        Returns:
            List of target snapshots with hydrated intervals.
        """
        intervals_by_snapshot_id = {i.snapshot_id: i for i in intervals}

        result = []
        for snapshot in snapshots:
            if snapshot.snapshot_id in intervals_by_snapshot_id:
                snapshot.merge_intervals(intervals_by_snapshot_id[snapshot.snapshot_id])
            result.append(snapshot)

        return result

    @classmethod
    def from_model(
        cls,
        model: Model,
        *,
        models: t.Dict[str, Model],
        ttl: str = c.DEFAULT_SNAPSHOT_TTL,
        project: str = "",
        version: t.Optional[str] = None,
        audits: t.Optional[t.Dict[str, Audit]] = None,
        cache: t.Optional[t.Dict[str, SnapshotFingerprint]] = None,
    ) -> Snapshot:
        """Creates a new snapshot for a model.

        Args:
            model: Model to snapshot.
            physical_schema: The schema of the snapshot which represents where it is stored.
            models: Dictionary of all models in the graph to make the fingerprint dependent on parent changes.
                If no dictionary is passed in the fingerprint will not be dependent on a model's parents.
            ttl: A TTL to determine how long orphaned (snapshots that are not promoted anywhere) should live.
            version: The version that a snapshot is associated with. Usually set during the planning phase.
            audits: Available audits by name.
            cache: Cache of model name to fingerprints.

        Returns:
            The newly created snapshot.
        """
        created_ts = now_timestamp()

        audits = audits or {}

        return cls(
            name=model.name,
            fingerprint=fingerprint_from_model(
                model,
                models=models,
                audits=audits,
                cache=cache,
            ),
            model=model,
            parents=tuple(
                SnapshotId(
                    name=name,
                    identifier=fingerprint_from_model(
                        models[name],
                        models=models,
                        audits=audits,
                        cache=cache,
                    ).to_identifier(),
                )
                for name in _parents_from_model(model, models)
            ),
            audits=tuple(model.referenced_audits(audits)),
            intervals=[],
            dev_intervals=[],
            project=project,
            created_ts=created_ts,
            updated_ts=created_ts,
            ttl=ttl,
            version=version,
        )

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, Snapshot) and self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        return hash((self.__class__, self.name, self.fingerprint))

    def add_interval(self, start: TimeLike, end: TimeLike, is_dev: bool = False) -> None:
        """Add a newly processed time interval to the snapshot.

        The actual stored intervals are [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive. This allows merging of ranges to be easier.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval. If end is a date, then it is considered inclusive.
                If it is a datetime object, then it is exclusive.
            is_dev: Indicates whether the given interval is being added while in development mode.
        """
        intervals = self.dev_intervals if is_dev else self.intervals

        intervals.append(self.inclusive_exclusive(start, end))

        if len(intervals) < 2:
            return

        merged_intervals = merge_intervals(intervals)
        if is_dev:
            self.dev_intervals = merged_intervals
        else:
            self.intervals = merged_intervals

    def remove_interval(
        self, start: TimeLike, end: TimeLike, latest: t.Optional[TimeLike] = None
    ) -> None:
        """Remove an interval from the snapshot.

        Args:
            start: Start interval to remove.
            end: End interval to remove.
            latest: The latest time to use for the end of the interval.
        """
        interval = self.inclusive_exclusive(start, end, latest, for_removal=True)
        self.intervals = remove_interval(self.intervals, *interval)
        self.dev_intervals = remove_interval(self.dev_intervals, *interval)

    def inclusive_exclusive(
        self,
        start: TimeLike,
        end: TimeLike,
        latest: t.Optional[TimeLike] = None,
        strict: bool = True,
        for_removal: bool = False,
    ) -> Interval:
        """Transform the inclusive start and end into a [start, end) pair.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive)
            latest: The latest time to use for the end of the interval. Defaults to now if not provided.
            strict: Whether to fail when the inclusive start is the same as the exclusive end.
            for_removal: Whether the interval is being used for removal.
        Returns:
            A [start, end) pair.
        """
        end = latest or now() if for_removal and self.depends_on_past else end
        start_ts = to_timestamp(self.model.cron_floor(start))
        end_ts = to_timestamp(
            self.model.cron_next(end)
            if is_date(end) and self.model.interval_unit() == IntervalUnit.DAY
            else self.model.cron_floor(end)
        )

        if (strict and start_ts >= end_ts) or (start_ts > end_ts):
            raise ValueError(
                f"`end` ({to_datetime(end_ts)}) must be greater than `start` ({to_datetime(start_ts)})"
            )
        return (start_ts, end_ts)

    def merge_intervals(self, other: t.Union[Snapshot, SnapshotIntervals]) -> None:
        """Inherits intervals from the target snapshot.

        Args:
            other: The target snapshot to inherit intervals from.
        """
        effective_from_ts = self.normalized_effective_from_ts or 0
        apply_effective_from = effective_from_ts > 0 and self.identifier != other.identifier

        for start, end in other.intervals:
            # If the effective_from is set, then intervals that come after it must come from
            # the current snapshost.
            if apply_effective_from and start < effective_from_ts:
                end = min(end, effective_from_ts)
            if not apply_effective_from or end <= effective_from_ts:
                self.add_interval(start, end)

        if self.identifier == other.identifier:
            for start, end in other.dev_intervals:
                self.add_interval(start, end, is_dev=True)

    def missing_intervals(
        self,
        start: TimeLike,
        end: TimeLike,
        latest: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Set[str]] = None,
    ) -> Intervals:
        """Find all missing intervals between [start, end].

        Although the inputs are inclusive, the returned stored intervals are
        [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive)
            latest: The date/time to use for latest (inclusive)
            restatements: A set of snapshot names being restated

        Returns:
            A list of all the missing intervals as epoch timestamps.
        """
        restatements = restatements or set()
        if self.is_symbolic or (self.is_seed and self.intervals):
            return []

        latest = make_inclusive_end(latest or now())
        missing = []

        start_ts, end_ts = (
            to_timestamp(ts)
            for ts in self.inclusive_exclusive(
                start, end, latest, strict=False, for_removal=self.name in restatements
            )
        )
        latest_ts = to_timestamp(latest)

        croniter = self.model.croniter(start_ts)
        dates = [start_ts]

        # get all individual dates with the addition of extra lookback dates up to the latest date
        # when a model has lookback, we need to check all the intervals between itself and its lookback exist.
        while True:
            ts = to_timestamp(croniter.get_next())

            if ts < end_ts:
                dates.append(ts)
            else:
                croniter.get_prev()
                break

        lookback = self.model.lookback

        for _ in range(lookback):
            ts = to_timestamp(croniter.get_next())
            if ts < latest_ts:
                dates.append(ts)
            else:
                break

        for i in range(len(dates)):
            if dates[i] >= end_ts:
                break
            current_ts = dates[i]
            next_ts = (
                dates[i + 1]
                if i + 1 < len(dates)
                else to_timestamp(self.model.cron_next(current_ts))
            )
            compare_ts = seq_get(dates, i + lookback) or dates[-1]

            for low, high in self.intervals:
                if compare_ts < low:
                    missing.append((current_ts, next_ts))
                    break
                elif current_ts >= low and compare_ts < high:
                    break
            else:
                missing.append((current_ts, next_ts))

        return missing

    def categorize_as(self, category: SnapshotChangeCategory) -> None:
        """Assigns the given category to this snapshot.

        Args:
            category: The change category to assign to this snapshot.
        """
        is_forward_only = category in (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        )
        if is_forward_only and self.previous_version:
            self.version = self.previous_version.data_version.version
            self.physical_schema_ = self.previous_version.physical_schema
        else:
            self.version = self.fingerprint.to_version()

        self.change_category = category

    def set_unpaused_ts(self, unpaused_dt: t.Optional[TimeLike]) -> None:
        """Sets the timestamp for when this snapshot was unpaused.

        Args:
            unpaused_dt: The datetime object of when this snapshot was unpaused.
        """
        self.unpaused_ts = to_timestamp(self.model.cron_floor(unpaused_dt)) if unpaused_dt else None

    def table_name(self, is_dev: bool = False, for_read: bool = False) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        self._ensure_categorized()
        assert self.version
        return self._table_name(self.version, is_dev, for_read)

    def table_name_for_mapping(self, is_dev: bool = False) -> str:
        """Full table name used by a child snapshot for table mapping during evaluation.

        Args:
            is_dev: Whether the table name will be used in development mode.
        """
        self._ensure_categorized()
        assert self.version

        if is_dev and self.is_forward_only:
            # If this snapshot is unpaused we shouldn't be using a temporary
            # table for mapping purposes.
            is_dev = self.is_paused

        return self._table_name(self.version, is_dev, True)

    def is_temporary_table(self, is_dev: bool) -> bool:
        """Provided whether the snapshot is used in a development mode or not, returns True
        if the snapshot targets a temporary table or a clone and False otherwise.
        """
        return is_dev and (self.is_forward_only or self.is_indirect_non_breaking) and self.is_paused

    def version_get_or_generate(self) -> str:
        """Helper method to get the version or generate it from the fingerprint."""
        return self.version or self.fingerprint.to_version()

    @property
    def physical_schema(self) -> str:
        if self.physical_schema_ is not None:
            return self.physical_schema_
        schema = exp.to_table(self.name).db
        if not schema:
            schema = c.DEFAULT_SCHEMA
        return f"{c.SQLMESH}__{schema}"

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to get the SnapshotTableInfo from the Snapshot."""
        self._ensure_categorized()
        return SnapshotTableInfo(
            physical_schema=self.physical_schema,
            name=self.name,
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            parents=self.parents,
            previous_versions=self.previous_versions,
            change_category=self.change_category,
            kind_name=self.model_kind_name,
        )

    @property
    def data_version(self) -> SnapshotDataVersion:
        self._ensure_categorized()
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            change_category=self.change_category,
            physical_schema=self.physical_schema,
        )

    @property
    def snapshot_intervals(self) -> SnapshotIntervals:
        self._ensure_categorized()
        return SnapshotIntervals(
            name=self.name,
            identifier=self.identifier,
            version=self.version,
            intervals=self.intervals.copy(),
            dev_intervals=self.dev_intervals.copy(),
        )

    @property
    def is_materialized_view(self) -> bool:
        """Returns whether or not this snapshot's model represents a materialized view."""
        return isinstance(self.model.kind, ViewKind) and self.model.kind.materialized

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        self._ensure_categorized()
        return self.fingerprint.to_version() == self.version

    @property
    def is_paused(self) -> bool:
        return self.unpaused_ts is None

    @property
    def normalized_effective_from_ts(self) -> t.Optional[int]:
        return (
            to_timestamp(self.model.cron_floor(self.effective_from))
            if self.effective_from
            else None
        )

    @property
    def start(self) -> t.Optional[datetime]:
        time = self.model.start or self._start
        if time:
            return to_datetime(time)
        return None

    @property
    def model_kind_name(self) -> ModelKindName:
        return self.model.kind.name

    @property
    def depends_on_past(self) -> bool:
        """Whether or not this models depends on past intervals to be accurate before loading following intervals."""
        return self.model.depends_on_past

    def _ensure_categorized(self) -> None:
        if not self.change_category:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been categorized yet.")
        if not self.version:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been versioned yet.")


SnapshotIdLike = t.Union[SnapshotId, SnapshotTableInfo, Snapshot]
SnapshotInfoLike = t.Union[SnapshotTableInfo, Snapshot]
SnapshotNameVersionLike = t.Union[SnapshotNameVersion, SnapshotTableInfo, Snapshot]


def table_name(physical_schema: str, name: str, version: str, is_temp: bool = False) -> str:
    temp_suffx = "__temp" if is_temp else ""
    return f"{physical_schema}.{name.replace('.', '__')}__{version}{temp_suffx}"


def fingerprint_from_model(
    model: Model,
    *,
    models: t.Dict[str, Model],
    audits: t.Optional[t.Dict[str, Audit]] = None,
    cache: t.Optional[t.Dict[str, SnapshotFingerprint]] = None,
) -> SnapshotFingerprint:
    """Helper function to generate a fingerprint based on a model's query and environment.

    This method tries to remove non meaningful differences to avoid ever changing fingerprints.
    The fingerprint is made up of two parts split by an underscore -- query_metadata. The query hash is
    determined purely by the rendered query and the metadata by everything else.

    Args:
        model: Model to fingerprint.
        models: Dictionary of all models in the graph to make the fingerprint dependent on parent changes.
            If no dictionary is passed in the fingerprint will not be dependent on a model's parents.
        audits: Available audits by name.
        cache: Cache of model name to fingerprints.

    Returns:
        The fingerprint.
    """
    cache = {} if cache is None else cache

    if model.name not in cache:
        parents = [
            fingerprint_from_model(
                models[table],
                models=models,
                audits=audits,
                cache=cache,
            )
            for table in model.depends_on
            if table in models
        ]

        parent_data_hash = _hash(sorted(p.to_version() for p in parents))

        parent_metadata_hash = _hash(
            sorted(h for p in parents for h in (p.metadata_hash, p.parent_metadata_hash))
        )

        cache[model.name] = SnapshotFingerprint(
            data_hash=_model_data_hash(model),
            metadata_hash=_model_metadata_hash(model, audits or {}),
            parent_data_hash=parent_data_hash,
            parent_metadata_hash=parent_metadata_hash,
        )

    return cache[model.name]


def _model_data_hash(model: Model) -> str:
    data = [
        str(model.sorted_python_env),
        model.kind.name,
        model.cron,
        model.storage_format,
        str(model.lookback),
        *(expr.sql() for expr in (model.partitioned_by or [])),
        *(model.clustered_by or []),
        model.stamp,
    ]

    if isinstance(model, _SqlBasedModel):
        pre_statements = (
            model.pre_statements if model.hash_raw_query else model.render_pre_statements()
        )
        post_statements = (
            model.post_statements if model.hash_raw_query else model.render_post_statements()
        )
        macro_defs = model.macro_definitions if model.hash_raw_query else []
    else:
        pre_statements = []
        post_statements = []
        macro_defs = []

    if isinstance(model, SqlModel):
        query = model.query if model.hash_raw_query else model.render_query() or model.query

        for e in (query, *pre_statements, *post_statements, *macro_defs):
            data.append(e.sql(comments=False))

        for macro_name, macro in sorted(model.jinja_macros.root_macros.items(), key=lambda x: x[0]):
            data.append(macro_name)
            data.append(macro.definition)

        for _, package in sorted(model.jinja_macros.packages.items(), key=lambda x: x[0]):
            for macro_name, macro in sorted(package.items(), key=lambda x: x[0]):
                data.append(macro_name)
                data.append(macro.definition)

    elif isinstance(model, PythonModel):
        data.append(model.entrypoint)
    elif isinstance(model, SeedModel):
        for e in (*pre_statements, *post_statements, *macro_defs):
            data.append(e.sql(comments=False))

        for column_name, column_hash in model.column_hashes.items():
            data.append(column_name)
            data.append(column_hash)

    for column_name, column_type in (model.columns_to_types_ or {}).items():
        data.append(column_name)
        data.append(column_type.sql())

    if isinstance(model.kind, kind.IncrementalByTimeRangeKind):
        data.append(model.kind.time_column.column)
        data.append(model.kind.time_column.format)
    elif isinstance(model.kind, kind.IncrementalByUniqueKeyKind):
        data.extend(model.kind.unique_key)

    return _hash(data)


def _model_metadata_hash(model: Model, audits: t.Dict[str, Audit]) -> str:
    metadata = [
        model.dialect,
        model.owner,
        model.description,
        str(model.start) if model.start else None,
        str(model.retention) if model.retention else None,
        str(model.batch_size) if model.batch_size is not None else None,
        json.dumps(model.mapping_schema, sort_keys=True),
        *sorted(model.tags),
        *sorted(model.grain),
        str(model.forward_only),
        str(model.disable_restatement),
    ]

    for audit_name, audit_args in sorted(model.audits, key=lambda a: a[0]):
        metadata.append(audit_name)

        if audit_name in BUILT_IN_AUDITS:
            for arg_name, arg_value in audit_args.items():
                metadata.append(arg_name)
                metadata.append(arg_value.sql(comments=True))
        elif audit_name in audits:
            audit = audits[audit_name]
            query = (
                audit.query
                if model.hash_raw_query
                else audit.render_query(model, **t.cast(t.Dict[str, t.Any], audit_args))
                or audit.query
            )
            metadata.extend(
                [
                    query.sql(comments=True),
                    audit.dialect,
                    str(audit.skip),
                    str(audit.blocking),
                ]
            )
        else:
            raise SQLMeshError(f"Unexpected audit name '{audit_name}'.")

    # Add comments from the model query.
    if model.is_sql:
        rendered_query = model.render_query()
        if rendered_query:
            for e, _, _ in rendered_query.walk():
                if e.comments:
                    metadata.extend(e.comments)

    return _hash(metadata)


def _hash(data: t.Iterable[t.Optional[str]]) -> str:
    return crc32(data)


def _parents_from_model(
    model: Model,
    models: t.Dict[str, Model],
) -> t.Set[str]:
    parent_tables = set()
    for table in model.depends_on:
        if table in models:
            parent_tables.add(table)
            if models[table].kind.is_embedded:
                parent_tables.update(_parents_from_model(models[table], models))

    return parent_tables


def merge_intervals(intervals: Intervals) -> Intervals:
    """Merge a list of intervals.

    Args:
        intervals: A list of intervals to merge together.

    Returns:
        A new list of sorted and merged intervals.
    """
    intervals = sorted(intervals)

    merged = [intervals[0]]

    for interval in intervals[1:]:
        current = merged[-1]

        if interval[0] <= current[1]:
            merged[-1] = (current[0], max(current[1], interval[1]))
        else:
            merged.append(interval)

    return merged


def _format_date_time(time_like: TimeLike, unit: t.Optional[IntervalUnit]) -> str:
    if unit is None or unit.is_date_granularity:
        return to_ds(time_like)
    return to_datetime(time_like).isoformat()[:19]


def format_intervals(intervals: Intervals, unit: t.Optional[IntervalUnit]) -> str:
    inclusive_intervals = [make_inclusive(start, end) for start, end in intervals]
    return ", ".join(
        " - ".join([_format_date_time(start, unit), _format_date_time(end, unit)])
        for start, end in inclusive_intervals
    )


def remove_interval(intervals: Intervals, remove_start: int, remove_end: int) -> Intervals:
    """Remove an interval from a list of intervals. Assumes that the correct start and end intervals have been
    passed in. Use `get_remove_interval` method of `Snapshot` to get the correct start/end given the snapshot's
    information.

    Args:
        intervals: A list of exclusive intervals.
        remove_start: The inclusive start to remove.
        remove_end: The exclusive end to remove.

    Returns:
        A new list of intervals.
    """
    modified: Intervals = []

    for start, end in intervals:
        if remove_start > start and remove_end < end:
            modified.extend(
                (
                    (start, remove_start),
                    (remove_end, end),
                )
            )
        elif remove_start > start:
            modified.append((start, min(remove_start, end)))
        elif remove_end < end:
            modified.append((max(remove_end, start), end))

    return modified


def to_table_mapping(snapshots: t.Iterable[Snapshot], is_dev: bool) -> t.Dict[str, str]:
    return {
        snapshot.name: snapshot.table_name_for_mapping(is_dev=is_dev)
        for snapshot in snapshots
        if snapshot.version and not snapshot.is_symbolic
    }


def has_paused_forward_only(
    targets: t.Iterable[SnapshotIdLike],
    snapshots: t.Union[t.List[Snapshot], t.Dict[SnapshotId, Snapshot]],
) -> bool:
    if not isinstance(snapshots, dict):
        snapshots = {s.snapshot_id: s for s in snapshots}
    for target in targets:
        target_snapshot = snapshots[target.snapshot_id]
        if target_snapshot.is_paused and target_snapshot.is_forward_only:
            return True
    return False
