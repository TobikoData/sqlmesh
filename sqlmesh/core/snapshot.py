"""
# Snapshot

A snapshot is a record of a model at a given time. Along with a copy of the model, a snapshot also
contains everything needed to evaluate the model and render its query. This allows SQLMesh to have
a consistent view of your project's history and its data as the project and its models evolve and change. Since model queries can have macros, each snapshot stores a copy of all macro definitions and
global variables at the time the snapshot is taken. Additionally, snapshots store the intervals of
time that they have data for.

# Fingerprints

Snapshots have unique fingerprints that are derived from their models. SQLMesh use these fingerprints
to determine when existing tables can be reused or whether a backfill is needed because a model's query
has changed. Because SQLMesh can understand SQL with SQLGlot, it can generate fingerprints in more
sophisticated ways where superficial changes to a model, e.g. applying formatting to its query, will not
return a new fingerprint since nothing was meaningfully changed.

For more information on how SQLmesh generates model fingerprints, see
`sqlmesh.core.snapshot.fingerprint_from_model`

# Change Categories

See `sqlmesh.core.plan`
"""
from __future__ import annotations

import typing as t
import zlib
from collections import defaultdict
from enum import IntEnum

from croniter import croniter_range
from pydantic import validator

from sqlmesh.core import constants as c
from sqlmesh.core.model import Model, parse_model_name
from sqlmesh.utils.date import (
    TimeLike,
    make_inclusive,
    now,
    now_timestamp,
    to_datetime,
    to_timestamp,
)
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

Interval = t.Tuple[int, int]
Intervals = t.List[Interval]


class SnapshotChangeCategory(IntEnum):
    """
    Values are ordered by decreasing severity and that ordering is required.

    BREAKING: The change requires that snapshot modified and downstream dependencies be rebuilt
    NON_BREAKING: The change requires that only the snapshot modified be rebuilt
    NO_CHANGE: The change requires no rebuilding
    """

    BREAKING = 1
    NON_BREAKING = 2
    NO_CHANGE = 3


class FingerprintMixin:
    fingerprint: str

    @property
    def data_hash(self):
        return self._hashes[0]

    @property
    def parent_hash(self):
        return self._hashes[1]

    @property
    def _hashes(self) -> tuple[str, str]:
        data_hash, parent_hash = self.fingerprint.split("_")
        return (data_hash, parent_hash)

    def data_hash_matches(self, other: t.Optional[FingerprintMixin]) -> bool:
        return other is not None and self.data_hash == other.data_hash


class SnapshotId(PydanticModel, FingerprintMixin, frozen=True):
    name: str
    fingerprint: str

    @property
    def snapshot_id(self) -> SnapshotId:
        """Helper method to return self."""
        return self


class SnapshotDataVersion(PydanticModel, FingerprintMixin, frozen=True):
    fingerprint: str
    version: str
    change_category: t.Optional[SnapshotChangeCategory]

    @property
    def data_version(self) -> SnapshotDataVersion:
        return self

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint == self.version


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
        schema = self.schema_name or "default"
        if environment.lower() != c.PROD:
            schema = f"{schema}__{environment}"
        return schema


class SnapshotInfoMixin(FingerprintMixin):
    name: str
    fingerprint: str
    physical_schema: str
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, fingerprint=self.fingerprint)

    @property
    def qualified_view_name(self) -> QualifiedViewName:
        (catalog, schema, table) = parse_model_name(self.name)
        return QualifiedViewName(catalog=catalog, schema_name=schema, table=table)

    @property
    def previous_version(self) -> t.Optional[SnapshotDataVersion]:
        """Helper method to get the previous data version."""
        if self.previous_versions:
            return self.previous_versions[-1]
        return None

    @property
    def data_version(self) -> SnapshotDataVersion:
        raise NotImplementedError

    @property
    def all_versions(self) -> t.Tuple[SnapshotDataVersion, ...]:
        """Returns previous versions with the current version trimmed to DATA_VERSION_LIMIT."""
        return (*self.previous_versions, self.data_version)[-c.DATA_VERSION_LIMIT :]


class SnapshotTableInfo(PydanticModel, SnapshotInfoMixin, frozen=True):
    name: str
    fingerprint: str
    version: str
    physical_schema: str
    parents: t.Tuple[SnapshotId, ...]
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    change_category: t.Optional[SnapshotChangeCategory]

    @property
    def table_name(self) -> str:
        """Returns the physical location of this snapshot."""
        return table_name(self.physical_schema, self.name, self.version)

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to return self."""
        return self

    @property
    def data_version(self) -> SnapshotDataVersion:
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            change_category=self.change_category,
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint == self.version


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
        intervals: List of [start, end) intervals showing which time ranges a snapshot has data for.
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
    """

    name: str
    fingerprint: str
    physical_schema: str
    model: Model
    parents: t.Tuple[SnapshotId, ...]
    intervals: Intervals
    created_ts: int
    updated_ts: int
    ttl: str
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    indirect_versions: t.Dict[str, t.Tuple[SnapshotDataVersion, ...]] = {}
    version: t.Optional[str] = None
    change_category: t.Optional[SnapshotChangeCategory] = None
    unpaused_ts: t.Optional[int] = None

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
    def merge_snapshots(
        targets: t.Iterable[SnapshotIdLike],
        snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> t.List[Snapshot]:
        """Merge target snapshots with others so that each target snapshot has intervals from all other snapshots with the same version.

        Args:
            targets: Iterable of snapshot-like objects
            snapshots: Dictionary of snapshot ids to snapshot.

        Returns:
            List of target snapshots with merged intervals.
        """
        merged = []
        snapshots_by_name_version = defaultdict(list)

        for s in snapshots.values():
            snapshots_by_name_version[(s.name, s.version)].append(s)

        for snapshot_like in targets:
            snapshot_id = snapshot_like.snapshot_id
            snapshot = snapshots.get(snapshot_id)
            if not snapshot:
                raise SQLMeshError(f"The snapshot {snapshot_id} was not found")

            snapshot = snapshot.copy()
            snapshot.intervals = []

            for other in snapshots_by_name_version[(snapshot.name, snapshot.version)]:
                snapshot.merge_intervals(other)

            merged.append(snapshot)

        return merged

    @classmethod
    def from_model(
        cls,
        model: Model,
        *,
        physical_schema: str,
        models: t.Dict[str, Model],
        ttl: str = c.DEFAULT_SNAPSHOT_TTL,
        version: t.Optional[str] = None,
        cache: t.Optional[t.Dict[str, str]] = None,
    ) -> Snapshot:
        """Creates a new snapshot for a model.

        Args:
            model: Model to snapshot.
            physical_schema: The schema of the snapshot which represents where it is stored.
            models: Dictionary of all models in the graph to make the fingerprint dependent on parent changes.
                If no dictionary is passed in the fingerprint will not be dependent on a model's parents.
            ttl: A TTL to determine how long orphaned (snapshots that are not promoted anywhere) should live.
            version: The version that a snapshot is associated with. Usually set during the planning phase.
            cache: Cache of model name to fingerprints.

        Returns:
            The newly created snapshot.
        """
        created_ts = now_timestamp()

        return cls(
            name=model.name,
            fingerprint=fingerprint_from_model(
                model,
                physical_schema=physical_schema,
                models=models,
                cache=cache,
            ),
            physical_schema=physical_schema,
            model=model,
            parents=tuple(
                SnapshotId(
                    name=name,
                    fingerprint=fingerprint_from_model(
                        models[name],
                        physical_schema=physical_schema,
                        models=models,
                        cache=cache,
                    ),
                )
                for name in _parents_from_model(model, models)
            ),
            intervals=[],
            created_ts=created_ts,
            updated_ts=created_ts,
            ttl=ttl,
            version=version,
        )

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, Snapshot) and self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        return hash((self.__class__, self.fingerprint))

    def add_interval(self, start: TimeLike, end: TimeLike) -> None:
        """Add a newly processed time interval to the snapshot.

        The actual stored intervals are [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive. This allows merging of ranges to be easier.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval. If end is a date, then it is considered inclusive.
                If it is a datetime object, then it is exclusive.
        """
        self.intervals.append(self._inclusive_exclusive(start, end))

        if len(self.intervals) < 2:
            return

        self.intervals = merge_intervals(self.intervals)

    def remove_interval(self, start: TimeLike, end: TimeLike) -> None:
        """Remove an interval from the snapshot.

        Args:
            start: Start interval to remove.
            end: End interval to remove.
        """
        self.intervals = remove_interval(
            self.intervals, *self._inclusive_exclusive(start, end)
        )

    def _inclusive_exclusive(self, start: TimeLike, end: TimeLike) -> t.Tuple[int, int]:
        start_dt, end_dt = make_inclusive(start, end)
        start_ts = to_timestamp(self.model.cron_floor(start_dt))
        end_ts = to_timestamp(self.model.cron_next(end_dt))

        if start_ts >= end_ts:
            raise ValueError("`end` must be >= `start`")
        return (start_ts, end_ts)

    def merge_intervals(self, other: Snapshot) -> None:
        """Inherits intervals from the target snapshot.

        Args:
            other: The target snapshot to inherit intervals from.
        """
        for start, end in other.intervals:
            self.add_interval(start, end)

    def missing_intervals(
        self, start: TimeLike, end: TimeLike, latest: t.Optional[TimeLike] = None
    ) -> Intervals:
        """Find all missing intervals between [start, end].

        Although the inputs are inclusive, the returned stored intervals are
        [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive)

        Returns:
            A list of all the missing intervals as epoch timestamps.
        """
        if self.is_embedded_kind:
            return []

        start_dt, end_dt = make_inclusive(start, self.model.cron_floor(end))

        if self.is_full_kind or self.is_view_kind:
            latest_dt = to_datetime(self.model.cron_floor(latest or now()))
            latest_ts = to_timestamp(latest_dt)
            # if the latest ts is stored in the last interval, nothing is missing
            # else returns the latest ts with the exclusive end ts.
            if self.intervals and self.intervals[-1][1] >= latest_ts:
                return []
            return [(to_timestamp(self.model.cron_prev(latest_dt)), latest_ts)]

        missing = []
        dates = list(croniter_range(start_dt, end_dt, self.model.normalized_cron()))
        size = len(dates)

        for i in range(size):
            current_ts = to_timestamp(dates[i])
            end_ts = (
                to_timestamp(dates[i + 1])
                if i + 1 < size
                else to_timestamp(self.model.cron_next(current_ts))
            )

            for low, high in self.intervals:
                if current_ts < low:
                    missing.append((current_ts, end_ts))
                    break
                elif current_ts < high:
                    break
            else:
                missing.append((current_ts, end_ts))

        return missing

    def set_version(
        self,
        version: t.Optional[
            str | SnapshotDataVersion | SnapshotTableInfo | Snapshot
        ] = None,
    ):
        """Set the version of this snapshot.

        If no version is passed, the fingerprint of the snapshot will be used.

        Args:
            version: Either a string or a TableInfo to use.
        """
        if isinstance(version, (SnapshotDataVersion, SnapshotTableInfo, Snapshot)):
            self.version = version.data_version.version
        else:
            self.version = version or self.fingerprint

    def set_unpaused_ts(self, unpaused_dt: t.Optional[TimeLike]) -> None:
        """Sets the timestamp for when this snapshot was unpaused.

        Args:
            unpaused_dt: The datetime object of when this snapshot was unpaused.
        """
        self.unpaused_ts = (
            to_timestamp(self.model.cron_floor(unpaused_dt)) if unpaused_dt else None
        )

    @property
    def table_name(self) -> str:
        """Full table name pointing to the materialized location of the snapshot."""
        if not self.version:
            raise SQLMeshError(
                f"Snapshot {self.snapshot_id} has not been versioned yet."
            )
        return table_name(self.physical_schema, self.name, self.version)

    @property
    def snapshot_id(self) -> SnapshotId:
        """Helper method to get the SnapshotId from the Snapshot."""
        return SnapshotId(name=self.name, fingerprint=self.fingerprint)

    @property
    def version_or_fingerprint(self) -> str:
        """Helper method to get the the version or fingerprint."""
        return self.version or self.fingerprint

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to get the SnapshotTableInfo from the Snapshot."""
        if not self.version:
            raise SQLMeshError(
                f"Snapshot {self.snapshot_id} has not been versioned yet."
            )
        return SnapshotTableInfo(
            physical_schema=self.physical_schema,
            name=self.name,
            fingerprint=self.fingerprint,
            version=self.version,
            parents=self.parents,
            previous_versions=self.previous_versions,
            change_category=self.change_category,
        )

    @property
    def data_version(self) -> SnapshotDataVersion:
        if not self.version:
            raise SQLMeshError(
                f"Snapshot {self.snapshot_id} has not been versioned yet."
            )
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            change_category=self.change_category,
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        if not self.version:
            raise SQLMeshError(
                f"Snapshot {self.snapshot_id} has not been versioned yet."
            )
        return self.fingerprint == self.version

    @property
    def is_full_kind(self):
        return self.model.kind.is_full

    @property
    def is_view_kind(self):
        return self.model.kind.is_view

    @property
    def is_incremental_kind(self):
        return self.model.kind.is_incremental

    @property
    def is_snapshot_kind(self):
        return self.model.kind.is_snapshot

    @property
    def is_embedded_kind(self):
        return self.model.kind.is_embedded

    @property
    def is_materialized(self):
        return self.model.kind.is_materialized


SnapshotIdLike = t.Union[SnapshotId, SnapshotTableInfo, Snapshot]
SnapshotInfoLike = t.Union[SnapshotTableInfo, Snapshot]


def table_name(physical_schema: str, name: str, version: str) -> str:
    return f"{physical_schema}.{name.replace('.', '__')}__{version}"


def fingerprint_from_model(
    model: Model,
    *,
    models: t.Dict[str, Model],
    physical_schema: str = "",
    cache: t.Optional[t.Dict[str, str]] = None,
) -> str:
    """Helper function to generate a fingerprint based on a model's query and environment.

    This method tries to remove non meaningful differences to avoid ever changing fingerprints.
    The fingerprint is made up of two parts split by an underscore -- query_metadata. The query hash is
    determined purely by the rendered query and the metadata by everything else.

    Args:
        model: Model to fingerprint.
        physical_schema: The physical_schema of the snapshot which represents where it is stored.
        models: Dictionary of all models in the graph to make the fingerprint dependent on parent changes.
            If no dictionary is passed in the fingerprint will not be dependent on a model's parents.
        cache: Cache of model name to fingerprints.

    Returns:
        The fingerprint.
    """
    cache = {} if cache is None else cache

    if model.name not in cache:
        data = [
            model.render_query().sql(identify=True, comments=False),
            str(sorted(model.python_env.items())),
            model.kind.name,
            model.cron,
            model.storage_format,
            physical_schema,
            *(model.partitioned_by or []),
            *(
                expression.sql(identify=True, comments=False)
                for expression in model.expressions or []
            ),
        ]

        parents = sorted(
            fingerprint_from_model(
                models[table],
                models=models,
                physical_schema=physical_schema,
                cache=cache,
            )
            for table in model.depends_on
            if table in models
        )

        cache[model.name] = "_".join((_hash(data), _hash(parents)))
    return cache[model.name]


def _hash(data: t.Iterable[t.Optional[str]]) -> str:
    return str(
        zlib.crc32(";".join("" if d is None else d for d in data).encode("utf-8"))
    )


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


def remove_interval(
    intervals: Intervals, remove_start: int, remove_end: int
) -> Intervals:
    """Remove an interval from a list of intervals.

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
