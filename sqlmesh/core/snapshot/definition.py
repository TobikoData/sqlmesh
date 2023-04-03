from __future__ import annotations

import typing as t
import zlib
from collections import defaultdict
from enum import IntEnum

from croniter import croniter_range
from pydantic import validator
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.audit import BUILT_IN_AUDITS, Audit
from sqlmesh.core.model import (
    Model,
    PythonModel,
    SeedModel,
    SqlModel,
    kind,
    parse_model_name,
)
from sqlmesh.core.model.meta import HookCall
from sqlmesh.utils.date import (
    TimeLike,
    is_date,
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
    FORWARD_ONLY = 3


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


class SnapshotDataVersion(PydanticModel, frozen=True):
    fingerprint: SnapshotFingerprint
    version: str
    change_category: t.Optional[SnapshotChangeCategory]

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
        schema = self.schema_name or "default"
        if environment.lower() != c.PROD:
            schema = f"{schema}__{environment}"
        return schema


class SnapshotInfoMixin:
    name: str
    fingerprint: SnapshotFingerprint
    physical_schema: str
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()

    def is_temporary_table(self, is_dev: bool) -> bool:
        """Provided whether the snapshot is used in a development mode or not, returns True
        if the snapshot targets a temporary table or a clone and False otherwise.
        """
        return is_dev and not self.is_new_version

    @property
    def identifier(self) -> str:
        return self.fingerprint.to_identifier()

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

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
    def is_new_version(self) -> bool:
        raise NotImplementedError

    @property
    def is_forward_only(self) -> bool:
        return not self.data_hash_matches(self.previous_version) and not self.is_new_version

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
            # If this snapshot is used for reading, return a temporary table
            # only if this snapshot captures direct changes applied to its model.
            version = self.fingerprint.to_version() if self.is_forward_only else version
            is_temp = self.is_temporary_table(True) and self.is_forward_only
        elif is_dev:
            version = self.fingerprint.to_version()
            is_temp = self.is_temporary_table(True)
        else:
            is_temp = False

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
    physical_schema: str
    parents: t.Tuple[SnapshotId, ...]
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    change_category: t.Optional[SnapshotChangeCategory]
    is_materialized: bool
    is_embedded_kind: bool

    def table_name(self, is_dev: bool = False, for_read: bool = False) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        return self._table_name(self.version, is_dev, for_read)

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
        return self.fingerprint.to_version() == self.version


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
    fingerprint: SnapshotFingerprint
    physical_schema: str
    model: Model
    parents: t.Tuple[SnapshotId, ...]
    audits: t.Tuple[Audit, ...]
    intervals: Intervals
    dev_intervals: Intervals
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
                physical_schema=physical_schema,
                models=models,
                audits=audits,
                cache=cache,
            ),
            physical_schema=physical_schema,
            model=model,
            parents=tuple(
                SnapshotId(
                    name=name,
                    identifier=fingerprint_from_model(
                        models[name],
                        physical_schema=physical_schema,
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
            created_ts=created_ts,
            updated_ts=created_ts,
            ttl=ttl,
            version=version,
        )

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, Snapshot) and self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        return hash((self.__class__, self.fingerprint))

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
        is_temp_table = self.is_temporary_table(is_dev)
        intervals = self.dev_intervals if is_temp_table else self.intervals

        intervals.append(self._inclusive_exclusive(start, end))

        if len(intervals) < 2:
            return

        merged_intervals = merge_intervals(intervals)
        if is_temp_table:
            self.dev_intervals = merged_intervals
        else:
            self.intervals = merged_intervals

    def remove_interval(self, start: TimeLike, end: TimeLike) -> None:
        """Remove an interval from the snapshot.

        Args:
            start: Start interval to remove.
            end: End interval to remove.
        """
        interval = self._inclusive_exclusive(start, end)
        self.intervals = remove_interval(self.intervals, *interval)
        self.dev_intervals = remove_interval(self.dev_intervals, *interval)

    def _inclusive_exclusive(self, start: TimeLike, end: TimeLike) -> t.Tuple[int, int]:
        start_ts = to_timestamp(self.model.cron_floor(start))
        end_ts = to_timestamp(
            self.model.cron_next(end) if is_date(end) else self.model.cron_floor(end)
        )

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
            latest: The date/time to use for latest (inclusive)

        Returns:
            A list of all the missing intervals as epoch timestamps.
        """
        if self.is_embedded_kind:
            return []

        if self.is_full_kind or self.is_view_kind or self.is_seed_kind:
            latest = latest or now()

            latest_start, latest_end = self._inclusive_exclusive(
                latest if is_date(latest) else self.model.cron_prev(self.model.cron_floor(latest)),
                latest,
            )
            # if the latest ts is stored in the last interval, nothing is missing
            # else returns the latest ts with the exclusive end ts.
            if self.intervals and self.intervals[-1][1] >= latest_end:
                return []
            return [(latest_start, latest_end)]

        missing = []
        start_dt, end_dt = (to_datetime(ts) for ts in self._inclusive_exclusive(start, end))
        dates = tuple(croniter_range(start_dt, end_dt, self.model.normalized_cron()))
        size = len(dates) - 1

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
        version: t.Optional[str | SnapshotDataVersion | SnapshotTableInfo | Snapshot] = None,
    ) -> None:
        """Set the version of this snapshot.

        If no version is passed, the fingerprint of the snapshot will be used.

        Args:
            version: Either a string or a TableInfo to use.
        """
        if isinstance(version, (SnapshotDataVersion, SnapshotTableInfo, Snapshot)):
            self.version = version.data_version.version
        else:
            self.version = version or self.fingerprint.to_version()

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
        self._ensure_version()
        assert self.version
        return self._table_name(self.version, is_dev, for_read)

    def table_name_for_mapping(self, is_dev: bool = False) -> str:
        """Full table name used by a child snapshot for table mapping during evaluation.

        Args:
            is_dev: Whether the table name will be used in development mode.
        """
        self._ensure_version()
        assert self.version

        if is_dev and self.is_forward_only:
            # If this snapshot is unpaused we shouldn't be using a temporary
            # table for mapping purposes.
            is_dev = self.is_paused

        return self._table_name(self.version, is_dev, True)

    def version_get_or_generate(self) -> str:
        """Helper method to get the version or generate it from the fingerprint."""
        return self.version or self.fingerprint.to_version()

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to get the SnapshotTableInfo from the Snapshot."""
        self._ensure_version()
        return SnapshotTableInfo(
            physical_schema=self.physical_schema,
            name=self.name,
            fingerprint=self.fingerprint,
            version=self.version,
            parents=self.parents,
            previous_versions=self.previous_versions,
            change_category=self.change_category,
            is_materialized=self.is_materialized,
            is_embedded_kind=self.is_embedded_kind,
        )

    @property
    def data_version(self) -> SnapshotDataVersion:
        self._ensure_version()
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            change_category=self.change_category,
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        self._ensure_version()
        return self.fingerprint.to_version() == self.version

    @property
    def is_full_kind(self) -> bool:
        return self.model.kind.is_full

    @property
    def is_view_kind(self) -> bool:
        return self.model.kind.is_view

    @property
    def is_incremental_by_time_range_kind(self) -> bool:
        return self.model.kind.is_incremental_by_time_range

    @property
    def is_incremental_by_unique_key_kind(self) -> bool:
        return self.model.kind.is_incremental_by_unique_key

    # @property
    # def is_snapshot_kind(self) -> bool:
    #     return self.model.kind.is_snapshot

    @property
    def is_embedded_kind(self) -> bool:
        return self.model.kind.is_embedded

    @property
    def is_seed_kind(self) -> bool:
        return self.model.kind.is_seed

    @property
    def is_materialized(self) -> bool:
        return self.model.kind.is_materialized

    @property
    def is_paused(self) -> bool:
        return self.unpaused_ts is None

    def _ensure_version(self) -> None:
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
    physical_schema: str = "",
    audits: t.Optional[t.Dict[str, Audit]] = None,
    cache: t.Optional[t.Dict[str, SnapshotFingerprint]] = None,
) -> SnapshotFingerprint:
    """Helper function to generate a fingerprint based on a model's query and environment.

    This method tries to remove non meaningful differences to avoid ever changing fingerprints.
    The fingerprint is made up of two parts split by an underscore -- query_metadata. The query hash is
    determined purely by the rendered query and the metadata by everything else.

    Args:
        model: Model to fingerprint.
        physical_schema: The physical_schema of the snapshot which represents where it is stored.
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
                physical_schema=physical_schema,
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
            data_hash=_model_data_hash(model, physical_schema),
            metadata_hash=_model_metadata_hash(model, audits or {}),
            parent_data_hash=parent_data_hash,
            parent_metadata_hash=parent_metadata_hash,
        )

    return cache[model.name]


def _model_data_hash(model: Model, physical_schema: str) -> str:
    def serialize_hooks(hooks: t.List[HookCall]) -> t.Iterable[str]:
        serialized = []
        for hook in hooks:
            if isinstance(hook, exp.Expression):
                serialized.append(hook.sql())
            else:
                name, args = hook
                serialized.append(
                    f"{name}:"
                    + ",".join(
                        f"{k}={v.sql(identify=True, comments=False)}"
                        for k, v in sorted(args.items())
                    )
                )
        return serialized

    data = [
        str(model.sorted_python_env),
        model.kind.name,
        model.cron,
        model.storage_format,
        physical_schema,
        *(model.partitioned_by or []),
        *(expression.sql(identify=True, comments=False) for expression in model.expressions or []),
        *serialize_hooks(model.pre),
        *serialize_hooks(model.post),
        model.stamp,
    ]

    if isinstance(model, SqlModel):
        data.append(model.query.sql(identify=True, comments=False))

        for macro_name, macro in sorted(model.jinja_macros.root_macros.items(), key=lambda x: x[0]):
            data.append(macro_name)
            data.append(macro.definition)

        for package in model.jinja_macros.packages.values():
            for macro_name, macro in sorted(package.items(), key=lambda x: x[0]):
                data.append(macro_name)
                data.append(macro.definition)
    elif isinstance(model, PythonModel):
        data.append(model.entrypoint)
        for column_name, column_type in model.columns_to_types.items():
            data.append(column_name)
            data.append(str(column_type))
    elif isinstance(model, SeedModel):
        data.append(str(model.kind.batch_size))
        data.append(model.seed.content)
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
        str(to_timestamp(model.start)) if model.start else None,
        str(model.batch_size) if model.batch_size is not None else None,
    ]

    for audit_name, audit_args in sorted(model.audits, key=lambda a: a[0]):
        metadata.append(audit_name)

        if audit_name in BUILT_IN_AUDITS:
            for arg_name, arg_value in audit_args.items():
                metadata.append(arg_name)
                metadata.append(arg_value.sql(identify=True, comments=True))
        elif audit_name in audits:
            audit = audits[audit_name]
            metadata.extend(
                [
                    audit.render_query(model, **t.cast(t.Dict[str, t.Any], audit_args)).sql(
                        identify=True, comments=True
                    ),
                    audit.dialect,
                    str(audit.skip),
                    str(audit.blocking),
                ]
            )
        else:
            raise SQLMeshError(f"Unexpected audit name '{audit_name}'.")

    # Add comments from the model query.
    for e, _, _ in model.render_query().walk():
        if e.comments:
            metadata.extend(e.comments)

    return _hash(metadata)


def _hash(data: t.Iterable[t.Optional[str]]) -> str:
    return str(zlib.crc32(";".join("" if d is None else d for d in data).encode("utf-8")))


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


def remove_interval(intervals: Intervals, remove_start: int, remove_end: int) -> Intervals:
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


def to_table_mapping(snapshots: t.Iterable[Snapshot], is_dev: bool) -> t.Dict[str, str]:
    return {
        snapshot.name: snapshot.table_name_for_mapping(is_dev=is_dev)
        for snapshot in snapshots
        if snapshot.version and not snapshot.is_embedded_kind
    }
