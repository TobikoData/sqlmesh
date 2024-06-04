from __future__ import annotations

import json
import re
import typing as t

from pydantic import Field

from sqlmesh.core import constants as c
from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.snapshot import SnapshotId, SnapshotTableInfo
from sqlmesh.utils import word_characters_only
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel, field_validator

T = t.TypeVar("T", bound="EnvironmentNamingInfo")


class EnvironmentNamingInfo(PydanticModel):
    """
    Information required for creating an object within an environment

    Args:
        name: The name of the environment.
        suffix_target: Indicates whether to append the environment name to the schema or table name.
        catalog_name_override: The name of the catalog to use for this environment if an override was provided
        normalize_name: Indicates whether the environment's name will be normalized. For example, if it's
            `dev`, then it will become `DEV` when targeting Snowflake.
    """

    name: str = c.PROD
    suffix_target: EnvironmentSuffixTarget = Field(default=EnvironmentSuffixTarget.SCHEMA)
    catalog_name_override: t.Optional[str] = None
    normalize_name: bool = True

    @field_validator("name", mode="before")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        return word_characters_only(v).lower()

    @field_validator("normalize_name", mode="before")
    @classmethod
    def _validate_normalize_name(cls, v: t.Any) -> bool:
        return True if v is None else bool(v)

    @t.overload
    @classmethod
    def sanitize_name(cls, v: str) -> str: ...

    @t.overload
    @classmethod
    def sanitize_name(cls, v: Environment) -> Environment: ...

    @classmethod
    def sanitize_name(cls, v: str | Environment) -> str | Environment:
        """
        Sanitizes the environment name so we create names that are valid names for database objects.
        This means alphanumeric and underscores only. Invalid characters are replaced with underscores.
        """
        if isinstance(v, Environment):
            return v
        if not isinstance(v, str):
            raise TypeError(f"Expected str or Environment, got {type(v).__name__}")
        return cls._sanitize_name(v)

    @classmethod
    def sanitize_names(cls, values: t.Iterable[str]) -> t.Set[str]:
        return {cls.sanitize_name(value) for value in values}

    @classmethod
    def from_environment_catalog_mapping(
        cls: t.Type[T],
        environment_catalog_mapping: t.Dict[re.Pattern, str],
        name: str = c.PROD,
        **kwargs: t.Any,
    ) -> T:
        construction_kwargs = dict(name=name, **kwargs)
        for re_pattern, catalog_name in environment_catalog_mapping.items():
            if re.match(re_pattern, name):
                return cls(
                    catalog_name_override=catalog_name,
                    **construction_kwargs,
                )
        return cls(**construction_kwargs)


class Environment(EnvironmentNamingInfo):
    """Represents an isolated environment.

    Environments are isolated workspaces that hold pointers to physical tables.

    Args:
        snapshots: The snapshots that are part of this environment.
        start_at: The start time of the environment.
        end_at: The end time of the environment.
        plan_id: The ID of the plan that last updated this environment.
        previous_plan_id: The ID of the previous plan that updated this environment.
        expiration_ts: The timestamp when this environment will expire.
        finalized_ts: The timestamp when this environment was finalized.
        promoted_snapshot_ids: The IDs of the snapshots that are promoted in this environment
            (i.e. for which the views are created). If not specified, all snapshots are promoted.
        previous_finalized_snapshots: Snapshots that were part of this environment last time it was finalized.
    """

    snapshots: t.List[SnapshotTableInfo]
    start_at: TimeLike
    end_at: t.Optional[TimeLike] = None
    plan_id: str
    previous_plan_id: t.Optional[str] = None
    expiration_ts: t.Optional[int] = None
    finalized_ts: t.Optional[int] = None
    promoted_snapshot_ids: t.Optional[t.List[SnapshotId]] = None
    previous_finalized_snapshots: t.Optional[t.List[SnapshotTableInfo]] = None

    @field_validator("snapshots", "previous_finalized_snapshots", mode="before")
    @classmethod
    def _convert_snapshots(
        cls, v: str | t.List[SnapshotTableInfo] | None
    ) -> t.List[SnapshotTableInfo] | None:
        if isinstance(v, str):
            return [SnapshotTableInfo.parse_obj(obj) for obj in json.loads(v)]
        return v

    @field_validator("promoted_snapshot_ids", mode="before")
    @classmethod
    def _convert_snapshot_ids(cls, v: str | t.List[SnapshotId]) -> t.List[SnapshotId]:
        if isinstance(v, str):
            return [SnapshotId.parse_obj(obj) for obj in json.loads(v)]
        return v

    @property
    def promoted_snapshots(self) -> t.List[SnapshotTableInfo]:
        if self.promoted_snapshot_ids is None:
            return self.snapshots

        promoted_snapshot_ids = set(self.promoted_snapshot_ids)
        return [s for s in self.snapshots if s.snapshot_id in promoted_snapshot_ids]

    @property
    def finalized_or_current_snapshots(self) -> t.List[SnapshotTableInfo]:
        return (
            self.snapshots
            if self.finalized_ts
            else self.previous_finalized_snapshots or self.snapshots
        )

    @property
    def naming_info(self) -> EnvironmentNamingInfo:
        return EnvironmentNamingInfo(
            name=self.name,
            suffix_target=self.suffix_target,
            catalog_name_override=self.catalog_name_override,
            normalize_name=self.normalize_name,
        )
