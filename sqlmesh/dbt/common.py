from __future__ import annotations

import typing as t
from enum import Enum, auto

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

T = t.TypeVar("T", bound="BaseConfig")


class UpdateStrategy(Enum):
    """Supported strategies for adding new config to existing config"""

    REPLACE = auto()  # Replace with new value
    APPEND = auto()  # Append list to existing list
    KEY_UPDATE = auto()  # Update dict key value with new dict key value
    KEY_APPEND = auto()  # Append dict key value to existing dict key value
    IMMUTABLE = auto()  # Raise if a key tries to change this value


def update_field(
    old: t.Optional[t.Any],
    new: t.Any,
    update_strategy: t.Optional[UpdateStrategy] = None,
) -> t.Any:
    """
    Update config field with new config value

    Args:
        old: The existing config value
        new: The new config value
        update_strategy: The strategy to use when updating the field

    Returns:
        The updated field
    """
    if not old:
        return new

    update_strategy = update_strategy or UpdateStrategy.REPLACE

    if update_strategy == UpdateStrategy.IMMUTABLE:
        raise ConfigError("Cannot modify property: {old}")

    if update_strategy == UpdateStrategy.REPLACE:
        return new
    if update_strategy == UpdateStrategy.APPEND:
        if not isinstance(old, list) or not isinstance(new, list):
            raise ConfigError("APPEND behavior requires list field")

        return old + new
    if update_strategy == UpdateStrategy.KEY_UPDATE:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        combined = old.copy()
        combined.update(new)
        return combined
    if update_strategy == UpdateStrategy.KEY_APPEND:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_APPEND behavior requires dictionary field")

        combined = old.copy()
        for key, value in new.items():
            if not isinstance(value, list):
                raise ConfigError(
                    "KEY_APPEND behavior requires list values in dictionary"
                )

            old_value = combined.get(key)
            if old_value:
                if not isinstance(old_value, list):
                    raise ConfigError(
                        "KEY_APPEND behavior requires list values in dictionary"
                    )

                combined[key] = old_value + value
            else:
                combined[key] = value

        return combined

    raise ConfigError(f"Unknown update strategy {update_strategy}")


class BaseConfig(PydanticModel):
    """
    Base configuration functionality for DBT configuration classes
    """

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {}

    class Config:
        extra = "allow"
        allow_mutation = True

    def update_with(self: T, config: t.Dict[str, t.Any]) -> T:
        """
        Update this instance's fields with the passed in config fields and return a new instance

        Args:
            config: Dict of config fields

        Returns:
            New instance updated with the passed in config fields
        """
        copy = self.copy()
        other = self.__class__(**config)

        for field in other.__fields_set__:
            if field in copy.__fields__:
                setattr(
                    copy,
                    field,
                    update_field(
                        getattr(copy, field),
                        getattr(other, field),
                        self._FIELD_UPDATE_STRATEGY.get(field),
                    ),
                )
            else:
                setattr(copy, field, getattr(other, field))

        return copy

    def replace(self, other: T) -> None:
        """
        Replace the contents of this instance with the passed in instance.

        Args:
            other: The instance to apply to this instance
        """
        for field in other.__fields_set__:
            setattr(self, field, getattr(other, field))


class GeneralConfig(BaseConfig):
    """
    General DBT configuration properties for models, sources, seeds, columns, etc.

    Args:
        description: Description of element
        tests: Tests for the element
        enabled: When false, the element is ignored
        docs: Documentation specific configuration
        perist_docs: Persist resource descriptions as column and/or relation comments in the database
        tags: List of tags that can be used for element grouping
        meta: Dictionary of metadata for the element
    """

    start: t.Optional[str] = None
    description: t.Optional[str] = None
    # TODO add test support
    tests: t.Dict[str, t.Any] = {}
    enabled: bool = True
    docs: t.Dict[str, t.Any] = {"show": True}
    persist_docs: t.Dict[str, t.Any] = {}
    tags: t.List[str] = []
    meta: t.Dict[str, t.Any] = {}

    @validator("enabled", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("docs", pre=True)
    def _validate_dict(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for key, value in v.items():
            if isinstance(value, str):
                v[key] = try_str_to_bool(value)

        return v

    @validator("persist_docs", pre=True)
    def _validate_persist_docs(cls, v: t.Dict[str, str]) -> t.Dict[str, bool]:
        return {key: bool(value) for key, value in v.items()}

    @validator("tags", pre=True)
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        return parse_meta(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **BaseConfig._FIELD_UPDATE_STRATEGY,
        **{
            "tests": UpdateStrategy.KEY_UPDATE,
            "docs": UpdateStrategy.KEY_UPDATE,
            "persist_docs": UpdateStrategy.KEY_UPDATE,
            "tags": UpdateStrategy.APPEND,
            "meta": UpdateStrategy.KEY_UPDATE,
        },
    }


def parse_meta(v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    for key, value in v.items():
        if isinstance(value, str):
            v[key] = try_str_to_bool(value)

    return v
