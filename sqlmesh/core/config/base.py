from __future__ import annotations

import typing as t
from enum import Enum, auto

from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

T = t.TypeVar("T", bound="BaseConfig")


class UpdateStrategy(Enum):
    """Supported strategies for adding new config to existing config"""

    REPLACE = auto()  # Replace with new value
    EXTEND = auto()  # Extend list to existing list
    KEY_UPDATE = auto()  # Update dict key value with new dict key value
    KEY_EXTEND = auto()  # Extend dict key value to existing dict key value
    IMMUTABLE = auto()  # Raise if a key tries to change this value
    NESTED_UPDATE = auto()  # Recursively updates the nested config


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


        The updated field
    """
    if not old:
        return new

    update_strategy = update_strategy or UpdateStrategy.REPLACE

    if update_strategy == UpdateStrategy.IMMUTABLE:
        raise ConfigError(f"Cannot modify property: {old}.")

    if update_strategy == UpdateStrategy.REPLACE:
        return new
    if update_strategy == UpdateStrategy.EXTEND:
        if not isinstance(old, list) or not isinstance(new, list):
            raise ConfigError("EXTEND behavior requires list field.")

        return old + new
    if update_strategy == UpdateStrategy.KEY_UPDATE:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field.")

        combined = old.copy()
        combined.update(new)
        return combined
    if update_strategy == UpdateStrategy.KEY_EXTEND:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_EXTEND behavior requires dictionary field.")

        combined = old.copy()
        for key, value in new.items():
            if not isinstance(value, list):
                raise ConfigError("KEY_EXTEND behavior requires list values in dictionary.")

            old_value = combined.get(key)
            if old_value:
                if not isinstance(old_value, list):
                    raise ConfigError("KEY_EXTEND behavior requires list values in dictionary.")

                combined[key] = old_value + value
            else:
                combined[key] = value

        return combined
    if update_strategy == UpdateStrategy.NESTED_UPDATE:
        if not isinstance(old, BaseConfig):
            raise ConfigError(
                f"NESTED_UPDATE behavior requires a config object. {type(old)} was given instead."
            )

        if type(new) != type(old):
            raise ConfigError(
                "NESTED_UPDATE behavior requires both values to have the same type. "
                f"{type(old)} and {type(new)} were given instead."
            )

        return old.update_with(new)

    raise ConfigError(f"Unknown update strategy {update_strategy}.")


class BaseConfig(PydanticModel):
    """Base configuration functionality for configuration classes."""

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {}

    def update_with(self: T, other: t.Union[t.Dict[str, t.Any], T]) -> T:
        """Updates this instance's fields with the passed in config fields and returns a new instance.

        Args:
            other: Other configuration.

        Returns:
            New instance updated with the passed in config fields
        """
        if isinstance(other, dict):
            other = self.__class__(**other)

        updated_fields = {}

        for field in other.fields_set:
            if field in self.all_field_infos():
                updated_fields[field] = update_field(
                    getattr(self, field),
                    getattr(other, field),
                    self._FIELD_UPDATE_STRATEGY.get(field),
                )
            else:
                updated_fields[field] = getattr(other, field)

        # Assign each field to trigger assignment validators
        updated = self.copy()
        for field, value in updated_fields.items():
            setattr(updated, field, value)

        return updated
