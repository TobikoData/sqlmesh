from __future__ import annotations

import typing as t
from enum import Enum, auto

from sqlmesh.utils.errors import ConfigError


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
    update_strategy: t.Optional[UpdateStrategy] = UpdateStrategy.REPLACE,
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

    raise ConfigError("Unknown update strategy {update_strategy}")
