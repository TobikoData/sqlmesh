from __future__ import annotations

from enum import Enum

from sqlmesh.core.config.base import BaseConfig


class AutoCategorizationMode(Enum):
    FULL = "full"
    """Full-auto mode in which the categorizer falls back to the most conservative choice (breaking)."""

    SEMI = "semi"
    """Semi-auto mode in which a user is promted to provide a category in case when the categorizer
    failed to determine it automatically.
    """

    OFF = "off"
    """Disables automatic categorization."""


class CategorizerConfig(BaseConfig):
    """Configuration for the automatic categorizer of snapshot changes.

    Args:
        python: the auto categorization mode for Python models.
        sql: the auto categorization mode for SQL models.
        seed: the auto categorization mode for Seed models.
    """

    external: AutoCategorizationMode = AutoCategorizationMode.FULL
    python: AutoCategorizationMode = AutoCategorizationMode.OFF
    sql: AutoCategorizationMode = AutoCategorizationMode.FULL
    seed: AutoCategorizationMode = AutoCategorizationMode.FULL
