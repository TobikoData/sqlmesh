from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.pydantic import field_validator


class JanitorConfig(BaseConfig):
    """The configuration for the janitor.

    Args:
        warn_on_delete_failure: Whether to warn instead of erroring if the janitor fails to delete the expired environment schema / views.
        expired_snapshots_batch_size: Maximum number of expired snapshots to clean in a single batch.
    """

    warn_on_delete_failure: bool = False
    expired_snapshots_batch_size: t.Optional[int] = None

    @field_validator("expired_snapshots_batch_size", mode="before")
    @classmethod
    def _validate_batch_size(cls, value: int) -> int:
        batch_size = int(value)
        if batch_size <= 0:
            raise ValueError("expired_snapshots_batch_size must be greater than 0")
        return batch_size
