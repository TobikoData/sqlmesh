from __future__ import annotations

import typing as t

from pydantic import validator

from sqlmesh.utils.errors import ConfigError


def _concurrent_tasks_validator(v: t.Any) -> int:
    if isinstance(v, str):
        v = int(v)
    if not isinstance(v, int) or v <= 0:
        raise ConfigError(
            f"The number of concurrent tasks must be an integer value greater than 0. '{v}' was provided"
        )
    return v


concurrent_tasks_validator = validator(
    "backfill_concurrent_tasks",
    "ddl_concurrent_tasks",
    "concurrent_tasks",
    pre=True,
    allow_reuse=True,
    check_fields=False,
)(_concurrent_tasks_validator)
