from __future__ import annotations

import typing as t
from pydantic import root_validator

from sqlmesh.core.config.base import BaseConfig


class MigrationConfig(BaseConfig):
    """Configuration for the SQLMesh state migration.

    Args:
        promoted_snapshots_only: If True, only snapshots that are part of at least one environment will be migrated.
            Otherwise, all snapshots will be migrated.
        state_tables: A dict of state tables to migrate.
    """

    _default_state_tables = {
        "snapshots_table": "_snapshots",
        "intervals_table": "_intervals",
        "environments_table": "_environments",
        "environment_statements_table": "_environment_statements",
        "auto_restatements_table": "_auto_restatements",
        "versions_table": "_versions",
        "seeds_table": "_seeds",
        "plan_dags_table": "_plan_dags",
    }

    promoted_snapshots_only: bool = True
    state_tables: t.Optional[dict[str, str]] = None

    @root_validator(pre=True)
    def validate_and_merge_state_tables(cls, values):
        external = values.get("state_tables", {})
        merged = cls._default_state_tables.copy()
        if isinstance(external, dict):
            for k, v in external.items():
                if k in merged:
                    merged[k] = v

        for k, v in merged.items():
            if not v or not isinstance(v, str) or v.strip() == "":
                raise ValueError(f"Table name for '{k}' cannot be empty.")

        seen = set()
        for v in merged.values():
            if v in seen:
                raise ValueError(f"Duplicate table name found: '{v}'.")
            seen.add(v)
        values["state_tables"] = merged
        return values
