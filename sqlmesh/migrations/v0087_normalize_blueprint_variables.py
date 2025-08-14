"""
Normalizes blueprint variables, so Customer_Field is stored as customer_field in the `python_env`:

MODEL (
  ...
  blueprints (
    Customer_Field := 1
  )
);

SELECT
  @customer_field AS col
"""

import json
import logging
from dataclasses import dataclass

from sqlglot import exp
from sqlmesh.core.console import get_console
from sqlmesh.utils.migration import index_text_type, blob_text_type


logger = logging.getLogger(__name__)


SQLMESH_BLUEPRINT_VARS = "__sqlmesh__blueprint__vars__"


# Make sure `SqlValue` is defined so it can be used by `eval` call in the migration
@dataclass
class SqlValue:
    """A SQL string representing a generated SQLGlot AST."""

    sql: str


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    migration_needed = False
    new_snapshots = []

    for (
        name,
        identifier,
        version,
        snapshot,
        kind_name,
        updated_ts,
        unpaused_ts,
        ttl_ms,
        unrestorable,
    ) in engine_adapter.fetchall(
        exp.select(
            "name",
            "identifier",
            "version",
            "snapshot",
            "kind_name",
            "updated_ts",
            "unpaused_ts",
            "ttl_ms",
            "unrestorable",
        ).from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]
        python_env = node.get("python_env") or {}

        migrate_snapshot = False

        if blueprint_vars_executable := python_env.get(SQLMESH_BLUEPRINT_VARS):
            blueprint_vars = eval(blueprint_vars_executable["payload"])

            for var, value in dict(blueprint_vars).items():
                lowercase_var = var.lower()
                if var != lowercase_var:
                    if lowercase_var in blueprint_vars:
                        get_console().log_warning(
                            "SQLMesh is unable to fully migrate the state database, because the "
                            f"model '{node['name']}' contains two blueprint variables ('{var}' and "
                            f"'{lowercase_var}') that resolve to the same value ('{lowercase_var}'). "
                            "This may result in unexpected changes being reported by the next "
                            "`sqlmesh plan` command. If this happens, consider renaming either variable, "
                            "so that the lowercase version of their names are different."
                        )
                    else:
                        del blueprint_vars[var]
                        blueprint_vars[lowercase_var] = value
                        migrate_snapshot = True

            if migrate_snapshot:
                migration_needed = True
                blueprint_vars_executable["payload"] = repr(blueprint_vars)

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
                "updated_ts": updated_ts,
                "unpaused_ts": unpaused_ts,
                "ttl_ms": ttl_ms,
                "unrestorable": unrestorable,
            }
        )

    if migration_needed and new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            target_columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build("text"),
                "updated_ts": exp.DataType.build("bigint"),
                "unpaused_ts": exp.DataType.build("bigint"),
                "ttl_ms": exp.DataType.build("bigint"),
                "unrestorable": exp.DataType.build("boolean"),
            },
        )
