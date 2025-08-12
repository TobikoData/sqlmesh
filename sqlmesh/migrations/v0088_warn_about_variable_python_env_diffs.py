"""
This script's goal is to warn users about two situations that could lead to a diff:

- They have blueprint models and some of their variables may be trimmed from `python_env`
- Variables are used in metadata-only contexts, e.g., within metadata-only macros

Context:

We used to store *all* blueprint variables in `python_env`, even though some of them were
redundant. For example, if a blueprint variable is only used in the model's `name` property,
then it is rendered once, at load time, and after that point it's not needed elsewhere.

This behavior is now different: we only store the blueprint variables that are required to render
expressions at runtime, such as model query or runtime-rendered properties, like `merge_filter`.

Additionally, variables were previously treated as non-metadata, regardless of how they were used.
This behavior changed as well: SQLMesh now analyzes variable references and tracks the data flow,
in order to detect whether changing them will result in a metadata diff for a given model.

Some examples where variables can be treated as metadata-only `python_env` executables are:

- A variable is referenced in metadata-only macros
- A variable is referenced in metadata-only expressions, such as virtual update statements
- A variable is passed as argument to metadata-only macros
"""

import json

from sqlglot import exp

from sqlmesh.core.console import get_console

SQLMESH_VARS = "__sqlmesh__vars__"
SQLMESH_BLUEPRINT_VARS = "__sqlmesh__blueprint__vars__"
METADATA_HASH_EXPRESSIONS = {"on_virtual_update", "audits", "signals", "audit_definitions"}


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    warning = (
        "SQLMesh detected that it may not be able to fully migrate the state database. This should not impact "
        "the migration process, but may result in unexpected changes being reported by the next `sqlmesh plan` "
        "command. Please run `sqlmesh diff prod` after the migration has completed, before making any new "
        "changes. If any unexpected changes are reported, consider running a forward-only plan to apply these "
        "changes and avoid unnecessary backfills: sqlmesh plan prod --forward-only. "
        "See https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans for more details.\n"
    )

    for (snapshot,) in engine_adapter.fetchall(
        exp.select("snapshot").from_(snapshots_table), quote_identifiers=True
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        # Standalone audits don't have a data hash, so they're unaffected
        if node.get("source_type") == "audit":
            continue

        python_env = node.get("python_env") or {}

        if SQLMESH_BLUEPRINT_VARS in python_env or (
            SQLMESH_VARS in python_env
            and (
                any(v.get("is_metadata") for v in python_env.values())
                or any(node.get(k) for k in METADATA_HASH_EXPRESSIONS)
            )
        ):
            get_console().log_warning(warning)
            return
