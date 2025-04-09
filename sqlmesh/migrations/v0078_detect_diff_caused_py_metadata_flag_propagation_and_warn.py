"""
Warn if there is both metadata and non-metadata reference in the python environment of a model.

The metadata status for macros and signals is now transitive, i.e. every dependency of a
metadata macro or signal is also metadata, unless it is referenced by a non-metadata object.

This means that global references of metadata objects may now be excluded from the
data hash calculation because of their new metadata status, which would lead to a
diff. This script detects the possibility for such a diff and warns users ahead of time.
"""

import json
from sqlglot import exp

from sqlmesh.core.console import get_console


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    for (snapshot,) in engine_adapter.fetchall(
        exp.select("snapshot").from_(snapshots_table), quote_identifiers=True
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        # Standalone audits don't have a data hash
        if node.get("source_type") == "audit":
            continue

        has_metadata = False
        has_non_metadata = False

        for k, v in (node.get("python_env") or {}).items():
            if v.get("is_metadata"):
                has_metadata = True
            else:
                has_non_metadata = True

            if has_metadata and has_non_metadata:
                get_console().log_warning(
                    f"Model '{node['name']}' references both metadata and non-metadata functions (macros or "
                    "signals). Since the metadata status is now propagated transitively, this means that the "
                    "next plan command may detect unexpected changes and prompt about backfilling this model, "
                    "or others, for the same reason. If this is a concern, consider running a forward-only plan "
                    "instead: https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans.\n"
                )
                return
