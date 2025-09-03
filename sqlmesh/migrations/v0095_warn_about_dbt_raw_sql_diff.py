"""
Warns dbt users about potential diffs due to inclusion of {{ config(...) }} blocks in model SQL.

Prior to this fix, SQLMesh wasn't including the {{ config(...) }} block in the model's SQL payload
when processing dbt models. Now these config blocks are properly included in the raw SQL, which
may cause diffs to appear for existing dbt models even though the actual SQL logic hasn't changed.

This is a one-time diff that will appear after upgrading, and applying a plan will resolve it.
"""

import json

from sqlglot import exp

from sqlmesh.core.console import get_console

SQLMESH_DBT_PACKAGE = "sqlmesh.dbt"


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    warning = (
        "SQLMesh now includes dbt's {{ config(...) }} blocks in the model's raw SQL when "
        "processing dbt models. This change ensures that all model attributes referenced "
        "in macros are properly tracked for fingerprinting. As a result, you may see diffs "
        "for existing dbt models even though the actual SQL logic hasn't changed. This is "
        "a one-time diff that will be resolved after applying a plan. Run 'sqlmesh diff prod' "
        "to review any changes, then apply a plan if the diffs look expected."
    )

    for (snapshot,) in engine_adapter.fetchall(
        exp.select("snapshot").from_(snapshots_table), quote_identifiers=True
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        jinja_macros = node.get("jinja_macros") or {}
        create_builtins_module = jinja_macros.get("create_builtins_module") or ""

        if create_builtins_module == SQLMESH_DBT_PACKAGE:
            get_console().log_warning(warning)
            return
