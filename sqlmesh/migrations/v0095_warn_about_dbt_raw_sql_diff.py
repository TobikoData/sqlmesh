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


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    pass


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
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

        jinja_macros = node.get("jinja_macros") or {}
        create_builtins_module = jinja_macros.get("create_builtins_module") or ""

        if create_builtins_module == SQLMESH_DBT_PACKAGE:
            get_console().log_warning(warning)
            return
