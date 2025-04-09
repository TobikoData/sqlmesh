"""
This migration script has two purposes:

1) Mark all python env macros referenced in audits, signals or on_virtual_update statements
   as metadata, unless they're referenced elsewhere in the model and they're not metadata-only.

2) Warn if there is both metadata and non-metadata reference in the python environment of a model.

   The metadata status for macros and signals is now transitive, i.e. every dependency of a
   metadata macro or signal is also metadata, unless it is referenced by a non-metadata object.

   This means that global references of metadata objects may now be excluded from the
   data hash calculation because of their new metadata status, which would lead to a
   diff. This script detects the possibility for such a diff and warns users ahead of time.
"""

import json

from sqlglot import exp

import sqlmesh.core.dialect as d
from sqlmesh.core.console import get_console


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    common_msg = (
        "Since the metadata status is now propagated transitively, this means that the next plan "
        "command may detect unexpected changes and prompt about backfilling this model, or others, "
        "for the same reason. If this is a concern, consider running a forward-only plan instead: "
        "https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans.\n"
    )

    for (snapshot,) in engine_adapter.fetchall(
        exp.select("snapshot").from_(snapshots_table), quote_identifiers=True
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        # Standalone audits don't have a data hash, so they're unaffected
        if node.get("source_type") == "audit":
            continue

        name = node["name"]
        python_env = node.get("python_env") or {}

        has_metadata = False
        has_non_metadata = False

        for k, v in python_env.items():
            if v.get("is_metadata"):
                has_metadata = True
            else:
                has_non_metadata = True

            if has_metadata and has_non_metadata:
                get_console().log_warning(
                    f"Model '{name}' references both metadata and non-metadata functions (macros or signals). "
                    + common_msg
                )
                return

        dialect = node.get("dialect")
        metadata_hash_statements = []

        if on_virtual_update := node.get("on_virtual_update"):
            metadata_hash_statements.extend(parse_expression(on_virtual_update, dialect))

        for _, audit_args in func_call_validator(node.get("audits") or []):
            metadata_hash_statements.extend(audit_args.values())

        for signal_name, signal_args in func_call_validator(
            node.get("signals") or [], is_signal=True
        ):
            metadata_hash_statements.extend(signal_args.values())

        if audit_definitions := node.get("audit_definitions"):
            audit_queries = [
                parse_expression(audit["query"], audit["dialect"])
                for audit in audit_definitions.values()
            ]
            metadata_hash_statements.extend(audit_queries)

        for macro_name in extract_used_macros(metadata_hash_statements):
            serialized_macro = python_env.get(macro_name)
            if isinstance(serialized_macro, dict) and not serialized_macro.get("is_metadata"):
                get_console().log_warning(
                    f"Model '{name}' references macro '{macro_name}' which is now implicitly treated as metadata-only. "
                    + common_msg
                )
                return


def extract_used_macros(expressions):
    used_macros = set()
    for expression in expressions:
        if isinstance(expression, d.Jinja):
            continue

        for macro_func in expression.find_all(d.MacroFunc):
            if macro_func.__class__ is d.MacroFunc:
                used_macros.add(macro_func.this.name.lower())

    return used_macros


def func_call_validator(v, is_signal=False):
    assert isinstance(v, list)

    audits = []
    for entry in v:
        if isinstance(entry, dict):
            args = entry
            name = "" if is_signal else entry.pop("name")
        else:
            assert isinstance(entry, (tuple, list))
            name, args = entry

        parsed_audit = {
            key: d.parse_one(value) if isinstance(value, str) else value
            for key, value in args.items()
        }
        audits.append((name.lower(), parsed_audit))

    return audits


def parse_expression(v, dialect):
    if v is None:
        return None

    if isinstance(v, list):
        return [d.parse_one(e, dialect=dialect) for e in v]

    assert isinstance(v, str)
    return d.parse_one(v, dialect=dialect)
