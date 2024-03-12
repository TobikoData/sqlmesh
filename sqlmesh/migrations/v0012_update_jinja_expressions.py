"""Fix expressions that contain jinja."""

import json
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.jinja import has_jinja
from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        audits = parsed_snapshot.get("audits", [])
        model = parsed_snapshot["model"]

        if "query" in model and has_jinja(model["query"]):
            model["query"] = _wrap_query(model["query"])

        _wrap_statements(model, "pre_statements")
        _wrap_statements(model, "post_statements")

        for audit in audits:
            if has_jinja(audit["query"]):
                audit["query"] = _wrap_query(audit["query"])
            _wrap_statements(audit, "expressions")

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build(index_type),
            },
            contains_json=True,
        )


def _wrap_statements(obj: t.Dict, key: str) -> None:
    updated_statements = []
    for statement in obj.get(key, []):
        if has_jinja(statement):
            statement = _wrap_statement(statement)
        updated_statements.append(statement)

    if updated_statements:
        obj[key] = updated_statements


def _wrap_query(sql: str) -> str:
    return f"JINJA_QUERY_BEGIN;\n{sql}\nJINJA_END;"


def _wrap_statement(sql: str) -> str:
    return f"JINJA_STATEMENT_BEGIN;\n{sql}\nJINJA_END;"
