"""Fix expressions that contain jinja."""
import json
import re
import typing as t

import pandas as pd
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = f"{schema}._snapshots"

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table)
    ):
        parsed_snapshot = json.loads(snapshot)
        audits = parsed_snapshot.get("audits", [])
        model = parsed_snapshot["model"]

        if "query" in model and _has_jinja(model["query"]):
            model["query"] = _wrap_query(model["query"])

        _wrap_statements(model, "pre_statements")
        _wrap_statements(model, "post_statements")

        for audit in audits:
            if _has_jinja(audit["query"]):
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

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build("text"),
                "identifier": exp.DataType.build("text"),
                "version": exp.DataType.build("text"),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build("text"),
            },
            contains_json=True,
        )


def _wrap_statements(obj: t.Dict, key: str) -> None:
    updated_statements = []
    for statement in obj.get(key, []):
        if _has_jinja(statement):
            statement = _wrap_statement(statement)
        updated_statements.append(statement)

    if updated_statements:
        obj[key] = updated_statements


def _wrap_query(sql: str) -> str:
    return f"JINJA_QUERY_BEGIN;\n{sql}\nJINJA_END;"


def _wrap_statement(sql: str) -> str:
    return f"JINJA_STATEMENT_BEGIN;\n{sql}\nJINJA_END;"


JINJA_REGEX = re.compile(r"({{|{%)")


def _has_jinja(value: str) -> bool:
    return JINJA_REGEX.search(value) is not None
