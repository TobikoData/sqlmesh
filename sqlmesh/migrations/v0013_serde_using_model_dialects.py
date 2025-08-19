"""Serialize SQL using the dialect of each model."""

import json
import typing as t

from sqlglot import exp, parse_one

from sqlmesh.utils.jinja import has_jinja
from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

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
        model = parsed_snapshot["model"]
        dialect = model["dialect"]

        _update_expression(model, "query", dialect)
        _update_expression_list(model, "pre_statements", dialect)
        _update_expression_list(model, "post_statements", dialect)

        for audit in parsed_snapshot.get("audits", []):
            dialect = audit["dialect"]
            _update_expression(audit, "query", dialect)
            _update_expression_list(audit, "expressions", dialect)

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
            target_columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build(index_type),
            },
        )


# Note: previously we used to do serde using the SQLGlot dialect, so we need to parse the
# stored queries using that dialect and then write them back using the correct dialect.


def _update_expression(obj: t.Dict, key: str, dialect: str) -> None:
    if key in obj and not has_jinja(obj[key]):
        obj[key] = parse_one(obj[key]).sql(dialect=dialect)


def _update_expression_list(obj: t.Dict, key: str, dialect: str) -> None:
    if key in obj:
        obj[key] = [
            (
                parse_one(expression).sql(dialect=dialect)
                if not has_jinja(expression)
                else expression
            )
            for expression in obj[key]
            if expression
        ]
