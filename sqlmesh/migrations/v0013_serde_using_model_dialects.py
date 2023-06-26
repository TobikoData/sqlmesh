"""Serialize SQL using the dialect of each model."""
import json

from sqlglot import exp, transpile

from sqlmesh.utils.jinja import has_jinja


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = f"{schema}._snapshots"

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table)
    ):
        parsed_snapshot = json.loads(snapshot)
        model = parsed_snapshot["model"]
        dialect = parsed_snapshot["dialect"]

        # Read using the SQLGlot dialect, write using the model's dialect
        if "query" in model and not has_jinja(model["query"]):
            model["query"] = transpile(model["query"], read="", write=dialect, identity=False)[0]

        for statement_kind in ("pre_statements_", "post_statements_"):
            if statement_kind in model and not has_jinja(model[statement_kind]):
                model[statement_kind] = [
                    transpile(statement, read="", write=dialect, identity=False)[0]
                    for statement in model[statement_kind]
                    if statement
                ]

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
