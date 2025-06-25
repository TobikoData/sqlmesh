"""Trim irrelevant attributes from the plan DAGs state."""

import json

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    plan_dags_table = "_plan_dags"
    if schema:
        plan_dags_table = f"{schema}.{plan_dags_table}"

    new_dag_specs = []

    for request_id, dag_id, dag_spec in engine_adapter.fetchall(
        exp.select("request_id", "dag_id", "dag_spec").from_(plan_dags_table),
        quote_identifiers=True,
    ):
        parsed_dag_spec = json.loads(dag_spec)
        for snapshot in parsed_dag_spec.get("new_snapshots", []):
            snapshot["node"].pop("hash_raw_query", None)

            for indirect_versions in snapshot.get("indirect_versions", {}).values():
                for indirect_version in indirect_versions:
                    # Only keep version and change_category.
                    version = indirect_version.get("version")
                    change_category = indirect_version.get("change_category")
                    indirect_version.clear()
                    indirect_version["version"] = version
                    indirect_version["change_category"] = change_category

        new_dag_specs.append(
            {
                "request_id": request_id,
                "dag_id": dag_id,
                "dag_spec": json.dumps(parsed_dag_spec),
            }
        )

    if new_dag_specs:
        engine_adapter.delete_from(plan_dags_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            plan_dags_table,
            pd.DataFrame(new_dag_specs),
            columns_to_types={
                "request_id": exp.DataType.build(index_type),
                "dag_id": exp.DataType.build(index_type),
                "dag_spec": exp.DataType.build(blob_type),
            },
        )
