"""Include environment in plan dag spec."""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    plan_dags_table = "_plan_dags"
    if state_sync.schema:
        plan_dags_table = f"{schema}.{plan_dags_table}"

    new_specs = []

    for request_id, dag_id, dag_spec in engine_adapter.fetchall(
        exp.select("request_id", "dag_id", "dag_spec").from_(plan_dags_table),
        quote_identifiers=True,
    ):
        parsed_dag_spec = json.loads(dag_spec)

        environment_naming_info = parsed_dag_spec.pop("environment_naming_info")
        promoted_snapshots = parsed_dag_spec.pop("promoted_snapshots", [])
        start = parsed_dag_spec.pop("start")
        parsed_dag_spec.pop("end", None)
        plan_id = parsed_dag_spec.pop("plan_id")
        previous_plan_id = parsed_dag_spec.pop("previous_plan_id", None)
        expiration_ts = parsed_dag_spec.pop("environment_expiration_ts", None)

        parsed_dag_spec["environment"] = {
            **environment_naming_info,
            "snapshots": promoted_snapshots,
            "start_at": start,
            "end_at": start,
            "plan_id": plan_id,
            "previous_plan_id": previous_plan_id,
            "expiration_ts": expiration_ts,
        }

        new_specs.append(
            {
                "request_id": request_id,
                "dag_id": dag_id,
                "dag_spec": json.dumps(parsed_dag_spec),
            }
        )

    if new_specs:
        engine_adapter.delete_from(plan_dags_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            plan_dags_table,
            pd.DataFrame(new_specs),
            columns_to_types={
                "request_id": exp.DataType.build(index_type),
                "dag_id": exp.DataType.build(index_type),
                "dag_spec": exp.DataType.build("text"),
            },
            contains_json=True,
        )
