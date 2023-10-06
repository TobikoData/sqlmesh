"""Creates the '_plan_dags' table if Airflow is used."""
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    plan_dags_table = "_plan_dags"

    if schema:
        engine_adapter.create_schema(schema)
        plan_dags_table = f"{schema}.{plan_dags_table}"

    text_type = index_text_type(engine_adapter.dialect)

    engine_adapter.create_state_table(
        plan_dags_table,
        {
            "request_id": exp.DataType.build(text_type),
            "dag_id": exp.DataType.build(text_type),
            "dag_spec": exp.DataType.build("text"),
        },
        primary_key=("request_id",),
    )

    engine_adapter.create_index(plan_dags_table, "dag_id_idx", ("dag_id",))
