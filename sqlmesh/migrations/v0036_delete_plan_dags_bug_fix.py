"""Add missing delete from migration #34."""


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    plan_dags_table = "_plan_dags"
    if state_sync.schema:
        plan_dags_table = f"{schema}.{plan_dags_table}"

    # At the time of migration plan_dags table is only needed for in-flight DAGs and therefore we can safely
    # just delete it instead of migrating it
    # If reusing this code verify that this is still the case
    engine_adapter.delete_from(plan_dags_table, "TRUE")
