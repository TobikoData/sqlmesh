"""Remove the obsolete _plan_dags table."""


def migrate_schemas(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    plan_dags_table = "_plan_dags"
    if schema:
        plan_dags_table = f"{schema}.{plan_dags_table}"

    engine_adapter.drop_table(plan_dags_table)


def migrate_rows(state_sync, **kwargs):  # type: ignore
    pass
