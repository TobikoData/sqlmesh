"""Remove the obsolete _plan_dags table."""


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    plan_dags_table = "_plan_dags"
    if schema:
        plan_dags_table = f"{schema}.{plan_dags_table}"

    engine_adapter.drop_table(plan_dags_table)


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    pass
