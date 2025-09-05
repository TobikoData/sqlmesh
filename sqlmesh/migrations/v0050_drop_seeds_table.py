"""Drop the seeds table."""


def migrate_schemas(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter

    seeds_table = "_seeds"
    if state_sync.schema:
        seeds_table = f"{state_sync.schema}.{seeds_table}"

    engine_adapter.drop_table(seeds_table)


def migrate_rows(state_sync, **kwargs):  # type: ignore
    pass
