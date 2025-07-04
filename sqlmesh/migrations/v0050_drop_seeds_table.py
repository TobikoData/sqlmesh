"""Drop the seeds table."""


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter

    state_tables = getattr(state_sync, "state_tables", {})
    seeds_table = state_tables.get("seeds_table", "_seeds")
    if state_sync.schema:
        seeds_table = f"{state_sync.schema}.{seeds_table}"

    engine_adapter.drop_table(seeds_table)
