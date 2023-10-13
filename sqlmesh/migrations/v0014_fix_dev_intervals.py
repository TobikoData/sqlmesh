"""Fix snapshot intervals that have been erroneously marked as dev."""


def migrate(state_sync, **kwargs):  # type: ignore
    schema = state_sync.schema
    intervals_table = "_intervals"
    if schema:
        intervals_table = f"{schema}.{intervals_table}"

    state_sync.engine_adapter.update_table(
        intervals_table,
        {"is_dev": False},
        where="1=1",
    )
