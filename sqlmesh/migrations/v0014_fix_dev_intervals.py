"""Fix snapshot intervals that have been erroneously marked as dev."""


def migrate(state_sync):  # type: ignore
    schema = state_sync.schema
    intervals_table = f"{schema}._intervals"

    state_sync.engine_adapter.update_table(
        intervals_table,
        {"is_dev": False},
        where="1=1",
    )
