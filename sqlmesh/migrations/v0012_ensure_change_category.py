import json


def migrate(state_sync):  # type: ignore
    """Ensure all snapshots have a change category"""

    snapshots_table = f"{state_sync.schema}._snapshots"

    for name, identifier, snapshot in state_sync.engine_adapter.fetchall(
        f"SELECT name, identifier, snapshot FROM {snapshots_table}"
    ):
        snapshot = json.loads(snapshot)
        if snapshot.get("change_category"):
            continue
        # Just set it as breaking since we don't know
        snapshot["change_category"] = 1
        # this is not efficient, I'm doing this because I'm lazy and there shouldn't be a lot of snapshots out there
        # without a change category. Please be better than me and don't copy this in future migrations
        state_sync.engine_adapter.update_table(
            snapshots_table,
            {"snapshot": json.dumps(snapshot)},
            where=f"name = '{name}' and identifier = '{identifier}'",
            contains_json=True,
        )
