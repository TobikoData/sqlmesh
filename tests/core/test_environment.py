import pytest

from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.snapshot import SnapshotId, SnapshotTableInfo
from sqlmesh.core.state_sync.engine_adapter import _environment_to_df


def test_sanitize_name():
    assert Environment.sanitize_name("12foo!#%@") == "12foo____"
    assert Environment.sanitize_name("__test@$@%") == "__test____"
    assert Environment.sanitize_name("test") == "test"
    assert Environment.sanitize_name("-test_") == "_test_"
    with pytest.raises(TypeError, match="Expected str or Environment, got int"):
        Environment.sanitize_name(123)


@pytest.mark.parametrize(
    "mapping, name, expected",
    [
        # Match the first pattern
        ({"^prod$": "prod_catalog", "^dev$": "dev_catalog"}, "prod", "prod_catalog"),
        # Match the second pattern
        ({"^prod$": "prod_catalog", "^dev$": "dev_catalog"}, "dev", "dev_catalog"),
        # Match no pattern
        (
            {"^prod$": "prod_catalog", "^dev$": "dev_catalog"},
            "develop",
            None,
        ),
        # Match both patterns but take the first
        (
            {".*": "catchall", "^prod$": "will_never_happen"},
            "prod",
            "catchall",
        ),
        # Don't need to test invalid regex pattern since regex patterns are validated when the config is parsed
    ],
)
def test_from_environment_catalog_mapping(mapping, name, expected):
    assert (
        EnvironmentNamingInfo.from_environment_catalog_mapping(
            mapping,
            name,
        ).catalog_name_override
        == expected
    )


def test_lazy_loading(sushi_context):
    snapshot_ids = [s.snapshot_id for s in sushi_context.snapshots.values()]
    snapshot_table_infos = [s.table_info for s in sushi_context.snapshots.values()]
    env = Environment(
        name="test",
        start_at="now",
        plan_id="plan_1",
        snapshots=snapshot_table_infos,
        promoted_snapshot_ids=snapshot_ids,
        previous_finalized_snapshots=snapshot_table_infos,
    )

    df = _environment_to_df(env)
    row = df.to_dict(orient="records")[0]
    env = Environment(**{field: row[field] for field in Environment.all_fields()})

    assert all(isinstance(snapshot, dict) for snapshot in env.snapshots_)
    assert all(isinstance(snapshot, SnapshotTableInfo) for snapshot in env.snapshots)
    assert all(isinstance(s_id, dict) for s_id in env.promoted_snapshot_ids_)
    assert all(isinstance(s_id, SnapshotId) for s_id in env.promoted_snapshot_ids)
    assert all(isinstance(snapshot, dict) for snapshot in env.previous_finalized_snapshots_)
    assert all(
        isinstance(snapshot, SnapshotTableInfo) for snapshot in env.previous_finalized_snapshots
    )

    with pytest.raises(ValueError, match="Must be a list of SnapshotTableInfo dicts or objects"):
        Environment(**{**env.dict(), **{"snapshots": [1, 2, 3]}})

    with pytest.raises(ValueError, match="Must be a list of SnapshotId dicts or objects"):
        Environment(**{**env.dict(), **{"promoted_snapshot_ids": [1, 2, 3]}})

    with pytest.raises(ValueError, match="Must be a list of SnapshotTableInfo dicts or objects"):
        Environment(**{**env.dict(), **{"previous_finalized_snapshots": [1, 2, 3]}})
