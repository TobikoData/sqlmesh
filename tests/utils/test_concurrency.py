from pytest_mock.plugin import MockerFixture

from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.utils.concurrency import concurrent_apply_to_snapshots


def test_concurrent_apply_to_snapshots(mocker: MockerFixture):
    snapshot_a = mocker.Mock()
    snapshot_a.snapshot_id = SnapshotId(name="model_a", fingerprint="snapshot_a")
    snapshot_a.parents = []

    snapshot_b = mocker.Mock()
    snapshot_b.snapshot_id = SnapshotId(name="model_b", fingerprint="snapshot_b")
    snapshot_b.parents = []

    snapshot_c = mocker.Mock()
    snapshot_c.snapshot_id = SnapshotId(name="model_c", fingerprint="snapshot_c")
    snapshot_c.parents = [snapshot_a.snapshot_id, snapshot_b.snapshot_id]

    snapshot_d = mocker.Mock()
    snapshot_d.snapshot_id = SnapshotId(name="model_d", fingerprint="snapshot_d")
    snapshot_d.parents = [snapshot_b.snapshot_id, snapshot_c.snapshot_id]

    processed_snapshots = []

    concurrent_apply_to_snapshots(
        [snapshot_a, snapshot_b, snapshot_c, snapshot_d],
        lambda s: processed_snapshots.append(s),
        2,
    )

    assert len(processed_snapshots) == 4
    assert processed_snapshots[0] in (snapshot_a, snapshot_b)
    assert processed_snapshots[1] in (snapshot_a, snapshot_b)
    assert processed_snapshots[2] == snapshot_c
    assert processed_snapshots[3] == snapshot_d
