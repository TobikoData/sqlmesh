from pytest_mock.plugin import MockerFixture

from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.utils.concurrency import create_topological_groups


def test_create_topological_groups(mocker: MockerFixture):
    snapshot_a = mocker.Mock()
    snapshot_a.snapshot_id = SnapshotId(name="model_a", fingerprint="snapshot_a")
    snapshot_a.parents = []

    snapshot_b = mocker.Mock()
    snapshot_b.snapshot_id = SnapshotId(name="model_b", fingerprint="snapshot_b")
    snapshot_b.parents = []

    snapshot_c = mocker.Mock()
    snapshot_c.snapshot_id = SnapshotId(name="model_c", fingerprint="snapshot_c")
    snapshot_c.parents = [snapshot_a.snapshot_id]

    snapshot_d = mocker.Mock()
    snapshot_d.snapshot_id = SnapshotId(name="model_d", fingerprint="snapshot_d")
    snapshot_d.parents = [snapshot_b.snapshot_id, snapshot_c.snapshot_id]

    groups = create_topological_groups([snapshot_a, snapshot_b, snapshot_c, snapshot_d])

    assert groups == [
        [snapshot_a, snapshot_b],
        [snapshot_c],
        [snapshot_d],
    ]
