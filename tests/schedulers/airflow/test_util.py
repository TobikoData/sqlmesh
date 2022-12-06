from pytest_mock.plugin import MockerFixture

from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.schedulers.airflow import util


def test_create_topological_snapshot_batches(mocker: MockerFixture):
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

    batches = util.create_topological_snapshot_batches(
        [snapshot_a, snapshot_b, snapshot_c, snapshot_d], 2, lambda _: True
    )

    assert batches == [
        [[snapshot_a], [snapshot_b]],
        [[snapshot_c], []],
        [[snapshot_d], []],
    ]
