import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.model import SqlModel
from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.schedulers.airflow.state_sync.http import HttpStateReader
from sqlmesh.utils.file_cache import FileCache


@pytest.fixture
def model() -> SqlModel:
    return SqlModel(
        name="test_model",
        storage_format="parquet",
        partitioned_by=["a"],
        query=parse_one("SELECT a, ds FROM tbl"),
        expressions=[
            parse_one("@DEF(key, 'value')"),
        ],
    )


def test_get_snapshots(
    mocker: MockerFixture,
    model: SqlModel,
    make_snapshot: t.Callable,
    mock_file_cache: FileCache,
    mock_airflow_client: AirflowClient,
):
    snapshot = make_snapshot(model, version="1")

    get_snapshot_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.client.AirflowClient.get_snapshot"
    )
    get_snapshot_mock.return_value = snapshot

    state_reader = HttpStateReader(mock_file_cache, mock_airflow_client)
    assert state_reader.get_snapshots([snapshot.snapshot_id]) == {
        snapshot.snapshot_id: snapshot
    }

    get_snapshot_mock.assert_called_once_with(snapshot.name, snapshot.fingerprint)


def test_get_snapshots_with_same_version(
    mocker: MockerFixture,
    model: SqlModel,
    make_snapshot: t.Callable,
    mock_file_cache: FileCache,
    mock_airflow_client: AirflowClient,
):
    snapshot = make_snapshot(model, version="1")

    get_snapshot_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.client.AirflowClient.get_snapshot"
    )
    get_snapshot_mock.return_value = snapshot

    get_snapshot_for_version_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.client.AirflowClient.get_snapshot_fingerprints_for_version"
    )
    get_snapshot_for_version_mock.return_value = [snapshot.fingerprint]

    new_snapshot = make_snapshot(model, version="1")
    new_snapshot.fingerprint = "new_fingerprint"

    state_reader = HttpStateReader(mock_file_cache, mock_airflow_client)
    assert state_reader.get_snapshots_with_same_version([new_snapshot]) == [snapshot]

    get_snapshot_mock.assert_called_once_with(snapshot.name, snapshot.fingerprint)
    get_snapshot_for_version_mock.assert_called_once_with(
        snapshot.name, version=snapshot.version
    )


def test_snapshots_exist(
    mocker: MockerFixture,
    model: SqlModel,
    make_snapshot: t.Callable,
    mock_file_cache: FileCache,
    mock_airflow_client: AirflowClient,
):
    snapshot = make_snapshot(model, version="1")

    get_snapshot_ids_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.client.AirflowClient.get_snapshot_ids"
    )
    get_snapshot_ids_mock.return_value = [snapshot.snapshot_id]

    state_reader = HttpStateReader(mock_file_cache, mock_airflow_client)

    assert state_reader.snapshots_exist([snapshot.snapshot_id]) == {
        snapshot.snapshot_id
    }

    assert (
        state_reader.snapshots_exist(
            [SnapshotId(name="test_name", fingerprint="test_fingerprint")]
        )
        == set()
    )

    assert get_snapshot_ids_mock.call_count == 2
