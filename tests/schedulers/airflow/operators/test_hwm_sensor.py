import pytest
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.model import SqlModel
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.schedulers.airflow.operators.hwm_sensor import HighWaterMarkSensor
from sqlmesh.utils.date import to_datetime


@pytest.mark.airflow
def test_no_current_hwm(mocker: MockerFixture, make_snapshot, random_name):
    this_snapshot = make_snapshot(SqlModel(name="this", query=parse_one("select 1, ds")))
    this_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    target_snapshot = make_snapshot(SqlModel(name="target", query=parse_one("select 2, ds")))
    target_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    task = HighWaterMarkSensor(
        target_snapshot_info=target_snapshot.table_info,
        this_snapshot=this_snapshot,
        task_id="test_hwm_task",
    )

    get_snapshots_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.get_snapshots"
    )
    get_snapshots_mock.return_value = {target_snapshot.snapshot_id: target_snapshot}

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_end = to_datetime("2022-01-01")

    context = Context(dag_run=dag_run_mock)  # type: ignore
    assert not task.poke(context)

    get_snapshots_mock.assert_called_once_with([target_snapshot.table_info])


@pytest.mark.airflow
def test_current_hwm_below_target(mocker: MockerFixture, make_snapshot):
    this_snapshot = make_snapshot(
        SqlModel(name="this", query=parse_one("select 1, ds")), version="a"
    )
    this_snapshot.change_category = SnapshotChangeCategory.BREAKING

    target_snapshot_v1 = make_snapshot(
        SqlModel(name="that", query=parse_one("select 2, ds")), version="b"
    )
    target_snapshot_v1.change_category = SnapshotChangeCategory.BREAKING

    target_snapshot_v2 = make_snapshot(
        SqlModel(name="that", query=parse_one("select 3, ds")), version="b"
    )
    target_snapshot_v2.change_category = SnapshotChangeCategory.FORWARD_ONLY

    target_snapshot_v2.add_interval("2022-01-01", "2022-01-01")

    task = HighWaterMarkSensor(
        target_snapshot_info=target_snapshot_v1.table_info,
        this_snapshot=this_snapshot,
        task_id="test_hwm_task",
    )

    get_snapshots_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.get_snapshots"
    )
    get_snapshots_mock.return_value = {target_snapshot_v1.snapshot_id: target_snapshot_v1}

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_end = to_datetime("2022-01-03")

    context = Context(dag_run=dag_run_mock)  # type: ignore

    assert not task.poke(context)

    get_snapshots_mock.assert_called_once_with([target_snapshot_v1.table_info])


@pytest.mark.airflow
def test_current_hwm_above_target(mocker: MockerFixture, make_snapshot):
    this_snapshot = make_snapshot(
        SqlModel(name="this", query=parse_one("select 1, ds")), version="a"
    )
    this_snapshot.change_category = SnapshotChangeCategory.BREAKING

    target_snapshot_v1 = make_snapshot(
        SqlModel(name="that", query=parse_one("select 2, ds")), version="b"
    )
    target_snapshot_v1.change_category = SnapshotChangeCategory.BREAKING
    target_snapshot_v1.add_interval("2022-01-01", "2022-01-02")

    task = HighWaterMarkSensor(
        target_snapshot_info=target_snapshot_v1.table_info,
        this_snapshot=this_snapshot,
        task_id="test_hwm_task",
    )

    get_snapshots_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.get_snapshots"
    )
    get_snapshots_mock.return_value = {target_snapshot_v1.snapshot_id: target_snapshot_v1}

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_end = to_datetime("2022-01-03")

    context = Context(dag_run=dag_run_mock)  # type: ignore

    assert task.poke(context)

    get_snapshots_mock.assert_called_once_with([target_snapshot_v1.table_info])
