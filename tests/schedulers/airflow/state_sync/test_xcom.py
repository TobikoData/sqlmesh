import typing as t

import pytest
from airflow.utils.session import create_session
from sqlalchemy.orm import Session

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model
from sqlmesh.core.model_kind import ModelKind, ModelKindName
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.schedulers.airflow.state_sync.xcom import XComStateSync
from sqlmesh.utils.date import to_timestamp


@pytest.fixture(scope="module")
def init_state(airflow_client: AirflowClient) -> None:
    # Making sure the Receiver DAG had its first run.
    airflow_client.apply_plan(
        [],
        Environment(
            name="prod",
            snapshots=[],
            start="2022-01-01",
            end="2022-01-01",
            plan_id="init_plan_id",
            previous_plan_id=None,
        ),
        "init_state_request_id",
    )


@pytest.fixture()
def airflow_db_session():
    with create_session() as session:
        yield session


@pytest.fixture()
def state_sync(init_state, airflow_db_session: Session) -> XComStateSync:
    return XComStateSync(airflow_db_session)


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_push_snapshots(
    make_snapshot: t.Callable, state_sync: XComStateSync, random_name: t.Callable
):
    model_name = random_name()
    snapshot = make_snapshot(_create_model(model_name), version="1")

    state_sync.push_snapshots([snapshot])

    new_snapshot = make_snapshot(_create_model(model_name, query="SELECT a, ds"))
    new_snapshot.version = snapshot.version
    state_sync.push_snapshots([new_snapshot])

    assert state_sync.get_snapshots(
        [snapshot.snapshot_id, new_snapshot.snapshot_id]
    ) == {s.snapshot_id: s for s in [snapshot, new_snapshot]}

    assert state_sync.get_snapshots_with_same_version([snapshot]) == [
        snapshot,
        new_snapshot,
    ]


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_add_interval(
    make_snapshot: t.Callable, state_sync: XComStateSync, random_name: t.Callable
):
    model_name = random_name()
    model = _create_model(model_name)

    snapshot = make_snapshot(model, version="1")

    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(
        snapshot.snapshot_id,
        "2022-08-16T00:00:00.000000Z",
        "2022-08-17T00:00:00.000000Z",
    )

    state_sync.add_interval(
        snapshot.snapshot_id,
        "2022-08-18T00:00:00.000000Z",
        "2022-08-20T00:00:00.000000Z",
    )

    actual_snapshot = state_sync.get_snapshots([snapshot.snapshot_id])[
        snapshot.snapshot_id
    ]
    assert len(actual_snapshot.intervals) == 2


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_promote_snapshots(
    make_snapshot: t.Callable, state_sync: XComStateSync, random_name: t.Callable
):
    environment = random_name()
    model_name = random_name()
    snapshot = make_snapshot(_create_model(model_name), version="1")

    assert state_sync.get_environment(environment) is None

    state_sync.push_snapshots([snapshot])

    state_sync.promote(
        Environment(
            name=environment,
            snapshots=[snapshot.table_info],
            start="2022-01-01",
            end="2022-01-01",
            plan_id="test_plan_id",
            previous_plan_id=None,
        )
    )

    assert state_sync.get_environment(environment).snapshots == [snapshot.table_info]  # type: ignore

    state_sync.promote(
        Environment(
            name=environment,
            snapshots=[],
            start="2022-01-01",
            end="2022-01-01",
            plan_id="test_plan_id_2",
            previous_plan_id="test_plan_id",
        )
    )
    assert state_sync.get_environment(environment).snapshots == []  # type: ignore


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_remove_expired_snapshots(
    make_snapshot: t.Callable, state_sync: XComStateSync, random_name: t.Callable
):
    environment = random_name()
    model_name = random_name()

    expired_updated_ts = "2022-08-16T00:00:00.000000Z"
    ttl = "in 1 week"

    expired_snapshot = make_snapshot(_create_model(model_name), version="1")
    expired_snapshot.updated_ts = to_timestamp(expired_updated_ts)
    expired_snapshot.ttl = ttl

    expired_promoted_snapshot = make_snapshot(
        _create_model(model_name, query="SELECT a, ds"),
        version="2",
    )
    expired_promoted_snapshot.version = expired_snapshot.version
    expired_promoted_snapshot.updated_ts = to_timestamp(expired_updated_ts)
    expired_promoted_snapshot.ttl = ttl

    unexpired_snapshot = make_snapshot(_create_model(random_name()), version="3")

    state_sync.push_snapshots(
        [expired_snapshot, expired_promoted_snapshot, unexpired_snapshot]
    )

    state_sync.promote(
        Environment(
            name=environment,
            snapshots=[expired_promoted_snapshot.table_info],
            start="2022-01-01",
            end="2022-01-01",
            plan_id="test_plan_id",
            previous_plan_id=None,
        )
    )
    state_sync.remove_expired_snapshots()

    assert (
        len(
            state_sync.get_snapshots(
                [
                    expired_snapshot.snapshot_id,
                    expired_promoted_snapshot.snapshot_id,
                    unexpired_snapshot.snapshot_id,
                ]
            )
        )
        == 3
    )

    state_sync.promote(
        Environment(
            name=environment,
            snapshots=[],
            start="2022-01-01",
            end="2022-01-01",
            plan_id="test_plan_id_2",
            previous_plan_id="test_plan_id",
        )
    )
    state_sync.remove_expired_snapshots()
    assert state_sync.get_snapshots(
        [
            expired_snapshot.snapshot_id,
            expired_promoted_snapshot.snapshot_id,
            unexpired_snapshot.snapshot_id,
        ]
    ) == {unexpired_snapshot.snapshot_id: unexpired_snapshot}


def _create_model(name: str, query: t.Optional[str] = None) -> Model:
    if not query:
        query = "SELECT * FROM sqlmesh.orders"
    return Model(
        name=name,
        kind=ModelKind(name=ModelKindName.FULL),
        query=query,
    )
