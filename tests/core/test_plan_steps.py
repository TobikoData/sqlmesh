import pytest
from sqlglot import parse_one
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.model import SqlModel, ModelKindName
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.plan.steps import (
    build_plan_steps,
    PhysicalLayerUpdateStep,
    BackfillStep,
    EnvironmentRecordUpdateStep,
    VirtualLayerUpdateStep,
    RestatementStep,
    MigrateSchemasStep,
)
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory, DeployabilityIndex, Snapshot
from sqlmesh.core.state_sync import StateReader
from sqlmesh.core.environment import Environment
from sqlmesh.utils.date import to_timestamp


@pytest.fixture
def snapshot_a(make_snapshot) -> Snapshot:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    return snapshot


@pytest.fixture
def snapshot_b(make_snapshot, snapshot_a: Snapshot) -> Snapshot:
    snapshot = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select 2, ds from a"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        ),
        nodes={'"a"': snapshot_a.model},
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    return snapshot


def test_build_plan_steps_basic(
    snapshot_a: Snapshot, snapshot_b: Snapshot, mocker: MockerFixture
) -> None:
    # Mock state reader
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    state_reader.get_environment.return_value = None

    # Create environment
    environment = Environment(
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[snapshot_a, snapshot_b],
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={},
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        indirectly_modified_snapshots={},
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 4

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 2
    assert {s.snapshot_id for s in physical_step.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    assert physical_step.deployability_index == DeployabilityIndex.all_deployable()

    # Verify BackfillStep
    backfill_step = steps[1]
    assert isinstance(backfill_step, BackfillStep)
    assert backfill_step.deployability_index == DeployabilityIndex.all_deployable()
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert len(backfill_step.snapshot_to_intervals) == 2
    assert backfill_step.snapshot_to_intervals[snapshot_a] == [expected_interval]
    assert backfill_step.snapshot_to_intervals[snapshot_b] == [expected_interval]

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[2], EnvironmentRecordUpdateStep)

    # Verify VirtualLayerUpdateStep
    virtual_step = steps[3]
    assert isinstance(virtual_step, VirtualLayerUpdateStep)
    assert len(virtual_step.promoted_snapshots) == 2
    assert len(virtual_step.demoted_snapshots) == 0
    assert {s.name for s in virtual_step.promoted_snapshots} == {'"a"', '"b"'}


def test_build_plan_steps_select_models(
    snapshot_a: Snapshot, snapshot_b: Snapshot, mocker: MockerFixture
) -> None:
    # Mock state reader
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    state_reader.get_environment.return_value = None

    # Create environment
    environment = Environment(
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[snapshot_a, snapshot_b],
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={},
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        indirectly_modified_snapshots={},
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill={snapshot_a.name},
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 4

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 1
    assert {s.snapshot_id for s in physical_step.snapshots} == {snapshot_a.snapshot_id}
    assert physical_step.deployability_index == DeployabilityIndex.all_deployable()

    # Verify BackfillStep
    backfill_step = steps[1]
    assert isinstance(backfill_step, BackfillStep)
    assert backfill_step.deployability_index == DeployabilityIndex.all_deployable()
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert len(backfill_step.snapshot_to_intervals) == 1
    assert backfill_step.snapshot_to_intervals[snapshot_a] == [expected_interval]

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[2], EnvironmentRecordUpdateStep)

    # Verify VirtualLayerUpdateStep
    virtual_step = steps[3]
    assert isinstance(virtual_step, VirtualLayerUpdateStep)
    assert len(virtual_step.promoted_snapshots) == 1
    assert len(virtual_step.demoted_snapshots) == 0
    assert {s.name for s in virtual_step.promoted_snapshots} == {'"a"'}


@pytest.mark.parametrize("skip_backfill,empty_backfill", [(True, False), (False, True)])
def test_build_plan_steps_basic_no_backfill(
    snapshot_a: Snapshot,
    snapshot_b: Snapshot,
    mocker: MockerFixture,
    skip_backfill: bool,
    empty_backfill: bool,
) -> None:
    # Mock state reader
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    state_reader.get_environment.return_value = None

    # Create environment
    environment = Environment(
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[snapshot_a, snapshot_b],
        environment=environment,
        no_gaps=False,
        skip_backfill=skip_backfill,
        empty_backfill=empty_backfill,
        restatements={},
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        indirectly_modified_snapshots={},
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 4

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 2
    assert {s.snapshot_id for s in physical_step.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify BackfillStep
    backfill_step = steps[1]
    assert isinstance(backfill_step, BackfillStep)
    assert backfill_step.deployability_index == DeployabilityIndex.all_deployable()
    assert backfill_step.snapshot_to_intervals == {}

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[2], EnvironmentRecordUpdateStep)

    # Verify VirtualLayerUpdateStep
    virtual_step = steps[3]
    assert isinstance(virtual_step, VirtualLayerUpdateStep)
    assert len(virtual_step.promoted_snapshots) == 2
    assert len(virtual_step.demoted_snapshots) == 0
    assert {s.name for s in virtual_step.promoted_snapshots} == {'"a"', '"b"'}


def test_build_plan_steps_restatement(
    snapshot_a: Snapshot, snapshot_b: Snapshot, mocker: MockerFixture
) -> None:
    # Mock state reader to return existing snapshots and environment
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {
        snapshot_a.snapshot_id: snapshot_a,
        snapshot_b.snapshot_id: snapshot_b,
    }
    existing_environment = Environment(
        name="prod",
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="previous_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        finalized_ts=to_timestamp("2023-01-02"),
    )
    state_reader.get_environment.return_value = existing_environment

    environment = Environment(
        name="prod",
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id="previous_plan",
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
    )

    # Create evaluatable plan with restatements
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[],  # No new snapshots
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={
            '"a"': (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
            '"b"': (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        },
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[],  # No changes
        indirectly_modified_snapshots={},  # No changes
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 4

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 2
    assert {s.snapshot_id for s in physical_step.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify RestatementStep
    restatement_step = steps[1]
    assert isinstance(restatement_step, RestatementStep)
    assert len(restatement_step.snapshot_intervals) == 2
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    for snapshot_info, interval in restatement_step.snapshot_intervals.items():
        assert interval == expected_interval
        assert snapshot_info.name in ('"a"', '"b"')

    # Verify BackfillStep
    backfill_step = steps[2]
    assert isinstance(backfill_step, BackfillStep)
    assert len(backfill_step.snapshot_to_intervals) == 2
    assert backfill_step.deployability_index == DeployabilityIndex.all_deployable()
    expected_backfill_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_step.snapshot_to_intervals.values():
        assert intervals == expected_backfill_interval

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[3], EnvironmentRecordUpdateStep)


def test_build_plan_steps_forward_only(
    snapshot_a: Snapshot, snapshot_b: Snapshot, make_snapshot, mocker: MockerFixture
) -> None:
    # Categorize snapshot_a as forward-only
    new_snapshot_a = make_snapshot(
        snapshot_a.model.copy(update={"stamp": "new_version"}),
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    new_snapshot_b = make_snapshot(
        snapshot_b.model.copy(),
        nodes={'"a"': new_snapshot_a.model},
    )
    new_snapshot_b.previous_versions = snapshot_b.all_versions
    new_snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    # Mock state reader to return existing snapshots and environment
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    existing_environment = Environment(
        name="prod",
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="previous_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        finalized_ts=to_timestamp("2023-01-02"),
    )
    state_reader.get_environment.return_value = existing_environment

    # Create environment
    environment = Environment(
        name="prod",
        snapshots=[new_snapshot_a.table_info, new_snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id="previous_plan",
        promoted_snapshot_ids=[new_snapshot_a.snapshot_id, new_snapshot_b.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[new_snapshot_a, new_snapshot_b],
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={},
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[new_snapshot_a.snapshot_id],
        indirectly_modified_snapshots={
            new_snapshot_a.name: [new_snapshot_b.snapshot_id],
        },
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 5

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 2
    assert {s.snapshot_id for s in physical_step.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }
    assert physical_step.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[1], EnvironmentRecordUpdateStep)

    # Verify MigrateSchemasStep
    migrate_step = steps[2]
    assert isinstance(migrate_step, MigrateSchemasStep)
    assert len(migrate_step.snapshots) == 2
    assert {s.snapshot_id for s in migrate_step.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }

    # Verify BackfillStep
    backfill_step = steps[3]
    assert isinstance(backfill_step, BackfillStep)
    assert len(backfill_step.snapshot_to_intervals) == 2
    expected_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_step.snapshot_to_intervals.values():
        assert intervals == expected_interval
    assert backfill_step.deployability_index == DeployabilityIndex.all_deployable()

    # Verify VirtualLayerUpdateStep
    virtual_step = steps[4]
    assert isinstance(virtual_step, VirtualLayerUpdateStep)
    assert len(virtual_step.promoted_snapshots) == 2
    assert len(virtual_step.demoted_snapshots) == 0
    assert {s.name for s in virtual_step.promoted_snapshots} == {'"a"', '"b"'}


def test_build_plan_steps_forward_only_dev(
    snapshot_a: Snapshot, snapshot_b: Snapshot, make_snapshot, mocker: MockerFixture
) -> None:
    # Categorize snapshot_a as forward-only
    new_snapshot_a = make_snapshot(
        snapshot_a.model.copy(update={"stamp": "new_version"}),
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    new_snapshot_b = make_snapshot(
        snapshot_b.model.copy(),
        nodes={'"a"': new_snapshot_a.model},
    )
    new_snapshot_b.previous_versions = snapshot_b.all_versions
    new_snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    # Mock state reader to return existing snapshots and environment
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    state_reader.get_environment.return_value = None

    # Create environment
    environment = Environment(
        name="dev",
        snapshots=[new_snapshot_a.table_info, new_snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[new_snapshot_a.snapshot_id, new_snapshot_b.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[new_snapshot_a, new_snapshot_b],
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={},
        is_dev=True,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[new_snapshot_a.snapshot_id],
        indirectly_modified_snapshots={
            new_snapshot_a.name: [new_snapshot_b.snapshot_id],
        },
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan steps
    steps = build_plan_steps(plan, state_reader, None)

    # Verify steps
    assert len(steps) == 4

    # Verify PhysicalLayerUpdateStep
    physical_step = steps[0]
    assert isinstance(physical_step, PhysicalLayerUpdateStep)
    assert len(physical_step.snapshots) == 2
    assert {s.snapshot_id for s in physical_step.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }
    assert physical_step.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify BackfillStep
    backfill_step = steps[1]
    assert isinstance(backfill_step, BackfillStep)
    assert len(backfill_step.snapshot_to_intervals) == 2
    expected_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_step.snapshot_to_intervals.values():
        assert intervals == expected_interval
    assert backfill_step.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify EnvironmentRecordUpdateStep
    assert isinstance(steps[2], EnvironmentRecordUpdateStep)

    # Verify VirtualLayerUpdateStep
    virtual_step = steps[3]
    assert isinstance(virtual_step, VirtualLayerUpdateStep)
    assert len(virtual_step.promoted_snapshots) == 2
    assert len(virtual_step.demoted_snapshots) == 0
    assert {s.name for s in virtual_step.promoted_snapshots} == {'"a"', '"b"'}
