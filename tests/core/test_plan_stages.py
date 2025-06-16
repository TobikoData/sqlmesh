import pytest
import typing as t
from sqlglot import parse_one
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.model import SqlModel, ModelKindName
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.plan.stages import (
    build_plan_stages,
    AfterAllStage,
    AuditOnlyRunStage,
    PhysicalLayerUpdateStage,
    CreateSnapshotRecordsStage,
    BeforeAllStage,
    BackfillStage,
    EnvironmentRecordUpdateStage,
    VirtualLayerUpdateStage,
    RestatementStage,
    MigrateSchemasStage,
    FinalizeEnvironmentStage,
    UnpauseStage,
)
from sqlmesh.core.snapshot.definition import (
    SnapshotChangeCategory,
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.core.environment import Environment, EnvironmentStatements
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


@pytest.fixture
def snapshot_c(make_snapshot, snapshot_a: Snapshot) -> Snapshot:
    snapshot = make_snapshot(
        SqlModel(
            name="c",
            query=parse_one("select * from a"),
            kind=dict(name=ModelKindName.VIEW),
        ),
        nodes={'"a"': snapshot_a.model},
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    return snapshot


def test_build_plan_stages_basic(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 7

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    assert physical_stage.deployability_index == DeployabilityIndex.all_deployable()

    # Verify BackfillStage
    backfill_stage = stages[2]
    assert isinstance(backfill_stage, BackfillStage)
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert len(backfill_stage.snapshot_to_intervals) == 2
    assert backfill_stage.snapshot_to_intervals[snapshot_a] == [expected_interval]
    assert backfill_stage.snapshot_to_intervals[snapshot_b] == [expected_interval]

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)
    assert stages[3].no_gaps_snapshot_names == {snapshot_a.name, snapshot_b.name}

    # Verify UnpauseStage
    assert isinstance(stages[4], UnpauseStage)
    assert {s.name for s in stages[4].promoted_snapshots} == {snapshot_a.name, snapshot_b.name}

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[5]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {snapshot_a.name, snapshot_b.name}

    state_reader.refresh_snapshot_intervals.assert_called_once()

    assert isinstance(stages[6], FinalizeEnvironmentStage)


def test_build_plan_stages_with_before_all_and_after_all(
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

    environment_statements = [
        EnvironmentStatements(
            before_all=["BEFORE ALL A", "BEFORE ALL B"],
            after_all=["AFTER ALL A", "AFTER ALL B"],
            python_env={},
            jinja_macros=None,
        )
    ]

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
        environment_statements=environment_statements,
        user_provided_flags=None,
    )

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 9

    # Verify BeforeAllStage
    before_all_stage = stages[0]
    assert isinstance(before_all_stage, BeforeAllStage)
    assert before_all_stage.statements == environment_statements

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[1]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[2]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    assert physical_stage.deployability_index == DeployabilityIndex.all_deployable()

    # Verify BackfillStage
    backfill_stage = stages[3]
    assert isinstance(backfill_stage, BackfillStage)
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert len(backfill_stage.snapshot_to_intervals) == 2
    assert backfill_stage.snapshot_to_intervals[snapshot_a] == [expected_interval]
    assert backfill_stage.snapshot_to_intervals[snapshot_b] == [expected_interval]

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[4], EnvironmentRecordUpdateStage)
    assert stages[4].no_gaps_snapshot_names == {snapshot_a.name, snapshot_b.name}

    # Verify UnpauseStage
    assert isinstance(stages[5], UnpauseStage)
    assert {s.name for s in stages[5].promoted_snapshots} == {snapshot_a.name, snapshot_b.name}

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[6]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"', '"b"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[7], FinalizeEnvironmentStage)

    # Verify AfterAllStage
    after_all_stage = stages[8]
    assert isinstance(after_all_stage, AfterAllStage)
    assert after_all_stage.statements == environment_statements


def test_build_plan_stages_select_models(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 7

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 1
    assert {s.snapshot_id for s in physical_stage.snapshots} == {snapshot_a.snapshot_id}
    assert physical_stage.deployability_index == DeployabilityIndex.all_deployable()

    # Verify BackfillStage
    backfill_stage = stages[2]
    assert isinstance(backfill_stage, BackfillStage)
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert len(backfill_stage.snapshot_to_intervals) == 1
    assert backfill_stage.snapshot_to_intervals[snapshot_a] == [expected_interval]

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)
    assert stages[3].no_gaps_snapshot_names == {snapshot_a.name}

    # Verify UnpauseStage
    assert isinstance(stages[4], UnpauseStage)
    assert {s.name for s in stages[4].promoted_snapshots} == {snapshot_a.name}

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[5]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 1
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[6], FinalizeEnvironmentStage)


@pytest.mark.parametrize("skip_backfill,empty_backfill", [(True, False), (False, True)])
def test_build_plan_stages_basic_no_backfill(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 7

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify BackfillStage
    backfill_stage = stages[2]
    assert isinstance(backfill_stage, BackfillStage)
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()
    assert backfill_stage.snapshot_to_intervals == {}

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)
    assert stages[3].no_gaps_snapshot_names == {snapshot_a.name, snapshot_b.name}

    # Verify UnpauseStage
    assert isinstance(stages[4], UnpauseStage)
    assert {s.name for s in stages[4].promoted_snapshots} == {snapshot_a.name, snapshot_b.name}

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[5]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"', '"b"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[6], FinalizeEnvironmentStage)


def test_build_plan_stages_restatement(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 5

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[0]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Verify RestatementStage
    restatement_stage = stages[1]
    assert isinstance(restatement_stage, RestatementStage)
    assert len(restatement_stage.snapshot_intervals) == 2
    expected_interval = (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    for snapshot_info, interval in restatement_stage.snapshot_intervals.items():
        assert interval == expected_interval
        assert snapshot_info.name in ('"a"', '"b"')

    # Verify BackfillStage
    backfill_stage = stages[2]
    assert isinstance(backfill_stage, BackfillStage)
    assert len(backfill_stage.snapshot_to_intervals) == 2
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()
    expected_backfill_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_stage.snapshot_to_intervals.values():
        assert intervals == expected_backfill_interval

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[4], FinalizeEnvironmentStage)


def test_build_plan_stages_forward_only(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 8

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }
    assert physical_stage.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[2], EnvironmentRecordUpdateStage)
    assert stages[2].no_gaps_snapshot_names == set()

    # Verify MigrateSchemasStage
    migrate_stage = stages[3]
    assert isinstance(migrate_stage, MigrateSchemasStage)
    assert len(migrate_stage.snapshots) == 2
    assert {s.snapshot_id for s in migrate_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }

    # Verify UnpauseStage
    assert isinstance(stages[4], UnpauseStage)
    assert {s.name for s in stages[4].promoted_snapshots} == {
        new_snapshot_a.name,
        new_snapshot_b.name,
    }

    # Verify BackfillStage
    backfill_stage = stages[5]
    assert isinstance(backfill_stage, BackfillStage)
    assert len(backfill_stage.snapshot_to_intervals) == 2
    expected_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_stage.snapshot_to_intervals.values():
        assert intervals == expected_interval
    assert backfill_stage.deployability_index == DeployabilityIndex.all_deployable()

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[6]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"', '"b"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[7], FinalizeEnvironmentStage)


def test_build_plan_stages_forward_only_dev(
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 6

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }
    assert physical_stage.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify BackfillStage
    backfill_stage = stages[2]
    assert isinstance(backfill_stage, BackfillStage)
    assert len(backfill_stage.snapshot_to_intervals) == 2
    expected_interval = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    for intervals in backfill_stage.snapshot_to_intervals.values():
        assert intervals == expected_interval
    assert backfill_stage.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[4]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"', '"b"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[5], FinalizeEnvironmentStage)


def test_build_plan_stages_audit_only(
    snapshot_a: Snapshot, snapshot_b: Snapshot, make_snapshot, mocker: MockerFixture
) -> None:
    # Categorize snapshot_a as forward-only
    new_snapshot_a = make_snapshot(
        snapshot_a.model.copy(update={"audits": [("not_null", {})]}),
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.categorize_as(SnapshotChangeCategory.METADATA)
    new_snapshot_a.add_interval("2023-01-01", "2023-01-02")

    new_snapshot_b = make_snapshot(
        snapshot_b.model.copy(),
        nodes={'"a"': new_snapshot_a.model},
    )
    new_snapshot_b.previous_versions = snapshot_b.all_versions
    new_snapshot_b.categorize_as(SnapshotChangeCategory.METADATA)
    new_snapshot_b.add_interval("2023-01-01", "2023-01-02")

    def _get_snapshots(snapshot_ids: t.List[SnapshotId]) -> t.Dict[SnapshotId, Snapshot]:
        if snapshot_a.snapshot_id in snapshot_ids and snapshot_b.snapshot_id in snapshot_ids:
            return {
                snapshot_a.snapshot_id: snapshot_a,
                snapshot_b.snapshot_id: snapshot_b,
            }
        return {}

    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.side_effect = _get_snapshots
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 7

    # Verify CreateSnapshotRecordsStage
    create_snapshot_records_stage = stages[0]
    assert isinstance(create_snapshot_records_stage, CreateSnapshotRecordsStage)
    assert len(create_snapshot_records_stage.snapshots) == 2
    assert {s.snapshot_id for s in create_snapshot_records_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }

    # Verify PhysicalLayerUpdateStage
    physical_stage = stages[1]
    assert isinstance(physical_stage, PhysicalLayerUpdateStage)
    assert len(physical_stage.snapshots) == 2
    assert {s.snapshot_id for s in physical_stage.snapshots} == {
        new_snapshot_a.snapshot_id,
        new_snapshot_b.snapshot_id,
    }
    assert physical_stage.deployability_index == DeployabilityIndex.create(
        [new_snapshot_a, new_snapshot_b]
    )

    # Verify AuditOnlyRunStage
    audit_only_stage = stages[2]
    assert isinstance(audit_only_stage, AuditOnlyRunStage)
    assert len(audit_only_stage.snapshots) == 1
    assert audit_only_stage.snapshots[0].snapshot_id == new_snapshot_a.snapshot_id

    # Verify BackfillStage
    backfill_stage = stages[3]
    assert isinstance(backfill_stage, BackfillStage)
    assert len(backfill_stage.snapshot_to_intervals) == 0

    # Verify EnvironmentRecordUpdateStage
    assert isinstance(stages[4], EnvironmentRecordUpdateStage)

    # Verify VirtualLayerUpdateStage
    virtual_stage = stages[5]
    assert isinstance(virtual_stage, VirtualLayerUpdateStage)
    assert len(virtual_stage.promoted_snapshots) == 2
    assert len(virtual_stage.demoted_snapshots) == 0
    assert {s.name for s in virtual_stage.promoted_snapshots} == {'"a"', '"b"'}

    # Verify FinalizeEnvironmentStage
    assert isinstance(stages[6], FinalizeEnvironmentStage)


def test_build_plan_stages_forward_only_ensure_finalized_snapshots(
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
        ensure_finalized_snapshots=True,
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    assert len(stages) == 8
    assert isinstance(stages[0], CreateSnapshotRecordsStage)
    assert isinstance(stages[1], PhysicalLayerUpdateStage)
    assert isinstance(stages[2], EnvironmentRecordUpdateStage)
    assert isinstance(stages[3], MigrateSchemasStage)
    assert isinstance(stages[4], BackfillStage)
    assert isinstance(stages[5], UnpauseStage)
    assert isinstance(stages[6], VirtualLayerUpdateStage)
    assert isinstance(stages[7], FinalizeEnvironmentStage)


def test_build_plan_stages_removed_model(
    snapshot_a: Snapshot, snapshot_b: Snapshot, mocker: MockerFixture
) -> None:
    # Mock state reader
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

    # Create environment
    environment = Environment(
        snapshots=[snapshot_a.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id="previous_plan",
        promoted_snapshot_ids=[snapshot_a.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[],
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
        directly_modified_snapshots=[],
        indirectly_modified_snapshots={},
        removed_snapshots=[snapshot_b.snapshot_id],
        requires_backfill=False,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 5

    assert isinstance(stages[0], PhysicalLayerUpdateStage)
    assert isinstance(stages[1], BackfillStage)
    assert isinstance(stages[2], EnvironmentRecordUpdateStage)
    assert isinstance(stages[3], VirtualLayerUpdateStage)
    assert isinstance(stages[4], FinalizeEnvironmentStage)

    virtual_layer_update_stage = stages[3]
    assert virtual_layer_update_stage.promoted_snapshots == set()
    assert virtual_layer_update_stage.demoted_snapshots == {snapshot_b.table_info}
    assert (
        virtual_layer_update_stage.demoted_environment_naming_info
        == existing_environment.naming_info
    )


def test_build_plan_stages_environment_suffix_target_changed(
    snapshot_a: Snapshot, snapshot_b: Snapshot, mocker: MockerFixture
) -> None:
    # Mock state reader
    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {
        snapshot_a.snapshot_id: snapshot_a,
        snapshot_b.snapshot_id: snapshot_b,
    }
    existing_environment = Environment(
        name="dev",
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
        name="dev",
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id="previous_plan",
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_b.snapshot_id],
        suffix_target=EnvironmentSuffixTarget.TABLE,
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[],
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
        directly_modified_snapshots=[],
        indirectly_modified_snapshots={},
        removed_snapshots=[],
        requires_backfill=False,
        models_to_backfill=None,
        interval_end_per_model=None,
        execution_time="2023-01-02",
        disabled_restatement_models=set(),
        environment_statements=None,
        user_provided_flags=None,
    )

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 5

    assert isinstance(stages[0], PhysicalLayerUpdateStage)
    assert isinstance(stages[1], BackfillStage)
    assert isinstance(stages[2], EnvironmentRecordUpdateStage)
    assert isinstance(stages[3], VirtualLayerUpdateStage)
    assert isinstance(stages[4], FinalizeEnvironmentStage)

    virtual_layer_update_stage = stages[3]
    assert virtual_layer_update_stage.promoted_snapshots == {
        snapshot_a.table_info,
        snapshot_b.table_info,
    }
    assert virtual_layer_update_stage.demoted_snapshots == {
        snapshot_a.table_info,
        snapshot_b.table_info,
    }
    assert (
        virtual_layer_update_stage.demoted_environment_naming_info
        == existing_environment.naming_info
    )


def test_build_plan_stages_indirect_non_breaking_no_migration(
    snapshot_a: Snapshot, snapshot_b: Snapshot, make_snapshot, mocker: MockerFixture
) -> None:
    # Categorize snapshot_a as forward-only
    new_snapshot_a = make_snapshot(
        snapshot_a.model.copy(update={"stamp": "new_version"}),
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    new_snapshot_b = make_snapshot(
        snapshot_b.model.copy(),
        nodes={'"a"': new_snapshot_a.model},
    )
    new_snapshot_b.previous_versions = snapshot_b.all_versions
    new_snapshot_b.change_category = SnapshotChangeCategory.INDIRECT_NON_BREAKING
    new_snapshot_b.version = new_snapshot_b.previous_version.data_version.version

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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 7

    assert isinstance(stages[0], CreateSnapshotRecordsStage)
    assert isinstance(stages[1], PhysicalLayerUpdateStage)
    assert isinstance(stages[2], BackfillStage)
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)
    assert isinstance(stages[4], UnpauseStage)
    assert isinstance(stages[5], VirtualLayerUpdateStage)
    assert isinstance(stages[6], FinalizeEnvironmentStage)


def test_build_plan_stages_indirect_non_breaking_view_migration(
    snapshot_a: Snapshot, snapshot_c: Snapshot, make_snapshot, mocker: MockerFixture
) -> None:
    # Categorize snapshot_a as forward-only
    new_snapshot_a = make_snapshot(
        snapshot_a.model.copy(update={"stamp": "new_version"}),
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    new_snapshot_c = make_snapshot(
        snapshot_c.model.copy(),
        nodes={'"a"': new_snapshot_a.model},
    )
    new_snapshot_c.previous_versions = snapshot_c.all_versions
    new_snapshot_c.change_category = SnapshotChangeCategory.INDIRECT_NON_BREAKING
    new_snapshot_c.version = new_snapshot_c.previous_version.data_version.version

    state_reader = mocker.Mock(spec=StateReader)
    state_reader.get_snapshots.return_value = {}
    existing_environment = Environment(
        name="prod",
        snapshots=[snapshot_a.table_info, snapshot_c.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="previous_plan",
        previous_plan_id=None,
        promoted_snapshot_ids=[snapshot_a.snapshot_id, snapshot_c.snapshot_id],
        finalized_ts=to_timestamp("2023-01-02"),
    )
    state_reader.get_environment.return_value = existing_environment

    # Create environment
    environment = Environment(
        name="prod",
        snapshots=[new_snapshot_a.table_info, new_snapshot_c.table_info],
        start_at="2023-01-01",
        end_at="2023-01-02",
        plan_id="test_plan",
        previous_plan_id="previous_plan",
        promoted_snapshot_ids=[new_snapshot_a.snapshot_id, new_snapshot_c.snapshot_id],
    )

    # Create evaluatable plan
    plan = EvaluatablePlan(
        start="2023-01-01",
        end="2023-01-02",
        new_snapshots=[new_snapshot_a, new_snapshot_c],
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
            new_snapshot_a.name: [new_snapshot_c.snapshot_id],
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

    # Build plan stages
    stages = build_plan_stages(plan, state_reader, None)

    # Verify stages
    assert len(stages) == 8

    assert isinstance(stages[0], CreateSnapshotRecordsStage)
    assert isinstance(stages[1], PhysicalLayerUpdateStage)
    assert isinstance(stages[2], BackfillStage)
    assert isinstance(stages[3], EnvironmentRecordUpdateStage)
    assert isinstance(stages[4], MigrateSchemasStage)
    assert isinstance(stages[5], UnpauseStage)
    assert isinstance(stages[6], VirtualLayerUpdateStage)
    assert isinstance(stages[7], FinalizeEnvironmentStage)

    migrate_schemas_stage = stages[4]
    assert {s.snapshot_id for s in migrate_schemas_stage.snapshots} == {new_snapshot_c.snapshot_id}
