import logging
import typing as t
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlmesh.utils.metaprogramming import Executable
from tests.core.test_table_diff import create_test_console, strip_ansi_codes
import time_machine
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.engine_adapter import DuckDBEngineAdapter
from sqlmesh.core.environment import EnvironmentNamingInfo, EnvironmentStatements
from sqlmesh.core.model import (
    ExternalModel,
    FullKind,
    IncrementalByTimeRangeKind,
    SeedKind,
    SeedModel,
    SqlModel,
    ModelKindName,
)
from sqlmesh.core.model.kind import OnDestructiveChange
from sqlmesh.core.model.seed import Seed
from sqlmesh.core.plan import Plan, PlanBuilder, SnapshotIntervals
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotFingerprint,
)
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    now,
    now_timestamp,
    to_date,
    to_datetime,
    to_timestamp,
    yesterday_ds,
)
from sqlmesh.utils.errors import PlanError


def test_forward_only_plan_sets_version(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), dialect="duckdb")
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select 2, ds"), dialect="duckdb")
    )
    snapshot_b.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            change_category=SnapshotChangeCategory.FORWARD_ONLY,
            dev_table_suffix="dev",
        ),
    )
    assert not snapshot_b.version

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot_b.name: (snapshot_b, snapshot_b)},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={snapshot_b.snapshot_id: snapshot_b},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan_builder = PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True)

    plan_builder.build()
    assert snapshot_b.version == "test_version"

    # Make sure that the choice can't be set manually.
    with pytest.raises(PlanError, match="Choice setting is not supported by a forward-only plan."):
        plan_builder.set_choice(snapshot_b, SnapshotChangeCategory.BREAKING).build()


def test_forward_only_dev(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    # Simulate a direct change.
    updated_snapshot = make_snapshot(
        SqlModel(
            **{
                **snapshot.model.dict(),
                "query": parse_one("select 2, ds"),
            }
        )
    )

    expected_start = to_date("2022-01-02")
    expected_end = to_date("2022-01-03")
    expected_interval_end = to_timestamp(to_date("2022-01-04"))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    yesterday_ds_mock = mocker.patch("sqlmesh.core.plan.builder.yesterday_ds")
    yesterday_ds_mock.return_value = expected_start

    now_mock = mocker.patch("sqlmesh.core.snapshot.definition.now")
    now_mock.return_value = expected_end

    mocker.patch("sqlmesh.core.plan.builder.now").return_value = expected_end
    mocker.patch("sqlmesh.core.plan.definition.now").return_value = expected_end

    plan = PlanBuilder(
        context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True, is_dev=True
    ).build()

    assert plan.restatements == {
        updated_snapshot.snapshot_id: (to_timestamp(expected_start), expected_interval_end)
    }
    assert plan.start == to_date(expected_start)
    assert plan.end == expected_end


def test_forward_only_metadata_change_dev(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    # Simulate a metadata change.
    updated_snapshot = make_snapshot(
        SqlModel(
            **{
                **snapshot.model.dict(),
                "owner": "new_owner",
            }
        )
    )

    expected_start = to_date("2022-01-02")
    expected_end = to_date("2022-01-03")

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: snapshot},
        new_snapshots={updated_snapshot.snapshot_id: snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    yesterday_ds_mock = mocker.patch("sqlmesh.core.plan.builder.yesterday_ds")
    yesterday_ds_mock.return_value = expected_start

    now_mock = mocker.patch("sqlmesh.core.snapshot.definition.now")
    now_mock.return_value = expected_end

    mocker.patch("sqlmesh.core.plan.builder.now").return_value = expected_end
    mocker.patch("sqlmesh.core.plan.definition.now").return_value = expected_end

    plan = PlanBuilder(
        context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True, is_dev=True
    ).build()

    assert not plan.restatements


def test_forward_only_plan_added_models(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1 as a, ds"), dialect="duckdb")
    )

    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select a, ds from a"), dialect="duckdb"),
        nodes={"a": snapshot_a.node},
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={},
        modified_snapshots={snapshot_a.name: (snapshot_a, snapshot_a)},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True).build()
    assert snapshot_a.change_category == SnapshotChangeCategory.FORWARD_ONLY
    assert snapshot_b.change_category == SnapshotChangeCategory.BREAKING


def test_forward_only_plan_categorizes_change_model_kind_as_breaking(
    make_snapshot, mocker: MockerFixture
):
    snapshot_old = make_snapshot(
        SqlModel(
            name="a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    # Simulate a change in the model kind.
    updated_snapshot = make_snapshot(
        SqlModel(
            **{
                **snapshot_old.model.dict(),
                "kind": dict(name=ModelKindName.VIEW),
            }
        )
    )

    context_diff = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot_old)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True).build()

    assert updated_snapshot.change_category == SnapshotChangeCategory.BREAKING


def test_paused_forward_only_parent(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            change_category=SnapshotChangeCategory.BREAKING,
            dev_table_suffix="dev",
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    snapshot_b_old = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds from a")))
    snapshot_b_old.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 3, ds from a")))
    assert not snapshot_b.version

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot_b.name: (snapshot_b, snapshot_b_old)},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={snapshot_b.snapshot_id: snapshot_b},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=False).build()
    assert snapshot_b.change_category == SnapshotChangeCategory.BREAKING


def test_forward_only_plan_allow_destructive_models(
    make_snapshot, make_snapshot_on_destructive_change
):
    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER

    # forward-only model, not forward-only plan
    snapshot_a_old, snapshot_a = make_snapshot_on_destructive_change()

    context_diff_a = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot_a.name: (snapshot_a, snapshot_a_old)},
        snapshots={snapshot_a.snapshot_id: snapshot_a, snapshot_a_old.snapshot_id: snapshot_a_old},
        new_snapshots={snapshot_a.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    with pytest.raises(
        PlanError, match="Plan requires a destructive change to a forward-only model"
    ):
        PlanBuilder(context_diff_a, schema_differ, forward_only=False).build()

    logger = logging.getLogger("sqlmesh.core.plan.builder")
    with patch.object(logger, "warning") as mock_logger:
        assert PlanBuilder(
            context_diff_a, schema_differ, forward_only=False, allow_destructive_models=['"a"']
        ).build()
        assert mock_logger.call_count == 0

    # forward-only plan, no forward-only model
    snapshot_b_old = make_snapshot(
        SqlModel(
            name="b",
            dialect="duckdb",
            query=parse_one("select '1' as one, '2022-01-01' ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            dialect="duckdb",
            query=parse_one("select 1 as one, '2022-01-01' ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    snapshot_c_old = make_snapshot(
        SqlModel(
            name="c",
            dialect="duckdb",
            query=parse_one("select '1' as one"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            dialect="duckdb",
            query=parse_one("select 1 as one"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    context_diff_b = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_b.name: (snapshot_b, snapshot_b_old),
            snapshot_c.name: (snapshot_c, snapshot_c_old),
        },
        snapshots={
            snapshot_b.snapshot_id: snapshot_b,
            snapshot_b_old.snapshot_id: snapshot_b_old,
            snapshot_c.snapshot_id: snapshot_c,
            snapshot_c_old.snapshot_id: snapshot_c_old,
        },
        new_snapshots={snapshot_b.snapshot_id: snapshot_b, snapshot_c.snapshot_id: snapshot_c},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    with pytest.raises(
        PlanError,
        match="""Plan requires a destructive change to a forward-only model.""",
    ):
        PlanBuilder(context_diff_b, schema_differ, forward_only=True).build()

    with pytest.raises(
        PlanError,
        match="""Plan requires a destructive change to a forward-only model.""",
    ):
        PlanBuilder(
            context_diff_b, schema_differ, forward_only=True, allow_destructive_models=['"b"']
        ).build()

    logger = logging.getLogger("sqlmesh.core.plan.builder")
    with patch.object(logger, "warning") as mock_logger:
        PlanBuilder(
            context_diff_b,
            schema_differ,
            forward_only=True,
            allow_destructive_models=['"b"', '"c"'],
        ).build()
        assert mock_logger.call_count == 0


def test_forward_only_model_on_destructive_change(
    make_snapshot, make_snapshot_on_destructive_change
):
    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER

    # direct change to A
    snapshot_a_old, snapshot_a = make_snapshot_on_destructive_change()

    context_diff_1 = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_a.name: (snapshot_a, snapshot_a_old),
        },
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
        },
        new_snapshots={
            snapshot_a.snapshot_id: snapshot_a,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    with pytest.raises(
        PlanError,
        match="""Plan requires a destructive change to a forward-only model.""",
    ):
        PlanBuilder(context_diff_1, schema_differ).build()

    # allow A, indirect change to B
    snapshot_a_old2, snapshot_a2 = make_snapshot_on_destructive_change(
        on_destructive_change=OnDestructiveChange.ALLOW
    )

    snapshot_b_old2 = make_snapshot(
        SqlModel(
            name="b",
            dialect="duckdb",
            query=parse_one("select one, '2022-01-01' ds from a"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        ),
        nodes={'"a"': snapshot_a_old2.model},
    )
    snapshot_b2 = make_snapshot(snapshot_b_old2.model, nodes={'"a"': snapshot_a2.model})
    snapshot_b2.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            dev_table_suffix="dev",
        ),
    )

    context_diff_2 = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_a2.name: (snapshot_a2, snapshot_a_old2),
            snapshot_b2.name: (snapshot_b2, snapshot_b_old2),
        },
        snapshots={
            snapshot_a2.snapshot_id: snapshot_a2,
            snapshot_b2.snapshot_id: snapshot_b2,
        },
        new_snapshots={
            snapshot_a2.snapshot_id: snapshot_a2,
            snapshot_b2.snapshot_id: snapshot_b2,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff_2, schema_differ).build()

    # allow A and B, indirect change to C
    snapshot_a_old3, snapshot_a3 = make_snapshot_on_destructive_change(
        on_destructive_change=OnDestructiveChange.ALLOW
    )

    snapshot_b_old3 = make_snapshot(
        SqlModel(
            name="b",
            dialect="duckdb",
            query=parse_one("select one, '2022-01-01' ds from a"),
            kind=IncrementalByTimeRangeKind(
                time_column="ds",
                forward_only=True,
                on_destructive_change=OnDestructiveChange.ALLOW,
            ),
        ),
        nodes={'"a"': snapshot_a_old3.model},
    )
    snapshot_b3 = make_snapshot(snapshot_b_old3.model, nodes={'"a"': snapshot_a3.model})
    snapshot_b3.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            dev_table_suffix="dev",
        ),
    )

    snapshot_c_old3 = make_snapshot(
        SqlModel(
            name="c",
            dialect="duckdb",
            query=parse_one("select one, '2022-01-01' ds from b"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        ),
        nodes={'"a"': snapshot_a_old3.model, '"b"': snapshot_b_old3.model},
    )
    snapshot_c3 = make_snapshot(
        snapshot_c_old3.model, nodes={'"a"': snapshot_a3.model, '"b"': snapshot_b3.model}
    )
    snapshot_c3.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            dev_table_suffix="dev",
        ),
    )

    context_diff_3 = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_a3.name: (snapshot_a3, snapshot_a_old3),
            snapshot_b3.name: (snapshot_b3, snapshot_b_old3),
            snapshot_c3.name: (snapshot_c3, snapshot_c_old3),
        },
        snapshots={
            snapshot_a3.snapshot_id: snapshot_a3,
            snapshot_b3.snapshot_id: snapshot_b3,
            snapshot_c3.snapshot_id: snapshot_c3,
        },
        new_snapshots={
            snapshot_a3.snapshot_id: snapshot_a3,
            snapshot_b3.snapshot_id: snapshot_b3,
            snapshot_c3.snapshot_id: snapshot_c3,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff_3, schema_differ).build()


def test_forward_only_model_on_destructive_change_no_column_types(
    make_snapshot_on_destructive_change,
):
    snapshot_a_old, snapshot_a = make_snapshot_on_destructive_change(
        old_query="select 1 as one, '2022-01-01' ds", new_query="select one, '2022-01-01' ds"
    )

    context_diff_1 = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_a.name: (snapshot_a, snapshot_a_old),
        },
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_a_old.snapshot_id: snapshot_a_old,
        },
        new_snapshots={
            snapshot_a.snapshot_id: snapshot_a,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    logger = logging.getLogger("sqlmesh.core.plan.builder")
    with patch.object(logger, "warning") as mock_logger:
        PlanBuilder(context_diff_1, DuckDBEngineAdapter.SCHEMA_DIFFER).build()
        assert mock_logger.call_count == 0


def test_missing_intervals_lookback(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, '2022-01-01' ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", lookback=2),
        )
    )
    snapshot_a.intervals = [(to_timestamp("2022-01-01"), to_timestamp("2022-01-05"))]
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        modified_snapshots={},
        removed_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
        },
        new_snapshots={snapshot_a.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = Plan(
        context_diff=context_diff,
        plan_id="",
        provided_start="2022-01-01",
        provided_end="2022-01-04",
        execution_time="2022-01-05 12:00",
        is_dev=True,
        skip_backfill=False,
        empty_backfill=False,
        no_gaps=False,
        forward_only=False,
        allow_destructive_models=set(),
        include_unmodified=True,
        environment_naming_info=EnvironmentNamingInfo(),
        directly_modified={snapshot_a.snapshot_id},
        indirectly_modified={},
        deployability_index=DeployabilityIndex.all_deployable(),
        restatements={},
        end_bounded=False,
        ensure_finalized_snapshots=False,
        interval_end_per_model=None,
    )

    assert not plan.missing_intervals


@pytest.mark.slow
@time_machine.travel(now(), tick=False)
def test_restate_models(sushi_context_pre_scheduling: Context):
    plan = sushi_context_pre_scheduling.plan(
        restate_models=["sushi.waiter_revenue_by_day", "tag:expensive"], no_prompts=True
    )

    start = to_timestamp(plan.start)
    tomorrow = to_timestamp(to_date("tomorrow"))

    assert plan.restatements == {
        sushi_context_pre_scheduling.get_snapshot(
            "sushi.waiter_revenue_by_day", raise_if_missing=True
        ).snapshot_id: (
            start,
            tomorrow,
        ),
        sushi_context_pre_scheduling.get_snapshot(
            "sushi.top_waiters", raise_if_missing=True
        ).snapshot_id: (
            start,
            tomorrow,
        ),
        sushi_context_pre_scheduling.get_snapshot(
            "sushi.customer_revenue_by_day", raise_if_missing=True
        ).snapshot_id: (
            start,
            tomorrow,
        ),
        sushi_context_pre_scheduling.get_snapshot(
            "sushi.customer_revenue_lifetime", raise_if_missing=True
        ).snapshot_id: (
            start,
            tomorrow,
        ),
    }
    assert plan.requires_backfill
    assert plan.models_to_backfill == {
        '"memory"."sushi"."customer_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_lifetime"',
        '"memory"."sushi"."items"',
        '"memory"."sushi"."order_items"',
        '"memory"."sushi"."orders"',
        '"memory"."sushi"."top_waiters"',
        '"memory"."sushi"."waiter_revenue_by_day"',
    }

    with pytest.raises(
        PlanError,
        match="Selector did not return any models. Please check your model selection and try again.",
    ):
        sushi_context_pre_scheduling.plan(restate_models=["unknown_model"], no_prompts=True)

    with pytest.raises(
        PlanError,
        match="Selector did not return any models. Please check your model selection and try again.",
    ):
        sushi_context_pre_scheduling.plan(restate_models=["tag:unknown_tag"], no_prompts=True)

    plan = sushi_context_pre_scheduling.plan(restate_models=["raw.demographics"], no_prompts=True)
    assert not plan.has_changes
    assert plan.restatements
    assert plan.models_to_backfill == {
        '"memory"."raw"."demographics"',
        '"memory"."sushi"."active_customers"',
        '"memory"."sushi"."customers"',
        '"memory"."sushi"."marketing"',
        '"memory"."sushi"."orders"',
        '"memory"."sushi"."raw_marketing"',
        '"memory"."sushi"."waiter_as_customer_by_day"',
        '"memory"."sushi"."waiter_names"',
        '"memory"."sushi"."waiters"',
    }


@pytest.mark.slow
@time_machine.travel(now(minute_floor=False), tick=False)
def test_restate_models_with_existing_missing_intervals(init_and_plan_context: t.Callable):
    sushi_context, plan = init_and_plan_context("examples/sushi")
    sushi_context.apply(plan)

    yesterday_ts = to_timestamp(yesterday_ds())

    assert not sushi_context.plan(no_prompts=True).requires_backfill
    waiter_revenue_by_day = sushi_context.snapshots['"memory"."sushi"."waiter_revenue_by_day"']
    sushi_context.state_sync.remove_intervals(
        [(waiter_revenue_by_day, (yesterday_ts, waiter_revenue_by_day.intervals[0][1]))]
    )
    assert sushi_context.plan(no_prompts=True).requires_backfill

    plan = sushi_context.plan(restate_models=["sushi.waiter_revenue_by_day"], no_prompts=True)

    one_day_ms = 24 * 60 * 60 * 1000

    today_ts = to_timestamp(to_date("today"))
    plan_start_ts = to_timestamp(plan.start)
    assert plan_start_ts == today_ts - 7 * one_day_ms

    expected_missing_intervals = [
        (i, i + one_day_ms) for i in range(plan_start_ts, today_ts, one_day_ms)
    ]
    assert len(expected_missing_intervals) == 7

    waiter_revenue_by_day_snapshot_id = sushi_context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    ).snapshot_id
    top_waiters_snapshot_id = sushi_context.get_snapshot(
        "sushi.top_waiters", raise_if_missing=True
    ).snapshot_id

    assert plan.restatements == {
        waiter_revenue_by_day_snapshot_id: (
            plan_start_ts,
            today_ts,
        ),
        top_waiters_snapshot_id: (
            plan_start_ts,
            to_timestamp(to_date("tomorrow")),
        ),
    }
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot_id,
            intervals=expected_missing_intervals,
        ),
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot_id,
            intervals=expected_missing_intervals,
        ),
    ]
    assert plan.requires_backfill
    assert plan.models_to_backfill == {
        top_waiters_snapshot_id.name,
        waiter_revenue_by_day_snapshot_id.name,
        '"memory"."sushi"."items"',
        '"memory"."sushi"."order_items"',
        '"memory"."sushi"."orders"',
    }


def test_restate_symbolic_model(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, key"),
            kind="EMBEDDED",
        )
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot_a.snapshot_id: snapshot_a},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = PlanBuilder(
        context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, restate_models=[snapshot_a.name]
    ).build()
    assert plan.restatements


def test_restate_seed_model(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="new_content"),
            column_hashes={"col": "hash2"},
            depends_on=set(),
        )
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot_a.snapshot_id: snapshot_a},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = PlanBuilder(
        context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, restate_models=[snapshot_a.name]
    ).build()
    assert not plan.restatements


def test_restate_missing_model(make_snapshot, mocker: MockerFixture):
    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    with pytest.raises(
        PlanError,
        match=r"Cannot restate model 'missing'. Model does not exist.",
    ):
        PlanBuilder(
            context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, restate_models=["missing"]
        ).build()


def test_new_snapshots_with_restatements(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot_a.snapshot_id: snapshot_a},
        new_snapshots={snapshot_a.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    with pytest.raises(
        PlanError,
        match=r"Model changes and restatements can't be a part of the same plan.*",
    ):
        PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, restate_models=["a"]).build()


def test_end_validation(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_a.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot_a.snapshot_id: snapshot_a},
        new_snapshots={snapshot_a.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    dev_plan_builder = PlanBuilder(context_diff, schema_differ, end="2022-01-03", is_dev=True)
    assert dev_plan_builder.build().end == "2022-01-03"
    dev_plan_builder.set_end("2022-01-04")
    assert dev_plan_builder.build().end == "2022-01-04"

    start_end_not_allowed_message = (
        "The start and end dates can't be set for a production plan without restatements."
    )

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        PlanBuilder(context_diff, schema_differ, end="2022-01-03").build()

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        PlanBuilder(context_diff, schema_differ, start="2022-01-03").build()

    prod_plan_builder = PlanBuilder(context_diff, schema_differ)

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        prod_plan_builder.set_end("2022-01-03").build()

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        prod_plan_builder.set_start("2022-01-03").build()

    context_diff.new_snapshots = {}
    restatement_prod_plan_builder = PlanBuilder(
        context_diff,
        schema_differ,
        start="2022-01-01",
        end="2022-01-03",
        restate_models=['"a"'],
    )
    assert restatement_prod_plan_builder.build().end == "2022-01-03"
    restatement_prod_plan_builder.set_end("2022-01-04")
    assert restatement_prod_plan_builder.build().end == "2022-01-04"


def test_forward_only_revert_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert not snapshot.is_forward_only

    forward_only_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))
    forward_only_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    forward_only_snapshot.version = snapshot.version
    forward_only_snapshot.unpaused_ts = now_timestamp()
    assert forward_only_snapshot.is_forward_only

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot.name: (snapshot, forward_only_snapshot)},
        snapshots={snapshot.snapshot_id: snapshot},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    with pytest.raises(
        PlanError,
        match=r"Attempted to revert to an unrevertable version of model.*",
    ):
        PlanBuilder(context_diff, schema_differ, forward_only=True).build()

    # Make sure the plan can be created if a new snapshot version was enforced.
    new_version_snapshot = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), stamp="test_stamp")
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    context_diff.modified_snapshots = {snapshot.name: (new_version_snapshot, forward_only_snapshot)}
    context_diff.new_snapshots = {new_version_snapshot.snapshot_id: new_version_snapshot}
    PlanBuilder(context_diff, schema_differ, forward_only=True).build()


def test_forward_only_plan_seed_models(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="content"),
            column_hashes={"col": "hash1"},
            depends_on=set(),
        )
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_a_updated = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="new_content"),
            column_hashes={"col": "hash2"},
            depends_on=set(),
        )
    )
    assert snapshot_a_updated.version is None
    assert snapshot_a_updated.change_category is None

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot_a_updated.name: (snapshot_a_updated, snapshot_a)},
        snapshots={snapshot_a_updated.snapshot_id: snapshot_a_updated},
        new_snapshots={snapshot_a_updated.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, forward_only=True).build()
    assert snapshot_a_updated.version == snapshot_a_updated.fingerprint.to_version()
    assert snapshot_a_updated.change_category == SnapshotChangeCategory.NON_BREAKING


def test_start_inference(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), start="2022-01-01")
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    snapshot_b.add_interval("2022-01-01", now())

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    plan = PlanBuilder(context_diff, schema_differ).build()
    assert len(plan.missing_intervals) == 1
    assert plan.missing_intervals[0].snapshot_id == snapshot_a.snapshot_id
    assert plan.start == to_timestamp("2022-01-01")

    # Test inference from existing intervals
    context_diff.snapshots = {snapshot_b.snapshot_id: snapshot_b}
    plan = PlanBuilder(context_diff, schema_differ).build()
    assert not plan.missing_intervals
    assert plan.start == to_datetime("2022-01-01")


def test_auto_categorization(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    updated_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER).build()

    assert updated_snapshot.version == updated_snapshot.fingerprint.to_version()
    assert updated_snapshot.change_category == SnapshotChangeCategory.BREAKING


def test_auto_categorization_missing_schema_downstream(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, 2, ds")))

    # selects * from `tbl` which is not defined and has an unknown schema
    # therefore we can't be sure what is included in the star select
    downstream_snapshot = make_snapshot(
        SqlModel(name="b", query=parse_one("select * from tbl"), depends_on={'"a"'}),
        nodes={'"a"': snapshot.model},
    )
    downstream_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_downstream_snapshot = make_snapshot(
        downstream_snapshot.model,
        nodes={'"a"': updated_snapshot.model},
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            updated_snapshot.name: (updated_snapshot, snapshot),
            updated_downstream_snapshot.name: (updated_downstream_snapshot, downstream_snapshot),
        },
        snapshots={
            updated_snapshot.snapshot_id: updated_snapshot,
            updated_downstream_snapshot.snapshot_id: updated_downstream_snapshot,
        },
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER).build()

    assert updated_snapshot.version
    assert updated_snapshot.change_category == SnapshotChangeCategory.BREAKING


def test_broken_references(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds FROM a")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={snapshot_a.snapshot_id: snapshot_a.table_info},
        modified_snapshots={snapshot_b.name: (snapshot_b, snapshot_b)},
        snapshots={snapshot_b.snapshot_id: snapshot_b},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    # Make sure the downstream snapshot doesn't have any parents,
    # since its only parent has been removed from the project.
    assert not snapshot_b.parents

    with pytest.raises(
        PlanError,
        match=r"""Removed '"a"' are referenced in '"b"'.*""",
    ):
        PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER).build()


def test_broken_references_external_model(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(ExternalModel(name="a", kind=dict(name=ModelKindName.EXTERNAL)))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds FROM a")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={snapshot_a.snapshot_id: snapshot_a.table_info},
        modified_snapshots={snapshot_b.name: (snapshot_b, snapshot_b)},
        snapshots={snapshot_b.snapshot_id: snapshot_b},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    # Make sure the downstream snapshot doesn't have any parents,
    # since its only parent has been removed from the project.
    assert not snapshot_b.parents

    # Shouldn't raise
    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER).build()


def test_effective_from(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a", query=parse_one("select 1, ds FROM b"), start="2023-01-01", dialect="duckdb"
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.add_interval("2023-01-01", "2023-03-01")

    updated_snapshot = make_snapshot(
        SqlModel(
            name="a", query=parse_one("select 2, ds FROM b"), start="2023-01-01", dialect="duckdb"
        )
    )
    updated_snapshot.previous_versions = snapshot.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    with pytest.raises(
        PlanError,
        match="Effective date can only be set for a forward-only plan.",
    ):
        PlanBuilder(context_diff, schema_differ).set_effective_from("2023-02-01").build()

    # The snapshot gets categorized as breaking in previous step so we want to reset that back to None
    updated_snapshot.change_category = None
    plan_builder = PlanBuilder(
        context_diff,
        schema_differ,
        forward_only=True,
        start="2023-01-01",
        end="2023-03-01",
        is_dev=True,
    )
    updated_snapshot.add_interval("2023-01-01", "2023-03-01")

    with pytest.raises(
        PlanError,
        match="Effective date cannot be in the future.",
    ):
        plan_builder.set_effective_from(now() + timedelta(days=1)).build()

    assert plan_builder.set_effective_from(None).build().effective_from is None
    assert updated_snapshot.effective_from is None
    assert not plan_builder.build().missing_intervals

    plan_builder.set_effective_from("2023-02-01")
    assert plan_builder.build().effective_from == "2023-02-01"
    assert updated_snapshot.effective_from == "2023-02-01"

    assert len(plan_builder.build().missing_intervals) == 1
    missing_intervals = plan_builder.build().missing_intervals[0]
    assert missing_intervals.intervals[0][0] == to_timestamp("2023-02-01")
    assert missing_intervals.intervals[-1][-1] == to_timestamp("2023-03-02")

    plan_builder.set_effective_from(None)
    assert plan_builder.build().effective_from is None
    assert updated_snapshot.effective_from is None


def test_effective_from_non_evaluatble_model(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            kind="EMBEDDED",
            query=parse_one("select 1, ds FROM b"),
            start="2023-01-01",
            dialect="duckdb",
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    updated_snapshot = make_snapshot(
        SqlModel(
            name="a",
            kind="EMBEDDED",
            query=parse_one("select 2, ds FROM b"),
            start="2023-01-01",
            dialect="duckdb",
        )
    )
    updated_snapshot.previous_versions = snapshot.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    plan_builder = PlanBuilder(
        context_diff,
        schema_differ,
        forward_only=True,
        start="2023-01-01",
        end="2023-03-01",
        is_dev=True,
    )

    plan_builder.set_effective_from("2023-02-01")
    assert plan_builder.build().effective_from == "2023-02-01"
    assert not updated_snapshot.effective_from


def test_new_environment_no_changes(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot.snapshot_id: snapshot},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    with pytest.raises(
        PlanError, match="Creating a new environment requires a change, but project files match.*"
    ):
        PlanBuilder(context_diff, schema_differ, is_dev=True).build()

    assert (
        PlanBuilder(context_diff, schema_differ).build().environment.promoted_snapshot_ids is None
    )
    assert (
        PlanBuilder(context_diff, schema_differ, is_dev=True, include_unmodified=True)
        .build()
        .environment.promoted_snapshot_ids
        is None
    )


def test_new_environment_with_changes(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 3, ds")))

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot_a.name: (updated_snapshot_a, snapshot_a)},
        snapshots={
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={updated_snapshot_a.snapshot_id: updated_snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    # Modified the existing model.
    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    assert PlanBuilder(
        context_diff, schema_differ, is_dev=True
    ).build().environment.promoted_snapshot_ids == [updated_snapshot_a.snapshot_id]

    # Updating the existing environment with a previously promoted snapshot.
    context_diff.previously_promoted_snapshot_ids = {
        updated_snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    context_diff.is_new_environment = False
    assert set(
        PlanBuilder(context_diff, schema_differ, is_dev=True)
        .build()
        .environment.promoted_snapshot_ids
        or []
    ) == {
        updated_snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Adding a new model
    snapshot_c = make_snapshot(SqlModel(name="c", query=parse_one("select 4, ds")))
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)
    context_diff.snapshots = {
        updated_snapshot_a.snapshot_id: updated_snapshot_a,
        snapshot_b.snapshot_id: snapshot_b,
        snapshot_c.snapshot_id: snapshot_c,
    }
    context_diff.added = {snapshot_c.snapshot_id}
    context_diff.modified_snapshots = {}
    context_diff.new_snapshots = {snapshot_c.snapshot_id: snapshot_c}

    assert set(
        PlanBuilder(context_diff, schema_differ, is_dev=True)
        .build()
        .environment.promoted_snapshot_ids
        or []
    ) == {
        updated_snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
        snapshot_c.snapshot_id,
    }


def test_forward_only_models(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            dialect="duckdb",
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 3, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
            dialect="duckdb",
        )
    )
    updated_snapshot.previous_versions = snapshot.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    PlanBuilder(context_diff, schema_differ, is_dev=True).build()
    assert updated_snapshot.change_category == SnapshotChangeCategory.FORWARD_ONLY

    updated_snapshot.change_category = None
    updated_snapshot.version = None
    PlanBuilder(context_diff, schema_differ, is_dev=True, forward_only=True).build()
    assert updated_snapshot.change_category == SnapshotChangeCategory.FORWARD_ONLY

    updated_snapshot.change_category = None
    updated_snapshot.version = None
    PlanBuilder(context_diff, schema_differ, forward_only=True).build()
    assert updated_snapshot.change_category == SnapshotChangeCategory.FORWARD_ONLY


def test_forward_only_models_model_kind_changed(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds"), kind=FullKind()))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 3, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        )
    )
    updated_snapshot.previous_versions = snapshot.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, is_dev=True).build()
    assert updated_snapshot.change_category == SnapshotChangeCategory.BREAKING


def test_indirectly_modified_forward_only_model(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1 as a, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 2 as a, ds")))
    updated_snapshot_a.previous_versions = snapshot_a.all_versions

    snapshot_b = make_snapshot(
        SqlModel(
            dialect="duckdb",
            name="b",
            query=parse_one("select a, ds from a"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        ),
        nodes={'"a"': snapshot_a.model},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    updated_snapshot_b = make_snapshot(snapshot_b.model, nodes={'"a"': updated_snapshot_a.model})
    updated_snapshot_b.previous_versions = snapshot_b.all_versions

    snapshot_c = make_snapshot(
        SqlModel(name="c", query=parse_one("select a, ds from b")), nodes={'"b"': snapshot_b.model}
    )
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot_c = make_snapshot(
        snapshot_c.model, nodes={'"b"': updated_snapshot_b.model, '"a"': updated_snapshot_a.model}
    )
    updated_snapshot_c.previous_versions = snapshot_c.all_versions

    snapshot_d = make_snapshot(
        SqlModel(name="d", query=parse_one("select a.a from a, b")),
        nodes={
            '"a"': snapshot_a.model,
            '"b"': snapshot_b.model,
        },
    )
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot_d = make_snapshot(
        snapshot_d.model, nodes={'"b"': updated_snapshot_b.model, '"a"': updated_snapshot_a.model}
    )
    updated_snapshot_d.previous_versions = snapshot_d.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            updated_snapshot_a.name: (updated_snapshot_a, snapshot_a),
            updated_snapshot_b.name: (updated_snapshot_b, snapshot_b),
            updated_snapshot_c.name: (updated_snapshot_c, snapshot_c),
            updated_snapshot_d.name: (updated_snapshot_d, snapshot_d),
        },
        snapshots={
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
            updated_snapshot_b.snapshot_id: updated_snapshot_b,
            updated_snapshot_c.snapshot_id: updated_snapshot_c,
            updated_snapshot_d.snapshot_id: updated_snapshot_d,
        },
        new_snapshots={
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
            updated_snapshot_b.snapshot_id: updated_snapshot_b,
            updated_snapshot_c.snapshot_id: updated_snapshot_c,
            updated_snapshot_d.snapshot_id: updated_snapshot_d,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, is_dev=True).build()
    assert plan.indirectly_modified == {
        updated_snapshot_a.snapshot_id: {
            updated_snapshot_b.snapshot_id,
            updated_snapshot_c.snapshot_id,
            updated_snapshot_d.snapshot_id,
        }
    }

    assert plan.directly_modified == {updated_snapshot_a.snapshot_id}

    assert updated_snapshot_a.change_category == SnapshotChangeCategory.BREAKING
    assert updated_snapshot_b.change_category == SnapshotChangeCategory.FORWARD_ONLY
    assert updated_snapshot_c.change_category == SnapshotChangeCategory.FORWARD_ONLY
    assert updated_snapshot_d.change_category == SnapshotChangeCategory.INDIRECT_BREAKING

    deployability_index = DeployabilityIndex.create(
        {
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
            updated_snapshot_b.snapshot_id: updated_snapshot_b,
            updated_snapshot_c.snapshot_id: updated_snapshot_c,
        }
    )
    assert deployability_index.is_representative(updated_snapshot_a)
    assert not deployability_index.is_representative(updated_snapshot_b)
    assert not deployability_index.is_representative(updated_snapshot_c)


def test_added_model_with_forward_only_parent(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1 as a, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select a, ds from a")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={snapshot_b.snapshot_id: snapshot_b},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, is_dev=True).build()
    assert snapshot_b.change_category == SnapshotChangeCategory.BREAKING


def test_added_forward_only_model(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1 as a, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        )
    )

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select a, ds from a")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_a.snapshot_id, snapshot_b.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER).build()
    assert snapshot_a.change_category == SnapshotChangeCategory.BREAKING
    assert snapshot_b.change_category == SnapshotChangeCategory.BREAKING


def test_disable_restatement(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", disable_restatement=True),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot.snapshot_id: snapshot},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    plan = PlanBuilder(context_diff, schema_differ, restate_models=['"a"']).build()
    assert not plan.restatements

    # Effective from doesn't apply to snapshots for which restatements are disabled.
    plan = PlanBuilder(
        context_diff, schema_differ, forward_only=True, effective_from="2023-01-01"
    ).build()
    assert plan.effective_from == "2023-01-01"
    assert snapshot.effective_from is None

    # Restatements should still be supported when in dev.
    plan = PlanBuilder(context_diff, schema_differ, is_dev=True, restate_models=['"a"']).build()
    assert plan.restatements == {
        snapshot.snapshot_id: (to_timestamp(plan.start), to_timestamp(to_date("tomorrow")))
    }

    # We don't want to restate a disable_restatement model if it is unpaused since that would be mean we are violating
    # the model kind property
    snapshot.unpaused_ts = 9999999999
    plan = PlanBuilder(context_diff, schema_differ, is_dev=True, restate_models=['"a"']).build()
    assert plan.restatements == {}


def test_revert_to_previous_value(make_snapshot, mocker: MockerFixture):
    """
    Make sure we can revert to previous snapshots with intervals if it already exists and not modify
    it's existing change category
    """
    old_snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), depends_on=set())
    )
    old_snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select 1, ds FROM a"), depends_on={"a"})
    )
    snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 2, ds"), depends_on=set())
    )
    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select 1, ds FROM a"), depends_on={"a"})
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_b.add_interval("2022-01-01", now())

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={
            snapshot_a.name: (snapshot_a, old_snapshot_a),
            snapshot_b.name: (snapshot_b, old_snapshot_b),
        },
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={snapshot_a.snapshot_id: snapshot_a},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan_builder = PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER)
    plan_builder.set_choice(snapshot_a, SnapshotChangeCategory.BREAKING)
    plan_builder.build()
    # Make sure it does not get assigned INDIRECT_BREAKING
    assert snapshot_b.change_category == SnapshotChangeCategory.FORWARD_ONLY


test_add_restatement_fixtures = [
    (
        "No dependencies single depends on self",
        {
            '"a"': {},
            '"b"': {},
        },
        {'"b"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"a"': ("1 week ago", "6 days ago"),
            '"b"': ("1 week ago", "today"),
        },
    ),
    (
        "Simple dependency with leaf depends on self",
        {
            '"a"': {},
            '"b"': {'"a"'},
        },
        {'"b"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"a"': ("1 week ago", "6 days ago"),
            '"b"': ("1 week ago", "today"),
        },
    ),
    (
        "Simple dependency with root depends on self",
        {
            '"a"': {},
            '"b"': {'"a"'},
        },
        {'"a"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"a"': ("1 week ago", "today"),
            '"b"': ("1 week ago", "today"),
        },
    ),
    (
        "Two unrelated subgraphs with root depends on self",
        {
            '"a"': {},
            '"b"': {},
            '"c"': {'"a"'},
            '"d"': {'"b"'},
        },
        {'"a"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"a"': ("1 week ago", "today"),
            '"b"': ("1 week ago", "6 days ago"),
            '"c"': ("1 week ago", "today"),
            '"d"': ("1 week ago", "6 days ago"),
        },
    ),
    (
        "Simple root depends on self with adjusted execution time",
        {
            '"a"': {},
            '"b"': {'"a"'},
        },
        {'"a"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "3 day ago",
        {
            '"a"': ("1 week ago", "2 days ago"),
            '"b"': ("1 week ago", "2 days ago"),
        },
    ),
    (
        """
        a -> c -> d
        b -> c -> e -> g
        b -> f -> g
        c depends on self
        restate a and b
        """,
        {
            '"a"': {},
            '"b"': {},
            '"c"': {'"a"', '"b"'},
            '"d"': {'"c"'},
            '"e"': {'"c"'},
            '"f"': {'"b"'},
            '"g"': {'"f"', '"e"'},
        },
        {'"c"'},
        {'"a"', '"b"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"a"': ("1 week ago", "6 days ago"),
            '"b"': ("1 week ago", "6 days ago"),
            '"c"': ("1 week ago", "today"),
            '"d"': ("1 week ago", "today"),
            '"e"': ("1 week ago", "today"),
            '"f"': ("1 week ago", "6 days ago"),
            '"g"': ("1 week ago", "today"),
        },
    ),
    (
        """
        a -> c -> d
        b -> c -> e -> g
        b -> f -> g
        c depends on self
        restate e
        """,
        {
            '"a"': {},
            '"b"': {},
            '"c"': {'"a"', '"b"'},
            '"d"': {'"c"'},
            '"e"': {'"c"'},
            '"f"': {'"b"'},
            '"g"': {'"f"', '"e"'},
        },
        {'"c"'},
        {'"e"'},
        "1 week ago",
        "1 week ago",
        "1 day ago",
        {
            '"e"': ("1 week ago", "6 days ago"),
            '"g"': ("1 week ago", "6 days ago"),
        },
    ),
]


@pytest.mark.parametrize(
    "graph,depends_on_self_names,restatement_names,start,end,execution_time,expected",
    [test[1:] for test in test_add_restatement_fixtures],
    ids=[test[0] for test in test_add_restatement_fixtures],
)
def test_add_restatements(
    graph: t.Dict[str, t.Set[str]],
    depends_on_self_names: t.Set[str],
    restatement_names: t.Set[str],
    start: str,
    end: str,
    execution_time: str,
    expected: t.Dict[str, t.Tuple[str, str]],
    make_snapshot,
    mocker,
):
    dag = DAG(graph)
    snapshots: t.Dict[str, Snapshot] = {}
    for snapshot_name in dag:
        depends_on = dag.upstream(snapshot_name)
        snapshot = make_snapshot(
            SqlModel(
                name=snapshot_name,
                kind=IncrementalByTimeRangeKind(time_column="ds"),
                cron="@daily",
                start="1 week ago",
                query=parse_one(
                    f"SELECT 1 FROM {snapshot_name}"
                    if snapshot_name in depends_on_self_names
                    else "SELECT 1"
                ),
                depends_on=depends_on,
            ),
            nodes={
                upstream_snapshot_name: snapshots[upstream_snapshot_name].model
                for upstream_snapshot_name in depends_on
            },
        )
        snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
        snapshots[snapshot_name] = snapshot

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot.snapshot_id: snapshot for snapshot in snapshots.values()},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        start=to_date(start),
        end=to_date(end),
        execution_time=to_date(execution_time),
        restate_models=restatement_names,
    ).build()

    assert {s_id.name: interval for s_id, interval in plan.restatements.items()} == {
        name: (to_timestamp(to_date(start)), to_timestamp(to_date(end)))
        for name, (start, end) in expected.items()
    }


def test_dev_plan_depends_past(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            # self reference query so it depends_on_self
            query=parse_one("select 1, ds FROM a"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_child = make_snapshot(
        SqlModel(
            name="a_child",
            query=parse_one("select 1, ds FROM a"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
        nodes={'"a"': snapshot.model},
    )
    snapshot_child.categorize_as(SnapshotChangeCategory.BREAKING)
    unrelated_snapshot = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select 1, ds"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
    )
    unrelated_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert snapshot.depends_on_self
    assert not snapshot_child.depends_on_self
    assert not unrelated_snapshot.depends_on_self
    assert snapshot_child.model.depends_on == {'"a"'}
    assert snapshot_child.parents == (snapshot.snapshot_id,)
    assert unrelated_snapshot.model.depends_on == set()

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot.snapshot_id, snapshot_child.snapshot_id, unrelated_snapshot.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot.snapshot_id: snapshot,
            snapshot_child.snapshot_id: snapshot_child,
            unrelated_snapshot.snapshot_id: unrelated_snapshot,
        },
        new_snapshots={
            snapshot.snapshot_id: snapshot,
            snapshot_child.snapshot_id: snapshot_child,
            unrelated_snapshot.snapshot_id: unrelated_snapshot,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    dev_plan_start_aligned = PlanBuilder(
        context_diff, schema_differ, start="2023-01-01", end="2023-01-10", is_dev=True
    ).build()
    assert len(dev_plan_start_aligned.new_snapshots) == 3
    assert sorted([x.name for x in dev_plan_start_aligned.new_snapshots]) == [
        '"a"',
        '"a_child"',
        '"b"',
    ]
    assert dev_plan_start_aligned.directly_modified == {
        snapshot.snapshot_id,
        snapshot_child.snapshot_id,
        unrelated_snapshot.snapshot_id,
    }
    assert dev_plan_start_aligned.indirectly_modified == {}

    dev_plan_start_ahead_of_model = PlanBuilder(
        context_diff, schema_differ, start="2023-01-02", end="2023-01-10", is_dev=True
    ).build()
    assert len(dev_plan_start_ahead_of_model.new_snapshots) == 3
    assert not dev_plan_start_ahead_of_model.deployability_index.is_deployable(snapshot)
    assert not dev_plan_start_ahead_of_model.deployability_index.is_deployable(snapshot_child)
    assert dev_plan_start_ahead_of_model.deployability_index.is_deployable(unrelated_snapshot)
    assert dev_plan_start_ahead_of_model.directly_modified == {
        snapshot.snapshot_id,
        snapshot_child.snapshot_id,
        unrelated_snapshot.snapshot_id,
    }
    assert dev_plan_start_ahead_of_model.indirectly_modified == {}


def test_dev_plan_depends_past_non_deployable(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            # self reference query so it depends_on_self
            query=parse_one("select 1, ds FROM a"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    updated_snapshot = make_snapshot(
        SqlModel(
            **{
                **snapshot.model.dict(),
                "query": parse_one("select 1, ds, 2 FROM a"),
            }
        ),
    )

    snapshot_child = make_snapshot(
        SqlModel(
            name="a_child",
            query=parse_one("select 1, ds FROM a"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
        nodes={'"a"': updated_snapshot.model},
    )
    unrelated_snapshot = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select 1, ds"),
            start="2023-01-01",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
    )

    assert updated_snapshot.depends_on_self
    assert not snapshot_child.depends_on_self
    assert not unrelated_snapshot.depends_on_self
    assert snapshot_child.model.depends_on == {'"a"'}
    assert snapshot_child.parents == (updated_snapshot.snapshot_id,)
    assert unrelated_snapshot.model.depends_on == set()

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_child.snapshot_id, unrelated_snapshot.snapshot_id},
        removed_snapshots={},
        modified_snapshots={snapshot.name: (updated_snapshot, snapshot)},
        snapshots={
            updated_snapshot.snapshot_id: updated_snapshot,
            snapshot_child.snapshot_id: snapshot_child,
            unrelated_snapshot.snapshot_id: unrelated_snapshot,
        },
        new_snapshots={
            updated_snapshot.snapshot_id: snapshot,
            snapshot_child.snapshot_id: snapshot_child,
            unrelated_snapshot.snapshot_id: unrelated_snapshot,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER

    def new_builder(start, end):
        builder = PlanBuilder(context_diff, schema_differ, start=start, end=end, is_dev=True)
        builder.set_choice(updated_snapshot, SnapshotChangeCategory.FORWARD_ONLY)
        builder.set_choice(snapshot_child, SnapshotChangeCategory.BREAKING)
        builder.set_choice(unrelated_snapshot, SnapshotChangeCategory.BREAKING)
        return builder

    dev_plan_start_aligned = new_builder("2023-01-01", "2023-01-10").build()
    assert sorted([x.name for x in dev_plan_start_aligned.new_snapshots]) == [
        '"a"',
        '"a_child"',
        '"b"',
    ]


def test_restatement_intervals_after_updating_start(sushi_context: Context):
    plan = sushi_context.plan(no_prompts=True, restate_models=["sushi.waiter_revenue_by_day"])
    snapshot_id = [
        snapshot.snapshot_id
        for snapshot in plan.snapshots
        if snapshot.name == '"memory"."sushi"."waiter_revenue_by_day"'
    ][0]
    restatement_interval = plan.restatements[snapshot_id]
    assert restatement_interval[0] == to_timestamp(plan.start)

    new_start = yesterday_ds()
    plan = sushi_context.plan(
        no_prompts=True, restate_models=["sushi.waiter_revenue_by_day"], start=new_start
    )
    new_restatement_interval = plan.restatements[snapshot_id]
    assert new_restatement_interval[0] == to_timestamp(new_start)
    assert new_restatement_interval != restatement_interval


def test_models_selected_for_backfill(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1 as one, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select one, ds from a")),
        nodes={'"a"': snapshot_a.model},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        new_snapshots={snapshot_b.snapshot_id: snapshot_b},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER

    plan = PlanBuilder(context_diff, schema_differ).build()
    assert plan.is_selected_for_backfill('"a"')
    assert plan.is_selected_for_backfill('"b"')
    assert plan.models_to_backfill is None
    assert {i.snapshot_id for i in plan.missing_intervals} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    plan = PlanBuilder(context_diff, schema_differ, is_dev=True, backfill_models={'"a"'}).build()
    assert plan.is_selected_for_backfill('"a"')
    assert not plan.is_selected_for_backfill('"b"')
    assert plan.models_to_backfill == {'"a"'}
    assert {i.snapshot_id for i in plan.missing_intervals} == {snapshot_a.snapshot_id}
    assert plan.environment.promoted_snapshot_ids == [snapshot_a.snapshot_id]

    plan = PlanBuilder(context_diff, schema_differ, is_dev=True, backfill_models={'"b"'}).build()
    assert plan.is_selected_for_backfill('"a"')
    assert plan.is_selected_for_backfill('"b"')
    assert plan.models_to_backfill == {'"a"', '"b"'}
    assert {i.snapshot_id for i in plan.missing_intervals} == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    assert plan.environment.promoted_snapshot_ids == [snapshot_b.snapshot_id]


def test_categorized_uncategorized(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    new_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={new_snapshot.name: (new_snapshot, snapshot)},
        snapshots={new_snapshot.snapshot_id: new_snapshot},
        new_snapshots={new_snapshot.snapshot_id: new_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan_builder = PlanBuilder(
        context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, auto_categorization_enabled=False
    )

    plan = plan_builder.build()
    assert plan.uncategorized == [new_snapshot]
    assert not plan.categorized

    plan_builder.set_choice(new_snapshot, SnapshotChangeCategory.NON_BREAKING)

    plan = plan_builder.build()
    assert not plan.uncategorized
    assert plan.categorized == [new_snapshot]


def test_environment_previous_finalized_snapshots(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1 as one, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    updated_snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 4 as four, ds")))
    updated_snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2 as two, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_c = make_snapshot(SqlModel(name="c", query=parse_one("select 3 as three, ds")))
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_d = make_snapshot(SqlModel(name="d", query=parse_one("select 5 as five, ds")))
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=True,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={snapshot_c.snapshot_id: snapshot_c.table_info},
        modified_snapshots={snapshot_a.name: (updated_snapshot_a, snapshot_a)},
        snapshots={
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
            snapshot_d.snapshot_id: snapshot_d,
        },
        new_snapshots={
            snapshot_b.snapshot_id: snapshot_b,
            updated_snapshot_a.snapshot_id: updated_snapshot_a,
        },
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=[snapshot_c.table_info, snapshot_d.table_info],
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    plan = PlanBuilder(context_diff, schema_differ).build()
    assert set(plan.environment.previous_finalized_snapshots or []) == {
        snapshot_c.table_info,
        snapshot_d.table_info,
    }

    context_diff.is_unfinalized_environment = False

    plan = PlanBuilder(context_diff, schema_differ).build()
    assert set(plan.environment.previous_finalized_snapshots or []) == {
        snapshot_a.table_info,
        snapshot_c.table_info,
        snapshot_d.table_info,
    }


def test_metadata_change(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    # Simulate a direct change.
    updated_snapshot = make_snapshot(
        SqlModel(
            **{
                **snapshot.model.dict(),
                "owner": "new_owner",
            }
        )
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan = PlanBuilder(context_diff, DuckDBEngineAdapter.SCHEMA_DIFFER, is_dev=True).build()

    assert (
        plan.snapshots[updated_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.METADATA
    )


def test_plan_start_when_preview_enabled(make_snapshot, mocker: MockerFixture):
    model_start = "2024-01-01"
    model = SqlModel(
        name="a",
        query=parse_one("select 1, ds"),
        dialect="duckdb",
        kind=dict(
            name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds", forward_only=True
        ),
        start=model_start,
    )

    snapshot = make_snapshot(model)
    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot.snapshot_id},
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot.snapshot_id: snapshot,
        },
        new_snapshots={snapshot.snapshot_id: snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    default_start_for_preview = "2024-06-09"

    plan_builder = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        default_start=default_start_for_preview,
        is_dev=True,
        enable_preview=True,
    )
    assert plan_builder.build().start == default_start_for_preview

    # When a model is modified then the backfill should be a preview.
    snapshot = make_snapshot(model)
    context_diff.added = set()
    context_diff.modified_snapshots = {snapshot.name: (snapshot, snapshot)}

    plan_builder = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        default_start=default_start_for_preview,
        is_dev=True,
        enable_preview=True,
    )
    assert plan_builder.build().start == default_start_for_preview


def test_interval_end_per_model(make_snapshot):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    new_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={new_snapshot.name: (new_snapshot, snapshot)},
        snapshots={new_snapshot.snapshot_id: new_snapshot},
        new_snapshots={new_snapshot.snapshot_id: new_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan_builder = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        interval_end_per_model={snapshot.name: to_timestamp("2023-01-09")},
    )
    assert plan_builder.build().interval_end_per_model == {
        snapshot.name: to_timestamp("2023-01-09")
    }

    # User-provided end should take precedence.
    plan_builder = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        interval_end_per_model={snapshot.name: to_timestamp("2023-01-09")},
        end="2023-01-10",
        is_dev=True,
    )
    assert plan_builder.build().interval_end_per_model is None


def test_unaligned_start_model_with_forward_only_preview(make_snapshot):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=dict(
                name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, forward_only=True, time_column="ds"
            ),
        )
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    new_snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 2, ds"),
            kind=dict(
                name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, forward_only=True, time_column="ds"
            ),
        )
    )
    new_snapshot_a.previous_versions = snapshot_a.all_versions
    new_snapshot_a.unpaused_ts = 1

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select 1 AS key"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, unique_key="key"),
            start="2024-01-01",
            depends_on={"a"},
        ),
        nodes={new_snapshot_a.name: new_snapshot_a.model},
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added={snapshot_b.snapshot_id},
        removed_snapshots={},
        snapshots={new_snapshot_a.snapshot_id: new_snapshot_a, snapshot_b.snapshot_id: snapshot_b},
        new_snapshots={
            new_snapshot_a.snapshot_id: new_snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
        },
        modified_snapshots={snapshot_a.name: (new_snapshot_a, snapshot_a)},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    plan_builder = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        enable_preview=True,
        is_dev=True,
    )
    plan = plan_builder.build()

    assert set(plan.restatements) == {new_snapshot_a.snapshot_id, snapshot_b.snapshot_id}
    assert not plan.deployability_index.is_deployable(new_snapshot_a)
    assert not plan.deployability_index.is_deployable(snapshot_b)


def test_restate_production_model_in_dev(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model_a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    prod_snapshot = make_snapshot(
        SqlModel(
            name="test_model_b",
            dialect="duckdb",
            query=parse_one("select 2, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )
    prod_snapshot.unpaused_ts = 1

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=True,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot.snapshot_id: snapshot, prod_snapshot.snapshot_id: prod_snapshot},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    mock_console = mocker.Mock()

    plan = PlanBuilder(
        context_diff,
        DuckDBEngineAdapter.SCHEMA_DIFFER,
        is_dev=True,
        restate_models={snapshot.name, prod_snapshot.name},
        console=mock_console,
    ).build()

    assert len(plan.restatements) == 1
    assert prod_snapshot.snapshot_id not in plan.restatements

    mock_console.log_warning.assert_called_once_with(
        "Cannot restate model '\"test_model_b\"' because the current version is used in production. "
        "Run the restatement against the production environment instead to restate this model."
    )


@time_machine.travel("2025-02-23 15:00:00 UTC")
def test_restate_daily_to_monthly(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1 as one"),
            cron="@daily",
            start="2025-01-01",
        ),
    )

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select one from a"),
            cron="@monthly",
            start="2025-01-01",
        ),
        nodes={'"a"': snapshot_a.model},
    )

    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            query=parse_one("select one from b"),
            cron="@daily",
            start="2025-01-01",
        ),
        nodes={
            '"a"': snapshot_a.model,
            '"b"': snapshot_b.model,
        },
    )

    snapshot_d = make_snapshot(
        SqlModel(
            name="d",
            query=parse_one("select one from b union all select one from a"),
            cron="@daily",
            start="2025-01-01",
        ),
        nodes={
            '"a"': snapshot_a.model,
            '"b"': snapshot_b.model,
        },
    )
    snapshot_e = make_snapshot(
        SqlModel(
            name="e",
            query=parse_one("select one from b"),
            cron="@daily",
            start="2025-01-01",
        ),
        nodes={
            '"a"': snapshot_a.model,
            '"b"': snapshot_b.model,
        },
    )

    context_diff = ContextDiff(
        environment="prod",
        is_new_environment=False,
        is_unfinalized_environment=True,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={
            snapshot_a.snapshot_id: snapshot_a,
            snapshot_b.snapshot_id: snapshot_b,
            snapshot_c.snapshot_id: snapshot_c,
            snapshot_d.snapshot_id: snapshot_d,
            snapshot_e.snapshot_id: snapshot_e,
        },
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER

    plan = PlanBuilder(
        context_diff,
        schema_differ,
        restate_models=[snapshot_a.name, snapshot_e.name],
        start="2025-02-15",
        end="2025-02-20",
    ).build()

    assert plan.restatements == {
        snapshot_a.snapshot_id: (1739577600000, 1740355200000),
        snapshot_b.snapshot_id: (1738368000000, 1740787200000),
        snapshot_c.snapshot_id: (1739577600000, 1740355200000),
        snapshot_d.snapshot_id: (1739577600000, 1740355200000),
        snapshot_e.snapshot_id: (1739577600000, 1740355200000),
    }


def test_plan_environment_statements_diff(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model_a",
            dialect="duckdb",
            query=parse_one("select 1, ds"),
            kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ds"),
        )
    )

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=False,
        is_unfinalized_environment=True,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={},
        snapshots={snapshot.snapshot_id: snapshot},
        new_snapshots={},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        environment_statements=[
            EnvironmentStatements(
                before_all=["CREATE OR REPLACE TABLE table_1 AS SELECT 1", "@test_macro()"],
                after_all=["CREATE OR REPLACE TABLE table_2 AS SELECT 2"],
                python_env={
                    "test_macro": Executable(
                        payload="def test_macro(evaluator):\n    return 'one'"
                    ),
                },
            )
        ],
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    assert context_diff.has_changes
    assert context_diff.has_environment_statements_changes

    console_output, terminal_console = create_test_console()
    for _, diff in context_diff.environment_statements_diff():
        terminal_console._print(diff)
    output = console_output.getvalue()
    stripped = strip_ansi_codes(output)

    expected_output = (
        "before_all:\n"
        "  + CREATE OR REPLACE TABLE table_1 AS SELECT 1\n"
        "  + @test_macro()\n\n"
        "after_all:\n"
        "  + CREATE OR REPLACE TABLE table_2 AS SELECT 2"
    )
    assert stripped == expected_output
    console_output.close()

    # Validate with python env included
    console_output, terminal_console = create_test_console()
    for _, diff in context_diff.environment_statements_diff(include_python_env=True):
        terminal_console._print(diff)
    output = console_output.getvalue()
    stripped = strip_ansi_codes(output)
    expected_output = (
        "before_all:\n"
        "  + CREATE OR REPLACE TABLE table_1 AS SELECT 1\n"
        "  + @test_macro()\n\n"
        "after_all:\n"
        "  + CREATE OR REPLACE TABLE table_2 AS SELECT 2\n\n"
        "dependencies:\n"
        "@@ -0,0 +1,2 @@\n\n"
        "+def test_macro(evaluator):\n"
        "+    return 'one'"
    )
    assert stripped == expected_output
    console_output.close()


def test_set_choice_for_forward_only_model(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            dialect="duckdb",
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 3, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
            dialect="duckdb",
        )
    )
    updated_snapshot.previous_versions = snapshot.all_versions

    context_diff = ContextDiff(
        environment="test_environment",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        create_from="prod",
        create_from_env_exists=True,
        added=set(),
        removed_snapshots={},
        modified_snapshots={updated_snapshot.name: (updated_snapshot, snapshot)},
        snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        new_snapshots={updated_snapshot.snapshot_id: updated_snapshot},
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
    )

    schema_differ = DuckDBEngineAdapter.SCHEMA_DIFFER
    plan_builder = PlanBuilder(context_diff, schema_differ, is_dev=True)

    with pytest.raises(PlanError, match='Forward-only model "a" cannot be categorized manually.'):
        plan_builder.set_choice(updated_snapshot, SnapshotChangeCategory.BREAKING)

    plan = plan_builder.build()
    assert (
        plan.snapshots[updated_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
