from datetime import timedelta

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.model import IncrementalByTimeRangeKind, SeedKind, SeedModel, SqlModel
from sqlmesh.core.model.seed import Seed
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot import (
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotFingerprint,
)
from sqlmesh.utils.date import now, to_date, to_ds, to_timestamp
from sqlmesh.utils.errors import PlanError


def test_forward_only_plan_sets_version(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            change_category=SnapshotChangeCategory.FORWARD_ONLY,
        ),
    )
    assert not snapshot_b.version

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"b": (snapshot_b, snapshot_b)}
    context_diff_mock.new_snapshots = {snapshot_b.snapshot_id: snapshot_b}
    context_diff_mock.added_materialized_models = set()

    plan = Plan(context_diff_mock, forward_only=True)

    assert snapshot_b.version == "test_version"

    # Make sure that the choice can't be set manually.
    with pytest.raises(PlanError, match="Choice setting is not supported by a forward-only plan."):
        plan.set_choice(snapshot_b, SnapshotChangeCategory.BREAKING)


def test_forward_only_dev(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    expected_start = to_date("2022-01-01")
    expected_end = to_date("2022-01-02")

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}
    context_diff_mock.added_materialized_models = set()

    yesterday_ds_mock = mocker.patch("sqlmesh.core.plan.definition.yesterday_ds")
    yesterday_ds_mock.return_value = expected_start

    now_ds_mock = mocker.patch("sqlmesh.core.plan.definition.now")
    now_ds_mock.return_value = to_date("2022-01-03")

    plan = Plan(context_diff_mock, forward_only=True, is_dev=True)

    assert plan.restatements == {"a"}
    assert plan.start == expected_start
    assert plan.end == expected_end

    yesterday_ds_mock.assert_called_once()
    now_ds_mock.call_count == 2


def test_forward_only_plan_new_models_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {"a"}
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}
    context_diff_mock.added_materialized_models = {"a"}

    with pytest.raises(
        PlanError,
        match="New models that require materialization can't be added as part of the forward-only plan.",
    ):
        Plan(context_diff_mock, forward_only=True)

    context_diff_mock.added_materialized_models = set()
    Plan(context_diff_mock, forward_only=True)


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
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds from a")))
    assert not snapshot_b.version

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"b": (snapshot_b, snapshot_b)}
    context_diff_mock.new_snapshots = {snapshot_b.snapshot_id: snapshot_b}
    context_diff_mock.added_materialized_models = set()

    with pytest.raises(
        PlanError,
        match=r"Model 'b' depends on a paused version of model 'a'.*",
    ):
        Plan(context_diff_mock, forward_only=False)


def test_restate_models(sushi_context_pre_scheduling: Context):
    plan = sushi_context_pre_scheduling.plan(
        restate_models=["sushi.waiter_revenue_by_day"], no_prompts=True
    )
    assert plan.restatements == {"sushi.waiter_revenue_by_day"}
    assert plan.requires_backfill

    with pytest.raises(PlanError, match=r"Cannot restate from 'unknown_model'.*"):
        sushi_context_pre_scheduling.plan(restate_models=["unknown_model"], no_prompts=True)


def test_restate_model_with_merge_strategy(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, key"),
            kind="VIEW",
        )
    )

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}
    context_diff_mock.added_materialized_models = set()

    with pytest.raises(
        PlanError,
        match="Cannot restate from 'a'. Either such model doesn't exist, no other materialized model references it.*",
    ):
        Plan(context_diff_mock, restate_models=["a"])


def test_new_snapshots_with_restatements(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    with pytest.raises(
        PlanError,
        match=r"Model changes and restatements can't be a part of the same plan.*",
    ):
        Plan(context_diff_mock, restate_models=["a"])


def test_end_validation(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    dev_plan = Plan(context_diff_mock, end="2022-01-03", is_dev=True)
    assert dev_plan.end == "2022-01-03"
    dev_plan.end = "2022-01-04"
    assert dev_plan.end == "2022-01-04"

    start_end_not_allowed_message = (
        "The start and end dates can't be set for a production plan without restatements."
    )

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        Plan(context_diff_mock, end="2022-01-03")

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        Plan(context_diff_mock, start="2022-01-03")

    prod_plan = Plan(context_diff_mock)

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        prod_plan.end = "2022-01-03"

    with pytest.raises(PlanError, match=start_end_not_allowed_message):
        prod_plan.start = "2022-01-03"

    context_diff_mock.new_snapshots = {}
    restatement_prod_plan = Plan(
        context_diff_mock,
        end="2022-01-03",
        restate_models=["a"],
    )
    assert restatement_prod_plan.end == "2022-01-03"
    restatement_prod_plan.end = "2022-01-04"
    assert restatement_prod_plan.end == "2022-01-04"


def test_forward_only_revert_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert not snapshot.is_forward_only

    forward_only_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))
    forward_only_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    forward_only_snapshot.version = snapshot.version
    assert forward_only_snapshot.is_forward_only

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"a": (snapshot, forward_only_snapshot)}
    context_diff_mock.new_snapshots = {}
    context_diff_mock.added_materialized_models = set()

    with pytest.raises(
        PlanError,
        match=r"Detected an existing version of model 'a' that has been previously superseded by a forward-only change.*",
    ):
        Plan(context_diff_mock, forward_only=True)

    # Make sure the plan can be created if a new snapshot version was enforced.
    new_version_snapshot = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), stamp="test_stamp")
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    context_diff_mock.modified_snapshots = {"a": (new_version_snapshot, forward_only_snapshot)}
    context_diff_mock.new_snapshots = {new_version_snapshot.snapshot_id: new_version_snapshot}
    Plan(context_diff_mock, forward_only=True)


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

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a_updated}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"a": (snapshot_a_updated, snapshot_a)}
    context_diff_mock.new_snapshots = {snapshot_a_updated.snapshot_id: snapshot_a_updated}
    context_diff_mock.added_materialized_models = set()

    Plan(context_diff_mock, forward_only=True)
    assert snapshot_a_updated.version == snapshot_a_updated.fingerprint.to_version()
    assert snapshot_a_updated.change_category == SnapshotChangeCategory.NON_BREAKING


def test_start_inference(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), start="2022-01-01")
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_b.snapshot_id: snapshot_b}

    snapshot_b.add_interval("2022-01-01", now())

    plan = Plan(context_diff_mock)
    assert len(plan.missing_intervals) == 1
    assert plan.missing_intervals[0].snapshot_name == snapshot_a.name
    assert plan.start == to_timestamp("2022-01-01")


def test_auto_categorization(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    updated_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": updated_snapshot}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"a": (updated_snapshot, snapshot)}
    context_diff_mock.new_snapshots = {updated_snapshot.snapshot_id: updated_snapshot}

    Plan(context_diff_mock)

    assert updated_snapshot.version == updated_snapshot.fingerprint.to_version()
    assert updated_snapshot.change_category == SnapshotChangeCategory.BREAKING


def test_auto_categorization_missing_schema_downstream(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))

    downstream_snapshot = make_snapshot(
        SqlModel(name="b", query=parse_one("select * from tbl"), depends_on={"a"}),
        nodes={"a": snapshot.model},
    )
    downstream_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_downstream_snapshot = make_snapshot(
        downstream_snapshot.model,
        nodes={"a": updated_snapshot.model},
    )

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": updated_snapshot, "b": downstream_snapshot}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {
        "a": (updated_snapshot, snapshot),
        "b": (updated_downstream_snapshot, downstream_snapshot),
    }
    context_diff_mock.new_snapshots = {updated_snapshot.snapshot_id: updated_snapshot}
    context_diff_mock.directly_modified.side_effect = lambda name: name == "a"

    Plan(context_diff_mock)

    assert updated_snapshot.version is None
    assert updated_snapshot.change_category is None


def test_end_from_missing_instead_of_now(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        )
    )

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    start_mock = mocker.patch("sqlmesh.core.snapshot.definition.earliest_start_date")
    start_mock.return_value = to_ds("2022-01-01")
    now_mock = mocker.patch("sqlmesh.core.plan.definition.now")
    now_mock.return_value = to_ds("2022-01-30")
    snapshot_a.add_interval("2022-01-01", "2022-01-05")

    plan = Plan(context_diff_mock, is_dev=True)
    assert plan.start == to_timestamp("2022-01-06")
    assert plan.end == to_date("2022-01-29")


def test_broken_references(make_snapshot, mocker: MockerFixture):
    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds FROM a")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = {"a"}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}

    with pytest.raises(
        PlanError,
        match=r"Removed models {'a'} are referenced in model 'b'.*",
    ):
        Plan(context_diff_mock)


def test_effective_from(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds FROM a")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.add_interval("2023-01-01", "2023-03-01")

    updated_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds FROM a")))

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": updated_snapshot}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"a": (updated_snapshot, snapshot)}
    context_diff_mock.new_snapshots = {updated_snapshot.snapshot_id: updated_snapshot}
    context_diff_mock.added_materialized_models = set()

    with pytest.raises(
        PlanError,
        match="Effective date can only be set for a forward-only plan.",
    ):
        plan = Plan(context_diff_mock)
        plan.effective_from = "2023-02-01"

    plan = Plan(
        context_diff_mock, forward_only=True, start="2023-01-01", end="2023-03-01", is_dev=True
    )
    updated_snapshot.add_interval("2023-01-01", "2023-03-01")

    with pytest.raises(
        PlanError,
        match="Effective date cannot be in the future.",
    ):
        plan.effective_from = now() + timedelta(days=1)

    assert plan.effective_from is None
    assert updated_snapshot.effective_from is None
    assert not plan.missing_intervals

    plan.effective_from = "2023-02-01"
    assert plan.effective_from == "2023-02-01"
    assert updated_snapshot.effective_from == "2023-02-01"

    assert len(plan.missing_intervals) == 1
    missing_intervals = plan.missing_intervals[0]
    assert missing_intervals.intervals[0][0] == to_timestamp("2023-02-01")
    assert missing_intervals.intervals[-1][-1] == to_timestamp("2023-03-02")

    plan.effective_from = None
    assert plan.effective_from is None
    assert updated_snapshot.effective_from is None


def test_new_environment_no_changes(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.promotable_models = set()
    context_diff_mock.new_snapshots = {}
    context_diff_mock.is_new_environment = True
    context_diff_mock.has_snapshot_changes = False
    context_diff_mock.environment = "test_dev"
    context_diff_mock.previous_plan_id = "previous_plan_id"

    with pytest.raises(PlanError, match="No changes were detected.*"):
        Plan(context_diff_mock, is_dev=True)

    assert Plan(context_diff_mock).environment.promoted_snapshot_ids is None
    assert (
        Plan(
            context_diff_mock, is_dev=True, include_unmodified=True
        ).environment.promoted_snapshot_ids
        is None
    )


def test_new_environment_with_changes(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 3, ds")))

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": updated_snapshot_a, "b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.modified_snapshots = {"a": (updated_snapshot_a, snapshot_a)}
    context_diff_mock.promotable_models = {"a"}
    context_diff_mock.new_snapshots = {updated_snapshot_a.snapshot_id: updated_snapshot_a}
    context_diff_mock.is_new_environment = True
    context_diff_mock.has_snapshot_changes = True
    context_diff_mock.environment = "test_dev"
    context_diff_mock.previous_plan_id = "previous_plan_id"

    # Modified the existing model.
    assert Plan(context_diff_mock, is_dev=True).environment.promoted_snapshot_ids == [
        updated_snapshot_a.snapshot_id
    ]

    # Updating the existing environment with a previously promoted snapshot.
    context_diff_mock.promotable_models = {"a", "b"}
    context_diff_mock.is_new_environment = False
    assert set(Plan(context_diff_mock, is_dev=True).environment.promoted_snapshot_ids or []) == {
        updated_snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }

    # Adding a new model
    snapshot_c = make_snapshot(SqlModel(name="c", query=parse_one("select 4, ds")))
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)
    context_diff_mock.snapshots = {"a": updated_snapshot_a, "b": snapshot_b, "c": snapshot_c}
    context_diff_mock.added = {"c"}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_c.snapshot_id: snapshot_c}
    context_diff_mock.promotable_models = {"a", "b", "c"}
    assert set(Plan(context_diff_mock, is_dev=True).environment.promoted_snapshot_ids or []) == {
        updated_snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
        snapshot_c.snapshot_id,
    }


def test_forward_only_models(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    updated_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 3, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        )
    )

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": updated_snapshot}
    context_diff_mock.removed = set()
    context_diff_mock.added = set()
    context_diff_mock.added_materialized_models = set()
    context_diff_mock.modified_snapshots = {"a": (updated_snapshot, snapshot)}
    context_diff_mock.new_snapshots = {updated_snapshot.snapshot_id: updated_snapshot}
    context_diff_mock.has_snapshot_changes = True
    context_diff_mock.environment = "test_dev"
    context_diff_mock.previous_plan_id = "previous_plan_id"

    with pytest.raises(
        PlanError, match="Model 'a' can only be changed as part of a forward-only plan.*"
    ):
        Plan(context_diff_mock, is_dev=True)

    Plan(context_diff_mock, is_dev=True, forward_only=True)
    Plan(context_diff_mock, forward_only=True)


def test_disable_restatement(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds", disable_restatement=True),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot}
    context_diff_mock.removed = set()
    context_diff_mock.added = set()
    context_diff_mock.added_materialized_models = set()
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}
    context_diff_mock.has_snapshot_changes = False
    context_diff_mock.is_new_environment = False
    context_diff_mock.environment = "test_dev"
    context_diff_mock.previous_plan_id = "previous_plan_id"

    with pytest.raises(PlanError, match="Restatement is disabled for models: 'a'.*"):
        Plan(context_diff_mock, restate_models=["a"])

    # Effective from doesn't apply to snapshots for which restatements are disabled.
    plan = Plan(context_diff_mock, forward_only=True, effective_from="2023-01-01")
    assert plan.effective_from == "2023-01-01"
    assert snapshot.effective_from is None

    # Restatements should still be supported when in dev.
    plan = Plan(context_diff_mock, is_dev=True, restate_models=["a"])
    assert plan.restatements == {"a"}


def test_revert_to_previous_value(make_snapshot, mocker: MockerFixture):
    """
    Make sure we can revert to previous snapshots with intervals if it already exists and not modify
    it's existing change category
    """
    old_snapshot_a = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), depends_on={})
    )
    old_snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select 1, ds FROM a"), depends_on={"a"})
    )
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds"), depends_on={}))
    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("select 1, ds FROM a"), depends_on={"a"})
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_b.add_interval("2022-01-01", now())

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = set()
    context_diff_mock.removed = set()
    context_diff_mock.directly_modified.side_effect = lambda x: x == "a"
    context_diff_mock.modified_snapshots = {
        "a": (snapshot_a, old_snapshot_a),
        "b": (snapshot_b, old_snapshot_b),
    }
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}
    context_diff_mock.added_materialized_models = set()

    plan = Plan(context_diff_mock)
    plan.set_choice(snapshot_a, SnapshotChangeCategory.BREAKING)
    # Make sure it does not get assigned INDIRECT_BREAKING
    assert snapshot_b.change_category == SnapshotChangeCategory.FORWARD_ONLY
