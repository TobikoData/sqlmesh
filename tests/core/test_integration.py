import typing as t
from datetime import date

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one
from sqlglot.expressions import DataType

from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import ModelKind
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotInfoLike,
    SnapshotTableInfo,
)

START = date(2022, 1, 1)
END = date(2022, 1, 7)


@pytest.fixture(autouse=True)
def mock_choices(mocker: MockerFixture):
    mocker.patch("sqlmesh.core.console.TerminalConsole._get_snapshot_change_category")
    mocker.patch("sqlmesh.core.console.TerminalConsole._prompt_backfill")


def plan_choice(plan: Plan, choice: SnapshotChangeCategory) -> None:
    for snapshot in plan.snapshots:
        if not snapshot.version:
            plan.set_choice(snapshot, choice)


@pytest.mark.integration
@pytest.mark.core_integration
def test_model_add(sushi_context: Context):
    initial_add(sushi_context, "dev")


@pytest.mark.integration
@pytest.mark.core_integration
def test_model_removed(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)

    top_waiters_snapshot_id = sushi_context.snapshots["sushi.top_waiters"].snapshot_id

    sushi_context.models.pop("sushi.top_waiters")
    removed = ["sushi.top_waiters"]
    [key for key in sushi_context.snapshots if key not in removed]

    def _validate_plan(context, plan):
        validate_plan_changes(plan, removed=removed)
        assert not plan.missing_intervals

    def _validate_apply(context):
        assert not sushi_context.snapshots.get("sushi.top_waiters")
        assert sushi_context.state_reader.get_snapshots([top_waiters_snapshot_id])
        env = sushi_context.state_reader.get_environment(environment)
        assert env
        assert all(snapshot.name != "sushi.top_waiters" for snapshot in env.snapshots)

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_non_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(
        sushi_context, environment, SnapshotChangeCategory.NON_BREAKING, False
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(
        sushi_context, environment, SnapshotChangeCategory.BREAKING, False
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_no_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(
        sushi_context, environment, SnapshotChangeCategory.NO_CHANGE, True
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_logical_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    previous_sushi_items_version = sushi_context.snapshots["sushi.items"].version

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(
        sushi_context, environment, SnapshotChangeCategory.NON_BREAKING
    )

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.FLOAT,
        DataType.Type.DOUBLE,
    )
    apply_to_environment(
        sushi_context, environment, SnapshotChangeCategory.NON_BREAKING
    )

    assert (
        sushi_context.snapshots["sushi.items"].version == previous_sushi_items_version
    )


def validate_query_change(
    context: Context,
    environment: str,
    change_category: SnapshotChangeCategory,
    logical: bool,
):
    versions = snapshots_to_versions(context.snapshots)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )

    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.top_waiters",
    ]
    not_modified = [
        key
        for key in context.snapshots
        if key not in directly_modified and key not in indirectly_modified
    ]

    if change_category == SnapshotChangeCategory.BREAKING and not logical:
        models_same = not_modified
        models_different = directly_modified + indirectly_modified
    elif change_category == SnapshotChangeCategory.NO_CHANGE:
        models_same = not_modified + directly_modified + indirectly_modified
        models_different = []
    else:
        models_same = not_modified + indirectly_modified
        models_different = directly_modified

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert bool(plan.missing_intervals) != logical

    def _validate_apply(context):
        current_versions = snapshots_to_versions(context.snapshots)
        validate_versions_same(models_same, versions, current_versions)
        validate_versions_different(models_different, versions, current_versions)

    apply_to_environment(
        context,
        environment,
        change_category,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


@pytest.mark.integration
@pytest.mark.core_integration
@pytest.mark.parametrize(
    "from_, to",
    [
        (ModelKind.INCREMENTAL, ModelKind.VIEW),
        (ModelKind.INCREMENTAL, ModelKind.EMBEDDED),
        (ModelKind.INCREMENTAL, ModelKind.FULL),
        (ModelKind.VIEW, ModelKind.EMBEDDED),
        (ModelKind.VIEW, ModelKind.FULL),
        (ModelKind.VIEW, ModelKind.INCREMENTAL),
        (ModelKind.EMBEDDED, ModelKind.VIEW),
        (ModelKind.EMBEDDED, ModelKind.FULL),
        (ModelKind.EMBEDDED, ModelKind.INCREMENTAL),
        (ModelKind.FULL, ModelKind.VIEW),
        (ModelKind.FULL, ModelKind.EMBEDDED),
        (ModelKind.FULL, ModelKind.INCREMENTAL),
    ],
)
def test_model_kind_change(from_: ModelKind, to: ModelKind, sushi_context: Context):
    environment = "prod"
    incremental_snapshot = sushi_context.snapshots["sushi.items"].copy()

    if from_ != ModelKind.INCREMENTAL:
        change_model_kind(sushi_context, from_)
        apply_to_environment(
            sushi_context, environment, SnapshotChangeCategory.NON_BREAKING
        )

    if to == ModelKind.INCREMENTAL:
        sushi_context.upsert_model(incremental_snapshot.model)
    else:
        change_model_kind(sushi_context, to)

    logical = to in (ModelKind.INCREMENTAL, ModelKind.EMBEDDED)
    validate_model_kind_change(to, sushi_context, environment, logical=logical)


def change_model_kind(context: Context, kind: ModelKind):
    if kind in (ModelKind.VIEW, ModelKind.EMBEDDED, ModelKind.FULL):
        sushi_items_query_no_dates = parse_one(
            "SELECT id::INT AS id, name::TEXT AS name, price::DOUBLE AS price, ds::TEXT AS ds FROM raw.items",
            read=context.dialect,
        )
        context.upsert_model(
            "sushi.items",
            query=sushi_items_query_no_dates,
            partitioned_by=None,
            audits={},
        )
    context.upsert_model("sushi.items", kind=kind)


def validate_model_kind_change(
    kind: ModelKind,
    context: Context,
    environment: str,
    *,
    logical: bool,
):
    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.top_waiters",
    ]

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert (
            next(
                snapshot
                for snapshot in plan.snapshots
                if snapshot.name == "sushi.items"
            ).model.kind
            == kind
        )
        assert bool(plan.missing_intervals) != logical

    apply_to_environment(
        context,
        environment,
        SnapshotChangeCategory.NON_BREAKING,
        plan_validators=[_validate_plan],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_environment_isolation(sushi_context: Context):
    prod_snapshots = sushi_context.snapshots.values()

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.top_waiters",
    ]

    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    # Verify prod unchanged
    validate_apply_basics(sushi_context, "prod", prod_snapshots)

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert not plan.missing_intervals

    apply_to_environment(
        sushi_context,
        "prod",
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_environment_promotion(sushi_context: Context):
    initial_add(sushi_context, "dev")

    # Simulate prod "ahead"
    change_data_type(
        sushi_context, "sushi.items", DataType.Type.DOUBLE, DataType.Type.FLOAT
    )
    apply_to_environment(sushi_context, "prod", SnapshotChangeCategory.BREAKING)

    # Simulate rebase
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    # Make changes in dev
    change_data_type(
        sushi_context, "sushi.items", DataType.Type.FLOAT, DataType.Type.INT
    )
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.NON_BREAKING)

    change_data_type(
        sushi_context, "sushi.top_waiters", DataType.Type.DOUBLE, DataType.Type.INT
    )
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    change_data_type(
        sushi_context,
        "sushi.customer_revenue_by_day",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.NO_CHANGE)

    # Promote to prod
    def _validate_plan(context, plan):
        assert (
            plan.snapshot_change_category(
                plan.context_diff.modified_snapshots["sushi.items"][0]
            )
            == SnapshotChangeCategory.NON_BREAKING
        )
        assert (
            plan.snapshot_change_category(
                plan.context_diff.modified_snapshots["sushi.top_waiters"][0]
            )
            == SnapshotChangeCategory.BREAKING
        )
        assert (
            plan.snapshot_change_category(
                plan.context_diff.modified_snapshots["sushi.customer_revenue_by_day"][0]
            )
            == SnapshotChangeCategory.NO_CHANGE
        )

    apply_to_environment(
        sushi_context,
        "prod",
        SnapshotChangeCategory.NON_BREAKING,
        plan_validators=[_validate_plan],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_no_override(sushi_context: Context) -> None:
    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )

    change_data_type(
        sushi_context,
        "sushi.order_items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )
    plan = sushi_context.plan("prod", start=START, end=END)
    items = plan.context_diff.snapshots["sushi.items"]
    order_items = plan.context_diff.snapshots["sushi.order_items"]
    waiter_revenue = plan.context_diff.snapshots["sushi.waiter_revenue_by_day"]
    plan.set_choice(items, SnapshotChangeCategory.BREAKING)
    plan.set_choice(order_items, SnapshotChangeCategory.NON_BREAKING)
    assert items.is_new_version
    assert waiter_revenue.is_new_version
    plan.set_choice(items, SnapshotChangeCategory.NON_BREAKING)
    assert not waiter_revenue.is_new_version


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_remote_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.BREAKING,
        SnapshotChangeCategory.NON_BREAKING,
        version_kind="remote",
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_local_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.NON_BREAKING,
        SnapshotChangeCategory.BREAKING,
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_no_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.NON_BREAKING,
        SnapshotChangeCategory.NON_BREAKING,
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.BREAKING,
        SnapshotChangeCategory.BREAKING,
        "new",
    )


def setup_rebase(
    context: Context,
    remote_choice: SnapshotChangeCategory,
    local_choice: SnapshotChangeCategory,
    version_kind: str = "local",
) -> None:
    initial_add(context, "dev")

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    plan = context.plan("prod", start=START, end=END)
    plan_choice(plan, remote_choice)
    remote_versions = {snapshot.name: snapshot.version for snapshot in plan.snapshots}
    context.apply(plan)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.FLOAT,
        DataType.Type.DOUBLE,
    )
    change_data_type(
        context,
        "sushi.order_items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )
    plan = context.plan("dev", start=START, end=END)
    plan_choice(plan, local_choice)
    local_versions = {snapshot.name: snapshot.version for snapshot in plan.snapshots}
    context.apply(plan)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    plan = context.plan("dev", start=START, end=END)

    assert plan.categorized == [context.snapshots["sushi.items"]]
    assert plan.indirectly_modified == {
        "sushi.items": {
            "sushi.waiter_revenue_by_day",
            "sushi.top_waiters",
            "sushi.customer_revenue_by_day",
        }
    }
    context.apply(plan)
    validate_apply_basics(context, "dev", plan.snapshots)

    if version_kind == "new":
        for versions in [remote_versions, local_versions]:
            assert (
                context.snapshots["sushi.waiter_revenue_by_day"].version
                != versions["sushi.waiter_revenue_by_day"]
            )
            assert (
                context.snapshots["sushi.top_waiters"].version
                != versions["sushi.top_waiters"]
            )
            assert (
                context.snapshots["sushi.customer_revenue_by_day"].version
                != versions["sushi.customer_revenue_by_day"]
            )
    else:
        if version_kind == "remote":
            versions = remote_versions
        else:
            versions = local_versions
        assert (
            context.snapshots["sushi.waiter_revenue_by_day"].version
            == versions["sushi.waiter_revenue_by_day"]
        )
        assert (
            context.snapshots["sushi.top_waiters"].version
            == versions["sushi.top_waiters"]
        )
        assert (
            context.snapshots["sushi.customer_revenue_by_day"].version
            == versions["sushi.customer_revenue_by_day"]
        )


@pytest.mark.integration
@pytest.mark.core_integration
@pytest.mark.parametrize(
    "change_categories, expected",
    [
        ([SnapshotChangeCategory.NO_CHANGE], SnapshotChangeCategory.NO_CHANGE),
        ([SnapshotChangeCategory.NON_BREAKING], SnapshotChangeCategory.NON_BREAKING),
        ([SnapshotChangeCategory.BREAKING], SnapshotChangeCategory.BREAKING),
        (
            [SnapshotChangeCategory.NO_CHANGE, SnapshotChangeCategory.NO_CHANGE],
            SnapshotChangeCategory.NO_CHANGE,
        ),
        (
            [SnapshotChangeCategory.NO_CHANGE, SnapshotChangeCategory.NON_BREAKING],
            SnapshotChangeCategory.NON_BREAKING,
        ),
        (
            [SnapshotChangeCategory.NO_CHANGE, SnapshotChangeCategory.BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.NON_BREAKING, SnapshotChangeCategory.NO_CHANGE],
            SnapshotChangeCategory.NON_BREAKING,
        ),
        (
            [SnapshotChangeCategory.NON_BREAKING, SnapshotChangeCategory.NON_BREAKING],
            SnapshotChangeCategory.NON_BREAKING,
        ),
        (
            [SnapshotChangeCategory.NON_BREAKING, SnapshotChangeCategory.BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.NO_CHANGE],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.NON_BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
    ],
)
def test_revert(
    sushi_context: Context,
    change_categories: t.List[SnapshotChangeCategory],
    expected: SnapshotChangeCategory,
):
    environment = "prod"
    original_snapshot_id = sushi_context.snapshots["sushi.items"]

    types = (DataType.Type.DOUBLE, DataType.Type.FLOAT, DataType.Type.DECIMAL)
    assert len(change_categories) < len(types)

    for i, category in enumerate(change_categories):
        change_data_type(sushi_context, "sushi.items", *types[i : i + 2])
        apply_to_environment(sushi_context, environment, category)
        assert sushi_context.snapshots["sushi.items"] != original_snapshot_id

    change_data_type(
        sushi_context, "sushi.items", types[len(change_categories)], types[0]
    )

    def _validate_plan(_, plan):
        snapshot = next(s for s in plan.snapshots if s.name == "sushi.items")
        assert plan.snapshot_change_category(snapshot) == expected
        assert not plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        change_categories[-1],
        plan_validators=[_validate_plan],
    )
    assert sushi_context.snapshots["sushi.items"] == original_snapshot_id


@pytest.mark.integration
@pytest.mark.core_integration
def test_revert_after_downstream_change(sushi_context: Context):
    environment = "prod"
    change_data_type(
        sushi_context, "sushi.items", DataType.Type.DOUBLE, DataType.Type.FLOAT
    )
    apply_to_environment(sushi_context, environment, SnapshotChangeCategory.BREAKING)

    change_data_type(
        sushi_context,
        "sushi.customer_revenue_by_day",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(
        sushi_context, environment, SnapshotChangeCategory.NON_BREAKING
    )

    change_data_type(
        sushi_context, "sushi.items", DataType.Type.FLOAT, DataType.Type.DOUBLE
    )

    def _validate_plan(_, plan):
        snapshot = next(s for s in plan.snapshots if s.name == "sushi.items")
        assert (
            plan.snapshot_change_category(snapshot) == SnapshotChangeCategory.BREAKING
        )
        assert plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
    )


def initial_add(context: Context, environment: str):
    assert not context.state_reader.get_environment(environment)

    plan = context.plan(environment, start=START, end=END)
    validate_plan_changes(plan, added=context.models)

    context.apply(plan)
    validate_apply_basics(context, environment, plan.snapshots)


def apply_to_environment(
    context: Context,
    environment: str,
    choice: SnapshotChangeCategory,
    plan_validators: t.Optional[t.Iterable[t.Callable]] = None,
    apply_validators: t.Optional[t.Iterable[t.Callable]] = None,
):
    plan_validators = plan_validators or []
    apply_validators = apply_validators or []

    plan = context.plan(environment, start=START, end=END)
    plan_choice(plan, choice)
    for validator in plan_validators:
        validator(context, plan)

    context.apply(plan)
    validate_apply_basics(context, environment, plan.snapshots)
    for validator in apply_validators:
        validator(context)


def change_data_type(
    context: Context, model_name: str, old_type: DataType.Type, new_type: DataType.Type
) -> None:
    model = context.models[model_name]

    data_types = model.query.find_all(DataType)
    for data_type in data_types:
        if data_type.this == old_type:
            data_type.set("this", new_type)

    context.upsert_model(model_name, query=model.query)


def validate_plan_changes(
    plan: Plan,
    *,
    added: t.Optional[t.Iterable[str]] = None,
    modified: t.Optional[t.Iterable[str]] = None,
    removed: t.Optional[t.Iterable[str]] = None,
) -> None:
    added = added or []
    modified = modified or []
    removed = removed or []
    assert set(added) == set(plan.context_diff.added)
    assert set(modified) == set(plan.context_diff.modified_snapshots)
    assert set(removed) == set(plan.context_diff.removed)


def validate_versions_same(
    model_names: t.List[str],
    versions: t.Dict[str, str],
    other_versions: t.Dict[str, str],
) -> None:
    for name in model_names:
        assert versions[name] == other_versions[name]


def validate_versions_different(
    model_names: t.List[str],
    versions: t.Dict[str, str],
    other_versions: t.Dict[str, str],
) -> None:
    for name in model_names:
        assert versions[name] != other_versions[name]


def validate_apply_basics(
    context: Context, environment: str, snapshots: t.Iterable[Snapshot]
) -> None:
    validate_snapshots_in_state_sync(snapshots, context)
    validate_state_sync_environment(snapshots, environment, context)
    validate_tables(snapshots, context)
    validate_environment_views(snapshots, environment, context)


def validate_snapshots_in_state_sync(
    snapshots: t.Iterable[Snapshot], context: Context
) -> None:
    snapshot_infos = map(to_snapshot_info, snapshots)
    state_sync_table_infos = map(
        to_snapshot_info, context.state_reader.get_snapshots(snapshots).values()
    )
    assert set(snapshot_infos) == set(state_sync_table_infos)


def validate_state_sync_environment(
    snapshots: t.Iterable[Snapshot], env: str, context: Context
) -> None:
    environment = context.state_reader.get_environment(env)
    assert environment
    snapshot_infos = map(to_snapshot_info, snapshots)
    environment_table_infos = map(to_snapshot_info, environment.snapshots)
    assert set(snapshot_infos) == set(environment_table_infos)


def validate_tables(snapshots: t.Iterable[Snapshot], context: Context) -> None:
    adapter = context.engine_adapter
    for snapshot in snapshots:
        table_should_exist = not snapshot.is_embedded_kind
        assert adapter.table_exists(snapshot.table_name) == table_should_exist
        if table_should_exist:
            assert bool(list(select_all(snapshot.table_name, adapter)))


def validate_environment_views(
    snapshots: t.Iterable[Snapshot], environment: str, context: Context
) -> None:
    adapter = context.engine_adapter
    for snapshot in snapshots:
        if snapshot.is_embedded_kind:
            continue

        view_name = snapshot.qualified_view_name.for_environment(
            environment=environment
        )
        assert adapter.table_exists(view_name)
        assert select_all(snapshot.table_name, adapter) == select_all(
            view_name, adapter
        )


def select_all(table: str, adapter: EngineAdapter) -> t.Iterable:
    return adapter.fetchall(f"select * from {table}")


def snapshots_to_versions(snapshots: t.Dict[str, Snapshot]) -> t.Dict[str, str]:
    return {k: v.version or "" for k, v in snapshots.items()}


def to_snapshot_info(snapshot: SnapshotInfoLike) -> SnapshotTableInfo:
    return snapshot.table_info
