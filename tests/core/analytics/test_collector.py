import json
import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.analytics.collector import AnalyticsCollector
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def collector(mocker: MockerFixture) -> AnalyticsCollector:
    dispatcher_mock = mocker.Mock()
    return AnalyticsCollector(dispatcher=dispatcher_mock)


def test_on_project_loaded(collector: AnalyticsCollector, mocker: MockerFixture):
    collector.on_project_loaded(
        project_type="NATIVE",
        models_count=1,
        audits_count=2,
        standalone_audits_count=3,
        macros_count=4,
        jinja_macros_count=5,
        load_time_sec=1.123,
        state_sync_fingerprint="test_fingerprint",
        project_name="test_project",
    )

    collector.flush()

    collector._dispatcher.add_event.assert_called_once_with(  # type: ignore
        {
            "user_id": mocker.ANY,
            "process_id": collector._process_id,
            "seq_num": 0,
            "event_type": "PROJECT_LOADED",
            "client_ts": mocker.ANY,
            "event": '{"project_type": "NATIVE", "models_count": 1, "audits_count": 2, "standalone_audits_count": 3, "macros_count": 4, "jinja_macros_count": 5, "load_time_ms": 1123, "state_sync_fingerprint": "test_fingerprint", "project_name_hash": "ad62917b5e639fad064d4c72dd0eca227d2cccab83bac09ded995db314e5b200", "dbt_version": null}',
        }
    )


def test_on_command(collector: AnalyticsCollector, mocker: MockerFixture):
    collector.on_python_api_command(command_name="test_python_api", command_args=["arg_1", "arg_2"])
    collector.on_magic_command(command_name="test_magic", command_args=["arg_1", "arg_2"])
    collector.on_cli_command(
        command_name="test_cli", command_args=["arg_1", "arg_2"], parent_command_names=[]
    )

    collector.flush()

    common_fields = {
        "user_id": mocker.ANY,
        "process_id": collector._process_id,
        "client_ts": mocker.ANY,
    }

    collector._dispatcher.add_event.assert_has_calls(  # type: ignore
        [
            call(
                {
                    "seq_num": 0,
                    "event_type": "PYTHON_API_COMMAND",
                    "event": '{"command_name": "test_python_api", "command_args": ["arg_1", "arg_2"]}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 1,
                    "event_type": "MAGIC_COMMAND",
                    "event": '{"command_name": "test_magic", "command_args": ["arg_1", "arg_2"]}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 2,
                    "event_type": "CLI_COMMAND",
                    "event": '{"command_name": "test_cli", "command_args": ["arg_1", "arg_2"], "parent_command_names": []}',
                    **common_fields,
                }
            ),
        ]
    )


def test_on_cicd_command(collector: AnalyticsCollector, mocker: MockerFixture):
    collector.on_cicd_command(
        command_name="test_cicd",
        command_args=["arg_1", "arg_2"],
        parent_command_names=["parent_a", "parent_b"],
        cicd_bot_config=None,
    )
    collector.on_cicd_command(
        command_name="test_cicd",
        command_args=["arg_1", "arg_2"],
        parent_command_names=["parent_a", "parent_b"],
        cicd_bot_config=GithubCICDBotConfig(),
    )

    collector.flush()

    common_fields = {
        "user_id": mocker.ANY,
        "process_id": collector._process_id,
        "client_ts": mocker.ANY,
    }

    collector._dispatcher.add_event.assert_has_calls(  # type: ignore
        [
            call(
                {
                    "seq_num": 0,
                    "event_type": "CICD_COMMAND",
                    "event": '{"command_name": "test_cicd", "command_args": ["arg_1", "arg_2"], "parent_command_names": ["parent_a", "parent_b"]}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 1,
                    "event_type": "CICD_COMMAND",
                    "event": '{"command_name": "test_cicd", "command_args": ["arg_1", "arg_2"], "parent_command_names": ["parent_a", "parent_b"], "cicd_bot_config": {"invalidate_environment_after_deploy": true, "enable_deploy_command": false, "auto_categorize_changes": {"external": "off", "python": "off", "sql": "off", "seed": "off"}, "skip_pr_backfill": true, "run_on_deploy_to_prod": true}}',
                    **common_fields,
                }
            ),
        ]
    )


@pytest.mark.slow
def test_on_plan_apply(
    collector: AnalyticsCollector, mocker: MockerFixture, init_and_plan_context: t.Callable
):
    context, plan = init_and_plan_context("examples/sushi")

    plan_id = plan.plan_id
    collector.on_plan_apply_start(
        plan=plan, engine_type="bigquery", state_sync_type="mysql", scheduler_type="builtin"
    )
    collector.on_plan_apply_end(plan_id=plan_id)
    collector.on_plan_apply_end(plan_id=plan_id, error=SQLMeshError("test_error"))

    collector.flush()

    common_fields = {
        "user_id": mocker.ANY,
        "process_id": collector._process_id,
        "client_ts": mocker.ANY,
    }

    collector._dispatcher.add_event.assert_has_calls(  # type: ignore
        [
            call(
                {
                    "seq_num": 0,
                    "event_type": "PLAN_APPLY_START",
                    "event": f'{{"plan_id": "{plan_id}", "engine_type": "bigquery", "state_sync_type": "mysql", "scheduler_type": "BUILTIN", "is_dev": false, "skip_backfill": false, "no_gaps": false, "forward_only": false, "ensure_finalized_snapshots": false, "has_restatements": false, "directly_modified_count": 17, "indirectly_modified_count": 0, "environment_name_hash": "6754af9632a2745e85c293e5aac0863370d9bd3330b9938c00cadfd215227d77"}}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 1,
                    "event_type": "PLAN_APPLY_END",
                    "event": f'{{"plan_id": "{plan_id}", "succeeded": true, "error": null}}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 2,
                    "event_type": "PLAN_APPLY_END",
                    "event": f'{{"plan_id": "{plan_id}", "succeeded": false, "error": "SQLMeshError"}}',
                    **common_fields,
                }
            ),
        ]
    )


@pytest.mark.slow
def test_on_snapshots_created(
    collector: AnalyticsCollector, mocker: MockerFixture, init_and_plan_context: t.Callable
):
    context, _ = init_and_plan_context("examples/sushi")

    new_snapshots = [
        context.get_snapshot("sushi.orders"),
        context.get_snapshot("sushi.waiter_revenue_by_day"),
        context.get_snapshot("sushi.top_waiters"),
    ]
    new_snapshots[0].categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshots[0].effective_from = "2024-01-01"
    new_snapshots[0].version = "test_version"

    new_snapshots[1].categorize_as(SnapshotChangeCategory.BREAKING)
    new_snapshots[2].categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)

    plan_id = "test_plan_id"

    collector.on_snapshots_created(new_snapshots=new_snapshots, plan_id=plan_id)

    collector.flush()

    snapshots = [
        {
            "name_hash": "84215ae191517da5c59da03fa46ed6ce09b3f4d68008d2d9ef4e3e8bead0e588",
            "identifier": new_snapshots[0].identifier,
            "version": new_snapshots[0].version,
            "node_type": "MODEL",
            "model_kind": "INCREMENTAL_BY_TIME_RANGE",
            "is_sql": False,
            "change_category": "FORWARD_ONLY",
            "dialect": "duckdb",
            "audits_count": 0,
            "effective_from_set": True,
        },
        {
            "name_hash": "9877934ae555dd62818f87445d31878eee279654513f2a4972c2d0c77eac6c7b",
            "identifier": new_snapshots[1].identifier,
            "version": new_snapshots[1].version,
            "node_type": "MODEL",
            "model_kind": "INCREMENTAL_BY_TIME_RANGE",
            "is_sql": True,
            "change_category": "BREAKING",
            "dialect": "duckdb",
            "audits_count": 1,
            "effective_from_set": False,
        },
        {
            "name_hash": "43e722d38768169cee67b816fec067a9c573002858b46551ae463caa545de39a",
            "identifier": new_snapshots[2].identifier,
            "version": new_snapshots[2].version,
            "node_type": "MODEL",
            "model_kind": "VIEW",
            "is_sql": True,
            "change_category": "INDIRECT_BREAKING",
            "dialect": "duckdb",
            "audits_count": 1,
            "effective_from_set": False,
        },
    ]

    collector._dispatcher.add_event.assert_called_once_with(  # type: ignore
        {
            "seq_num": 0,
            "event_type": "SNAPSHOTS_CREATED",
            "event": f'{{"plan_id": "{plan_id}", "snapshots": {json.dumps(snapshots)}}}',
            "user_id": mocker.ANY,
            "process_id": collector._process_id,
            "client_ts": mocker.ANY,
        },
    )


def test_on_run(collector: AnalyticsCollector, mocker: MockerFixture):
    run_id = "test_run_id"
    collector.on_run_start(run_id=run_id, engine_type="bigquery", state_sync_type="mysql")
    collector.on_run_end(run_id=run_id)
    collector.on_run_end(run_id=run_id, error=SQLMeshError("test_error"))

    collector.flush()

    common_fields = {
        "user_id": mocker.ANY,
        "process_id": collector._process_id,
        "client_ts": mocker.ANY,
    }

    collector._dispatcher.add_event.assert_has_calls(  # type: ignore
        [
            call(
                {
                    "seq_num": 0,
                    "event_type": "RUN_START",
                    "event": f'{{"run_id": "{run_id}", "engine_type": "bigquery", "state_sync_type": "mysql"}}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 1,
                    "event_type": "RUN_END",
                    "event": f'{{"run_id": "{run_id}", "succeeded": true, "error": null}}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 2,
                    "event_type": "RUN_END",
                    "event": f'{{"run_id": "{run_id}", "succeeded": false, "error": "SQLMeshError"}}',
                    **common_fields,
                }
            ),
        ]
    )


def test_on_migration(collector: AnalyticsCollector, mocker: MockerFixture):
    collector.on_migration_end(
        from_sqlmesh_version="0.0.0", state_sync_type="mysql", migration_time_sec=1.123
    )
    collector.on_migration_end(
        from_sqlmesh_version="0.0.0",
        state_sync_type="mysql",
        error=SQLMeshError("test_error"),
        migration_time_sec=1.321,
    )

    collector.flush()

    common_fields = {
        "user_id": mocker.ANY,
        "process_id": collector._process_id,
        "client_ts": mocker.ANY,
    }

    collector._dispatcher.add_event.assert_has_calls(  # type: ignore
        [
            call(
                {
                    "seq_num": 0,
                    "event_type": "MIGRATION_END",
                    "event": '{"from_sqlmesh_version": "0.0.0", "state_sync_type": "mysql", "succeeded": true, "error": null, "migration_time_ms": 1123}',
                    **common_fields,
                }
            ),
            call(
                {
                    "seq_num": 1,
                    "event_type": "MIGRATION_END",
                    "event": '{"from_sqlmesh_version": "0.0.0", "state_sync_type": "mysql", "succeeded": false, "error": "SQLMeshError", "migration_time_ms": 1321}',
                    **common_fields,
                }
            ),
        ]
    )
