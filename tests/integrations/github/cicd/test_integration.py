"""
These integration tests are for testing integrating with SQLMesh and not integrating with Github.
Therefore Github calls are still mocked but context is fully evaluated.
"""

import os
import pathlib
import typing as t
from unittest import mock

import pytest
import time_machine
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.config import CategorizerConfig, Config, ModelDefaultsConfig, LinterConfig
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd import command
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    GithubController,
)
from sqlmesh.utils.errors import CICDBotError, SQLMeshError
from tests.integrations.github.cicd.conftest import MockIssueComment

pytestmark = [
    pytest.mark.slow,
    pytest.mark.github,
]


def get_environment_objects(controller: GithubController, environment: str) -> t.List[DataObject]:
    return controller._context.engine_adapter.get_data_objects(f"sushi__{environment}")


def get_num_days_loaded(controller: GithubController, environment: str, model: str) -> int:
    return controller._context.engine_adapter.fetchdf(
        f"SELECT distinct event_date FROM sushi__{environment}.{model}"
    ).shape[0]


def get_columns(
    controller: GithubController, environment: t.Optional[str], model: str
) -> t.Dict[str, exp.DataType]:
    table = f"sushi__{environment}.{model}" if environment else f"sushi.{model}"
    return controller._context.engine_adapter.columns(table)


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_linter(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with the Linter enabled will contain a new check with the linter specific output.

    Scenarios:
    - PR with linter errors leads to job failures & skips
    - PR with linter warnings leads to job successes
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    before_all = [
        "CREATE SCHEMA IF NOT EXISTS raw",
        "DROP VIEW IF EXISTS raw.demographics",
        "CREATE VIEW raw.demographics AS (SELECT 1 AS customer_id, '00000' AS zip)",
    ]

    # Case 1: Test for linter errors
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        linter=LinterConfig(enabled=True, rules="ALL"),
        before_all=before_all,
    )

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
        config=config,
    )

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        with pytest.raises(CICDBotError):
            command._run_all(controller)

    assert "SQLMesh - Linter" in controller._check_run_mapping
    linter_checks_runs = controller._check_run_mapping["SQLMesh - Linter"].all_kwargs
    assert "Linter **errors** for" in linter_checks_runs[2]["output"]["summary"]
    assert GithubCheckConclusion(linter_checks_runs[2]["conclusion"]).is_failure

    for check in (
        "SQLMesh - PR Environment Synced",
        "SQLMesh - Prod Plan Preview",
    ):
        assert check in controller._check_run_mapping
        check_runs = controller._check_run_mapping[check].all_kwargs
        assert GithubCheckConclusion(check_runs[-1]["conclusion"]).is_skipped

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "linter=failure\nrun_unit_tests=success\npr_environment_name=hello_world_2\npr_environment_synced=skipped\nprod_plan_preview=skipped\n"
        )

    # empty github file for next case
    open(github_output_file, "w").close()

    # Case 2: Test for linter warnings
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        linter=LinterConfig(enabled=True, warn_rules="ALL"),
        before_all=before_all,
    )

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
        config=config,
    )

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Linter" in controller._check_run_mapping
    linter_checks_runs = controller._check_run_mapping["SQLMesh - Linter"].all_kwargs
    assert "Linter warnings for" in linter_checks_runs[-1]["output"]["summary"]
    assert GithubCheckConclusion(linter_checks_runs[-1]["conclusion"]).is_success

    for check in (
        "SQLMesh - Run Unit Tests",
        "SQLMesh - PR Environment Synced",
        "SQLMesh - Prod Plan Preview",
    ):
        assert check in controller._check_run_mapping
        check_runs = controller._check_run_mapping[check].all_kwargs
        assert GithubCheckConclusion(check_runs[-1]["conclusion"]).is_success

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "linter=success\nrun_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_merge_pr_has_non_breaking_change(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with a non-breaking change and auto-categorization will be backfilled, merged, and deployed to prod

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-25 - 2022-12-31]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)
"""

    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_directly_modified_summary in prod_environment_synced_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`"""
        in comment_body
    )
    assert expected_prod_plan_directly_modified_summary in comment_body
    assert expected_prod_plan_indirectly_modified_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_merge_pr_has_non_breaking_change_diff_start(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Different PR start correctly affects only the PR environment and prod still has no gaps

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR with auto-categorization and different PR start
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start="3 days ago",
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-29 - 2022-12-31]
  **Dates *not* loaded in PR:** [2022-12-25 - 2022-12-28]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"

    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)
"""

    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_directly_modified_summary in prod_environment_synced_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    # 7 days since the prod deploy went through and backfilled the remaining days
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`"""
        in comment_body
    )
    assert expected_prod_plan_directly_modified_summary in comment_body
    assert expected_prod_plan_indirectly_modified_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_merge_pr_has_non_breaking_change_no_categorization(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR without auto-categorization but with changes errors asking for user to categorize

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR without auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        with pytest.raises(CICDBotError):
            command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_action_required
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """:warning: Action Required to create or update PR Environment `hello_world_2` :warning:

The following models could not be categorized automatically:
- "memory"."sushi"."waiter_revenue_by_day"

Run `sqlmesh plan hello_world_2` locally to apply these changes"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 2
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[1]["conclusion"]).is_skipped
    assert (
        prod_plan_preview_checks_runs[1]["output"]["title"]
        == "Skipped generating prod plan preview since PR was not synchronized"
    )
    assert (
        prod_plan_preview_checks_runs[1]["output"]["summary"]
        == "Skipped generating prod plan preview since PR was not synchronized"
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped
    assert prod_checks_runs[1]["output"]["title"] == "Skipped deployment"
    assert (
        prod_checks_runs[1]["output"]["summary"]
        == "Skipped Deploying to Production because the PR environment was not updated"
    )

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 0
    assert "new_col" not in get_columns(controller, None, "waiter_revenue_by_day")

    assert not mock_pull_request.merge.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=action_required\nprod_plan_preview=skipped\nprod_environment_synced=skipped\n"
        )


def test_merge_pr_has_no_changes(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR is opened that has no changes and approved. Should be merged and no-op deploy.

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - No changes made in PR
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE, invalidate_environment_after_deploy=False
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_skipped
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        ":next_track_button: Skipped creating or updating PR Environment `hello_world_2` :next_track_button:\n\nNo changes were detected compared to the prod environment."
        in pr_checks_runs[2]["output"]["summary"]
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    expected_prod_plan_summary = (
        "**No changes to plan: project files match the `prod` environment**"
    )
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert expected_prod_plan_summary in prod_plan_preview_checks_runs[2]["output"]["summary"]

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 0

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        f""":robot: **SQLMesh Bot Info** :robot:
<details>
  <summary>:ship: Prod Plan Being Applied</summary>"""
        in comment_body
    )
    assert expected_prod_plan_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=skipped\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_no_merge_since_no_deploy_signal(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR is updated and has auto-categorization but lacks approval or comment so no deploy

    Scenario:
    - PR is not merged
    - PR has not been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Non-breaking changes made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(side_effect=lambda: [])
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-25 - 2022-12-31]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)"""

    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)
"""

    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped
    assert prod_checks_runs[1]["output"]["title"] == "Skipped deployment"
    assert (
        prod_checks_runs[1]["output"]["summary"]
        == "Skipped Deploying to Production because a required approver has not approved"
    )

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_failure
    assert approval_checks_runs[2]["output"]["title"] == "Need a Required Approval"
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" not in get_columns(controller, None, "waiter_revenue_by_day")

    assert not mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`"""
    )

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=failure\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=skipped\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_no_merge_since_no_deploy_signal_no_approvers_defined(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR is updated with auto-categorization and has no required approvers defined on the project so no deploy

    Scenario:
    - PR is not merged
    - PR has not been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Non-breaking changes made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            skip_pr_backfill=False,
            default_pr_start="2 days ago",
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [User(username="test", github_username="test_github", roles=[])]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-30 - 2022-12-31]
  **Dates *not* loaded in PR:** [2022-12-25 - 2022-12-29]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" not in controller._check_run_mapping
    assert "SQLMesh - Has Required Approval" not in controller._check_run_mapping

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 2
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" not in get_columns(controller, None, "waiter_revenue_by_day")

    assert not mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`"""
    )

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_deploy_comment_pre_categorized(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR is updated without categorization but has already been categorized by plan/apply and therefore deploys because of a deploy comment

    Scenario:
    - PR is not merged
    - PR has not been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - PR is triggered by a deploy comment
    - Changes are already categorized prior to opening the PR
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_command_deploy.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_off(),
            enable_deploy_command=True,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [User(username="test", github_username="test_github", roles=[])]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    # Manually categorize the change as non-breaking and don't backfill anything
    controller._context.plan(
        "dev",
        no_prompts=True,
        auto_apply=True,
        categorizer_config=CategorizerConfig.all_full(),
        skip_backfill=True,
    )

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-25 - 2022-12-31]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)
"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_directly_modified_summary in prod_environment_synced_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" not in controller._check_run_mapping

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
- :arrow_forward: To **apply** this PR's plan to prod, comment:
  - `/deploy`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>"""
        in comment_body
    )
    assert expected_prod_plan_directly_modified_summary in comment_body
    assert expected_prod_plan_indirectly_modified_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_error_msg_when_applying_plan_with_bug(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with auto-categorization but has a mistake in the model so apply fails

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Bugged change made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            auto_categorize_changes=CategorizerConfig.all_full(),
            invalidate_environment_after_deploy=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make an error by adding a column that doesn't exist
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("non_existing_col", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        with pytest.raises(CICDBotError):
            command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_failure
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    summary = pr_checks_runs[2]["output"]["summary"].replace("\n", "")
    assert '**Skipped models*** `"memory"."sushi"."top_waiters"`' in summary
    assert '**Failed models*** `"memory"."sushi"."waiter_revenue_by_day"`' in summary
    assert 'Binder Error: Referenced column "non_existing_col" not found in FROM clause!' in summary

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 2
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[1]["conclusion"]).is_skipped
    assert (
        prod_plan_preview_checks_runs[1]["output"]["title"]
        == "Skipped generating prod plan preview since PR was not synchronized"
    )
    assert (
        prod_plan_preview_checks_runs[1]["output"]["summary"]
        == "Skipped generating prod plan preview since PR was not synchronized"
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped
    assert prod_checks_runs[1]["output"]["title"] == "Skipped deployment"
    assert (
        prod_checks_runs[1]["output"]["summary"]
        == "Skipped Deploying to Production because the PR environment was not updated"
    )

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 0
    assert not mock_pull_request.merge.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=failure\nprod_plan_preview=skipped\nprod_environment_synced=skipped\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_overlapping_changes_models(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with breaking and non-breaking change that both affect a common child. Ensuring that child is reported correctly.

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]

    # These changes have shared children and this ensures we don't repeat the children in the output
    # Make a non-breaking change
    model = controller._context.get_model("sushi.customers").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    # Make a breaking change
    model = controller._context.get_model("sushi.waiter_names").copy()
    model.seed.content += "10,Trey\n"
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.customers` (Non-breaking)
  **Kind:** FULL [full refresh]

- `memory.sushi.waiter_names` (Breaking)
  **Kind:** SEED [full refresh]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.active_customers` (Indirect Non-breaking)
  **Kind:** CUSTOM [full refresh]

- `memory.sushi.count_customers_active` (Indirect Non-breaking)
  **Kind:** FULL [full refresh]

- `memory.sushi.count_customers_inactive` (Indirect Non-breaking)
  **Kind:** FULL [full refresh]

- `memory.sushi.waiter_as_customer_by_day` (Indirect Breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-25 - 2022-12-31]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.customers` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -32,7 +32,8 @@
  
   SELECT DISTINCT
     CAST(o.customer_id AS INT) AS customer_id,
     m.status,
  -  d.zip
  +  d.zip,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN (
     WITH current_marketing AS (
  ```
  Indirectly Modified Children:
    - `memory.sushi.active_customers` (Indirect Non-breaking)
    - `memory.sushi.count_customers_active` (Indirect Non-breaking)
    - `memory.sushi.count_customers_inactive` (Indirect Non-breaking)
    - `memory.sushi.waiter_as_customer_by_day` (Indirect Breaking)


* `memory.sushi.waiter_names` (Breaking)


  Indirectly Modified Children:
    - `memory.sushi.waiter_as_customer_by_day` (Indirect Breaking)"""

    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.active_customers` (Indirect Non-breaking)
- `memory.sushi.count_customers_active` (Indirect Non-breaking)
- `memory.sushi.count_customers_inactive` (Indirect Non-breaking)
- `memory.sushi.waiter_as_customer_by_day` (Indirect Breaking)"""

    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_directly_modified_summary in prod_environment_synced_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 6
    assert "new_col" in get_columns(controller, "hello_world_2", "customers")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        f""":robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>"""
        in comment_body
    )
    assert expected_prod_plan_directly_modified_summary in comment_body
    assert expected_prod_plan_indirectly_modified_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_pr_add_model(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with an added model and auto-categorization will be backfilled, merged, and deployed to prod

    Scenario:
    - PR is not merged
    - /deploy command has been issued
    - Tests passed
    - PR Merge Method defined
    - Changes made in PR with auto-categorization
    """

    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_command_deploy.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            auto_categorize_changes=CategorizerConfig.all_full(),
            enable_deploy_command=True,
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)

    # Add a model
    (controller._context.path / "models" / "cicd_test_model.sql").write_text(
        """
        MODEL (
            name sushi.cicd_test_model,
            kind FULL                          
        );
                
        select 1;
        """
    )
    controller._context.load()
    assert '"memory"."sushi"."cicd_test_model"' in controller._context.models

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    print(test_checks_runs[2]["output"]["summary"])
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Added
- `memory.sushi.cicd_test_model` (Breaking)
  **Kind:** FULL [full refresh]"""
        in pr_env_summary
    )

    expected_prod_plan_summary = """**Added Models:**
- `memory.sushi.cicd_test_model` (Breaking)"""

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert expected_prod_plan_summary in prod_plan_preview_checks_runs[2]["output"]["summary"]

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_summary in prod_environment_synced_summary

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
- :arrow_forward: To **apply** this PR's plan to prod, comment:
  - `/deploy`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>"""
        in comment_body
    )
    assert expected_prod_plan_summary in comment_body

    assert (
        github_output_file.read_text()
        == "run_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
    )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_pr_delete_model(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with a removed model and auto-categorization will be backfilled, merged, and deployed to prod

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR with auto-categorization
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Remove a model
    model = controller._context.get_model("sushi.top_waiters").copy()
    del controller._context._models[model.fqn]
    controller._context.dag = controller._context.dag.prune(*controller._context._models.keys())

    github_output_file = tmp_path / "github_output.txt"

    mocker.patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.insert_overwrite_by_time_partition",
        side_effect=Exception("Test Exception"),
    )

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    print(test_checks_runs[2]["output"]["summary"])
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Removed
- `memory.sushi.top_waiters` (Breaking)"""
        in pr_env_summary
    )

    expected_prod_plan_summary = """**Removed Models:**
- `memory.sushi.top_waiters` (Breaking)"""

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert expected_prod_plan_summary in prod_plan_preview_checks_runs[2]["output"]["summary"]

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    prod_environment_synced_summary = prod_checks_runs[2]["output"]["summary"]
    assert "**Generated Prod Plan**" in prod_environment_synced_summary
    assert expected_prod_plan_summary in prod_environment_synced_summary

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 0

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    comment_body = created_comments[0].body
    assert (
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>"""
        in comment_body
    )
    assert expected_prod_plan_summary in comment_body

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_has_required_approval_but_not_base_branch(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    PR with a non-breaking change and auto-categorization will be backfilled, but NOT automatically merged or deployed to production if it is branched from a non-production branch.

    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is disabled
    - Changes made in PR with auto-categorization
    - PR is not merged, despite having required approval, since the base branch is not prod
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.base.ref = "feature/branch"
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(username="test_github", state="APPROVED")]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
    )
    controller._context.plan("prod", no_prompts=True, auto_apply=True)
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    # Make a non-breaking change
    model = controller._context.get_model("sushi.waiter_revenue_by_day").copy()
    model.query.select(exp.alias_("1", "new_col"), copy=False)
    controller._context.upsert_model(model)

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success
    assert test_checks_runs[2]["output"]["title"] == "Tests Passed"
    assert (
        test_checks_runs[2]["output"]["summary"].strip()
        == "**Successfully Ran `3` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    pr_env_summary = pr_checks_runs[2]["output"]["summary"]
    assert (
        """### Directly Modified
- `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  **Kind:** INCREMENTAL_BY_TIME_RANGE
  **Dates loaded in PR:** [2022-12-25 - 2022-12-31]"""
        in pr_env_summary
    )
    assert (
        """### Indirectly Modified
- `memory.sushi.top_waiters` (Indirect Non-breaking)
  **Kind:** VIEW [recreate view]"""
        in pr_env_summary
    )

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success
    expected_prod_plan_directly_modified_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  --- 
  
  +++ 
  
  @@ -17,7 +17,8 @@
  
   SELECT
     CAST(o.waiter_id AS INT) AS waiter_id,
     CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
  -  CAST(o.event_date AS DATE) AS event_date
  +  CAST(o.event_date AS DATE) AS event_date,
  +  1 AS new_col
   FROM sushi.orders AS o
   LEFT JOIN sushi.order_items AS oi
     ON o.id = oi.order_id AND o.event_date = oi.event_date
  ```
  Indirectly Modified Children:
    - `memory.sushi.top_waiters` (Indirect Non-breaking)"""

    expected_prod_plan_indirectly_modified_summary = """**Indirectly Modified:**
- `memory.sushi.top_waiters` (Indirect Non-breaking)"""

    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    prod_plan_preview_summary = prod_plan_preview_checks_runs[2]["output"]["summary"]
    assert (
        "This is a preview that shows the differences between this PR environment `hello_world_2` and `prod`"
        in prod_plan_preview_summary
    )
    assert expected_prod_plan_directly_modified_summary in prod_plan_preview_summary
    assert expected_prod_plan_indirectly_modified_summary in prod_plan_preview_summary

    assert "SQLMesh - Prod Environment Synced" not in controller._check_run_mapping

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success
    assert (
        approval_checks_runs[2]["output"]["title"]
        == "Obtained approval from required approvers: test_github"
    )
    assert (
        approval_checks_runs[2]["output"]["summary"]
        == """**List of possible required approvers:**
- `test_github`
"""
    )

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" not in get_columns(controller, None, "waiter_revenue_by_day")

    assert not mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`"""
    )

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\n"
        )


def test_unexpected_error_is_handled(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    mocker: MockerFixture,
):
    """
    Scenario:
    - Plan throws a SQLMeshError due to a migration version mismatch
    - Outcome should be a nice error like the CLI gives and not a stack trace
    """

    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockIssueComment] = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(),
        mock_out_context=True,
    )
    assert isinstance(controller, GithubController)

    assert isinstance(controller._context.apply, mocker.MagicMock)
    controller._context.apply.side_effect = SQLMeshError(
        "SQLGlot (local) is using version 'X' which is ahead of 'Y' (remote). Please run a migration"
    )

    command._update_pr_environment(controller)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs  # type: ignore
    assert pr_checks_runs[1]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    summary = pr_checks_runs[1]["output"]["summary"]
    assert (
        "**Error:** SQLGlot (local) is using version 'X' which is ahead of 'Y' (remote). Please run a migration"
        in pr_checks_runs[1]["output"]["summary"]
    )
    assert "SQLMeshError" not in summary
    assert "Traceback (most recent call last)" not in summary
