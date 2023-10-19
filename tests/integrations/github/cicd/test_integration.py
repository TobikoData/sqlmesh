"""
These integration tests are for testing integrating with SQLMesh and not integrating with Github.
Therefore Github calls are still mocked but context is fully evaluated.
"""
import typing as t

import pytest
from freezegun import freeze_time
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd import command
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    GithubController,
)
from tests.integrations.github.cicd.fixtures import MockIssueComment

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]


def get_environment_objects(controller: GithubController, environment: str) -> t.List[DataObject]:
    return controller._context.engine_adapter._get_data_objects(f"sushi__{environment}")


def get_num_days_loaded(controller: GithubController, environment: str, model: str) -> int:
    return int(
        controller._context.engine_adapter.fetchdf(
            f"SELECT distinct ds FROM sushi__{environment}.{model}"
        ).count()
    )


def get_columns(
    controller: GithubController, environment: t.Optional[str], model: str
) -> t.Dict[str, exp.DataType]:
    table = f"sushi__{environment}.{model}" if environment else f"sushi.{model}"
    return controller._context.engine_adapter.columns(table)


@freeze_time("2023-01-01 15:00:00")
def test_merge_pr_has_non_breaking_change(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>sushi.waiter_revenue_by_day</td><td>Non-breaking</td><td>2022-12-25 - 2022-12-31</td></tr></tbody></table>"""
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
    expected_prod_plan_summary = """```diff
--- 

+++ 

@@ -1,7 +1,8 @@

 SELECT
   CAST(o.waiter_id AS INT) AS waiter_id,
   CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
-  CAST(o.ds AS TEXT) AS ds
+  CAST(o.ds AS TEXT) AS ds,
+  1 AS new_col
 FROM sushi.orders AS o
 LEFT JOIN sushi.order_items AS oi
   ON o.id = oi.order_id AND o.ds = oi.ds
```

```

Directly Modified: sushi.waiter_revenue_by_day (Non-breaking)
└── Indirectly Modified Children:
    └── sushi.top_waiters (Indirect Non-breaking)

```

"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan_summary
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    assert (
        prod_checks_runs[2]["output"]["summary"]
        == "**Generated Prod Plan**\n" + expected_prod_plan_summary
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

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == f"""**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>

{expected_prod_plan_summary}
</details>

"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_merge_pr_has_non_breaking_change_diff_start(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>sushi.waiter_revenue_by_day</td><td>Non-breaking</td><td>2022-12-29 - 2022-12-31</td></tr></tbody></table>"""
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
    expected_prod_plan = """```diff
--- 

+++ 

@@ -1,7 +1,8 @@

 SELECT
   CAST(o.waiter_id AS INT) AS waiter_id,
   CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
-  CAST(o.ds AS TEXT) AS ds
+  CAST(o.ds AS TEXT) AS ds,
+  1 AS new_col
 FROM sushi.orders AS o
 LEFT JOIN sushi.order_items AS oi
   ON o.id = oi.order_id AND o.ds = oi.ds
```

```

Directly Modified: sushi.waiter_revenue_by_day (Non-breaking)
└── Indirectly Modified Children:
    └── sushi.top_waiters (Indirect Non-breaking)

```

**Models needing backfill (missing dates):**


* `sushi.waiter_revenue_by_day`: 2022-12-25 - 2022-12-28



"""
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    assert (
        prod_checks_runs[2]["output"]["summary"] == "**Generated Prod Plan**\n" + expected_prod_plan
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

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    # 7 days since the prod deploy went through and backfilled the remaining days
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == f"""**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>

{expected_prod_plan}
</details>

"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_merge_pr_has_non_breaking_change_no_categorization(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_action_required
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == ":warning: Action Required to create or update PR Environment `hello_world_2`. There are likely uncateogrized changes. Run `plan` locally to apply these changes. If you want the bot to automatically categorize changes, then check documentation (https://sqlmesh.readthedocs.io/en/stable/integrations/github/) for more information."
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
    skip_reason = "Skipped Deploying to Production because the PR environment was not updated"
    assert prod_checks_runs[1]["output"]["title"] == skip_reason
    assert prod_checks_runs[1]["output"]["summary"] == skip_reason

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


def test_merge_pr_has_no_changes(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
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
        pr_checks_runs[2]["output"]["summary"]
        == ":next_track_button: Skipped creating or updating PR Environment `hello_world_2`. No changes were detected compared to the prod environment."
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
    expected_prod_plan_summary = "No changes to apply."
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan_summary
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    assert (
        prod_checks_runs[2]["output"]["summary"]
        == "**Generated Prod Plan**\n" + expected_prod_plan_summary
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

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == f"""**SQLMesh Bot Info**
<details>
  <summary>Prod Plan Being Applied</summary>

{expected_prod_plan_summary}
</details>

"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_no_merge_since_no_deploy_signal(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>sushi.waiter_revenue_by_day</td><td>Non-breaking</td><td>2022-12-25 - 2022-12-31</td></tr></tbody></table>"""
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
    expected_prod_plan = """```diff
--- 

+++ 

@@ -1,7 +1,8 @@

 SELECT
   CAST(o.waiter_id AS INT) AS waiter_id,
   CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
-  CAST(o.ds AS TEXT) AS ds
+  CAST(o.ds AS TEXT) AS ds,
+  1 AS new_col
 FROM sushi.orders AS o
 LEFT JOIN sushi.order_items AS oi
   ON o.id = oi.order_id AND o.ds = oi.ds
```

```

Directly Modified: sushi.waiter_revenue_by_day (Non-breaking)
└── Indirectly Modified Children:
    └── sushi.top_waiters (Indirect Non-breaking)

```

"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped
    skip_reason = "Skipped Deploying to Production because a required approver has not approved"
    assert prod_checks_runs[1]["output"]["title"] == skip_reason
    assert prod_checks_runs[1]["output"]["summary"] == skip_reason

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_neutral
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
        == """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_no_merge_since_no_deploy_signal_no_approvers_defined(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>sushi.waiter_revenue_by_day</td><td>Non-breaking</td><td>2022-12-30 - 2022-12-31</td></tr></tbody></table>"""
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
    expected_prod_plan = """```diff
--- 

+++ 

@@ -1,7 +1,8 @@

 SELECT
   CAST(o.waiter_id AS INT) AS waiter_id,
   CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
-  CAST(o.ds AS TEXT) AS ds
+  CAST(o.ds AS TEXT) AS ds,
+  1 AS new_col
 FROM sushi.orders AS o
 LEFT JOIN sushi.order_items AS oi
   ON o.id = oi.order_id AND o.ds = oi.ds
```

```

Directly Modified: sushi.waiter_revenue_by_day (Non-breaking)
└── Indirectly Modified Children:
    └── sushi.top_waiters (Indirect Non-breaking)

```

**Models needing backfill (missing dates):**


* `sushi.waiter_revenue_by_day`: 2022-12-25 - 2022-12-29



"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan
    )

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
        == """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_deploy_comment_pre_categorized(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
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
    model.query.expressions.append(exp.alias_("1", "new_col"))
    controller._context.upsert_model(model)

    # Manually categorize the change as non-breaking and don't backfill anything
    controller._context.plan(
        "dev",
        no_prompts=True,
        auto_apply=True,
        categorizer_config=CategorizerConfig.all_full(),
        skip_backfill=True,
    )

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        pr_checks_runs[2]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>sushi.waiter_revenue_by_day</td><td>Non-breaking</td><td>2022-12-31 - 2022-12-31</td></tr></tbody></table>"""
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
    expected_prod_plan = """```diff
--- 

+++ 

@@ -1,7 +1,8 @@

 SELECT
   CAST(o.waiter_id AS INT) AS waiter_id,
   CAST(SUM(oi.quantity * i.price) AS DOUBLE) AS revenue,
-  CAST(o.ds AS TEXT) AS ds
+  CAST(o.ds AS TEXT) AS ds,
+  1 AS new_col
 FROM sushi.orders AS o
 LEFT JOIN sushi.order_items AS oi
   ON o.id = oi.order_id AND o.ds = oi.ds
```

```

Directly Modified: sushi.waiter_revenue_by_day (Non-breaking)
└── Indirectly Modified Children:
    └── sushi.top_waiters (Indirect Non-breaking)

```

**Models needing backfill (missing dates):**


* `sushi.waiter_revenue_by_day`: 2022-12-25 - 2022-12-30



"""
    assert prod_plan_preview_checks_runs[2]["output"]["title"] == "Prod Plan Preview"
    assert (
        prod_plan_preview_checks_runs[2]["output"]["summary"]
        == "**Preview of Prod Plan**\n" + expected_prod_plan
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success
    assert prod_checks_runs[2]["output"]["title"] == "Deployed to Prod"
    assert (
        prod_checks_runs[2]["output"]["summary"] == "**Generated Prod Plan**\n" + expected_prod_plan
    )

    assert "SQLMesh - Has Required Approval" not in controller._check_run_mapping

    assert len(get_environment_objects(controller, "hello_world_2")) == 2
    assert get_num_days_loaded(controller, "hello_world_2", "waiter_revenue_by_day") == 7
    assert "new_col" in get_columns(controller, "hello_world_2", "waiter_revenue_by_day")
    assert "new_col" in get_columns(controller, None, "waiter_revenue_by_day")

    assert mock_pull_request.merge.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == f"""**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>

{expected_prod_plan}
</details>

"""
    )


@freeze_time("2023-01-01 15:00:00")
def test_error_msg_when_applying_plan_with_bug(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
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
    model.query.expressions.append(exp.alias_("non_existing_col", "new_col"))
    controller._context.upsert_model(model)

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
        == "**Successfully Ran `2` Tests Against `duckdb`**"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_failure
    assert pr_checks_runs[2]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"
    assert (
        'Binder Error: Referenced column "non_existing_col" not found in FROM clause!'
        in pr_checks_runs[2]["output"]["summary"]
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
    skip_reason = "Skipped Deploying to Production because the PR environment was not updated"
    assert prod_checks_runs[1]["output"]["title"] == skip_reason
    assert prod_checks_runs[1]["output"]["summary"] == skip_reason

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
