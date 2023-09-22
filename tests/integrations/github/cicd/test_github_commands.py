# type: ignore
from unittest import TestCase
from unittest.result import TestResult

from pytest_mock.plugin import MockerFixture

from sqlmesh.core import constants as c
from sqlmesh.core.plan import Plan
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd import command
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    MergeMethod,
)
from sqlmesh.utils.errors import PlanError

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]


def test_run_all_success_with_approvers_approved(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - No PR Merge Method defined
    - Delete environment is disabled
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=None, delete=False)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(prod_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.SUCCESS
    )

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>


**Models needing backfill (missing dates):**"""
    )


def test_run_all_success_with_approvers_approved_merge_delete(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(prod_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.SUCCESS
    )

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert mock_pull_request.merge.called
    assert controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>


**Models needing backfill (missing dates):**"""
    )


def test_run_all_missing_approval(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has not been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [
            make_pull_request_review(username="test_github", state="CHANGES_REQUESTED")
        ]
    )
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=None, delete=False)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]) == GithubCheckConclusion.SKIPPED

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.NEUTRAL
    )

    assert len(controller._context.apply.call_args_list) == 1
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == """**SQLMesh Bot Info**\n- PR Virtual Data Environment: hello_world_2"""
    )


def test_run_all_test_failed(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests failed
    - PR Merge Method defined
    - Delete environment is enabled
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    test_result = TestResult()
    test_result.addFailure(TestCase(), (None, None, None))
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda: (test_result, "some error")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=MergeMethod.MERGE, delete=True)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]) == GithubCheckConclusion.SKIPPED

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.FAILURE

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.SUCCESS
    )

    assert len(controller._context.apply.call_args_list) == 1
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert (
        created_comments[0].body
        == """**SQLMesh Bot Info**\n- PR Virtual Data Environment: hello_world_2"""
    )


def test_pr_update_failure(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - PR environment update failed
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    def raise_on_pr_plan(plan: Plan):
        if plan.environment.name == "hello_world_2":
            raise PlanError("Failed to update PR environment")

    controller._context.apply = mocker.MagicMock(side_effect=lambda plan: raise_on_pr_plan(plan))

    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(pr_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.ACTION_REQUIRED
    )

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]) == GithubCheckConclusion.SKIPPED

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.SUCCESS
    )

    assert len(controller._context.apply.call_args_list) == 1
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0


def test_prod_update_failure(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Prod environment update failed
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    def raise_on_prod_plan(plan: Plan):
        if plan.environment.name == c.PROD:
            raise PlanError("Failed to update Prod environment")

    controller._context.apply = mocker.MagicMock(side_effect=lambda plan: raise_on_prod_plan(plan))

    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(prod_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(prod_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.ACTION_REQUIRED
    )

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(approval_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(approval_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[2]["conclusion"])
        == GithubCheckConclusion.SUCCESS
    )

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>


**Models needing backfill (missing dates):**"""
    )


def test_comment_command_invalid(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    make_event_issue_comment,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Comment triggered this action and it was invalid
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
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
        make_event_issue_comment("created", "not a command"), github_client
    )
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)

    assert len(controller._check_run_mapping) == 0

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0


def test_comment_command_deploy_prod(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    make_event_issue_comment,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - No required approvers defined
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Comment triggered this action and it is a valid deploy comment
    """
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_pull_request = mock_repo.get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(lambda: [])
    mock_pull_request.merged = False
    mock_pull_request.merge = mocker.MagicMock()

    controller = make_controller(make_event_issue_comment("created", "/deploy"), github_client)
    controller._context._run_tests = mocker.MagicMock(side_effect=lambda: (TestResult(), ""))
    controller._context.users = []
    controller._context.invalidate_environment = mocker.MagicMock()

    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(pr_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(pr_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(prod_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(prod_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]) == GithubCheckStatus.QUEUED
    assert GithubCheckStatus(test_checks_runs[1]["status"]) == GithubCheckStatus.IN_PROGRESS
    assert GithubCheckStatus(test_checks_runs[2]["status"]) == GithubCheckStatus.COMPLETED
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]) == GithubCheckConclusion.SUCCESS

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 1
    assert GithubCheckStatus(approval_checks_runs[0]["status"]) == GithubCheckStatus.COMPLETED
    assert (
        GithubCheckConclusion(approval_checks_runs[0]["conclusion"])
        == GithubCheckConclusion.SKIPPED
    )

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert mock_pull_request.merge.called
    assert controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """**SQLMesh Bot Info**
- PR Virtual Data Environment: hello_world_2
<details>
  <summary>Prod Plan Being Applied</summary>


**Models needing backfill (missing dates):**"""
    )
